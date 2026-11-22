import json
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any

from shop_bot.data_manager import database
from shop_bot.data_manager import remnawave_repository as rw_repo
from shop_bot.modules import remnawave_api

logger = logging.getLogger(__name__)

MSK = timezone(timedelta(hours=3), name="MSK")
DAY_MS = Decimal(86400000)
GIB = Decimal(1024 ** 3)


class TariffChangeError(RuntimeError):
    def __init__(self, message: str, status_code: int = 400):
        super().__init__(message)
        self.status_code = status_code


def _get_key(key_id: int) -> dict[str, Any]:
    key = rw_repo.get_key_by_id(key_id)
    if not key:
        raise TariffChangeError("Подписка не найдена", 404)
    if not key.get("host_name"):
        raise TariffChangeError("У подписки не указан хост")
    return key


def _get_plan_for_host(plan_id: Any, host_name: str) -> dict[str, Any] | None:
    try:
        normalized_id = int(plan_id)
    except (TypeError, ValueError):
        return None
    plan = rw_repo.get_plan_by_id(normalized_id)
    if not plan:
        return None
    if database.normalize_host_name(str(plan.get("host_name") or "")) != database.normalize_host_name(host_name):
        return None
    return plan


def _find_current_plan(key: dict[str, Any]) -> dict[str, Any]:
    plan = _get_plan_for_host(key.get("plan_id"), key["host_name"])
    if plan:
        return plan
    with sqlite3.connect(database.DB_FILE) as conn:
        row = conn.execute(
            """
            SELECT metadata
            FROM transactions
            WHERE user_id = ?
              AND LOWER(COALESCE(status, '')) IN ('paid', 'completed', 'success')
              AND json_valid(metadata)
              AND CAST(json_extract(metadata, '$.key_id') AS INTEGER) = ?
              AND json_extract(metadata, '$.plan_id') IS NOT NULL
            ORDER BY datetime(created_date) DESC, transaction_id DESC
            LIMIT 1
            """,
            (key.get("user_id"), key["key_id"]),
        ).fetchone()
    if row:
        try:
            metadata = json.loads(row[0] or "{}")
            plan = _get_plan_for_host(metadata.get("plan_id"), key["host_name"])
            if plan:
                return plan
        except (TypeError, ValueError, json.JSONDecodeError):
            pass
    raise TariffChangeError("Не удалось определить текущий тариф подписки")


def _parse_expiry_ms(key: dict[str, Any]) -> int:
    value = key.get("expire_at") or key.get("expiry_date")
    if isinstance(value, datetime):
        expire_at = value
    else:
        raw = str(value or "").strip()
        if not raw:
            raise TariffChangeError("Для подписки без срока действия пересчёт недоступен")
        try:
            expire_at = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError as exc:
            raise TariffChangeError("Некорректная дата окончания подписки") from exc
    if expire_at.tzinfo is None:
        expire_at = expire_at.replace(tzinfo=MSK)
    return int(expire_at.timestamp() * 1000)


def _plan_rate(plan: dict[str, Any]) -> tuple[int, Decimal, Decimal]:
    try:
        months = int(plan.get("months") or 0)
        duration_days = int(plan.get("duration_days") or 0) or months * 30
    except (TypeError, ValueError) as exc:
        raise TariffChangeError(f"Некорректная длительность тарифа «{plan.get('plan_name') or plan.get('plan_id')}»") from exc
    if duration_days <= 0:
        raise TariffChangeError(f"Не указана длительность тарифа «{plan.get('plan_name') or plan.get('plan_id')}»")
    try:
        price = Decimal(str(plan.get("price") or "0"))
        if not price.is_finite() or price <= 0:
            raise InvalidOperation
        daily_price = price / Decimal(duration_days)
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise TariffChangeError(f"Цена тарифа «{plan.get('plan_name') or plan.get('plan_id')}» должна быть больше нуля") from exc
    return duration_days, price, daily_price


def _plan_match_key(plan: dict[str, Any], current_duration: int, current_months: int) -> tuple[Any, ...]:
    try:
        months = int(plan.get("months") or 0)
        duration = int(plan.get("duration_days") or 0) or months * 30
        plan_id = int(plan.get("plan_id") or 0)
    except (TypeError, ValueError):
        return (1, 1, 1, 10 ** 9, 10 ** 9)
    return (
        0 if duration == current_duration else 1,
        0 if months == current_months else 1,
        0 if duration == 30 or months == 1 else 1,
        abs(duration - current_duration),
        plan_id,
    )


def _panel_identity(host: dict[str, Any], fallback_url: str) -> str:
    return str(host.get("remnawave_base_url") or fallback_url or "").strip().rstrip("/").casefold()


def _plan_limits(plan: dict[str, Any], host: dict[str, Any], purchased_traffic_gb: int = 0) -> dict[str, Any]:
    try:
        raw_gb = plan.get("traffic_limit_gb")
        raw_bytes = plan.get("traffic_limit_bytes")
        if raw_gb not in (None, ""):
            traffic_gb = Decimal(str(raw_gb))
            if not traffic_gb.is_finite() or traffic_gb < 0:
                raise InvalidOperation
            traffic_bytes = int((traffic_gb * GIB).to_integral_value(rounding=ROUND_HALF_UP))
        elif raw_bytes not in (None, ""):
            traffic_bytes = int(raw_bytes)
            if traffic_bytes < 0:
                raise InvalidOperation
            traffic_gb = Decimal(traffic_bytes) / GIB
        else:
            traffic_gb = Decimal(0)
            traffic_bytes = 0
        hwid_limit = int(plan.get("hwid_limit") or 0)
        if hwid_limit < 0:
            raise InvalidOperation
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise TariffChangeError(f"Некорректные лимиты тарифа «{plan.get('plan_name') or plan.get('plan_id')}»") from exc
    purchased_bytes = int((Decimal(max(0, purchased_traffic_gb)) * GIB).to_integral_value(rounding=ROUND_HALF_UP))
    strategy = str(
        plan.get("traffic_limit_strategy")
        or host.get("default_traffic_strategy")
        or host.get("traffic_limit_strategy")
        or "NO_RESET"
    ).strip().upper()
    if strategy not in {"NO_RESET", "DAY", "WEEK", "MONTH", "MONTH_ROLLING"}:
        raise TariffChangeError(f"Некорректная стратегия сброса трафика тарифа «{plan.get('plan_name') or plan.get('plan_id')}»")
    squad_uuid = str(plan.get("squad_uuid") or host.get("squad_uuid") or "").strip()
    return {
        "traffic_limit_gb": float(traffic_gb),
        "traffic_limit_bytes": traffic_bytes,
        "purchased_traffic_gb": max(0, purchased_traffic_gb),
        "total_traffic_limit_gb": float(Decimal(traffic_bytes + purchased_bytes) / GIB),
        "total_traffic_limit_bytes": traffic_bytes + purchased_bytes,
        "traffic_limit_strategy": strategy,
        "hwid_limit": hwid_limit,
        "squad_uuid": squad_uuid,
    }


def _build_plan_option(
    plan: dict[str, Any],
    host: dict[str, Any],
    current_plan_id: int,
    current_daily_price: Decimal,
    remaining_ms: int,
    now_ms: int,
    expiry_ms: int,
    purchased_traffic_gb: int,
) -> dict[str, Any]:
    duration_days, price, daily_price = _plan_rate(plan)
    limits = _plan_limits(plan, host, purchased_traffic_gb)
    if not limits["squad_uuid"]:
        raise TariffChangeError(f"Для тарифа «{plan.get('plan_name') or plan.get('plan_id')}» не указан Squad UUID")
    plan_id = int(plan["plan_id"])
    is_current = plan_id == current_plan_id
    if remaining_ms > 0:
        converted_ms = int(
            (Decimal(remaining_ms) * current_daily_price / daily_price).to_integral_value(rounding=ROUND_HALF_UP)
        )
        new_expiry_ms = now_ms + converted_ms
    else:
        converted_ms = 0
        new_expiry_ms = expiry_ms
    change_type = "equal"
    if daily_price > current_daily_price:
        change_type = "upgrade"
    elif daily_price < current_daily_price:
        change_type = "downgrade"
    return {
        "plan_id": plan_id,
        "plan_name": str(plan.get("plan_name") or f"Тариф #{plan_id}"),
        "host_name": str(plan.get("host_name") or ""),
        "months": int(plan.get("months") or 0),
        "duration_days": duration_days,
        "price": float(price),
        "daily_price": float(daily_price),
        **limits,
        "is_current": is_current,
        "available": not is_current,
        "error": "Это текущий тариф" if is_current else None,
        "change_type": change_type,
        "new_remaining_ms": converted_ms,
        "new_remaining_days": converted_ms / 86400000,
        "new_expiry_ms": new_expiry_ms,
    }


def _build_tariff_data(key: dict[str, Any], now: datetime | None = None) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    host = rw_repo.get_squad(key["host_name"])
    if not host:
        raise TariffChangeError("Хост подписки не найден", 404)
    current_plan = _find_current_plan(key)
    current_duration, current_price, current_daily_price = _plan_rate(current_plan)
    current_plan_id = int(current_plan["plan_id"])
    expiry_ms = _parse_expiry_ms(key)
    now_dt = now or datetime.now(MSK)
    if now_dt.tzinfo is None:
        now_dt = now_dt.replace(tzinfo=MSK)
    now_ms = int(now_dt.timestamp() * 1000)
    remaining_ms = max(0, expiry_ms - now_ms)
    remaining_value = (Decimal(remaining_ms) / DAY_MS) * current_daily_price
    purchased_traffic_gb = int(rw_repo.get_key_active_traffic_gb(key["key_id"]) or 0)
    current_limits = _plan_limits(current_plan, host, purchased_traffic_gb)
    current = {
        "plan_id": current_plan_id,
        "plan_name": str(current_plan.get("plan_name") or f"Тариф #{current_plan_id}"),
        "host_name": str(current_plan.get("host_name") or key["host_name"]),
        "months": int(current_plan.get("months") or 0),
        "duration_days": current_duration,
        "price": float(current_price),
        "daily_price": float(current_daily_price),
        **current_limits,
        "remaining_ms": remaining_ms,
        "remaining_days": remaining_ms / 86400000,
        "remaining_value_rub": float(remaining_value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)),
        "expiry_ms": expiry_ms,
        "is_expired": remaining_ms <= 0,
    }
    options = []
    unavailable = []
    current_months = int(current_plan.get("months") or 0)
    panel_fallback = str(rw_repo.get_setting("remnawave_base_url") or "")
    current_panel = _panel_identity(host, panel_fallback)
    for target_host in rw_repo.list_squads(active_only=True):
        target_host_name = str(target_host.get("host_name") or "").strip()
        if not target_host_name or database.normalize_host_name(target_host_name) == database.normalize_host_name(key["host_name"]):
            continue
        target_panel = _panel_identity(target_host, panel_fallback)
        if not current_panel or not target_panel or target_panel != current_panel:
            unavailable.append({
                "plan_id": 0,
                "plan_name": target_host_name,
                "available": False,
                "is_current": False,
                "error": "Не настроен адрес Remnawave" if not current_panel or not target_panel else "Хост подключён к другой панели Remnawave",
            })
            continue
        candidates = [
            plan for plan in (rw_repo.get_plans_for_host(target_host_name) or [])
            if plan.get("is_active") not in (0, "0", False)
        ]
        if not candidates:
            unavailable.append({
                "plan_id": 0,
                "plan_name": target_host_name,
                "available": False,
                "is_current": False,
                "error": "На хосте нет активных тарифов",
            })
            continue
        candidates.sort(key=lambda item: _plan_match_key(item, current_duration, current_months))
        last_error = None
        for plan in candidates:
            try:
                option = _build_plan_option(
                    plan,
                    target_host,
                    current_plan_id,
                    current_daily_price,
                    remaining_ms,
                    now_ms,
                    expiry_ms,
                    purchased_traffic_gb,
                )
                option["matched_plan_name"] = option["plan_name"]
                option["plan_name"] = target_host_name
                option["host_name"] = target_host_name
                options.append(option)
                break
            except (TariffChangeError, TypeError, ValueError) as exc:
                last_error = exc
        else:
            unavailable.append({
                "plan_id": 0,
                "plan_name": target_host_name,
                "available": False,
                "is_current": False,
                "error": str(last_error or "На хосте нет активных тарифов"),
            })
    data = {
        "key": {
            "key_id": int(key["key_id"]),
            "user_id": int(key.get("user_id") or 0),
            "host_name": key["host_name"],
            "email": key.get("key_email") or key.get("email") or "",
            "subscription_url": key.get("subscription_url") or "",
        },
        "current": current,
        "plans": options + unavailable,
        "calculated_at_ms": now_ms,
    }
    return data, current_plan, host


def get_key_tariff_options(key_id: int) -> dict[str, Any]:
    key = _get_key(key_id)
    data, _, _ = _build_tariff_data(key)
    return data


def _resolve_external_squad(key: dict[str, Any], plan: dict[str, Any]) -> str | None:
    metadata: dict[str, Any] = {}
    try:
        parsed = json.loads(plan.get("metadata") or "{}")
        if isinstance(parsed, dict):
            metadata = parsed
    except (TypeError, json.JSONDecodeError):
        pass
    for field in ("external_squad_uuid", "externalSquadUuid"):
        value = str(metadata.get(field) or "").strip()
        if value and value != "0":
            return value
    user = rw_repo.get_user(key.get("user_id")) or {}
    if user.get("seller_active"):
        seller = database.get_seller_user(key.get("user_id")) or {}
        value = str(seller.get("seller_uuid") or "").strip()
        if value and value != "0":
            return value
    return None


async def _apply_remnawave_state(
    key: dict[str, Any],
    *,
    expiry_ms: int,
    hwid_limit: int | None,
    traffic_limit_gb: float,
    traffic_limit_strategy: str,
    squad_uuid: str,
    external_squad_uuid: str | None,
    status: str,
    host_name: str,
) -> dict[str, Any] | None:
    return await remnawave_api.create_or_update_key_on_host(
        host_name=host_name,
        email=key.get("key_email") or key.get("email"),
        expiry_timestamp_ms=expiry_ms,
        force_expiry=True,
        description=key.get("description"),
        tag=key.get("tag"),
        telegram_id=key.get("user_id"),
        hwid_limit=hwid_limit,
        reset_hwid_limit=hwid_limit is None,
        traffic_limit_gb=traffic_limit_gb,
        traffic_limit_strategy=traffic_limit_strategy,
        external_squad_uuid=external_squad_uuid,
        set_external_squad=True,
        squad_uuid=squad_uuid,
        status=status,
        allow_create=False,
    )


async def change_key_tariff(key_id: int, target_plan_id: int) -> dict[str, Any]:
    key = _get_key(key_id)
    data, current_plan, host = _build_tariff_data(key)
    target = next((item for item in data["plans"] if item.get("plan_id") == target_plan_id), None)
    if not target:
        raise TariffChangeError("Новый тариф не найден на хосте подписки", 404)
    if target.get("is_current"):
        raise TariffChangeError("Этот тариф уже установлен")
    if not target.get("available"):
        raise TariffChangeError(target.get("error") or "Выбранный тариф недоступен")
    target_plan = _get_plan_for_host(target_plan_id, target["host_name"])
    if not target_plan:
        raise TariffChangeError("Новый тариф не найден на выбранном хосте", 404)
    remote_before = await remnawave_api.get_user_by_id(key.get("remnawave_user_uuid"), host_name=key["host_name"])
    if not remote_before:
        raise TariffChangeError("Пользователь подписки не найден в Remnawave", 404)
    external_squad = _resolve_external_squad(key, target_plan)
    status = "DISABLED" if remote_before.get("status") == "DISABLED" else "ACTIVE"
    result = await _apply_remnawave_state(
        key,
        expiry_ms=target["new_expiry_ms"],
        hwid_limit=target["hwid_limit"] or None,
        traffic_limit_gb=target["total_traffic_limit_gb"],
        traffic_limit_strategy=target["traffic_limit_strategy"],
        squad_uuid=target["squad_uuid"],
        external_squad_uuid=external_squad,
        status=status,
        host_name=target["host_name"],
    )
    if not result or not result.get("expiry_timestamp_ms"):
        raise TariffChangeError("Не удалось обновить подписку в Remnawave. Данные в базе не изменены", 502)
    actual_expiry_ms = int(result["expiry_timestamp_ms"])
    actual_traffic_bytes = result.get("traffic_limit_bytes")
    if actual_traffic_bytes is None:
        actual_traffic_bytes = target["total_traffic_limit_bytes"]
    updated = rw_repo.update_key(
        key_id,
        host_name=target["host_name"],
        squad_uuid=target["squad_uuid"],
        remnawave_user_uuid=str(result.get("client_uuid") or key.get("remnawave_user_uuid") or "") or None,
        short_uuid=str(result.get("short_uuid") or key.get("short_uuid") or "") or None,
        expire_at_ms=actual_expiry_ms,
        traffic_limit_bytes=int(actual_traffic_bytes),
        traffic_limit_strategy=str(result.get("traffic_limit_strategy") or target["traffic_limit_strategy"]),
        plan_id=target_plan_id,
    )
    if not updated:
        old_squad = str(key.get("squad_uuid") or current_plan.get("squad_uuid") or host.get("squad_uuid") or "").strip()
        try:
            rollback = await _apply_remnawave_state(
                key,
                expiry_ms=_parse_expiry_ms(key),
                hwid_limit=remote_before.get("hwidDeviceLimit"),
                traffic_limit_gb=int(remote_before.get("trafficLimitBytes") or 0) / (1024 ** 3),
                traffic_limit_strategy=str(remote_before.get("trafficLimitStrategy") or key.get("traffic_limit_strategy") or "NO_RESET"),
                squad_uuid=old_squad,
                external_squad_uuid=remote_before.get("externalSquadUuid"),
                status=status,
                host_name=key["host_name"],
            )
            if not rollback:
                logger.critical("Не удалось откатить ключ %s в Remnawave после ошибки базы данных", key_id)
        except Exception:
            logger.exception("Ошибка отката ключа %s в Remnawave после ошибки базы данных", key_id)
        raise TariffChangeError("Remnawave обновлён, но база данных вернула ошибку. Выполнена попытка отката", 500)
    now_ms = int(datetime.now(MSK).timestamp() * 1000)
    actual_remaining_ms = max(0, actual_expiry_ms - now_ms)
    return {
        "ok": True,
        "message": f"Тариф изменён на «{target['plan_name']}»",
        "key_id": key_id,
        "plan_id": target_plan_id,
        "plan_name": target["plan_name"],
        "new_expiry_ms": actual_expiry_ms,
        "new_remaining_ms": actual_remaining_ms,
        "new_remaining_days": actual_remaining_ms / 86400000,
        "subscription_url": key.get("subscription_url") or "",
    }
