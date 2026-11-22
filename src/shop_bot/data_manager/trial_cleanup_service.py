import logging
from datetime import datetime, timedelta, timezone

from shop_bot.data_manager import database
from shop_bot.data_manager import remnawave_repository as rw_repo
from shop_bot.modules import remnawave_api

logger = logging.getLogger(__name__)

MIN_AGE_DAYS = 1
DEFAULT_INTERVAL_HOURS = 24

_started_at = datetime.now(timezone(timedelta(hours=3)))
_last_run_at: datetime | None = None


def _msk_now() -> datetime:
    return datetime.now(timezone(timedelta(hours=3)))


def _to_int(value, default: int) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def _to_number(value) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _check_connected(remote_user: dict) -> bool:
    traffic = remote_user.get("userTraffic") or {}
    if traffic.get("firstConnectedAt") or traffic.get("onlineAt"):
        return True
    return _to_number(traffic.get("usedTrafficBytes")) > 0 or _to_number(traffic.get("lifetimeUsedTrafficBytes")) > 0


async def cleanup_trial_keys(min_age_days: int) -> dict:
    global _last_run_at
    days = max(MIN_AGE_DAYS, _to_int(min_age_days, MIN_AGE_DAYS))
    created_before = (_msk_now().replace(tzinfo=None) - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    stats = {"checked": 0, "removed": 0, "reset": 0, "connected": 0, "errors": 0, "items": []}
    try:
        candidates = database.get_trial_cleanup_candidates(created_before)
    except Exception as exc:
        logger.error("Очистка триалов: не удалось получить список ключей: %s", exc)
        return stats

    for key in candidates:
        stats["checked"] += 1
        owner_id = key.get("user_id")
        host_name = (key.get("host_name") or "").strip()
        email = (key.get("email") or key.get("key_email") or "").strip()
        entry = {
            "key_id": key.get("key_id"),
            "user_id": owner_id,
            "email": email,
            "status": "kept",
            "reason": "",
            "trial_reset": False,
        }
        remote_user = None
        try:
            remote_uuid = key.get("remnawave_user_uuid")
            if remote_uuid:
                remote_user = await remnawave_api.get_user_by_id(remote_uuid, host_name=host_name or None)
            if not remote_user and email:
                remote_user = await remnawave_api.get_user_by_email(email, host_name=host_name or None)
        except Exception as exc:
            stats["errors"] += 1
            entry["status"] = "error"
            entry["reason"] = "панель не ответила на запрос"
            stats["items"].append(entry)
            logger.warning("Очистка триалов: не удалось проверить '%s': %s", email, exc)
            continue

        if isinstance(remote_user, dict):
            if _check_connected(remote_user):
                stats["connected"] += 1
                entry["reason"] = "уже подключался — оставлен"
                stats["items"].append(entry)
                continue
            if not host_name or not email:
                stats["errors"] += 1
                entry["status"] = "error"
                entry["reason"] = "нет сервера или email в базе"
                stats["items"].append(entry)
                continue
            try:
                deleted_remote = await remnawave_api.delete_client_on_host(host_name, email)
            except Exception as exc:
                deleted_remote = False
                logger.warning("Очистка триалов: не удалось удалить '%s' из панели: %s", email, exc)
            if not deleted_remote:
                stats["errors"] += 1
                entry["status"] = "error"
                entry["reason"] = "не удалось удалить в панели"
                stats["items"].append(entry)
                continue
            entry["reason"] = "не подключался — удалён"
        else:
            entry["reason"] = "в панели уже нет — запись очищена"

        if email and rw_repo.delete_key_by_email(email):
            stats["removed"] += 1
        if owner_id and database.reset_trial_used(int(owner_id)):
            stats["reset"] += 1
            entry["trial_reset"] = True
        entry["status"] = "removed"
        if entry["trial_reset"]:
            entry["reason"] += ", пробный период освобождён"
        stats["items"].append(entry)

    _last_run_at = _msk_now()
    logger.info(
        "Очистка триалов: старше %s дн., проверено %s, удалено %s, освобождено %s, подключались %s, ошибок %s.",
        days,
        stats["checked"],
        stats["removed"],
        stats["reset"],
        stats["connected"],
        stats["errors"],
    )
    return stats


async def maybe_run_auto_cleanup() -> dict | None:
    if (rw_repo.get_setting("trial_cleanup_auto") or "false").strip().lower() != "true":
        return None
    interval_hours = max(1, _to_int(rw_repo.get_setting("trial_cleanup_interval_hours"), DEFAULT_INTERVAL_HOURS))
    now = _msk_now()
    reference = _last_run_at or _started_at
    if (now - reference).total_seconds() < interval_hours * 3600:
        return None
    days = max(MIN_AGE_DAYS, _to_int(rw_repo.get_setting("trial_cleanup_days"), MIN_AGE_DAYS))
    return await cleanup_trial_keys(days)
