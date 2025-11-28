import json
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Any
from urllib.parse import quote
import re

import httpx

from shop_bot.data_manager import remnawave_repository as rw_repo

logger = logging.getLogger(__name__)

try:
    logging.getLogger("httpx").setLevel(logging.WARNING)
except Exception:
    pass


class RemnawaveAPIError(RuntimeError):
    """Base error for Remnawave API interactions."""


def _normalize_email_for_remnawave(email: str) -> str:
    """Normalize and validate email for Remnawave API.

    - Lowercases the email
    - If domain is missing or email invalid, tries to sanitize local-part by replacing
      any characters outside [a-z0-9._+-] with '_'
    - Validates with a conservative regex that excludes '/'
    - Raises RemnawaveAPIError if validation still fails
    """
    if not email:
        raise RemnawaveAPIError("email is required")
    e = (email or "").strip().lower()

    if "@" not in e:
        raise RemnawaveAPIError(f"Invalid email (no domain): {email}")
    local, domain = e.split("@", 1)

    local = re.sub(r"[^a-z0-9._+\-]", "_", local)

    local = re.sub(r"\.+", ".", local)

    local = local.strip("._-")

    if not local or not re.match(r"^[a-z0-9]", local):
        local = f"u{local}" if local else f"user{int(datetime.utcnow().timestamp())}"
    e_sanitized = f"{local}@{domain}"

    pattern = re.compile(r"^[a-z0-9](?:[a-z0-9._+\-]*[a-z0-9])?@[a-z0-9](?:[a-z0-9\-]*[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9\-]*[a-z0-9])?)+$")

    if ".." in e_sanitized or not pattern.match(e_sanitized):
        raise RemnawaveAPIError(f"Invalid email after normalization: {e_sanitized}")
    return e_sanitized


def _normalize_username_for_remnawave(name: str | None) -> str:
    """Normalize username to only letters, numbers, underscores and dashes.

    - Lowercase
    - Replace invalid characters with '_'
    - Trim leading/trailing '_' and '-'
    - Ensure starts with alnum; if not, prefix with 'u'
    - Limit length to 32 characters
    - Fallback to 'user<timestamp>' if empty
    """
    base = (name or "").strip().lower()
    base = re.sub(r"[^a-z0-9_\-]", "_", base)
    base = base.strip("_-")
    if not base or not re.match(r"^[a-z0-9]", base):
        base = f"u{base}" if base else f"user{int(datetime.utcnow().timestamp())}"
    if len(base) > 32:
        base = base[:32].rstrip("_-") or base[:32]

    if len(base) < 3:

        suffix = str(int(datetime.utcnow().timestamp()))
        base = (base + suffix)[:3]

        if len(base) < 3:
            base = (base + "usr")[:3]
    return base

def _load_config() -> dict[str, Any]:
    """Backward-compatible global config loader (deprecated)."""
    base_url = (rw_repo.get_setting("remnawave_base_url") or "").strip().rstrip("/")
    token = (rw_repo.get_setting("remnawave_api_token") or "").strip()
    cookies = {}
    is_local = False
    if not base_url or not token:
        raise RemnawaveAPIError("Remnawave API settings are not configured")
    return {"base_url": base_url, "token": token, "cookies": cookies, "is_local": is_local}


def _load_config_for_host(host_name: str) -> dict[str, Any]:
    """Load Remnawave API config for a specific host from xui_hosts."""
    if not host_name:
        raise RemnawaveAPIError("host_name is required")
    squad = rw_repo.get_squad(host_name)
    if not squad:
        raise RemnawaveAPIError(f"Host '{host_name}' not found")
    base_url = (squad.get("remnawave_base_url") or "").strip().rstrip("/")
    token = (squad.get("remnawave_api_token") or "").strip()
    if not base_url or not token:

        try:
            return _load_config()
        except RemnawaveAPIError:
            raise RemnawaveAPIError(f"Remnawave API settings are not configured for host '{host_name}'")
    return {"base_url": base_url, "token": token, "cookies": {}, "is_local": False}


def _build_headers(config: dict[str, Any]) -> dict[str, str]:
    headers = {
        "Authorization": f"Bearer {config['token']}",
        "Content-Type": "application/json",
    }
    if config.get("is_local"):
        headers["X-Forwarded-Proto"] = "https"
        headers["X-Forwarded-For"] = "127.0.0.1"
    return headers


async def _request(
    method: str,
    path: str,
    *,
    json_payload: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
    expected_status: tuple[int, ...] = (200,),
) -> httpx.Response:

    config = _load_config()
    url = f"{config['base_url']}{path}"
    headers = _build_headers(config)

    async with httpx.AsyncClient(cookies=config["cookies"], timeout=30.0) as client:

        try:
            full_url = httpx.URL(url).copy_merge_params(params or {})
            logger.info("➡️ Remnawave: %s %s", method.upper(), str(full_url))
        except Exception:
            pass
        t0 = time.perf_counter()
        response = await client.request(
            method=method,
            url=url,
            headers=headers,
            json=json_payload,
            params=params,
        )
        dt_ms = int((time.perf_counter() - t0) * 1000)
        try:
            status = response.status_code
            ok = "OK" if status in expected_status else "ERROR"
            logger.info("⬅️ Remnawave: %s %s — %s (%d мс)", method.upper(), path, f"{status} {ok}", dt_ms)
        except Exception:
            pass

    if response.status_code not in expected_status:
        try:
            detail = response.json()
        except json.JSONDecodeError:
            detail = response.text
        logger.warning("Remnawave API %s %s завершился ошибкой: %s", method, path, detail)
        raise RemnawaveAPIError(f"Remnawave API request failed: {response.status_code} {detail}")

    return response


async def _request_for_host(
    host_name: str,
    method: str,
    path: str,
    *,
    json_payload: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
    expected_status: tuple[int, ...] = (200,),
) -> httpx.Response:
    config = _load_config_for_host(host_name)
    url = f"{config['base_url']}{path}"
    headers = _build_headers(config)

    async with httpx.AsyncClient(cookies=config["cookies"], timeout=30.0) as client:

        try:
            full_url = httpx.URL(url).copy_merge_params(params or {})
            logger.info("➡️ Remnawave[%s]: %s %s", host_name, method.upper(), str(full_url))
        except Exception:
            pass
        t0 = time.perf_counter()
        response = await client.request(
            method=method,
            url=url,
            headers=headers,
            json=json_payload,
            params=params,
        )
        dt_ms = int((time.perf_counter() - t0) * 1000)
        try:
            status = response.status_code
            ok = "OK" if status in expected_status else "ERROR"
            logger.info("⬅️ Remnawave[%s]: %s %s — %s (%d мс)", host_name, method.upper(), path, f"{status} {ok}", dt_ms)
        except Exception:
            pass

    if response.status_code not in expected_status:
        try:
            detail = response.json()
        except json.JSONDecodeError:
            detail = response.text
        logger.warning("Remnawave API %s %s завершился ошибкой: %s", method, path, detail)
        raise RemnawaveAPIError(f"Remnawave API request failed: {response.status_code} {detail}")

    return response


def _to_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt_utc = dt.astimezone(timezone.utc)
    return dt_utc.isoformat().replace("+00:00", "Z")


async def get_user_by_email(email: str, *, host_name: str | None = None) -> dict[str, Any] | None:
    if not email:
        return None
    encoded_email = quote(email.strip())
    if host_name:
        response = await _request_for_host(host_name, "GET", f"/api/users/by-email/{encoded_email}", expected_status=(200, 404))
    else:
        response = await _request("GET", f"/api/users/by-email/{encoded_email}", expected_status=(200, 404))
    if response.status_code == 404:
        return None
    payload = response.json()

    data: Any
    if isinstance(payload, dict):
        inner = payload.get("response")
        data = inner if inner is not None else payload
    else:
        data = payload

    if isinstance(data, list):

        for item in data:
            if isinstance(item, dict):
                return item
        return None
    return data if isinstance(data, dict) else None


async def get_user_by_uuid(user_uuid: str, *, host_name: str | None = None) -> dict[str, Any] | None:
    if not user_uuid:
        return None
    encoded_uuid = quote(user_uuid.strip())
    if host_name:
        response = await _request_for_host(host_name, "GET", f"/api/users/{encoded_uuid}", expected_status=(200, 404))
    else:
        response = await _request("GET", f"/api/users/{encoded_uuid}", expected_status=(200, 404))
    if response.status_code == 404:
        return None
    payload = response.json()
    return payload.get("response") if isinstance(payload, dict) else None


async def ensure_user(
    *,
    host_name: str,
    email: str,
    squad_uuid: str,
    expire_at: datetime,
    traffic_limit_bytes: int | None = None,
    traffic_limit_strategy: str | None = None,
    description: str | None = None,
    tag: str | None = None,
    username: str | None = None,
) -> dict[str, Any]:
    if not email:
        raise RemnawaveAPIError("email is required for ensure_user")
    if not squad_uuid:
        raise RemnawaveAPIError("squad_uuid is required for ensure_user")


    email = _normalize_email_for_remnawave(email)
    current = await get_user_by_email(email, host_name=host_name)
    expire_iso = _to_iso(expire_at)
    traffic_limit_strategy = traffic_limit_strategy or "NO_RESET"

    payload: dict[str, Any]
    method: str
    path: str

    if current:
        current_expire = current.get("expireAt")
        if current_expire:
            try:
                current_dt = datetime.fromisoformat(current_expire.replace("Z", "+00:00"))
                if current_dt > expire_at:
                    expire_iso = _to_iso(current_dt)
            except ValueError:
                pass

        logger.info(
            "Remnawave: найден пользователь %s (%s) на '%s' — обновляю срок до %s",
            email,
            current.get("uuid"),
            host_name,
            expire_iso,
        )

        payload = {
            "uuid": current.get("uuid"),
            "status": "ACTIVE",
            "expireAt": expire_iso,
            "activeInternalSquads": [squad_uuid],
            "email": email,
        }

        if traffic_limit_bytes is not None:
            payload["trafficLimitBytes"] = traffic_limit_bytes
        if traffic_limit_strategy is not None:
            payload["trafficLimitStrategy"] = traffic_limit_strategy
        if description:
            payload["description"] = description
        if tag:
            payload["tag"] = tag
        method = "PATCH"
        path = "/api/users"
    else:
        logger.info(
            "Remnawave: пользователь %s не найден на '%s' — создаю нового (сквад %s, срок до %s)",
            email,
            host_name,
            squad_uuid,
            expire_iso,
        )
        generated_username = _normalize_username_for_remnawave(username or email.split("@")[0])
        payload = {
            "username": generated_username,
            "status": "ACTIVE",
            "expireAt": expire_iso,
            "activeInternalSquads": [squad_uuid],
            "email": email,
        }

        if traffic_limit_bytes is not None:
            payload["trafficLimitBytes"] = traffic_limit_bytes
        if traffic_limit_strategy is not None:
            payload["trafficLimitStrategy"] = traffic_limit_strategy
        if description:
            payload["description"] = description
        if tag:
            payload["tag"] = tag
        method = "POST"
        path = "/api/users"

    response = await _request_for_host(host_name, method, path, json_payload=payload, expected_status=(200, 201))
    data = response.json() or {}
    result = data.get("response") if isinstance(data, dict) else None
    if not result:
        raise RemnawaveAPIError("Remnawave API returned unexpected payload")

    action = "создан" if method == "POST" else "обновлён"
    logger.info(
        "Remnawave: пользователь %s (%s) на '%s' успешно %s. Истекает: %s",
        email,
        result.get("uuid"),
        host_name,
        action,
        result.get("expireAt"),
    )
    return result




async def list_users(host_name: str, squad_uuid: str | None = None, size: int | None = 500) -> list[dict[str, Any]]:
    params: dict[str, Any] = {}
    if size is not None:
        params["size"] = size
    if squad_uuid:
        params["squadUuid"] = squad_uuid
    response = await _request_for_host(host_name, "GET", "/api/users", params=params, expected_status=(200,))
    payload = response.json() or {}
    raw_users = []
    if isinstance(payload, dict):
        body = payload.get("response") if isinstance(payload.get("response"), dict) else payload
        raw_users = body.get("users") or body.get("data") or []
    if not isinstance(raw_users, list):
        raw_users = []
    if squad_uuid:
        filtered: list[dict[str, Any]] = []
        for user in raw_users:
            squads = user.get("activeInternalSquads") or user.get("internalSquads") or []
            if isinstance(squads, list):
                for item in squads:
                    if isinstance(item, dict):
                        if item.get("uuid") == squad_uuid:
                            filtered.append(user)
                            break
                    elif isinstance(item, str) and item == squad_uuid:
                        filtered.append(user)
                        break
            elif isinstance(squads, str) and squads == squad_uuid:
                filtered.append(user)
        return filtered
    return raw_users
async def delete_user(user_uuid: str) -> bool:
    """Глобальный вариант (устарел): удаление без привязки к хосту.
    Сохраняется для обратной совместимости, но предпочтительно использовать host-specific путь ниже.
    """
    if not user_uuid:
        return False
    encoded_uuid = quote(user_uuid.strip())
    response = await _request("DELETE", f"/api/users/{encoded_uuid}", expected_status=(200, 204, 404))
    if response.status_code == 404:
        logger.info("Remnawave: пользователь %s не найден при удалении (возможно, уже удалён)", user_uuid)
    elif response.status_code in (200, 204):
        logger.info("Remnawave: пользователь %s успешно удалён (HTTP %s)", user_uuid, response.status_code)
    return True


async def delete_user_on_host(host_name: str, user_uuid: str) -> bool:
    """Удаление пользователя на конкретном хосте, используя конфиг хоста."""
    if not user_uuid:
        return False
    encoded_uuid = quote(user_uuid.strip())
    response = await _request_for_host(host_name, "DELETE", f"/api/users/{encoded_uuid}", expected_status=(200, 204, 404))
    if response.status_code == 404:
        logger.info("Remnawave[%s]: пользователь %s не найден при удалении (возможно, уже удалён)", host_name, user_uuid)
    elif response.status_code in (200, 204):
        logger.info("Remnawave[%s]: пользователь %s успешно удалён (HTTP %s)", host_name, user_uuid, response.status_code)
    return True


async def reset_user_traffic(user_uuid: str) -> bool:
    if not user_uuid:
        return False
    encoded_uuid = quote(user_uuid.strip())
    await _request("POST", f"/api/users/{encoded_uuid}/actions/reset-traffic", expected_status=(200, 204))
    return True


async def set_user_status(user_uuid: str, active: bool) -> bool:
    if not user_uuid:
        return False
    encoded_uuid = quote(user_uuid.strip())
    action = "enable" if active else "disable"
    await _request("POST", f"/api/users/{encoded_uuid}/actions/{action}", expected_status=(200, 204))
    return True


def extract_subscription_url(user_payload: dict[str, Any] | None) -> str | None:
    if not user_payload:
        return None
    return user_payload.get("subscriptionUrl")




async def create_or_update_key_on_host(
    host_name: str,
    email: str,
    days_to_add: int | None = None,
    expiry_timestamp_ms: int | None = None,
    *,
    description: str | None = None,
    tag: str | None = None,
) -> dict | None:
    """Legacy совместимость: создаёт/обновляет пользователя Remnawave и возвращает данные по ключу."""
    try:
        squad = rw_repo.get_squad(host_name)
        if not squad:
            logger.error("Remnawave: не найден сквад/хост '%s'", host_name)
            return None
        squad_uuid = (squad.get('squad_uuid') or '').strip()
        if not squad_uuid:
            logger.error("Remnawave: сквад '%s' не имеет squad_uuid", host_name)
            return None

        if expiry_timestamp_ms is not None:
            target_dt = datetime.fromtimestamp(expiry_timestamp_ms / 1000, tz=timezone.utc)
        else:
            days = days_to_add if days_to_add is not None else int(rw_repo.get_setting('default_extension_days') or 30)
            if days <= 0:
                days = 1
            target_dt = datetime.now(timezone.utc) + timedelta(days=days)

        traffic_limit_bytes = squad.get('default_traffic_limit_bytes')
        traffic_limit_strategy = squad.get('default_traffic_strategy') or 'NO_RESET'

        user_payload = await ensure_user(
            host_name=host_name,
            email=email,
            squad_uuid=squad_uuid,
            expire_at=target_dt,
            traffic_limit_bytes=traffic_limit_bytes,
            traffic_limit_strategy=traffic_limit_strategy,
            description=description,
            tag=tag,
            username=email.split('@')[0] if email else None,
        )

        subscription_url = extract_subscription_url(user_payload) or ''
        expire_at_str = user_payload.get('expireAt')
        try:
            expire_dt = datetime.fromisoformat(expire_at_str.replace('Z', '+00:00')) if expire_at_str else target_dt
        except Exception:
            expire_dt = target_dt
        expiry_ts_ms = int(expire_dt.replace(tzinfo=timezone.utc).timestamp() * 1000)

        return {
            'client_uuid': user_payload.get('uuid'),
            'short_uuid': user_payload.get('shortUuid'),
            'email': email,
            'host_name': squad.get('host_name') or host_name,
            'squad_uuid': squad_uuid,
            'subscription_url': subscription_url,
            'traffic_limit_bytes': user_payload.get('trafficLimitBytes'),
            'traffic_limit_strategy': user_payload.get('trafficLimitStrategy'),
            'expiry_timestamp_ms': expiry_ts_ms,
            'connection_string': subscription_url,
        }
    except RemnawaveAPIError as exc:
        logger.error("Remnawave: ошибка create_or_update_key_on_host %s/%s: %s", host_name, email, exc)
    except Exception:
        logger.exception("Remnawave: непредвиденная ошибка create_or_update_key_on_host для %s/%s", host_name, email)
    return None


async def get_key_details_from_host(key_data: dict) -> dict | None:
    email = key_data.get('key_email') or key_data.get('email')
    user_uuid = key_data.get('remnawave_user_uuid') or key_data.get('xui_client_uuid')
    try:
        user_payload = None
        host_name = key_data.get('host_name')
        if not host_name:

            sq = key_data.get('squad_uuid') or key_data.get('squadUuid')
            if sq:
                squad = rw_repo.get_squad(sq)
                host_name = squad.get('host_name') if squad else None
        if email:
            user_payload = await get_user_by_email(email, host_name=host_name)
        if not user_payload and user_uuid:
            user_payload = await get_user_by_uuid(user_uuid, host_name=host_name)
        if not user_payload:
            logger.warning("Remnawave: не найден пользователь для ключа %s", key_data.get('key_id'))
            return None
        subscription_url = extract_subscription_url(user_payload)
        return {
            'connection_string': subscription_url or '',
            'subscription_url': subscription_url,
            'user': user_payload,
        }
    except RemnawaveAPIError as exc:
        logger.error("Remnawave: ошибка получения деталей ключа %s: %s", key_data.get('key_id'), exc)
    except Exception:
        logger.exception("Remnawave: непредвиденная ошибка получения деталей ключа %s", key_data.get('key_id'))
    return None


async def delete_client_on_host(host_name: str, client_email: str) -> bool:
    try:

        user_payload = await get_user_by_email(client_email, host_name=host_name)
        if not user_payload:
            logger.info("Remnawave: пользователь %s уже отсутствует", client_email)
            return True
        if isinstance(user_payload, list):

            user_payload = next((u for u in user_payload if isinstance(u, dict)), None)
        user_uuid = user_payload.get('uuid') if isinstance(user_payload, dict) else None
        if not user_uuid:
            logger.warning("Remnawave: нет uuid для пользователя %s", client_email)
            return False
        logger.info("Remnawave: удаляю пользователя %s (%s) на '%s'...", client_email, user_uuid, host_name)
        await delete_user_on_host(host_name, user_uuid)
        logger.info("Remnawave: пользователь %s (%s) успешно удалён на '%s'", client_email, user_uuid, host_name)
        return True
    except RemnawaveAPIError as exc:
        logger.error("Remnawave: ошибка удаления пользователя %s: %s", client_email, exc)
    except Exception:
        logger.exception("Remnawave: непредвиденная ошибка удаления пользователя %s", client_email)
    return False
