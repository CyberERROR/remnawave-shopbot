import json
import re
import time
from urllib.parse import urlparse, parse_qs, quote

from shop_bot.data_manager.database import get_setting, update_setting

YOOKASSA_PROXY_SETTING = "yookassa_proxies"
YOOKASSA_PING_URL = "https://yookassa.ru"
YOOKASSA_PROXY_SCHEMES = ("http", "https", "socks4", "socks5")

_PROXY_PATTERNS = (
    r"^(?P<login>[^:@/\s]+):(?P<password>[^@/\s]+)@(?P<host>[A-Za-z0-9.\-]+):(?P<port>\d+)$",
    r"^(?P<host>[A-Za-z0-9.\-]+):(?P<port>\d+):(?P<login>[^:@/\s]+):(?P<password>.+)$",
    r"^(?P<host>[A-Za-z0-9.\-]+):(?P<port>\d+)@(?P<login>[^:@/\s]+):(?P<password>[^@\s]+)$",
    r"^(?P<login>[^:@/\s]+):(?P<password>[^:@/\s]+):(?P<host>[A-Za-z0-9.\-]+):(?P<port>\d+)$",
    r"^(?P<host>[A-Za-z0-9.\-]+):(?P<port>\d+)$",
)

_SOCKS_DEFAULT_PORTS = (1080, 1081, 1085, 9050)


def parse_proxy(raw: str, forced_scheme: str | None = None) -> dict | None:
    value = (raw or "").strip()
    if not value:
        return None
    forced = forced_scheme if forced_scheme in YOOKASSA_PROXY_SCHEMES else None
    if value.lower().startswith("tg://"):
        params = parse_qs(urlparse(value).query)
        host = (params.get("server") or params.get("host") or [""])[0].strip()
        port_raw = (params.get("port") or [""])[0].strip()
        if not host or not port_raw.isdigit():
            return None
        login = (params.get("user") or [""])[0].strip() or None
        password = (params.get("pass") or [""])[0].strip() or None
        return {"scheme": "socks5", "host": host, "port": int(port_raw), "login": login, "password": password}
    if "://" in value:
        parsed = urlparse(value)
        scheme = (parsed.scheme or "").lower()
        try:
            port = parsed.port
        except ValueError:
            return None
        if scheme not in YOOKASSA_PROXY_SCHEMES or not parsed.hostname or not port:
            return None
        return {"scheme": scheme, "host": parsed.hostname, "port": int(port), "login": parsed.username or None, "password": parsed.password or None}
    for pattern in _PROXY_PATTERNS:
        match = re.match(pattern, value)
        if match:
            groups = match.groupdict()
            port = int(groups["port"])
            scheme = forced or ("socks5" if port in _SOCKS_DEFAULT_PORTS else "http")
            return {"scheme": scheme, "host": groups["host"], "port": port, "login": groups.get("login") or None, "password": groups.get("password") or None}
    return None


def build_proxy_url(proxy: dict) -> str:
    auth = ""
    login = proxy.get("login") or ""
    password = proxy.get("password") or ""
    if login or password:
        auth = f"{quote(login, safe='')}:{quote(password, safe='')}@"
    return f"{proxy['scheme']}://{auth}{proxy['host']}:{proxy['port']}"


def describe_proxy_for_log(proxy_url: str) -> str:
    parsed = urlparse(proxy_url or "")
    if not parsed.hostname:
        return "proxy"
    return f"{parsed.scheme or 'http'}://{parsed.hostname}:{parsed.port or '*'}"


def get_yookassa_proxies() -> list[dict]:
    raw = get_setting(YOOKASSA_PROXY_SETTING)
    try:
        items = json.loads(raw) if raw else []
    except (ValueError, TypeError):
        items = []
    if not isinstance(items, list):
        return []
    proxies = []
    for item in items:
        if isinstance(item, dict) and isinstance(item.get("raw"), str) and item["raw"].strip():
            entry = {"raw": item["raw"].strip(), "enabled": bool(item.get("enabled", True))}
            if item.get("scheme") in YOOKASSA_PROXY_SCHEMES:
                entry["scheme"] = item["scheme"]
            proxies.append(entry)
        elif isinstance(item, str) and item.strip():
            proxies.append({"raw": item.strip(), "enabled": True})
    return proxies


def save_yookassa_proxies(proxies: list[dict]) -> None:
    update_setting(YOOKASSA_PROXY_SETTING, json.dumps(proxies, ensure_ascii=False))


def get_active_yookassa_proxy_url() -> str | None:
    for item in get_yookassa_proxies():
        if not item.get("enabled"):
            continue
        parsed = parse_proxy(item.get("raw") or "", item.get("scheme"))
        if parsed:
            return build_proxy_url(parsed)
    return None


def apply_detected_proxy_scheme(raw: str, scheme: str) -> None:
    raw = (raw or "").strip()
    if not raw or scheme not in YOOKASSA_PROXY_SCHEMES:
        return
    proxies = get_yookassa_proxies()
    changed = False
    for item in proxies:
        if item["raw"] == raw and item.get("scheme") != scheme:
            item["scheme"] = scheme
            changed = True
    if changed:
        save_yookassa_proxies(proxies)


def create_yookassa_proxy_connector(proxy_url: str | None):
    if not proxy_url:
        return None, None
    scheme = (urlparse(proxy_url).scheme or "http").lower()
    if scheme in ("socks4", "socks5"):
        try:
            from aiohttp_socks import ProxyConnector
        except ImportError as exc:
            raise RuntimeError("SOCKS-прокси недоступен: установите библиотеку aiohttp-socks") from exc
        return ProxyConnector.from_url(proxy_url), None
    return None, proxy_url


def _requests_socks_available() -> bool:
    try:
        import socks
        return True
    except ImportError:
        return False


def _describe_ping_error(exc: Exception) -> str:
    text = str(exc) or ""
    lowered = text.lower()
    if "badstatusline" in lowered or "connection aborted" in lowered:
        return "Прокси вернул не-HTTP ответ"
    if "missing dependencies for socks" in lowered:
        return "SOCKS-пинг недоступен: установите pysocks"
    if "timed out" in lowered or "timeout" in lowered:
        return "Таймаут соединения"
    if "connection refused" in lowered:
        return "Соединение отклонено"
    if "proxyerror" in type(exc).__name__.lower() or "proxy" in lowered:
        return "Прокси отклонил соединение"
    return (text[:120] or "Ошибка соединения").strip()


def _ping_yookassa_via(proxy_url: str, timeout_seconds: int) -> dict:
    import requests
    started = time.monotonic()
    try:
        response = requests.get(YOOKASSA_PING_URL, proxies={"http": proxy_url, "https": proxy_url}, timeout=(timeout_seconds, timeout_seconds), stream=True, allow_redirects=True)
        status_code = response.status_code
        response.close()
        elapsed_ms = int((time.monotonic() - started) * 1000)
        if status_code < 400:
            return {"ok": True, "status": status_code, "ms": elapsed_ms, "message": f"OK · {elapsed_ms} мс"}
        return {"ok": False, "status": status_code, "ms": elapsed_ms, "message": f"HTTP {status_code} (прокси отвечает)"}
    except Exception as exc:
        return {"ok": False, "message": _describe_ping_error(exc)}


def check_yookassa_proxy(raw: str, timeout_seconds: int = 10, forced_scheme: str | None = None) -> dict:
    parsed = parse_proxy(raw, forced_scheme)
    if not parsed:
        return {"ok": False, "message": "Неверный формат прокси"}

    primary = _ping_yookassa_via(build_proxy_url(parsed), timeout_seconds)
    if "status" in primary:
        primary["scheme"] = parsed["scheme"]
        return primary

    primary_message = primary.get("message") or "Ошибка соединения"
    needs_fallback = (
        parsed["scheme"].startswith("socks")
        or primary_message in ("Прокси вернул не-HTTP ответ", "SOCKS-пинг недоступен: установите pysocks")
    )
    if not needs_fallback:
        return {"ok": False, "message": primary_message}

    socks_supported = _requests_socks_available()
    fallbacks = []
    for scheme in ("socks5", "socks4", "http"):
        if scheme == parsed["scheme"]:
            continue
        if scheme.startswith("socks") and not socks_supported:
            continue
        fallbacks.append(scheme)
    for scheme in fallbacks:
        variant = {**parsed, "scheme": scheme}
        result = _ping_yookassa_via(build_proxy_url(variant), timeout_seconds)
        if "status" in result:
            result["scheme"] = scheme
            result["message"] += f" · сработал как {scheme}"
            return result
    hint = "" if socks_supported else " · для автоподбора схемы установите pysocks"
    return {"ok": False, "message": primary_message + hint}
