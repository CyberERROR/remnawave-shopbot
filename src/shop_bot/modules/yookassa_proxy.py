"""
YooKassa Proxy Management Module

Provides intelligent proxy management for YooKassa API with automatic selection,
health checking, and fallback mechanisms.
"""

import asyncio
import json
import re
import time
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Callable
from urllib.parse import urlparse
import logging

import aiohttp
from yookassa import Configuration

logger = logging.getLogger(__name__)

# Constants
CHECK_INTERVAL_SECONDS = 600  # 10 minutes
CHECK_TIMEOUT_SECONDS = 8
YOOKASSA_API_URL = "https://api.yookassa.ru/v3"


@dataclass
class ProxyStatus:
    """Status information for a single proxy"""
    proxy: str  # Original proxy string (masked in logs)
    is_working: bool = False
    latency_ms: float = 0.0
    last_checked: float = 0.0
    error_message: str = ""
    is_best: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "proxy": self.masked_proxy,
            "is_working": self.is_working,
            "latency_ms": round(self.latency_ms, 1) if self.is_working else None,
            "last_checked": self.last_checked,
            "error_message": self.error_message if not self.is_working else "",
            "is_best": self.is_best,
        }

    @property
    def masked_proxy(self) -> str:
        """Return proxy string with password masked"""
        return mask_proxy_password(self.proxy)


class YooKassaProxyManager:
    """
    Manages proxies for YooKassa API with automatic health checking
    and selection of the best performing proxy.
    """

    _instance: Optional["YooKassaProxyManager"] = None
    _lock = asyncio.Lock()

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._initialized = True

        self._proxy_statuses: Dict[str, ProxyStatus] = {}
        self._active_proxy: Optional[str] = None
        self._monitor_task: Optional[asyncio.Task] = None
        self._settings_callback: Optional[Callable[[str], Optional[str]]] = None
        self._update_settings_callback: Optional[Callable[[str, str], bool]] = None

    def initialize(
        self,
        settings_callback: Callable[[str], Optional[str]],
        update_settings_callback: Callable[[str, str], bool],
    ) -> None:
        """Initialize the manager with database callbacks"""
        self._settings_callback = settings_callback
        self._update_settings_callback = update_settings_callback

        # Load existing active proxy
        if self._settings_callback:
            active = self._settings_callback("yookassa_active_proxy")
            if active:
                self._active_proxy = active
                logger.info(f"YooKassaProxy: Loaded active proxy: {mask_proxy_password(active)}")

    def _parse_proxies(self, proxies_text: str) -> List[str]:
        """Parse proxy list from text (newlines, commas, or semicolons as separators)"""
        if not proxies_text:
            return []

        # Replace commas and semicolons with newlines for uniform parsing
        normalized = proxies_text.replace(",", "\n").replace(";", "\n")
        lines = [line.strip() for line in normalized.split("\n")]
        return [line for line in lines if line and not line.startswith("#")]

    def _get_proxies_from_settings(self) -> List[str]:
        """Get proxy list from database settings"""
        if not self._settings_callback:
            return []
        proxies_text = self._settings_callback("yookassa_proxies") or ""
        return self._parse_proxies(proxies_text)

    def _save_active_proxy(self, proxy: str) -> None:
        """Save active proxy to settings"""
        self._active_proxy = proxy
        if self._update_settings_callback:
            self._update_settings_callback("yookassa_active_proxy", proxy)

    def _save_proxy_status(self, statuses: List[ProxyStatus]) -> None:
        """Save proxy status JSON to settings"""
        if self._update_settings_callback:
            status_dict = {
                "last_check": time.time(),
                "working_count": sum(1 for s in statuses if s.is_working),
                "total_count": len(statuses),
                "proxies": [s.to_dict() for s in statuses],
            }
            self._update_settings_callback("yookassa_proxy_status", json.dumps(status_dict, ensure_ascii=False))

    def get_active_proxy(self) -> Optional[str]:
        """Get currently active (best) proxy"""
        return self._active_proxy

    def get_all_proxy_statuses(self) -> List[ProxyStatus]:
        """Get all proxy statuses from last check"""
        return list(self._proxy_statuses.values())

    def _is_proxy_enabled(self) -> bool:
        """Check if proxy mode is enabled in settings"""
        if not self._settings_callback:
            return False
        enabled = self._settings_callback("yookassa_proxy_enabled") or "false"
        return enabled.strip().lower() == "true"

    def _get_setting_value(self, key: str) -> Optional[str]:
        if self._settings_callback:
            return self._settings_callback(key)
        try:
            from shop_bot.data_manager.database import get_setting
            return get_setting(key)
        except Exception:
            return None

    async def start_monitor(self) -> None:
        """Start background monitoring task"""
        if self._monitor_task is not None and not self._monitor_task.done():
            logger.debug("YooKassaProxy: Monitor already running")
            return

        self._monitor_task = asyncio.create_task(self._monitor_loop())
        logger.info("YooKassaProxy: Monitor started")

    async def stop_monitor(self) -> None:
        """Stop background monitoring task"""
        if self._monitor_task is not None and not self._monitor_task.done():
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        self._monitor_task = None
        logger.info("YooKassaProxy: Monitor stopped")

    async def _monitor_loop(self) -> None:
        """Background loop for periodic proxy checking"""
        while True:
            try:
                # Only check proxies if proxy mode is enabled
                if self._is_proxy_enabled():
                    await self.check_proxies()
                else:
                    logger.debug("YooKassaProxy: Proxy mode disabled, skipping check")
            except Exception as e:
                logger.error(f"YooKassaProxy: Error in monitor loop: {e}")
            await asyncio.sleep(CHECK_INTERVAL_SECONDS)

    async def check_proxies(self) -> List[ProxyStatus]:
        """Check all configured proxies and return their statuses"""
        proxies = self._get_proxies_from_settings()

        if not proxies:
            logger.debug("YooKassaProxy: No proxies configured")
            self._proxy_statuses.clear()
            self._save_active_proxy("")
            return []

        logger.info(f"YooKassaProxy: Checking {len(proxies)} proxies...")

        # Check all proxies concurrently
        tasks = [self._check_single_proxy(p) for p in proxies]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        statuses: List[ProxyStatus] = []
        working_proxies: List[ProxyStatus] = []

        for proxy, result in zip(proxies, results):
            if isinstance(result, Exception):
                status = ProxyStatus(
                    proxy=proxy,
                    is_working=False,
                    error_message=str(result),
                    last_checked=time.time(),
                )
            else:
                status = result
                if status.is_working:
                    working_proxies.append(status)

            statuses.append(status)
            self._proxy_statuses[proxy] = status

        # Sort working by latency and select best
        if working_proxies:
            working_proxies.sort(key=lambda x: x.latency_ms)
            best = working_proxies[0]
            best.is_best = True
            self._save_active_proxy(best.proxy)

            # Mark others as not best
            for p in working_proxies[1:]:
                p.is_best = False

            logger.info(
                f"YooKassaProxy: Best proxy selected: {best.masked_proxy} ({best.latency_ms:.1f}ms), "
                f"working: {len(working_proxies)}/{len(proxies)}"
            )
        else:
            self._save_active_proxy("")
            logger.warning(f"YooKassaProxy: No working proxies found ({len(proxies)} checked)")

        self._save_proxy_status(statuses)
        return statuses

    async def _check_single_proxy(self, proxy: str) -> ProxyStatus:
        """Check a single proxy against YooKassa /me endpoint"""
        shop_id = self._get_setting_value("yookassa_shop_id")
        secret_key = self._get_setting_value("yookassa_secret_key")

        if not shop_id or not secret_key:
            return ProxyStatus(
                proxy=proxy,
                is_working=False,
                error_message="YooKassa credentials not configured",
                last_checked=time.time(),
            )

        start_time = time.time()
        proxy_url = self._normalize_proxy_url(proxy)

        try:
            timeout = aiohttp.ClientTimeout(total=CHECK_TIMEOUT_SECONDS, connect=5, sock_read=CHECK_TIMEOUT_SECONDS)
            auth = aiohttp.BasicAuth(str(shop_id), str(secret_key))

            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(
                    f"{YOOKASSA_API_URL}/me",
                    auth=auth,
                    proxy=proxy_url,
                    ssl=True,
                ) as response:
                    elapsed = (time.time() - start_time) * 1000

                    if response.status == 200:
                        try:
                            data = await response.json()
                            # Validate response contains account_id
                            if data.get("account_id") == str(shop_id):
                                return ProxyStatus(
                                    proxy=proxy,
                                    is_working=True,
                                    latency_ms=elapsed,
                                    last_checked=time.time(),
                                )
                            else:
                                return ProxyStatus(
                                    proxy=proxy,
                                    is_working=False,
                                    error_message=f"Invalid response: account_id mismatch",
                                    last_checked=time.time(),
                                )
                        except json.JSONDecodeError:
                            return ProxyStatus(
                                proxy=proxy,
                                is_working=False,
                                error_message="Invalid JSON response",
                                last_checked=time.time(),
                            )
                    else:
                        text = await response.text()
                        return ProxyStatus(
                            proxy=proxy,
                            is_working=False,
                            error_message=f"HTTP {response.status}: {text[:200]}",
                            last_checked=time.time(),
                        )

        except asyncio.TimeoutError:
            return ProxyStatus(
                proxy=proxy,
                is_working=False,
                error_message=f"Timeout after {CHECK_TIMEOUT_SECONDS}s",
                last_checked=time.time(),
            )
        except Exception as e:
            return ProxyStatus(
                proxy=proxy,
                is_working=False,
                error_message=str(e)[:200],
                last_checked=time.time(),
            )

    def _normalize_proxy_url(self, proxy: str) -> str:
        """Normalize proxy string to aiohttp format"""
        proxy = proxy.strip()

        # Already has protocol
        if proxy.startswith(("http://", "https://", "socks5://", "socks4://")):
            return proxy

        # Add http:// as default
        return f"http://{proxy}"

    async def create_payment_with_proxy(
        self,
        payment_create_func: Callable,
        payload: Dict[str, Any],
        idempotence_key: str,
        timeout: float = 12.0,
    ) -> Dict[str, Any]:
        """
        Create a YooKassa payment using the best available proxy.
        Falls back to direct connection if proxy fails.

        Args:
            payment_create_func: Function that creates the payment (sync YooKassa SDK call)
            payload: Payment payload
            idempotence_key: Idempotence key
            timeout: Timeout in seconds for proxy attempt

        Returns:
            Payment response data

        Raises:
            Exception: If both proxy and direct attempts fail
        """
        if not self._is_proxy_enabled():
            logger.debug("YooKassa: Proxy mode disabled, using direct connection")
            if payment_create_func is None:
                shop_id = self._get_setting_value("yookassa_shop_id")
                secret_key = self._get_setting_value("yookassa_secret_key")
                return await self._create_payment_via_proxy(
                    shop_id, secret_key, payload, idempotence_key, None, timeout
                )
            return await self._create_payment_direct(payment_create_func, payload, idempotence_key, timeout)

        active_proxy = self._active_proxy

        if not active_proxy and self._get_proxies_from_settings():
            try:
                await self.check_proxies()
                active_proxy = self._active_proxy
            except Exception as e:
                logger.warning(f"YooKassaProxy: proxy check before payment failed: {e}")

        if active_proxy:
            logger.info(f"YooKassa: trying via proxy {mask_proxy_password(active_proxy)}")

            shop_id = self._get_setting_value("yookassa_shop_id")
            secret_key = self._get_setting_value("yookassa_secret_key")

            try:
                result = await self._create_payment_via_proxy(
                    shop_id, secret_key, payload, idempotence_key, active_proxy, timeout
                )
                logger.info(f"YooKassa: Payment created successfully via proxy")
                return result
            except Exception as e:
                logger.warning(f"YooKassa via proxy failed: {e}, fallback to direct")

        logger.info("YooKassa: creating payment directly (no proxy or fallback)")
        if payment_create_func is None:
            shop_id = self._get_setting_value("yookassa_shop_id")
            secret_key = self._get_setting_value("yookassa_secret_key")
            return await self._create_payment_via_proxy(
                shop_id, secret_key, payload, idempotence_key, None, timeout * 2
            )
        return await self._create_payment_direct(payment_create_func, payload, idempotence_key, timeout * 2)

    async def _create_payment_via_proxy(
        self,
        shop_id: str,
        secret_key: str,
        payload: Dict[str, Any],
        idempotence_key: str,
        proxy: Optional[str],
        timeout: float,
    ) -> Dict[str, Any]:
        """Create payment through a specific proxy"""
        if not shop_id or not secret_key:
            raise RuntimeError("YooKassa credentials not configured")

        proxy_url = self._normalize_proxy_url(proxy) if proxy else None
        aio_timeout = aiohttp.ClientTimeout(total=timeout, connect=5, sock_read=timeout)
        auth = aiohttp.BasicAuth(str(shop_id), str(secret_key))
        headers = {"Idempotence-Key": str(idempotence_key)}

        # Prepare metadata for API (convert None to empty strings)
        api_payload = dict(payload)
        if isinstance(api_payload.get("metadata"), dict):
            api_payload["metadata"] = {str(k): "" if v is None else str(v) for k, v in api_payload["metadata"].items()}

        async with aiohttp.ClientSession(timeout=aio_timeout) as session:
            async with session.post(
                f"{YOOKASSA_API_URL}/payments",
                json=api_payload,
                headers=headers,
                auth=auth,
                proxy=proxy_url,
                ssl=True,
            ) as response:
                raw = await response.text()
                if response.status >= 400:
                    raise RuntimeError(f"YooKassa HTTP {response.status}: {raw[:500]}")
                data = json.loads(raw or "{}")
                if not data.get("confirmation", {}).get("confirmation_url"):
                    raise RuntimeError("YooKassa did not return confirmation_url")
                return data

    async def _create_payment_direct(
        self,
        payment_create_func: Callable,
        payload: Dict[str, Any],
        idempotence_key: str,
        timeout: float,
    ) -> Dict[str, Any]:
        """Create payment directly without proxy using the SDK with timeout protection"""
        import concurrent.futures

        loop = asyncio.get_running_loop()
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        future = loop.run_in_executor(executor, payment_create_func, payload, idempotence_key)
        try:
            return await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            logger.error(f"YooKassa: Direct payment creation timed out after {timeout}s")
            future.cancel()
            raise RuntimeError(f"YooKassa payment creation timed out after {timeout} seconds")
        except Exception as e:
            logger.error(f"YooKassa: Direct payment creation failed: {e}")
            raise
        finally:
            executor.shutdown(wait=False, cancel_futures=True)


# Global instance
_proxy_manager: Optional[YooKassaProxyManager] = None


def get_proxy_manager() -> YooKassaProxyManager:
    """Get the global proxy manager instance"""
    global _proxy_manager
    if _proxy_manager is None:
        _proxy_manager = YooKassaProxyManager()
    return _proxy_manager


def mask_proxy_password(proxy: str) -> str:
    """Mask password in proxy string for logging"""
    if not proxy:
        return proxy

    try:
        # Handle format: protocol://login:password@host:port
        if "://" in proxy:
            parsed = urlparse(proxy)
            if parsed.username and parsed.password:
                return proxy.replace(f":{parsed.password}@", ":***@")
            return proxy

        # Handle format: login:password@host:port
        match = re.match(r"^(?:(?P<login>[^:]+):(?P<pass>[^@]+)@)?(?P<rest>.*)$", proxy)
        if match and match.group("pass"):
            return f"{match.group('login') or ''}:***@{match.group('rest')}"

    except Exception:
        pass

    return proxy


async def check_yookassa_proxies() -> List[ProxyStatus]:
    """Public function to trigger proxy check"""
    manager = get_proxy_manager()
    return await manager.check_proxies()


async def create_yookassa_payment_with_proxy(
    payment_create_func: Callable,
    payload: Dict[str, Any],
    idempotence_key: str,
    timeout: float = 12.0,
) -> Dict[str, Any]:
    """Public function to create payment with proxy support"""
    manager = get_proxy_manager()
    return await manager.create_payment_with_proxy(payment_create_func, payload, idempotence_key, timeout)
