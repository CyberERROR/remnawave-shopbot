import hashlib
import hmac
import json
import logging
import uuid
from decimal import Decimal

import aiohttp

from shop_bot.data_manager.remnawave_repository import get_setting

logger = logging.getLogger(__name__)

ROLLYPAY_BASE_URL = "https://rollypay.io"


def _get_rollypay_api_key() -> str:
    return (get_setting("rollypay_api_key") or "").strip()


def _get_rollypay_signing_secret() -> str:
    return (get_setting("rollypay_signing_secret") or "").strip()


def is_rollypay_test_mode() -> bool:
    return (get_setting("rollypay_test_mode") or "false").strip().lower() == "true"


def _get_rollypay_redirect_urls() -> dict:
    try:
        username = (get_setting("telegram_bot_username") or "").strip().lstrip("@")
    except Exception:
        username = ""
    if not username:
        return {}
    link = f"https://t.me/{username}"
    return {"success_redirect_url": link, "fail_redirect_url": link}


async def _rollypay_request(method: str, path: str, payload: dict | None = None) -> dict | None:
    api_key = _get_rollypay_api_key()
    if not api_key:
        logger.error("RollyPay: не задан API-ключ в настройках.")
        return None
    headers = {
        "X-API-Key": api_key,
        "X-Nonce": str(uuid.uuid4()),
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    url = f"{ROLLYPAY_BASE_URL}/api/v1{path}"
    timeout = aiohttp.ClientTimeout(total=20)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.request(method, url, headers=headers, json=payload) as response:
                text = await response.text()
                if response.status >= 400:
                    logger.error(f"RollyPay: HTTP {response.status} на {method} {path}: {text[:500]}")
                    return None
                try:
                    data = json.loads(text or "{}")
                except json.JSONDecodeError:
                    logger.error(f"RollyPay: некорректный JSON в ответе на {method} {path}: {text[:500]}")
                    return None
                if not isinstance(data, dict):
                    logger.error(f"RollyPay: неожиданный формат ответа на {method} {path}: {data}")
                    return None
                return data
    except Exception as e:
        logger.error(f"RollyPay: ошибка запроса {method} {path}: {e}", exc_info=True)
        return None


async def create_rollypay_payment_request(
    payment_id: str,
    price: float,
    description: str | None = None,
    customer_id: int | str | None = None,
) -> str | None:
    if not _get_rollypay_api_key():
        logger.error("RollyPay: платежи недоступны — отсутствует API-ключ.")
        return None
    try:
        amount = f"{Decimal(str(price)).quantize(Decimal('0.01'))}"
    except Exception:
        logger.error(f"RollyPay: некорректная сумма платежа {price}.")
        return None
    body: dict = {
        "amount": amount,
        "payment_currency": "RUB",
        "payment_method": "sbp",
        "order_id": str(payment_id),
        "description": (description or "Оплата заказа").strip(),
    }
    if customer_id is not None:
        body["customer_id"] = str(customer_id)
    body.update(_get_rollypay_redirect_urls())
    if is_rollypay_test_mode():
        body["test"] = True
    data = await _rollypay_request("POST", "/payments", body)
    if not data:
        return None
    pay_url = (data.get("pay_url") or "").strip()
    if not pay_url:
        logger.error(f"RollyPay: в ответе нет pay_url: {data}")
        return None
    logger.info(f"RollyPay: платёж {payment_id} создан, статус {data.get('status')}.")
    return pay_url


async def check_rollypay_payment(payment_id: str) -> dict | None:
    if not payment_id:
        return None
    return await _rollypay_request("GET", f"/payments/{str(payment_id).strip()}")


async def get_rollypay_balance() -> dict | None:
    return await _rollypay_request("GET", "/balance")


def verify_rollypay_webhook_signature(raw_body: bytes, timestamp: str, signature: str) -> bool:
    signing_secret = _get_rollypay_signing_secret()
    if not signing_secret:
        logger.error("RollyPay: не задан signing_secret, проверка подписи невозможна.")
        return False
    timestamp = (timestamp or "").strip()
    signature = (signature or "").strip()
    if not timestamp or not signature:
        return False
    expected = hmac.new(
        signing_secret.encode(),
        timestamp.encode() + b"." + (raw_body or b""),
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(expected, signature)
