import logging
import uuid
import base64
import hashlib
import json
import aiohttp
from decimal import Decimal
from shop_bot.data_manager.remnawave_repository import get_setting, create_payload_pending

logger = logging.getLogger(__name__)

async def create_heleket_payment_request(
    payment_id: str,
    price: float,
    metadata: dict,
) -> str | None:
    """
    Создание инвойса в Heleket и возврат payment URL.
    """

    merchant_id = (get_setting("heleket_merchant_id") or "").strip()
    api_key = (get_setting("heleket_api_key") or "").strip()
    if not (merchant_id and api_key):
        logger.error("Heleket: не заданы merchant_id/api_key в настройках.")
        return None

    amount_str = f"{Decimal(str(price)).quantize(Decimal('0.01'))}"
    body: dict = {
        "amount": amount_str,
        "currency": "RUB",
        "order_id": payment_id,
        "description": json.dumps(metadata, ensure_ascii=False, separators=(",", ":")),
    }

    try:
        domain = (get_setting("domain") or "").strip()
    except Exception:
        domain = ""
    if domain:
        if not domain.startswith("http"):
            domain = f"https://{domain}"
        cb = f"{domain.rstrip('/')}/heleket-webhook"
        body["url_callback"] = cb

    body_json = json.dumps(body, ensure_ascii=False, separators=(",", ":"))
    base64_payload = base64.b64encode(body_json.encode()).decode()
    sign = hashlib.md5((base64_payload + api_key).encode()).hexdigest()

    headers = {
        "merchant": merchant_id,
        "sign": sign,
        "Content-Type": "application/json",
    }

    url = "https://api.heleket.com/v1/payment"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, data=body_json.encode('utf-8'), timeout=20) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    logger.error(f"Heleket: HTTP {resp.status}: {text}")
                    return None
                data = await resp.json(content_type=None)

                if isinstance(data, dict) and data.get("state") == 0:
                    try:
                        result = data.get("result") or {}
                        pay_url = result.get("url")
                        if pay_url:
                            return pay_url
                    except Exception:
                        pass
                logger.error(f"Heleket: неожиданный ответ API: {data}")
                return None
    except Exception as e:
        logger.error(f"Heleket: ошибка при создании инвойса: {e}", exc_info=True)
        return None
