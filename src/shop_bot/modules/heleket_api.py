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
    user_id: int,
    price: float,
    months: int,
    host_name: str | None,
    state_data: dict,
) -> str | None:
    """
    Создание инвойса в Heleket и возврат payment URL.

    Требования API:
      - POST https://api.heleket.com/v1/payment
      - Заголовки: merchant, sign (md5(base64(json_body)+API_KEY))
      - Тело (минимум): { amount, currency, order_id }
      - Дополнительно: url_callback (наш вебхук), description (положим JSON метаданных)
    """

    merchant_id = (get_setting("heleket_merchant_id") or "").strip()
    api_key = (get_setting("heleket_api_key") or "").strip()
    if not (merchant_id and api_key):
        logger.error("Heleket: не заданы merchant_id/api_key в настройках.")
        return None

    payment_id = str(uuid.uuid4())

    metadata = {
        "user_id": int(user_id),
        "months": int(months or 0),
        "price": float(Decimal(str(price)).quantize(Decimal("0.01"))),
        "action": state_data.get("action"),
        "key_id": state_data.get("key_id"),
        "host_name": host_name or state_data.get("host_name"),
        "plan_id": state_data.get("plan_id"),
        "customer_email": state_data.get("customer_email"),
        "payment_method": "Heleket",
        "payment_id": payment_id,
        "promo_code": state_data.get("promo_code"),
        "promo_discount": state_data.get("promo_discount"),
    }

    try:
        create_payload_pending(payment_id, user_id, float(metadata["price"]), metadata)
    except Exception as e:
        logger.warning(f"Heleket: не удалось создать pending: {e}")

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
