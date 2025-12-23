"""
Fride Payment System API Module
Handles payment creation and status checking for Fride.io
"""
import aiohttp
import logging
import json
import hmac
import hashlib
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


class FrideAPI:
    """API client for Fride payment system"""
    
    BASE_URL = "https://api.fride.io"
    
    def __init__(self, merchant_id: str, api_key: str):
        """
        Initialize Fride API client
        
        Args:
            merchant_id: Fride Merchant ID
            api_key: Fride API Key
        """
        self.merchant_id = merchant_id
        self.api_key = api_key
    
    def _get_headers(self) -> dict:
        """Get authentication headers for API requests"""
        return {
            "X-Api-Key": self.api_key,
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
    
    async def create_payment(
        self,
        amount: float,
        description: str,
        payment_id: str,
        user_id: int,
        tariff_name: str = "VPN Subscription"
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Create a payment (invoice) in Fride
        
        Args:
            amount: Payment amount in RUB
            description: Payment description
            payment_id: Internal payment ID (order_id)
            user_id: User ID for metadata
            tariff_name: Tariff name for metadata
        
        Returns:
            Tuple of (invoice_id, payment_url) or (None, None) on error
        """
        payload = {
            "merchant_id": self.merchant_id,
            "order_id": payment_id,
            "amount": round(amount, 2),
            "currency": "RUB",
            "comment": description,
            "custom_fields": {
                "user_id": str(user_id),
                "tariff_name": tariff_name
            }
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.BASE_URL}/invoices/create",
                    json=payload,
                    headers=self._get_headers(),
                    timeout=aiohttp.ClientTimeout(total=20)
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        logger.info(f"Fride create_payment response: {data}")
                        
                        invoice_id = data.get("id")
                        payment_url = data.get("url")
                        
                        if invoice_id and payment_url:
                            logger.info(f"Fride payment created: invoice_id={invoice_id}, url={payment_url}")
                            return invoice_id, payment_url
                        else:
                            logger.error(f"Fride payment created but missing id or url: {data}")
                            return None, None
                    else:
                        error_text = await response.text()
                        logger.error(f"Fride payment creation failed: {response.status} - {error_text}")
                        return None, None
                        
        except aiohttp.ClientError as e:
            logger.error(f"Fride HTTP error during payment creation: {e}")
            return None, None
        except Exception as e:
            logger.error(f"Fride unexpected error during payment creation: {e}", exc_info=True)
            return None, None
    
    async def check_payment(self, payment_id: str) -> bool:
        """
        Check payment status in Fride
        
        Args:
            payment_id: Internal payment ID (order_id)
        
        Returns:
            True if payment is confirmed, False otherwise
        """
        params = {
            "order_id": payment_id,
            "merchant_id": self.merchant_id
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{self.BASE_URL}/invoice/getInfo",
                    headers=self._get_headers(),
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=20)
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        logger.info(f"Fride check_payment response for {payment_id}: {data}")
                        
                        status = data.get("status")
                        # 'success' and 'hold' are considered successful
                        if status in ["success", "hold", "paid"]:
                            logger.info(f"Fride payment {payment_id} is CONFIRMED (status: {status})")
                            return True
                        else:
                            logger.info(f"Fride payment {payment_id} status is {status}")
                            return False
                    else:
                        error_text = await response.text()
                        logger.error(f"Fride check_payment failed for {payment_id}: {response.status} - {error_text}")
                        return False
                        
        except aiohttp.ClientError as e:
            logger.error(f"Fride HTTP error during status check for {payment_id}: {e}")
            return False
        except Exception as e:
            logger.error(f"Fride unexpected error during status check for {payment_id}: {e}", exc_info=True)
            return False

    @staticmethod
    def check_signature(hook_body: dict, header_signature: str, secret_key: str) -> bool:
        """
        Check webhook signature
        
        Args:
            hook_body: Parsed JSON body of the webhook
            header_signature: X-Signature header value
            secret_key: Webhook secret key
            
        Returns:
            True if signature is valid
        """
        try:
            # Sort keys and dump to string without spaces
            sorted_hook_json = json.dumps(hook_body, sort_keys=True, separators=(',', ':'))
            
            calc_sign = hmac.new(
                secret_key.encode('utf-8'),
                msg=sorted_hook_json.encode('utf-8'),
                digestmod=hashlib.sha256
            ).hexdigest()
            
            return hmac.compare_digest(header_signature, calc_sign)
        except Exception as e:
            logger.error(f"Error checking Fride signature: {e}")
            return False
