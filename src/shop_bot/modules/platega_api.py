"""
Platega Payment System API Module
Handles payment creation and status checking for Platega.io
"""
import aiohttp
import uuid
import logging
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


class PlategaAPI:
    """API client for Platega payment system"""
    
    BASE_URL = "https://app.platega.io"
    
    def __init__(self, merchant_id: str, api_key: str):
        """
        Initialize Platega API client
        
        Args:
            merchant_id: Platega Merchant ID
            api_key: Platega API Secret Key
        """
        self.merchant_id = merchant_id
        self.api_key = api_key
    
    def _get_headers(self) -> dict:
        """Get authentication headers for API requests"""
        return {
            "X-MerchantId": self.merchant_id,
            "X-Secret": self.api_key,
            "Content-Type": "application/json"
        }
    
    async def create_payment(
        self,
        amount: float,
        description: str,
        payment_id: str,
        return_url: str,
        failed_url: str,
        payment_method: int = 2
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Create a payment in Platega
        
        Args:
            amount: Payment amount in RUB
            description: Payment description
            payment_id: Internal payment ID (will be used as payload)
            return_url: URL to redirect on successful payment
            failed_url: URL to redirect on failed payment
            payment_method: Payment method (2=СБП/QR, 10=CardRu, 12=International)
        
        Returns:
            Tuple of (transaction_id, payment_url) or (None, None) on error
        """
        payload = {
            "paymentMethod": payment_method,
            "id": str(uuid.uuid4()),  # Generate UUID for Platega transaction
            "paymentDetails": {
                "amount": int(amount),  # Platega expects integer (rubles)
                "currency": "RUB"
            },
            "description": description,
            "return": return_url,
            "failedUrl": failed_url,
            "payload": payment_id  # Our internal payment ID
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.BASE_URL}/transaction/process",
                    json=payload,
                    headers=self._get_headers(),
                    timeout=aiohttp.ClientTimeout(total=20)
                ) as response:
                    response.raise_for_status()
                    data = await response.json()
                    
                    logger.info(f"Platega create_payment response: {data}")
                    
                    if data.get("status") == "PENDING" and "redirect" in data:
                        transaction_id = data.get("transactionId")
                        payment_url = data.get("redirect")
                        logger.info(f"Platega payment created: transaction_id={transaction_id}, url={payment_url}")
                        return transaction_id, payment_url
                    else:
                        logger.error(f"Platega payment creation failed: {data}")
                        return None, None
                        
        except aiohttp.ClientError as e:
            logger.error(f"Platega HTTP error during payment creation: {e}")
            return None, None
        except Exception as e:
            logger.error(f"Platega unexpected error during payment creation: {e}", exc_info=True)
            return None, None
    
    async def check_payment(self, transaction_id: str) -> bool:
        """
        Check payment status in Platega
        
        Args:
            transaction_id: Platega transaction ID
        
        Returns:
            True if payment is confirmed, False otherwise
        """
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{self.BASE_URL}/transaction/{transaction_id}",
                    headers=self._get_headers(),
                    timeout=aiohttp.ClientTimeout(total=20)
                ) as response:
                    response.raise_for_status()
                    data = await response.json()
                    
                    logger.info(f"Platega check_payment response for {transaction_id}: {data}")
                    
                    status = data.get("status")
                    if status == "CONFIRMED":
                        logger.info(f"Platega payment {transaction_id} is CONFIRMED")
                        return True
                    elif status in ["PENDING", "PROCESSING"]:
                        logger.info(f"Platega payment {transaction_id} is {status}")
                        return False
                    else:
                        logger.warning(f"Platega payment {transaction_id} has status: {status}")
                        return False
                        
        except aiohttp.ClientError as e:
            logger.error(f"Platega HTTP error during status check for {transaction_id}: {e}")
            return False
        except Exception as e:
            logger.error(f"Platega unexpected error during status check for {transaction_id}: {e}", exc_info=True)
            return False
