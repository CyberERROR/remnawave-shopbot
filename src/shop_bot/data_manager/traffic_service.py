import logging
from datetime import datetime, timezone, timedelta
from typing import Any

from shop_bot.data_manager import remnawave_repository as repo
from shop_bot.modules import remnawave_api

logger = logging.getLogger(__name__)


def calculate_key_total_traffic_gb(key_id: int) -> int:
    """
    Calculates total traffic limit for a key:
    base plan traffic_limit_gb + sum of active traffic purchases.
    """
    key = repo.get_key_by_id(key_id) if hasattr(repo, 'get_key_by_id') else None
    if not key:
        return 0

    base_gb = 0
    plan_id = key.get("plan_id")
    if plan_id:
        plan = repo.get_plan_by_id(plan_id) if hasattr(repo, 'get_plan_by_id') else None
        if plan:
            base_gb = int(plan.get("traffic_limit_gb") or 0)

    active_purchased_gb = repo.get_key_active_traffic_gb(key_id)
    return base_gb + active_purchased_gb


def get_traffic_tier_price(tier_id: int) -> dict | None:
    """Returns traffic tier data from database (single source of truth for price)."""
    return repo.get_traffic_tier_by_id(tier_id)


def calculate_traffic_package_price(tier_id: int, months: int = 1) -> float:
    """Calculates total price for a traffic package given duration in months."""
    tier = get_traffic_tier_price(tier_id)
    if not tier:
        return 0.0
    price_per_month = float(tier.get("price") or 0.0)
    effective_months = max(1, months)
    return price_per_month * effective_months


async def process_traffic_purchase(
    key_id: int,
    tier_id: int,
    months: int = 1,
    payment_id: str | None = None,
    expires_at: str | None = None,
    price_override: float | None = None,
) -> dict[str, Any]:
    """
    Processes a confirmed traffic top-up purchase:
    1. Fetches package price from DB.
    2. Records purchase entry with expiry date.
    3. Recalculates total traffic limit (base + active purchases).
    4. Applies updated traffic limit to Remnawave.
    """
    tier = get_traffic_tier_price(tier_id)
    if not tier:
        return {"success": False, "error": f"Traffic tier #{tier_id} not found"}

    key = repo.get_key_by_id(key_id) if hasattr(repo, 'get_key_by_id') else None
    if not key:
        return {"success": False, "error": f"Key #{key_id} not found"}
    if tier.get("host_name") != key.get("host_name"):
        return {"success": False, "error": "Traffic tier does not belong to key host"}

    traffic_gb = int(tier.get("traffic_gb") or 0)
    price = price_override if price_override is not None else calculate_traffic_package_price(tier_id, months)
    effective_months = max(1, months)

    expires_dt = datetime.now(timezone.utc) + timedelta(days=30 * effective_months)
    expires_at_str = expires_at or expires_dt.strftime("%Y-%m-%d %H:%M:%S")

    if payment_id:
        active_purchases = repo.get_active_key_traffic_purchases(key_id)
        if any(purchase.get("payment_id") == payment_id for purchase in active_purchases):
            return {"success": True, "synced": True, "key_id": key_id}

    purchase_id = repo.add_key_traffic_purchase(
        key_id=key_id,
        traffic_gb=traffic_gb,
        price=price,
        expires_at=expires_at_str,
        payment_id=payment_id,
    )

    if not purchase_id:
        return {"success": False, "error": "Failed to record traffic purchase in DB"}

    # Recalculate total limit and update Remnawave
    remnawave_user_id = key.get("remnawave_user_uuid") or key.get("user_id")
    host_name = key.get("host_name")
    stats = await remnawave_api.get_user_traffic_stats_on_host(remnawave_user_id, host_name=host_name)
    current_limit_gb = float(stats.get("traffic_limit_gb") or 0) if stats else 0
    new_total_gb = int(round(current_limit_gb + traffic_gb))

    sync_success = True
    if remnawave_user_id:
        try:
            sync_success = await remnawave_api.update_user_traffic_limit_on_host(
                user_id=remnawave_user_id,
                traffic_limit_gb=new_total_gb,
                host_name=host_name,
            )
        except Exception as e:
            logger.error("Failed to sync traffic limit to Remnawave for key %s: %s", key_id, e)
            sync_success = False

    logger.info(
        "Traffic purchase #%s processed for key #%s: +%s GB, total limit: %s GB, sync: %s",
        purchase_id,
        key_id,
        traffic_gb,
        new_total_gb,
        sync_success,
    )

    return {
        "success": True,
        "purchase_id": purchase_id,
        "key_id": key_id,
        "traffic_gb_added": traffic_gb,
        "new_total_traffic_gb": new_total_gb,
        "synced": sync_success,
    }


async def sync_key_traffic_limit(key_id: int) -> bool:
    """Syncs current total traffic limit for a key to Remnawave."""
    key = repo.get_key_by_id(key_id) if hasattr(repo, 'get_key_by_id') else None
    if not key:
        return False
    remnawave_user_id = key.get("remnawave_user_uuid") or key.get("user_id")
    host_name = key.get("host_name")
    if not remnawave_user_id:
        return False

    new_total_gb = calculate_key_total_traffic_gb(key_id)
    try:
        return await remnawave_api.update_user_traffic_limit_on_host(
            user_id=remnawave_user_id,
            traffic_limit_gb=new_total_gb,
            host_name=host_name,
        )
    except Exception as e:
        logger.error("Error syncing traffic limit for key %s: %s", key_id, e)
        return False


async def process_conditional_traffic_reset(
    key_id: int,
    threshold_gb: float | None = None,
) -> bool:
    """
    Performs conditional traffic reset:
    If remaining traffic > threshold_gb: skip reset (do nothing).
    If remaining traffic < threshold_gb: reset used traffic counter on Remnawave
    and restore total traffic limit (base + active purchases).
    """
    key = repo.get_key_by_id(key_id) if hasattr(repo, 'get_key_by_id') else None
    if not key:
        return False

    remnawave_user_id = key.get("remnawave_user_uuid") or key.get("user_id")
    host_name = key.get("host_name")
    if not remnawave_user_id:
        return False

    # Determine threshold
    if threshold_gb is None:
        plan_id = key.get("plan_id")
        plan = repo.get_plan_by_id(plan_id) if (plan_id and hasattr(repo, 'get_plan_by_id')) else None
        threshold_gb = float(plan.get("traffic_limit_gb") or 0) if plan else 0.0

    try:
        stats = await remnawave_api.get_user_traffic_stats_on_host(remnawave_user_id, host_name=host_name)
        if stats:
            limit_gb = stats.get("traffic_limit_gb", 0.0)
            used_gb = stats.get("used_traffic_gb", 0.0)
            remaining_gb = max(0.0, limit_gb - used_gb)

            if threshold_gb > 0 and remaining_gb >= threshold_gb:
                logger.info(
                    "Key #%s remaining traffic (%.2f GB) is >= threshold (%.2f GB). Skipping reset.",
                    key_id,
                    remaining_gb,
                    threshold_gb,
                )
                return True

        # Perform reset
        await remnawave_api.reset_user_traffic_on_host(remnawave_user_id, host_name=host_name)
        await sync_key_traffic_limit(key_id)
        logger.info("Conditional traffic reset executed for key #%s", key_id)
        return True
    except Exception as e:
        logger.error("Error in process_conditional_traffic_reset for key %s: %s", key_id, e)
        return False


async def expire_old_traffic_purchases() -> int:
    """
    Expires old traffic purchases and updates Remnawave limits for affected keys.
    """
    expired_items = repo.expire_key_traffic_purchases()
    if not expired_items:
        return 0

    affected_key_ids = {item["key_id"] for item in expired_items if item.get("key_id")}
    synced_count = 0
    for key_id in affected_key_ids:
        if await sync_key_traffic_limit(key_id):
            synced_count += 1

    logger.info("Expired %s traffic purchases across %s keys", len(expired_items), len(affected_key_ids))
    return len(expired_items)
