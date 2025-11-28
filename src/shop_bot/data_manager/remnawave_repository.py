import logging
import sqlite3
from datetime import datetime
from typing import Any

from shop_bot.data_manager import database

logger = logging.getLogger(__name__)

DB_FILE = database.DB_FILE
normalize_host_name = database.normalize_host_name


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


def _normalize_email(value: str | None) -> str:
    return (value or "").strip().lower()


def _default_expire_at_ms() -> int:
    return int(datetime.utcnow().timestamp() * 1000)


def list_squads(active_only: bool = False) -> list[dict[str, Any]]:
    query = "SELECT * FROM xui_hosts"
    params: list[Any] = []
    if active_only:
        query += " WHERE COALESCE(is_active, 1) = 1"
    query += " ORDER BY sort_order ASC, host_name ASC"
    with _connect() as conn:
        cursor = conn.cursor()
        cursor.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]


def get_squad(identifier: str) -> dict[str, Any] | None:
    if not identifier:
        return None
    ident = identifier.strip()
    if not ident:
        return None
    normalized = normalize_host_name(ident)
    with _connect() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT *
            FROM xui_hosts
            WHERE TRIM(host_name) = TRIM(?)
               OR TRIM(host_name) = TRIM(?)
               OR TRIM(squad_uuid) = TRIM(?)
               OR TRIM(squad_uuid) = TRIM(?)
            LIMIT 1
            """,
            (ident, normalized, ident, normalized),
        )
        row = cursor.fetchone()
        return dict(row) if row else None


def get_key_by_id(key_id: int) -> dict | None:
    return database.get_key_by_id(key_id)


def get_key_by_email(email: str) -> dict | None:
    return database.get_key_by_email(email)


def get_key_by_remnawave_uuid(remnawave_uuid: str) -> dict | None:
    return database.get_key_by_remnawave_uuid(remnawave_uuid)


def record_key(
    user_id: int,
    squad_uuid: str,
    remnawave_user_uuid: str,
    email: str,
    *,
    host_name: str | None = None,
    expire_at_ms: int | None = None,
    short_uuid: str | None = None,
    subscription_url: str | None = None,
    traffic_limit_bytes: int | None = None,
    traffic_limit_strategy: str | None = None,
    tag: str | None = None,
    description: str | None = None,
) -> int | None:
    expire_ms = expire_at_ms if expire_at_ms is not None else _default_expire_at_ms()
    email_normalized = _normalize_email(email)
    host_name_norm = normalize_host_name(host_name) if host_name else None

    existing = None
    if email_normalized:
        existing = database.get_key_by_email(email_normalized)
    if not existing and remnawave_user_uuid:
        existing = database.get_key_by_remnawave_uuid(remnawave_user_uuid)

    try:
        if existing:
            key_id = existing['key_id']
            database.update_key_fields(
                key_id,
                host_name=host_name_norm or existing.get('host_name'),
                squad_uuid=squad_uuid or existing.get('squad_uuid'),
                remnawave_user_uuid=remnawave_user_uuid or existing.get('remnawave_user_uuid'),
                short_uuid=short_uuid or existing.get('short_uuid'),
                email=email_normalized or existing.get('email'),
                subscription_url=subscription_url,
                expire_at_ms=expire_ms,
                traffic_limit_bytes=traffic_limit_bytes,
                traffic_limit_strategy=traffic_limit_strategy,
                tag=tag,
                description=description,
            )
            return key_id

        return database.add_new_key(
            user_id=user_id,
            host_name=host_name_norm,
            remnawave_user_uuid=remnawave_user_uuid,
            key_email=email_normalized or email,
            expiry_timestamp_ms=expire_ms,
            squad_uuid=squad_uuid,
            short_uuid=short_uuid,
            subscription_url=subscription_url,
            traffic_limit_bytes=traffic_limit_bytes,
            traffic_limit_strategy=traffic_limit_strategy,
            description=description,
            tag=tag,
        )
    except Exception:
        logger.exception("Remnawave repository failed to record key for user %s", user_id)
        return None


def record_key_from_payload(
    user_id: int,
    payload: dict[str, Any],
    *,
    host_name: str | None = None,
    description: str | None = None,
    tag: str | None = None,
) -> int | None:
    if not payload:
        return None
    squad_uuid = (payload.get('squad_uuid') or payload.get('squadUuid') or '').strip()
    remnawave_user_uuid = (payload.get('client_uuid') or payload.get('uuid') or payload.get('id') or '').strip()
    email = payload.get('email') or payload.get('accountEmail') or ''
    expire_at_ms = payload.get('expiry_timestamp_ms')
    if expire_at_ms is None:
        expire_iso = payload.get('expireAt') or payload.get('expiryDate')
        if expire_iso:
            try:
                expire_at_ms = int(datetime.fromisoformat(str(expire_iso).replace('Z', '+00:00')).timestamp() * 1000)
            except Exception:
                expire_at_ms = None
    return record_key(
        user_id=user_id,
        squad_uuid=squad_uuid,
        remnawave_user_uuid=remnawave_user_uuid,
        email=email,
        host_name=host_name or payload.get('host_name'),
        expire_at_ms=expire_at_ms,
        short_uuid=payload.get('short_uuid') or payload.get('shortUuid'),
        subscription_url=payload.get('subscription_url')
            or payload.get('connection_string')
            or payload.get('subscriptionUrl'),
        traffic_limit_bytes=payload.get('traffic_limit_bytes') or payload.get('trafficLimitBytes'),
        traffic_limit_strategy=payload.get('traffic_limit_strategy') or payload.get('trafficLimitStrategy'),
        tag=tag or payload.get('tag'),
        description=description or payload.get('description'),
    )


def update_key(
    key_id: int,
    *,
    host_name: str | None = None,
    squad_uuid: str | None = None,
    remnawave_user_uuid: str | None = None,
    short_uuid: str | None = None,
    email: str | None = None,
    subscription_url: str | None = None,
    expire_at_ms: int | None = None,
    traffic_limit_bytes: int | None = None,
    traffic_limit_strategy: str | None = None,
    tag: str | None = None,
    description: str | None = None,
) -> bool:
    return database.update_key_fields(
        key_id,
        host_name=host_name,
        squad_uuid=squad_uuid,
        remnawave_user_uuid=remnawave_user_uuid,
        short_uuid=short_uuid,
        email=email,
        subscription_url=subscription_url,
        expire_at_ms=expire_at_ms,
        traffic_limit_bytes=traffic_limit_bytes,
        traffic_limit_strategy=traffic_limit_strategy,
        tag=tag,
        description=description,
    )


def delete_key_by_email(email: str) -> bool:
    return database.delete_key_by_email(email)




_LEGACY_FORWARDERS = (
    "add_support_message",
    "add_to_balance",
    "add_to_referral_balance",
    "add_to_referral_balance_all",
    "adjust_user_balance",
    "ban_user",
    "create_gift_key",
    "create_host",
    "create_pending_transaction",
    "create_payload_pending",
    "create_plan",
    "create_support_ticket",
    "deduct_from_balance",
    "deduct_from_referral_balance",
    "delete_host",
    "delete_key_by_id",
    "delete_plan",
    "delete_ticket",
    "delete_user_keys",
    "find_and_complete_ton_transaction",
    "find_and_complete_pending_transaction",
    "get_latest_pending_for_user",
    "get_pending_status",
    "get_pending_metadata",
    "get_admin_ids",
    "get_admin_stats",
    "get_all_hosts",
    "get_all_keys",
    "get_all_settings",
    "get_all_tickets_count",
    "get_all_users",
    "get_balance",
    "get_closed_tickets_count",
    "get_daily_stats_for_charts",
    "get_host",
    "get_keys_for_host",
    "get_keys_for_user",
    "get_latest_speedtest",
    "get_next_key_number",
    "get_open_tickets_count",
    "get_paginated_transactions",
    "get_plan_by_id",
    "get_plans_for_host",
    "get_recent_transactions",
    "get_referral_balance",
    "get_referral_balance_all",
    "get_referral_count",
    "get_referrals_for_user",
    "get_setting",
    "get_speedtests",
    "get_ticket",
    "get_ticket_by_thread",
    "get_ticket_messages",
    "get_or_create_open_ticket",
    "get_tickets_paginated",
    "get_total_keys_count",
    "get_total_spent_sum",
    "get_user",
    "get_user_count",
    "get_user_keys",

    "get_users_paginated",
    "get_keys_counts_for_users",
    "get_user_tickets",
    "insert_host_speedtest",
    "initialize_db",
    "is_admin",
    "log_transaction",
    "register_user_if_not_exists",
    "run_migration",
    "set_referral_start_bonus_received",
    "set_terms_agreed",
    "set_ticket_status",
    "set_trial_used",
    "unban_user",
    "update_host_name",
    "update_host_ssh_settings",
    "update_host_subscription_url",
    "update_host_url",
    "update_key_comment",
    "update_key_fields",
    "update_key_host",
    "update_key_host_and_info",
    "update_key_status_from_server",
    "update_plan",
    "update_setting",
    "update_ticket_subject",
    "update_ticket_thread_info",
    "update_user_stats",

    "get_all_ssh_targets",
    "get_ssh_target",
    "create_ssh_target",
    "update_ssh_target_fields",
    "delete_ssh_target",

    "insert_resource_metric",
    "get_latest_resource_metric",
    "get_metrics_series",
)

for _name in _LEGACY_FORWARDERS:
    if _name not in globals():
        globals()[_name] = getattr(database, _name)

__all__ = sorted(
    name for name in globals()
    if not name.startswith('_') and name not in {"logging", "sqlite3", "datetime", "Any", "database", "logger"}
)




def create_gift_token(
    token: str,
    host_name: str,
    days: int,
    *,
    activation_limit: int = 1,
    expires_at: datetime | None = None,
    created_by: int | None = None,
    comment: str | None = None,
) -> bool:
    token_s = (token or "").strip()
    if not token_s:
        raise ValueError("token is required")
    host_name_n = normalize_host_name(host_name)
    days_i = int(days)
    limit_i = int(activation_limit or 1)
    if days_i <= 0 or limit_i <= 0:
        raise ValueError("days and activation_limit must be positive")

    try:
        with _connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO gift_tokens (token, host_name, days, activation_limit, expires_at, created_by, comment)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    token_s,
                    host_name_n,
                    days_i,
                    limit_i,
                    expires_at.isoformat() if isinstance(expires_at, datetime) else expires_at,
                    created_by,
                    comment,
                ),
            )
            conn.commit()
            return True
    except sqlite3.IntegrityError:
        return False


def get_gift_token(token: str) -> dict | None:
    token_s = (token or "").strip()
    if not token_s:
        return None
    with _connect() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM gift_tokens WHERE token = ?", (token_s,))
        row = cursor.fetchone()
        return dict(row) if row else None


def list_gift_tokens(active_only: bool = False) -> list[dict]:
    query = "SELECT * FROM gift_tokens"
    params: list[Any] = []
    if active_only:
        query += " WHERE (activation_limit IS NULL OR activation_limit > activations_used)"
        query += " AND (expires_at IS NULL OR datetime(expires_at) >= datetime('now'))"
    query += " ORDER BY created_at DESC"
    with _connect() as conn:
        cursor = conn.cursor()
        cursor.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]


def delete_gift_token(token: str) -> bool:
    token_s = (token or "").strip()
    if not token_s:
        return False
    with _connect() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM gift_tokens WHERE token = ?", (token_s,))
        conn.commit()
        return cursor.rowcount > 0


def claim_gift_token(token: str, user_id: int, key_id: int | None = None) -> dict | None:
    token_s = (token or "").strip()
    if not token_s:
        return None
    user_id_i = int(user_id)
    now_iso = datetime.utcnow().isoformat()
    with _connect() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT token, host_name, days, activation_limit, activations_used, expires_at
            FROM gift_tokens
            WHERE token = ?
            """,
            (token_s,),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        record = dict(row)
        expires_at = record.get("expires_at")
        if expires_at:
            try:
                exp_dt = datetime.fromisoformat(str(expires_at))
            except Exception:
                exp_dt = None
            if exp_dt and exp_dt < datetime.utcnow():
                return None
        activation_limit = record.get("activation_limit") or 0
        activations_used = record.get("activations_used") or 0
        if activation_limit and activations_used >= activation_limit:
            return None

        try:
            cursor.execute(
                """
                UPDATE gift_tokens
                SET activations_used = activations_used + 1,
                    last_claimed_at = ?
                WHERE token = ?
                """,
                (now_iso, token_s),
            )
            cursor.execute(
                """
                INSERT INTO gift_token_claims (token, user_id, key_id, claimed_at)
                VALUES (?, ?, ?, ?)
                """,
                (token_s, user_id_i, key_id, now_iso),
            )
            conn.commit()
            record["activations_used"] = activations_used + 1
            record["claimed_by"] = user_id_i
            record["claimed_at"] = now_iso
            record["key_id"] = key_id
            return record
        except sqlite3.Error:
            conn.rollback()
            return None




def create_promo_code(
    code: str,
    *,
    discount_percent: float | None = None,
    discount_amount: float | None = None,
    usage_limit_total: int | None = None,
    usage_limit_per_user: int | None = None,
    valid_from: datetime | None = None,
    valid_until: datetime | None = None,
    created_by: int | None = None,
    description: str | None = None,
) -> bool:
    code_s = (code or "").strip().upper()
    if not code_s:
        raise ValueError("code is required")
    if (discount_percent or 0) <= 0 and (discount_amount or 0) <= 0:
        raise ValueError("discount must be positive")
    try:
        with _connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO promo_codes (
                    code, discount_percent, discount_amount,
                    usage_limit_total, usage_limit_per_user,
                    valid_from, valid_until, created_by, description
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    code_s,
                    float(discount_percent) if discount_percent is not None else None,
                    float(discount_amount) if discount_amount is not None else None,
                    usage_limit_total,
                    usage_limit_per_user,
                    valid_from.isoformat() if isinstance(valid_from, datetime) else valid_from,
                    valid_until.isoformat() if isinstance(valid_until, datetime) else valid_until,
                    created_by,
                    description,
                ),
            )
            conn.commit()
            return True
    except sqlite3.IntegrityError:
        return False


def get_promo_code(code: str) -> dict | None:
    code_s = (code or "").strip().upper()
    if not code_s:
        return None
    with _connect() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM promo_codes WHERE code = ?", (code_s,))
        row = cursor.fetchone()
        return dict(row) if row else None


def list_promo_codes(include_inactive: bool = True) -> list[dict]:
    query = "SELECT * FROM promo_codes"
    if not include_inactive:
        query += " WHERE is_active = 1"
    query += " ORDER BY created_at DESC"
    with _connect() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        return [dict(row) for row in cursor.fetchall()]


def check_promo_code_available(code: str, user_id: int) -> tuple[dict | None, str | None]:
    """Проверить возможность использования промокода, не изменяя лимиты."""
    code_s = (code or "").strip().upper()
    if not code_s:
        return None, "empty_code"
    user_id_i = int(user_id)
    with _connect() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT code, discount_percent, discount_amount,
                   usage_limit_total, usage_limit_per_user,
                   used_total, valid_from, valid_until, is_active
            FROM promo_codes
            WHERE code = ?
            """,
            (code_s,),
        )
        promo_row = cursor.fetchone()
        if promo_row is None:
            return None, "not_found"
        promo = dict(promo_row)
        if not promo.get("is_active"):
            return None, "inactive"
        now_dt = datetime.utcnow()
        valid_from = promo.get("valid_from")
        if valid_from:
            try:
                if datetime.fromisoformat(str(valid_from)) > now_dt:
                    return None, "not_started"
            except Exception:
                pass
        valid_until = promo.get("valid_until")
        if valid_until:
            try:
                if datetime.fromisoformat(str(valid_until)) < now_dt:
                    try:
                        update_promo_code_status(code_s, is_active=False)
                    except Exception:
                        pass
                    return None, "expired"
            except Exception:
                pass
        usage_limit_total = promo.get("usage_limit_total")
        used_total = promo.get("used_total") or 0
        if usage_limit_total and used_total >= usage_limit_total:
            return None, "total_limit_reached"
        usage_limit_per_user = promo.get("usage_limit_per_user")
        if usage_limit_per_user:
            cursor.execute(
                "SELECT COUNT(1) FROM promo_code_usages WHERE code = ? AND user_id = ?",
                (code_s, user_id_i),
            )
            per_user_count = cursor.fetchone()[0]
            if per_user_count >= usage_limit_per_user:
                return None, "user_limit_reached"
        return promo, None


def update_promo_code_status(code: str, *, is_active: bool | None = None) -> bool:
    code_s = (code or "").strip().upper()
    if not code_s:
        return False
    sets: list[str] = []
    params: list[Any] = []
    if is_active is not None:
        sets.append("is_active = ?")
        params.append(1 if is_active else 0)
    if not sets:
        return False
    params.append(code_s)
    with _connect() as conn:
        cursor = conn.cursor()
        cursor.execute(f"UPDATE promo_codes SET {', '.join(sets)} WHERE code = ?", params)
        conn.commit()
        return cursor.rowcount > 0


def delete_promo_code(code: str) -> bool:
    code_s = (code or "").strip().upper()
    if not code_s:
        return False
    with _connect() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM promo_codes WHERE code = ?", (code_s,))
        conn.commit()
        return cursor.rowcount > 0


def redeem_promo_code(code: str, user_id: int, *, applied_amount: float, order_id: str | None = None) -> dict | None:
    code_s = (code or "").strip().upper()
    if not code_s:
        return None
    user_id_i = int(user_id)
    applied_amount_f = float(applied_amount)
    now_iso = datetime.utcnow().isoformat()
    with _connect() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT code, discount_percent, discount_amount,
                   usage_limit_total, usage_limit_per_user,
                   used_total, valid_from, valid_until, is_active
            FROM promo_codes
            WHERE code = ?
            """,
            (code_s,),
        )
        promo_row = cursor.fetchone()
        if promo_row is None:
            return None
        promo = dict(promo_row)
        if not promo.get("is_active"):
            return None
        valid_from = promo.get("valid_from")
        valid_until = promo.get("valid_until")
        now_dt = datetime.utcnow()
        if valid_from:
            try:
                if datetime.fromisoformat(str(valid_from)) > now_dt:
                    return None
            except Exception:
                pass
        if valid_until:
            try:
                if datetime.fromisoformat(str(valid_until)) < now_dt:
                    try:
                        update_promo_code_status(code_s, is_active=False)
                    except Exception:
                        pass
                    return None
            except Exception:
                pass
        usage_limit_total = promo.get("usage_limit_total")
        used_total = promo.get("used_total") or 0
        if usage_limit_total and used_total >= usage_limit_total:
            return None
        usage_limit_per_user = promo.get("usage_limit_per_user")
        per_user_count = 0
        if usage_limit_per_user:
            cursor.execute(
                "SELECT COUNT(1) FROM promo_code_usages WHERE code = ? AND user_id = ?",
                (code_s, user_id_i),
            )
            per_user_count = cursor.fetchone()[0]
            if per_user_count >= usage_limit_per_user:
                return None
        try:
            cursor.execute(
                """
                INSERT INTO promo_code_usages (code, user_id, applied_amount, order_id, used_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (code_s, user_id_i, applied_amount_f, order_id, now_iso),
            )
            cursor.execute(
                """
                UPDATE promo_codes
                SET used_total = COALESCE(used_total, 0) + 1
                WHERE code = ?
                """,
                (code_s,),
            )
            conn.commit()
            promo["used_total"] = used_total + 1
            promo["usage_limit_per_user"] = usage_limit_per_user
            promo["user_used_count"] = per_user_count + 1
            promo["redeemed_by"] = user_id_i
            promo["applied_amount"] = applied_amount_f
            promo["order_id"] = order_id
            promo["used_at"] = now_iso
            return promo
        except sqlite3.Error as e:
            conn.rollback()
            if str(e).startswith("FOREIGN KEY constraint failed"):
                return None
            raise
