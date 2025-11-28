import sqlite3
from datetime import datetime, timezone
import logging
from pathlib import Path
import json
import re
from typing import Any

logger = logging.getLogger(__name__)


import os
if os.path.exists("/app/project/users.db"):

    DB_FILE = Path("/app/project/users.db")
elif os.path.exists("users-20251005-173430.db"):

    DB_FILE = Path("users-20251005-173430.db")
elif os.path.exists("users.db"):

    DB_FILE = Path("users.db")
else:

    DB_FILE = Path("users.db")


def _now_str() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def _to_datetime_str(ts_ms: int | None) -> str | None:
    if ts_ms is None:
        return None
    try:
        dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def _normalize_email(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip().lower()
    return cleaned or None


def _normalize_key_row(row: sqlite3.Row | dict | None) -> dict | None:
    if row is None:
        return None
    data = dict(row)
    email = _normalize_email(data.get("email") or data.get("key_email"))
    if email:
        data["email"] = email
        data["key_email"] = email
    rem_uuid = data.get("remnawave_user_uuid") or data.get("xui_client_uuid")
    if rem_uuid:
        data["remnawave_user_uuid"] = rem_uuid
        data["xui_client_uuid"] = rem_uuid
    expire_value = data.get("expire_at") or data.get("expiry_date")
    if expire_value:
        expire_str = expire_value.strftime("%Y-%m-%d %H:%M:%S") if isinstance(expire_value, datetime) else str(expire_value)
        data["expire_at"] = expire_str
        data["expiry_date"] = expire_str
    created_value = data.get("created_at") or data.get("created_date")
    if created_value:
        created_str = created_value.strftime("%Y-%m-%d %H:%M:%S") if isinstance(created_value, datetime) else str(created_value)
        data["created_at"] = created_str
        data["created_date"] = created_str
    subscription_url = data.get("subscription_url") or data.get("connection_string")
    if subscription_url:
        data["subscription_url"] = subscription_url
        data.setdefault("connection_string", subscription_url)
    return data


def _get_table_columns(cursor: sqlite3.Cursor, table: str) -> set[str]:
    cursor.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in cursor.fetchall()}


def _ensure_table_column(cursor: sqlite3.Cursor, table: str, column: str, definition: str) -> None:
    columns = _get_table_columns(cursor, table)
    if column not in columns:
        cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def _ensure_unique_index(cursor: sqlite3.Cursor, name: str, table: str, column: str) -> None:
    cursor.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS {name} ON {table}({column})")


def _ensure_index(cursor: sqlite3.Cursor, name: str, table: str, column: str) -> None:
    cursor.execute(f"CREATE INDEX IF NOT EXISTS {name} ON {table}({column})")


def normalize_host_name(name: str | None) -> str:
    """Normalize host name by trimming and removing invisible/unicode spaces."""
    s = (name or "").strip()
    for ch in ("\u00A0", "\u200B", "\u200C", "\u200D", "\uFEFF"):
        s = s.replace(ch, "")
    return s


def initialize_db():
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    telegram_id INTEGER PRIMARY KEY,
                    username TEXT,
                    total_spent REAL DEFAULT 0,
                    total_months INTEGER DEFAULT 0,
                    trial_used BOOLEAN DEFAULT 0,
                    agreed_to_terms BOOLEAN DEFAULT 0,
                    registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_banned BOOLEAN DEFAULT 0,
                    balance REAL DEFAULT 0,
                    referred_by INTEGER,
                    referral_balance REAL DEFAULT 0,
                    referral_balance_all REAL DEFAULT 0,
                    referral_start_bonus_received BOOLEAN DEFAULT 0
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS pending_transactions (
                    payment_id TEXT PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    amount_rub REAL,
                    metadata TEXT,
                    status TEXT DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS vpn_keys (
                    key_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    host_name TEXT,
                    squad_uuid TEXT,
                    remnawave_user_uuid TEXT,
                    short_uuid TEXT,
                    email TEXT UNIQUE,
                    key_email TEXT UNIQUE,
                    subscription_url TEXT,
                    expire_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    traffic_limit_bytes INTEGER,
                    traffic_limit_strategy TEXT DEFAULT 'NO_RESET',
                    tag TEXT,
                    description TEXT
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS transactions (
                    username TEXT,
                    transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    payment_id TEXT UNIQUE NOT NULL,
                    user_id INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    amount_rub REAL NOT NULL,
                    amount_currency REAL,
                    currency_name TEXT,
                    payment_method TEXT,
                    metadata TEXT,
                    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS bot_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS button_configs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    menu_type TEXT NOT NULL,
                    button_id TEXT NOT NULL,
                    text TEXT NOT NULL,
                    callback_data TEXT,
                    url TEXT,
                    row_position INTEGER DEFAULT 0,
                    column_position INTEGER DEFAULT 0,
                    button_width INTEGER DEFAULT 1,
                    is_active INTEGER DEFAULT 1,
                    sort_order INTEGER DEFAULT 0,
                    metadata TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(menu_type, button_id)
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS xui_hosts(
                    host_name TEXT PRIMARY KEY,
                    squad_uuid TEXT UNIQUE,
                    description TEXT,
                    default_traffic_limit_bytes INTEGER,
                    default_traffic_strategy TEXT DEFAULT 'NO_RESET',
                    host_url TEXT,
                    host_username TEXT,
                    host_pass TEXT,
                    host_inbound_id INTEGER,
                    subscription_url TEXT,
                    ssh_host TEXT,
                    ssh_port INTEGER,
                    ssh_user TEXT,
                    ssh_password TEXT,
                    ssh_key_path TEXT,
                    is_active INTEGER DEFAULT 1,
                    sort_order INTEGER DEFAULT 0,
                    metadata TEXT
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS plans (
                    plan_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    host_name TEXT,
                    squad_uuid TEXT,
                    plan_name TEXT NOT NULL,
                    months INTEGER,
                    duration_days INTEGER,
                    price REAL NOT NULL,
                    traffic_limit_bytes INTEGER,
                    traffic_limit_strategy TEXT DEFAULT 'NO_RESET',
                    is_active INTEGER DEFAULT 1,
                    sort_order INTEGER DEFAULT 0,
                    metadata TEXT,
                    FOREIGN KEY (host_name) REFERENCES xui_hosts (host_name)
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS support_tickets (
                    ticket_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT "open",
                    subject TEXT,
                    forum_chat_id TEXT,
                    message_thread_id INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS support_messages (
                    message_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticket_id INTEGER NOT NULL,
                    sender TEXT NOT NULL,
                    content TEXT NOT NULL,
                    media TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (ticket_id) REFERENCES support_tickets (ticket_id)
                )
            ''')

            try:
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_support_tickets_thread ON support_tickets(forum_chat_id, message_thread_id)")
            except Exception:
                pass
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS host_speedtests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    host_name TEXT NOT NULL,
                    method TEXT NOT NULL,
                    ping_ms REAL,
                    jitter_ms REAL,
                    download_mbps REAL,
                    upload_mbps REAL,
                    server_name TEXT,
                    server_id TEXT,
                    ok INTEGER NOT NULL DEFAULT 1,
                    error TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_host_speedtests_host_time ON host_speedtests(host_name, created_at DESC)")

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS resource_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scope TEXT NOT NULL,                -- 'local' | 'host' | 'target'
                    object_name TEXT NOT NULL,          -- 'panel' | host_name | target_name
                    cpu_percent REAL,
                    mem_percent REAL,
                    disk_percent REAL,
                    load1 REAL,
                    net_bytes_sent INTEGER,
                    net_bytes_recv INTEGER,
                    raw_json TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_resource_metrics_scope_time ON resource_metrics(scope, object_name, created_at DESC)")

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS speedtest_ssh_targets (
                    target_name TEXT PRIMARY KEY,
                    ssh_host TEXT NOT NULL,
                    ssh_port INTEGER DEFAULT 22,
                    ssh_user TEXT,
                    ssh_password TEXT,
                    ssh_key_path TEXT,
                    description TEXT,
                    is_active INTEGER DEFAULT 1,
                    sort_order INTEGER DEFAULT 0,
                    metadata TEXT
                )
            ''')
            default_settings = {
                "panel_login": "admin",
                "panel_password": "admin",
                "about_text": None,
                "terms_url": None,
                "privacy_url": None,
                "support_user": None,
                "support_text": None,
                "channel_url": None,
                "force_subscription": "true",
                "receipt_email": "example@example.com",
                "telegram_bot_token": None,
                "telegram_bot_username": None,
                "trial_enabled": "true",
                "trial_duration_days": "3",
                "enable_referrals": "true",
                "referral_percentage": "10",
                "referral_discount": "5",
                "minimum_withdrawal": "100",
                "admin_telegram_id": None,
                "admin_telegram_ids": None,
                "yookassa_shop_id": None,
                "yookassa_secret_key": None,
                "sbp_enabled": "false",
                "cryptobot_token": None,
                "heleket_merchant_id": None,
                "heleket_api_key": None,
                "domain": None,
                "ton_wallet_address": None,
                "tonapi_key": None,
                "support_forum_chat_id": None,
                "enable_fixed_referral_bonus": "false",
                "fixed_referral_bonus_amount": "50",
                "referral_reward_type": "percent_purchase",
                "referral_on_start_referrer_amount": "20",
                "backup_interval_days": "1",

                "monitoring_enabled": "true",
                "monitoring_interval_sec": "300",
                "monitoring_cpu_threshold": "90",
                "monitoring_mem_threshold": "90",
                "monitoring_disk_threshold": "90",
                "monitoring_alert_cooldown_sec": "3600",
                "remnawave_base_url": None,
                "remnawave_api_token": None,
                "remnawave_cookies": "{}",
                "remnawave_is_local_network": "false",
                "default_extension_days": "30",

                "main_menu_text": None,
                "howto_intro_text": None,
                "howto_android_text": None,
                "howto_ios_text": None,
                "howto_windows_text": None,
                "howto_linux_text": None,

                "btn_trial_text": None,
                "btn_profile_text": None,
                "btn_my_keys_text": None,
                "btn_buy_key_text": None,
                "btn_topup_text": None,
                "btn_referral_text": None,
                "btn_support_text": None,
                "btn_about_text": None,
                "btn_speed_text": None,
                "btn_howto_text": None,
                "btn_admin_text": None,
                "btn_back_to_menu_text": None,

                "stars_enabled": "false",
                "yoomoney_enabled": "false",
                "yoomoney_wallet": None,
                "yoomoney_secret": None,

                "yoomoney_api_token": None,
                "yoomoney_client_id": None,
                "yoomoney_client_secret": None,
                "yoomoney_redirect_uri": None,
                "stars_per_rub": "1",
            }
            run_migration()
            for key, value in default_settings.items():
                cursor.execute(
                    "INSERT OR IGNORE INTO bot_settings (key, value) VALUES (?, ?)",
                    (key, value),
                )
            conn.commit()
            

            initialize_default_button_configs()
            

            update_existing_my_keys_button()
            

            try:
                cursor.execute("ALTER TABLE button_configs ADD COLUMN button_width INTEGER DEFAULT 1")
                logging.info("Added button_width column to button_configs table")
            except sqlite3.OperationalError:

                pass
            
            logging.info("База данных инициализирована")
    except sqlite3.Error as e:
        logging.error("Не удалось инициализировать базу данных: %s", e)


def _ensure_users_columns(cursor: sqlite3.Cursor) -> None:
    mapping = {
        "referred_by": "INTEGER",
        "balance": "REAL DEFAULT 0",
        "referral_balance": "REAL DEFAULT 0",
        "referral_balance_all": "REAL DEFAULT 0",
        "referral_start_bonus_received": "BOOLEAN DEFAULT 0",
    }
    for column, definition in mapping.items():
        _ensure_table_column(cursor, "users", column, definition)


def _ensure_hosts_columns(cursor: sqlite3.Cursor) -> None:
    extras = {
        "squad_uuid": "TEXT",
        "description": "TEXT",
        "default_traffic_limit_bytes": "INTEGER",
        "default_traffic_strategy": "TEXT DEFAULT 'NO_RESET'",
        "is_active": "INTEGER DEFAULT 1",
        "sort_order": "INTEGER DEFAULT 0",
        "metadata": "TEXT",
        "subscription_url": "TEXT",
        "ssh_host": "TEXT",
        "ssh_port": "INTEGER",
        "ssh_user": "TEXT",
        "ssh_password": "TEXT",
        "ssh_key_path": "TEXT",

        "remnawave_base_url": "TEXT",
        "remnawave_api_token": "TEXT",
    }
    for column, definition in extras.items():
        _ensure_table_column(cursor, "xui_hosts", column, definition)


def _ensure_plans_columns(cursor: sqlite3.Cursor) -> None:
    extras = {
        "squad_uuid": "TEXT",
        "duration_days": "INTEGER",
        "traffic_limit_bytes": "INTEGER",
        "traffic_limit_strategy": "TEXT DEFAULT 'NO_RESET'",
        "is_active": "INTEGER DEFAULT 1",
        "sort_order": "INTEGER DEFAULT 0",
        "metadata": "TEXT",
    }
    for column, definition in extras.items():
        _ensure_table_column(cursor, "plans", column, definition)


def _ensure_support_tickets_columns(cursor: sqlite3.Cursor) -> None:
    extras = {
        "forum_chat_id": "TEXT",
        "message_thread_id": "INTEGER",
    }
    for column, definition in extras.items():
        _ensure_table_column(cursor, "support_tickets", column, definition)


def _finalize_vpn_key_indexes(cursor: sqlite3.Cursor) -> None:
    _ensure_unique_index(cursor, "uq_vpn_keys_email", "vpn_keys", "email")
    _ensure_unique_index(cursor, "uq_vpn_keys_key_email", "vpn_keys", "key_email")
    _ensure_index(cursor, "idx_vpn_keys_user_id", "vpn_keys", "user_id")
    _ensure_index(cursor, "idx_vpn_keys_rem_uuid", "vpn_keys", "remnawave_user_uuid")
    _ensure_index(cursor, "idx_vpn_keys_expire_at", "vpn_keys", "expire_at")


def _rebuild_vpn_keys_table(cursor: sqlite3.Cursor) -> None:
    columns = _get_table_columns(cursor, "vpn_keys")
    legacy_markers = {"xui_client_uuid", "expiry_date", "created_date", "connection_string"}
    required = {"remnawave_user_uuid", "email", "expire_at", "created_at", "updated_at"}
    if required.issubset(columns) and not (columns & legacy_markers):
        _finalize_vpn_key_indexes(cursor)
        return

    cursor.execute("ALTER TABLE vpn_keys RENAME TO vpn_keys_legacy")
    cursor.execute('''
        CREATE TABLE vpn_keys (
            key_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            host_name TEXT,
            squad_uuid TEXT,
            remnawave_user_uuid TEXT,
            short_uuid TEXT,
            email TEXT UNIQUE,
            key_email TEXT UNIQUE,
            subscription_url TEXT,
            expire_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            traffic_limit_bytes INTEGER,
            traffic_limit_strategy TEXT DEFAULT 'NO_RESET',
            tag TEXT,
            description TEXT
        )
    ''')
    old_columns = _get_table_columns(cursor, "vpn_keys_legacy")

    def has(column: str) -> bool:
        return column in old_columns

    def col(column: str, default: str = "NULL") -> str:
        return column if has(column) else default

    rem_uuid_expr = "remnawave_user_uuid" if has("remnawave_user_uuid") else ("xui_client_uuid" if has("xui_client_uuid") else "NULL")
    email_expr = "LOWER(email)" if has("email") else ("LOWER(key_email)" if has("key_email") else "NULL")
    key_email_expr = "LOWER(key_email)" if has("key_email") else ("LOWER(email)" if has("email") else "NULL")
    subscription_expr = col("subscription_url", "connection_string" if has("connection_string") else "NULL")
    expire_expr = col("expire_at", "expiry_date" if has("expiry_date") else "NULL")
    created_expr = col("created_at", "created_date" if has("created_date") else "CURRENT_TIMESTAMP")
    updated_expr = col("updated_at", created_expr)
    traffic_strategy_expr = col("traffic_limit_strategy", "'NO_RESET'")

    select_clause = ",\n            ".join([
        f"{col('key_id')} AS key_id",
        f"{col('user_id')} AS user_id",
        f"{col('host_name')} AS host_name",
        f"{col('squad_uuid')} AS squad_uuid",
        f"{rem_uuid_expr} AS remnawave_user_uuid",
        f"{col('short_uuid')} AS short_uuid",
        f"{email_expr} AS email",
        f"{key_email_expr} AS key_email",
        f"{subscription_expr} AS subscription_url",
        f"{expire_expr} AS expire_at",
        f"{created_expr} AS created_at",
        f"{updated_expr} AS updated_at",
        f"{col('traffic_limit_bytes')} AS traffic_limit_bytes",
        f"{traffic_strategy_expr} AS traffic_limit_strategy",
        f"{col('tag')} AS tag",
        f"{col('description')} AS description",
    ])

    cursor.execute(
        f"""
        INSERT INTO vpn_keys (
            key_id,
            user_id,
            host_name,
            squad_uuid,
            remnawave_user_uuid,
            short_uuid,
            email,
            key_email,
            subscription_url,
            expire_at,
            created_at,
            updated_at,
            traffic_limit_bytes,
            traffic_limit_strategy,
            tag,
            description
        )
        SELECT
            {select_clause}
        FROM vpn_keys_legacy
        """
    )
    cursor.execute("DROP TABLE vpn_keys_legacy")
    cursor.execute("SELECT MAX(key_id) FROM vpn_keys")
    max_id = cursor.fetchone()[0]
    if max_id is not None:
        cursor.execute("INSERT OR REPLACE INTO sqlite_sequence(name, seq) VALUES('vpn_keys', ?)", (max_id,))
    _finalize_vpn_key_indexes(cursor)


def _ensure_vpn_keys_schema(cursor: sqlite3.Cursor) -> None:
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='vpn_keys'")
    if cursor.fetchone() is None:
        cursor.execute('''
            CREATE TABLE vpn_keys (
                key_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                host_name TEXT,
                squad_uuid TEXT,
                remnawave_user_uuid TEXT,
                short_uuid TEXT,
                email TEXT UNIQUE,
                key_email TEXT UNIQUE,
                subscription_url TEXT,
                expire_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                traffic_limit_bytes INTEGER,
                traffic_limit_strategy TEXT DEFAULT 'NO_RESET',
                tag TEXT,
                description TEXT
            )
        ''')
        _finalize_vpn_key_indexes(cursor)
        return
    _rebuild_vpn_keys_table(cursor)


def run_migration():
    if not DB_FILE.exists():
        logging.error("Файл базы данных отсутствует, миграция пропущена.")
        return

    logging.info("Запуск миграций базы данных: %s", DB_FILE)

    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA foreign_keys = OFF")
            _ensure_users_columns(cursor)
            _ensure_hosts_columns(cursor)
            _ensure_plans_columns(cursor)
            _ensure_support_tickets_columns(cursor)
            _ensure_vpn_keys_schema(cursor)
            _ensure_ssh_targets_table(cursor)
            _ensure_gift_tokens_table(cursor)
            _ensure_promo_tables(cursor)

            try:
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_support_tickets_thread ON support_tickets(forum_chat_id, message_thread_id)")
            except Exception:
                pass

            try:
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS pending_transactions (
                        payment_id TEXT PRIMARY KEY,
                        user_id INTEGER NOT NULL,
                        amount_rub REAL,
                        metadata TEXT,
                        status TEXT DEFAULT 'pending',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
            except Exception:
                pass
            cursor.execute("PRAGMA foreign_keys = ON")
            conn.commit()
    except sqlite3.Error as e:
        logging.error("Сбой миграции базы данных: %s", e)


def insert_resource_metric(
    scope: str,
    object_name: str,
    *,
    cpu_percent: float | None = None,
    mem_percent: float | None = None,
    disk_percent: float | None = None,
    load1: float | None = None,
    net_bytes_sent: int | None = None,
    net_bytes_recv: int | None = None,
    raw_json: str | None = None,
) -> int | None:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''
                INSERT INTO resource_metrics (
                    scope, object_name, cpu_percent, mem_percent, disk_percent, load1,
                    net_bytes_sent, net_bytes_recv, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    (scope or '').strip(),
                    (object_name or '').strip(),
                    cpu_percent, mem_percent, disk_percent, load1,
                    net_bytes_sent, net_bytes_recv, raw_json,
                )
            )
            conn.commit()
            return cursor.lastrowid
    except sqlite3.Error as e:
        logging.error("Failed to insert resource metric for %s/%s: %s", scope, object_name, e)
        return None


def get_latest_resource_metric(scope: str, object_name: str) -> dict | None:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(
                '''
                SELECT * FROM resource_metrics
                WHERE scope = ? AND object_name = ?
                ORDER BY created_at DESC
                LIMIT 1
                ''',
                ((scope or '').strip(), (object_name or '').strip())
            )
            row = cursor.fetchone()
            return dict(row) if row else None
    except sqlite3.Error as e:
        logging.error("Failed to get latest resource metric for %s/%s: %s", scope, object_name, e)
        return None


def get_metrics_series(scope: str, object_name: str, *, since_hours: int = 24, limit: int = 500) -> list[dict]:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            


            if since_hours == 1:
                hours_filter = 2
            else:
                hours_filter = max(1, int(since_hours))
            

            cursor.execute(
                f'''
                SELECT created_at, cpu_percent, mem_percent, disk_percent, load1
                FROM resource_metrics
                WHERE scope = ? AND object_name = ?
                  AND created_at >= datetime('now', ?)
                ORDER BY created_at ASC
                LIMIT ?
                ''',
                (
                    (scope or '').strip(),
                    (object_name or '').strip(),
                    f'-{hours_filter} hours',
                    max(10, int(limit)),
                )
            )
            rows = cursor.fetchall() or []
            

            logging.debug(f"get_metrics_series: {scope}/{object_name}, since_hours={since_hours}, found {len(rows)} records")
            
            return [dict(r) for r in rows]
    except sqlite3.Error as e:
        logging.error("Failed to get metrics series for %s/%s: %s", scope, object_name, e)
        return []


def create_host(name: str, url: str, user: str, passwd: str, inbound: int, subscription_url: str | None = None):
    try:
        name = normalize_host_name(name)
        url = (url or "").strip()
        user = (user or "").strip()
        passwd = passwd or ""
        try:
            inbound = int(inbound)
        except Exception:
            pass
        subscription_url = (subscription_url or None)

        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "INSERT INTO xui_hosts (host_name, host_url, host_username, host_pass, host_inbound_id, subscription_url) VALUES (?, ?, ?, ?, ?, ?)",
                    (name, url, user, passwd, inbound, subscription_url)
                )
            except sqlite3.OperationalError:
                cursor.execute(
                    "INSERT INTO xui_hosts (host_name, host_url, host_username, host_pass, host_inbound_id) VALUES (?, ?, ?, ?, ?)",
                    (name, url, user, passwd, inbound)
                )
            conn.commit()
            logging.info(f"Успешно создан новый хост: {name}")
    except sqlite3.Error as e:
        logging.error(f"Ошибка при создании хоста '{name}': {e}")

def update_host_subscription_url(host_name: str, subscription_url: str | None) -> bool:
    try:
        host_name = normalize_host_name(host_name)
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM xui_hosts WHERE TRIM(host_name) = TRIM(?)", (host_name,))
            exists = cursor.fetchone() is not None
            if not exists:
                logging.warning(f"update_host_subscription_url: хост с именем '{host_name}' не найден (после TRIM)")
                return False

            cursor.execute(
                "UPDATE xui_hosts SET subscription_url = ? WHERE TRIM(host_name) = TRIM(?)",
                (subscription_url, host_name)
            )
            conn.commit()
            return True
    except sqlite3.Error as e:
        logging.error(f"Не удалось обновить subscription_url для хоста '{host_name}': {e}")
        return False

def set_referral_start_bonus_received(user_id: int) -> bool:
    """Пометить, что пользователь получил стартовый бонус за реферальную регистрацию."""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE users SET referral_start_bonus_received = 1 WHERE telegram_id = ?",
                (user_id,)
            )
            conn.commit()
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        logging.error(f"Не удалось пометить получение стартового реферального бонуса для пользователя {user_id}: {e}")
        return False

def update_host_url(host_name: str, new_url: str) -> bool:
    """Обновить URL панели XUI для указанного хоста."""
    try:
        host_name = normalize_host_name(host_name)
        new_url = (new_url or "").strip()
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM xui_hosts WHERE TRIM(host_name) = TRIM(?)", (host_name,))
            if cursor.fetchone() is None:
                logging.warning(f"update_host_url: хост с именем '{host_name}' не найден")
                return False

            cursor.execute(
                "UPDATE xui_hosts SET host_url = ? WHERE TRIM(host_name) = TRIM(?)",
                (new_url, host_name)
            )
            conn.commit()
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        logging.error(f"Не удалось обновить host_url для хоста '{host_name}': {e}")
        return False

def update_host_remnawave_settings(
    host_name: str,
    *,
    remnawave_base_url: str | None = None,
    remnawave_api_token: str | None = None,
    squad_uuid: str | None = None,
) -> bool:
    """Обновить Remnawave-настройки на уровне конкретного хоста.
    Пустые строки превращаются в NULL. Поля, равные None, не изменяются.
    """
    try:
        host_name_n = normalize_host_name(host_name)
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM xui_hosts WHERE TRIM(host_name) = TRIM(?)", (host_name_n,))
            if cursor.fetchone() is None:
                logging.warning(f"update_host_remnawave_settings: хост не найден '{host_name_n}'")
                return False

            sets: list[str] = []
            params: list[Any] = []
            if remnawave_base_url is not None:
                value = (remnawave_base_url or '').strip() or None
                sets.append("remnawave_base_url = ?")
                params.append(value)
            if remnawave_api_token is not None:
                value = (remnawave_api_token or '').strip() or None
                sets.append("remnawave_api_token = ?")
                params.append(value)
            if squad_uuid is not None:
                value = (squad_uuid or '').strip() or None
                sets.append("squad_uuid = ?")
                params.append(value)
            if not sets:
                return True
            params.append(host_name_n)
            sql = f"UPDATE xui_hosts SET {', '.join(sets)} WHERE TRIM(host_name) = TRIM(?)"
            cursor.execute(sql, params)
            conn.commit()
            return True
    except sqlite3.Error as e:
        logging.error(f"Не удалось обновить Remnawave-настройки для хоста '{host_name}': {e}")
        return False

def update_host_name(old_name: str, new_name: str) -> bool:
    """Переименовать хост во всех связанных таблицах (xui_hosts, plans, vpn_keys)."""
    try:
        old_name_n = normalize_host_name(old_name)
        new_name_n = normalize_host_name(new_name)
        if not new_name_n:
            logging.warning("update_host_name: new host name is empty after normalization")
            return False
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM xui_hosts WHERE TRIM(host_name) = TRIM(?)", (old_name_n,))
            if cursor.fetchone() is None:
                logging.warning(f"update_host_name: исходный хост не найден '{old_name_n}'")
                return False
            cursor.execute("SELECT 1 FROM xui_hosts WHERE TRIM(host_name) = TRIM(?)", (new_name_n,))
            exists_target = cursor.fetchone() is not None
            if exists_target and old_name_n.lower() != new_name_n.lower():
                logging.warning(f"update_host_name: целевое имя '{new_name_n}' уже используется")
                return False

            cursor.execute(
                "UPDATE xui_hosts SET host_name = TRIM(?) WHERE TRIM(host_name) = TRIM(?)",
                (new_name_n, old_name_n)
            )
            cursor.execute(
                "UPDATE plans SET host_name = TRIM(?) WHERE TRIM(host_name) = TRIM(?)",
                (new_name_n, old_name_n)
            )
            cursor.execute(
                "UPDATE vpn_keys SET host_name = TRIM(?) WHERE TRIM(host_name) = TRIM(?)",
                (new_name_n, old_name_n)
            )
            conn.commit()
            return True
    except sqlite3.Error as e:
        logging.error(f"Не удалось переименовать хост с '{old_name}' на '{new_name}': {e}")
        return False

def delete_host(host_name: str):
    try:
        host_name = normalize_host_name(host_name)
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM plans WHERE TRIM(host_name) = TRIM(?)", (host_name,))
            cursor.execute("DELETE FROM xui_hosts WHERE TRIM(host_name) = TRIM(?)", (host_name,))
            conn.commit()
            logging.info(f"Хост '{host_name}' и его тарифы успешно удалены.")
    except sqlite3.Error as e:
        logging.error(f"Ошибка удаления хоста '{host_name}': {e}")

def get_host(host_name: str) -> dict | None:
    try:
        host_name = normalize_host_name(host_name)
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM xui_hosts WHERE TRIM(host_name) = TRIM(?)", (host_name,))
            result = cursor.fetchone()
            return dict(result) if result else None
    except sqlite3.Error as e:
        logging.error(f"Ошибка получения хоста '{host_name}': {e}")
        return None

def update_host_ssh_settings(
    host_name: str,
    ssh_host: str | None = None,
    ssh_port: int | None = None,
    ssh_user: str | None = None,
    ssh_password: str | None = None,
    ssh_key_path: str | None = None,
) -> bool:
    """Обновить SSH-параметры для speedtest/maintenance по хосту.
    Переданные None значения очищают соответствующие поля (ставят NULL).
    """
    try:
        host_name_n = normalize_host_name(host_name)
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM xui_hosts WHERE TRIM(host_name) = TRIM(?)", (host_name_n,))
            if cursor.fetchone() is None:
                logging.warning(f"update_host_ssh_settings: хост не найден '{host_name_n}'")
                return False

            cursor.execute(
                """
                UPDATE xui_hosts
                SET ssh_host = ?, ssh_port = ?, ssh_user = ?, ssh_password = ?, ssh_key_path = ?
                WHERE TRIM(host_name) = TRIM(?)
                """,
                (
                    (ssh_host or None),
                    (int(ssh_port) if ssh_port is not None else None),
                    (ssh_user or None),
                    (ssh_password if ssh_password is not None else None),
                    (ssh_key_path or None),
                    host_name_n,
                ),
            )
            conn.commit()
            return True
    except sqlite3.Error as e:
        logging.error(f"Не удалось обновить SSH-настройки для хоста '{host_name}': {e}")
        return False

def delete_key_by_id(key_id: int) -> bool:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM vpn_keys WHERE key_id = ?", (key_id,))
            affected = cursor.rowcount
            conn.commit()
            return affected > 0
    except sqlite3.Error as e:
        logging.error(f"Не удалось удалить ключ по id {key_id}: {e}")
        return False

def update_key_comment(key_id: int, comment: str) -> bool:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE vpn_keys SET comment = ? WHERE key_id = ?", (comment, key_id))
            conn.commit()
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        logging.error(f"Не удалось обновить комментарий ключа для {key_id}: {e}")
        return False

def get_all_hosts() -> list[dict]:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM xui_hosts")
            hosts = cursor.fetchall()

            result = []
            for row in hosts:
                d = dict(row)
                d['host_name'] = normalize_host_name(d.get('host_name'))
                result.append(d)
            return result
    except sqlite3.Error as e:
        logging.error(f"Ошибка получения списка всех хостов: {e}")
        return []

def get_speedtests(host_name: str, limit: int = 20) -> list[dict]:
    """Получить последние результаты спидтестов по хосту (ssh/net), новые сверху."""
    try:
        host_name_n = normalize_host_name(host_name)
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            try:
                limit_int = int(limit)
            except Exception:
                limit_int = 20
            cursor.execute(
                """
                SELECT id, host_name, method, ping_ms, jitter_ms, download_mbps, upload_mbps,
                       server_name, server_id, ok, error, created_at
                FROM host_speedtests
                WHERE TRIM(host_name) = TRIM(?)
                ORDER BY datetime(created_at) DESC
                LIMIT ?
                """,
                (host_name_n, limit_int),
            )
            rows = cursor.fetchall()
            return [dict(r) for r in rows]
    except sqlite3.Error as e:
        logging.error(f"Не удалось получить speedtest-данные для хоста '{host_name}': {e}")
        return []

def get_latest_speedtest(host_name: str) -> dict | None:
    """Получить последний по времени спидтест для хоста."""
    try:
        host_name_n = normalize_host_name(host_name)
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id, host_name, method, ping_ms, jitter_ms, download_mbps, upload_mbps,
                       server_name, server_id, ok, error, created_at
                FROM host_speedtests
                WHERE TRIM(host_name) = TRIM(?)
                ORDER BY datetime(created_at) DESC
                LIMIT 1
                """,
                (host_name_n,),
            )
            row = cursor.fetchone()
            return dict(row) if row else None
    except sqlite3.Error as e:
        logging.error(f"Не удалось получить последний speedtest для хоста '{host_name}': {e}")
        return None

def insert_host_speedtest(
    host_name: str,
    method: str,
    ping_ms: float | None = None,
    jitter_ms: float | None = None,
    download_mbps: float | None = None,
    upload_mbps: float | None = None,
    server_name: str | None = None,
    server_id: str | None = None,
    ok: bool = True,
    error: str | None = None,
) -> bool:
    """Сохранить результат спидтеста в таблицу host_speedtests."""
    try:
        host_name_n = normalize_host_name(host_name)
        method_s = (method or '').strip().lower()
        if method_s not in ('ssh', 'net'):
            method_s = 'ssh'
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''
                INSERT INTO host_speedtests
                (host_name, method, ping_ms, jitter_ms, download_mbps, upload_mbps, server_name, server_id, ok, error)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                '''
                , (
                    host_name_n,
                    method_s,
                    ping_ms,
                    jitter_ms,
                    download_mbps,
                    upload_mbps,
                    server_name,
                    server_id,
                    1 if ok else 0,
                    (error or None)
                )
            )
            conn.commit()
            return True
    except sqlite3.Error as e:
        logging.error(f"Не удалось сохранить запись speedtest для '{host_name}': {e}")
        return False



def _ensure_ssh_targets_table(cursor: sqlite3.Cursor) -> None:
    """Миграция: создать таблицу speedtest_ssh_targets при необходимости и добавить недостающие столбцы."""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS speedtest_ssh_targets (
            target_name TEXT PRIMARY KEY,
            ssh_host TEXT NOT NULL,
            ssh_port INTEGER DEFAULT 22,
            ssh_user TEXT,
            ssh_password TEXT,
            ssh_key_path TEXT,
            description TEXT,
            is_active INTEGER DEFAULT 1,
            sort_order INTEGER DEFAULT 0,
            metadata TEXT
        )
    """)

    extras = {
        "ssh_host": "TEXT",
        "ssh_port": "INTEGER",
        "ssh_user": "TEXT",
        "ssh_password": "TEXT",
        "ssh_key_path": "TEXT",
        "description": "TEXT",
        "is_active": "INTEGER DEFAULT 1",
        "sort_order": "INTEGER DEFAULT 0",
        "metadata": "TEXT",
    }
    for column, definition in extras.items():
        _ensure_table_column(cursor, "speedtest_ssh_targets", column, definition)


def _ensure_gift_tokens_table(cursor: sqlite3.Cursor) -> None:
    """Миграция для таблиц подарочных токенов."""
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS gift_tokens (
            token TEXT PRIMARY KEY,
            host_name TEXT NOT NULL,
            days INTEGER NOT NULL,
            activation_limit INTEGER DEFAULT 1,
            activations_used INTEGER DEFAULT 0,
            expires_at TIMESTAMP,
            created_by INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_claimed_at TIMESTAMP,
            comment TEXT
        )
        """
    )
    _ensure_index(cursor, "idx_gift_tokens_host", "gift_tokens", "host_name")
    _ensure_index(cursor, "idx_gift_tokens_expires", "gift_tokens", "expires_at")
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS gift_token_claims (
            claim_id INTEGER PRIMARY KEY AUTOINCREMENT,
            token TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            key_id INTEGER,
            claimed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(token) REFERENCES gift_tokens(token) ON DELETE CASCADE
        )
        """
    )
    _ensure_index(cursor, "idx_gift_token_claims_token", "gift_token_claims", "token")
    _ensure_index(cursor, "idx_gift_token_claims_user", "gift_token_claims", "user_id")


def _ensure_promo_tables(cursor: sqlite3.Cursor) -> None:
    """Создание таблиц промокодов и истории их использования."""
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS promo_codes (
            code TEXT PRIMARY KEY,
            discount_percent REAL,
            discount_amount REAL,
            usage_limit_total INTEGER,
            usage_limit_per_user INTEGER,
            used_total INTEGER DEFAULT 0,
            valid_from TIMESTAMP,
            valid_until TIMESTAMP,
            is_active INTEGER DEFAULT 1,
            created_by INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            description TEXT
        )
        """
    )
    _ensure_index(cursor, "idx_promo_codes_valid", "promo_codes", "valid_until")
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS promo_code_usages (
            usage_id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            applied_amount REAL,
            order_id TEXT,
            used_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(code) REFERENCES promo_codes(code) ON DELETE CASCADE
        )
        """
    )
    _ensure_index(cursor, "idx_promo_code_usages_code", "promo_code_usages", "code")
    _ensure_index(cursor, "idx_promo_code_usages_user", "promo_code_usages", "user_id")


def get_all_ssh_targets() -> list[dict]:
    """Вернуть все SSH-цели для спидтестов (включая неактивные), сортировка по sort_order, затем по имени."""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM speedtest_ssh_targets ORDER BY sort_order ASC, target_name ASC")
            rows = cursor.fetchall()
            return [dict(r) for r in rows]
    except sqlite3.Error as e:
        logging.error(f"Не удалось получить список SSH-целей: {e}")
        return []


def get_ssh_target(target_name: str) -> dict | None:
    try:
        name = normalize_host_name(target_name)
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM speedtest_ssh_targets WHERE TRIM(target_name) = TRIM(?)", (name,))
            row = cursor.fetchone()
            return dict(row) if row else None
    except sqlite3.Error as e:
        logging.error(f"Не удалось получить SSH-цель '{target_name}': {e}")
        return None


def create_ssh_target(
    target_name: str,
    ssh_host: str,
    ssh_port: int | None = 22,
    ssh_user: str | None = None,
    ssh_password: str | None = None,
    ssh_key_path: str | None = None,
    description: str | None = None,
    *,
    sort_order: int | None = 0,
    is_active: int | None = 1,
) -> bool:
    try:
        name = normalize_host_name(target_name)
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO speedtest_ssh_targets
                    (target_name, ssh_host, ssh_port, ssh_user, ssh_password, ssh_key_path, description, is_active, sort_order)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    name,
                    (ssh_host or '').strip(),
                    int(ssh_port) if ssh_port is not None else None,
                    (ssh_user or None),
                    (ssh_password if ssh_password is not None else None),
                    (ssh_key_path or None),
                    (description or None),
                    1 if (is_active is None or int(is_active) != 0) else 0,
                    int(sort_order or 0),
                )
            )
            conn.commit()
            return True
    except sqlite3.Error as e:
        logging.error(f"Не удалось создать SSH-цель '{target_name}': {e}")
        return False


def update_ssh_target_fields(
    target_name: str,
    *,
    ssh_host: str | None = None,
    ssh_port: int | None = None,
    ssh_user: str | None = None,
    ssh_password: str | None = None,
    ssh_key_path: str | None = None,
    description: str | None = None,
    sort_order: int | None = None,
    is_active: int | None = None,
) -> bool:
    try:
        name = normalize_host_name(target_name)
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM speedtest_ssh_targets WHERE TRIM(target_name) = TRIM(?)", (name,))
            if cursor.fetchone() is None:
                logging.warning(f"update_ssh_target_fields: цель не найдена '{name}'")
                return False
            sets: list[str] = []
            params: list[Any] = []
            if ssh_host is not None:
                sets.append("ssh_host = ?")
                params.append((ssh_host or '').strip())
            if ssh_port is not None:
                try:
                    val = int(ssh_port)
                except Exception:
                    val = None
                sets.append("ssh_port = ?")
                params.append(val)
            if ssh_user is not None:
                sets.append("ssh_user = ?")
                params.append(ssh_user or None)
            if ssh_password is not None:
                sets.append("ssh_password = ?")
                params.append(ssh_password)
            if ssh_key_path is not None:
                sets.append("ssh_key_path = ?")
                params.append(ssh_key_path or None)
            if description is not None:
                sets.append("description = ?")
                params.append(description or None)
            if sort_order is not None:
                try:
                    so = int(sort_order)
                except Exception:
                    so = 0
                sets.append("sort_order = ?")
                params.append(so)
            if is_active is not None:
                sets.append("is_active = ?")
                params.append(1 if int(is_active) != 0 else 0)
            if not sets:
                return True
            params.append(name)
            sql = f"UPDATE speedtest_ssh_targets SET {', '.join(sets)} WHERE TRIM(target_name) = TRIM(?)"
            cursor.execute(sql, params)
            conn.commit()
            return True
    except sqlite3.Error as e:
        logging.error(f"Не удалось обновить SSH-цель '{target_name}': {e}")
        return False


def delete_ssh_target(target_name: str) -> bool:
    try:
        name = normalize_host_name(target_name)
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM speedtest_ssh_targets WHERE TRIM(target_name) = TRIM(?)", (name,))
            affected = cursor.rowcount
            conn.commit()
            return affected > 0
    except sqlite3.Error as e:
        logging.error(f"Не удалось удалить SSH-цель '{target_name}': {e}")
        return False

def get_admin_stats() -> dict:
    """Return aggregated statistics for the admin dashboard.
    Includes:
    - total_users: count of users
    - total_keys: count of all keys
    - active_keys: keys with expire_at in the future
    - total_income: sum of amount_rub for successful transactions
    """
    stats = {
        "total_users": 0,
        "total_keys": 0,
        "active_keys": 0,
        "total_income": 0.0,

        "today_new_users": 0,
        "today_income": 0.0,
        "today_issued_keys": 0,
    }
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()

            cursor.execute("SELECT COUNT(*) FROM users")
            row = cursor.fetchone()
            stats["total_users"] = (row[0] or 0) if row else 0


            cursor.execute("SELECT COUNT(*) FROM vpn_keys")
            row = cursor.fetchone()
            stats["total_keys"] = (row[0] or 0) if row else 0


            cursor.execute("SELECT COUNT(*) FROM vpn_keys WHERE expire_at IS NOT NULL AND datetime(expire_at) > CURRENT_TIMESTAMP")
            row = cursor.fetchone()
            stats["active_keys"] = (row[0] or 0) if row else 0


            cursor.execute(
                """
                SELECT COALESCE(SUM(amount_rub), 0)
                FROM transactions
                WHERE status IN ('paid','success','succeeded')
                  AND LOWER(COALESCE(payment_method, '')) <> 'balance'
                """
            )
            row = cursor.fetchone()
            stats["total_income"] = float(row[0] or 0.0) if row else 0.0



            cursor.execute(
                "SELECT COUNT(*) FROM users WHERE date(registration_date) = date('now')"
            )
            row = cursor.fetchone()
            stats["today_new_users"] = (row[0] or 0) if row else 0


            cursor.execute(
                """
                SELECT COALESCE(SUM(amount_rub), 0)
                FROM transactions
                WHERE status IN ('paid','success','succeeded')
                  AND date(created_date) = date('now')
                  AND LOWER(COALESCE(payment_method, '')) <> 'balance'
                """
            )
            row = cursor.fetchone()
            stats["today_income"] = float(row[0] or 0.0) if row else 0.0


            cursor.execute(
                "SELECT COUNT(*) FROM vpn_keys WHERE date(COALESCE(created_at, updated_at, CURRENT_TIMESTAMP)) = date('now')"
            )
            row = cursor.fetchone()
            stats["today_issued_keys"] = (row[0] or 0) if row else 0
    except sqlite3.Error as e:
        logging.error(f"Failed to get admin stats: {e}")
    return stats

def get_all_keys() -> list[dict]:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM vpn_keys")
            return [_normalize_key_row(row) for row in cursor.fetchall()]
    except sqlite3.Error as e:
        logging.error(f"Failed to get all keys: {e}")
        return []


def get_keys_for_user(user_id: int) -> list[dict]:
    return get_user_keys(user_id)

def update_key_email(key_id: int, new_email: str) -> bool:
    normalized = _normalize_email(new_email) or new_email.strip()
    return update_key_fields(key_id, email=normalized)

def update_key_host(key_id: int, new_host_name: str) -> bool:
    return update_key_fields(key_id, host_name=new_host_name)

def create_gift_key(user_id: int, host_name: str, key_email: str, months: int, remnawave_user_uuid: str | None = None) -> int | None:
    """Создать подарочный ключ: expiry = now + months."""
    try:
        from datetime import timedelta

        months_value = max(1, int(months or 1))
        expiry_dt = datetime.utcnow() + timedelta(days=30 * months_value)
        expiry_ms = int(expiry_dt.timestamp() * 1000)
        uuid_value = remnawave_user_uuid or f"GIFT-{user_id}-{int(datetime.utcnow().timestamp())}"
        return add_new_key(
            user_id=user_id,
            host_name=host_name,
            remnawave_user_uuid=uuid_value,
            key_email=key_email,
            expiry_timestamp_ms=expiry_ms,
        )
    except sqlite3.Error as e:
        logging.error(f"Failed to create gift key for user {user_id}: {e}")
        return None
    except Exception as e:
        logging.error(f"Failed to create gift key for user {user_id}: {e}")
        return None

def get_setting(key: str) -> str | None:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT value FROM bot_settings WHERE key = ?", (key,))
            result = cursor.fetchone()
            return result[0] if result else None
    except sqlite3.Error as e:
        logging.error(f"Failed to get setting '{key}': {e}")
        return None

def get_admin_ids() -> set[int]:
    """Возвращает множество ID администраторов из настроек.
    Поддерживает оба варианта: одиночный 'admin_telegram_id' и список 'admin_telegram_ids'
    через запятую/пробелы или JSON-массив.
    """
    ids: set[int] = set()
    try:
        single = get_setting("admin_telegram_id")
        if single:
            try:
                ids.add(int(single))
            except Exception:
                pass
        multi_raw = get_setting("admin_telegram_ids")
        if multi_raw:
            s = (multi_raw or "").strip()

            try:
                arr = json.loads(s)
                if isinstance(arr, list):
                    for v in arr:
                        try:
                            ids.add(int(v))
                        except Exception:
                            pass
                    return ids
            except Exception:
                pass

            parts = [p for p in re.split(r"[\s,]+", s) if p]
            for p in parts:
                try:
                    ids.add(int(p))
                except Exception:
                    pass
    except Exception as e:
        logging.warning(f"get_admin_ids failed: {e}")
    return ids

def is_admin(user_id: int) -> bool:
    """Проверка прав администратора по списку ID из настроек."""
    try:
        return int(user_id) in get_admin_ids()
    except Exception:
        return False


def create_payload_pending(payment_id: str, user_id: int, amount_rub: float | None, metadata: dict | None) -> bool:
    try:
        print(f"[DEBUG] create_payload_pending called: payment_id={payment_id}, user_id={user_id}, amount_rub={amount_rub}, metadata={metadata}")
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS pending_transactions (
                    payment_id TEXT PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    amount_rub REAL,
                    metadata TEXT,
                    status TEXT DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            cursor.execute(
                '''
                INSERT OR REPLACE INTO pending_transactions (payment_id, user_id, amount_rub, metadata, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, COALESCE((SELECT status FROM pending_transactions WHERE payment_id = ?), 'pending'),
                        COALESCE((SELECT created_at FROM pending_transactions WHERE payment_id = ?), CURRENT_TIMESTAMP), CURRENT_TIMESTAMP)
                '''
                , (payment_id, int(user_id), float(amount_rub) if amount_rub is not None else None, json.dumps(metadata or {}), payment_id, payment_id)
            )
            conn.commit()
            return True
    except sqlite3.Error as e:
        logging.error(f"Failed to create payload pending {payment_id}: {e}")
        return False

def _get_pending_metadata(payment_id: str) -> dict | None:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS pending_transactions (
                    payment_id TEXT PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    amount_rub REAL,
                    metadata TEXT,
                    status TEXT DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            cursor.execute("SELECT * FROM pending_transactions WHERE payment_id = ?", (payment_id,))
            row = cursor.fetchone()
            if not row:
                return None
            try:
                meta = json.loads(row["metadata"] or "{}")
            except Exception:
                meta = {}

            meta.setdefault('payment_id', payment_id)
            return meta
    except sqlite3.Error as e:
        logging.error(f"Failed to read pending transaction {payment_id}: {e}")
        return None


def get_pending_metadata(payment_id: str) -> dict | None:
    """Public wrapper to fetch pending metadata by payment_id WITHOUT marking it paid.
    Returns metadata dict or None if not found.
    """
    return _get_pending_metadata(payment_id)


def get_pending_status(payment_id: str) -> str | None:
    """Return status of pending transaction: 'pending', 'paid', or None if not found."""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS pending_transactions (
                    payment_id TEXT PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    amount_rub REAL,
                    metadata TEXT,
                    status TEXT DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            cursor.execute("SELECT status FROM pending_transactions WHERE payment_id = ?", (payment_id,))
            row = cursor.fetchone()
            if not row:
                return None
            return (row[0] or '').strip() or None
    except sqlite3.Error as e:
        logging.error(f"Failed to get status for pending {payment_id}: {e}")
        return None

def _complete_pending(payment_id: str) -> bool:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE pending_transactions SET status = 'paid', updated_at = CURRENT_TIMESTAMP WHERE payment_id = ?",
                (payment_id,)
            )
            conn.commit()
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        logging.error(f"Failed to complete pending transaction {payment_id}: {e}")
        return False

def find_and_complete_ton_transaction(payment_id: str, amount_ton: float | None = None) -> dict | None:
    """Locate pending transaction by payment_id and mark it paid. Return metadata for processing.
    The amount check is not enforced here; validation should be done on the webhook provider side.
    """
    meta = _get_pending_metadata(payment_id)
    if not meta:
        return None
    _complete_pending(payment_id)
    return meta

def find_and_complete_pending_transaction(payment_id: str) -> dict | None:
    logging.info(f"🔍 Ищем ожидающую транзакцию: {payment_id}")
    meta = _get_pending_metadata(payment_id)
    if not meta:
        logging.warning(f"❌ Ожидающая транзакция не найдена: {payment_id}")
        return None
    
    user_id = meta.get('user_id', 'неизвестно')
    amount = meta.get('price', 0)
    logging.info(f"✅ Найдена ожидающая транзакция: пользователь {user_id}, сумма {amount:.2f} RUB")
    
    success = _complete_pending(payment_id)
    if success:
        logging.info(f"✅ Транзакция отмечена как оплаченная: {payment_id}")
    else:
        logging.error(f"❌ Не удалось отметить транзакцию как оплаченную: {payment_id}")
    return meta

def get_latest_pending_for_user(user_id: int) -> dict | None:
    """Return metadata of the most recent pending transaction for the user (without completing it)."""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS pending_transactions (
                    payment_id TEXT PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    amount_rub REAL,
                    metadata TEXT,
                    status TEXT DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            cursor.execute(
                """
                SELECT payment_id, metadata FROM pending_transactions
                WHERE user_id = ? AND status = 'pending'
                ORDER BY datetime(created_at) DESC, datetime(updated_at) DESC
                LIMIT 1
                """,
                (int(user_id),)
            )
            row = cursor.fetchone()
            if not row:
                return None
            try:
                meta = json.loads(row["metadata"] or "{}")
            except Exception:
                meta = {}
            meta.setdefault('payment_id', row["payment_id"]) 
            return meta
    except sqlite3.Error as e:
        logging.error(f"Failed to read latest pending for user {user_id}: {e}")
        return None
        
def get_referrals_for_user(user_id: int) -> list[dict]:
    """Возвращает список пользователей, которых пригласил данный user_id.
    Поля: telegram_id, username, registration_date, total_spent.
    """
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT telegram_id, username, registration_date, total_spent
                FROM users
                WHERE referred_by = ?
                ORDER BY registration_date DESC
                """,
                (user_id,)
            )
            rows = cursor.fetchall()
            return [dict(r) for r in rows]
    except sqlite3.Error as e:
        logging.error(f"Failed to get referrals for user {user_id}: {e}")
        return []
        
def get_all_settings() -> dict:
    settings = {}
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT key, value FROM bot_settings")
            rows = cursor.fetchall()
            for row in rows:
                settings[row['key']] = row['value']
    except sqlite3.Error as e:
        logging.error(f"Failed to get all settings: {e}")
    return settings

def update_setting(key: str, value: str):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT OR REPLACE INTO bot_settings (key, value) VALUES (?, ?)", (key, value))
            conn.commit()
            logging.info(f"Setting '{key}' updated.")
    except sqlite3.Error as e:
        logging.error(f"Failed to update setting '{key}': {e}")


def get_button_configs(menu_type: str) -> list[dict]:
    """Get all button configurations for a specific menu type"""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM button_configs 
                WHERE menu_type = ? AND is_active = 1 
                ORDER BY sort_order, row_position, column_position
            """, (menu_type,))
            results = [dict(row) for row in cursor.fetchall()]

            return results
    except sqlite3.Error as e:
        logging.error(f"Failed to get button configs for {menu_type}: {e}")
        return []

def get_button_config(menu_type: str, button_id: str) -> dict | None:
    """Get a specific button configuration by menu_type and button_id"""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM button_configs 
                WHERE menu_type = ? AND button_id = ?
            """, (menu_type, button_id))
            row = cursor.fetchone()
            if row:
                return dict(row)
            return None
    except sqlite3.Error as e:
        logging.error(f"Failed to get button config for {menu_type}/{button_id}: {e}")
        return None

def create_button_config(menu_type: str, button_id: str, text: str, callback_data: str = None, 
                        url: str = None, row_position: int = 0, column_position: int = 0, 
                        button_width: int = 1, metadata: str = None) -> bool:
    """Create a new button configuration"""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO button_configs 
                (menu_type, button_id, text, callback_data, url, row_position, column_position, button_width, metadata, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (menu_type, button_id, text, callback_data, url, row_position, column_position, button_width, metadata))
            conn.commit()
            logging.info(f"Button config created: {menu_type}/{button_id}")
            return True
    except sqlite3.Error as e:
        logging.error(f"Failed to create button config: {e}")
        return False

def update_button_config(button_id: int, text: str = None, callback_data: str = None, 
                        url: str = None, row_position: int = None, column_position: int = None, 
                        button_width: int = None, is_active: bool = None, sort_order: int = None, metadata: str = None) -> bool:
    """Update an existing button configuration"""
    try:
        logging.info(f"update_button_config called for {button_id}: text={text}, callback_data={callback_data}, url={url}, row={row_position}, col={column_position}, active={is_active}, sort={sort_order}")
        
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            

            updates = []
            params = []
            
            if text is not None:
                updates.append("text = ?")
                params.append(text)
            if callback_data is not None:
                updates.append("callback_data = ?")
                params.append(callback_data)
            if url is not None:
                updates.append("url = ?")
                params.append(url)
            if row_position is not None:
                updates.append("row_position = ?")
                params.append(row_position)
            if column_position is not None:
                updates.append("column_position = ?")
                params.append(column_position)
            if button_width is not None:
                updates.append("button_width = ?")
                params.append(button_width)
            if is_active is not None:
                updates.append("is_active = ?")
                params.append(1 if is_active else 0)
            if sort_order is not None:
                updates.append("sort_order = ?")
                params.append(sort_order)
            if metadata is not None:
                updates.append("metadata = ?")
                params.append(metadata)
            
            if not updates:
                return True
                
            updates.append("updated_at = CURRENT_TIMESTAMP")
            params.append(button_id)
            
            query = f"UPDATE button_configs SET {', '.join(updates)} WHERE id = ?"
            logging.info(f"Executing query: {query} with params: {params}")
            cursor.execute(query, params)
            
            if cursor.rowcount == 0:
                logging.warning(f"No button found with id {button_id}")
                return False
                
            conn.commit()
            logging.info(f"Button config {button_id} updated successfully")
            return True
    except sqlite3.Error as e:
        logging.error(f"Failed to update button config {button_id}: {e}")
        return False

def delete_button_config(button_id: int) -> bool:
    """Delete a button configuration"""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM button_configs WHERE id = ?", (button_id,))
            conn.commit()
            logging.info(f"Button config {button_id} deleted")
            return True
    except sqlite3.Error as e:
        logging.error(f"Failed to delete button config {button_id}: {e}")
        return False

def update_existing_my_keys_button():
    """Update existing my_keys button to include key count template and set proper button widths"""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()

            cursor.execute("""
                UPDATE button_configs 
                SET text = '🔑 Мои ключи ({len(user_keys)})', updated_at = CURRENT_TIMESTAMP
                WHERE menu_type = 'main_menu' AND button_id = 'my_keys'
            """)
            if cursor.rowcount > 0:
                logging.info("Updated my_keys button text to include key count template")
            

            wide_buttons = [
                ("trial", 2),
                ("referral", 2),
                ("admin", 2),
            ]
            
            for button_id, width in wide_buttons:
                cursor.execute("""
                    UPDATE button_configs 
                    SET button_width = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE menu_type = 'main_menu' AND button_id = ?
                """, (width, button_id))
                if cursor.rowcount > 0:
                    logging.info(f"Updated {button_id} button width to {width}")
            
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Failed to update button configurations: {e}")

def reorder_button_configs(menu_type: str, button_orders: list[dict]) -> bool:
    """Reorder button configurations for a menu type"""
    try:
        logging.info(f"Reordering {len(button_orders)} buttons for {menu_type}")
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            for order_data in button_orders:
                button_id = order_data.get('button_id')
                sort_order = order_data.get('sort_order', 0)
                row_position = order_data.get('row_position', 0)
                column_position = order_data.get('column_position', 0)
                button_width = order_data.get('button_width', None)
                
                logging.info(f"Updating {button_id}: sort={sort_order}, row={row_position}, col={column_position}, width={button_width}")
                

                if button_width is not None:
                    cursor.execute(
                        """
                        UPDATE button_configs 
                        SET sort_order = ?, row_position = ?, column_position = ?, button_width = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE menu_type = ? AND button_id = ?
                        """,
                        (sort_order, row_position, column_position, int(button_width), menu_type, button_id),
                    )
                else:
                    cursor.execute(
                        """
                        UPDATE button_configs 
                        SET sort_order = ?, row_position = ?, column_position = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE menu_type = ? AND button_id = ?
                        """,
                        (sort_order, row_position, column_position, menu_type, button_id),
                    )
                

                if cursor.rowcount == 0:
                    logging.warning(f"No button found with menu_type={menu_type}, button_id={button_id}")
                else:
                    logging.info(f"Updated button {button_id}")
                    
            conn.commit()
            logging.info(f"Button configs reordered for {menu_type}")
            return True
    except sqlite3.Error as e:
        logging.error(f"Failed to reorder button configs for {menu_type}: {e}")
        return False

def initialize_default_button_configs():
    """Initialize default button configurations for all menu types"""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            

            cursor.execute("SELECT COUNT(*) FROM button_configs")
            count = cursor.fetchone()[0]
            if count > 0:
                logging.info("Button configs already exist, skipping initialization")
                return True
            

            main_menu_buttons = [
                ("trial", "🎁 Попробовать бесплатно", "get_trial", 0, 0, 0, 2),
                ("profile", "👤 Мой профиль", "show_profile", 1, 0, 1, 1),
                ("my_keys", "🔑 Мои ключи ({len(user_keys)})", "manage_keys", 1, 1, 2, 1),
                ("buy_key", "🛒 Купить ключ", "buy_new_key", 2, 0, 3, 1),
                ("topup", "💳 Пополнить баланс", "top_up_start", 2, 1, 4, 1),
                ("referral", "🤝 Реферальная программа", "show_referral_program", 3, 0, 5, 2),
                ("support", "🆘 Поддержка", "show_help", 4, 0, 6, 1),
                ("about", "ℹ️ О проекте", "show_about", 4, 1, 7, 1),
                ("speed", "⚡ Скорость", "user_speedtest_last", 5, 0, 8, 1),
                ("howto", "❓ Как использовать", "howto_vless", 5, 1, 9, 1),
                ("admin", "⚙️ Админка", "admin_menu", 6, 0, 10, 2),
            ]
            
            for button_id, text, callback_data, row_pos, col_pos, sort_order, button_width in main_menu_buttons:
                cursor.execute("""
                    INSERT INTO button_configs 
                    (menu_type, button_id, text, callback_data, row_position, column_position, sort_order, button_width, is_active)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
                """, ("main_menu", button_id, text, callback_data, row_pos, col_pos, sort_order, button_width))
            

            admin_menu_buttons = [
                ("users", "👥 Пользователи", "admin_users", 0, 0, 0),
                ("host_keys", "🌍 Ключи на хосте", "admin_host_keys", 0, 1, 1),
                ("gift_key", "🎁 Выдать ключ", "admin_gift_key", 1, 0, 2),
                ("promo", "🎟 Промокоды", "admin_promo_menu", 1, 1, 3),
                ("speedtest", "⚡ Тест скорости", "admin_speedtest", 2, 0, 4),
                ("monitor", "📊 Мониторинг", "admin_monitor", 2, 1, 5),
                ("backup", "🗄 Бэкап БД", "admin_backup_db", 3, 0, 6),
                ("restore", "♻️ Восстановить БД", "admin_restore_db", 3, 1, 7),
                ("admins", "👮 Администраторы", "admin_admins_menu", 4, 0, 8),
                ("broadcast", "📢 Рассылка", "start_broadcast", 4, 1, 9),
                ("back_to_menu", "⬅️ Назад в меню", "back_to_main_menu", 5, 0, 10),
            ]
            
            for button_id, text, callback_data, row_pos, col_pos, sort_order in admin_menu_buttons:
                cursor.execute("""
                    INSERT INTO button_configs 
                    (menu_type, button_id, text, callback_data, row_position, column_position, sort_order, is_active)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 1)
                """, ("admin_menu", button_id, text, callback_data, row_pos, col_pos, sort_order))
            

            profile_menu_buttons = [
                ("topup", "💳 Пополнить баланс", "top_up_start", 0, 0, 0),
                ("referral", "🤝 Реферальная программа", "show_referral_program", 1, 0, 1),
                ("back_to_menu", "⬅️ Назад в меню", "back_to_main_menu", 2, 0, 2),
            ]
            
            for button_id, text, callback_data, row_pos, col_pos, sort_order in profile_menu_buttons:
                cursor.execute("""
                    INSERT INTO button_configs 
                    (menu_type, button_id, text, callback_data, row_position, column_position, sort_order, is_active)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 1)
                """, ("profile_menu", button_id, text, callback_data, row_pos, col_pos, sort_order))
            

            support_menu_buttons = [
                ("new_ticket", "✍️ Новое обращение", "support_new_ticket", 0, 0, 0),
                ("my_tickets", "📨 Мои обращения", "support_my_tickets", 1, 0, 1),
                ("external", "🆘 Внешняя поддержка", "support_external", 2, 0, 2),
                ("back_to_menu", "⬅️ Назад в меню", "back_to_main_menu", 3, 0, 3),
            ]
            
            for button_id, text, callback_data, row_pos, col_pos, sort_order in support_menu_buttons:
                cursor.execute("""
                    INSERT INTO button_configs 
                    (menu_type, button_id, text, callback_data, row_position, column_position, sort_order, is_active)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 1)
                """, ("support_menu", button_id, text, callback_data, row_pos, col_pos, sort_order))
            
            conn.commit()
            logging.info("Default button configurations initialized")
            return True
            
    except sqlite3.Error as e:
        logging.error(f"Failed to initialize default button configs: {e}")
        return False

def create_plan(host_name: str, plan_name: str, months: int, price: float):
    try:
        host_name = normalize_host_name(host_name)
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO plans (host_name, plan_name, months, price) VALUES (?, ?, ?, ?)",
                (host_name, plan_name, months, price)
            )
            conn.commit()
            logging.info(f"Created new plan '{plan_name}' for host '{host_name}'.")
    except sqlite3.Error as e:
        logging.error(f"Failed to create plan for host '{host_name}': {e}")

def get_plans_for_host(host_name: str) -> list[dict]:
    try:
        host_name = normalize_host_name(host_name)
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM plans WHERE TRIM(host_name) = TRIM(?) ORDER BY months", (host_name,))
            plans = cursor.fetchall()
            return [dict(plan) for plan in plans]
    except sqlite3.Error as e:
        logging.error(f"Failed to get plans for host '{host_name}': {e}")
        return []

def get_plan_by_id(plan_id: int) -> dict | None:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM plans WHERE plan_id = ?", (plan_id,))
            plan = cursor.fetchone()
            return dict(plan) if plan else None
    except sqlite3.Error as e:
        logging.error(f"Failed to get plan by id '{plan_id}': {e}")
        return None

def delete_plan(plan_id: int):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM plans WHERE plan_id = ?", (plan_id,))
            conn.commit()
            logging.info(f"Deleted plan with id {plan_id}.")
    except sqlite3.Error as e:
        logging.error(f"Failed to delete plan with id {plan_id}: {e}")

def update_plan(plan_id: int, plan_name: str, months: int, price: float) -> bool:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE plans SET plan_name = ?, months = ?, price = ? WHERE plan_id = ?",
                (plan_name, months, price, plan_id)
            )
            conn.commit()
            if cursor.rowcount == 0:
                logging.warning(f"No plan updated for id {plan_id} (not found).")
                return False
            logging.info(f"Updated plan {plan_id}: name='{plan_name}', months={months}, price={price}.")
            return True
    except sqlite3.Error as e:
        logging.error(f"Failed to update plan {plan_id}: {e}")
        return False

def register_user_if_not_exists(telegram_id: int, username: str, referrer_id):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT referred_by FROM users WHERE telegram_id = ?", (telegram_id,))
            row = cursor.fetchone()
            if not row:

                cursor.execute(
                    "INSERT INTO users (telegram_id, username, registration_date, referred_by) VALUES (?, ?, ?, ?)",
                    (telegram_id, username, datetime.now(), referrer_id)
                )
            else:

                cursor.execute("UPDATE users SET username = ? WHERE telegram_id = ?", (username, telegram_id))
                current_ref = row[0]
                if referrer_id and (current_ref is None or str(current_ref).strip() == "") and int(referrer_id) != int(telegram_id):
                    try:
                        cursor.execute("UPDATE users SET referred_by = ? WHERE telegram_id = ?", (int(referrer_id), telegram_id))
                    except Exception:

                        pass
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Failed to register user {telegram_id}: {e}")

def add_to_referral_balance(user_id: int, amount: float):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET referral_balance = referral_balance + ? WHERE telegram_id = ?", (amount, user_id))
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Failed to add to referral balance for user {user_id}: {e}")

def set_referral_balance(user_id: int, value: float):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET referral_balance = ? WHERE telegram_id = ?", (value, user_id))
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Failed to set referral balance for user {user_id}: {e}")

def set_referral_balance_all(user_id: int, value: float):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET referral_balance_all = ? WHERE telegram_id = ?", (value, user_id))
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Failed to set total referral balance for user {user_id}: {e}")

def add_to_referral_balance_all(user_id: int, amount: float):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE users SET referral_balance_all = referral_balance_all + ? WHERE telegram_id = ?",
                (amount, user_id)
            )
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Failed to add to total referral balance for user {user_id}: {e}")

def get_referral_balance_all(user_id: int) -> float:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT referral_balance_all FROM users WHERE telegram_id = ?", (user_id,))
            row = cursor.fetchone()
            return row[0] if row else 0.0
    except sqlite3.Error as e:
        logging.error(f"Failed to get total referral balance for user {user_id}: {e}")
        return 0.0

def get_referral_balance(user_id: int) -> float:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT referral_balance FROM users WHERE telegram_id = ?", (user_id,))
            result = cursor.fetchone()
            return result[0] if result else 0.0
    except sqlite3.Error as e:
        logging.error(f"Failed to get referral balance for user {user_id}: {e}")
        return 0.0

def get_balance(user_id: int) -> float:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT balance FROM users WHERE telegram_id = ?", (user_id,))
            result = cursor.fetchone()
            return result[0] if result else 0.0
    except sqlite3.Error as e:
        logging.error(f"Failed to get balance for user {user_id}: {e}")
        return 0.0

def adjust_user_balance(user_id: int, delta: float) -> bool:
    """Скорректировать баланс пользователя на указанную дельту (может быть отрицательной)."""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET balance = COALESCE(balance, 0) + ? WHERE telegram_id = ?", (float(delta), user_id))
            conn.commit()
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        logging.error(f"Failed to adjust balance for user {user_id}: {e}")
        return False

def set_balance(user_id: int, value: float) -> bool:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET balance = ? WHERE telegram_id = ?", (value, user_id))
            conn.commit()
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        logging.error(f"Failed to set balance for user {user_id}: {e}")
        return False

def add_to_balance(user_id: int, amount: float) -> bool:
    try:
        logging.info(f"💳 Добавляем {amount:.2f} RUB к балансу пользователя {user_id}")
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()

            cursor.execute("SELECT telegram_id, balance FROM users WHERE telegram_id = ?", (int(user_id),))
            user_row = cursor.fetchone()
            if not user_row:
                logging.error(f"❌ Пользователь {user_id} не найден в базе данных")
                return False
            
            old_balance = user_row[1] or 0.0
            cursor.execute(
                "UPDATE users SET balance = COALESCE(balance, 0) + ? WHERE telegram_id = ?",
                (float(amount), int(user_id))
            )
            conn.commit()
            success = cursor.rowcount > 0
            if success:
                new_balance = old_balance + float(amount)
                logging.info(f"✅ Баланс обновлен: пользователь {user_id} | {old_balance:.2f} → {new_balance:.2f} RUB (+{amount:.2f})")
            else:
                logging.error(f"❌ Не удалось обновить баланс для пользователя {user_id}: строки не затронуты")
            return success
    except sqlite3.Error as e:
        logging.error(f"💥 Ошибка базы данных при пополнении баланса для пользователя {user_id}: {e}")
        return False

def deduct_from_balance(user_id: int, amount: float) -> bool:
    """Атомарное списание с основного баланса при достаточности средств."""
    if amount <= 0:
        return True
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("BEGIN IMMEDIATE")
            cursor.execute("SELECT balance FROM users WHERE telegram_id = ?", (user_id,))
            row = cursor.fetchone()
            current = row[0] if row and row[0] is not None else 0.0
            if current < amount:
                conn.rollback()
                return False
            cursor.execute(
                "UPDATE users SET balance = COALESCE(balance, 0) - ? WHERE telegram_id = ?",
                (float(amount), int(user_id))
            )
            conn.commit()
            return True
    except sqlite3.Error as e:
        logging.error(f"Failed to deduct from balance for user {user_id}: {e}")
        return False

def deduct_from_referral_balance(user_id: int, amount: float) -> bool:
    """Атомарное списание с реферального баланса при достаточности средств."""
    if amount <= 0:
        return True
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("BEGIN IMMEDIATE")
            cursor.execute("SELECT referral_balance FROM users WHERE telegram_id = ?", (user_id,))
            row = cursor.fetchone()
            current = row[0] if row else 0.0
            if current < amount:
                conn.rollback()
                return False
            cursor.execute("UPDATE users SET referral_balance = referral_balance - ? WHERE telegram_id = ?", (amount, user_id))
            conn.commit()
            return True
    except sqlite3.Error as e:
        logging.error(f"Failed to deduct from referral balance for user {user_id}: {e}")
        return False

def get_referral_count(user_id: int) -> int:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM users WHERE referred_by = ?", (user_id,))
            return cursor.fetchone()[0] or 0
    except sqlite3.Error as e:
        logging.error(f"Failed to get referral count for user {user_id}: {e}")
        return 0

def get_user(telegram_id: int):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM users WHERE telegram_id = ?", (telegram_id,))
            user_data = cursor.fetchone()
            return dict(user_data) if user_data else None
    except sqlite3.Error as e:
        logging.error(f"Failed to get user {telegram_id}: {e}")
        return None

def set_terms_agreed(telegram_id: int):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET agreed_to_terms = 1 WHERE telegram_id = ?", (telegram_id,))
            conn.commit()
            logging.info(f"Пользователь {telegram_id} согласился с условиями.")
    except sqlite3.Error as e:
        logging.error(f"Failed to set terms agreed for user {telegram_id}: {e}")

def update_user_stats(telegram_id: int, amount_spent: float, months_purchased: int):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET total_spent = total_spent + ?, total_months = total_months + ? WHERE telegram_id = ?", (amount_spent, months_purchased, telegram_id))
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Failed to update user stats for {telegram_id}: {e}")

def get_user_count() -> int:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM users")
            return cursor.fetchone()[0] or 0
    except sqlite3.Error as e:
        logging.error(f"Failed to get user count: {e}")
        return 0

def get_total_keys_count() -> int:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM vpn_keys")
            return cursor.fetchone()[0] or 0
    except sqlite3.Error as e:
        logging.error(f"Failed to get total keys count: {e}")
        return 0

def get_total_spent_sum() -> float:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT COALESCE(SUM(amount_rub), 0.0)
                FROM transactions
                WHERE LOWER(COALESCE(status, '')) IN ('paid', 'completed', 'success')
                  AND LOWER(COALESCE(payment_method, '')) <> 'balance'
                """
            )
            val = cursor.fetchone()
            return (val[0] if val else 0.0) or 0.0
    except sqlite3.Error as e:
        logging.error(f"Failed to get total spent sum: {e}")
        return 0.0

def create_pending_transaction(payment_id: str, user_id: int, amount_rub: float, metadata: dict) -> int:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO transactions (payment_id, user_id, status, amount_rub, metadata) VALUES (?, ?, ?, ?, ?)",
                (payment_id, user_id, 'pending', amount_rub, json.dumps(metadata))
            )
            conn.commit()
            return cursor.lastrowid
    except sqlite3.Error as e:
        logging.error(f"Failed to create pending transaction: {e}")
        return 0

def find_and_complete_ton_transaction(payment_id: str, amount_ton: float) -> dict | None:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute("SELECT * FROM transactions WHERE payment_id = ? AND status = 'pending'", (payment_id,))
            transaction = cursor.fetchone()
            if not transaction:
                logger.warning(f"TON Webhook: Получен платеж для неизвестного или уже обработанного payment_id: {payment_id}")
                return None
            
            
            cursor.execute(
                "UPDATE transactions SET status = 'paid', amount_currency = ?, currency_name = 'TON', payment_method = 'TON' WHERE payment_id = ?",
                (amount_ton, payment_id)
            )
            conn.commit()
            
            return json.loads(transaction['metadata'])
    except sqlite3.Error as e:
        logging.error(f"Failed to complete TON transaction {payment_id}: {e}")
        return None

def log_transaction(username: str, transaction_id: str | None, payment_id: str | None, user_id: int, status: str, amount_rub: float, amount_currency: float | None, currency_name: str | None, payment_method: str, metadata: str):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """INSERT INTO transactions
                   (username, transaction_id, payment_id, user_id, status, amount_rub, amount_currency, currency_name, payment_method, metadata, created_date)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (username, transaction_id, payment_id, user_id, status, amount_rub, amount_currency, currency_name, payment_method, metadata, datetime.now())
            )
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Failed to log transaction for user {user_id}: {e}")

def get_paginated_transactions(page: int = 1, per_page: int = 15) -> tuple[list[dict], int]:
    offset = (page - 1) * per_page
    transactions = []
    total = 0
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM transactions")
            total = cursor.fetchone()[0]

            query = "SELECT * FROM transactions ORDER BY created_date DESC LIMIT ? OFFSET ?"
            cursor.execute(query, (per_page, offset))
            
            for row in cursor.fetchall():
                transaction_dict = dict(row)
                
                metadata_str = transaction_dict.get('metadata')
                if metadata_str:
                    try:
                        metadata = json.loads(metadata_str)
                        transaction_dict['host_name'] = metadata.get('host_name', 'N/A')
                        transaction_dict['plan_name'] = metadata.get('plan_name', 'N/A')
                    except json.JSONDecodeError:
                        transaction_dict['host_name'] = 'Error'
                        transaction_dict['plan_name'] = 'Error'
                else:
                    transaction_dict['host_name'] = 'N/A'
                    transaction_dict['plan_name'] = 'N/A'
                
                transactions.append(transaction_dict)
            
    except sqlite3.Error as e:
        logging.error(f"Failed to get paginated transactions: {e}")
    
    return transactions, total

def set_trial_used(telegram_id: int):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET trial_used = 1 WHERE telegram_id = ?", (telegram_id,))
            conn.commit()
            logging.info(f"Trial period marked as used for user {telegram_id}.")
    except sqlite3.Error as e:
        logging.error(f"Failed to set trial used for user {telegram_id}: {e}")

def add_new_key(
    user_id: int,
    host_name: str | None,
    remnawave_user_uuid: str,
    key_email: str,
    expiry_timestamp_ms: int,
    *,
    squad_uuid: str | None = None,
    short_uuid: str | None = None,
    subscription_url: str | None = None,
    traffic_limit_bytes: int | None = None,
    traffic_limit_strategy: str | None = None,
    description: str | None = None,
    tag: str | None = None,
) -> int | None:
    host_name_norm = normalize_host_name(host_name) if host_name else None
    email_normalized = _normalize_email(key_email) or key_email.strip()
    expire_str = _to_datetime_str(expiry_timestamp_ms) or _now_str()
    created_str = _now_str()
    strategy_value = traffic_limit_strategy or "NO_RESET"
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO vpn_keys (
                    user_id,
                    host_name,
                    squad_uuid,
                    remnawave_user_uuid,
                    short_uuid,
                    email,
                    key_email,
                    subscription_url,
                    expire_at,
                    created_at,
                    updated_at,
                    traffic_limit_bytes,
                    traffic_limit_strategy,
                    tag,
                    description
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    user_id,
                    host_name_norm,
                    squad_uuid,
                    remnawave_user_uuid,
                    short_uuid,
                    email_normalized,
                    email_normalized,
                    subscription_url,
                    expire_str,
                    created_str,
                    created_str,
                    traffic_limit_bytes,
                    strategy_value,
                    tag,
                    description,
                ),
            )
            conn.commit()
            return cursor.lastrowid
    except sqlite3.IntegrityError as e:
        logging.error(
            "Failed to add new key for user %s due to integrity error: %s",
            user_id,
            e,
        )
        return None
    except sqlite3.Error as e:
        logging.error("Failed to add new key for user %s: %s", user_id, e)
        return None


def _apply_key_updates(key_id: int, updates: dict[str, Any]) -> bool:
    if not updates:
        return False
    updates = dict(updates)
    updates["updated_at"] = _now_str()
    columns = ", ".join(f"{column} = ?" for column in updates)
    values = list(updates.values())
    values.append(key_id)
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"UPDATE vpn_keys SET {columns} WHERE key_id = ?",
                tuple(values),
            )
            conn.commit()
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        logging.error("Failed to update key %s: %s", key_id, e)
        return False


def update_key_fields(
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
    updates: dict[str, Any] = {}
    if host_name is not None:
        updates["host_name"] = normalize_host_name(host_name)
    if squad_uuid is not None:
        updates["squad_uuid"] = squad_uuid
    if remnawave_user_uuid is not None:
        updates["remnawave_user_uuid"] = remnawave_user_uuid
    if short_uuid is not None:
        updates["short_uuid"] = short_uuid
    if email is not None:
        normalized = _normalize_email(email) or email.strip()
        updates["email"] = normalized
        updates["key_email"] = normalized
    if subscription_url is not None:
        updates["subscription_url"] = subscription_url
    if expire_at_ms is not None:
        expire_str = _to_datetime_str(expire_at_ms) or _now_str()
        updates["expire_at"] = expire_str
    if traffic_limit_bytes is not None:
        updates["traffic_limit_bytes"] = traffic_limit_bytes
    if traffic_limit_strategy is not None:
        updates["traffic_limit_strategy"] = traffic_limit_strategy or "NO_RESET"
    if tag is not None:
        updates["tag"] = tag
    if description is not None:
        updates["description"] = description
    return _apply_key_updates(key_id, updates)


def delete_key_by_email(email: str) -> bool:
    lookup = _normalize_email(email) or email.strip()
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM vpn_keys WHERE email = ? OR key_email = ?",
                (lookup, lookup),
            )
            affected = cursor.rowcount
            conn.commit()
            logger.debug("delete_key_by_email('%s') affected=%s", email, affected)
            return affected > 0
    except sqlite3.Error as e:
        logging.error("Failed to delete key '%s': %s", email, e)
        return False


def get_user_keys(user_id: int) -> list[dict]:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM vpn_keys WHERE user_id = ? ORDER BY datetime(created_at) DESC, key_id DESC",
                (user_id,),
            )
            rows = cursor.fetchall()
            return [_normalize_key_row(row) for row in rows]
    except sqlite3.Error as e:
        logging.error("Failed to get keys for user %s: %s", user_id, e)
        return []


def get_key_by_id(key_id: int) -> dict | None:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM vpn_keys WHERE key_id = ?", (key_id,))
            row = cursor.fetchone()
            return _normalize_key_row(row)
    except sqlite3.Error as e:
        logging.error("Failed to get key by ID %s: %s", key_id, e)
        return None


def get_key_by_email(key_email: str) -> dict | None:
    lookup = _normalize_email(key_email) or key_email.strip()
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM vpn_keys WHERE email = ? OR key_email = ?",
                (lookup, lookup),
            )
            row = cursor.fetchone()
            return _normalize_key_row(row)
    except sqlite3.Error as e:
        logging.error("Failed to get key by email %s: %s", key_email, e)
        return None


def get_key_by_remnawave_uuid(remnawave_uuid: str) -> dict | None:
    if not remnawave_uuid:
        return None
    try:
        normalized_uuid = remnawave_uuid.strip()
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM vpn_keys WHERE remnawave_user_uuid = ? LIMIT 1",
                (normalized_uuid,),
            )
            row = cursor.fetchone()
            return _normalize_key_row(row)
    except sqlite3.Error as e:
        logging.error("Failed to get key by remnawave uuid %s: %s", remnawave_uuid, e)
        return None


def update_key_info(key_id: int, new_remnawave_uuid: str, new_expiry_ms: int, **kwargs) -> bool:
    return update_key_fields(
        key_id,
        remnawave_user_uuid=new_remnawave_uuid,
        expire_at_ms=new_expiry_ms,
        **kwargs,
    )


def update_key_host_and_info(
    key_id: int,
    new_host_name: str,
    new_remnawave_uuid: str,
    new_expiry_ms: int,
    **kwargs,
) -> bool:
    return update_key_fields(
        key_id,
        host_name=new_host_name,
        remnawave_user_uuid=new_remnawave_uuid,
        expire_at_ms=new_expiry_ms,
        **kwargs,
    )


def get_next_key_number(user_id: int) -> int:
    return len(get_user_keys(user_id)) + 1


def get_keys_for_host(host_name: str) -> list[dict]:
    try:
        host_name_normalized = normalize_host_name(host_name)
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM vpn_keys WHERE TRIM(host_name) = TRIM(?)",
                (host_name_normalized,),
            )
            rows = cursor.fetchall()
            return [_normalize_key_row(row) for row in rows]
    except sqlite3.Error as e:
        logging.error("Failed to get keys for host '%s': %s", host_name, e)
        return []


def get_all_vpn_users() -> list[dict]:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT user_id FROM vpn_keys")
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
    except sqlite3.Error as e:
        logging.error("Failed to get all vpn users: %s", e)
        return []


def update_key_status_from_server(key_email: str, client_data) -> bool:
    try:
        normalized_email = _normalize_email(key_email) or key_email.strip()
        existing = get_key_by_email(normalized_email)
        if client_data:
            if isinstance(client_data, dict):
                remote_uuid = client_data.get('uuid') or client_data.get('id')
                expire_value = client_data.get('expireAt') or client_data.get('expiryDate')
                subscription_url = client_data.get('subscriptionUrl') or client_data.get('subscription_url')
                expiry_ms = None
                if expire_value:
                    try:
                        remote_dt = datetime.fromisoformat(str(expire_value).replace('Z', '+00:00'))
                        expiry_ms = int(remote_dt.timestamp() * 1000)
                    except Exception:
                        expiry_ms = None
            else:
                remote_uuid = getattr(client_data, 'id', None) or getattr(client_data, 'uuid', None)
                expiry_ms = getattr(client_data, 'expiry_time', None)
                subscription_url = getattr(client_data, 'subscription_url', None)
            if not existing:
                return False
            return update_key_fields(
                existing['key_id'],
                remnawave_user_uuid=remote_uuid,
                expire_at_ms=expiry_ms,
                subscription_url=subscription_url,
            )
        if existing:
            return delete_key_by_email(normalized_email)
        return True
    except sqlite3.Error as e:
        logging.error("Failed to update key status for %s: %s", key_email, e)
        return False


def get_daily_stats_for_charts(days: int = 30) -> dict:
    stats = {'users': {}, 'keys': {}}
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT date(registration_date) AS day, COUNT(*)
                FROM users
                WHERE registration_date >= date('now', ?)
                GROUP BY day
                ORDER BY day
                """,
                (f'-{days} days',),
            )
            for day, count in cursor.fetchall():
                stats['users'][day] = count

            cursor.execute(
                """
                SELECT date(COALESCE(created_at, updated_at, CURRENT_TIMESTAMP)) AS day, COUNT(*)
                FROM vpn_keys
                WHERE COALESCE(created_at, updated_at, CURRENT_TIMESTAMP) >= date('now', ?)
                GROUP BY day
                ORDER BY day
                """,
                (f'-{days} days',),
            )
            for day, count in cursor.fetchall():
                stats['keys'][day] = count
    except sqlite3.Error as e:
        logging.error("Failed to get daily stats for charts: %s", e)
    return stats


def get_recent_transactions(limit: int = 15) -> list[dict]:
    transactions: list[dict] = []
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT
                    k.key_id,
                    k.host_name,
                    k.created_at,
                    u.telegram_id,
                    u.username
                FROM vpn_keys k
                JOIN users u ON k.user_id = u.telegram_id
                ORDER BY datetime(k.created_at) DESC, k.key_id DESC
                LIMIT ?
                """,
                (limit,),
            )
            for row in cursor.fetchall():
                transactions.append(
                    {
                        "key_id": row["key_id"],
                        "host_name": row["host_name"],
                        "created_at": row["created_at"],
                        "telegram_id": row["telegram_id"],
                        "username": row["username"],
                    }
                )
    except sqlite3.Error as e:
        logging.error("Failed to get recent transactions: %s", e)
    return transactions


def get_all_users() -> list[dict]:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM users ORDER BY registration_date DESC")
            return [dict(row) for row in cursor.fetchall()]
    except sqlite3.Error as e:
        logging.error(f"Failed to get all users: {e}")
        return []

def get_users_paginated(page: int = 1, per_page: int = 30, q: str | None = None) -> tuple[list[dict], int]:
    """Вернуть пользователей постранично и общее количество (с учётом фильтра).

    Фильтр q ищет по username (LIKE) и по текстовому представлению telegram_id.
    """
    page = max(1, int(page or 1))
    per_page = max(1, int(per_page or 30))
    offset = (page - 1) * per_page
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            if q:
                q_like = f"%{q.strip()}%"

                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM users
                    WHERE (username LIKE ?)
                       OR (CAST(telegram_id AS TEXT) LIKE ?)
                    """,
                    (q_like, q_like),
                )
                total = cursor.fetchone()[0] or 0

                cursor.execute(
                    """
                    SELECT *
                    FROM users
                    WHERE (username LIKE ?)
                       OR (CAST(telegram_id AS TEXT) LIKE ?)
                    ORDER BY registration_date DESC
                    LIMIT ? OFFSET ?
                    """,
                    (q_like, q_like, per_page, offset),
                )
            else:
                cursor.execute("SELECT COUNT(*) FROM users")
                total = cursor.fetchone()[0] or 0
                cursor.execute(
                    "SELECT * FROM users ORDER BY registration_date DESC LIMIT ? OFFSET ?",
                    (per_page, offset),
                )
            users = [dict(row) for row in cursor.fetchall()]
            return users, total
    except sqlite3.Error as e:
        logging.error(f"Failed to get users paginated: {e}")
        return [], 0

def get_keys_counts_for_users(user_ids: list[int]) -> dict[int, int]:
    """Вернуть словарь {user_id: keys_count} по списку пользователей."""
    result: dict[int, int] = {}
    if not user_ids:
        return result

    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            placeholders = ",".join(["?"] * len(user_ids))
            query = f"SELECT user_id, COUNT(*) AS cnt FROM vpn_keys WHERE user_id IN ({placeholders}) GROUP BY user_id"
            cursor.execute(query, tuple(int(x) for x in user_ids))
            for row in cursor.fetchall() or []:
                uid = int(row[0])
                cnt = int(row[1] or 0)
                result[uid] = cnt
    except sqlite3.Error as e:
        logging.error("Failed to get keys counts for users: %s", e)
    return result

def ban_user(telegram_id: int):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET is_banned = 1 WHERE telegram_id = ?", (telegram_id,))
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Failed to ban user {telegram_id}: {e}")

def unban_user(telegram_id: int):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET is_banned = 0 WHERE telegram_id = ?", (telegram_id,))
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Failed to unban user {telegram_id}: {e}")

def delete_user_keys(user_id: int):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM vpn_keys WHERE user_id = ?", (user_id,))
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Failed to delete keys for user {user_id}: {e}")

def create_support_ticket(user_id: int, subject: str | None = None) -> int | None:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()

            try:
                cursor.execute(
                    "SELECT ticket_id FROM support_tickets WHERE user_id = ? AND status = 'open' ORDER BY updated_at DESC LIMIT 1",
                    (user_id,)
                )
                row = cursor.fetchone()
                if row and row[0]:
                    return int(row[0])
            except Exception:
                pass

            cursor.execute(
                "INSERT INTO support_tickets (user_id, subject) VALUES (?, ?)",
                (user_id, subject)
            )
            conn.commit()
            return cursor.lastrowid
    except sqlite3.Error as e:
        logging.error(f"Failed to create support ticket for user {user_id}: {e}")
        return None

def get_or_create_open_ticket(user_id: int, subject: str | None = None) -> tuple[int | None, bool]:
    """Возвращает ID открытого тикета пользователя и флаг, создан ли новый.
    Если открытого тикета нет — создаёт новый и возвращает (id, True).
    """
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()

            cursor.execute(
                "SELECT ticket_id FROM support_tickets WHERE user_id = ? AND status = 'open' ORDER BY updated_at DESC LIMIT 1",
                (user_id,)
            )
            row = cursor.fetchone()
            if row and row[0]:
                return int(row[0]), False

            cursor.execute(
                "INSERT INTO support_tickets (user_id, subject) VALUES (?, ?)",
                (user_id, subject)
            )
            conn.commit()
            return int(cursor.lastrowid), True
    except sqlite3.Error as e:
        logging.error(f"Failed to get_or_create_open_ticket for user {user_id}: {e}")
        return None, False

def add_support_message(ticket_id: int, sender: str, content: str) -> int | None:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO support_messages (ticket_id, sender, content) VALUES (?, ?, ?)",
                (ticket_id, sender, content)
            )
            cursor.execute(
                "UPDATE support_tickets SET updated_at = CURRENT_TIMESTAMP WHERE ticket_id = ?",
                (ticket_id,)
            )
            conn.commit()
            return cursor.lastrowid
    except sqlite3.Error as e:
        logging.error(f"Failed to add support message to ticket {ticket_id}: {e}")
        return None

def update_ticket_thread_info(ticket_id: int, forum_chat_id: str | None, message_thread_id: int | None) -> bool:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE support_tickets SET forum_chat_id = ?, message_thread_id = ?, updated_at = CURRENT_TIMESTAMP WHERE ticket_id = ?",
                (forum_chat_id, message_thread_id, ticket_id)
            )
            conn.commit()
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        logging.error(f"Failed to update thread info for ticket {ticket_id}: {e}")
        return False

def get_ticket(ticket_id: int) -> dict | None:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM support_tickets WHERE ticket_id = ?", (ticket_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
    except sqlite3.Error as e:
        logging.error(f"Failed to get ticket {ticket_id}: {e}")
        return None

def get_ticket_by_thread(forum_chat_id: str, message_thread_id: int) -> dict | None:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM support_tickets WHERE forum_chat_id = ? AND message_thread_id = ?",
                (str(forum_chat_id), int(message_thread_id))
            )
            row = cursor.fetchone()
            return dict(row) if row else None
    except sqlite3.Error as e:
        logging.error(f"Failed to get ticket by thread {forum_chat_id}/{message_thread_id}: {e}")
        return None

def get_user_tickets(user_id: int, status: str | None = None) -> list[dict]:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            if status:
                cursor.execute(
                    "SELECT * FROM support_tickets WHERE user_id = ? AND status = ? ORDER BY updated_at DESC",
                    (user_id, status)
                )
            else:
                cursor.execute(
                    "SELECT * FROM support_tickets WHERE user_id = ? ORDER BY updated_at DESC",
                    (user_id,)
                )
            return [dict(r) for r in cursor.fetchall()]
    except sqlite3.Error as e:
        logging.error(f"Failed to get tickets for user {user_id}: {e}")
        return []

def get_ticket_messages(ticket_id: int) -> list[dict]:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM support_messages WHERE ticket_id = ? ORDER BY created_at ASC",
                (ticket_id,)
            )
            return [dict(r) for r in cursor.fetchall()]
    except sqlite3.Error as e:
        logging.error(f"Failed to get messages for ticket {ticket_id}: {e}")
        return []

def set_ticket_status(ticket_id: int, status: str) -> bool:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE support_tickets SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE ticket_id = ?",
                (status, ticket_id)
            )
            conn.commit()
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        logging.error(f"Failed to set status '{status}' for ticket {ticket_id}: {e}")
        return False

def update_ticket_subject(ticket_id: int, subject: str) -> bool:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE support_tickets SET subject = ?, updated_at = CURRENT_TIMESTAMP WHERE ticket_id = ?",
                (subject, ticket_id)
            )
            conn.commit()
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        logging.error(f"Failed to update subject for ticket {ticket_id}: {e}")
        return False

def delete_ticket(ticket_id: int) -> bool:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM support_messages WHERE ticket_id = ?",
                (ticket_id,)
            )
            cursor.execute(
                "DELETE FROM support_tickets WHERE ticket_id = ?",
                (ticket_id,)
            )
            conn.commit()
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        logging.error(f"Failed to delete ticket {ticket_id}: {e}")
        return False

def get_tickets_paginated(page: int = 1, per_page: int = 20, status: str | None = None) -> tuple[list[dict], int]:
    offset = (page - 1) * per_page
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            if status:
                cursor.execute("SELECT COUNT(*) FROM support_tickets WHERE status = ?", (status,))
                total = cursor.fetchone()[0] or 0
                cursor.execute(
                    "SELECT * FROM support_tickets WHERE status = ? ORDER BY updated_at DESC LIMIT ? OFFSET ?",
                    (status, per_page, offset)
                )
            else:
                cursor.execute("SELECT COUNT(*) FROM support_tickets")
                total = cursor.fetchone()[0] or 0
                cursor.execute(
                    "SELECT * FROM support_tickets ORDER BY updated_at DESC LIMIT ? OFFSET ?",
                    (per_page, offset)
                )
            return [dict(r) for r in cursor.fetchall()], total
    except sqlite3.Error as e:
        logging.error("Failed to get paginated support tickets: %s", e)
        return [], 0

def get_open_tickets_count() -> int:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM support_tickets WHERE status = 'open'")
            return cursor.fetchone()[0] or 0
    except sqlite3.Error as e:
        logging.error("Failed to get open tickets count: %s", e)
        return 0

def get_closed_tickets_count() -> int:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM support_tickets WHERE status = 'closed'")
            return cursor.fetchone()[0] or 0
    except sqlite3.Error as e:
        logging.error("Failed to get closed tickets count: %s", e)
        return 0

def get_all_tickets_count() -> int:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM support_tickets")
            return cursor.fetchone()[0] or 0
    except sqlite3.Error as e:
        logging.error("Failed to get all tickets count: %s", e)
        return 0



