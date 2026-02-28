import sqlite3
from datetime import datetime, timezone, timedelta
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


# ===== GET_MSK_TIME =====
def get_msk_time() -> datetime:
    return datetime.now(timezone(timedelta(hours=3)))
# ========================


# ===== _NOW_STR =====
def _now_str() -> str:
    return get_msk_time().strftime("%Y-%m-%d %H:%M:%S")
# ======================


# ===== _TO_DATETIME_STR =====
def _to_datetime_str(ts_ms: int | None) -> str | None:
    if ts_ms is None:
        return None
    try:
        dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).astimezone(get_msk_time().tzinfo)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None
# ============================


# ===== _NORMALIZE_EMAIL =====
def _normalize_email(value: str | None) -> str | None:
    if value is None: return None
    cleaned = value.strip().lower(); return cleaned or None
# ============================


# ===== _NORMALIZE_KEY_ROW =====
def _normalize_key_row(row: sqlite3.Row | dict | None) -> dict | None:
    if row is None: return None
    data = dict(row)
    email = _normalize_email(data.get("email") or data.get("key_email"))
    if email: data["email"] = email; data["key_email"] = email
    rem_uuid = data.get("remnawave_user_uuid") or data.get("xui_client_uuid")
    if rem_uuid: data["remnawave_user_uuid"] = rem_uuid; data["xui_client_uuid"] = rem_uuid
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
    if subscription_url: data["subscription_url"] = subscription_url; data.setdefault("connection_string", subscription_url)
    return data
# ==============================


# ===== _GET_TABLE_COLUMNS =====
def _get_table_columns(cursor: sqlite3.Cursor, table: str) -> set[str]:
    cursor.execute(f"PRAGMA table_info({table})"); return {row[1] for row in cursor.fetchall()}
# ==============================


# ===== _ENSURE_TABLE_COLUMN =====
def _ensure_table_column(cursor: sqlite3.Cursor, table: str, column: str, definition: str) -> None:
    columns = _get_table_columns(cursor, table)
    if column not in columns: cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
# ================================


# ===== _ENSURE_UNIQUE_INDEX =====
def _ensure_unique_index(cursor: sqlite3.Cursor, name: str, table: str, column: str) -> None:
    cursor.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS {name} ON {table}({column})")
# ================================


# ===== _ENSURE_INDEX =====
def _ensure_index(cursor: sqlite3.Cursor, name: str, table: str, column: str) -> None:
    cursor.execute(f"CREATE INDEX IF NOT EXISTS {name} ON {table}({column})")
# ===========================


# ===== NORMALIZE_HOST_NAME =====
def normalize_host_name(name: str | None) -> str:
    s = (name or "").strip()
    for ch in ("\u00A0", "\u200B", "\u200C", "\u200D", "\uFEFF"): s = s.replace(ch, "")
    return s
# ===============================


# ===== GET_DB_CONNECTION =====
def get_db_connection():
    conn = sqlite3.connect(DB_FILE, timeout=30.0)
    wal_enabled = False
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM bot_settings WHERE key='enable_wal_mode'")
        row = cursor.fetchone()
        if row and row[0] == '1':
            wal_enabled = True
    except Exception:
        pass
    if wal_enabled:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
    else:
        conn.execute("PRAGMA journal_mode=DELETE")
    return conn
# ==============================





class DbExecResult:
    def __init__(self, cursor: sqlite3.Cursor):
        self.lastrowid = cursor.lastrowid; self.rowcount = cursor.rowcount

# ===== _EXEC =====
def _exec(sql: str, params: tuple | list = (), error_msg: str = "", commit: bool = True) -> DbExecResult | None:
    try:
        with sqlite3.connect(DB_FILE, timeout=30.0) as conn:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            if commit: conn.commit()
            return DbExecResult(cursor)
    except sqlite3.Error as e:
        if error_msg: logging.error(f"{error_msg}: {e}")
        return None
# =================


# ===== _FETCH_ROW =====
def _fetch_row(sql: str, params: tuple | list = (), error_msg: str = "") -> dict | None:
    try:
        with sqlite3.connect(DB_FILE, timeout=30.0) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(sql, params)
            row = cursor.fetchone()
            return dict(row) if row else None
    except sqlite3.Error as e:
        if error_msg: logging.error(f"{error_msg}: {e}")
        return None
# ======================


# ===== _FETCH_LIST =====
def _fetch_list(sql: str, params: tuple | list = (), error_msg: str = "") -> list[dict]:
    try:
        with sqlite3.connect(DB_FILE, timeout=30.0) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(sql, params)
            return [dict(row) for row in cursor.fetchall()]
    except sqlite3.Error as e:
        if error_msg: logging.error(f"{error_msg}: {e}")
        return []
# =======================


# ===== _FETCH_VAL =====
def _fetch_val(sql: str, params: tuple | list = (), default: Any = None, error_msg: str = "") -> Any:
    row = _fetch_row(sql, params, error_msg)
    return list(row.values())[0] if row else default
# ======================

# ===== УНИВЕРСАЛЬНЫЕ ХЕЛПЕРЫ DRY =====

def _check_rowcount(cursor, entity_name: str, context: str = "") -> bool:
    if cursor and cursor.rowcount == 0:
        msg = f"{context}: {entity_name} не найден" if context else f"{entity_name} не найден"
        logging.warning(msg)
        return False
    return cursor is not None

def _exec_with_check(sql: str, params: tuple | list, entity_name: str, error_msg: str = "", context: str = "") -> bool:
    row = _fetch_row(f"SELECT 1 FROM {entity_name.split()[0] if ' ' in entity_name else entity_name}", params[:1] if params else (), "")
    if not row:
        if context: logging.warning(f"{context}: объект не найден")
        return False
    cursor = _exec(sql, params, error_msg)
    return cursor is not None

def _get_count_stat(query: str, default=0) -> int:
    r = _fetch_row(query, (), "")
    return int(r["c"]) if r and "c" in r else (int(r["s"]) if r and "s" in r else default)

# ========================


# ===== INITIALIZE_DB =====
def initialize_db():
    try:
        with sqlite3.connect(DB_FILE, timeout=30.0) as conn:
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
                    referral_start_bonus_received BOOLEAN DEFAULT 0,
                    is_pinned BOOLEAN DEFAULT 0,
                    seller_active INTEGER DEFAULT 0,
                    auth_token TEXT
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
                    description TEXT,
                    comment_key TEXT
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
            # Инициализация дефолтных настроек
            cursor.execute('''
                INSERT OR IGNORE INTO bot_settings (key, value) 
                VALUES (?, ?)
            ''', ('pay_info_comment', json.dumps({"id": 1, "username": 1, "first_name": 1, "host_name": 1})))
            
            cursor.execute('''
                INSERT OR IGNORE INTO bot_settings (key, value) 
                VALUES (?, ?)
            ''', ('skip_email', '0'))
            
            cursor.execute('''
                INSERT OR IGNORE INTO bot_settings (key, value) 
                VALUES (?, ?)
            ''', ('enable_wal_mode', '0'))

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS other (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            ''')
            # Инициализация записи для рассылки
            _ensure_default_values(cursor, "other", {
                "newsletter": json.dumps({}),
                "sg_promt": "",
                "theme_newsletter": json.dumps({}),
                "auto_start_bot": "0"
            })

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
                    metadata TEXT,
                    see INTEGER DEFAULT 1
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
                    hwid_limit INTEGER DEFAULT 0,
                    traffic_limit_gb INTEGER DEFAULT 0,
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

            _ensure_index(cursor, "idx_support_tickets_status", "support_tickets", "status")
            _ensure_index(cursor, "idx_support_tickets_thread", "support_tickets", "forum_chat_id, message_thread_id")
            _ensure_index(cursor, "idx_support_messages_ticket_id", "support_messages", "ticket_id")
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS seller_users (
                    id_seller INTEGER PRIMARY KEY AUTOINCREMENT,
                    seller_sale REAL DEFAULT 0,
                    seller_ref REAL DEFAULT 0,
                    seller_uuid TEXT DEFAULT '0',
                    user_id INTEGER UNIQUE
                )
            ''')
            _ensure_unique_index(cursor, "idx_seller_users_user_id", "seller_users", "user_id")

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
                
                "platega_enabled": "false",
                "platega_crypto_enabled": "false",
                "platega_merchant_id": None,
                "platega_api_key": None,

                "main_menu_image": None,
                "profile_image": None,  
                "topup_image": None, 
                "referral_image": None,
                "support_image": None,
                "about_image": None,
                "speedtest_image": None,
                "howto_image": None,
                "topup_amount_image": None,

                "payment_image": None,
                "buy_server_image": None,
                "buy_plan_image": None,
                "enter_email_image": None,
                "key_info_image": None,
                "extend_plan_image": None,
                "keys_list_image": None,
                "payment_method_image": None,
                "key_comments_image": None,
                "key_ready_image": None,
                "devices_list_image": None,
                "key_gemini": None,
                "stealth_login_enabled": "0",
                "stealth_login_hotkey": "ctrl+b",
            }
            _ensure_default_values(cursor, "bot_settings", default_settings)
            conn.commit()
            

            



            try:
                cursor.execute("ALTER TABLE button_configs ADD COLUMN button_width INTEGER DEFAULT 1")
                logging.info("Добавлена колонка button_width в таблицу button_configs")
            except sqlite3.OperationalError:
                pass
            
            logging.info("База данных инициализирована")
        
        run_migration()
        
    except sqlite3.Error as e:
        logging.error("Не удалось инициализировать базу данных: %s", e)
# =========================


# ===== _ENSURE_DEFAULT_VALUES =====
def _ensure_default_values(cursor: sqlite3.Cursor, table: str, defaults: dict) -> None:
    for key, value in defaults.items():
        try:
            cursor.execute(
                f"INSERT OR IGNORE INTO {table} (key, value) VALUES (?, ?)",
                (key, value)
            )
        except Exception: pass
# ==================================


# ===== _ENSURE_USERS_COLUMNS =====
def _ensure_users_columns(cursor: sqlite3.Cursor) -> None:
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='users'")
    if not cursor.fetchone(): return
    mapping = {
        "referred_by": "INTEGER",
        "balance": "REAL DEFAULT 0",
        "referral_balance": "REAL DEFAULT 0",
        "referral_balance_all": "REAL DEFAULT 0",
        "referral_start_bonus_received": "BOOLEAN DEFAULT 0",
        "is_pinned": "BOOLEAN DEFAULT 0",
        "seller_active": "INTEGER DEFAULT 0",
        "auth_token": "TEXT",
    }
    for column, definition in mapping.items():
        _ensure_table_column(cursor, "users", column, definition)


# =================================


# ===== _ENSURE_HOSTS_COLUMNS =====
def _ensure_hosts_columns(cursor: sqlite3.Cursor) -> None:
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='xui_hosts'")
    if not cursor.fetchone(): return
    extras = {
        "squad_uuid": "TEXT",
        "description": "TEXT",
        "default_traffic_limit_bytes": "INTEGER",
        "default_traffic_strategy": "TEXT DEFAULT 'NO_RESET'",
        "default_traffic_reset_at": "TEXT",
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
        "see": "INTEGER DEFAULT 1",
        "traffic_limit_strategy": "TEXT DEFAULT 'NO_RESET'",
        "device_mode": "TEXT DEFAULT 'plan'",
        "tier_lock_extend": "INTEGER DEFAULT 0",
    }
    for column, definition in extras.items():
        _ensure_table_column(cursor, "xui_hosts", column, definition)


# =================================


def _ensure_device_tiers_table(cursor: sqlite3.Cursor) -> None:
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS device_tiers (
            tier_id INTEGER PRIMARY KEY AUTOINCREMENT,
            host_name TEXT NOT NULL,
            device_count INTEGER NOT NULL,
            price REAL NOT NULL DEFAULT 0,
            sort_order INTEGER DEFAULT 0,
            UNIQUE(host_name, device_count)
        )
    ''')


# ===== _ENSURE_PLANS_COLUMNS =====
def _ensure_plans_columns(cursor: sqlite3.Cursor) -> None:
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='plans'")
    if not cursor.fetchone(): return
    extras = {
        "squad_uuid": "TEXT",
        "duration_days": "INTEGER",
        "traffic_limit_bytes": "INTEGER",
        "traffic_limit_strategy": "TEXT DEFAULT 'NO_RESET'",
        "is_active": "INTEGER DEFAULT 1",
        "sort_order": "INTEGER DEFAULT 0",
        "metadata": "TEXT",
        "hwid_limit": "INTEGER DEFAULT 0",
        "traffic_limit_gb": "INTEGER DEFAULT 0",
    }
    for column, definition in extras.items():
        _ensure_table_column(cursor, "plans", column, definition)


# =================================


# ===== _ENSURE_SUPPORT_TICKETS_COLUMNS =====
def _ensure_support_tickets_columns(cursor: sqlite3.Cursor) -> None:
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='support_tickets'")
    if not cursor.fetchone(): return
    extras = {
        "forum_chat_id": "TEXT",
        "message_thread_id": "INTEGER",
    }
    for column, definition in extras.items():
        _ensure_table_column(cursor, "support_tickets", column, definition)


# ===========================================


# ===== _FINALIZE_VPN_KEY_INDEXES =====
def _finalize_vpn_key_indexes(cursor: sqlite3.Cursor) -> None:
    _ensure_unique_index(cursor, "uq_vpn_keys_email", "vpn_keys", "email")
    _ensure_unique_index(cursor, "uq_vpn_keys_key_email", "vpn_keys", "key_email")
    _ensure_index(cursor, "idx_vpn_keys_user_id", "vpn_keys", "user_id")
    _ensure_index(cursor, "idx_vpn_keys_rem_uuid", "vpn_keys", "remnawave_user_uuid")
    _ensure_index(cursor, "idx_vpn_keys_expire_at", "vpn_keys", "expire_at")


# =====================================


# ===== _REBUILD_VPN_KEYS_TABLE =====
def _rebuild_vpn_keys_table(cursor: sqlite3.Cursor) -> None:
    columns = _get_table_columns(cursor, "vpn_keys")
    legacy_markers = {"xui_client_uuid", "expiry_date", "created_date", "connection_string"}
    required = {"remnawave_user_uuid", "email", "expire_at", "created_at", "updated_at"}
    if required.issubset(columns) and not (columns & legacy_markers): _finalize_vpn_key_indexes(cursor); return

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
            description TEXT,
            comment_key TEXT
        )
    ''')
    old_columns = _get_table_columns(cursor, "vpn_keys_legacy")

    def has(column: str) -> bool: return column in old_columns

    def col(column: str, default: str = "NULL") -> str: return column if has(column) else default

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
        f"{col('comment_key')} AS comment_key",
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
            description,
            comment_key
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


# ===================================


# ===== _ENSURE_VPN_KEYS_SCHEMA =====
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
                description TEXT,
                comment_key TEXT
            )
        ''')
        _finalize_vpn_key_indexes(cursor)
        return
    _rebuild_vpn_keys_table(cursor)


# ===================================


# ===== RUN_MIGRATION =====
# ===========================================
# ===== _ENSURE_WEBAPP_SETTINGS_TABLE =====
def _ensure_webapp_settings_table(cursor: sqlite3.Cursor):
    try:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS webapp_settings (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                webapp_title TEXT DEFAULT 'VPN',
                webapp_domen TEXT DEFAULT '',
                webapp_enable INTEGER DEFAULT 0,
                webapp_logo TEXT DEFAULT '',
                webapp_icon TEXT DEFAULT ''
            )
        ''')
        
        cursor.execute("PRAGMA table_info(webapp_settings)")
        columns = {row[1] for row in cursor.fetchall()}
        
        if "webapp_title" not in columns:
            cursor.execute("ALTER TABLE webapp_settings ADD COLUMN webapp_title TEXT DEFAULT 'VPN'")
        if "webapp_domen" not in columns:
            cursor.execute("ALTER TABLE webapp_settings ADD COLUMN webapp_domen TEXT DEFAULT ''")
        if "webapp_enable" not in columns:
            cursor.execute("ALTER TABLE webapp_settings ADD COLUMN webapp_enable INTEGER DEFAULT 0")
        if "webapp_logo" not in columns:
            cursor.execute("ALTER TABLE webapp_settings ADD COLUMN webapp_logo TEXT DEFAULT ''")
        if "webapp_icon" not in columns:
            cursor.execute("ALTER TABLE webapp_settings ADD COLUMN webapp_icon TEXT DEFAULT ''")

        cursor.execute("INSERT OR IGNORE INTO webapp_settings (id, webapp_title, webapp_domen, webapp_enable, webapp_logo, webapp_icon) VALUES (1, 'VPN', '', 0, '', '')")
            
    except Exception as e:
        logging.error(f"Ошибка миграции webapp_settings: {e}")

# ===========================================


# ===== RUN_MIGRATION =====
def run_migration():
    if not DB_FILE.exists(): logging.error("Файл базы данных отсутствует, миграция пропущена."); return

    logging.info("Запуск миграций базы данных: %s", DB_FILE)

    try:
        with sqlite3.connect(DB_FILE, timeout=30.0) as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA foreign_keys = OFF")
            _ensure_users_columns(cursor)
            _ensure_hosts_columns(cursor)
            _ensure_device_tiers_table(cursor)
            _ensure_plans_columns(cursor)
            _ensure_support_tickets_columns(cursor)
            _ensure_vpn_keys_schema(cursor)
            _ensure_table_column(cursor, "vpn_keys", "comment_key", "TEXT")
            _ensure_ssh_targets_table(cursor)
            _ensure_host_speedtests_table(cursor)
            _ensure_resource_metrics_table(cursor)
            _ensure_gift_tokens_table(cursor)
            _ensure_promo_tables(cursor)
            _ensure_webapp_settings_table(cursor)
            try:
                cursor.execute("ALTER TABLE seller_users RENAME COLUMN sellr_ref TO seller_ref")
                logging.info("Переименована колонка sellr_ref в seller_ref в таблице seller_users")
            except Exception:
                pass

            _ensure_seller_users_table(cursor)

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
            
            _ensure_default_values(cursor, "bot_settings", {
                "skip_email": "0",
                "enable_wal_mode": "0"
            })
            
            _ensure_default_values(cursor, "other", {
                "theme_newsletter": json.dumps({}),
                "auto_start_bot": "0"
            })
            
            _ensure_pending_transactions_table(cursor)
            _ensure_default_button_configs(cursor)
            

            try:
                cursor.execute("""
                    UPDATE button_configs 
                    SET text = '🔑 Мои ключи ({len(user_keys)})', updated_at = CURRENT_TIMESTAMP
                    WHERE menu_type = 'main_menu' AND button_id = 'my_keys'
                """)
                wide_buttons = [("trial", 2), ("referral", 2), ("admin", 2)]
                for button_id, width in wide_buttons:
                    cursor.execute("""
                        UPDATE button_configs 
                        SET button_width = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE menu_type = 'main_menu' AND button_id = ?
                    """, (width, button_id))
            except Exception:
                pass


            cursor.execute("PRAGMA foreign_keys = ON")
            conn.commit()
    except sqlite3.Error as e:
        logging.error("Сбой миграции базы данных: %s", e)

# =========================


# ===== _ENSURE_PENDING_TRANSACTIONS_TABLE =====
def _ensure_pending_transactions_table(cursor: sqlite3.Cursor) -> None:
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

# ==============================================


# ===== _ENSURE_DEFAULT_BUTTON_CONFIGS =====
def _ensure_default_button_configs(cursor: sqlite3.Cursor) -> None:
    def menu_has_buttons(m_type):
        cursor.execute("SELECT 1 FROM button_configs WHERE menu_type = ? LIMIT 1", (m_type,))
        return cursor.fetchone() is not None

    if not menu_has_buttons("main_menu"):
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
    

    if not menu_has_buttons("admin_menu"):
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
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
            """, ("admin_menu", button_id, text, callback_data, row_pos, col_pos, sort_order))
    

    if not menu_has_buttons("profile_menu"):
        profile_menu_buttons = [
            ("topup", "💳 Пополнить баланс", "top_up_start", 0, 0, 0),
            ("referral", "🤝 Реферальная программа", "show_referral_program", 1, 0, 1),
            ("back_to_menu", "⬅️ Назад в меню", "back_to_main_menu", 2, 0, 2),
        ]
        
        for button_id, text, callback_data, row_pos, col_pos, sort_order in profile_menu_buttons:
            cursor.execute("""
                INSERT INTO button_configs 
                (menu_type, button_id, text, callback_data, row_position, column_position, sort_order, is_active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
            """, ("profile_menu", button_id, text, callback_data, row_pos, col_pos, sort_order))
    

    if not menu_has_buttons("support_menu"):
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
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
            """, ("support_menu", button_id, text, callback_data, row_pos, col_pos, sort_order))

    if not menu_has_buttons("key_info_menu"):
        key_info_menu_buttons = [
            ("connect", "📲 Подключиться", None, "{connection_string}", 0, 0, 0, 1),
            ("extend", "➕ Продлить этот ключ", "extend_key_{key_id}", None, 1, 0, 1, 1),
            ("qr", "📱 Показать QR-код", "show_qr_{key_id}", None, 2, 0, 2, 2),
            ("howto", "📖 Инструкция", "howto_vless_{key_id}", None, 2, 1, 3, 1),
            ("comments", "📝 Комментарии", "key_comments_{key_id}", None, 3, 0, 4, 1),
            ("back", "⬅️ Назад к списку ключей", "manage_keys", None, 4, 0, 5, 1),
        ]

        for button_id, text, callback_data, url, row_pos, col_pos, sort_order, width in key_info_menu_buttons:
            cursor.execute("""
                INSERT INTO button_configs 
                (menu_type, button_id, text, callback_data, url, row_position, column_position, sort_order, button_width, is_active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            """, ("key_info_menu", button_id, text, callback_data, url, row_pos, col_pos, sort_order, width))


# ==========================================


# ===== _ENSURE_SSH_TARGETS_TABLE =====
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
            metadata TEXT,
            time_auto TEXT DEFAULT '{}'
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
        "time_auto": "TEXT DEFAULT '{}'",
    }
    for column, definition in extras.items():
        _ensure_table_column(cursor, "speedtest_ssh_targets", column, definition)


# =====================================


# ===== _ENSURE_GIFT_TOKENS_TABLE =====
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


# =====================================


# ===== GET_USER_ID_BY_GIFT_TOKEN =====
def get_user_id_by_gift_token(token: str) -> int | None:
    row = _fetch_row("SELECT user_id FROM gift_token_claims WHERE token = ? ORDER BY claimed_at DESC LIMIT 1", (token,), f"Ошибка поиска user_id по токену {token}")
    return row["user_id"] if row else None
# =====================================


# ===== _ENSURE_SELLER_USERS_TABLE =====
def _ensure_seller_users_table(cursor: sqlite3.Cursor) -> None:
    """Миграция для таблицы seller_users."""
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS seller_users (
            id_seller INTEGER PRIMARY KEY AUTOINCREMENT,
            seller_sale REAL DEFAULT 0,
            seller_ref REAL DEFAULT 0,
            seller_uuid TEXT DEFAULT '0',
            user_id INTEGER UNIQUE
        )
    ''')
    
    mapping = {
        "seller_sale": "REAL DEFAULT 0",
        "seller_ref": "REAL DEFAULT 0",
        "seller_uuid": "TEXT DEFAULT '0'",
        "user_id": "INTEGER UNIQUE"
    }
    for column, definition in mapping.items():
        _ensure_table_column(cursor, "seller_users", column, definition)

    _ensure_unique_index(cursor, "idx_seller_users_user_id", "seller_users", "user_id")
# ====================================


# ===== _ENSURE_PROMO_TABLES =====
def _ensure_promo_tables(cursor: sqlite3.Cursor) -> None:
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS promo_codes (
            code TEXT PRIMARY KEY,
            discount_percent REAL,
            discount_amount REAL,
            promo_type TEXT DEFAULT 'discount',
            reward_value INTEGER DEFAULT 0,
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
    
    mapping = {
        "promo_type": "TEXT DEFAULT 'discount'",
        "reward_value": "INTEGER DEFAULT 0"
    }
    for column, definition in mapping.items():
        _ensure_table_column(cursor, "promo_codes", column, definition)

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


# =================================


# ===== _ENSURE_HOST_SPEEDTESTS_TABLE =====
def _ensure_host_speedtests_table(cursor: sqlite3.Cursor) -> None:
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


# =========================================


# ===== _ENSURE_RESOURCE_METRICS_TABLE =====
def _ensure_resource_metrics_table(cursor: sqlite3.Cursor) -> None:
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




# ==========================================


# ===== INSERT_RESOURCE_METRIC =====
def insert_resource_metric(
    scope: str,
    object_name: str,
    cpu_percent: float | None = None,
    mem_percent: float | None = None,
    disk_percent: float | None = None,
    load1: float | None = None,
    net_bytes_sent: int | None = None,
    net_bytes_recv: int | None = None,
    raw_json: str | None = None
) -> int | None:
    cursor = _exec(
        """
        INSERT INTO resource_metrics (
            scope, object_name, cpu_percent, mem_percent, disk_percent, load1, 
            net_bytes_sent, net_bytes_recv, raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            (scope or '').strip(), (object_name or '').strip(),
            cpu_percent, mem_percent, disk_percent, load1, 
            net_bytes_sent, net_bytes_recv, raw_json
        ),
        f"Не удалось сохранить метрики ресурсов scope={scope} object={object_name}"
    )
    return cursor.lastrowid if cursor else None


# ==================================


# ===== GET_LATEST_RESOURCE_METRIC =====
def get_latest_resource_metric(scope: str, object_name: str) -> dict | None:
    return _fetch_row(
        """
        SELECT * FROM resource_metrics
        WHERE scope = ? AND object_name = ?
        ORDER BY created_at DESC
        LIMIT 1
        """,
        ((scope or '').strip(), (object_name or '').strip()),
        f"Не удалось получить последние метрики ресурсов scope={scope} object={object_name}"
    )


# ======================================


# ===== GET_RESOURCE_METRICS =====
def get_resource_metrics(
    scope: str,
    object_name: str,
    limit: int = 20
) -> list[dict]:
    return _fetch_list(
        """
        SELECT *
        FROM resource_metrics
        WHERE scope = ? AND object_name = ?
        ORDER BY datetime(created_at) DESC
        LIMIT ?
        """,
        ((scope or '').strip(), (object_name or '').strip(), limit),
        f"Не удалось получить метрики ресурсов scope={scope} object={object_name}"
    )


# ==============================


# ===== GET_METRICS_SERIES =====
def get_metrics_series(scope: str, object_name: str, *, since_hours: int = 24, limit: int = 500) -> list[dict]:
    if since_hours == 1:
        hours_filter = 2
    else:
        hours_filter = max(1, int(since_hours))
    
    rows = _fetch_list(
        f'''
        SELECT created_at, cpu_percent, mem_percent, disk_percent, load1
        FROM resource_metrics
        WHERE scope = ? AND object_name = ?
            AND created_at >= datetime('now', '+3 hours', ?)
        ORDER BY created_at ASC
        LIMIT ?
        ''',
        (
            (scope or '').strip(),
            (object_name or '').strip(),
            f'-{hours_filter} hours',
            max(10, int(limit)),
        ),
        f"Не удалось получить серию метрик для {scope}/{object_name}"
    )
    logging.debug(f"get_metrics_series: {scope}/{object_name}, since_hours={since_hours}, found {len(rows)} records")
    return rows




# ==============================


# ===== CREATE_HOST =====
def create_host(name: str, url: str, user: str, passwd: str, inbound: int, subscription_url: str | None = None):
    name = normalize_host_name(name)
    url = (url or "").strip()
    user = (user or "").strip()
    passwd = passwd or ""
    try:
        inbound = int(inbound)
    except Exception:
        pass
    subscription_url = (subscription_url or None)

    cursor = _exec(
         "INSERT INTO xui_hosts (host_name, host_url, host_username, host_pass, host_inbound_id, subscription_url) VALUES (?, ?, ?, ?, ?, ?)",
         (name, url, user, passwd, inbound, subscription_url),
         ""
    )
    if cursor:
        logging.info(f"Успешно создан новый хост: {name}")
        return

    cursor = _exec(
         "INSERT INTO xui_hosts (host_name, host_url, host_username, host_pass, host_inbound_id) VALUES (?, ?, ?, ?, ?)",
         (name, url, user, passwd, inbound),
         f"Ошибка при создании хоста '{name}'"
    )
    if cursor:
         logging.info(f"Успешно создан новый хост (fallback): {name}")

# =======================


# ===== UPDATE_HOST_SUBSCRIPTION_URL =====
def update_host_subscription_url(host_name: str, subscription_url: str | None) -> bool:
    host_name = normalize_host_name(host_name)
    cursor = _exec(
        "UPDATE xui_hosts SET subscription_url = ? WHERE TRIM(host_name) = TRIM(?)",
        (subscription_url, host_name),
        f"Не удалось обновить subscription_url для хоста '{host_name}'"
    )
    return _check_rowcount(cursor, f"хост '{host_name}'", "update_host_subscription_url")
# ========================================

# ===== UPDATE_HOST_DESCRIPTION =====

# ===== UPDATE_HOST_DESCRIPTION =====
# Обновление описания хоста
def update_host_description(host_name: str, description: str | None) -> bool:
    host_name = normalize_host_name(host_name)
    cursor = _exec(
        "UPDATE xui_hosts SET description = ? WHERE TRIM(host_name) = TRIM(?)",
        (description, host_name),
        f"Не удалось обновить описание для хоста '{host_name}'"
    )
    return _check_rowcount(cursor, f"хост '{host_name}'", "update_host_description")
# ===================================

# ===== UPDATE_HOST_TRAFFIC_SETTINGS =====

# ===== UPDATE_HOST_TRAFFIC_SETTINGS =====
# Обновление стратегии лимита трафика для хоста
# Default: 'NO_RESET'
def update_host_traffic_settings(host_name: str, traffic_strategy: str | None = 'NO_RESET') -> bool:
    host_name = normalize_host_name(host_name)
    cursor = _exec(
        "UPDATE xui_hosts SET default_traffic_strategy = ? WHERE TRIM(host_name) = TRIM(?)",
        (traffic_strategy or 'NO_RESET', host_name),
        f"Не удалось обновить настройки трафика для хоста '{host_name}'"
    )
    return _check_rowcount(cursor, f"хост '{host_name}'", "update_host_traffic_settings")
# ========================================


# ===== SET_REFERRAL_START_BONUS_RECEIVED =====
def set_referral_start_bonus_received(user_id: int) -> bool:
    return _check_rowcount(_exec(
        "UPDATE users SET referral_start_bonus_received = 1 WHERE telegram_id = ?",
        (user_id,),
        f"Не удалось установить бонус реферала для пользователя {user_id}"
    ), f"пользователь {user_id}", "")
# =============================================


# ===== UPDATE_HOST_URL =====
# Обновление URL хоста
def update_host_url(host_name: str, new_url: str) -> bool:
    host_name = normalize_host_name(host_name)
    cursor = _exec(
        "UPDATE xui_hosts SET host_url = ? WHERE TRIM(host_name) = TRIM(?)",
        (new_url, host_name),
        f"Не удалось обновить URL для хоста '{host_name}'"
    )
    return _check_rowcount(cursor, f"хост '{host_name}'", "update_host_url")
# ===========================


# ===== ADD_SELLER_USER =====
def add_seller_user(user_id: int, seller_sale: float = 0, seller_ref: float = 0, seller_uuid: str = "0") -> int | None:
    cursor = _exec(
        """
        INSERT OR REPLACE INTO seller_users (user_id, seller_sale, seller_ref, seller_uuid) 
        VALUES (?, ?, ?, ?)
        """,
        (user_id, seller_sale, seller_ref, str(seller_uuid)),
        "Не удалось добавить продавца"
    )
    return cursor.lastrowid if cursor else None

# =======================


# ===== GET_SELLER_USER =====
def get_seller_user(user_id: int) -> dict | None:
    row = _fetch_row("SELECT * FROM seller_users WHERE user_id = ?", (user_id,), f"Не удалось получить продавца {user_id}")
    if not row:
        return {
            "user_id": user_id,
            "seller_sale": 0.0,
            "seller_ref": 0.0,
            "seller_uuid": "0",
        }
    return row

# =======================


# ===== DELETE_SELLER_USER =====
def delete_seller_user(user_id: int) -> bool:
    cursor = _exec("DELETE FROM seller_users WHERE user_id = ?", (user_id,), f"Не удалось удалить продавца {user_id}")
    return cursor is not None

# ==========================


# ===== UPDATE_HOST_REMNAWAVE_SETTINGS =====
def update_host_remnawave_settings(
    host_name: str,
    *,
    remnawave_base_url: str | None = None,
    remnawave_api_token: str | None = None,
    squad_uuid: str | None = None,
) -> bool:
    host_name_n = normalize_host_name(host_name)
    row = _fetch_row("SELECT 1 FROM xui_hosts WHERE TRIM(host_name) = TRIM(?)", (host_name_n,), "")
    if not row:
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
    cursor = _exec(sql, params, f"Не удалось обновить Remnawave-настройки для хоста '{host_name}'")
    return cursor is not None

# ========================================


# ===== UPDATE_HOST_SSH_SETTINGS =====
def update_host_ssh_settings(
    host_name: str,
    ssh_host: str | None = None,
    ssh_port: int | None = None,
    ssh_user: str | None = None,
    ssh_password: str | None = None,
    ssh_key_path: str | None = None,
) -> bool:
    host_name_n = normalize_host_name(host_name)
    row = _fetch_row("SELECT 1 FROM xui_hosts WHERE TRIM(host_name) = TRIM(?)", (host_name_n,), "")
    if not row:
        logging.warning(f"update_host_ssh_settings: хост не найден '{host_name_n}'")
        return False

    cursor = _exec(
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
        f"Не удалось обновить SSH-параметры для хоста '{host_name}'"
    )
    return cursor is not None
# ====================================


# ===== UPDATE_HOST_NAME =====
def update_host_name(old_name: str, new_name: str) -> bool:
    old_n = normalize_host_name(old_name)
    new_n = normalize_host_name(new_name)
    if not old_n or not new_n:
        return False
    if old_n == new_n:
        return True

    row = _fetch_row("SELECT 1 FROM xui_hosts WHERE TRIM(host_name) = TRIM(?)", (old_n,), "")
    if not row:
        logging.warning(f"update_host_name: исходный хост не найден '{old_n}'")
        return False

    row_new = _fetch_row("SELECT 1 FROM xui_hosts WHERE TRIM(host_name) = TRIM(?)", (new_n,), "")
    if row_new:
        logging.warning(f"update_host_name: новое имя занято '{new_n}'")
        return False

    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA foreign_keys=OFF")
            cursor.execute(
                "UPDATE xui_hosts SET host_name = ? WHERE TRIM(host_name) = TRIM(?)",
                (new_n, old_n)
            )
            cursor.execute(
                "UPDATE plans SET host_name = ? WHERE TRIM(host_name) = TRIM(?)",
                (new_n, old_n)
            )
            cursor.execute(
                "UPDATE vpn_keys SET host_name = ? WHERE TRIM(host_name) = TRIM(?)",
                (new_n, old_n)
            )
            cursor.execute(
                "UPDATE host_speedtests SET host_name = ? WHERE TRIM(host_name) = TRIM(?)",
                (new_n, old_n)
            )
            conn.commit()
            cursor.execute("PRAGMA foreign_keys=ON")
            return True
    except sqlite3.Error as e: logging.error(f"Не удалось переименовать хост '{old_name}' -> '{new_name}': {e}"); return False

# ===== DELETE_HOST =====
# Удаление хоста и всех связанных тарифов
def delete_host(host_name: str):
    try:
        host_name = normalize_host_name(host_name)
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM plans WHERE TRIM(host_name) = TRIM(?)", (host_name,))
            cursor.execute("DELETE FROM xui_hosts WHERE TRIM(host_name) = TRIM(?)", (host_name,))
            conn.commit()
            logging.info(f"Хост '{host_name}' и его тарифы успешно удалены.")
    except sqlite3.Error as e: logging.error(f"Ошибка удаления хоста '{host_name}': {e}")
# =========================


# ===== GET_HOST =====
# Получение информации о хосте по имени
# Fallback: None если хост не найден
def get_host(host_name: str) -> dict | None:
    try:
        host_name = normalize_host_name(host_name)
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM xui_hosts WHERE TRIM(host_name) = TRIM(?)", (host_name,))
            result = cursor.fetchone(); return dict(result) if result else None
    except sqlite3.Error as e: logging.error(f"Ошибка получения хоста '{host_name}': {e}"); return None
# ==================


# ===== TOGGLE_HOST_VISIBILITY =====
# Переключение видимости хоста (поле see)
def toggle_host_visibility(host_name: str, visible: int) -> bool:
    host_name_n = normalize_host_name(host_name)
    visible_int = 1 if visible else 0
    row = _fetch_row("SELECT 1 FROM xui_hosts WHERE TRIM(host_name) = TRIM(?)", (host_name_n,), "")
    if not row: logging.warning(f"toggle_host_visibility: хост не найден '{host_name_n}'"); return False
    cursor = _exec(
        "UPDATE xui_hosts SET see = ? WHERE TRIM(host_name) = TRIM(?)",
        (visible_int, host_name_n),
        f"Не удалось обновить видимость для хоста '{host_name}'"
    )
    if cursor and cursor.rowcount > 0: logging.info(f"Видимость хоста '{host_name_n}' обновлена: see={visible_int}")
    return _check_rowcount(cursor, f"хост '{host_name_n}'", "")
# ==================================


def get_device_tiers(host_name: str) -> list[dict]:
    return _fetch_list("SELECT * FROM device_tiers WHERE TRIM(host_name)=TRIM(?) ORDER BY sort_order, device_count", (host_name,))

def add_device_tier(host_name: str, device_count: int, price: float) -> int | None:
    r = _exec("INSERT OR REPLACE INTO device_tiers (host_name, device_count, price) VALUES (?,?,?)", (host_name, device_count, price))
    return r.lastrowid if r else None

def delete_device_tier(tier_id: int) -> bool:
    r = _exec("DELETE FROM device_tiers WHERE tier_id=?", (tier_id,))
    return r is not None and r.rowcount > 0

def get_device_tier_by_id(tier_id: int) -> dict | None:
    return _fetch_row("SELECT * FROM device_tiers WHERE tier_id=?", (tier_id,))

def update_host_device_mode(host_name: str, mode: str) -> bool:
    r = _exec("UPDATE xui_hosts SET device_mode=? WHERE TRIM(host_name)=TRIM(?)", (mode, host_name))
    return r is not None and r.rowcount > 0


# ===== DELETE_KEY_BY_ID =====
# Удаление ключа по key_id
def delete_key_by_id(key_id: int) -> bool:
    return _check_rowcount(_exec("DELETE FROM vpn_keys WHERE key_id = ?", (key_id,), f"Не удалось удалить ключ по id {key_id}"), f"ключ {key_id}", "")
# ============================


# ===== UPDATE_KEY_COMMENT =====
# Обновление комментария (description) для ключа
def update_key_comment(key_id: int, comment: str) -> bool:
    return _check_rowcount(_exec("UPDATE vpn_keys SET description = ? WHERE key_id = ?", (comment, key_id), f"Не удалось обновить комментарий ключа для {key_id}"), f"ключ {key_id}", "")
# ==============================


# ===== GET_ALL_HOSTS =====
def get_all_hosts(visible_only: bool = False) -> list[dict]:
    # Сначала пытаемся выполнить запрос
    sql = "SELECT * FROM xui_hosts ORDER BY sort_order ASC, host_name ASC"
    if visible_only: sql = "SELECT * FROM xui_hosts WHERE see = 1 ORDER BY sort_order ASC, host_name ASC"
    
    rows = _fetch_list(sql, (), "")
    if not rows:
        # Если пусто или ошибка, возможно нет колонки see (хотя миграция должна была сработать)
        # Пробуем через старый механизм fallback только если реально была ошибка
        # Но у нас _fetch_list возвращает [], так что сложно отличить "пусто" от "ошибка".
        # Однако, раз мы строго следим за миграциями, колонка see должна быть.
        # Если ошибка была, она залогировалась в _fetch_list.
        pass

    result = []
    for row in rows:
        d = dict(row)
        d['host_name'] = normalize_host_name(d.get('host_name'))
        result.append(d)
    return result

# =========================


# ===== GET_SPEEDTESTS =====
def get_speedtests(host_name: str, limit: int = 20) -> list[dict]:
    host_name_n = normalize_host_name(host_name)
    try:
        limit_int = int(limit)
    except Exception: limit_int = 20
        
    return _fetch_list(
        """
        SELECT id, host_name, method, ping_ms, jitter_ms, download_mbps, upload_mbps,
               server_name, server_id, ok, error, created_at
        FROM host_speedtests
        WHERE TRIM(host_name) = TRIM(?)
        ORDER BY datetime(created_at) DESC
        LIMIT ?
        """,
        (host_name_n, limit_int),
        f"Не удалось получить speedtest-данные для хоста '{host_name}'"
    )

# ========================


# ===== GET_LATEST_SPEEDTEST =====
def get_latest_speedtest(host_name: str) -> dict | None:
    host_name_n = normalize_host_name(host_name)
    return _fetch_row(
        """
        SELECT id, host_name, method, ping_ms, jitter_ms, download_mbps, upload_mbps,
               server_name, server_id, ok, error, created_at
        FROM host_speedtests
        WHERE TRIM(host_name) = TRIM(?)
        ORDER BY datetime(created_at) DESC
        LIMIT 1
        """,
        (host_name_n,),
        f"Не удалось получить последний speedtest для хоста '{host_name}'"
    )

# ===== INSERT_HOST_SPEEDTEST =====
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
    error: str | None = None
) -> int | None:
    host_name_n = normalize_host_name(host_name)
    cursor = _exec(
        """
        INSERT INTO host_speedtests (host_name, method, ping_ms, jitter_ms, download_mbps, upload_mbps, server_name, server_id, ok, error)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (host_name_n, method, ping_ms, jitter_ms, download_mbps, upload_mbps, server_name, server_id, 1 if ok else 0, error),
        f"Не удалось сохранить запись speedtest для '{host_name}'"
    )
    return cursor.lastrowid if cursor else None






# ===== GET_ALL_SSH_TARGETS =====
def get_all_ssh_targets() -> list[dict]:
    return _fetch_list("SELECT * FROM speedtest_ssh_targets ORDER BY sort_order ASC, target_name ASC", (), "Не удалось получить список SSH-целей")


# ===========================


# ===== GET_SSH_TARGET =====
def get_ssh_target(target_name: str) -> dict | None:
    name = normalize_host_name(target_name)
    return _fetch_row("SELECT * FROM speedtest_ssh_targets WHERE TRIM(target_name) = TRIM(?)", (name,), f"Не удалось получить SSH-цель '{target_name}'")


# ========================


# ===== CREATE_SSH_TARGET =====
# Создание новой SSH-цели для speedtest
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
    name = normalize_host_name(target_name)
    cursor = _exec(
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
        ),
        f"Не удалось создать SSH-цель '{target_name}'"
    )
    return cursor is not None
# ===========================


# ===== UPDATE_SSH_TARGET_FIELDS =====
# Обновление полей SSH-цели (выборочное обновление)
# Параметры с None не обновляются
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
    name = normalize_host_name(target_name)
    row = _fetch_row("SELECT 1 FROM speedtest_ssh_targets WHERE TRIM(target_name) = TRIM(?)", (name,), "")
    if not row: logging.warning(f"update_ssh_target_fields: цель не найдена '{name}'"); return False
        
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
    
    if not sets: return True
    
    params.append(name)
    sql = f"UPDATE speedtest_ssh_targets SET {', '.join(sets)} WHERE TRIM(target_name) = TRIM(?)"
    cursor = _exec(sql, params, f"Не удалось обновить SSH-цель '{target_name}'")
    return cursor is not None








# ===== DELETE_SSH_TARGET =====
# Удаление SSH-цели по имени
def delete_ssh_target(target_name: str) -> bool:
    return _check_rowcount(_exec(
        "DELETE FROM speedtest_ssh_targets WHERE TRIM(target_name) = TRIM(?)",
        (normalize_host_name(target_name),),
        f"Не удалось удалить SSH-цель '{target_name}'"
    ), f"SSH-цель '{target_name}'", "")
# =============================


# ===== RENAME_SSH_TARGET =====
# Переименование SSH-цели с обновлением связанных speedtest-записей
def rename_ssh_target(old_target_name: str, new_target_name: str) -> bool:
    old_name = normalize_host_name(old_target_name)
    new_name = normalize_host_name(new_target_name)
    
    if old_name == new_name: return True
    
    row = _fetch_row("SELECT 1 FROM speedtest_ssh_targets WHERE TRIM(target_name) = TRIM(?)", (old_name,), "")
    if not row: logging.warning(f"rename_ssh_target: старая цель не найдена '{old_name}'"); return False
    
    row_new = _fetch_row("SELECT 1 FROM speedtest_ssh_targets WHERE TRIM(target_name) = TRIM(?)", (new_name,), "")
    if row_new: logging.warning(f"rename_ssh_target: новое имя уже занято '{new_name}'"); return False
    
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE speedtest_ssh_targets SET target_name = ? WHERE TRIM(target_name) = TRIM(?)",
                (new_name, old_name)
            )
            cursor.execute(
                "UPDATE host_speedtests SET host_name = ? WHERE TRIM(host_name) = TRIM(?)",
                (new_name, old_name)
            )
            conn.commit()
            logging.info(f"SSH-цель переименована: '{old_name}' → '{new_name}'")
            return True
    except sqlite3.Error as e: logging.error(f"Не удалось переименовать SSH-цель '{old_target_name}' → '{new_target_name}': {e}"); return False



# ===== GET_ADMIN_STATS =====
# Получение статистики для админ-панели
# Возвращает: total_users, total_keys, active_keys, total_income, today_new_users, today_income, today_issued_keys
def get_admin_stats() -> dict:
    stats = {}
    stats["total_users"] = _get_count_stat("SELECT COUNT(*) as c FROM users")
    stats["total_keys"] = _get_count_stat("SELECT COUNT(*) as c FROM vpn_keys")
    stats["active_keys"] = _get_count_stat("SELECT COUNT(*) as c FROM vpn_keys WHERE expire_at IS NOT NULL AND datetime(expire_at) > CURRENT_TIMESTAMP")
    stats["total_income"] = float(_get_count_stat("""
        SELECT COALESCE(SUM(amount_rub), 0) as s FROM transactions
        WHERE status IN ('paid','success','succeeded') AND LOWER(COALESCE(payment_method, '')) <> 'balance'
    """))
    stats["today_new_users"] = _get_count_stat("SELECT COUNT(*) as c FROM users WHERE date(registration_date) = date('now', '+3 hours')")
    stats["today_income"] = float(_get_count_stat("""
        SELECT COALESCE(SUM(amount_rub), 0) as s FROM transactions
        WHERE status IN ('paid','success','succeeded') AND date(created_date) = date('now', '+3 hours') 
          AND LOWER(COALESCE(payment_method, '')) <> 'balance'
    """))
    stats["today_issued_keys"] = _get_count_stat("SELECT COUNT(*) as c FROM vpn_keys WHERE date(COALESCE(created_at, updated_at, CURRENT_TIMESTAMP)) = date('now', '+3 hours')")
    return stats
# =======================

# ===== GET_ALL_KEYS =====
# Получение всех ключей из БД с нормализацией
def get_all_keys() -> list[dict]:
    rows = _fetch_list("SELECT * FROM vpn_keys ORDER BY key_id DESC", (), "Не удалось получить все ключи")
    return [_normalize_key_row(row) for row in rows]
# =========================


# ===== GET_KEYS_FOR_USER =====
def get_keys_for_user(user_id: int) -> list[dict]:
    return get_user_keys(user_id)

# =============================


# ===== UPDATE_KEY_EMAIL =====
def update_key_email(key_id: int, new_email: str) -> bool:
    normalized = _normalize_email(new_email) or new_email.strip()
    return update_key_fields(key_id, email=normalized)

# ============================


# ===== UPDATE_KEY_HOST =====
def update_key_host(key_id: int, new_host_name: str) -> bool:
    return update_key_fields(key_id, host_name=new_host_name)

# ===========================


# ===== CREATE_GIFT_KEY =====
def create_gift_key(user_id: int, host_name: str, key_email: str, months: int, remnawave_user_uuid: str | None = None) -> int | None:
    try:
        from datetime import timedelta

        months_value = max(1, int(months or 1))
        expiry_dt = get_msk_time() + timedelta(days=30 * months_value)
        expiry_ms = int(expiry_dt.timestamp() * 1000)
        uuid_value = remnawave_user_uuid or f"GIFT-{user_id}-{int(get_msk_time().timestamp())}"
        return add_new_key(
            user_id=user_id,
            host_name=host_name,
            remnawave_user_uuid=uuid_value,
            key_email=key_email,
            expiry_timestamp_ms=expiry_ms,
        )
    except Exception as e:
        logging.error(f"Не удалось создать подарочный ключ для пользователя {user_id}: {e}")
        return None
# ===========================


# ===== GET_SETTING =====
def get_setting(key: str) -> str | None:
    row = _fetch_row("SELECT value FROM bot_settings WHERE key = ?", (key,), f"Не удалось получить настройку '{key}'")
    return row["value"] if row else None

# =======================


# ===== GET_ADMIN_IDS =====
def get_admin_ids() -> set[int]:
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
        logging.warning(f"Ошибка get_admin_ids: {e}")
    return ids
# =========================


# ===== IS_ADMIN =====
def is_admin(user_id: int) -> bool:
    try:
        return int(user_id) in get_admin_ids()
    except Exception: return False


# ====================


# ===== CREATE_PAYLOAD_PENDING =====
def create_payload_pending(payment_id: str, user_id: int, amount_rub: float | None, metadata: dict | None) -> bool:
    print(f"[DEBUG] create_payload_pending called: payment_id={payment_id}, user_id={user_id}, amount_rub={amount_rub}, metadata={metadata}")
    cursor = _exec(
        """
        INSERT OR REPLACE INTO pending_transactions (payment_id, user_id, amount_rub, metadata, status, created_at, updated_at)
        VALUES (?, ?, ?, ?, COALESCE((SELECT status FROM pending_transactions WHERE payment_id = ?), 'pending'),
                COALESCE((SELECT created_at FROM pending_transactions WHERE payment_id = ?), CURRENT_TIMESTAMP), CURRENT_TIMESTAMP)
        """,
        (payment_id, int(user_id), float(amount_rub) if amount_rub is not None else None, json.dumps(metadata or {}), payment_id, payment_id),
        f"Не удалось создать ожидающую транзакцию {payment_id}"
    )
    return cursor is not None
# ==============================


# ===== _GET_PENDING_METADATA =====
def _get_pending_metadata(payment_id: str) -> dict | None:
    row = _fetch_row("SELECT * FROM pending_transactions WHERE payment_id = ?", (payment_id,), f"Не удалось прочитать ожидающую транзакцию {payment_id}")
    if not row: return None
    try:
        meta = json.loads(row["metadata"] or "{}")
    except Exception: meta = {}

    meta.setdefault('payment_id', payment_id)
    return meta
# =================================


# ===== GET_PENDING_METADATA =====
def get_pending_metadata(payment_id: str) -> dict | None:
    return _get_pending_metadata(payment_id)


# ================================


# ===== GET_PENDING_STATUS =====
def get_pending_status(payment_id: str) -> str | None:
    row = _fetch_row("SELECT status FROM pending_transactions WHERE payment_id = ?", (payment_id,), f"Не удалось получить статус для ожидающей {payment_id}")
    return (row["status"] or '').strip() or None if row else None

# ==============================


# ===== _COMPLETE_PENDING =====
def _complete_pending(payment_id: str) -> bool:
    cursor = _exec(
        "UPDATE pending_transactions SET status = 'paid', updated_at = CURRENT_TIMESTAMP WHERE payment_id = ? AND status != 'paid'",
        (payment_id,),
        f"Не удалось завершить ожидающую транзакцию {payment_id}"
    )
    return cursor is not None and cursor.rowcount > 0
# ===========================


# ===== FIND_AND_COMPLETE_PENDING_TRANSACTION =====
def find_and_complete_pending_transaction(payment_id: str) -> dict | None:
    logging.info(f"🔍 Ищем ожидающую транзакцию: {payment_id}")
    meta = _get_pending_metadata(payment_id)
    if not meta: logging.warning(f"❌ Ожидающая транзакция не найдена: {payment_id}"); return None
    
    user_id = meta.get('user_id', 'неизвестно')
    amount = meta.get('price', 0)
    logging.info(f"✅ Найдена ожидающая транзакция: пользователь {user_id}, сумма {amount:.2f} RUB")
    
    success = _complete_pending(payment_id)
    if success:
        logging.info(f"✅ Транзакция отмечена как оплаченная: {payment_id}")
        return meta
    else:
        logging.warning(f"⚠️ Транзакция {payment_id} уже была оплачена или заблокирована (дубликат вебхука)")
        return None
# =================================================


# ===== GET_LATEST_PENDING_FOR_USER =====
def get_latest_pending_for_user(user_id: int) -> dict | None:
    row = _fetch_row(
        """
        SELECT payment_id, metadata FROM pending_transactions
        WHERE user_id = ? AND status = 'pending'
        ORDER BY datetime(created_at) DESC, datetime(updated_at) DESC
        LIMIT 1
        """,
        (int(user_id),),
        f"Не удалось получить последнюю ожидающую для пользователя {user_id}"
    )
    if not row:
        return None
    try:
        meta = json.loads(row["metadata"] or "{}")
    except Exception:
        meta = {}
    meta.setdefault('payment_id', row["payment_id"]) 
    return meta
# =======================================


# ===== GET_REFERRALS_FOR_USER =====
def get_referrals_for_user(user_id: int) -> list[dict]:
    rows = _fetch_list(
        """
        SELECT telegram_id, username, registration_date, total_spent
        FROM users
        WHERE referred_by = ?
        ORDER BY registration_date DESC
        """,
        (user_id,),
        f"Не удалось получить рефералов для пользователя {user_id}"
    )
    return [dict(r) for r in rows]
# ====================================


# ===== GET_ALL_SETTINGS =====
def get_all_settings() -> dict:
    rows = _fetch_list("SELECT key, value FROM bot_settings", (), "Не удалось получить все настройки")
    return {row['key']: row['value'] for row in rows}

# ============================


# ===== UPDATE_SETTING =====
def update_setting(key: str, value: str):
    cursor = _exec(
        "INSERT OR REPLACE INTO bot_settings (key, value) VALUES (?, ?)",
        (key, value),
        f"Не удалось обновить настройку '{key}'"
    )
    if cursor: logging.info(f"Настройка '{key}' обновлена.")
# ==========================


# ===== GET_BUTTON_CONFIGS =====
def get_button_configs(menu_type: str, include_inactive: bool = False) -> list[dict]:
    query = """
        SELECT * FROM button_configs 
        WHERE menu_type = ? 
        ORDER BY sort_order, row_position, column_position
    """
    if not include_inactive:
        query = """
            SELECT * FROM button_configs 
            WHERE menu_type = ? AND is_active = 1 
            ORDER BY sort_order, row_position, column_position
        """
        
    rows = _fetch_list(query, (menu_type,), f"Не удалось получить конфиг кнопок для {menu_type}")
    return [dict(r) for r in rows]
# ============================


# ===== GET_BUTTON_CONFIG =====
def get_button_config(menu_type: str, button_id: str) -> dict | None:
    row = _fetch_row(
        """
        SELECT * FROM button_configs 
        WHERE menu_type = ? AND button_id = ?
        """,
        (menu_type, button_id),
        f"Не удалось получить конфиг кнопки {menu_type}/{button_id}"
    )
    return dict(row) if row else None

# =============================


# ===== CREATE_BUTTON_CONFIG =====
def create_button_config(menu_type: str, button_id: str, text: str, callback_data: str = None, 
                        url: str = None, row_position: int = 0, column_position: int = 0, 
                        button_width: int = 1, metadata: str = None) -> bool:
    cursor = _exec(
        """
        INSERT OR REPLACE INTO button_configs 
        (menu_type, button_id, text, callback_data, url, row_position, column_position, button_width, metadata, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        (menu_type, button_id, text, callback_data, url, row_position, column_position, button_width, metadata),
        "Не удалось создать конфиг кнопки"
    )
    if cursor: logging.info(f"Конфиг кнопки создан: {menu_type}/{button_id}"); return True
    return False
# ================================


# ===== UPDATE_BUTTON_CONFIG =====
def update_button_config(button_id: int, text: str = None, callback_data: str = None, 
                        url: str = None, row_position: int = None, column_position: int = None, 
                        button_width: int = None, is_active: bool = None, sort_order: int = None, metadata: str = None) -> bool:
    logging.info(f"update_button_config called for {button_id}: text={text}, callback_data={callback_data}, url={url}, row={row_position}, col={column_position}, active={is_active}, sort={sort_order}")
    
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
    
    if not updates: return True
        
    updates.append("updated_at = CURRENT_TIMESTAMP")
    params.append(button_id)
    
    query = f"UPDATE button_configs SET {', '.join(updates)} WHERE id = ?"
    cursor = _exec(query, params, f"Не удалось обновить конфиг кнопки {button_id}")
    
    if cursor and cursor.rowcount > 0: logging.info(f"Конфиг кнопки {button_id} успешно обновлён"); return True
    if cursor and cursor.rowcount == 0: logging.warning(f"Кнопка с id {button_id} не найдена")
    return False
# ================================


# ===== REORDER_BUTTON_CONFIGS =====
def reorder_button_configs(menu_type: str, button_orders: list[dict]) -> bool:
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
                is_active = order_data.get('is_active')
                
                set_clauses = [
                    "sort_order = ?",
                    "row_position = ?",
                    "column_position = ?",
                    "updated_at = CURRENT_TIMESTAMP"
                ]
                query_params = [sort_order, row_position, column_position]

                if button_width is not None:
                    set_clauses.insert(3, "button_width = ?")
                    query_params.insert(3, int(button_width))
                
                if is_active is not None:
                    set_clauses.insert(len(set_clauses)-1, "is_active = ?")
                    query_params.insert(len(query_params), 1 if is_active else 0)

                query_params.append(menu_type)
                query_params.append(button_id)

                cursor.execute(
                    f"""
                    UPDATE button_configs 
                    SET {', '.join(set_clauses)}
                    WHERE menu_type = ? AND button_id = ?
                    """,
                    query_params,
                )
            conn.commit()
            return True
    except sqlite3.Error as e:
        logging.error(f"Failed to reorder button configs for {menu_type}: {e}")
        return False
# ==================================


# ===== UPDATE_EXISTING_MY_KEYS_BUTTON =====
def update_existing_my_keys_button():
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE button_configs SET button_id = 'my_keys' WHERE button_id = 'keys'")
            conn.commit()
            return True
    except sqlite3.Error:
        return False
# ==========================================


# ===== DELETE_BUTTON_CONFIG =====
def delete_button_config(button_id: int) -> bool:
    cursor = _exec("DELETE FROM button_configs WHERE id = ?", (button_id,), f"Не удалось удалить конфиг кнопки {button_id}")
    if cursor: logging.info(f"Конфиг кнопки {button_id} удалён"); return True
    return False
# ================================



# ===== CREATE_PLAN =====
def create_plan(host_name: str, plan_name: str, months: int, price: float, hwid_limit: int = 0, traffic_limit_gb: int = 0):
    host_name = normalize_host_name(host_name)
    cursor = _exec(
        "INSERT INTO plans (host_name, plan_name, months, price, hwid_limit, traffic_limit_gb) VALUES (?, ?, ?, ?, ?, ?)",
        (host_name, plan_name, months, price, hwid_limit, traffic_limit_gb),
        f"Не удалось создать тариф для хоста '{host_name}'"
    )
    if cursor: new_id = cursor.lastrowid; logging.info(f"Created new plan '{plan_name}' for host '{host_name}' with HWID={hwid_limit}, Traffic={traffic_limit_gb}GB."); return new_id
    return None
# =======================


# ===== GET_PLANS_FOR_HOST =====
def get_plans_for_host(host_name: str) -> list[dict]:
    host_name = normalize_host_name(host_name)
    rows = _fetch_list("SELECT * FROM plans WHERE TRIM(host_name) = TRIM(?) ORDER BY months", (host_name,), f"Не удалось получить тарифы для хоста '{host_name}'")
    return [dict(plan) for plan in rows]

# ==============================


# ===== GET_PLAN_BY_ID =====
def get_plan_by_id(plan_id: int) -> dict | None:
    row = _fetch_row("SELECT * FROM plans WHERE plan_id = ?", (plan_id,), f"Не удалось получить тариф по id '{plan_id}'")
    return dict(row) if row else None

# ==========================


# ===== DELETE_PLAN =====
def delete_plan(plan_id: int):
    cursor = _exec("DELETE FROM plans WHERE plan_id = ?", (plan_id,), f"Не удалось удалить тариф с id {plan_id}")
    if cursor: logging.info(f"Удалён тариф с id {plan_id}.")
# =======================


# ===== UPDATE_PLAN =====
def update_plan(plan_id: int, plan_name: str, months: int, price: float, hwid_limit: int = 0, traffic_limit_gb: int = 0) -> bool:
    cursor = _exec(
        "UPDATE plans SET plan_name = ?, months = ?, price = ?, hwid_limit = ?, traffic_limit_gb = ? WHERE plan_id = ?",
        (plan_name, months, price, hwid_limit, traffic_limit_gb, plan_id),
        f"Не удалось обновить тариф {plan_id}"
    )
    if cursor and cursor.rowcount > 0: logging.info(f"Updated plan {plan_id}: name='{plan_name}', months={months}, price={price}, hwid={hwid_limit}, traffic={traffic_limit_gb}."); return True
    if cursor and cursor.rowcount == 0: logging.warning(f"No plan updated for id {plan_id} (not found).")
    return False
# =======================


def register_user_if_not_exists(telegram_id: int, username: str, referrer_id):
    row = _fetch_row("SELECT referred_by FROM users WHERE telegram_id = ?", (telegram_id,), "")
    
    if not row:
        _exec(
            "INSERT INTO users (telegram_id, username, registration_date, referred_by) VALUES (?, ?, ?, ?)",
            (telegram_id, username, get_msk_time().replace(tzinfo=None).replace(microsecond=0), referrer_id),
            f"Не удалось зарегистрировать пользователя {telegram_id}"
        )
    else:
        _exec("UPDATE users SET username = ? WHERE telegram_id = ?", (username, telegram_id), "")
        
        current_ref = row['referred_by']
        if referrer_id and (current_ref is None or str(current_ref).strip() == "") and int(referrer_id) != int(telegram_id):
            _exec("UPDATE users SET referred_by = ? WHERE telegram_id = ?", (int(referrer_id), telegram_id), "")

def add_to_referral_balance(user_id: int, amount: float):
    _exec("UPDATE users SET referral_balance = referral_balance + ? WHERE telegram_id = ?", (amount, user_id), f"Не удалось добавить реферальный баланс для пользователя {user_id}")

def set_referral_balance(user_id: int, value: float):
    _exec("UPDATE users SET referral_balance = ? WHERE telegram_id = ?", (value, user_id), f"Не удалось установить реферальный баланс для пользователя {user_id}")

def set_referral_balance_all(user_id: int, value: float):
    _exec("UPDATE users SET referral_balance_all = ? WHERE telegram_id = ?", (value, user_id), f"Не удалось установить общий реф-баланс для пользователя {user_id}")

def add_to_referral_balance_all(user_id: int, amount: float):
    _exec(
        "UPDATE users SET referral_balance_all = referral_balance_all + ? WHERE telegram_id = ?",
        (amount, user_id),
        f"Не удалось добавить к общему реф-балансу для пользователя {user_id}"
    )

def get_referral_balance_all(user_id: int) -> float:
    row = _fetch_row("SELECT referral_balance_all FROM users WHERE telegram_id = ?", (user_id,), f"Не удалось получить общий реф-баланс для пользователя {user_id}")
    return row["referral_balance_all"] if row else 0.0

def get_referral_balance(user_id: int) -> float:
    row = _fetch_row("SELECT referral_balance FROM users WHERE telegram_id = ?", (user_id,), f"Не удалось получить реф-баланс для пользователя {user_id}")
    return row["referral_balance"] if row else 0.0

def get_balance(user_id: int) -> float:
    row = _fetch_row("SELECT balance FROM users WHERE telegram_id = ?", (user_id,), f"Не удалось получить баланс для пользователя {user_id}")
    return row["balance"] if row else 0.0

def adjust_user_balance(user_id: int, delta: float) -> bool:
    cursor = _exec(
        "UPDATE users SET balance = COALESCE(balance, 0) + ? WHERE telegram_id = ?",
        (float(delta), user_id),
        f"Не удалось изменить баланс для пользователя {user_id}"
    )
    return cursor is not None and cursor.rowcount > 0

def set_balance(user_id: int, value: float) -> bool:
    cursor = _exec(
        "UPDATE users SET balance = ? WHERE telegram_id = ?",
        (value, user_id),
        f"Не удалось установить баланс для пользователя {user_id}"
    )
    return cursor is not None and cursor.rowcount > 0

def add_to_balance(user_id: int, amount: float) -> bool:
    logging.info(f"💳 Добавляем {amount:.2f} RUB к балансу пользователя {user_id}")
    
    # Check if user exists first to match original logic logging
    row = _fetch_row("SELECT telegram_id, balance FROM users WHERE telegram_id = ?", (int(user_id),), "")
    if not row: logging.error(f"❌ Пользователь {user_id} не найден в базе данных"); return False

    old_balance = row["balance"] or 0.0
    
    cursor = _exec(
        "UPDATE users SET balance = COALESCE(balance, 0) + ? WHERE telegram_id = ?",
        (float(amount), int(user_id)),
        f"Ошибка базы данных при пополнении баланса для пользователя {user_id}"
    )
    
    if cursor and cursor.rowcount > 0:
        new_balance = old_balance + float(amount)
        logging.info(f"✅ Баланс обновлен: пользователь {user_id} | {old_balance:.2f} → {new_balance:.2f} RUB (+{amount:.2f})")
        return True
    
    logging.error(f"❌ Не удалось обновить баланс для пользователя {user_id}: строки не затронуты")
    return False

def deduct_from_balance(user_id: int, amount: float) -> bool:
    if amount <= 0: return True
        
    cursor = _exec(
        "UPDATE users SET balance = balance - ? WHERE telegram_id = ? AND balance >= ?",
        (amount, user_id, amount),
        f"Не удалось списать с баланса для пользователя {user_id}"
    )
    if cursor and cursor.rowcount > 0: return True
        
    return False
# ============================


# ===== DEDUCT_FROM_REFERRAL_BALANCE =====
def deduct_from_referral_balance(user_id: int, amount: float) -> bool:
    if amount <= 0: return True
    cursor = _exec(
        "UPDATE users SET referral_balance = referral_balance - ? WHERE telegram_id = ? AND referral_balance >= ?",
        (amount, user_id, amount),
        f"Не удалось списать с реферального баланса для пользователя {user_id}"
    )
    if cursor and cursor.rowcount > 0: return True
    return False
# ======================================


# ===== GET_REFERRAL_COUNT =====
def get_referral_count(user_id: int) -> int:
    row = _fetch_row("SELECT COUNT(*) as c FROM users WHERE referred_by = ?", (user_id,), f"Не удалось получить кол-во рефералов для пользователя {user_id}")
    return row["c"] if row else 0
# ==============================


# ===== GET_USER =====
def get_user(telegram_id: int):
    row = _fetch_row("SELECT * FROM users WHERE telegram_id = ?", (telegram_id,), f"Не удалось получить пользователя {telegram_id}")
    return dict(row) if row else None
# ==================


# ===== GET_TRANSACTION =====
def get_transaction(payment_id: str) -> dict | None:
    row = _fetch_row("SELECT * FROM transactions WHERE payment_id = ?", (payment_id,), f"Не удалось получить транзакцию {payment_id}")
    return dict(row) if row else None
# =========================


# ===== SET_TERMS_AGREED =====
def set_terms_agreed(telegram_id: int):
    cursor = _exec("UPDATE users SET agreed_to_terms = 1 WHERE telegram_id = ?", (telegram_id,), f"Не удалось установить согласие с условиями для пользователя {telegram_id}")
    if cursor: logging.info(f"Пользователь {telegram_id} согласился с условиями.")
# ==========================


# ===== UPDATE_USER_STATS =====
def update_user_stats(telegram_id: int, amount_spent: float, months_purchased: int):
    _exec("UPDATE users SET total_spent = total_spent + ?, total_months = total_months + ? WHERE telegram_id = ?", (amount_spent, months_purchased, telegram_id), f"Не удалось обновить статистику пользователя {telegram_id}")
# ===========================


# ===== GET_USER_COUNT =====
def get_user_count() -> int:
    row = _fetch_row("SELECT COUNT(*) as c FROM users", (), "Не удалось получить кол-во пользователей")
    return row["c"] if row else 0
# ========================


# ===== GET_TOTAL_KEYS_COUNT =====
def get_total_keys_count() -> int:
    row = _fetch_row("SELECT COUNT(*) as c FROM vpn_keys", (), "Не удалось получить кол-во ключей")
    return row["c"] if row else 0
# ==============================


# ===== GET_TOTAL_SPENT_SUM =====
def get_total_spent_sum() -> float:
    row = _fetch_row(
        """
        SELECT COALESCE(SUM(amount_rub), 0.0) as s
        FROM transactions
        WHERE LOWER(COALESCE(status, '')) IN ('paid', 'completed', 'success')
          AND LOWER(COALESCE(payment_method, '')) <> 'balance'
        """,
        (),
        "Не удалось получить общую сумму расходов"
    )
    return row["s"] if row else 0.0
# =============================


# ===== GET_TOTAL_SPENT_BY_METHOD =====
def get_total_spent_by_method(payment_method: str) -> float:
    val = _fetch_val(
        """
        SELECT COALESCE(SUM(amount_rub), 0.0)
        FROM transactions
        WHERE LOWER(COALESCE(status, '')) IN ('paid', 'completed', 'success')
          AND LOWER(payment_method) = LOWER(?)
        """,
        (payment_method,),
        0.0,
        f"Не удалось получить доход по методу {payment_method}"
    )
    return float(val) if val is not None else 0.0
# ===================================


# ===== CREATE_PENDING_TRANSACTION =====
def create_pending_transaction(payment_id: str, user_id: int, amount_rub: float, metadata: dict) -> int:
    cursor = _exec(
        "INSERT INTO transactions (payment_id, user_id, status, amount_rub, metadata) VALUES (?, ?, ?, ?, ?)",
        (payment_id, user_id, 'pending', amount_rub, json.dumps(metadata)),
        f"Не удалось создать ожидающую транзакцию для пользователя {user_id}"
    )
    return cursor.lastrowid if cursor else 0
# ====================================


# ===== LOG_TRANSACTION_SIMPLE =====
def log_transaction_simple(user_id: int, amount: float, method: str, description: str) -> bool:
    logging.info(f"📝 Логирование транзакции: user={user_id}, amount={amount}, method={method}")
    cursor = _exec(
        """
        INSERT INTO transactions (user_id, amount_rub, payment_method, status, description, created_date)
        VALUES (?, ?, ?, 'paid', ?, ?)
        """,
        (user_id, amount, method, description, get_msk_time().replace(tzinfo=None).replace(microsecond=0)),
        f"Не удалось залогировать транзакцию для пользователя {user_id}"
    )
    if cursor: logging.info(f"✅ Транзакция успешно сохранена для пользователя {user_id}"); return True
    return False
# ==================================

# ===== FIND_AND_COMPLETE_TON_TRANSACTION =====
def find_and_complete_ton_transaction(payment_id: str, amount_ton: float) -> dict | None:
    row = _fetch_row("SELECT * FROM transactions WHERE payment_id = ? AND status = 'pending'", (payment_id,), "")
    if not row: logging.warning(f"TON Webhook: Получен платеж для неизвестного или уже обработанного payment_id: {payment_id}"); return None
        
    cursor = _exec(
        "UPDATE transactions SET status = 'paid', amount_currency = ?, currency_name = 'TON', payment_method = 'TON' WHERE payment_id = ?",
        (amount_ton, payment_id),
        f"Не удалось завершить TON-транзакцию {payment_id}"
    )
    
    if cursor and cursor.rowcount > 0:
        try: return json.loads(row['metadata'])
        except Exception: return {}

    return None
# ===============================================


# ===== LOG_TRANSACTION =====
def log_transaction(username: str, transaction_id: str | None, payment_id: str | None, user_id: int, status: str, amount_rub: float, amount_currency: float | None, currency_name: str | None, payment_method: str, metadata: str):
    _exec(
        """INSERT INTO transactions
           (username, transaction_id, payment_id, user_id, status, amount_rub, amount_currency, currency_name, payment_method, metadata, created_date)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (username, transaction_id, payment_id, user_id, status, amount_rub, amount_currency, currency_name, payment_method, metadata, get_msk_time().replace(tzinfo=None).replace(microsecond=0)),
        f"Не удалось залогировать транзакцию для пользователя {user_id}"
    )
# ===========================

# ===== CHECK_TRANSACTION_EXISTS =====
def check_transaction_exists(payment_id: str) -> bool:
    row = _fetch_row("SELECT 1 as ex FROM transactions WHERE payment_id = ? LIMIT 1", (payment_id,), f"Не удалось проверить транзакцию {payment_id}")
    return bool(row)

def get_paginated_transactions(page: int = 1, per_page: int = 15) -> tuple[list[dict], int]:
    offset = (page - 1) * per_page
    transactions = []
    total = 0

    r_count = _fetch_row("SELECT COUNT(*) as c FROM transactions", (), "Не удалось получить кол-во транзакций")
    total = r_count["c"] if r_count else 0

    query = "SELECT * FROM transactions ORDER BY created_date DESC LIMIT ? OFFSET ?"
    rows = _fetch_list(query, (per_page, offset), "Не удалось получить страницу транзакций")

    for row in rows:
        transaction_dict = dict(row)
        
        metadata_str = transaction_dict.get('metadata')
        if metadata_str:
            try:
                metadata = json.loads(metadata_str)
                transaction_dict['action'] = metadata.get('action')
                transaction_dict['host_name'] = metadata.get('host_name', 'N/A')
                transaction_dict['plan_name'] = metadata.get('plan_name', 'N/A')
            except json.JSONDecodeError:
                transaction_dict['action'] = None
                transaction_dict['host_name'] = 'Error'
                transaction_dict['plan_name'] = 'Error'
        else:
            transaction_dict['host_name'] = 'N/A'
            transaction_dict['plan_name'] = 'N/A'
        
        transactions.append(transaction_dict)
    
    return transactions, total
# ==========================================


# ===== SET_TRIAL_USED =====
def set_trial_used(telegram_id: int):
    cursor = _exec("UPDATE users SET trial_used = 1 WHERE telegram_id = ?", (telegram_id,), f"Не удалось установить trial_used для пользователя {telegram_id}")
    if cursor: logging.info(f"Пробный период отмечен как использованный для пользователя {telegram_id}.")
# ========================


# ===== ADD_NEW_KEY =====
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
    comment_key: str | None = None,
    created_at_ms: int | None = None,
) -> int | None:
    host_name_norm = normalize_host_name(host_name) if host_name else None
    email_normalized = _normalize_email(key_email) or key_email.strip()
    expire_str = _to_datetime_str(expiry_timestamp_ms) or _now_str()
    created_str = _to_datetime_str(created_at_ms) or _now_str() if created_at_ms is not None else _now_str()
    strategy_value = traffic_limit_strategy or "NO_RESET"
    
    cursor = _exec(
        """
        INSERT INTO vpn_keys (
            user_id, host_name, squad_uuid, remnawave_user_uuid, short_uuid, email, key_email,
            subscription_url, expire_at, created_at, updated_at, traffic_limit_bytes,
            traffic_limit_strategy, tag, description, comment_key
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            user_id, host_name_norm, squad_uuid, remnawave_user_uuid, short_uuid, email_normalized,
            email_normalized, subscription_url, expire_str, created_str, created_str,
            traffic_limit_bytes, strategy_value, tag, description, comment_key
        ),
        f"Не удалось добавить новый ключ для пользователя {user_id}"
    )
    return cursor.lastrowid if cursor else None
# =======================


# ===== _APPLY_KEY_UPDATES =====
def _apply_key_updates(key_id: int, updates: dict[str, Any]) -> bool:
    if not updates: return False
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
    except sqlite3.Error as e: logging.error("Не удалось обновить ключ %s: %s", key_id, e); return False
# ==============================


# ===== UPDATE_KEY_FIELDS =====
def update_key_fields(
    key_id: int,
    *,
    user_id: int | None = None,
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
    comment_key: str | None = None,
) -> bool:
    updates: dict[str, Any] = {}
    if user_id is not None:
        updates["user_id"] = user_id
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
    if comment_key is not None:
        updates["comment_key"] = comment_key
    return _apply_key_updates(key_id, updates)
# ===========================


# ===== DELETE_KEY_BY_EMAIL =====
def delete_key_by_email(email: str) -> bool:
    lookup = _normalize_email(email) or email.strip()
    cursor = _exec(
        "DELETE FROM vpn_keys WHERE email = ? OR key_email = ?",
        (lookup, lookup),
        "Не удалось удалить ключ"
    )
    if cursor: logger.debug("delete_key_by_email('%s') affected=%s", email, cursor.rowcount); return cursor.rowcount > 0
    return False
# ===========================


# ===== GET_USER_KEYS =====
def get_user_keys(user_id: int) -> list[dict]:
    rows = _fetch_list(
        "SELECT * FROM vpn_keys WHERE user_id = ? ORDER BY datetime(created_at) DESC, key_id DESC",
        (user_id,),
        f"Не удалось получить ключи для пользователя {user_id}"
    )
    return [_normalize_key_row(row) for row in rows]
# ===========================


# ===== GET_KEY_BY_ID =====
def get_key_by_id(key_id: int) -> dict | None:
    row = _fetch_row(
        "SELECT * FROM vpn_keys WHERE key_id = ?",
        (key_id,),
        f"Не удалось получить ключ по ID {key_id}"
    )
    return _normalize_key_row(row)
# =========================


# ===== GET_KEY_BY_EMAIL =====
def get_key_by_email(key_email: str) -> dict | None:
    lookup = _normalize_email(key_email) or key_email.strip()
    row = _fetch_row(
        "SELECT * FROM vpn_keys WHERE email = ? OR key_email = ?",
        (lookup, lookup),
        f"Не удалось получить ключ по email {key_email}"
    )
    return _normalize_key_row(row)
# =================================


# ===== GET_KEY_BY_REMNAWAVE_UUID =====
def get_key_by_remnawave_uuid(remnawave_uuid: str) -> dict | None:
    if not remnawave_uuid: return None
    normalized_uuid = remnawave_uuid.strip()
    row = _fetch_row(
        "SELECT * FROM vpn_keys WHERE remnawave_user_uuid = ? LIMIT 1",
        (normalized_uuid,),
        f"Не удалось получить ключ по remnawave uuid {remnawave_uuid}"
    )
    return _normalize_key_row(row)
# ===========================


# ===== UPDATE_KEY_INFO =====
def update_key_info(key_id: int, new_remnawave_uuid: str, new_expiry_ms: int, **kwargs) -> bool:
    return update_key_fields(
        key_id,
        remnawave_user_uuid=new_remnawave_uuid,
        expire_at_ms=new_expiry_ms,
        **kwargs,
    )


# ===== UPDATE_KEY_HOST_AND_INFO =====
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


# ===== GET_NEXT_KEY_NUMBER =====
def get_next_key_number(user_id: int) -> int:
    count = _fetch_val("SELECT COUNT(*) FROM vpn_keys WHERE user_id = ?", (user_id,), 0)
    return int(count) + 1
# ===========================


# ===== GET_KEYS_FOR_HOST =====
def get_keys_for_host(host_name: str) -> list[dict]:
    host_name_normalized = normalize_host_name(host_name)
    rows = _fetch_list(
        "SELECT * FROM vpn_keys WHERE TRIM(host_name) = TRIM(?)",
        (host_name_normalized,),
        f"Не удалось получить ключи для хоста '{host_name}'"
    )
    return [_normalize_key_row(row) for row in rows]
# =============================


# ===== GET_ALL_VPN_USERS =====
def get_all_vpn_users() -> list[dict]:
    return _fetch_list("SELECT DISTINCT user_id FROM vpn_keys", (), "Не удалось получить всех VPN пользователей")
# ===========================


# ===== UPDATE_KEY_STATUS_FROM_SERVER =====
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
                    except Exception: expiry_ms = None
            else:
                remote_uuid = getattr(client_data, 'id', None) or getattr(client_data, 'uuid', None)
                expiry_ms = getattr(client_data, 'expiry_time', None)
                subscription_url = getattr(client_data, 'subscription_url', None)
            if not existing: return False
            return update_key_fields(
                existing['key_id'],
                remnawave_user_uuid=remote_uuid,
                expire_at_ms=expiry_ms,
                subscription_url=subscription_url,
            )
        if existing: return delete_key_by_email(normalized_email)
        return True
    except sqlite3.Error as e: logging.error("Не удалось обновить статус ключа для %s: %s", key_email, e); return False
# ===========================


# ===== GET_DAILY_STATS_FOR_CHARTS =====
def get_daily_stats_for_charts(days: int = 30) -> dict:
    stats = {'users': {}, 'keys': {}, 'income': {}}
    time_filter = ""
    params = []
    group_fmt = "%Y-%m-%d"
    
    if days > 0:
        time_filter = " >= datetime('now', '+3 hours', ?)"
        params.append(f'-{days} days')
        if days == 1: group_fmt = "%Y-%m-%d %H:00"
    
    def get_data(table, date_col, is_count=True):
        nonlocal group_fmt
        where_clause = f"WHERE {date_col} {time_filter}" if time_filter else ""
        
        if is_count:
            query = f"SELECT STRFTIME('{group_fmt}', {date_col}) AS period, COUNT(*) as cnt FROM {table} {where_clause} GROUP BY period ORDER BY period"
        else:
            income_filter = "LOWER(COALESCE(status, '')) IN ('paid', 'completed', 'success') AND LOWER(COALESCE(payment_method, '')) <> 'balance'"
            if where_clause:
                where_clause += f" AND {income_filter}"
            else:
                where_clause = f"WHERE {income_filter}"
            query = f"SELECT STRFTIME('{group_fmt}', {date_col}) AS period, payment_method, SUM(amount_rub) as total FROM {table} {where_clause} GROUP BY period, payment_method ORDER BY period"
        
        return _fetch_list(query, tuple(params), "Не удалось получить данные статистики по дням")

    for row in get_data("users", "registration_date"):
        stats['users'][row['period']] = row['cnt']

    for row in get_data("vpn_keys", "COALESCE(created_at, updated_at, CURRENT_TIMESTAMP)"):
        stats['keys'][row['period']] = row['cnt']

    for row in get_data("transactions", "created_date", is_count=False):
        period = row['period']
        method = row['payment_method']
        amount = row['total']
        if period not in stats['income']:
            stats['income'][period] = {}
        stats['income'][period][method or 'Other'] = float(amount) if amount else 0.0
    return stats
# ==========================


# ===== GET_RECENT_TRANSACTIONS =====
def get_recent_transactions(limit: int = 15) -> list[dict]:
    query = """
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
    """
    rows = _fetch_list(query, (limit,), "Не удалось получить последние транзакции")
    return rows


# ===== GET_ALL_USERS =====
# Получение всех пользователей с сортировкой по дате регистрации
def get_all_users() -> list[dict]:
    return _fetch_list("SELECT * FROM users ORDER BY registration_date DESC", (), "Не удалось получить всех пользователей")

    return rows
# ===================================


# ===== GET_USERS_PAGINATED =====
def get_users_paginated(page: int = 1, per_page: int = 30, q: str | None = None) -> tuple[list[dict], int]:
    """Вернуть пользователей постранично и общее количество (с учётом фильтра).

    Фильтр q ищет по username (LIKE) и по текстовому представлению telegram_id.
    """
    page = max(1, int(page or 1))
    per_page = max(1, int(per_page or 30))
    offset = (page - 1) * per_page
    
    if q:
        q_like = f"%{q.strip()}%"
        
        count_query = """
            SELECT COUNT(*)
            FROM users
            WHERE (username LIKE ?)
               OR (CAST(telegram_id AS TEXT) LIKE ?)
        """
        total = _fetch_val(count_query, (q_like, q_like), 0, "Не удалось подсчитать пользователей с фильтром") or 0

        data_query = """
            SELECT *
            FROM users
            WHERE (username LIKE ?)
               OR (CAST(telegram_id AS TEXT) LIKE ?)
            ORDER BY is_pinned DESC, registration_date DESC
            LIMIT ? OFFSET ?
        """
        users = _fetch_list(data_query, (q_like, q_like, per_page, offset), "Не удалось получить страницу пользователей с фильтром")
    else:
        total = _fetch_val("SELECT COUNT(*) FROM users", (), 0, "Не удалось подсчитать пользователей") or 0
        
        data_query = "SELECT * FROM users ORDER BY is_pinned DESC, registration_date DESC LIMIT ? OFFSET ?"
        users = _fetch_list(data_query, (per_page, offset), "Не удалось получить страницу пользователей")

    return users, total


    return users, total
# ========================


# ===== TOGGLE_USER_PIN =====
def toggle_user_pin(user_id: int) -> bool:
    cursor = _exec(
        "UPDATE users SET is_pinned = NOT COALESCE(is_pinned, 0) WHERE telegram_id = ?",
        (user_id,),
        f"Не удалось переключить закреп для пользователя {user_id}"
    )
    return cursor is not None and cursor.rowcount > 0

    return cursor is not None and cursor.rowcount > 0
# ===========================


# ===== GET_KEYS_COUNTS_FOR_USERS =====
def get_keys_counts_for_users(user_ids: list[int]) -> dict[int, int]:
    result: dict[int, int] = {}
    if not user_ids: return result

    placeholders = ",".join(["?"] * len(user_ids))
    query = f"SELECT user_id, COUNT(*) AS cnt FROM vpn_keys WHERE user_id IN ({placeholders}) GROUP BY user_id"
    
    rows = _fetch_list(query, tuple(int(x) for x in user_ids), "Не удалось получить кол-во ключей для пользователей")
    
    for row in rows: result[int(row['user_id'])] = int(row['cnt'] or 0)
        
    return result

# ===== BAN_USER =====
# Установка флага is_banned=1 для пользователя
def ban_user(telegram_id: int):
    _exec("UPDATE users SET is_banned = 1 WHERE telegram_id = ?", (telegram_id,), f"Не удалось забанить пользователя {telegram_id}")

# ===== UNBAN_USER =====
# Снятие бана (is_banned=0) для пользователя
def unban_user(telegram_id: int):
    _exec("UPDATE users SET is_banned = 0 WHERE telegram_id = ?", (telegram_id,), f"Не удалось разбанить пользователя {telegram_id}")

# ===== DELETE_USER_KEYS =====
# Удаление всех ключей пользователя
def delete_user_keys(user_id: int):
    _exec("DELETE FROM vpn_keys WHERE user_id = ?", (user_id,), f"Не удалось удалить ключи пользователя {user_id}")

# ===== CREATE_SUPPORT_TICKET =====
def create_support_ticket(user_id: int, subject: str | None = None) -> int | None:
    row = _fetch_row(
        "SELECT ticket_id FROM support_tickets WHERE user_id = ? AND status = 'open' ORDER BY updated_at DESC LIMIT 1",
        (user_id,),
        ""
    )
    if row and row['ticket_id']: return int(row['ticket_id'])

    cursor = _exec(
        "INSERT INTO support_tickets (user_id, subject) VALUES (?, ?)",
        (user_id, subject),
        f"Не удалось создать тикет поддержки для пользователя {user_id}"
    )
    return cursor.lastrowid if cursor else None

    return cursor.lastrowid if cursor else None
# ===========================


# ===== GET_OR_CREATE_OPEN_TICKET =====
def get_or_create_open_ticket(user_id: int, subject: str | None = None) -> tuple[int | None, bool]:
    row = _fetch_row(
        "SELECT ticket_id FROM support_tickets WHERE user_id = ? AND status = 'open' ORDER BY updated_at DESC LIMIT 1",
        (user_id,),
        f"Не удалось получить тикет для пользователя {user_id}"
    )
    if row and row['ticket_id']: return int(row['ticket_id']), False
    cursor = _exec(
        "INSERT INTO support_tickets (user_id, subject) VALUES (?, ?)",
        (user_id, subject),
        f"Не удалось создать/получить тикет для пользователя {user_id}"
    )
    if cursor and cursor.lastrowid: return int(cursor.lastrowid), True
    return None, False

    return None, False
# ===================================


# ===== ADD_SUPPORT_MESSAGE =====
def add_support_message(ticket_id: int, sender: str, content: str) -> int | None:
    cursor = _exec(
        "INSERT INTO support_messages (ticket_id, sender, content) VALUES (?, ?, ?)",
        (ticket_id, sender, content),
        f"Не удалось добавить сообщение в тикет {ticket_id}"
    )
    if cursor and cursor.lastrowid: mid = cursor.lastrowid; _exec("UPDATE support_tickets SET updated_at = CURRENT_TIMESTAMP WHERE ticket_id = ?", (ticket_id,), "Не удалось обновить время тикета"); return mid
    return None
# =============================


# ===== UPDATE_TICKET_THREAD_INFO =====
def update_ticket_thread_info(ticket_id: int, forum_chat_id: str | None, message_thread_id: int | None) -> bool:
    cursor = _exec(
        "UPDATE support_tickets SET forum_chat_id = ?, message_thread_id = ?, updated_at = CURRENT_TIMESTAMP WHERE ticket_id = ?",
        (forum_chat_id, message_thread_id, ticket_id),
        f"Не удалось обновить инфо о треде для тикета {ticket_id}"
    )
    return cursor is not None and cursor.rowcount > 0

    return cursor is not None and cursor.rowcount > 0
# =================================


# ===== GET_TICKET =====
def get_ticket(ticket_id: int) -> dict | None:
    return _fetch_row("SELECT * FROM support_tickets WHERE ticket_id = ?", (ticket_id,), f"Не удалось получить тикет {ticket_id}")

    return _fetch_row("SELECT * FROM support_tickets WHERE ticket_id = ?", (ticket_id,), f"Не удалось получить тикет {ticket_id}")
# ==================


# ===== GET_TICKET_BY_THREAD =====
def get_ticket_by_thread(forum_chat_id: str, message_thread_id: int) -> dict | None:
    return _fetch_row(
        "SELECT * FROM support_tickets WHERE forum_chat_id = ? AND message_thread_id = ?",
        (str(forum_chat_id), int(message_thread_id)),
        f"Не удалось получить тикет по треду {forum_chat_id}/{message_thread_id}"
    )

    return _fetch_row(
        "SELECT * FROM support_tickets WHERE forum_chat_id = ? AND message_thread_id = ?",
        (str(forum_chat_id), int(message_thread_id)),
        f"Не удалось получить тикет по треду {forum_chat_id}/{message_thread_id}"
    )
# ============================


# ===== GET_USER_TICKETS =====
def get_user_tickets(user_id: int, status: str | None = None) -> list[dict]:
    if status:
        return _fetch_list(
            "SELECT * FROM support_tickets WHERE user_id = ? AND status = ? ORDER BY updated_at DESC",
            (user_id, status),
            f"Не удалось получить тикеты для пользователя {user_id}"
        )
    return _fetch_list(
        "SELECT * FROM support_tickets WHERE user_id = ? ORDER BY updated_at DESC",
        (user_id,),
        f"Не удалось получить тикеты для пользователя {user_id}"
    )

    return _fetch_list(
        "SELECT * FROM support_tickets WHERE user_id = ? ORDER BY updated_at DESC",
        (user_id,),
        f"Не удалось получить тикеты для пользователя {user_id}"
    )
# ============================


# ===== GET_TICKET_MESSAGES =====
def get_ticket_messages(ticket_id: int) -> list[dict]:
    return _fetch_list(
        "SELECT * FROM support_messages WHERE ticket_id = ? ORDER BY created_at ASC",
        (ticket_id,),
        f"Не удалось получить сообщения для тикета {ticket_id}"
    )
# ===============================


# ===== SET_TICKET_STATUS =====
def set_ticket_status(ticket_id: int, status: str) -> bool:
    cursor = _exec(
        "UPDATE support_tickets SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE ticket_id = ?",
        (status, ticket_id),
        f"Не удалось установить статус '{status}' для тикета {ticket_id}"
    )
    return cursor is not None and cursor.rowcount > 0

    return cursor is not None and cursor.rowcount > 0
# ===========================


# ===== UPDATE_TICKET_SUBJECT =====
def update_ticket_subject(ticket_id: int, subject: str) -> bool:
    cursor = _exec(
        "UPDATE support_tickets SET subject = ?, updated_at = CURRENT_TIMESTAMP WHERE ticket_id = ?",
        (subject, ticket_id),
        f"Не удалось обновить тему для тикета {ticket_id}"
    )
    return cursor is not None and cursor.rowcount > 0

    return cursor is not None and cursor.rowcount > 0
# ===============================


# ===== DELETE_TICKET =====
def delete_ticket(ticket_id: int) -> bool:
    _exec("DELETE FROM support_messages WHERE ticket_id = ?", (ticket_id,), "Не удалось удалить сообщения тикета")
    cursor = _exec("DELETE FROM support_tickets WHERE ticket_id = ?", (ticket_id,), f"Не удалось удалить тикет {ticket_id}")
    return cursor is not None and cursor.rowcount > 0

    return cursor is not None and cursor.rowcount > 0
# ===========================


# ===== GET_TICKETS_PAGINATED =====
def get_tickets_paginated(page: int = 1, per_page: int = 20, status: str | None = None) -> tuple[list[dict], int]:
    offset = (page - 1) * per_page
    
    if status:
        total = _fetch_val("SELECT COUNT(*) FROM support_tickets WHERE status = ?", (status,), 0) or 0
        where_clause = " WHERE t.status = ?"
        params = [status]
    else:
        total = _fetch_val("SELECT COUNT(*) FROM support_tickets", (), 0) or 0
        where_clause = ""
        params = []
    
    base_query = """
        SELECT t.*, 
               u.username,
               (SELECT sender FROM support_messages 
                WHERE ticket_id = t.ticket_id 
                ORDER BY created_at DESC LIMIT 1) as last_sender
        FROM support_tickets t
        LEFT JOIN users u ON t.user_id = u.telegram_id
    """
    
    order_clause = """
        ORDER BY 
        CASE 
            WHEN t.status = 'open' AND (
                SELECT sender FROM support_messages 
                WHERE ticket_id = t.ticket_id 
                ORDER BY created_at DESC LIMIT 1
            ) != 'admin' THEN 1
            WHEN t.status = 'open' THEN 2
            ELSE 3
        END ASC,
        t.updated_at DESC
    """
    
    full_query = base_query + where_clause + order_clause + " LIMIT ? OFFSET ?"
    params.extend([per_page, offset])
    
    rows = _fetch_list(full_query, tuple(params), "Не удалось получить страницу тикетов поддержки")
    return rows, total
# ===========================


# ===== GET_OPEN_TICKETS_COUNT =====
def get_open_tickets_count() -> int:
    return _fetch_val("SELECT COUNT(*) FROM support_tickets WHERE status = 'open'", (), 0) or 0
# ==============================


# ===== GET_WAITING_TICKETS_COUNT =====
def get_waiting_tickets_count() -> int:
    query = """
        SELECT COUNT(*) FROM support_tickets t
        WHERE t.status = 'open' AND (
            SELECT sender FROM support_messages 
            WHERE ticket_id = t.ticket_id 
            ORDER BY created_at DESC LIMIT 1
        ) != 'admin'
    """
    return _fetch_val(query, (), 0, "Не удалось получить кол-во ожидающих тикетов")
# ===================================


# ===== GET_SUPPORT_BADGE_COUNTS =====
def get_support_badge_counts() -> dict:
    """Универсальная функция для получения всех счетчиков бейджей в один запрос."""
    try:
        # Получаем общее количество открытых тикетов
        open_count = _fetch_val("SELECT COUNT(*) FROM support_tickets WHERE status = 'open'", (), 0) or 0
        
        # Получаем количество тикетов, ожидающих ответа админа (последнее сообщение не от админа)
        waiting_count = _fetch_val("""
            SELECT COUNT(*) FROM support_tickets t
            WHERE t.status = 'open' AND (
                SELECT sender FROM support_messages 
                WHERE ticket_id = t.ticket_id 
                ORDER BY created_at DESC LIMIT 1
            ) != 'admin'
        """, (), 0) or 0
        
        return {
            "ok": True,
            "open_count": open_count,
            "waiting_tickets_count": waiting_count
        }
    except Exception as e:
        logger.error(f"Ошибка при получении счетчиков бейджей: {e}")
        return {"ok": False, "error": str(e), "open_count": 0, "waiting_tickets_count": 0}


# ===== GET_CLOSED_TICKETS_COUNT =====
def get_closed_tickets_count() -> int:
    return _fetch_val("SELECT COUNT(*) FROM support_tickets WHERE status = 'closed'", (), 0) or 0
# ==================================


# ===== GET_ALL_TICKETS_COUNT =====
def get_all_tickets_count() -> int:
    return _fetch_val("SELECT COUNT(*) FROM support_tickets", (), 0) or 0
# ===============================


# ===== GET_OTHER_VALUE =====
def get_other_value(key: str) -> str | None:
    return _fetch_val("SELECT value FROM other WHERE key = ?", (key,), None, f"Не удалось получить other-значение для {key}")


    return _fetch_val("SELECT value FROM other WHERE key = ?", (key,), None, f"Не удалось получить other-значение для {key}")
# =========================


# ===== SET_OTHER_VALUE =====
def set_other_value(key: str, value: str) -> bool:
    cursor = _exec(
        "INSERT OR REPLACE INTO other (key, value) VALUES (?, ?)",
        (key, value),
        f"Не удалось установить other-значение для {key}"
    )
    return cursor is not None




    return cursor is not None
# =======================


# ===== UPDATE_SSH_TARGET_SCHEDULER =====
def update_ssh_target_scheduler(target_name: str, time_auto: str) -> bool:
    name = normalize_host_name(target_name)
    cursor = _exec(
        "UPDATE speedtest_ssh_targets SET time_auto = ? WHERE TRIM(target_name) = TRIM(?)",
        (time_auto, name),
        f"Не удалось обновить планировщик для '{target_name}'"
    )
    return cursor is not None and cursor.rowcount > 0
# ===================================


# ===== UPDATE_HOST_SORT_ORDER =====
def update_host_sort_order(host_name: str, sort_order: int) -> bool:
    name = normalize_host_name(host_name)
    cursor = _exec(
        "UPDATE xui_hosts SET sort_order = ? WHERE TRIM(host_name) = TRIM(?)",
        (sort_order, name),
        "Не удалось обновить sort_order хоста"
    )
    if cursor and cursor.rowcount > 0: logging.info(f"Обновлён sort_order хоста '{name}': {sort_order}"); return True
    logging.warning(f"Хост '{name}' не найден для обновления sort_order"); return False
# ==============================


# ===== UPDATE_SSH_TARGET_SORT_ORDER =====
def update_ssh_target_sort_order(target_name: str, sort_order: int) -> bool:
    name = normalize_host_name(target_name)
    cursor = _exec(
        "UPDATE speedtest_ssh_targets SET sort_order = ? WHERE TRIM(target_name) = TRIM(?)",
        (sort_order, name),
        "Не удалось обновить sort_order SSH-цели"
    )
    if cursor and cursor.rowcount > 0: logging.info(f"Обновлён sort_order SSH-цели '{name}': {sort_order}"); return True
    logging.warning(f"SSH-цель '{name}' не найдена для обновления sort_order"); return False
# ====================================


# ===== GET_OTHER_SETTING =====
def get_other_setting(key: str, default: Any = None) -> Any:
    val = get_other_value(key)
    return val if val is not None else default
# =========================


# ===== UPDATE_OTHER_SETTING =====
def update_other_setting(key: str, value: Any) -> bool:
    return set_other_value(key, str(value))


def get_all_other_settings() -> dict:
    with sqlite3.connect(DB_FILE) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT key, value FROM other")
        return {row['key']: row['value'] for row in cursor.fetchall()}

# ===========================================
# ===== WEBAPP SETTINGS =====
# Проверка и получение настроек веб-приложения
def get_webapp_settings() -> dict:
    row = _fetch_row("SELECT * FROM webapp_settings WHERE id = 1")
    if not row:
        _ensure_webapp_settings_table(sqlite3.connect(DB_FILE).cursor())
        row = _fetch_row("SELECT * FROM webapp_settings WHERE id = 1")
    # Преобразуем sqlite3.Row в обычный словарь
    return dict(row) if row else {}

# Обновление настроек веб-приложения
def update_webapp_settings(webapp_title: str = None, webapp_domen: str = None, webapp_enable: int = None, webapp_logo: str = None, webapp_icon: str = None) -> bool:
    try:
        updates = []
        params = []
        if webapp_title is not None:
            updates.append("webapp_title = ?")
            params.append(webapp_title)
        if webapp_domen is not None:
            updates.append("webapp_domen = ?")
            params.append(webapp_domen)
        if webapp_enable is not None:
            updates.append("webapp_enable = ?")
            params.append(int(webapp_enable))
        if webapp_logo is not None:
            updates.append("webapp_logo = ?")
            params.append(webapp_logo)
        if webapp_icon is not None:
            updates.append("webapp_icon = ?")
            params.append(webapp_icon)
        
        if not updates:
            return False
        
        # Строим SQL запрос
        sql = f"UPDATE webapp_settings SET {', '.join(updates)} WHERE id = 1"
        return _exec(sql, tuple(params))
    except Exception as e:
        logging.error(f"Ошибка при обновлении настроек webapp: {e}")
        return False
def update_user_auth_token(user_id: int, token: str | None) -> bool:
    return _exec("UPDATE users SET auth_token = ? WHERE telegram_id = ?", (token, user_id), "Failed to update auth_token") is not None

def get_user_by_auth_token(token: str) -> dict | None:
    if not token: return None
    return _fetch_row("SELECT * FROM users WHERE auth_token = ?", (token,), "Failed to get user by auth_token")

def get_auth_token_by_user_id(user_id: int) -> str | None:
    row = _fetch_row("SELECT auth_token FROM users WHERE telegram_id = ?", (user_id,), "Failed to get auth_token by user_id")
    return row["auth_token"] if row else None
