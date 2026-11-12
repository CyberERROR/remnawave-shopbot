import asyncio
import copy
import json
import logging
import re
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from html import escape
from html.parser import HTMLParser
from urllib.parse import urlparse

from aiogram import F, Router
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, LabeledPrice
from aiogram.utils.keyboard import InlineKeyboardBuilder
from flask import current_app, jsonify, render_template, request

from shop_bot.data_manager import remnawave_repository as rw_repo
from shop_bot.data_manager.database import DB_FILE

logger = logging.getLogger(__name__)

SETTINGS_KEY = "automatic_notifications"
EVENT_TYPES = ("trial", "unpaid", "expiry")
MSK_TZ = timezone(timedelta(hours=3), name="MSK")
DEFAULT_CONFIG = {
    "trial": {
        "enabled": False,
        "start_delay_minutes": 5,
        "repeat_interval_minutes": 1440,
        "active_duration_minutes": 259200,
        "text": "🎁 <b>Для вас открыт пробный период</b>\n\nПопробуйте все возможности сервиса бесплатно. Активируйте пробный доступ одним нажатием.",
        "columns": 1,
        "buttons": [
            {"text": "🎁 Активировать пробный период", "action": "trial", "url": "", "emoji_id": ""}
        ],
    },
    "unpaid": {
        "enabled": False,
        "start_delay_minutes": 5,
        "repeat_interval_minutes": 5,
        "active_duration_minutes": 25,
        "text": "💳 <b>У вас есть неоплаченный счёт</b>\n\nТариф: <b>{plan_name}</b>\nСумма: <b>{amount} ₽</b>\nСпособ оплаты: <b>{payment_method}</b>\n\nЗавершите оплату, чтобы активировать выбранные услуги.",
        "columns": 1,
        "buttons": [
            {"text": "💳 Оплатить счёт", "action": "payment", "url": "", "emoji_id": ""}
        ],
    },
    "expiry": {
        "enabled": True,
        "start_before_minutes": 4320,
        "repeat_interval_minutes": 1440,
        "stop_before_minutes": 0,
        "text": "⚠️ <b>Срок действия подписки скоро закончится</b>\n\nКлюч: <b>{key_name}</b>\nОсталось: <b>{time_left}</b>\nДата окончания: <b>{expiry_date}</b>\n\nПродлите подписку, чтобы не остаться без доступа к VPN.",
        "columns": 2,
        "buttons": [
            {"text": "➕ Продлить ключ", "action": "extend", "url": "", "emoji_id": ""},
            {"text": "🔑 Мои ключи", "action": "manage_keys", "url": "", "emoji_id": ""}
        ],
    },
}

_storage_ready = False
_storage_lock = threading.Lock()

TRIAL_USER_CONDITIONS = """
    COALESCE(u.trial_used, 0) = 0
    AND COALESCE(u.is_banned, 0) = 0
    AND COALESCE(u.total_spent, 0) <= 0
    AND NOT EXISTS (
        SELECT 1 FROM automatic_notifications blocked
        WHERE blocked.user_id = u.telegram_id AND blocked.event_type = 'trial' AND blocked.status = 'blocked'
    )
    AND NOT EXISTS (
        SELECT 1 FROM vpn_keys k
        WHERE k.user_id = u.telegram_id
          AND SUBSTR(LOWER(COALESCE(NULLIF(k.key_email, ''), k.email, '')), 1, 6) <> 'trial_'
    )
"""


class TelegramHTMLValidator(HTMLParser):
    allowed_tags = {
        "b", "strong", "i", "em", "u", "ins", "s", "strike", "del",
        "a", "code", "pre", "blockquote", "tg-spoiler", "tg-emoji", "span",
    }
    allowed_attributes = {
        "a": {"href"},
        "tg-emoji": {"emoji-id"},
        "blockquote": {"expandable"},
        "span": {"class"},
        "code": {"class"},
    }

    def __init__(self):
        super().__init__(convert_charrefs=False)
        self.stack = []

    def handle_starttag(self, tag, attrs):
        tag = tag.lower()
        if tag not in self.allowed_tags:
            raise ValueError(f"Тег <{tag}> не поддерживается Telegram")
        attributes = dict(attrs)
        allowed = self.allowed_attributes.get(tag, set())
        if any(name not in allowed for name, _ in attrs):
            raise ValueError(f"Недопустимый атрибут у тега <{tag}>")
        if tag == "a":
            href = (attributes.get("href") or "").strip()
            if not _is_http_url(href):
                raise ValueError("В HTML-ссылках разрешены только адреса http:// и https://")
        if tag == "tg-emoji" and not re.fullmatch(r"\d{1,64}", attributes.get("emoji-id") or ""):
            raise ValueError("Укажите числовой emoji-id для Premium Emoji")
        if tag == "span" and attributes.get("class") != "tg-spoiler":
            raise ValueError("Для <span> разрешён только class=\"tg-spoiler\"")
        if tag == "code" and attributes.get("class") and not attributes["class"].startswith("language-"):
            raise ValueError("Для <code> разрешён только class=\"language-...\"")
        self.stack.append(tag)

    def handle_startendtag(self, tag, attrs):
        raise ValueError("Самозакрывающиеся теги не поддерживаются")

    def handle_endtag(self, tag):
        tag = tag.lower()
        if not self.stack or self.stack[-1] != tag:
            raise ValueError(f"Некорректно закрыт тег </{tag}>")
        self.stack.pop()

    def close(self):
        super().close()
        if self.stack:
            raise ValueError(f"Тег <{self.stack[-1]}> не закрыт")


def _is_http_url(value):
    try:
        parsed = urlparse(str(value or "").strip())
        return parsed.scheme in {"http", "https"} and bool(parsed.netloc)
    except Exception:
        return False


def _is_payment_url(value):
    try:
        parsed = urlparse(str(value or "").strip())
        return parsed.scheme in {"http", "https", "tg", "tc"} and bool(parsed.netloc or parsed.path)
    except Exception:
        return False


def _validate_message_html(text):
    parser = TelegramHTMLValidator()
    try:
        parser.feed(text)
        parser.close()
    except ValueError:
        raise
    except Exception as error:
        raise ValueError(f"Некорректная HTML-разметка: {error}") from error


def _as_bool(value):
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _normalize_button(event_type, raw_button):
    if not isinstance(raw_button, dict):
        raise ValueError("Некорректные параметры кнопки")
    text = str(raw_button.get("text") or "").strip()
    action = str(raw_button.get("action") or "url").strip()
    url = str(raw_button.get("url") or "").strip()
    emoji_id = str(raw_button.get("emoji_id") or raw_button.get("icon_emoji_id") or "").strip()
    if not text or len(text) > 64:
        raise ValueError("Текст кнопки должен содержать от 1 до 64 символов")
    if action not in {"url", "trial", "payment", "buy", "main_menu", "extend", "manage_keys"}:
        raise ValueError("Неизвестное действие кнопки")
    if action == "payment" and event_type != "unpaid":
        raise ValueError("Кнопка оплаты доступна только в сценарии неоплаченного счёта")
    if action == "extend" and event_type != "expiry":
        raise ValueError("Кнопка продления доступна только в сценарии окончания подписки")
    if action == "url" and not _is_http_url(url):
        raise ValueError(f"Для кнопки «{text}» укажите корректную http:// или https:// ссылку")
    if len(url) > 2048:
        raise ValueError("Ссылка кнопки слишком длинная")
    if emoji_id and not re.fullmatch(r"\d{1,64}", emoji_id):
        raise ValueError(f"Premium Emoji ID кнопки «{text}» должен содержать только цифры")
    return {"text": text, "action": action, "url": url if action == "url" else "", "emoji_id": emoji_id}


def _normalize_schedule(event_type, raw_config, defaults):
    try:
        if event_type == "expiry":
            start_before_minutes = int(raw_config.get("start_before_minutes", defaults["start_before_minutes"]))
            repeat_interval_minutes = int(raw_config.get("repeat_interval_minutes", defaults["repeat_interval_minutes"]))
            stop_before_minutes = int(raw_config.get("stop_before_minutes", defaults["stop_before_minutes"]))
        else:
            start_delay_minutes = int(raw_config.get("start_delay_minutes", raw_config.get("delay_minutes", defaults["start_delay_minutes"])))
            repeat_interval_minutes = int(raw_config.get("repeat_interval_minutes", defaults["repeat_interval_minutes"]))
            active_duration_minutes = int(raw_config.get("active_duration_minutes", defaults["active_duration_minutes"]))
    except (TypeError, ValueError) as error:
        raise ValueError("Все параметры расписания должны быть целыми числами минут") from error
    if event_type == "expiry":
        if start_before_minutes < 1 or start_before_minutes > 5256000:
            raise ValueError("Начало серии должно быть от 1 минуты до 10 лет до окончания")
        if stop_before_minutes < 0 or stop_before_minutes >= start_before_minutes:
            raise ValueError("Остановка должна быть раньше начала серии и не меньше нуля")
        if not 1 <= repeat_interval_minutes <= 5256000:
            raise ValueError("Интервал повторения должен быть от 1 минуты до 10 лет")
        return {
            "start_before_minutes": start_before_minutes,
            "repeat_interval_minutes": repeat_interval_minutes,
            "stop_before_minutes": stop_before_minutes,
        }
    max_duration = 5256000 if event_type == "trial" else 10080
    if not 0 <= start_delay_minutes < active_duration_minutes:
        raise ValueError("Начало отправки должно быть раньше окончания общего срока")
    if repeat_interval_minutes < 1:
        raise ValueError("Интервал повторения должен быть не меньше 1 минуты")
    if not 1 <= active_duration_minutes <= max_duration:
        raise ValueError(f"Общий срок должен быть от 1 до {max_duration} минут")
    if repeat_interval_minutes > 5256000:
        raise ValueError("Интервал повторения не может превышать 10 лет")
    return {
        "start_delay_minutes": start_delay_minutes,
        "repeat_interval_minutes": repeat_interval_minutes,
        "active_duration_minutes": active_duration_minutes,
    }


def _normalize_event(event_type, raw_config):
    if event_type not in EVENT_TYPES or not isinstance(raw_config, dict):
        raise ValueError("Неизвестный сценарий уведомления")
    defaults = DEFAULT_CONFIG[event_type]
    text = str(raw_config.get("text", defaults["text"]) or "").strip()
    if not text or len(text) > 4000:
        raise ValueError("Текст сообщения должен содержать от 1 до 4000 символов")
    _validate_message_html(text)
    schedule = _normalize_schedule(event_type, raw_config, defaults)
    try:
        columns = int(raw_config.get("columns", defaults["columns"]))
    except (TypeError, ValueError) as error:
        raise ValueError("Некорректное количество колонок") from error
    if columns not in {1, 2, 3}:
        raise ValueError("Допустимо от 1 до 3 кнопок в ряду")
    raw_buttons = raw_config.get("buttons", defaults["buttons"])
    if not isinstance(raw_buttons, list) or len(raw_buttons) > 8:
        raise ValueError("Можно добавить не более 8 кнопок")
    buttons = [_normalize_button(event_type, button) for button in raw_buttons]
    return {
        "enabled": _as_bool(raw_config.get("enabled", defaults["enabled"])),
        **schedule,
        "text": text,
        "columns": columns,
        "buttons": buttons,
    }


def get_notification_config():
    config = copy.deepcopy(DEFAULT_CONFIG)
    try:
        raw = rw_repo.get_other_value(SETTINGS_KEY)
        saved = json.loads(raw) if raw else {}
    except Exception as error:
        logger.warning("Не удалось прочитать настройки автоматических уведомлений: %s", error)
        saved = {}
    if not isinstance(saved, dict):
        saved = {}
    for event_type in EVENT_TYPES:
        if isinstance(saved.get(event_type), dict):
            try:
                config[event_type] = _normalize_event(event_type, saved[event_type])
            except ValueError as error:
                logger.warning("Некорректные сохранённые настройки %s: %s", event_type, error)
    return config


def _ensure_storage():
    global _storage_ready
    if _storage_ready:
        return
    with _storage_lock:
        if _storage_ready:
            return
        with sqlite3.connect(DB_FILE, timeout=30.0) as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS automatic_notifications (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_type TEXT NOT NULL,
                    target_key TEXT NOT NULL,
                    reference_key TEXT,
                    user_id INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    error TEXT,
                    scheduled_for TIMESTAMP,
                    created_at TIMESTAMP NOT NULL,
                    sent_at TIMESTAMP,
                    UNIQUE(event_type, target_key)
                );
                CREATE INDEX IF NOT EXISTS idx_automatic_notifications_user
                    ON automatic_notifications(user_id, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_automatic_notifications_status
                    ON automatic_notifications(status, event_type);
                """
            )
            columns = {row[1] for row in conn.execute("PRAGMA table_info(automatic_notifications)")}
            if "reference_key" not in columns:
                conn.execute("ALTER TABLE automatic_notifications ADD COLUMN reference_key TEXT")
            if "scheduled_for" not in columns:
                conn.execute("ALTER TABLE automatic_notifications ADD COLUMN scheduled_for TIMESTAMP")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_automatic_notifications_reference ON automatic_notifications(reference_key, event_type, status)"
            )
        _storage_ready = True


def _connection():
    conn = sqlite3.connect(DB_FILE, timeout=30.0)
    conn.row_factory = sqlite3.Row
    return conn


def _claim_notification(event_type, target_key, user_id, reference_key, scheduled_for):
    _ensure_storage()
    now = datetime.now(MSK_TZ).strftime("%Y-%m-%d %H:%M:%S")
    with _connection() as conn:
        cursor = conn.execute(
            "INSERT OR IGNORE INTO automatic_notifications (event_type, target_key, reference_key, user_id, status, scheduled_for, created_at) VALUES (?, ?, ?, ?, 'pending', ?, ?)",
            (event_type, str(target_key), str(reference_key), int(user_id), scheduled_for, now),
        )
        return cursor.rowcount == 1


def _finish_notification(event_type, target_key, status, error=None):
    sent_at = datetime.now(MSK_TZ).strftime("%Y-%m-%d %H:%M:%S") if status == "sent" else None
    with _connection() as conn:
        conn.execute(
            "UPDATE automatic_notifications SET status = ?, error = ?, sent_at = ? WHERE event_type = ? AND target_key = ?",
            (status, str(error or "")[:1000] or None, sent_at, event_type, str(target_key)),
        )


def _split_minutes(event_type, total_minutes):
    total = int(total_minutes or 0)
    if total == 0:
        return {"value": 0, "unit": "minutes"}
    units = [("months", 43200), ("days", 1440), ("hours", 60), ("minutes", 1)]
    if event_type == "unpaid":
        units = [("hours", 60), ("minutes", 1)]
    for unit, multiplier in units:
        if total % multiplier == 0:
            return {"value": total // multiplier, "unit": unit}
    return {"value": total, "unit": "minutes"}


def _get_schedule_display(config):
    result = {}
    for event_type in EVENT_TYPES:
        start_key = "start_before_minutes" if event_type == "expiry" else "start_delay_minutes"
        end_key = "stop_before_minutes" if event_type == "expiry" else "active_duration_minutes"
        result[event_type] = {
            "start": _split_minutes(event_type, config[event_type][start_key]),
            "interval": _split_minutes(event_type, config[event_type]["repeat_interval_minutes"]),
            "duration": _split_minutes(event_type, config[event_type][end_key]),
        }
    return result


def _get_unresponsive_users(event_config):
    duration_seconds = int(event_config["active_duration_minutes"]) * 60
    cutoff = (datetime.now(MSK_TZ) - timedelta(seconds=duration_seconds)).strftime("%Y-%m-%d %H:%M:%S")
    with _connection() as conn:
        rows = conn.execute(
            f"""
            WITH campaign_users AS (
                SELECT u.telegram_id, u.username, u.first_name, u.registration_date,
                       COUNT(n.id) AS notifications_count, MAX(n.sent_at) AS last_sent_at
                FROM users u
                INNER JOIN automatic_notifications n
                    ON n.user_id = u.telegram_id AND n.event_type = 'trial' AND n.status = 'sent'
                WHERE {TRIAL_USER_CONDITIONS}
                  AND u.registration_date < ?
                GROUP BY u.telegram_id, u.username, u.first_name, u.registration_date
            )
            SELECT *, COUNT(*) OVER() AS total_count
            FROM campaign_users
            ORDER BY last_sent_at DESC
            LIMIT 200
            """,
            (cutoff,),
        ).fetchall()
    result = [dict(row) for row in rows]
    return result, int(rows[0]["total_count"] if rows else 0)


def get_notification_dashboard():
    _ensure_storage()
    config = get_notification_config()
    unpaid_cutoff = (
        datetime.now(timezone.utc) - timedelta(minutes=int(config["unpaid"]["active_duration_minutes"]))
    ).strftime("%Y-%m-%d %H:%M:%S")
    expiry_now = datetime.now(MSK_TZ)
    expiry_after = (expiry_now + timedelta(minutes=int(config["expiry"]["stop_before_minutes"]))).strftime("%Y-%m-%d %H:%M:%S")
    expiry_before = (expiry_now + timedelta(minutes=int(config["expiry"]["start_before_minutes"]))).strftime("%Y-%m-%d %H:%M:%S")
    with _connection() as conn:
        rows = conn.execute(
            "SELECT event_type, status, COUNT(*) AS amount FROM automatic_notifications GROUP BY event_type, status"
        ).fetchall()
        recent = conn.execute(
            """
            SELECT n.*, u.username, u.first_name
            FROM automatic_notifications n
            LEFT JOIN users u ON u.telegram_id = n.user_id
            ORDER BY n.id DESC
            LIMIT 50
            """
        ).fetchall()
        pending_payments = conn.execute(
            """
            SELECT p.*
            FROM pending_transactions p
            WHERE p.status = 'pending'
              AND p.created_at >= ?
              AND NOT EXISTS (
                  SELECT 1 FROM transactions t
                  WHERE t.payment_id = p.payment_id
                    AND LOWER(COALESCE(t.status, '')) IN ('paid', 'success', 'succeeded')
              )
            ORDER BY p.created_at DESC
            LIMIT 500
            """,
            (unpaid_cutoff,),
        ).fetchall()
        expiring_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM vpn_keys k
            INNER JOIN users u ON u.telegram_id = k.user_id
            WHERE k.expire_at IS NOT NULL
              AND k.expire_at > ?
              AND k.expire_at <= ?
              AND COALESCE(u.is_banned, 0) = 0
            """,
            (expiry_after, expiry_before),
        ).fetchone()[0]
    stats = {
        "active": sum(1 for event_type in EVENT_TYPES if config[event_type]["enabled"]),
        "sent": 0,
        "failed": 0,
        "unpaid": 0,
        "expiring": int(expiring_count or 0),
        "trial_sent": 0,
        "unpaid_sent": 0,
        "expiry_sent": 0,
        "unresponsive": 0,
    }
    for row in rows:
        amount = int(row["amount"] or 0)
        if row["status"] == "sent":
            stats["sent"] += amount
            if row["event_type"] in EVENT_TYPES:
                stats[f"{row['event_type']}_sent"] += amount
        elif row["status"] == "failed":
            stats["failed"] += amount
    for row in pending_payments:
        payment = dict(row)
        try:
            metadata = json.loads(payment.get("metadata") or "{}")
        except Exception:
            metadata = {}
        metadata.setdefault("payment_id", payment["payment_id"])
        metadata.setdefault("price", payment.get("amount_rub"))
        payment_method = str(metadata.get("payment_method") or "")
        payment_url = str(metadata.get("payment_url") or metadata.get("pay_url") or "").strip()
        if payment_method != "Telegram Stars" and not _is_payment_url(payment_url):
            continue
        if payment_method in {"CryptoBot", "TON Connect"} and _has_matching_successful_payment(payment, metadata):
            continue
        stats["unpaid"] += 1
    unresponsive, unresponsive_total = _get_unresponsive_users(config["trial"])
    stats["unresponsive"] = unresponsive_total
    return {
        "config": config,
        "schedule": _get_schedule_display(config),
        "stats": stats,
        "recent": [dict(row) for row in recent],
        "unresponsive": unresponsive,
    }


def update_pending_payment_url(payment_id, payment_url):
    if not payment_id or not _is_payment_url(payment_url):
        return False
    try:
        metadata = rw_repo.get_pending_metadata(str(payment_id)) or {}
        metadata["payment_url"] = str(payment_url).strip()
        with _connection() as conn:
            cursor = conn.execute(
                "UPDATE pending_transactions SET metadata = ? WHERE payment_id = ? AND status = 'pending'",
                (json.dumps(metadata, ensure_ascii=False), str(payment_id)),
            )
            return cursor.rowcount > 0
    except Exception as error:
        logger.warning("Не удалось сохранить ссылку оплаты %s: %s", payment_id, error)
        return False


def _get_pending_payment(payment_id):
    with _connection() as conn:
        row = conn.execute(
            "SELECT * FROM pending_transactions WHERE payment_id = ?",
            (str(payment_id),),
        ).fetchone()
    if not row:
        return None
    result = dict(row)
    try:
        result["parsed_metadata"] = json.loads(result.get("metadata") or "{}")
    except Exception:
        result["parsed_metadata"] = {}
    return result


def _format_amount(value):
    try:
        return f"{float(value or 0):.2f}"
    except (TypeError, ValueError):
        return "0.00"


def _build_context(user, metadata=None, payment_url=None):
    metadata = metadata or {}
    user = user or {}
    plan = None
    if metadata.get("plan_id"):
        try:
            plan = rw_repo.get_plan_by_id(metadata["plan_id"])
        except Exception:
            plan = None
    default_plan_name = "Пополнение баланса" if metadata.get("action") == "top_up" else "Выбранный тариф"
    return {
        "first_name": user.get("first_name") or user.get("username") or "Пользователь",
        "username": user.get("username") or "—",
        "amount": _format_amount(metadata.get("price")),
        "payment_method": metadata.get("payment_method") or "—",
        "plan_name": metadata.get("plan_name") or (plan or {}).get("plan_name") or default_plan_name,
        "host_name": metadata.get("host_name") or "—",
        "payment_id": metadata.get("payment_id") or "—",
        "trial_days": rw_repo.get_setting("trial_duration_days") or "3",
        "payment_url": payment_url or metadata.get("payment_url") or "",
    }


def _render_text(template, context):
    def replace(match):
        key = match.group(1)
        if key not in context:
            return match.group(0)
        value = context.get(key)
        return escape(str("—" if value is None or value == "" else value), quote=True)

    return re.sub(r"\{([a-z_]+)\}", replace, template)


def _build_keyboard(event_config, context):
    buttons = event_config.get("buttons") or []
    if not buttons:
        return None
    builder = InlineKeyboardBuilder()
    added = 0
    for button in buttons:
        kwargs = {"text": button["text"]}
        if button.get("emoji_id"):
            kwargs["icon_custom_emoji_id"] = button["emoji_id"]
        action = button["action"]
        if action == "url":
            kwargs["url"] = button["url"]
        elif action == "trial":
            kwargs["callback_data"] = "get_trial"
        elif action == "buy":
            kwargs["callback_data"] = "buy_new_key"
        elif action == "main_menu":
            kwargs["callback_data"] = "back_to_main_menu"
        elif action == "manage_keys":
            kwargs["callback_data"] = "manage_keys"
        elif action == "extend":
            key_id = context.get("key_id")
            if not key_id:
                continue
            kwargs["callback_data"] = f"extend_key_{int(key_id)}"
        elif action == "payment":
            payment_url = context.get("payment_url") or ""
            payment_id = str(context.get("payment_id") or "")
            if context.get("preview") and _is_payment_url(payment_url):
                kwargs["url"] = payment_url
            elif payment_id and payment_id != "—":
                kwargs["callback_data"] = f"auto_notify_pay:{payment_id}"
            else:
                continue
        try:
            builder.add(InlineKeyboardButton(**kwargs))
            added += 1
        except TypeError:
            kwargs.pop("icon_custom_emoji_id", None)
            builder.add(InlineKeyboardButton(**kwargs))
            added += 1
    if not added:
        return None
    builder.adjust(int(event_config.get("columns") or 1))
    return builder.as_markup()


async def _send_event_message(bot, event_config, user_id, context):
    text = _render_text(event_config["text"], context)
    if len(text) > 4096:
        raise ValueError("После подстановки переменных сообщение превышает 4096 символов")
    keyboard = _build_keyboard(event_config, context)
    await bot.send_message(
        chat_id=int(user_id),
        text=text,
        parse_mode="HTML",
        reply_markup=keyboard,
        disable_web_page_preview=True,
    )


def _to_timestamp(value, default_timezone):
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=default_timezone)
        return parsed.timestamp()
    except (TypeError, ValueError):
        return None


def _schedule_slot(started_ts, now_ts, event_config):
    start_ts = started_ts + int(event_config["start_delay_minutes"]) * 60
    end_ts = started_ts + int(event_config["active_duration_minutes"]) * 60
    if now_ts < start_ts or now_ts >= end_ts:
        return None
    interval_seconds = int(event_config["repeat_interval_minutes"]) * 60
    occurrence = int((now_ts - start_ts) // interval_seconds)
    return start_ts + occurrence * interval_seconds, end_ts


def _last_sent_within_interval(value, now_ts, interval_seconds):
    sent_ts = _to_timestamp(value, MSK_TZ)
    return sent_ts is not None and now_ts - sent_ts < interval_seconds


def _add_schedule_context(context, started_ts, now_ts, end_ts):
    remaining_seconds = max(0, int(end_ts - now_ts))
    context["remaining_minutes"] = max(1, (remaining_seconds + 59) // 60)
    context["remaining_days"] = max(1, (remaining_seconds + 86399) // 86400)
    context["campaign_started_at"] = datetime.fromtimestamp(started_ts, timezone.utc).astimezone(MSK_TZ).strftime("%d.%m.%Y")
    return context


def _expiry_slot(expiry_ts, now_ts, event_config):
    start_ts = expiry_ts - int(event_config["start_before_minutes"]) * 60
    duration_seconds = (int(event_config["start_before_minutes"]) - int(event_config["stop_before_minutes"])) * 60
    slot = _schedule_slot(
        start_ts,
        now_ts,
        {
            "start_delay_minutes": 0,
            "active_duration_minutes": duration_seconds // 60,
            "repeat_interval_minutes": event_config["repeat_interval_minutes"],
        },
    )
    return (slot[0], expiry_ts) if slot else None


def _plural_word(value, forms):
    number = abs(int(value)) % 100
    last = number % 10
    if 11 <= number <= 14:
        return forms[2]
    if last == 1:
        return forms[0]
    if 2 <= last <= 4:
        return forms[1]
    return forms[2]


def _add_expiry_context(context, expiry_ts, now_ts):
    remaining_seconds = max(0, int(expiry_ts - now_ts))
    total_minutes = (remaining_seconds + 59) // 60
    days = total_minutes // 1440
    hours = (total_minutes % 1440) // 60
    minutes = total_minutes % 60
    if days > 0:
        time_left = f"{days} {_plural_word(days, ('день', 'дня', 'дней'))} {hours:02d} {_plural_word(hours, ('час', 'часа', 'часов'))}"
    elif hours > 0:
        time_left = f"{hours} {_plural_word(hours, ('час', 'часа', 'часов'))} {minutes:02d} {_plural_word(minutes, ('минута', 'минуты', 'минут'))}"
    else:
        time_left = f"{minutes} {_plural_word(minutes, ('минута', 'минуты', 'минут'))}"
    context.update({
        "time_left": time_left,
        "days_left": days,
        "hours_left": hours,
        "minutes_left": minutes,
        "remaining_days": days,
        "remaining_minutes": total_minutes,
        "expiry_date": datetime.fromtimestamp(expiry_ts, timezone.utc).astimezone(MSK_TZ).strftime("%d.%m.%Y в %H:%M"),
    })
    return context


async def _deliver_automatic(bot, event_type, reference_key, user_id, event_config, context, scheduled_ts):
    target_key = f"{reference_key}:{int(scheduled_ts)}"
    scheduled_for = datetime.fromtimestamp(scheduled_ts, timezone.utc).astimezone(MSK_TZ).strftime("%Y-%m-%d %H:%M:%S")
    if not _claim_notification(event_type, target_key, user_id, reference_key, scheduled_for):
        return False
    try:
        await _send_event_message(bot, event_config, user_id, context)
        _finish_notification(event_type, target_key, "sent")
        return True
    except TelegramForbiddenError as error:
        try:
            _finish_notification(event_type, target_key, "blocked", error)
        except Exception as storage_error:
            logger.error("Не удалось записать блокировку уведомления %s: %s", target_key, storage_error)
        logger.info("Пользователь %s заблокировал бота; серия %s остановлена", user_id, event_type)
        return "blocked"
    except Exception as error:
        try:
            _finish_notification(event_type, target_key, "failed", error)
        except Exception as storage_error:
            logger.error("Не удалось обновить журнал уведомления %s: %s", target_key, storage_error)
        logger.warning("Не удалось отправить автоматическое уведомление %s пользователю %s: %s", event_type, user_id, error)
        return False


async def _process_trial_notifications(bot, event_config):
    if not event_config["enabled"] or not _as_bool(rw_repo.get_setting("trial_enabled")):
        return
    _ensure_storage()
    now_ts = datetime.now(MSK_TZ).timestamp()
    duration_seconds = int(event_config["active_duration_minutes"]) * 60
    cutoff = (datetime.now(MSK_TZ) - timedelta(seconds=duration_seconds)).strftime("%Y-%m-%d %H:%M:%S")
    with _connection() as conn:
        users = conn.execute(
            f"""
            SELECT u.*, (
                SELECT MAX(n.sent_at)
                FROM automatic_notifications n
                WHERE n.user_id = u.telegram_id AND n.event_type = 'trial' AND n.status = 'sent'
            ) AS last_sent_at
            FROM users u
            WHERE {TRIAL_USER_CONDITIONS}
              AND u.registration_date >= ?
            ORDER BY u.registration_date DESC
            LIMIT 500
            """,
            (cutoff,),
        ).fetchall()
    interval_seconds = int(event_config["repeat_interval_minutes"]) * 60
    for row in users:
        user = dict(row)
        user_id = int(user["telegram_id"])
        registered_ts = _to_timestamp(user.get("registration_date"), MSK_TZ)
        if registered_ts is None:
            continue
        slot = _schedule_slot(registered_ts, now_ts, event_config)
        if not slot or _last_sent_within_interval(user.get("last_sent_at"), now_ts, interval_seconds):
            continue
        scheduled_ts, end_ts = slot
        context = _build_context(user, {"payment_id": str(user_id)})
        context = _add_schedule_context(context, registered_ts, now_ts, end_ts)
        await _deliver_automatic(bot, "trial", user_id, user_id, event_config, context, scheduled_ts)
        await asyncio.sleep(0.05)


def _has_matching_successful_payment(payment, metadata):
    payment_id = str(payment["payment_id"])
    user_id = int(payment["user_id"])
    amount = float(metadata.get("price") or payment.get("amount_rub") or 0)
    payment_method = str(metadata.get("payment_method") or "")
    with _connection() as conn:
        exact = conn.execute(
            "SELECT 1 FROM transactions WHERE payment_id = ? AND LOWER(COALESCE(status, '')) IN ('paid', 'success', 'succeeded') LIMIT 1",
            (payment_id,),
        ).fetchone()
        if exact:
            return True
        if payment_method not in {"CryptoBot", "TON Connect"}:
            return False
        rows = conn.execute(
            """
            SELECT created_date
            FROM transactions
            WHERE user_id = ?
              AND LOWER(COALESCE(status, '')) IN ('paid', 'success', 'succeeded')
              AND ABS(COALESCE(amount_rub, 0) - ?) < 0.01
              AND payment_method IN (?, ?)
            ORDER BY created_date DESC
            LIMIT 5
            """,
            (user_id, amount, payment_method, "TON" if payment_method == "TON Connect" else payment_method),
        ).fetchall()
    created_ts = _to_timestamp(payment.get("created_at"), timezone.utc)
    if created_ts is None:
        return False
    now_ts = datetime.now(timezone.utc).timestamp()
    for row in rows:
        transaction_ts = _to_timestamp(row["created_date"], MSK_TZ)
        if transaction_ts is not None and created_ts - 5 <= transaction_ts <= now_ts + 5:
            return True
    return False


async def _process_unpaid_notifications(bot, event_config):
    if not event_config["enabled"]:
        return
    _ensure_storage()
    now_ts = datetime.now(timezone.utc).timestamp()
    duration_seconds = int(event_config["active_duration_minutes"]) * 60
    cutoff = (datetime.now(timezone.utc) - timedelta(seconds=duration_seconds)).strftime("%Y-%m-%d %H:%M:%S")
    with _connection() as conn:
        payments = conn.execute(
            """
            SELECT p.*, u.username, u.first_name, u.is_banned, (
                SELECT MAX(n.sent_at)
                FROM automatic_notifications n
                WHERE (n.reference_key = p.payment_id OR (n.reference_key IS NULL AND n.target_key = p.payment_id))
                  AND n.event_type = 'unpaid' AND n.status = 'sent'
            ) AS last_sent_at
            FROM pending_transactions p
            LEFT JOIN users u ON u.telegram_id = p.user_id
            WHERE p.status = 'pending'
              AND p.created_at >= ?
              AND NOT EXISTS (
                  SELECT 1 FROM transactions t
                  WHERE t.payment_id = p.payment_id
                    AND LOWER(COALESCE(t.status, '')) IN ('paid', 'success', 'succeeded')
              )
              AND NOT EXISTS (
                  SELECT 1 FROM automatic_notifications blocked
                  WHERE blocked.reference_key = p.payment_id AND blocked.event_type = 'unpaid' AND blocked.status = 'blocked'
              )
            ORDER BY p.created_at DESC
            LIMIT 500
            """,
            (cutoff,),
        ).fetchall()
    interval_seconds = int(event_config["repeat_interval_minutes"]) * 60
    for row in payments:
        payment = dict(row)
        if payment.get("is_banned"):
            continue
        try:
            metadata = json.loads(payment.get("metadata") or "{}")
        except Exception:
            metadata = {}
        metadata.setdefault("payment_id", payment["payment_id"])
        metadata.setdefault("price", payment.get("amount_rub"))
        payment_method = str(metadata.get("payment_method") or "")
        payment_url = str(metadata.get("payment_url") or metadata.get("pay_url") or "").strip()
        if payment_method != "Telegram Stars" and not _is_payment_url(payment_url):
            continue
        created_ts = _to_timestamp(payment.get("created_at"), timezone.utc)
        if created_ts is None:
            continue
        slot = _schedule_slot(created_ts, now_ts, event_config)
        if not slot or _last_sent_within_interval(payment.get("last_sent_at"), now_ts, interval_seconds):
            continue
        if _has_matching_successful_payment(payment, metadata):
            continue
        scheduled_ts, end_ts = slot
        user = {
            "telegram_id": payment["user_id"],
            "username": payment.get("username"),
            "first_name": payment.get("first_name"),
        }
        context = _build_context(user, metadata, payment_url)
        context = _add_schedule_context(context, created_ts, now_ts, end_ts)
        await _deliver_automatic(bot, "unpaid", payment["payment_id"], payment["user_id"], event_config, context, scheduled_ts)
        await asyncio.sleep(0.05)


async def _process_expiry_notifications(bot, event_config):
    if not event_config["enabled"]:
        return
    _ensure_storage()
    now_dt = datetime.now(MSK_TZ)
    now_ts = now_dt.timestamp()
    active_after = (now_dt + timedelta(minutes=int(event_config["stop_before_minutes"]))).strftime("%Y-%m-%d %H:%M:%S")
    start_before = (now_dt + timedelta(minutes=int(event_config["start_before_minutes"]))).strftime("%Y-%m-%d %H:%M:%S")
    with _connection() as conn:
        keys = conn.execute(
            """
            SELECT k.key_id, k.user_id, k.host_name, k.key_email, k.email, k.expire_at,
                   u.username, u.first_name, (
                SELECT MAX(n.sent_at)
                FROM automatic_notifications n
                WHERE n.reference_key = CAST(k.key_id AS TEXT) AND n.event_type = 'expiry' AND n.status = 'sent'
            ) AS last_sent_at
            FROM vpn_keys k
            INNER JOIN users u ON u.telegram_id = k.user_id
            WHERE k.expire_at IS NOT NULL
              AND k.expire_at > ?
              AND k.expire_at <= ?
              AND COALESCE(u.is_banned, 0) = 0
              AND NOT EXISTS (
                  SELECT 1 FROM automatic_notifications blocked
                  WHERE blocked.reference_key = CAST(k.key_id AS TEXT) AND blocked.event_type = 'expiry' AND blocked.status = 'blocked'
              )
            ORDER BY k.expire_at ASC
            LIMIT 500
            """,
            (active_after, start_before),
        ).fetchall()
    interval_seconds = int(event_config["repeat_interval_minutes"]) * 60
    blocked_users = set()
    for row in keys:
        key = dict(row)
        user_id = int(key["user_id"])
        if user_id in blocked_users:
            continue
        expiry_ts = _to_timestamp(key.get("expire_at"), MSK_TZ)
        if expiry_ts is None:
            continue
        slot = _expiry_slot(expiry_ts, now_ts, event_config)
        if not slot or _last_sent_within_interval(key.get("last_sent_at"), now_ts, interval_seconds):
            continue
        scheduled_ts, _ = slot
        key_id = int(key["key_id"])
        context = {
            "first_name": key.get("first_name") or key.get("username") or "Пользователь",
            "username": key.get("username") or "—",
            "key_id": key_id,
            "key_name": key.get("key_email") or key.get("email") or f"Ключ #{key_id}",
            "host_name": key.get("host_name") or "—",
        }
        context = _add_expiry_context(context, expiry_ts, now_ts)
        result = await _deliver_automatic(bot, "expiry", key_id, user_id, event_config, context, scheduled_ts)
        if result == "blocked":
            blocked_users.add(user_id)
        await asyncio.sleep(0.05)


async def process_automatic_notifications(bot):
    config = get_notification_config()
    try:
        await _process_trial_notifications(bot, config["trial"])
    except Exception as error:
        logger.error("Ошибка обработки уведомлений о пробном периоде: %s", error, exc_info=True)
    try:
        await _process_unpaid_notifications(bot, config["unpaid"])
    except Exception as error:
        logger.error("Ошибка обработки неоплаченных счетов: %s", error, exc_info=True)
    try:
        await _process_expiry_notifications(bot, config["expiry"])
    except Exception as error:
        logger.error("Ошибка обработки окончания подписок: %s", error, exc_info=True)


async def automatic_notification_loop(bot_controller):
    await asyncio.sleep(5)
    while True:
        try:
            running = bot_controller.get_status().get("is_running")
            bot = bot_controller.get_bot_instance() if running else None
            if bot:
                await process_automatic_notifications(bot)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            logger.error("Ошибка цикла автоматических уведомлений: %s", error, exc_info=True)
        await asyncio.sleep(20)


def get_notification_router():
    router = Router(name="automatic_notifications")

    @router.callback_query(F.data.startswith("auto_notify_pay:"))
    async def open_pending_payment(callback):
        payment_id = callback.data.split(":", 1)[1]
        payment = _get_pending_payment(payment_id)
        if not payment or int(payment["user_id"]) != int(callback.from_user.id):
            await callback.answer("Счёт не найден или недоступен.", show_alert=True)
            return
        if payment.get("status") != "pending":
            await callback.answer("Этот счёт уже обработан.", show_alert=True)
            return
        metadata = payment.get("parsed_metadata") or {}
        metadata.setdefault("payment_id", payment["payment_id"])
        metadata.setdefault("price", payment.get("amount_rub"))
        created_ts = _to_timestamp(payment.get("created_at"), timezone.utc)
        active_seconds = int(get_notification_config()["unpaid"]["active_duration_minutes"]) * 60
        if created_ts is None or datetime.now(timezone.utc).timestamp() >= created_ts + active_seconds:
            await callback.answer("Время оплаты этого счёта истекло. Оформите новый счёт.", show_alert=True)
            return
        if _has_matching_successful_payment(payment, metadata):
            await callback.answer("Этот счёт уже оплачен.", show_alert=True)
            return
        payment_url = str(metadata.get("payment_url") or "").strip()
        if _is_payment_url(payment_url):
            markup = InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="💳 Перейти к оплате", url=payment_url)]]
            )
            await callback.message.answer("💳 <b>Ссылка на оплату счёта</b>", reply_markup=markup)
            await callback.answer()
            return
        if metadata.get("payment_method") == "Telegram Stars":
            try:
                ratio = Decimal(str(rw_repo.get_setting("stars_per_rub") or "0"))
                if ratio <= 0:
                    raise ValueError("Не настроен курс Telegram Stars")
                price = Decimal(str(metadata.get("price") or payment.get("amount_rub") or 0))
                stars_amount = max(1, int((price * ratio).quantize(Decimal("1"), rounding=ROUND_HALF_UP)))
                months = int(float(metadata.get("months") or 0))
                action = metadata.get("action")
                if action == "top_up":
                    title = "Пополнение баланса"
                    description = f"Пополнение баланса на {_format_amount(price)} RUB"
                else:
                    title = f"{'Продление' if action == 'extend' else 'Подписка'} на {months} мес."
                    description = f"Оплата выбранного тарифа на {_format_amount(price)} RUB"
                await callback.message.answer_invoice(
                    title=title[:32],
                    description=description[:255],
                    prices=[LabeledPrice(label=title[:32], amount=stars_amount)],
                    payload=str(payment_id),
                    currency="XTR",
                )
                await callback.answer()
            except Exception as error:
                logger.warning("Не удалось повторно выставить счёт Stars %s: %s", payment_id, error)
                await callback.answer("Не удалось выставить счёт. Попробуйте оформить его заново.", show_alert=True)
            return
        await callback.answer("Ссылка на этот счёт недоступна. Оформите платёж заново.", show_alert=True)

    return router


def register_notification_routes(flask_app, login_required, get_common_template_data):
    @flask_app.route("/notifications")
    @login_required
    def notification_page():
        dashboard = get_notification_dashboard()
        return render_template("notification.html", **dashboard, **get_common_template_data())

    @flask_app.route("/notifications/settings", methods=["POST"])
    @login_required
    def notification_settings_save():
        payload = request.get_json(silent=True) or {}
        try:
            config = {
                event_type: _normalize_event(event_type, payload.get(event_type) or {})
                for event_type in EVENT_TYPES
            }
            if not rw_repo.set_other_value(SETTINGS_KEY, json.dumps(config, ensure_ascii=False)):
                raise RuntimeError("Не удалось записать настройки в базу данных")
            return jsonify({"ok": True, "message": "Настройки уведомлений сохранены", "config": config})
        except ValueError as error:
            return jsonify({"ok": False, "error": str(error)}), 400
        except Exception as error:
            logger.error("Ошибка сохранения уведомлений: %s", error, exc_info=True)
            return jsonify({"ok": False, "error": "Не удалось сохранить настройки уведомлений"}), 500

    @flask_app.route("/notifications/preview", methods=["POST"])
    @login_required
    def notification_preview():
        payload = request.get_json(silent=True) or {}
        event_type = payload.get("event_type")
        try:
            if event_type not in EVENT_TYPES:
                raise ValueError("Неизвестный сценарий уведомления")
            event_config = _normalize_event(event_type, payload.get("config") or {})
            admin_id = int(rw_repo.get_setting("admin_telegram_id") or 0)
            if not admin_id:
                return jsonify({"ok": False, "error": "Не настроен Telegram ID администратора"}), 400
            from shop_bot.webhook_server.app import _bot_controller
            bot = _bot_controller.get_bot_instance() if _bot_controller else None
            if not bot:
                return jsonify({"ok": False, "error": "Основной бот не запущен"}), 503
            loop = current_app.config.get("EVENT_LOOP")
            if not loop or not loop.is_running():
                return jsonify({"ok": False, "error": "Цикл событий бота недоступен"}), 503
            admin = rw_repo.get_user(admin_id) or {}
            bot_username = (rw_repo.get_setting("telegram_bot_username") or "").strip()
            metadata = {
                "payment_id": "preview",
                "price": 199.0,
                "payment_method": "YooKassa" if event_type == "unpaid" else "—",
                "plan_name": "Премиум на 1 месяц",
                "host_name": "Основной сервер",
                "payment_url": f"https://t.me/{bot_username}" if bot_username else "https://t.me/",
            }
            context = _build_context(admin, metadata)
            now_ts = datetime.now(timezone.utc).timestamp()
            if event_type == "expiry":
                try:
                    admin_keys = rw_repo.get_user_keys(admin_id) or []
                except Exception:
                    admin_keys = []
                preview_key = admin_keys[0] if admin_keys else {}
                context.update({
                    "key_id": preview_key.get("key_id"),
                    "key_name": preview_key.get("key_email") or preview_key.get("email") or "Демонстрационный ключ",
                    "host_name": preview_key.get("host_name") or "Основной сервер",
                })
                context = _add_expiry_context(context, now_ts + 3 * 86400, now_ts)
            else:
                context = _add_schedule_context(context, now_ts, now_ts, now_ts + int(event_config["active_duration_minutes"]) * 60)
            context["preview"] = True
            asyncio.run_coroutine_threadsafe(
                _send_event_message(bot, event_config, admin_id, context),
                loop,
            ).result(timeout=20)
            return jsonify({"ok": True, "message": "Предпросмотр отправлен администратору"})
        except ValueError as error:
            return jsonify({"ok": False, "error": str(error)}), 400
        except TelegramBadRequest as error:
            return jsonify({"ok": False, "error": f"Telegram отклонил сообщение: {error}"}), 400
        except Exception as error:
            logger.error("Ошибка предпросмотра уведомления: %s", error, exc_info=True)
            return jsonify({"ok": False, "error": f"Не удалось отправить предпросмотр: {error}"}), 500
