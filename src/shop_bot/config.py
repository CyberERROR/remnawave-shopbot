from datetime import datetime, timedelta, timezone
import os
import time

# --- TIME CONFIGURATION ---
# Force MSK (UTC+3)
os.environ['TZ'] = 'Etc/GMT-3'
if hasattr(time, 'tzset'):
    time.tzset()

def get_msk_time():
    """Returns current time in MSK (UTC+3)"""
    return datetime.now(timezone(timedelta(hours=3), name='MSK'))
# --------------------------

from aiogram import html

def _get_msg_setting(key, default):
    try:
        from shop_bot.data_manager.remnawave_repository import get_setting
        val = get_setting(key)
        if val is not None and str(val).strip():
            return str(val)
    except Exception:
        pass
    return default

import re

_NAMED_COND_RE = re.compile(r'\{(\w+)\s*=\s*((?:[^{}]|\{(?:[^{}]|\{[^{}]*\})*\})*?)\s+or\s+((?:[^{}]|\{(?:[^{}]|\{[^{}]*\})*\})*?)\}')

def parse_named_conditionals(text, conditions):
    if not text or '=' not in text:
        return text
    prev = None
    while prev != text:
        prev = text
        def _replace(m):
            name = m.group(1)
            if name in conditions:
                return m.group(2).strip() if conditions[name] else m.group(3).strip()
            return m.group(0)
        text = _NAMED_COND_RE.sub(_replace, text)
    return text

def safe_format(text, **kwargs):
    if not text or '{' not in text:
        return text
    from string import Formatter
    fmt = Formatter()
    try:
        parsed = list(fmt.parse(text))
    except ValueError:
        return text
    result = []
    for literal, field_name, format_spec, conversion in parsed:
        result.append(literal)
        if field_name is not None:
            try:
                obj = kwargs[field_name]
                if conversion:
                    obj = fmt.convert_field(obj, conversion)
                if format_spec:
                    obj = fmt.format_field(obj, format_spec)
                result.append(str(obj))
            except (KeyError, IndexError, ValueError, AttributeError):
                spec = ''
                if conversion:
                    spec += '!' + conversion
                if format_spec:
                    spec += ':' + format_spec
                result.append('{' + field_name + spec + '}')
    return ''.join(result)

def format_msg_vars(text, **kwargs):
    if not text or '{' not in text:
        return text
    try:
        return text.format(**kwargs)
    except (KeyError, IndexError, ValueError):
        return text

CHOOSE_PLAN_MESSAGE_DEFAULT = "Выберите подходящий тариф:"
_CHOOSE_PAYMENT_DEFAULT = (
    "{is_addons_menu = ➕ <b>Допы подписки</b>\n\nВыберите, что хотите дополнительно докупить: or }"
    "{is_confirm_devices = 📑 <b>Подтверждение</b>\n<blockquote>• Сейчас: {current_limit} устройств\n• Докупается: +{added} устройств\n• Станет: {target_devices} устройств\n• До конца подписки: {days_left} дн.</blockquote>\n\n<b>🧾 К оплате: {price}₽</b> or }"
    "{is_confirm_traffic = 📑 <b>Подтверждение</b>\n<blockquote>• Текущий лимит: {current_limit} ГБ\n• Использовано: {used} ГБ\n• Докупается: +{added} ГБ\n• Новый лимит: {new_limit} ГБ\n• Будет доступно: {available} ГБ\n• До конца подписки: {days_left} дн.</blockquote>\n\n<b>🧾 К оплате: {price}₽</b> or }"
    "{is_purchase = 📑 <b>Итог:</b>\n<blockquote>{summary}</blockquote>\n\n or }"
    "{is_topup = ✅ Сумма принята: {topup_amount} RUB\n or }"
    "{is_addon_pay = 💳 <b>Оплата дополнения</b>\n\n or }"
    "{has_price = <b>🧾 К оплате: {price}₽</b>\n or }"
    "{promo_text}\n"
    "Выберите удобный способ оплаты:"
)

def _get_choose_plan_message():
    return _get_msg_setting("msg_choose_plan_text", CHOOSE_PLAN_MESSAGE_DEFAULT)

def get_choose_payment_text(**kwargs):
    tpl = _get_msg_setting("msg_choose_payment_text", _CHOOSE_PAYMENT_DEFAULT)
    conds = {k: bool(kwargs.get(k)) for k in [
        'is_addons_menu', 'is_confirm_devices', 'is_confirm_traffic',
        'is_purchase', 'is_topup', 'is_addon_pay', 'has_price'
    ]}
    tpl = parse_named_conditionals(tpl, conds)
    defaults = {
        'current_limit': '', 'added': '', 'target_devices': '', 'days_left': '',
        'used': '', 'new_limit': '', 'available': '', 'price': '',
        'summary': '', 'topup_amount': '', 'promo_text': ''
    }
    defaults.update(kwargs)
    return safe_format(tpl, **defaults)

CHOOSE_PLAN_MESSAGE = CHOOSE_PLAN_MESSAGE_DEFAULT


_PROFILE_TEXT_DEFAULT = (
    "<b>👤 ПРОФИЛЬ:</b> {username} / <b>iD:</b> <code>{user_id}</code>\n\n"
    "<b>💎 ПОДПИСКА</b>\n"
    "{vpn_status = 🛡 Статус VPN: Активен ✅ or ❌ Неактивен (срок истек)}\n"
    "<b>⏳ Осталось:</b> {vpn_remaining = {days_left} д. {hours_left} ч. or -}\n"
    "<b>💲 Потрачено всего:</b> {total_spent} RUB\n"
    "<b>📅 Приобретено месяцев:</b> {total_months}\n\n"
    "<b>💼 ФИНАНСЫ</b>\n"
    "<b>💳 Основной баланс:</b> {main_balance} RUB\n"
    "<b>🤝 Рефералов:</b> {referral_count}\n"
    "<b>💰 Заработано:</b> {total_ref_earned} RUB\n\n"
    "{has_partner = <b>👑 ПАРТНЕРСКАЯ ПРОГРАММА</b>\n or }"
    "{has_ref = <b>👥 Реферальный бонус:</b> +{s_ref}%\n or }"
    "{has_sale = <b>🛍 Персональная скидка:</b> -{s_sale}%\n or }"
    "{has_squad = <b>🛰 Индивидуальный Сквад:</b> ✅\n or }"
)

def get_profile_text(username, user_id, total_spent, total_months, vpn_active, vpn_remaining, main_balance, referral_count, total_ref_earned, days_left=0, hours_left=0, seller_info=None):
    tpl = _get_msg_setting("msg_profile_text", _PROFILE_TEXT_DEFAULT)

    has_partner = False
    has_ref = False
    has_sale = False
    has_squad = False
    s_ref = "0"
    s_sale = "0"
    if seller_info:
        s_ref_val = seller_info.get('ref', 0)
        s_sale_val = seller_info.get('sale', 0)
        s_squad_val = seller_info.get('squad_uuid')
        has_ref = bool(s_ref_val and float(s_ref_val) > 0)
        has_sale = bool(s_sale_val and float(s_sale_val) > 0)
        has_squad = bool(s_squad_val and str(s_squad_val) != '0' and str(s_squad_val).strip())
        has_partner = has_ref or has_sale or has_squad
        if has_ref:
            s_ref = str(s_ref_val)
        if has_sale:
            s_sale = str(s_sale_val)

    conds = {
        'vpn_status': vpn_active, 'vpn_remaining': vpn_active,
        'has_partner': has_partner, 'has_ref': has_ref,
        'has_sale': has_sale, 'has_squad': has_squad
    }
    tpl = parse_named_conditionals(tpl, conds)
    text = safe_format(
        tpl,
        username=username, user_id=user_id,
        total_spent=f"{total_spent:.0f}", total_months=total_months,
        days_left=days_left, hours_left=hours_left,
        main_balance=f"{main_balance:.0f}", referral_count=referral_count,
        total_ref_earned=f"{total_ref_earned:.2f}",
        s_ref=s_ref, s_sale=s_sale
    )
    return text

def _get_status_text(remaining):
    total_seconds = int(remaining.total_seconds())
    if total_seconds < 0:
        return "Не активен (Истек)"
    return "Активен"

def _format_remaining_details(remaining: timedelta) -> str:
    total_seconds = int(remaining.total_seconds())
    if total_seconds <= 0:
        return "0мин"

    minutes = (total_seconds // 60) % 60
    hours = (total_seconds // 3600) % 24
    days = remaining.days % 365
    years = remaining.days // 365

    parts = []
    if years > 0:
        parts.append(f"{years}г.")
    if days > 0:
        parts.append(f"{days}д.")
    if hours > 0:
        parts.append(f"{hours}ч.")
    if minutes > 0:
        parts.append(f"{minutes}мин")

    return " ".join(parts) if parts else "меньше минуты"

def get_key_info_text(key_number, expiry_date, created_date, connection_string, email=None, hwid_limit=None, hwid_usage=None, traffic_limit=None, traffic_used=None, comment=None):
    now = get_msk_time().replace(tzinfo=None)
    
    # Ensure expiry_date is comparable (naive vs naive)
    if expiry_date.tzinfo:
        expiry_date = expiry_date.astimezone(get_msk_time().tzinfo).replace(tzinfo=None)
        
    remaining = expiry_date - now
    days_left = remaining.days
    
    status_icon = "🟢"
    status_text = _get_status_text(remaining)
    remaining_str = _format_remaining_details(remaining)
    
    if days_left <= 10:
        status_icon = "🟡"
    
    if days_left < 0:
        status_icon = "🔴"
        remaining_str = "0мин"

    def _fmt_bytes(val):
        try:
            b = float(val)
        except (TypeError, ValueError):
            return str(val)
        if b <= 0:
            return "0 Б"
        if b >= 1099511627776:
            return f"{b / 1099511627776:.1f} ТБ".replace('.0', '')
        if b >= 1073741824:
            return f"{b / 1073741824:.1f} ГБ".replace('.0', '')
        if b >= 1048576:
            return f"{b / 1048576:.1f} МБ".replace('.0', '')
        return f"{b / 1024:.1f} КБ".replace('.0', '')

    traffic_block = ""
    if traffic_limit is not None:
        try:
            t_lim_val = float(traffic_limit)
        except (TypeError, ValueError):
            t_lim_val = 0
        t_lim_display = "∞" if t_lim_val <= 0 else _fmt_bytes(traffic_limit)
        used_display = _fmt_bytes(traffic_used) if traffic_used else "0 Б"
        traffic_block = f"{used_display} / {t_lim_display}"

    hwid_block = ""
    if hwid_limit is not None:
        limit_str = str(hwid_limit)
        limit_display = "∞" if limit_str == "0" or (limit_str.isdigit() and int(limit_str) > 98) else limit_str
        hwid_block = f"{hwid_usage} / {limit_display}"

    if email and str(email).endswith("@bot.local"):
        email = str(email).replace("@bot.local", "@bot")

    comment_block = ""
    if comment:
        comment_block = f"💬 <b>Комментарий:</b> <blockquote>{html.quote(comment)}</blockquote>\n"

    _KEY_INFO_DEFAULT = (
        "🔑 <b>Информация о ключе #{key_number}</b>\n\n"
        "📅 <b>Сроки действия:</b>\n"
        "{status_icon} <b>Статус:</b> {status_text}\n"
        "➕ <b>Куплен:</b> {created_date}\n"
        "🕙 <b>Истекает:</b> {expiry_date}\n"
        "⏳ <b>Осталось:</b> {remaining_str}\n"
        "💌 <b>ID ключа:</b> <code>{email}</code>\n\n"
        "📉 <b>Использование:</b>\n"
        "🛰 <b>Лимит трафика:</b> {traffic_block}\n"
        "📱 <b>Лимит устройств:</b> {hwid_block}\n"
        "🗽 <b>Ваш ключ:</b>\n<code>{connection_string}</code>"
        "\n\n{comment_block}"
    )

    tpl = _get_msg_setting("msg_key_info_text", _KEY_INFO_DEFAULT)
    return safe_format(
        tpl,
        key_number=key_number, status_icon=status_icon, status_text=status_text,
        created_date=created_date.strftime('%d.%m.%Y'),
        expiry_date=expiry_date.strftime('%d.%m.%Y %H:%M'),
        remaining_str=remaining_str, email=email,
        traffic_block=traffic_block, hwid_block=hwid_block,
        connection_string=connection_string, comment_block=comment_block
    )


_PURCHASE_SUCCESS_DEFAULT = (
    "🎉 <b>Ваш ключ #{key_number} {action_text = продлен or готов}!</b>\n\n"
    "📅 <b>Сроки действия:</b>\n"
    "⏳ <b>Действует до: {expiry_date_str}</b>\n"
    "💌 <b>ID ключа:</b> <code>{email_display}</code>\n\n"
    "🗽 <b>Ваш ключ:</b>\n"
    "<code>{connection_string}</code>"
)

def get_purchase_success_text(action: str, key_number: int, expiry_date, connection_string: str, email: str = None):
    is_extend = action == "extend"
    expiry_date_str = expiry_date.strftime('%d.%m %H:%M')
    
    if email and str(email).endswith("@bot.local"):
        email = str(email).replace("@bot.local", "@bot")
    email_display = email if email else "Не указан"

    tpl = _get_msg_setting("msg_purchase_success_text", _PURCHASE_SUCCESS_DEFAULT)
    tpl = parse_named_conditionals(tpl, {'action_text': is_extend})
    return safe_format(
        tpl,
        key_number=key_number,
        expiry_date_str=expiry_date_str, email_display=email_display,
        connection_string=connection_string
    )