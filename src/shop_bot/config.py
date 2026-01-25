from datetime import datetime, timedelta
from aiogram import html

CHOOSE_PLAN_MESSAGE = "Выберите подходящий тариф:"
CHOOSE_PAYMENT_METHOD_MESSAGE = "Выберите удобный способ оплаты:"
VPN_INACTIVE_TEXT = "❌ <b>Статус VPN:</b> Неактивен (срок истек)"
VPN_NO_DATA_TEXT = "ℹ️ <b>Статус VPN:</b> У вас пока нет активных ключей."

def get_profile_text(username, total_spent, total_months, vpn_status_text):
    return (
        f"👤 <b>Профиль:</b> {username}\n\n"
        f"💰 <b>Потрачено всего:</b> {total_spent:.0f} RUB\n"
        f"📅 <b>Приобретено месяцев:</b> {total_months}\n\n"
        f"{vpn_status_text}"
    )

def get_vpn_active_text(days_left, hours_left):
    return (
        f"✅ <b>Статус VPN:</b> Активен\n"
        f"⏳ <b>Осталось:</b> {days_left} д. {hours_left} ч."
    )

def _get_status_text(remaining):
    total_seconds = int(remaining.total_seconds())
    if total_seconds < 0:
        return "Не активен (Истек)"
    
    minutes = total_seconds // 60
    hours = minutes // 60
    days = hours // 24
    
    if days >= 365:
        years = round(days / 365, 1)
        return f"Активен ({years} год.)"
    if days >= 30:
        months = int(round(days / 30))
        return f"Активен ({months} мес.)"
    if days >= 1:
        return f"Активен ({days} д.)"
    if hours >= 1:
        return f"Активен ({hours} ч.)"
    return f"Активен ({max(1, minutes)} мин.)"

def get_key_info_text(key_number, expiry_date, created_date, connection_string, email=None, hwid_limit=None, hwid_usage=None, traffic_limit=None, traffic_used=None, comment=None):
    now = datetime.now()
    remaining = expiry_date - now
    days_left = remaining.days
    
    status_icon = "🟢"
    status_text = _get_status_text(remaining)
    
    if days_left <= 10:
        status_icon = "🟡"
    
    if days_left < 0:
        status_icon = "🔴"

    traffic_block = ""
    if traffic_limit:
        t_lim_str = str(traffic_limit).strip()
        t_lim_display = "∞" if t_lim_str == "0" or t_lim_str.startswith("0 ") else t_lim_str
        traffic_block = f"{traffic_used} / {t_lim_display}"

    hwid_block = ""
    if hwid_limit is not None:
        limit_str = str(hwid_limit)
        limit_display = "∞" if limit_str == "0" or (limit_str.isdigit() and int(limit_str) > 98) else limit_str
        hwid_block = f"{hwid_usage} / {limit_display}"

    if email and str(email).endswith("@bot.local"):
        email = str(email).replace("@bot.local", "@bot")

    comment_block = ""
    if comment:
        comment_block = f"💬 <b>Комментарий: {html.quote(comment)} ♻️</b>\n"

    return (
        f"🔑 <b>Информация о ключе #{key_number}</b>\n"
        f"{comment_block}"
        f"\n📅 <b>Сроки действия:</b>\n"
        f"{status_icon} <b>Статус:</b> {status_text}\n"
        f"➕ <b>Куплен:</b> {created_date.strftime('%d.%m.%Y')}\n"
        f"⏳ <b>Истекает:</b> {expiry_date.strftime('%d.%m.%Y %H:%M')}\n"
        f"💌 <b>ID ключа:</b> <code>{email}</code>\n\n"
        f"📉 <b>Использование:</b>\n"
        f"🛰 <b>Лимит трафика:</b> {traffic_block}\n" 
        f"📱 <b>Лимит устройств:</b> {hwid_block}\n"
        f"🗽 <b>Ваш ключ:</b>\n<code>{connection_string}</code>"
    )


def get_purchase_success_text(action: str, key_number: int, expiry_date, connection_string: str, email: str = None):
    action_text = "продлен" if action == "extend" else "готов"
    expiry_date_str = expiry_date.strftime('%d.%m %H:%M')
    
    # Обработка email для скрытия служебного суффикса @bot.local
    if email and str(email).endswith("@bot.local"):
        email = str(email).replace("@bot.local", "@bot")
    email_display = email if email else "Не указан"

    return (
        f"🎉 <b>Ваш ключ #{key_number} {action_text}!</b>\n\n"
        f"📅 <b>Сроки действия:</b>\n"
        f"⏳ <b>Действует до: {expiry_date_str}</b>\n"
        f"💌 <b>ID ключа:</b> <code>{email_display}</code>\n\n"
        f"🗽 <b>Ваш ключ:</b>\n"
        f"<code>{connection_string}</code>"
    )