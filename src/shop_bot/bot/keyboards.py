import logging
import hashlib
import urllib.parse

from datetime import datetime

from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from aiogram.utils.keyboard import InlineKeyboardBuilder

from shop_bot.data_manager.remnawave_repository import get_setting
from shop_bot.data_manager.database import get_button_configs
from shop_bot.config import get_msk_time

logger = logging.getLogger(__name__)

main_reply_keyboard = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="🏠 Главное меню")]],
    resize_keyboard=True
)

def create_main_menu_keyboard(user_keys: list, trial_available: bool, is_admin: bool, balance: float = 0.0) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    
    if trial_available:
        text = get_setting("btn_trial_text") or "🎁 Попробовать бесплатно"
        builder.button(text=apply_html_to_button_text(text), callback_data="get_trial")
    
    builder.button(text=apply_html_to_button_text(get_setting("btn_profile_text") or "👤 Мой профиль"), callback_data="show_profile")
    base_my_keys = (get_setting("btn_my_keys_text") or "🔑 Мои ключи")
    keys_count = len(user_keys) if user_keys else 0
    builder.button(text=apply_html_to_button_text(f"{base_my_keys} ({keys_count})"), callback_data="manage_keys")
    
    builder.button(text=apply_html_to_button_text(get_setting("btn_buy_key_text") or "🛒 Купить ключ"), callback_data="buy_new_key")
    btn_topup_text = get_setting("btn_topup_text") or "💳 Пополнить баланс"
    if balance > 0:
        btn_topup_text += f" ({int(balance)})"
    builder.button(text=apply_html_to_button_text(btn_topup_text), callback_data="top_up_start")
    
    builder.button(text=(get_setting("btn_referral_text") or "🤝 Реферальная программа"), callback_data="show_referral_program")
    

    builder.button(text=(get_setting("btn_support_text") or "🆘 Поддержка"), callback_data="show_help")
    builder.button(text=(get_setting("btn_about_text") or "ℹ️ О проекте"), callback_data="show_about")
    

    builder.button(text=(get_setting("btn_speed_text") or "⚡ Скорость"), callback_data="user_speedtest_last")
    builder.button(text=(get_setting("btn_howto_text") or "❓ Как использовать"), callback_data="howto_vless")
    

    if is_admin:
        builder.button(text=(get_setting("btn_admin_text") or "⚙️ Админка"), callback_data="admin_menu")
    

    layout = []
    if trial_available:
        layout.append(1)
    layout.extend([2, 2, 1, 2, 2])
    if is_admin:
        layout.append(1)
    
    builder.adjust(*layout)
    
    return builder.as_markup()

def create_admin_menu_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="👥 Пользователи", callback_data="admin_users")
    builder.button(text="🎁 Выдать ключ", callback_data="admin_gift_key")
    builder.button(text="⚡ Тест скорости", callback_data="admin_speedtest")
    builder.button(text="🗄 Бэкап БД", callback_data="admin_backup_db")
    builder.button(text="👮 Администраторы", callback_data="admin_admins_menu")
    builder.button(text="🌍 Ключи на хосте", callback_data="admin_host_keys")
    builder.button(text="🎟 Промокоды", callback_data="admin_promo_menu")
    builder.button(text="📊 Мониторинг", callback_data="admin_monitor")
    builder.button(text="♻️ Восстановить БД", callback_data="admin_restore_db")
    builder.button(text="📢 Рассылка", callback_data="start_broadcast")
    builder.button(text=(get_setting("btn_back_to_menu_text") or "⬅️ Назад в меню"), callback_data="back_to_main_menu")

    builder.adjust(2, 2, 2, 2, 2, 1)
    return builder.as_markup()

def create_admins_menu_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="➕ Добавить админа", callback_data="admin_add_admin")
    builder.button(text="➖ Снять админа", callback_data="admin_remove_admin")
    builder.button(text="📋 Список админов", callback_data="admin_view_admins")
    builder.button(text="⬅️ В админ-меню", callback_data="admin_menu")
    builder.adjust(2, 2)
    return builder.as_markup()

def create_admin_users_keyboard(users: list[dict], page: int = 0, page_size: int = 10) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    start = page * page_size
    end = start + page_size
    for u in users[start:end]:
        user_id = u.get('telegram_id') or u.get('user_id') or u.get('id')
        username = u.get('username') or '—'
        title = f"{user_id} • @{username}" if username != '—' else f"{user_id}"
        builder.button(text=title, callback_data=f"admin_view_user_{user_id}")

    total = len(users)
    have_prev = page > 0
    have_next = end < total
    if have_prev:
        builder.button(text="⬅️ Назад", callback_data=f"admin_users_page_{page-1}")
    if have_next:
        builder.button(text="Вперёд ➡️", callback_data=f"admin_users_page_{page+1}")
    builder.button(text="⬅️ В админ-меню", callback_data="admin_menu")

    rows = [1] * len(users[start:end])
    tail = []
    if have_prev or have_next:
        tail.append(2 if (have_prev and have_next) else 1)
    tail.append(1)
    builder.adjust(*(rows + tail if rows else ([2] if (have_prev or have_next) else []) + [1]))
    return builder.as_markup()

def create_admin_user_actions_keyboard(user_id: int, is_banned: bool | None = None) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="➕ Начислить баланс", callback_data=f"admin_add_balance_{user_id}")
    builder.button(text="➖ Списать баланс", callback_data=f"admin_deduct_balance_{user_id}")
    builder.button(text="🎁 Выдать ключ", callback_data=f"admin_gift_key_{user_id}")
    builder.button(text="🤝 Рефералы пользователя", callback_data=f"admin_user_referrals_{user_id}")
    if is_banned is True:
        builder.button(text="✅ Разбанить", callback_data=f"admin_unban_user_{user_id}")
    else:
        builder.button(text="🚫 Забанить", callback_data=f"admin_ban_user_{user_id}")
    builder.button(text="✏️ Ключи пользователя", callback_data=f"admin_user_keys_{user_id}")
    builder.button(text="⬅️ К списку", callback_data="admin_users")
    builder.button(text="⬅️ В админ-меню", callback_data="admin_menu")

    builder.adjust(2, 2, 2, 2, 1)
    return builder.as_markup()

def create_admin_user_keys_keyboard(user_id: int, keys: list[dict]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    if keys:
        for k in keys:
            kid = k.get('key_id')
            host = k.get('host_name') or '—'
            email = k.get('key_email') or '—'
            title = f"#{kid} • {host} • {email[:20]}"
            builder.button(text=title, callback_data=f"admin_edit_key_{kid}")
    else:
        builder.button(text="Ключей нет", callback_data="noop")
    builder.button(text="⬅️ Назад", callback_data=f"admin_view_user_{user_id}")
    builder.adjust(1)
    return builder.as_markup()

def create_admin_key_actions_keyboard(key_id: int, user_id: int | None = None) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="➕ Добавить дни", callback_data=f"admin_key_extend_{key_id}")
    builder.button(text="🗑 Удалить ключ", callback_data=f"admin_key_delete_{key_id}")
    builder.button(text="⬅️ Назад к ключам", callback_data=f"admin_key_back_{key_id}")
    if user_id is not None:
        builder.button(text="👤 Перейти к пользователю", callback_data=f"admin_view_user_{user_id}")
        builder.adjust(2, 2)
    else:
        builder.adjust(2, 1)
    return builder.as_markup()

def create_admin_delete_key_confirm_keyboard(key_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Подтвердить удаление", callback_data=f"admin_key_delete_confirm_{key_id}")
    builder.button(text="❌ Отмена", callback_data=f"admin_key_delete_cancel_{key_id}")
    builder.adjust(1)
    return builder.as_markup()

def create_cancel_keyboard(callback: str = "admin_cancel") -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="❌ Отмена", callback_data=callback)
    return builder.as_markup()


def create_admin_cancel_keyboard() -> InlineKeyboardMarkup:
    return create_cancel_keyboard("admin_cancel")


def create_admin_promo_menu_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="➕ Создать промокод", callback_data="admin_promo_create")
    builder.button(text="📋 Список промокодов", callback_data="admin_promo_list")
    builder.button(text="⬅️ В админ-меню", callback_data="admin_menu")
    builder.adjust(1)
    return builder.as_markup()


def create_admin_promo_discount_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="% Процент", callback_data="admin_promo_discount_percent")
    builder.button(text="₽ Фиксированная", callback_data="admin_promo_discount_amount")
    builder.button(text="❌ Отмена", callback_data="admin_cancel")
    builder.adjust(2, 1)
    return builder.as_markup()

def create_admin_promo_code_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="🔄 Сгенерировать автоматически", callback_data="admin_promo_code_auto")
    builder.button(text="✍️ Ввести вручную", callback_data="admin_promo_code_custom")
    builder.button(text="❌ Отмена", callback_data="admin_cancel")
    builder.adjust(1, 1, 1)
    return builder.as_markup()

def create_admin_promo_limit_keyboard(kind: str) -> InlineKeyboardMarkup:

    prefix = "admin_promo_limit_total_" if kind == "total" else "admin_promo_limit_user_"
    builder = InlineKeyboardBuilder()
    builder.button(text="♾ Без лимита", callback_data=f"{prefix}inf")
    for v in (1, 5, 10, 50, 100):
        builder.button(text=str(v), callback_data=f"{prefix}{v}")
    builder.button(text="✍️ Другое число", callback_data=f"{prefix}custom")
    builder.button(text="❌ Отмена", callback_data="admin_cancel")
    builder.adjust(2, 3, 1, 1)
    return builder.as_markup()

def create_admin_promo_valid_from_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="⏱ Сейчас", callback_data="admin_promo_valid_from_now")
    builder.button(text="🗓 Сегодня 00:00", callback_data="admin_promo_valid_from_today")
    builder.button(text="🗓 Завтра 00:00", callback_data="admin_promo_valid_from_tomorrow")
    builder.button(text="➡️ Пропустить", callback_data="admin_promo_valid_from_skip")
    builder.button(text="✍️ Другая дата", callback_data="admin_promo_valid_from_custom")
    builder.button(text="❌ Отмена", callback_data="admin_cancel")
    builder.adjust(2, 2, 2)
    return builder.as_markup()

def create_admin_promo_valid_until_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="+1 день", callback_data="admin_promo_valid_until_plus1d")
    builder.button(text="+7 дней", callback_data="admin_promo_valid_until_plus7d")
    builder.button(text="+30 дней", callback_data="admin_promo_valid_until_plus30d")
    builder.button(text="➡️ Пропустить", callback_data="admin_promo_valid_until_skip")
    builder.button(text="✍️ Другая дата", callback_data="admin_promo_valid_until_custom")
    builder.button(text="❌ Отмена", callback_data="admin_cancel")
    builder.adjust(3, 2, 1)
    return builder.as_markup()

def create_admin_promo_description_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="➡️ Пропустить", callback_data="admin_promo_desc_skip")
    builder.button(text="✍️ Ввести текст", callback_data="admin_promo_desc_custom")
    builder.button(text="❌ Отмена", callback_data="admin_cancel")
    builder.adjust(1)
    return builder.as_markup()

def create_broadcast_options_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="➕ Добавить кнопку", callback_data="broadcast_add_button")
    builder.button(text="➡️ Пропустить", callback_data="broadcast_skip_button")
    builder.button(text="❌ Отмена", callback_data="cancel_broadcast")
    builder.adjust(2, 1)
    return builder.as_markup()

def create_broadcast_confirmation_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Отправить всем", callback_data="confirm_broadcast")
    builder.button(text="❌ Отмена", callback_data="cancel_broadcast")
    builder.adjust(2)
    return builder.as_markup()

def create_broadcast_cancel_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="❌ Отмена", callback_data="cancel_broadcast")
    return builder.as_markup()

def create_about_keyboard(channel_url: str | None, terms_url: str | None, privacy_url: str | None) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    if channel_url:
        builder.button(text="📰 Наш канал", url=channel_url)
    if terms_url:
        builder.button(text="📄 Условия использования", url=terms_url)
    if privacy_url:
        builder.button(text="🔒 Политика конфиденциальности", url=privacy_url)
    builder.button(text=(get_setting("btn_back_to_menu_text") or "⬅️ Назад в меню"), callback_data="back_to_main_menu")
    builder.adjust(1)
    return builder.as_markup()
    
def create_support_keyboard(support_user: str | None = None) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()

    username = (support_user or "").strip()
    if not username:
        username = (get_setting("support_bot_username") or get_setting("support_user") or "").strip()

    url: str | None = None
    if username:
        if username.startswith("@"):
            url = f"tg://resolve?domain={username[1:]}"
        elif username.startswith("tg://"):
            url = username
        elif username.startswith("http://") or username.startswith("https://"):


            try:

                part = username.split("/")[-1].split("?")[0]
                if part:
                    url = f"tg://resolve?domain={part}"
            except Exception:
                url = username
        else:

            url = f"tg://resolve?domain={username}"

    if url:
        builder.button(text=(get_setting("btn_support_text") or "🆘 Поддержка"), url=url)
        builder.button(text=(get_setting("btn_back_to_menu_text") or "⬅️ Назад в меню"), callback_data="back_to_main_menu")
    else:

        builder.button(text=(get_setting("btn_support_text") or "🆘 Поддержка"), callback_data="show_help")
        builder.button(text=(get_setting("btn_back_to_menu_text") or "⬅️ Назад в меню"), callback_data="back_to_main_menu")
    builder.adjust(1)
    return builder.as_markup()

def create_support_bot_link_keyboard(support_bot_username: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    username = support_bot_username.lstrip("@")
    deep_link = f"tg://resolve?domain={username}&start=new"
    builder.button(text="🆘 Открыть поддержку", url=deep_link)
    builder.button(text=(get_setting("btn_back_to_menu_text") or "⬅️ Назад в меню"), callback_data="back_to_main_menu")
    builder.adjust(1)
    return builder.as_markup()

def create_support_menu_keyboard(has_external: bool = False) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="✍️ Новое обращение", callback_data="support_new_ticket")
    builder.button(text="📨 Мои обращения", callback_data="support_my_tickets")
    if has_external:
        builder.button(text="🆘 Внешняя поддержка", callback_data="support_external")
    builder.button(text=(get_setting("btn_back_to_menu_text") or "⬅️ Назад в меню"), callback_data="back_to_main_menu")
    builder.adjust(1)
    return builder.as_markup()

def create_tickets_list_keyboard(tickets: list[dict]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    if tickets:
        for t in tickets:
            title = f"#{t['ticket_id']} • {t.get('status','open')}"
            if t.get('subject'):
                title += f" • {t['subject'][:20]}"
            builder.button(text=title, callback_data=f"support_view_{t['ticket_id']}")
    builder.button(text="⬅️ Назад", callback_data="support_menu")
    builder.adjust(1)
    return builder.as_markup()

def create_ticket_actions_keyboard(ticket_id: int, is_open: bool = True) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    if is_open:
        builder.button(text="💬 Ответить", callback_data=f"support_reply_{ticket_id}")
        builder.button(text="✅ Закрыть", callback_data=f"support_close_{ticket_id}")
    builder.button(text="⬅️ К списку", callback_data="support_my_tickets")
    builder.adjust(1)
    return builder.as_markup()

def create_host_selection_keyboard(hosts: list, action: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for host in hosts:
        callback_data = f"select_host_{action}_{host['host_name']}"
        builder.button(text=host['host_name'], callback_data=callback_data)
    builder.button(text="⬅️ Назад", callback_data="back_to_main_menu")
    builder.adjust(1)
    return builder.as_markup()

def create_plans_keyboard(plans: list[dict], action: str, host_name: str, key_id: int = 0) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for plan in plans:
        callback_data = f"buy_{host_name}_{plan['plan_id']}_{action}_{key_id}"
        builder.button(text=f"{plan['plan_name']} - {plan['price']:.0f} RUB", callback_data=callback_data)
    
    if action == "extend":
        back_callback = "manage_keys"
    else:
        from shop_bot.data_manager.remnawave_repository import get_all_hosts
        hosts = get_all_hosts(visible_only=True) or []
        back_callback = "back_to_main_menu" if len(hosts) == 1 else "buy_new_key"
        
    builder.button(text="⬅️ Назад", callback_data=back_callback)
    builder.adjust(1) 
    return builder.as_markup()


def create_device_tiers_keyboard(tiers: list[dict], host_name: str, plan_id: int, action: str, key_id: int = 0, selected_tier_id: int = None) -> InlineKeyboardMarkup:
    from shop_bot.data_manager.database import get_plan_by_id, get_setting
    plan = get_plan_by_id(plan_id) if plan_id else None
    months = int(plan.get('months') or 1) if plan else 1
    base_devices = int(get_setting(f"base_device_{host_name}") or "1")

    builder = InlineKeyboardBuilder()
    base_icon = "🟢" if selected_tier_id == 0 else "⚪️"
    builder.button(text=f"{base_icon} {base_devices} (вкл.)", callback_data="select_tier_0")
    total_btns = 1
    for t in tiers:
        is_selected = (selected_tier_id == t['tier_id'])
        icon = "🟢" if is_selected else "⚪️"
        diff = t['device_count'] - base_devices
        if diff < 0: diff = 0
        total_price = diff * t['price'] * months
        label = f"{icon} {t['device_count']} (+{total_price:.0f}₽)"
        builder.button(text=label, callback_data=f"select_tier_{t['tier_id']}")
        total_btns += 1
    if selected_tier_id is not None:
        builder.button(text="✅ Продолжить", callback_data="confirm_tier")
    if action == "extend":
        back_cb = "manage_keys"
    else:
        from shop_bot.data_manager.remnawave_repository import get_all_hosts
        hosts = get_all_hosts(visible_only=True) or []
        back_cb = "back_to_main_menu" if len(hosts) == 1 else "buy_new_key"
    builder.button(text="⬅️ Назад", callback_data=back_cb)
    rows = [2] * ((total_btns + 1) // 2)
    if selected_tier_id is not None:
        rows.append(1)
    rows.append(1)
    builder.adjust(*rows)
    return builder.as_markup()

def create_skip_email_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="➡️ Продолжить без почты", callback_data="skip_email")
    builder.button(text="⬅️ Назад к тарифам", callback_data="back_to_plans")
    builder.adjust(1)
    return builder.as_markup()

def create_payment_method_keyboard(
    payment_methods: dict,
    action: str,
    key_id: int,
    show_balance: bool | None = None,
    main_balance: float | None = None,
    price: float | None = None,
    promo_applied: bool = False,
    back_callback: str = "back_to_email_prompt"
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()


    pm = {
        "yookassa": bool((get_setting("yookassa_shop_id") or "") and (get_setting("yookassa_secret_key") or "")),
        "platega": ((get_setting("platega_enabled") or "false").strip().lower() == "true"),
        "platega_crypto": ((get_setting("platega_crypto_enabled") or "false").strip().lower() == "true"),
        "heleket": bool((get_setting("heleket_merchant_id") or "") and (get_setting("heleket_api_key") or "")),
        "cryptobot": bool(get_setting("cryptobot_token") or ""),
        "tonconnect": bool((get_setting("ton_wallet_address") or "") and (get_setting("tonapi_key") or "")),
        "yoomoney": ((get_setting("yoomoney_enabled") or "false").strip().lower() == "true"), 
        "stars": ((get_setting("stars_enabled") or "false").strip().lower() == "true"),
    }


    if show_balance:
        label = "💼 Оплатить с баланса"
        if main_balance is not None:
            try:
                label += f" ({main_balance:.0f} RUB)"
            except Exception:
                pass
        builder.button(text=label, callback_data="pay_balance")


    if pm.get("yookassa"):
        if (get_setting("sbp_enabled") or "false").strip().lower() == "true":
            builder.button(text="🏦 СБП / Банковская карта", callback_data="pay_yookassa")
        else:
            builder.button(text="🏦 Банковская карта", callback_data="pay_yookassa")
    
    if pm.get("platega"):
        builder.button(text="💳 СБП / Platega", callback_data="pay_platega")
    if pm.get("platega_crypto"):
        builder.button(text="🪙 Crypto / Platega", callback_data="pay_platega_crypto")
    if pm.get("cryptobot"):
        builder.button(text="💎 Криптовалюта", callback_data="pay_cryptobot")
    elif pm.get("heleket"):
        builder.button(text="💎 Криптовалюта", callback_data="pay_heleket")
    if pm.get("tonconnect"):
        callback_data_ton = "pay_tonconnect"
        logger.info(f"Creating TON button with callback_data: '{callback_data_ton}'")
        builder.button(text="🪙 TON Connect", callback_data=callback_data_ton)
    if pm.get("stars"):
        builder.button(text="⭐ Telegram Stars", callback_data="pay_stars")
    if pm.get("yoomoney"):
        builder.button(text="💜 ЮMoney (кошелёк)", callback_data="pay_yoomoney")
    


    if not promo_applied:
        builder.button(text="🎟 Ввести промокод", callback_data="enter_promo_code")

    builder.button(text="⬅️ Назад", callback_data=back_callback)
    builder.adjust(1)
    return builder.as_markup()

def create_ton_connect_keyboard(connect_url: str, back_callback: str = "back_to_main_menu") -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="🚀 Открыть кошелек", url=connect_url)
    builder.button(text=(get_setting("btn_back_to_menu_text") or "⬅️ Назад в меню"), callback_data=back_callback)
    builder.adjust(1)
    return builder.as_markup()

def create_payment_keyboard(payment_url: str, back_callback: str = "back_to_main_menu") -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="Перейти к оплате", url=payment_url)
    builder.button(text=(get_setting("btn_back_to_menu_text") or "⬅️ Назад в меню"), callback_data=back_callback)
    builder.adjust(1)
    return builder.as_markup()

def create_yoomoney_payment_keyboard(payment_url: str, payment_id: str, back_callback: str = "back_to_main_menu") -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="Перейти к оплате", url=payment_url)
    builder.button(text="🔄 Проверить оплату", callback_data=f"check_pending:{payment_id}")
    builder.button(text=(get_setting("btn_back_to_menu_text") or "⬅️ Назад в меню"), callback_data=back_callback)
    builder.adjust(1)
    return builder.as_markup()

def create_cryptobot_payment_keyboard(payment_url: str, invoice_id: int | str, back_callback: str = "back_to_main_menu") -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="Перейти к оплате", url=payment_url)
    builder.button(text="🔄 Проверить оплату", callback_data=f"check_crypto_invoice:{invoice_id}")
    builder.button(text=(get_setting("btn_back_to_menu_text") or "⬅️ Назад в меню"), callback_data=back_callback)
    builder.adjust(1)
    return builder.as_markup()

def create_topup_payment_method_keyboard(payment_methods: dict) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()

    pm = {
        "yookassa": bool((get_setting("yookassa_shop_id") or "") and (get_setting("yookassa_secret_key") or "")),
        "heleket": bool((get_setting("heleket_merchant_id") or "") and (get_setting("heleket_api_key") or "")),
        "cryptobot": bool(get_setting("cryptobot_token") or ""),
        "tonconnect": bool((get_setting("ton_wallet_address") or "") and (get_setting("tonapi_key") or "")),
        "yoomoney": ((get_setting("yoomoney_enabled") or "false").strip().lower() == "true"),
        "platega": ((get_setting("platega_enabled") or "false").strip().lower() == "true"),
        "platega_crypto": ((get_setting("platega_crypto_enabled") or "false").strip().lower() == "true"),
        "stars": ((get_setting("stars_enabled") or "false").strip().lower() == "true"),
    }

    if pm.get("yookassa"):
        if (get_setting("sbp_enabled") or "false").strip().lower() == "true":
            builder.button(text="🏦 СБП / Банковская карта", callback_data="topup_pay_yookassa")
        else:
            builder.button(text="🏦 Банковская карта", callback_data="topup_pay_yookassa")

    if pm.get("cryptobot"):
        builder.button(text="💎 Криптовалюта", callback_data="topup_pay_cryptobot")
    elif pm.get("heleket"):
        builder.button(text="💎 Криптовалюта", callback_data="topup_pay_heleket")
    if pm.get("tonconnect"):
        builder.button(text="🪙 TON Connect", callback_data="topup_pay_tonconnect")
    if pm.get("stars"):
        builder.button(text="⭐ Telegram Stars", callback_data="topup_pay_stars")
    if pm.get("yoomoney"):
        builder.button(text="💜 ЮMoney (кошелёк)", callback_data="topup_pay_yoomoney")
    if pm.get("platega"):
        builder.button(text="💳 СБП / Platega", callback_data="topup_pay_platega")
    if pm.get("platega_crypto"):
        builder.button(text="💎 Крипта / Platega", callback_data="topup_pay_platega_crypto")

    builder.button(text="⬅️ Назад", callback_data="show_profile")
    builder.adjust(1)
    return builder.as_markup()

def get_declension(n, forms):
    n = abs(n) % 100
    n1 = n % 10
    if n > 10 and n < 20: return forms[2]
    if n1 > 1 and n1 < 5: return forms[1]
    if n1 == 1: return forms[0]
    return forms[2]

def get_time_str(expiry_date: datetime) -> str:
    now = get_msk_time().replace(tzinfo=None)
    
    if expiry_date.tzinfo:
        expiry_date = expiry_date.astimezone(get_msk_time().tzinfo).replace(tzinfo=None)
    
    diff = expiry_date - now
    total_seconds = int(diff.total_seconds())
    
    if total_seconds < 0:
        return "(истек)"

    minutes = total_seconds // 60
    hours = minutes // 60
    days = hours // 24
    
    if days >= 365:
        years = int(round(days / 365.25))
        word = get_declension(years, ['год', 'года', 'лет'])
        return f"({years} {word})"
    elif days >= 30:
        months = int(round(days / 30.44))
        return f"({months} мес)"
    elif days >= 1:
        word = get_declension(days, ['день', 'дня', 'дней'])
        return f"({days} {word})"
    elif hours >= 1:
        return f"({hours} ч)"
    else:
        valid_min = max(1, minutes)
        return f"({valid_min} мин)"

def create_keys_management_keyboard(keys: list) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()

    if keys:
        for i, key in enumerate(keys):
            try:
                expiry_dt = datetime.fromisoformat(key['expiry_date'])
                if expiry_dt.tzinfo:
                     expiry_dt = expiry_dt.astimezone(get_msk_time().tzinfo).replace(tzinfo=None)
            except:
                expiry_dt = datetime.min

            status_icon = "✅" if expiry_dt > get_msk_time().replace(tzinfo=None) else "❌"
            host_name = key.get('host_name', 'Неизвестный хост')
            
            time_str = get_time_str(expiry_dt)

            # button_text = f"{status_icon} Ключ #{i+1} ({host_name}) {time_str}"
            button_text = f"{status_icon} #{i+1} ({host_name}) {time_str}"
            builder.button(text=button_text, callback_data=f"show_key_{key['key_id']}")
            
    builder.button(text=(get_setting("btn_buy_key_text") or "🛒 Купить ключ"), callback_data="buy_new_key")
    builder.button(text=(get_setting("btn_back_to_menu_text") or "⬅️ Назад в меню"), callback_data="back_to_main_menu")
    builder.adjust(1)
    return builder.as_markup()

def create_key_info_keyboard(key_id: int, connection_string: str | None = None) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    if connection_string:
        builder.button(text="📲 Подключиться", web_app=WebAppInfo(url=connection_string))
    builder.button(text="➕ Продлить этот ключ", callback_data=f"extend_key_{key_id}")
    builder.button(text="📱 Показать QR-код", callback_data=f"show_qr_{key_id}")
    builder.button(text="📱 Устройства", callback_data=f"key_devices_{key_id}")
    builder.button(text="📖 Инструкция", callback_data=f"howto_vless_{key_id}")
    builder.button(text="📝 Комментарии к ключу", callback_data=f"key_comments_{key_id}")
    builder.button(text="⬅️ Назад к списку ключей", callback_data="manage_keys")
    builder.adjust(1, 1, 2, 1, 1, 1) 
    return builder.as_markup()

def create_qr_keyboard(key_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="⬅️ Назад к ключу", callback_data=f"show_key_{key_id}")
    builder.adjust(1)
    return builder.as_markup()

def create_devices_list_keyboard(devices: list, key_id: int, page: int = 0, total_pages: int = 1) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder() 
    
    start_index = page * 5
    end_index = start_index + 5
    current_page_devices = devices[start_index:end_index]

    if current_page_devices:
        for i, dev in enumerate(current_page_devices):
            abs_index = start_index + i + 1
            
            dev_id = dev.get('hwid') or dev.get('uuid') or dev.get('id')
            if not dev_id:
                continue 
            builder.button(text=f"🗑 Удалить #{abs_index}", callback_data=f"del_dev_{dev_id}_{key_id}")
    
    row_btns = []
    if total_pages > 1:
        if page > 0:
            row_btns.append(InlineKeyboardButton(text="⬅️", callback_data=f"key_devices_{key_id}_{page-1}"))
        
        row_btns.append(InlineKeyboardButton(text=f"{page+1}/{total_pages}", callback_data="ignore"))
        
        if page < total_pages - 1:
            row_btns.append(InlineKeyboardButton(text="➡️", callback_data=f"key_devices_{key_id}_{page+1}"))
    
    builder.adjust(2)
    
    markup = builder.as_markup()
    
    if row_btns:
        markup.inline_keyboard.append(row_btns)
        
    markup.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Назад к ключу", callback_data=f"show_key_{key_id}")])
    
    return markup

def create_howto_vless_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="📱 Android", callback_data="howto_android")
    builder.button(text="📱 iOS", callback_data="howto_ios")
    builder.button(text="💻 Windows", callback_data="howto_windows")
    builder.button(text="🐧 Linux", callback_data="howto_linux")
    builder.button(text=(get_setting("btn_back_to_menu_text") or "⬅️ Назад в меню"), callback_data="back_to_main_menu")
    builder.adjust(2, 2, 1)
    return builder.as_markup()

def create_howto_vless_keyboard_key(key_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="📱 Android", callback_data="howto_android")
    builder.button(text="📱 iOS", callback_data="howto_ios")
    builder.button(text="💻 Windows", callback_data="howto_windows")
    builder.button(text="🐧 Linux", callback_data="howto_linux")
    builder.button(text="⬅️ Назад к ключу", callback_data=f"show_key_{key_id}")
    builder.adjust(2, 2, 1)
    return builder.as_markup()

def create_back_to_menu_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text=(get_setting("btn_back_to_menu_text") or "⬅️ Назад в меню"), callback_data="back_to_main_menu")
    return builder.as_markup()

def create_profile_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text=(get_setting("btn_topup_text") or "💳 Пополнить баланс"), callback_data="top_up_start")
    builder.button(text=(get_setting("btn_referral_text") or "🤝 Реферальная программа"), callback_data="show_referral_program")
    builder.button(text="🎁 Ввести промокод", callback_data="promo_uni")
    builder.button(text=(get_setting("btn_back_to_menu_text") or "⬅️ Назад в меню"), callback_data="back_to_main_menu")
    builder.adjust(1)
    return builder.as_markup()

def create_uni_promo_keys_keyboard(keys: list, code: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for i, key in enumerate(keys):
        host_name = key.get('host_name', 'Неизвестный хост')
        builder.button(text=f"Ключ #{i+1} ({host_name})", callback_data=f"apply_uni_{code}_{key['key_id']}")
    builder.button(text="❌ Отмена", callback_data="show_profile")
    builder.adjust(1)
    return builder.as_markup()

def create_key_comments_keyboard(key_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="⬅️ Назад к ключу", callback_data=f"show_key_{key_id}")
    builder.adjust(1)
    return builder.as_markup()

def create_welcome_keyboard(channel_url: str | None, is_subscription_forced: bool = False) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    
    if channel_url and is_subscription_forced:
        builder.button(text="📢 Перейти в канал", url=channel_url)
        builder.button(text="✅ Я подписался", callback_data="check_subscription_and_agree")
    elif channel_url:
        builder.button(text="📢 Наш канал (не обязательно)", url=channel_url)
        builder.button(text="✅ Принимаю условия", callback_data="check_subscription_and_agree")
    else:
        builder.button(text="✅ Принимаю условия", callback_data="check_subscription_and_agree")
        
    builder.adjust(1)
    return builder.as_markup()

def get_main_menu_button() -> InlineKeyboardButton:
    return InlineKeyboardButton(text="🏠 В главное меню", callback_data="show_main_menu")

def get_buy_button() -> InlineKeyboardButton:
    return InlineKeyboardButton(text="💳 Купить подписку", callback_data="buy_vpn")


def create_admin_users_pick_keyboard(users: list[dict], page: int = 0, page_size: int = 10, action: str = "gift") -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    start = page * page_size
    end = start + page_size
    for u in users[start:end]:
        user_id = u.get('telegram_id') or u.get('user_id') or u.get('id')
        username = u.get('username') or '—'
        title = f"{user_id} • @{username}" if username != '—' else f"{user_id}"
        builder.button(text=title, callback_data=f"admin_{action}_pick_user_{user_id}")
    total = len(users)
    have_prev = page > 0
    have_next = end < total
    if have_prev:
        builder.button(text="⬅️ Назад", callback_data=f"admin_{action}_pick_user_page_{page-1}")
    if have_next:
        builder.button(text="Вперёд ➡️", callback_data=f"admin_{action}_pick_user_page_{page+1}")
    builder.button(text="⬅️ В админ-меню", callback_data="admin_menu")
    rows = [1] * len(users[start:end])
    tail = []
    if have_prev or have_next:
        tail.append(2 if (have_prev and have_next) else 1)
    tail.append(1)
    builder.adjust(*(rows + tail if rows else ([2] if (have_prev or have_next) else []) + [1]))
    return builder.as_markup()

def create_admin_hosts_pick_keyboard(hosts: list[dict], action: str = "gift") -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    if hosts:
        for h in hosts:
            name = h.get('host_name')
            if action == "speedtest":

                builder.button(text=name, callback_data=f"admin_{action}_pick_host_{name}")
                builder.button(text="🛠 Автоустановка", callback_data=f"admin_speedtest_autoinstall_{name}")
            else:
                builder.button(text=name, callback_data=f"admin_{action}_pick_host_{name}")
    else:
        builder.button(text="Хостов нет", callback_data="noop")

    if action == "speedtest":
        builder.button(text="🚀 Запустить для всех", callback_data="admin_speedtest_run_all")
        builder.button(text="🔌 SSH цели", callback_data="admin_speedtest_ssh_targets")
    builder.button(text="⬅️ Назад", callback_data=f"admin_{action}_back_to_users")

    if action == "speedtest":
        rows = [2] * (len(hosts) if hosts else 1)

        tail = [2, 1]
    else:
        rows = [1] * (len(hosts) if hosts else 1)
        tail = [1]
    builder.adjust(*(rows + tail))
    return builder.as_markup()


def create_admin_ssh_targets_keyboard(ssh_targets: list[dict]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    if ssh_targets:
        for t in ssh_targets:
            name = t.get('target_name')

            try:
                digest = hashlib.sha1((name or '').encode('utf-8', 'ignore')).hexdigest()
            except Exception:
                digest = hashlib.sha1(str(name).encode('utf-8', 'ignore')).hexdigest()

            builder.button(text=name, callback_data=f"stt:{digest}")
            builder.button(text="🛠 Автоустановка", callback_data=f"stti:{digest}")
    else:
        builder.button(text="SSH-целей нет", callback_data="noop")

    builder.button(text="🚀 Запустить для всех", callback_data="admin_speedtest_run_all_targets")
    builder.button(text="⬅️ В админ-меню", callback_data="admin_menu")

    rows = [2] * (len(ssh_targets) if ssh_targets else 1)
    rows.extend([1, 1])
    builder.adjust(*rows)
    return builder.as_markup()

def create_admin_keys_for_host_keyboard(
    host_name: str,
    keys: list[dict],
    page: int = 0,
    page_size: int = 10,
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    total = len(keys or [])
    if not keys:
        builder.button(text="Ключей на хосте нет", callback_data="noop")
        builder.button(text="⬅️ К выбору хоста", callback_data="admin_hostkeys_back_to_hosts")
        builder.button(text="⬅️ В админ-меню", callback_data="admin_menu")
        builder.adjust(1)
        return builder.as_markup()

    start = max(page, 0) * page_size
    end = start + page_size
    page_items = keys[start:end]

    for k in page_items:
        kid = k.get('key_id')
        email = (k.get('key_email') or '—')
        expiry_raw = k.get('expiry_date') or '—'

        try:
            dt = datetime.fromisoformat(str(expiry_raw))
            if dt.tzinfo:
                dt = dt.astimezone(get_msk_time().tzinfo)
            expiry = dt.strftime('%d.%m.%Y')
        except Exception:
            expiry = str(expiry_raw)[:10]

        title = f"#{kid} • {email[:18]} • {expiry}"
        builder.button(text=title, callback_data=f"admin_edit_key_{kid}")

    have_prev = start > 0
    have_next = end < total
    if have_prev:
        builder.button(text="⬅️ Назад", callback_data=f"admin_hostkeys_page_{page-1}")
    if have_next:
        builder.button(text="Вперёд ➡️", callback_data=f"admin_hostkeys_page_{page+1}")

    builder.button(text="⬅️ К выбору хоста", callback_data="admin_hostkeys_back_to_hosts")
    builder.button(text="⬅️ В админ-меню", callback_data="admin_menu")

    rows = [1] * len(page_items)
    tail = []
    if have_prev or have_next:
        tail.append(2 if (have_prev and have_next) else 1)
    tail.append(2)
    builder.adjust(*(rows + tail if rows else tail))
    return builder.as_markup()

def apply_html_to_button_text(text: str) -> str:
    import re
    if not text: return text
    
    def to_bold(m):
        content = m.group(1)
        res = ""
        for char in content:
            if 'A' <= char <= 'Z': res += chr(ord(char) + 0x1D400 - ord('A'))
            elif 'a' <= char <= 'z': res += chr(ord(char) + 0x1D41A - ord('a'))
            elif '0' <= char <= '9': res += chr(ord(char) + 0x1D7CE - ord('0'))
            else: res += char
        return res

    def to_italic(m):
        content = m.group(1)
        res = ""
        for char in content:
            if 'A' <= char <= 'Z': res += chr(ord(char) + 0x1D434 - ord('A'))
            elif 'a' <= char <= 'z': res += chr(ord(char) + 0x1D44E - ord('a'))
            else: res += char
        return res

    text = re.sub(r'<b>(.*?)</b>', to_bold, text, flags=re.DOTALL)
    text = re.sub(r'<i>(.*?)</i>', to_italic, text, flags=re.DOTALL)
    
    clean_text = re.sub(r'<[^>]+>', '', text)
    return clean_text

def create_admin_months_pick_keyboard(action: str = "gift") -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for m in (1, 3, 6, 12):
        builder.button(text=f"{m} мес.", callback_data=f"admin_{action}_pick_months_{m}")
    builder.button(text="⬅️ Назад", callback_data=f"admin_{action}_back_to_hosts")
    builder.adjust(2, 2, 1)
    return builder.as_markup()


def create_dynamic_keyboard(menu_type: str, user_keys: list = None, trial_available: bool = False, is_admin: bool = False, balance: float = 0.0, key_id: int = None, connection_string: str = None) -> InlineKeyboardMarkup:
    """Create a keyboard based on database configuration"""
    try:
        button_configs = get_button_configs(menu_type)

        
        if not button_configs:
            logger.warning(f"No button configs found for {menu_type}, using fallback")

            if menu_type == "main_menu":
                return create_main_menu_keyboard(user_keys or [], trial_available, is_admin, balance)
            elif menu_type == "admin_menu":
                return create_admin_menu_keyboard()
            elif menu_type == "profile_menu":
                return create_profile_keyboard()
            elif menu_type == "support_menu":
                return create_support_menu_keyboard()
            elif menu_type == "key_info_menu" and key_id is not None:
                return create_key_info_keyboard(key_id, connection_string)
            else:
                return create_back_to_menu_keyboard()

        builder = InlineKeyboardBuilder()
        

        rows: dict[int, list[dict]] = {}
        for config in button_configs:
            row_pos = config.get('row_position', 0)
            rows.setdefault(row_pos, []).append(config)


        layout: list[int] = []
        for row_pos in sorted(rows.keys()):
            original_row = sorted(rows[row_pos], key=lambda x: x.get('column_position', 0))
            included_row: list[dict] = []
            row_buttons_objs: list[InlineKeyboardButton] = []



            for cfg in original_row:
                text = cfg.get('text', '')
                callback_data = cfg.get('callback_data')
                url = cfg.get('url')
                button_id = cfg.get('button_id', '')


                if menu_type == "main_menu" and button_id == "trial" and not trial_available:

                    continue
                

                if menu_type == "main_menu" and button_id == "admin" and not is_admin:

                    continue


                if menu_type == "main_menu" and user_keys is not None and "({len(user_keys)})" in text:
                    keys_count = len(user_keys) if user_keys else 0
                    text = text.replace("({len(user_keys)})", f"({keys_count})")
                
                if menu_type == "main_menu" and "{balance}" in text:
                    text = text.replace("{balance}", f"{int(balance)}")
                if menu_type == "main_menu" and "{len(balance)}" in text:
                     text = text.replace("{len(balance)}", f"{int(balance)}")

                # Placeholders for Key Info
                if key_id is not None:
                    if callback_data and "{key_id}" in callback_data:
                        callback_data = callback_data.replace("{key_id}", str(key_id))
                    if url and "{key_id}" in url:
                        url = url.replace("{key_id}", str(key_id))

                if connection_string:
                   if url and "{connection_string}" in url:
                       url = url.replace("{connection_string}", connection_string)
                       pass
                
                is_web_app = False
                if cfg.get('url') == "{connection_string}" and connection_string:
                     is_web_app = True

                if is_web_app:
                     row_buttons_objs.append(InlineKeyboardButton(text=apply_html_to_button_text(text), web_app=WebAppInfo(url=url)))
                     included_row.append(cfg)

                elif url:
                    row_buttons_objs.append(InlineKeyboardButton(text=apply_html_to_button_text(text), url=url))
                    included_row.append(cfg)
                elif callback_data:
                    row_buttons_objs.append(InlineKeyboardButton(text=apply_html_to_button_text(text), callback_data=callback_data))
                    included_row.append(cfg)


            if not included_row:
                continue
            
            if row_buttons_objs:
                builder.row(*row_buttons_objs)
                layout.append(len(row_buttons_objs)) 
        return builder.as_markup()
        
    except Exception as e:
        logger.error(f"Error creating dynamic keyboard for {menu_type}: {e}")

        if menu_type == "main_menu":
            return create_main_menu_keyboard(user_keys or [], trial_available, is_admin, balance)
        elif menu_type == "key_info_menu" and key_id is not None:
             return create_key_info_keyboard(key_id, connection_string)
        else:
            return create_back_to_menu_keyboard()

def create_dynamic_main_menu_keyboard(user_keys: list, trial_available: bool, is_admin: bool, balance: float = 0.0) -> InlineKeyboardMarkup:
    """Create main menu keyboard using dynamic configuration"""
    return create_dynamic_keyboard("main_menu", user_keys, trial_available, is_admin, balance)

def create_dynamic_admin_menu_keyboard() -> InlineKeyboardMarkup:
    """Create admin menu keyboard using dynamic configuration"""
    return create_dynamic_keyboard("admin_menu")

def create_dynamic_profile_keyboard() -> InlineKeyboardMarkup:
    """Create profile keyboard using dynamic configuration"""
    return create_dynamic_keyboard("profile_menu")

def create_dynamic_support_menu_keyboard() -> InlineKeyboardMarkup:
    """Create support menu keyboard using dynamic configuration"""
    return create_dynamic_keyboard("support_menu")

def create_dynamic_key_info_keyboard(key_id: int, connection_string: str | None = None) -> InlineKeyboardMarkup:
    """Create key info keyboard using dynamic configuration"""
    return create_dynamic_keyboard("key_info_menu", key_id=key_id, connection_string=connection_string)

def create_back_to_profile_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="⬅️ Назад в профиль", callback_data="show_profile")
    builder.adjust(1)
    return builder.as_markup()

def create_referral_keyboard(referral_link: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    
    referral_discount = get_setting("referral_discount") or "0"
    share_text = (
        f"🔥 ТВОЯ ЛИЧНАЯ СКИДКА {referral_discount}%!\n\n"
        f"Тебе открыт доступ к закрытому VPN. 🤫\n"
        f"🚀 YouTube 4K | 🌐 Много стран | 🛡 Анонимность\n\n"
        f"👇 ЗАБИРАЙ, ПОКА НЕ СГОРЕЛО! 👇\n" 
    )
    
    encoded_text = urllib.parse.quote(share_text)
    encoded_url = urllib.parse.quote(referral_link)
    full_share_url = f"https://t.me/share/url?url={encoded_url}&text={encoded_text}"
    
    builder.button(text="📤 Поделиться", url=full_share_url)
    builder.button(text="⬅️ Назад в профиль", callback_data="show_profile")
    
    builder.adjust(1)
    return builder.as_markup()
