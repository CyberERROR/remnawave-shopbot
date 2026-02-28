import logging
import os
import uuid
import qrcode
import aiohttp
import re
import aiohttp
import hashlib
import json
import base64
import asyncio
import time
from collections import deque

from urllib.parse import urlencode
from hmac import compare_digest
from functools import wraps
from io import BytesIO
from yookassa import Payment, Configuration
from datetime import datetime, timedelta, timezone
from aiosend import CryptoPay, TESTNET
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict

from pytonconnect import TonConnect
from aiogram import Router, F, Bot, types, html
from aiogram.types import BufferedInputFile, LabeledPrice, PreCheckoutQuery, FSInputFile, InputMediaPhoto
from aiogram.filters import Command, CommandObject, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.enums import ChatMemberStatus
from aiogram.exceptions import TelegramBadRequest
from aiogram.utils.keyboard import InlineKeyboardBuilder
from shop_bot.bot import keyboards
from shop_bot.modules.platega_api import PlategaAPI
from shop_bot.modules.heleket_api import create_heleket_payment_request
from shop_bot.data_manager.remnawave_repository import (
    add_to_balance,
    deduct_from_balance,
    get_setting,
    get_user,
    register_user_if_not_exists,
    get_next_key_number,
    create_payload_pending,
    get_pending_status,
    find_and_complete_pending_transaction,
    get_user_keys,
    get_balance,
    get_referral_count,
    get_plan_by_id,
    get_all_hosts,
    get_plans_for_host,
    redeem_promo_code,
    check_promo_code_available,
    update_promo_code_status,
    record_key_from_payload,
    add_to_referral_balance_all,
    get_referral_balance_all,
    get_referral_balance,
    get_all_users,
    set_terms_agreed,
    set_referral_start_bonus_received,
    set_trial_used,
    update_user_stats,
    log_transaction,
    is_admin,
    get_host,
    check_transaction_exists,
    get_device_tiers,
    get_device_tier_by_id,
    redeem_universal_promo,
)
from shop_bot.data_manager.database import get_seller_user, adjust_user_balance

from shop_bot.config import (
    get_profile_text,
    get_vpn_active_text,
    VPN_INACTIVE_TEXT,
    VPN_NO_DATA_TEXT,
    get_key_info_text,
    CHOOSE_PAYMENT_METHOD_MESSAGE,
    get_purchase_success_text,
    get_msk_time
)
from shop_bot.data_manager import remnawave_repository as rw_repo
from shop_bot.modules import remnawave_api

TELEGRAM_BOT_USERNAME = None
PAYMENT_METHODS = None
ADMIN_ID = None
CRYPTO_BOT_TOKEN = get_setting("cryptobot_token")

logger = logging.getLogger(__name__)

user_command_times = {}
user_blocked_until = {}
user_spam_level = {}

# ===== УТИЛИТА БЕЗОПАСНАЯ СТРОКА =====
# Преобразует входное значение в строку, заменяя None на пустую строку
def safe_str(val):
    if val is None: return ""
    return str(val)

def get_device_emoji(user_agent: str = "", platform: str = "", device_model: str = "") -> str:
    combined = f"{user_agent} {platform} {device_model}".lower()
    if any(k in combined for k in ("iphone", "ipad", "ios")):
        return '🍏'
    if any(k in combined for k in ("mac", "darwin", "macos")):
        return '🍎'
    if any(k in combined for k in ("windows", "win32", "win64", "win", "pc")):
        return '🖥'
    if "linux" in combined:
        return '🐧'
    if "android" in combined:
        return '📱'
    if any(k in combined for k in ("tv", "smart", "tizen", "webos")):
        return '📺'
    return '⚙️'
# ===== Конец функции safe_str =====

# ===== ГЕНЕРАЦИЯ КОММЕНТАРИЯ ТРАНЗАКЦИИ =====
# Создает стандартизированное описание для транзакции на основе данных пользователя и типа действия
def get_transaction_comment(user: types.User, action_type: str, value: any, host_name: str = None) -> str:
    pay_info_json = get_setting('pay_info_comment')
    try: pay_info = json.loads(pay_info_json) if pay_info_json else {}
    except (ValueError, TypeError): pay_info = {}
        
    user_id = user.id
    username = f"@{user.username}" if user.username else None
    first_name = user.first_name or None
    
    user_info_parts = []
    
    if pay_info.get('id', 1): user_info_parts.append(f"ID: {user_id}")
    if pay_info.get('username', 1) and username: user_info_parts.append(f"User: {username}")
    if pay_info.get('first_name', 1) and first_name: user_info_parts.append(f"Имя: {first_name}")
    if pay_info.get('host_name', 1) and host_name: user_info_parts.append(f"Хост: {host_name}")
    
    user_info = ", ".join(user_info_parts)
    info_suffix = f" ({user_info})" if user_info else ""
    
    if action_type == 'new': return f"Подписка на {value} мес.{info_suffix}"
    elif action_type == 'extend': return f"Продление на {value} мес.{info_suffix}"
    elif action_type == 'topup': return f"Пополнение баланса на {value} RUB{info_suffix}"
    return f"Транзакция (ID: {user_id})"
# ===== Конец функции get_transaction_comment =====




# ===== ПОЛУЧЕНИЕ СКИДКИ ПРОДАВЦА =====
def get_seller_discount_percent(user_id: int) -> Decimal:
    try:
        user_data = get_user(user_id)
        if not user_data:
            return Decimal("0")
        
        is_active = user_data.get('seller_active')
        
        if is_active:
            seller_info = get_seller_user(user_id)
            if seller_info:
                raw_sale = seller_info.get('seller_sale', 0)
                seller_ref = seller_info.get('seller_ref', 0)
                seller_uuid = seller_info.get('seller_uuid', '0')
                sale = Decimal(str(raw_sale))
                logger.info(f"[SELLER_{user_id}] - информация о Seller (скидка на тарифы {raw_sale}%, Реф {seller_ref}%, Сквад Remna: {seller_uuid})")
                return sale
    except Exception as e:
        logger.error(f"[SELLER_{user_id}] - ошибка: {e}")
    return Decimal("0")
# ===== Конец функции get_seller_discount_percent =====


# ===== ПОЛУЧЕНИЕ ИНДИВИДУАЛЬНОГО РЕФЕРАЛЬНОГО ПРОЦЕНТА ДЛЯ SELLER =====
def get_seller_referral_percent(user_id: int) -> Decimal:
    try:
        user_data = get_user(user_id)
        if not user_data:
            return Decimal("0")
        
        is_active = user_data.get('seller_active')
        
        if is_active:
            seller_info = get_seller_user(user_id)
            if seller_info:
                seller_ref = seller_info.get('seller_ref', 0)
                ref_percent = Decimal(str(seller_ref))
                logger.info(f"[SELLER_{user_id}] - использован индивидуальный реферальный процент {seller_ref}%")
                return ref_percent
    except Exception as e:
        logger.error(f"[SELLER_{user_id}] - ошибка получения реф%: {e}")
    return Decimal("0")
# ===== Конец функции get_seller_referral_percent =====


# ===== ПОЛУЧЕНИЕ ВНЕШНЕГО СКВАДА ДЛЯ SELLER =====
def get_seller_external_squad(user_id: int) -> str | None:
    try:
        user_data = get_user(user_id)
        if not user_data:
            return None
        
        is_active = user_data.get('seller_active')
        
        if is_active:
            seller_info = get_seller_user(user_id)
            if seller_info:
                seller_uuid = seller_info.get('seller_uuid', '').strip()
                if seller_uuid and seller_uuid != '0':
                    logger.info(f"[SELLER_{user_id}] - используется внешний сквад Remnawave: {seller_uuid}")
                    return seller_uuid
    except Exception as e:
        logger.error(f"[SELLER_{user_id}] - ошибка получения external squad: {e}")
    return None
# ===== Конец функции get_seller_external_squad =====


# ===== РАСЧЕТ ЦЕНЫ ЗАКАЗА =====
# Вычисляет итоговую стоимость подписки с учетом потенциальных скидок для рефералов и промокодов
def calculate_order_price(plan: dict, user_data: dict, promo_code: str = None, promo_discount: Decimal = 0) -> Decimal:
    base_price = Decimal(str(plan['price']))
    
    # Seller Discount
    try:
        # Determine User ID from user_data
        uid = user_data.get('telegram_id') or user_data.get('user_id') or user_data.get('id')
        if uid:
            sale_percent = get_seller_discount_percent(int(uid))
            if sale_percent > 0:
                discount = (base_price * sale_percent / 100).quantize(Decimal("0.01"))
                base_price -= discount
                # logger.info(f"CalcPrice: Applied {sale_percent}% discount. New price: {base_price}")
    except Exception as e:
        logger.error(f"Error in calculate_order_price discount block: {e}")

    if user_data.get('referred_by') and user_data.get('total_spent', 0) == 0:
        try:
            discount_percentage = Decimal(get_setting("referral_discount") or "0")
            if discount_percentage > 0:
                discount_amount = (base_price * discount_percentage / 100).quantize(Decimal("0.01"))
                base_price -= discount_amount
        except Exception: pass

    if promo_code and promo_discount > 0:
        try: discount_dec = Decimal(str(promo_discount))
        except Exception: discount_dec = Decimal("0.00")
        base_price = (base_price - discount_dec).quantize(Decimal("0.01"))
    
    if base_price < Decimal('0.01'): base_price = Decimal('0.01')
    return base_price
# ===== Конец функции calculate_order_price =====

# ===== СОЗДАНИЕ ОЖИДАЮЩЕГО ПЛАТЕЖА =====
# Регистрирует временную запись о платеже в базе данных и формирует метаданные для обработки
async def create_pending_payment(user_id: int, amount: float, payment_method: str, action: str, metadata_source: dict, plan_id: int = None, months: int = 0) -> str:
    payment_id = str(uuid.uuid4())
    metadata = {
        "user_id": user_id,
        "months": months,
        "price": float(amount),
        "action": action,
        "key_id": metadata_source.get('key_id'),
        "host_name": metadata_source.get('host_name'),
        "plan_id": plan_id,
        "customer_email": metadata_source.get('customer_email'),
        "payment_method": payment_method,
        "payment_id": payment_id,
        "promo_code": metadata_source.get("promo_code"),
        "promo_discount": metadata_source.get("promo_discount"),
        "tier_device_count": metadata_source.get("tier_device_count"),
        "tier_price": metadata_source.get("tier_price"),
    }
    create_payload_pending(payment_id, user_id, float(amount), metadata)
    return payment_id, metadata
# ===== Конец функции create_pending_payment =====

# ===== ПОЛУЧЕНИЕ КЛАВИАТУРЫ ОПЛАТЫ =====
# Генерирует соответствующую inline-клавиатуру в зависимости от выбранного метода платежа
def get_payment_keyboard(payment_method: str, pay_url: str = None, invoice_id: int = None, back_callback: str = "back_to_main_menu"):
    if payment_method == 'CryptoBot': return keyboards.create_cryptobot_payment_keyboard(pay_url, invoice_id, back_callback)
    elif payment_method in ['YooMoney', 'Heleket', 'Platega', 'Platega Crypto', 'YooKassa']:
         if payment_method == 'YooMoney' and invoice_id: return keyboards.create_yoomoney_payment_keyboard(pay_url, str(invoice_id), back_callback)
         return keyboards.create_payment_keyboard(pay_url, back_callback)
    elif payment_method == 'TON Connect': return keyboards.create_ton_connect_keyboard(pay_url, back_callback)
    return None
# ===== Конец функции get_payment_keyboard =====

# ===== ОТПРАВКА ИНСТРУКЦИЙ ПОДКЛЮЧЕНИЯ =====
# Формирует и отправляет пользователю подробную инструкцию по настройке VPN для выбранной ОС
async def send_instruction_response(callback: types.CallbackQuery, os_type: str, instruction_key: str = None):
    await callback.answer()
    
    text_key = instruction_key or f"howto_{os_type}_text"
    image_key = "howto_image"
    instruction_text = get_setting(text_key)
    
    if not instruction_text:
        defaults = {
            "android": (
                "<b>Подключение на Android</b>\n\n"
                "1. <b>Установите V2RayTun:</b> Google Play.\n"
                "2. <b>Скопируйте ключ:</b> В разделе «Моя подписка».\n"
                "3. <b>Импорт:</b> В приложении нажмите «+» -> «Импорт из буфера».\n"
                "4. <b>Подключение:</b> Нажмите кнопку подключения."
            ),
            "ios": (
                "<b>Подключение на iOS</b>\n\n"
                "1. <b>Установите V2RayTun:</b> App Store.\n"
                "2. <b>Скопируйте ключ:</b> В боте.\n"
                "3. <b>Импорт:</b> В приложении нажмите «+» -> «Импорт из буфера».\n"
                "4. <b>Подключение:</b> Включите переключатель."
            ),
            "windows": (
                "<b>Подключение на Windows</b>\n\n"
                "1. <b>Скачайте Nekoray:</b> GitHub.\n"
                "2. <b>Импорт:</b> Скопируйте ключ -> Server -> Import from clipboard.\n"
                "3. <b>Запуск:</b> Server -> Start."
            ),
            "linux": (
                "<b>Подключение на Linux</b>\n\n"
                "Используйте Nekoray или любой клиент с поддержкой VLESS."
            )
        }
        instruction_text = defaults.get(os_type, f"Инструкция для {os_type} не найдена.")

    image_path = get_setting(image_key)
    photo_path = image_path if (image_path and os.path.exists(image_path)) else None

    try: markup = keyboards.create_howto_vless_keyboard()
    except Exception:
        builder = InlineKeyboardBuilder()
        builder.button(text="⬅️ Назад", callback_data="howto_vless")
        markup = builder.as_markup()
    
    await smart_edit_message(callback.message, instruction_text, markup, photo_path)
# ===== Конец функции send_instruction_response =====

# ===== СОЗДАНИЕ ИНВОЙСА CRYPTOBOT =====
# Взаимодействует с API CryptoBot для генерации ссылки на оплату в криптовалюте
async def create_cryptobot_api_invoice(amount: float, payload_str: str) -> tuple[str, int] | None:
    token = (get_setting("cryptobot_token") or "").strip()
    if not token:
        logger.error("CryptoBot: API токен не сконфигурирован в настройках.")
        return None

    price_str = f"{Decimal(str(amount)).quantize(Decimal('0.01'))}"
    body = {"amount": price_str, "currency_type": "fiat", "fiat": "RUB", "payload": payload_str}
    headers = {"Crypto-Pay-API-Token": token, "Content-Type": "application/json"}
    url = "https://pay.crypt.bot/api/createInvoice"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=body, timeout=20) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    logger.error(f"CryptoBot: Ошибка HTTP {resp.status}: {text}")
                    return None
                data = await resp.json(content_type=None)
                if isinstance(data, dict) and data.get("ok") and isinstance(data.get("result"), dict):
                    res = data["result"]
                    pay_url = res.get("bot_invoice_url") or res.get("invoice_url")
                    invoice_id = res.get("invoice_id")
                    if pay_url and invoice_id is not None: return pay_url, int(invoice_id)
                logger.error(f"CryptoBot: Получен неожиданный ответ от API: {data}")
                return None
    except Exception as e:
        logger.error(f"CryptoBot: Критическая ошибка при создании счета: {e}", exc_info=True)
        return None
# ===== Конец функции create_cryptobot_api_invoice =====

# ===== СОСТОЯНИЯ ПОКУПКИ КЛЮЧА =====
# Группа состояний для процесса выбора хоста и тарифного плана при покупке нового ключа
class KeyPurchase(StatesGroup):
    waiting_for_host_selection = State()
    waiting_for_plan_selection = State()

# ===== СОСТОЯНИЯ ОНБОРДИНГА =====
# Состояние ожидания подписки на канал и принятия пользовательского соглашения
class Onboarding(StatesGroup):
    waiting_for_subscription_and_agreement = State()

# ===== СОСТОЯНИЯ ПРОЦЕССА ОПЛАТЫ =====
# Состояния для сбора email, выбора метода оплаты и применения промокода
class PaymentProcess(StatesGroup):
    waiting_for_email = State()
    waiting_for_payment_method = State()
    waiting_for_promo_code = State()

class PromoUniProcess(StatesGroup):
    waiting_for_promo_code = State()

# ===== СОСТОЯНИЯ ПОПОЛНЕНИЯ БАЛАНСА =====
# Состояния для ввода суммы и выбора метода пополнения личного баланса
class TopUpProcess(StatesGroup):
    waiting_for_amount = State()
    waiting_for_topup_method = State()

# ===== СОСТОЯНИЕ КОММЕНТАРИЯ К КЛЮЧУ =====
# Ожидание ввода пользовательского комментария для идентификации ключа в списке
class KeyCommentState(StatesGroup):
    waiting_for_comment = State()

# ===== СОСТОЯНИЯ ПОДДЕРЖКИ =====
# Диалоговые состояния для тикетов службы поддержки: тема, сообщение и ответ администратора
class SupportDialog(StatesGroup):
    waiting_for_subject = State()
    waiting_for_message = State()
    waiting_for_reply = State()

# ===== ВАЛИДАЦИЯ EMAIL =====
# Проверяет корректность введенного адреса электронной почты с помощью регулярного выражения
def is_valid_email(email: str) -> bool:
    pattern = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
    return re.match(pattern, email) is not None
# ===== Конец функции is_valid_email =====

# ===== УМНОЕ РЕДАКТИРОВАНИЕ СООБЩЕНИЯ =====
# Обновляет текст, клавиатуру и медиа-файл в сообщении, либо отправляет новое при необходимости
async def smart_edit_message(message: types.Message, text: str, reply_markup=None, photo_path: str = None):
    from aiogram.types import FSInputFile, InputMediaPhoto
    has_photo, want_photo = bool(message.photo), bool(photo_path and os.path.exists(photo_path or ""))
    
    if has_photo and want_photo:
        media = InputMediaPhoto(media=FSInputFile(photo_path), caption=text)
        try: return await message.edit_media(media=media, reply_markup=reply_markup)
        except TelegramBadRequest: return await message.answer_photo(photo=FSInputFile(photo_path), caption=text, reply_markup=reply_markup)
    elif has_photo and not want_photo:
        try: await message.delete()
        except TelegramBadRequest: pass
        return await message.answer(text, reply_markup=reply_markup)
    elif not has_photo and want_photo:
        try: await message.delete()
        except TelegramBadRequest: pass
        return await message.answer_photo(photo=FSInputFile(photo_path), caption=text, reply_markup=reply_markup)
    else:
        try: return await message.edit_text(text, reply_markup=reply_markup)
        except TelegramBadRequest: return await message.answer(text, reply_markup=reply_markup)
# ===== Конец функции smart_edit_message =====

# ===== ОТОБРАЖЕНИЕ ГЛАВНОГО МЕНЮ =====
# Отправляет или редактирует сообщение, показывая пользователю главное меню бота
async def show_main_menu(message: types.Message, edit_message: bool = False):
    user_id = message.chat.id
    user_db_data, user_keys = get_user(user_id), get_user_keys(user_id)
    trial_available, is_admin_flag = not (user_db_data and user_db_data.get('trial_used')), is_admin(user_id)
    text = get_setting("main_menu_text") or "🏠 <b>Главное меню</b>\n\nВыберите действие:"
    main_menu_image = get_setting("main_menu_image")
    photo_path = main_menu_image if (main_menu_image and os.path.exists(main_menu_image)) else None
    try: balance = get_balance(user_id)
    except Exception: balance = 0.0
    try: keyboard = keyboards.create_dynamic_main_menu_keyboard(user_keys, trial_available, is_admin_flag, balance)
    except Exception as e:
        logger.warning(f"Ошибка формирования динамического меню: {e}")
        keyboard = keyboards.create_main_menu_keyboard(user_keys, trial_available, is_admin_flag, balance)
    if edit_message: await smart_edit_message(message, text, keyboard, photo_path)
    else:
        if photo_path:
            from aiogram.types import FSInputFile
            await message.answer_photo(photo=FSInputFile(photo_path), caption=text, reply_markup=keyboard)
        else: await message.answer(text, reply_markup=keyboard)
# ===== Конец функции show_main_menu =====

# ===== ЗАВЕРШЕНИЕ ОНБОРДИНГА =====
# Фиксирует согласие пользователя с правилами и отображает главное меню
async def process_successful_onboarding(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    try: set_terms_agreed(user_id)
    except Exception as e: logger.error(f"Не удалось сохранить согласие пользователя {user_id}: {e}")
    try: await callback.answer()
    except Exception: pass
    try: await show_main_menu(callback.message, edit_message=True)
    except Exception:
        try: await callback.message.answer("✅ Условия приняты. Добро пожаловать!")
        except Exception: pass
    try: await state.clear()
    except Exception: pass
# ===== Конец функции process_successful_onboarding =====

# ===== ДЕКОРАТОР АНТИ-СПАМ =====
# Ограничивает частоту выполнения команд пользователем для предотвращения перегрузки бота
def anti_spam(f):
    @wraps(f)
    async def decorated_function(event: types.Update, *args, **kwargs):
        user_id, current_time = event.from_user.id, time.time()
        blocked_until = user_blocked_until.get(user_id)
        if blocked_until:
            if current_time < blocked_until: return
            else: del user_blocked_until[user_id]
        
        if user_id not in user_command_times: user_command_times[user_id] = deque(maxlen=5)
        times = user_command_times[user_id]
        recent_count = sum(1 for t in times if current_time - t < 1.0)
        
        if recent_count >= 3:
            if user_id in user_spam_level:
                last_spam_time, current_block_time = user_spam_level[user_id]
                block_duration = min(current_block_time * 2, 320) if current_time - last_spam_time < 60 else 10
            else: block_duration = 10
            
            user_spam_level[user_id], user_blocked_until[user_id] = (current_time, block_duration), current_time + block_duration
            user_command_times[user_id].clear()
            
            message_text = (
                "⛔️ <b>Обнаружен спам!</b>\n\n"
                "❌ <i>Пожалуйста, не отправляйте команды слишком часто.</i>\n\n"
                f"⏳ <b>Блокировка:</b> {int(block_duration)} секунд\n"
                f"💡 <i>Я смогу вам ответить через {int(block_duration)} секунд.</i>"
            )
            try:
                if isinstance(event, types.CallbackQuery): await event.answer(message_text, show_alert=True)
                else: await event.answer(message_text)
            except Exception: pass
            return
        
        times.append(current_time)
        if user_id in user_spam_level:
            last_spam_time, _ = user_spam_level[user_id]
            if current_time - last_spam_time > 60: del user_spam_level[user_id]
        
        return await f(event, *args, **kwargs)
    return decorated_function
# ===== Конец декоратора anti_spam =====

# ===== ДЕКОРАТОР ОБЯЗАТЕЛЬНОЙ РЕГИСТРАЦИИ =====
# Проверяет, зарегистрирован ли пользователь в системе, прежде чем разрешить выполнение команды
def registration_required(f):
    @wraps(f)
    async def decorated_function(event: types.Update, *args, **kwargs):
        user_id = event.from_user.id
        if get_user(user_id): return await f(event, *args, **kwargs)
        else:
            message_text = "Пожалуйста, для начала работы со мной, отправьте команду /start"
            if isinstance(event, types.CallbackQuery): await event.answer(message_text, show_alert=True)
            else: await event.answer(message_text)
    return decorated_function
# ===== Конец декоратора registration_required =====

def get_user_router() -> Router:
    user_router = Router()

    # ===== ОБРАБОТЧИК КОМАНДЫ /START =====
    # Инициализирует взаимодействие с ботом, регистрирует пользователя и обрабатывает реферальные инвайты
    @user_router.message(CommandStart())
    @anti_spam
    async def start_handler(message: types.Message, state: FSMContext, bot: Bot, command: CommandObject):
        user_id = message.from_user.id
        username = message.from_user.username or message.from_user.full_name
        referrer_id = None
        
        # Проверяем, существует ли пользователь, ДО обработки реферального кода
        user_data = get_user(user_id)

        # Обрабатываем реферальный код ТОЛЬКО если пользователя ещё нет в базе
        if not user_data and command.args and command.args.startswith('ref_'):
            try:
                potential_referrer_id = int(command.args.split('_')[1])
                if potential_referrer_id != user_id:
                    referrer_id = potential_referrer_id
                    logger.info(f"Пользователь {user_id} пришел по реферальной ссылке от {referrer_id}")
            except (IndexError, ValueError):
                logger.warning(f"Получен некорректный реферальный код: {command.args}")
                
        register_user_if_not_exists(user_id, username, referrer_id)

        if referrer_id:
            try:
                display_name = f"@{message.from_user.username}" if message.from_user.username else message.from_user.full_name
                await bot.send_message(
                    chat_id=referrer_id,
                    text=(
                        "🎉 <b>У вас новый реферал!</b>\n"
                        f"📃 user: {display_name} / id: <code>{user_id}</code>\n\n" 
                    )
                )
            except Exception as e:
                logger.error(f"Ошибка отправки уведомления реферу {referrer_id} о новом реферале {user_id}: {e}")
        
        # Если пользователя не было, обновляем user_data после регистрации
        if not user_data:
            user_data = get_user(user_id)

        if command.args and command.args.startswith('auth_'):
            try:
                from shop_bot.webapp.handlers import TEMP_AUTH_TOKENS
            except ImportError:
                TEMP_AUTH_TOKENS = {}
                
            auth_token = command.args.replace('auth_', '')
            if auth_token in TEMP_AUTH_TOKENS:
                TEMP_AUTH_TOKENS[auth_token] = user_id
                await message.answer("✅ <b>Авторизация успешна!</b>\n\nМожете вернуться в браузер — страница обновится автоматически.")
                return

        try: reward_type = (get_setting("referral_reward_type") or "percent_purchase").strip()
        except Exception: reward_type = "percent_purchase"
        
        if reward_type == "fixed_start_referrer" and referrer_id and user_data and not user_data.get('referral_start_bonus_received'):
            try:
                amount_raw = get_setting("referral_on_start_referrer_amount") or "20"
                start_bonus = Decimal(str(amount_raw)).quantize(Decimal("0.01"))
            except Exception: start_bonus = Decimal("20.00")
            
            if start_bonus > 0:
                try: ok = add_to_balance(int(referrer_id), float(start_bonus))
                except Exception as e:
                    logger.warning(f"Ошибка начисления стартового бонуса рефереру {referrer_id}: {e}")
                    ok = False

                try: add_to_referral_balance_all(int(referrer_id), float(start_bonus))
                except Exception as e: logger.warning(f"Ошибка обновления общего реф. баланса для {referrer_id}: {e}")

                try: set_referral_start_bonus_received(user_id)
                except Exception: pass

                try:
                    await bot.send_message(
                        chat_id=int(referrer_id),
                        text=(
                            "🎁 Начисление за приглашение!\n"
                            f"Новый пользователь: {message.from_user.full_name} (ID: {user_id})\n"
                            f"Бонус: {float(start_bonus):.2f} RUB"
                        )
                    )
                except Exception: pass

        if user_data and user_data.get('agreed_to_terms'):
            await message.answer(f"👋 С возвращением, {html.bold(message.from_user.full_name)}!", reply_markup=keyboards.main_reply_keyboard)
            await show_main_menu(message)
            return

        terms_url, privacy_url, channel_url = get_setting("terms_url"), get_setting("privacy_url"), get_setting("channel_url")

        if not channel_url and (not terms_url or not privacy_url):
            set_terms_agreed(user_id)
            await show_main_menu(message)
            return

        is_subscription_forced = get_setting("force_subscription") == "true"
        show_welcome_screen = (is_subscription_forced and channel_url) or (terms_url and privacy_url)

        if not show_welcome_screen:
            set_terms_agreed(user_id)
            await show_main_menu(message)
            return

        welcome_parts = ["<b>Добро пожаловать!</b>\n"]
        if is_subscription_forced and channel_url: welcome_parts.append("Для доступа к функциям, пожалуйста, подпишитесь на наш канал.")
        
        if terms_url and privacy_url:
            welcome_parts.append(
                "Также необходимо принять "
                f"<a href='{terms_url}'>Условия использования</a> и "
                f"<a href='{privacy_url}'>Политику конфиденциальности</a>."
            )
        
        welcome_parts.append("\nПосле этого нажмите кнопку ниже.")
        await message.answer(
            "\n".join(welcome_parts),
            reply_markup=keyboards.create_welcome_keyboard(channel_url=channel_url, is_subscription_forced=is_subscription_forced),
            disable_web_page_preview=True
        )
        await state.set_state(Onboarding.waiting_for_subscription_and_agreement)
    # ===== Конец функции start_handler =====

    # ===== ПРОВЕРКА ПОДПИСКИ =====
    # Проверяет, подписан ли пользователь на обязательный канал перед завершением онбординга
    @user_router.callback_query(Onboarding.waiting_for_subscription_and_agreement, F.data == "check_subscription_and_agree")
    @anti_spam
    async def check_subscription_handler(callback: types.CallbackQuery, state: FSMContext, bot: Bot):
        user_id, channel_url = callback.from_user.id, get_setting("channel_url")
        is_subscription_forced = get_setting("force_subscription") == "true"

        if not is_subscription_forced or not channel_url:
            await process_successful_onboarding(callback, state)
            return
            
        try:
            if '@' not in channel_url and 't.me/' not in channel_url:
                logger.error(f"Недопустимый формат URL канала: {channel_url}. Проверка подписки пропущена.")
                await process_successful_onboarding(callback, state)
                return

            channel_id = '@' + channel_url.split('/')[-1] if 't.me/' in channel_url else channel_url
            member = await bot.get_chat_member(chat_id=channel_id, user_id=user_id)
            
            if member.status in [ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR]: await process_successful_onboarding(callback, state)
            else: await callback.answer("❌ Вы еще не подписались на канал. Пожалуйста, подпишитесь и попробуйте снова.", show_alert=True)
        except Exception as e:
            logger.error(f"Ошибка проверки подписки (ID: {user_id}, Канал: {channel_url}): {e}")
            await callback.answer("⚠️ Не удалось проверить подписку. Убедитесь, что бот является администратором канала.", show_alert=True)
    # ===== Конец функции check_subscription_handler =====

    # ===== ЗАГЛУШКА ОНБОРДИНГА =====
    # Уведомляет пользователя о необходимости завершить регистрацию при отправке сообщений в процессе онбординга
    @user_router.message(Onboarding.waiting_for_subscription_and_agreement)
    @anti_spam
    async def onboarding_fallback_handler(message: types.Message):
        await message.answer("⚠️ Пожалуйста, выполните требуемые действия и нажмите кнопку в сообщении выше для продолжения.")
    # ===== Конец функции onboarding_fallback_handler =====

    # ===== ОБРАБОТЧИК КНОПКИ ГЛАВНОГО МЕНЮ =====
    # Отображает главное меню при нажатии на текстовую кнопку в клавиатуре
    @user_router.message(F.text == "🏠 Главное меню")
    @anti_spam
    @registration_required
    async def main_menu_handler(message: types.Message):
        await show_main_menu(message)
    # ===== Конец функции main_menu_handler =====

    # ===== ВОЗВРАТ В ГЛАВНОЕ МЕНЮ (CALLBACK) =====
    # Редактирует текущее сообщение, возвращая пользователя в корневое меню бота
    @user_router.callback_query(F.data == "back_to_main_menu")
    @anti_spam
    @registration_required
    async def back_to_main_menu_handler(callback: types.CallbackQuery):
        await callback.answer()
        await show_main_menu(callback.message, edit_message=True)
    # ===== Конец функции back_to_main_menu_handler =====

    # ===== ОТОБРАЖЕНИЕ ГЛАВНОГО МЕНЮ (CALLBACK) =====
    # Обрабатывает запрос на показ главного меню через callback-кнопку
    @user_router.callback_query(F.data == "show_main_menu")
    @anti_spam
    @registration_required
    async def show_main_menu_cb(callback: types.CallbackQuery):
        await callback.answer()
        await show_main_menu(callback.message, edit_message=True)
    # ===== Конец функции show_main_menu_cb =====

    # ===== ОТОБРАЖЕНИЕ ПРОФИЛЯ ПОЛЬЗОВАТЕЛЯ =====
    # Формирует и выводит информацию пользователя: баланс, статус VPN и реферальную статистику
    @user_router.callback_query(F.data == "show_profile")
    @anti_spam
    @registration_required
    async def profile_handler_callback(callback: types.CallbackQuery):
        await callback.answer()
        user_id = callback.from_user.id
        user_db_data, user_keys = get_user(user_id), get_user_keys(user_id)
        if not user_db_data:
            await callback.answer("⚠️ Не удалось загрузить данные профиля.", show_alert=True)
            return
            
        username = html.bold(user_db_data.get('username', 'Пользователь'))
        total_spent, total_months = user_db_data.get('total_spent', 0), user_db_data.get('total_months', 0)
        now = get_msk_time().replace(tzinfo=None)
        
        # Helper to parse date and make it naive MSK compatible
        def parse_to_naive_msk(date_str):
            try:
                dt = datetime.fromisoformat(date_str)
                if dt.tzinfo:
                    dt = dt.astimezone(get_msk_time().tzinfo).replace(tzinfo=None)
                return dt
            except:
                return datetime.min

        active_keys = [key for key in user_keys if parse_to_naive_msk(key['expiry_date']) > now]
        
        if active_keys:
            latest_key = max(active_keys, key=lambda k: parse_to_naive_msk(k['expiry_date']))
            latest_expiry_date = parse_to_naive_msk(latest_key['expiry_date'])
            time_left = latest_expiry_date - now
            vpn_remaining = get_vpn_active_text(time_left.days, time_left.seconds // 3600)
            vpn_status = "Активен"
        elif user_keys: 
            vpn_status = "Неактивен"
            vpn_remaining = "0 д. 0 ч."
        else: 
            vpn_status = "Нет ключей"
            vpn_remaining = "-"
        
        try: main_balance = get_balance(user_id)
        except Exception: main_balance = 0.0

        try: referral_count = get_referral_count(user_id)
        except Exception: referral_count = 0
        
        try: total_ref_earned = float(get_referral_balance_all(user_id))
        except Exception: total_ref_earned = 0.0

        seller_info_dict = None
        if user_db_data.get('seller_active'):
             s_info = get_seller_user(user_id)
             if s_info:
                 seller_info_dict = {
                     'sale': s_info.get('seller_sale', 0),
                     'ref': s_info.get('seller_ref', 0),
                     'squad_uuid': s_info.get('seller_uuid', '0')
                 }

        final_text = get_profile_text(
            username, user_id, total_spent, total_months, 
            vpn_status, vpn_remaining, 
            main_balance, referral_count, total_ref_earned, 
            seller_info_dict
        )
        profile_image = get_setting("profile_image")
        await smart_edit_message(callback.message, final_text, keyboards.create_dynamic_profile_keyboard(), profile_image)
    # ===== Конец функции profile_handler_callback =====

    # ===== НАЧАЛО ПОПОЛНЕНИЯ БАЛАНСА =====
    # Запрашивает у пользователя желаемую сумму для пополнения личного счета в боте
    @user_router.callback_query(F.data == "top_up_start")
    @anti_spam
    @registration_required
    async def topup_start_handler(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer()
        topup_amount_image = get_setting("topup_amount_image")
        msg = await smart_edit_message(
            callback.message,
            "💰 <b>Пополнение баланса</b>\n\nВведите сумму пополнения в рублях:\n🔹 Минимум: 10 RUB\n🔹 Максимум: 100 000 RUB",
            keyboards.create_back_to_menu_keyboard(),
            topup_amount_image
        )
        if msg: await state.update_data(topup_prompt_mid=msg.message_id)
        await state.set_state(TopUpProcess.waiting_for_amount)
    # ===== Конец функции topup_start_handler =====

    # ===== ОБРАБОТКА ВВОДА СУММЫ =====
    # Обрабатывает и валидирует сообщение с суммой пополнения от пользователя
    @user_router.message(TopUpProcess.waiting_for_amount)
    @anti_spam
    async def topup_amount_input(message: types.Message, state: FSMContext, bot: Bot):
        try: await message.delete()
        except: pass

        data = await state.get_data()
        prompt_mid, chat_id = data.get('topup_prompt_mid'), message.chat.id
        topup_amount_image = get_setting("topup_amount_image")
        
        async def edit_prompt(text: str, kb=None, image_key: str = None):
            if not prompt_mid:
                new_msg = await message.answer(text, reply_markup=kb)
                await state.update_data(topup_prompt_mid=new_msg.message_id)
                return
            target_image_path = get_setting(image_key) if image_key else None
            has_new_photo, has_old_photo = bool(target_image_path and os.path.exists(target_image_path)), bool(topup_amount_image and os.path.exists(topup_amount_image))

            try:
                if has_old_photo and has_new_photo:
                    media = InputMediaPhoto(media=FSInputFile(target_image_path), caption=text)
                    await bot.edit_message_media(chat_id=chat_id, message_id=prompt_mid, media=media, reply_markup=kb)
                elif not has_old_photo and not has_new_photo: await bot.edit_message_text(chat_id=chat_id, message_id=prompt_mid, text=text, reply_markup=kb)
                else:
                    try: await bot.delete_message(chat_id=chat_id, message_id=prompt_mid)
                    except: pass
                    if has_new_photo:
                        new_msg = await message.answer_photo(photo=FSInputFile(target_image_path), caption=text, reply_markup=kb)
                        await state.update_data(topup_prompt_mid=new_msg.message_id)
                    else:
                        new_msg = await message.answer(text, reply_markup=kb)
                        await state.update_data(topup_prompt_mid=new_msg.message_id)
            except TelegramBadRequest:
                try:
                    if has_new_photo: new_msg = await message.answer_photo(photo=FSInputFile(target_image_path), caption=text, reply_markup=kb)
                    else: new_msg = await message.answer(text, reply_markup=kb)
                    if new_msg: await state.update_data(topup_prompt_mid=new_msg.message_id)
                except: pass

        text_input = (message.text or "").replace(",", ".").strip()
        try: amount = Decimal(text_input)
        except Exception:
            await edit_prompt("❌ Пожалуйста, введите корректную сумму числом (например, 500).", keyboards.create_back_to_menu_keyboard(), "topup_amount_image")
            return
        if amount <= 0:
            await edit_prompt("❌ Сумма пополнения должна быть больше ноля.", keyboards.create_back_to_menu_keyboard(), "topup_amount_image")
            return
        if amount < Decimal("10"):
            await edit_prompt("❌ Минимальная сумма пополнения составляет 10 RUB.", keyboards.create_back_to_menu_keyboard(), "topup_amount_image")
            return
        if amount > Decimal("100000"):
            await edit_prompt("❌ Максимальная сумма пополнения составляет 100 000 RUB.", keyboards.create_back_to_menu_keyboard(), "topup_amount_image")
            return
            
        final_amount = amount.quantize(Decimal("0.01"))
        await state.update_data(topup_amount=float(final_amount))
        
        await edit_prompt(
            (
                f"✅ Сумма принята: {final_amount:.2f} RUB\n"
                "Выберите удобный способ оплаты:"
            ),
            keyboards.create_topup_payment_method_keyboard(PAYMENT_METHODS),
            "payment_method_image"
        )
        await state.set_state(TopUpProcess.waiting_for_topup_method)
    # ===== Конец функции topup_amount_input =====

    # ===== ПОПОЛНЕНИЕ ЧЕРЕЗ YOOKASSA =====
    # Создает платеж в системе YooKassa и отправляет ссылку на оплату пользователю
    @user_router.callback_query(TopUpProcess.waiting_for_topup_method, F.data == "topup_pay_yookassa")
    async def topup_pay_yookassa(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer("⏳ Создаю ссылку на оплату...")
        yookassa_shop_id, yookassa_secret_key = get_setting("yookassa_shop_id"), get_setting("yookassa_secret_key")
        
        if not yookassa_shop_id or not yookassa_secret_key:
            await callback.message.answer("⚠️ Сервис YooKassa временно не настроен. Обратитесь к поддержке.")
            await state.clear()
            return
            
        Configuration.account_id, Configuration.secret_key = yookassa_shop_id, yookassa_secret_key
        data = await state.get_data()
        amount = Decimal(str(data.get('topup_amount', 0)))
        logger.info(f"Пополнение (YooKassa): пользователь {callback.from_user.id}, сумма {amount} RUB")
        if amount <= 0:
            await smart_edit_message(callback.message, "❌ Ошибка: Некорректная сумма. Начните процесс заново.")
            await state.clear()
            return
            
        user_id = callback.from_user.id
        try:
            payment_id, metadata = await create_pending_payment(user_id=user_id, amount=float(amount), payment_method="YooKassa", action="top_up", metadata_source=data)
            price_str_for_api = f"{amount:.2f}"
            customer_email, receipt = get_setting("receipt_email"), None
            
            if customer_email and is_valid_email(customer_email):
                receipt = {
                    "customer": {"email": customer_email},
                    "items": [{
                        "description": get_transaction_comment(callback.from_user, 'topup', price_str_for_api),
                        "quantity": "1.00",
                        "amount": {"value": price_str_for_api, "currency": "RUB"},
                        "vat_code": "1",
                        "payment_subject": "service",
                        "payment_mode": "full_payment"
                    }]
                }

            description_str = get_transaction_comment(callback.from_user, 'topup', price_str_for_api)
            payment_payload = {
                "amount": {"value": price_str_for_api, "currency": "RUB"},
                "confirmation": {"type": "redirect", "return_url": f"https://t.me/{TELEGRAM_BOT_USERNAME}"},
                "capture": True,
                "description": description_str,
                "metadata": metadata
            }
            if receipt: payment_payload['receipt'] = receipt
            
            payment = Payment.create(payment_payload, payment_id)
            payment_image = get_setting("payment_image")
            await smart_edit_message(callback.message, "💳 <b>Оплата через ЮKassa</b>\nНажмите на кнопку ниже для оплаты картой или через СБП:", get_payment_keyboard("YooKassa", payment.confirmation.confirmation_url, back_callback="back_to_topup_options"), payment_image)
        except Exception as e:
            logger.error(f"YooKassa: Ошибка создания платежа для {user_id}: {e}", exc_info=True)
            await callback.message.answer("⚠️ Не удалось создать ссылку на оплату. Попробуйте другой метод.")
            await state.clear()
    # ===== Конец функции topup_pay_yookassa =====


    # ===== СОЗДАНИЕ СЧЕТА В TELEGRAM STARS (ПОКУПКА) =====
    # Формирует инвойс для оплаты выбранного тарифа через внутреннюю валюту Telegram Stars
    @user_router.callback_query(PaymentProcess.waiting_for_payment_method, F.data == "pay_stars")
    @anti_spam
    async def create_stars_invoice_handler(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer("⏳ Подготовлю счёт в Telegram Stars...")
        data = await state.get_data()
        plan = get_plan_by_id(data.get('plan_id'))
        if not plan:
            await smart_edit_message(callback.message, "❌ Ошибка: Тариф не найден в системе.")
            await state.clear()
            return

        user_id, user_data = callback.from_user.id, get_user(callback.from_user.id)
        price_rub = Decimal(str(data.get('final_price', plan['price'])))

        try:
            stars_ratio_raw = get_setting("stars_per_rub") or '0'
            stars_ratio = Decimal(stars_ratio_raw)
        except Exception: stars_ratio = Decimal('0')
        
        if stars_ratio <= 0:
            await smart_edit_message(callback.message, "⚠️ Оплата через Telegram Stars временно недоступна.")
            await state.clear()
            return

        stars_amount = max(1, int((price_rub * stars_ratio).quantize(Decimal('1'), rounding=ROUND_HALF_UP)))
        months = int(plan['months'])
        
        try:
            payment_id, _ = await create_pending_payment(user_id=user_id, amount=float(price_rub), payment_method="Telegram Stars", action=data.get('action'), metadata_source=data, plan_id=data.get('plan_id'), months=months)
            logger.info(f"Оплата (Stars): пользователь {user_id}, план {data.get('plan_id')}, сумма {price_rub} RUB")
            description_str = get_transaction_comment(callback.from_user, 'new' if data.get('action') == 'new' else 'extend', months, data.get('host_name'))
            title = f"{'Подписка' if data.get('action') == 'new' else 'Продление'} на {months} мес."
            
            await callback.message.answer_invoice(title=title, description=description_str, prices=[LabeledPrice(label=title, amount=stars_amount)], payload=payment_id, currency="XTR")
            await state.clear()
        except Exception as e:
            logger.error(f"Stars: Не удалось создать счет для {user_id}: {e}")
            await smart_edit_message(callback.message, "❌ Ошибка при создании счета Stars. Попробуйте другой метод.")
            await state.clear()
    # ===== Конец функции create_stars_invoice_handler =====

    # ===== СОЗДАНИЕ СЧЕТА В STARS (ПОПОЛНЕНИЕ) =====
    # Формирует инвойс для пополнения баланса личного кабинета через Telegram Stars
    @user_router.callback_query(TopUpProcess.waiting_for_topup_method, F.data == "topup_pay_stars")
    @anti_spam
    async def topup_stars_handler(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer("⏳ Подготовлю счёт в Telegram Stars...")
        data = await state.get_data()
        user_id, amount_rub = callback.from_user.id, Decimal(str(data.get('topup_amount', 0)))
        
        if amount_rub <= 0:
            await smart_edit_message(callback.message, "❌ Ошибка: Введена некорректная сумма.")
            await state.clear()
            return
        
        try:
            stars_ratio_raw = get_setting("stars_per_rub") or '0'
            stars_ratio = Decimal(stars_ratio_raw)
        except Exception: stars_ratio = Decimal('0')
        
        if stars_ratio <= 0:
            await smart_edit_message(callback.message, "⚠️ Оплата через Telegram Stars временно недоступна.")
            await state.clear()
            return
            
        stars_amount = max(1, int((amount_rub * stars_ratio).quantize(Decimal('1'), rounding=ROUND_HALF_UP)))
        
        try:
            payment_id, _ = await create_pending_payment(user_id=user_id, amount=float(amount_rub), payment_method="Telegram Stars", action="top_up", metadata_source=data)
            logger.info(f"Пополнение (Stars): пользователь {user_id}, сумма {amount_rub} RUB")
            description_str = get_transaction_comment(callback.from_user, 'topup', f"{amount_rub:.2f}")

            await callback.message.answer_invoice(title="Пополнение баланса", description=description_str, prices=[LabeledPrice(label="Пополнение", amount=stars_amount)], payload=payment_id, currency="XTR")
            await state.clear()
        except Exception as e:
            logger.error(f"Stars TopUp: Не удалось создать счет для {user_id}: {e}")
            await smart_edit_message(callback.message, "❌ Ошибка при создании счета в Stars.")
            await state.clear()
    # ===== Конец функции topup_stars_handler =====

    # ===== ПРОВЕРКА ПЕРЕД ОПЛАТОЙ =====
    # Подтверждает готовность системы к проведению транзакции Telegram Payments
    @user_router.pre_checkout_query()
    async def pre_checkout_handler(pre_checkout_q: PreCheckoutQuery):
        try: await pre_checkout_q.answer(ok=True)
        except Exception: pass
    # ===== Конец функции pre_checkout_handler =====

    # ===== ОБРАБОТКА УСПЕШНОЙ ОПЛАТЫ STARS =====
    # Обрабатывает уведомление об успешной транзакции Stars и активирует услугу или баланс
    @user_router.message(F.successful_payment)
    async def stars_success_handler(message: types.Message, bot: Bot):
        try: payload = message.successful_payment.invoice_payload if message.successful_payment else None
        except Exception: payload = None
        if not payload: return
        
        metadata = find_and_complete_pending_transaction(payload)
        if not metadata:
            logger.warning(f"Stars Success: Транзакция {payload} не найдена в базе.")
            try: fallback = get_latest_pending_for_user(message.from_user.id)
            except Exception as e:
                fallback = None
                logger.error(f"Stars Success: Ошибка при поиске резервных данных для {message.from_user.id}: {e}")
            
            if fallback and (fallback.get('payment_method') == 'Telegram Stars'):
                pid = fallback.get('payment_id') or payload
                logger.info(f"Stars Success: Использование резервных данных для {message.from_user.id}, pid={pid}")
                metadata = find_and_complete_pending_transaction(pid)
        
        if not metadata:
            try: total_stars = int(getattr(message.successful_payment, 'total_amount', 0) or 0)
            except Exception: total_stars = 0
            try:
                stars_ratio_raw = get_setting("stars_per_rub") or '0'
                stars_ratio = Decimal(stars_ratio_raw)
            except Exception: stars_ratio = Decimal('0')
            
            if total_stars > 0 and stars_ratio > 0:
                amount_rub = (Decimal(total_stars) / stars_ratio).quantize(Decimal('0.01'))
                metadata = {"user_id": message.from_user.id, "price": float(amount_rub), "action": "top_up", "payment_method": "Telegram Stars", "payment_id": payload}
                logger.info(f"Stars Success: Реконструкция пополнения — {amount_rub} RUB")
            else:
                logger.warning("Stars Success: Данные платежа не восстановлены, обработка прекращена.")
                return

        try:
            if message.from_user and message.from_user.username: metadata.setdefault('tg_username', message.from_user.username)
        except Exception: pass
        await process_successful_payment(bot, metadata)
    # ===== Конец функции stars_success_handler =====

    # ===== ГЕНЕРАЦИЯ ССЫЛКИ YOOMONEY =====
    # Создает URL-адрес для проведения быстрого платежа через форму YooMoney
    def _build_yoomoney_link(receiver: str, amount_rub: Decimal, label: str, description: str) -> str:
        base = "https://yoomoney.ru/quickpay/confirm.xml"
        params = {
            "receiver": (receiver or "").strip(),
            "quickpay-form": "donate",
            "targets": description[:50],
            "formcomment": description,
            "short-dest": description,
            "sum": f"{amount_rub:.2f}",
            "label": label,
            "successURL": f"https://t.me/{TELEGRAM_BOT_USERNAME}",
        }
        return base + "?" + urlencode(params)
    # ===== Конец функции _build_yoomoney_link =====

    # ===== ОПЛАТА ПОДПИСКИ ЧЕРЕЗ YOOMONEY =====
    # Формирует ссылку на оплату подписки через YooMoney и отправляет её пользователю
    @user_router.callback_query(PaymentProcess.waiting_for_payment_method, F.data == "pay_yoomoney")
    @anti_spam
    async def pay_yoomoney_handler(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer("⏳ Подготовка YooMoney...")
        data = await state.get_data()
        plan_id = data.get('plan_id')
        plan = get_plan_by_id(plan_id)
        if not plan:
            logger.error(f"YooMoney: Неверный тариф ID={plan_id}")
            await callback.message.answer("❌ Тариф не найден. Выберите другой.")
            await state.clear()
            return
            
        wallet, secret = get_setting("yoomoney_wallet"), get_setting("yoomoney_secret")
        if not wallet or not secret:
            await smart_edit_message(callback.message, "⚠️ YooMoney временно отключен.")
            await state.clear()
            return

        w = (wallet or "").strip()
        if not (w.isdigit() and len(w) >= 11):
            await smart_edit_message(callback.message, "❌ Ошибка конфигурации YooMoney. Обратитесь к администратору.")
            await state.clear()
            return
            
        user_data = get_user(callback.from_user.id)
        price_rub = Decimal(str(data.get('final_price', plan['price'])))
        logger.info(f"Оплата (YooMoney): пользователь {callback.from_user.id}, план {plan_id}, сумма {price_rub} RUB, действие {data.get('action')}")

        if price_rub < Decimal("1.00"):
            await smart_edit_message(callback.message, "❌ Минимум для YooMoney — 1 RUB. Выберите другой тариф.")
            await state.clear()
            return
            
        user_id, months = callback.from_user.id, int(plan['months'])
        payment_id, _ = await create_pending_payment(user_id=user_id, amount=float(price_rub), payment_method="YooMoney", action=data.get('action'), metadata_source=data, plan_id=plan_id, months=months)
        description_str = get_transaction_comment(callback.from_user, 'new' if data.get('action') == 'new' else 'extend', months, data.get('host_name'))
        pay_url = _build_yoomoney_link(wallet, price_rub, payment_id, description_str)
        payment_image = get_setting("payment_image")
        
        await smart_edit_message(callback.message, "💳 <b>Оплата через YooMoney</b>\nНажмите на кнопку ниже для оплаты через кошелёк:", get_payment_keyboard("YooMoney", pay_url, invoice_id=payment_id, back_callback="back_to_payment_options"), payment_image)
    # ===== Конец функции pay_yoomoney_handler =====

    # ===== ПОПОЛНЕНИЕ БАЛАНСА ЧЕРЕЗ YOOMONEY =====
    # Создает ссылку для пополнения личного счета пользователя через YooMoney
    @user_router.callback_query(TopUpProcess.waiting_for_topup_method, F.data == "topup_pay_yoomoney")
    @anti_spam
    async def topup_yoomoney_handler(callback: types.CallbackQuery, state: FSMContext):
        user_id = callback.from_user.id
        await callback.answer("⏳ Инициализация YooMoney...")
        data = await state.get_data()
        amount_rub, wallet, secret = Decimal(str(data.get('topup_amount', 0))), get_setting("yoomoney_wallet"), get_setting("yoomoney_secret")
        logger.info(f"Пополнение (YooMoney): пользователь {user_id}, сумма {amount_rub} RUB")
        
        if not wallet or not secret or amount_rub <= 0:
            logger.warning(f"YooMoney: Недостаточно настроек или неверная сумма {amount_rub}")
            await smart_edit_message(callback.message, "⚠️ YooMoney временно недоступен.")
            await state.clear()
            return
            
        w = (wallet or "").strip()
        if not (w.isdigit() and len(w) >= 11):
            logger.warning(f"YooMoney: Некорректный формат кошелька {w}")
            await smart_edit_message(callback.message, "❌ Ошибка настройки платежного адреса.")
            await state.clear()
            return
            
        if amount_rub < Decimal("1.00"):
            await smart_edit_message(callback.message, "❌ Минимум для YooMoney — 1 RUB.")
            await state.clear()
            return
        
        payment_id, _ = await create_pending_payment(user_id=user_id, amount=float(amount_rub), payment_method="YooMoney", action="top_up", metadata_source=data)
        description_str = get_transaction_comment(callback.from_user, 'topup', f"{amount_rub:.2f}")
        pay_url = _build_yoomoney_link(wallet, amount_rub, payment_id, description_str)
        payment_image = get_setting("payment_image")
        
        await smart_edit_message(callback.message, "💳 <b>Оплата через YooMoney</b>\nНажмите на кнопку ниже для оплаты через кошелёк:", get_payment_keyboard("YooMoney", pay_url, invoice_id=payment_id, back_callback="back_to_topup_options"), payment_image)
    # ===== Конец функции topup_yoomoney_handler =====

    # ===== РУЧНАЯ ПРОВЕРКА ПЛАТЕЖА =====
    # Позволяет пользователю вручную запросить проверку статуса транзакции через API
    @user_router.callback_query(F.data.startswith("check_pending:"))
    @anti_spam
    async def check_pending_payment_handler(callback: types.CallbackQuery, bot: Bot):
        try: pid = callback.data.split(":", 1)[1]
        except Exception:
            await callback.answer("❌ Некорректный ID платежа.", show_alert=True)
            return
        
        logger.info(f"YooMoney Check: Проверка {pid}")
        try: status = get_pending_status(pid) or ""
        except Exception as e:
            logger.error(f"YooMoney: Ошибка проверки статуса {pid}: {e}")
            status = ""
            
        if status.lower() == 'paid':
            await callback.answer("✅ Платеж уже обработан! Баланс обновлен.", show_alert=True)
            return

        token = (get_setting('yoomoney_api_token') or '').strip()
        if not token:
            if not status: await callback.answer("❌ Платеж не найден.", show_alert=True)
            else: await callback.answer("⏳ Оплата еще не зачислена. Проверьте позже.", show_alert=True)
            return

        try:
            async with aiohttp.ClientSession() as session:
                data, headers = {"label": pid, "records": "10"}, {"Authorization": f"Bearer {token}", "Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"}
                async with session.post("https://yoomoney.ru/api/operation-history", data=data, headers=headers, timeout=15) as resp:
                    if resp.status != 200:
                        await callback.answer("⚠️ Ошибка связи с API YooMoney.", show_alert=True)
                        return
                    text = await resp.text()
        except Exception as e:
            logger.error(f"YooMoney: Ошибка API для {pid}: {e}")
            await callback.answer("⚠️ Ошибка сетевого соединения с платежным сервисом.", show_alert=True)
            return
            
        try: payload = json.loads(text)
        except Exception: payload = {}
        
        ops, paid = payload.get('operations') or [], False
        for op in ops:
            if str(op.get('label')) == pid and str(op.get('status','')).lower() in {"success","done"}:
                paid = True
                break
                
        if paid:
            logger.info(f"YooMoney Check: Платеж {pid} подтвержден.")
            metadata = find_and_complete_pending_transaction(pid)
            if metadata: await process_successful_payment(bot, metadata)
            await callback.answer("✅ Оплата прошла успешно! Ваш профиль активирован.", show_alert=True)
            return

        await callback.answer("⏳ Оплата пока не поступила. Попробуйте обновить через минуту.", show_alert=True)
    # ===== Конец функции check_pending_payment_handler =====

    # ===== ОПЛАТА ПОДПИСКИ ЧЕРЕЗ HELEKET =====
    # Генерирует счет на оплату подписки через платежную систему Heleket
    @user_router.callback_query(PaymentProcess.waiting_for_payment_method, F.data == "pay_heleket")
    @anti_spam
    async def pay_heleket_handler(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer("⏳ Создание счета Heleket...")
        data = await state.get_data()
        plan_id = data.get('plan_id')
        if not plan_id:
            logger.error(f"Heleket: Отсутствует plan_id для {callback.from_user.id}")
            await smart_edit_message(callback.message, "❌ Ошибка: Тариф не определен.")
            await state.clear()
            return

        plan = get_plan_by_id(plan_id)
        if not plan:
            logger.error(f"Heleket: Тариф ID {plan_id} не найден.")
            await smart_edit_message(callback.message, "❌ Тариф не найден в базе.")
            await state.clear()
            return
            
        user_id, user_data = callback.from_user.id, get_user(callback.from_user.id)
        price_rub, months = Decimal(str(data.get('final_price', plan['price']))), int(plan['months'])
        logger.info(f"Оплата (Heleket): пользователь {user_id}, план {plan_id}, сумма {price_rub} RUB, действие {data.get('action')}")
        
        try:
            payment_id, metadata = await create_pending_payment(user_id=user_id, amount=float(price_rub), payment_method="Heleket", action=data.get('action'), metadata_source=data, plan_id=plan_id, months=months)
            pay_url = await create_heleket_payment_request(payment_id=payment_id, price=float(price_rub), metadata=metadata)

            if pay_url:
                payment_image = get_setting("payment_image")
                await smart_edit_message(callback.message, "💎 <b>Оплата через Heleket</b>\nНажмите на кнопку ниже для оплаты криптовалютой:", get_payment_keyboard("Heleket", pay_url), payment_image)
                await state.clear()
            else:
                await smart_edit_message(callback.message, "❌ Ошибка сервиса Heleket. Попробуйте другой способ оплаты.")
        except Exception as e:
            logger.error(f"Heleket Ошибка: {e}", exc_info=True)
            await smart_edit_message(callback.message, "⚠️ Произошла внутренняя ошибка при создании платежа.")
            await state.clear()
    # ===== Конец функции pay_heleket_handler =====

    # ===== ОПЛАТА ПОДПИСКИ ЧЕРЕЗ PLATEGA =====
    # Создает ссылку на оплату через систему СБП (система быстрых платежей) Platega
    @user_router.callback_query(PaymentProcess.waiting_for_payment_method, F.data == "pay_platega")
    @anti_spam
    async def pay_platega_handler(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer("⏳ Генерация ссылки СБП...")
        data = await state.get_data()
        plan_id = data.get('plan_id')
        
        if not plan_id:
            logger.error(f"Platega: Отсутствует plan_id для {callback.from_user.id}")
            await smart_edit_message(callback.message, "❌ Ошибка: Тариф не выбран.")
            await state.clear()
            return
        
        plan = get_plan_by_id(plan_id)
        if not plan:
            logger.error(f"Platega: Тариф {plan_id} не найден.")
            await smart_edit_message(callback.message, "❌ Выбранный тариф недоступен.")
            await state.clear()
            return
        
        merchant_id, api_key = get_setting("platega_merchant_id"), get_setting("platega_api_key")
        if not merchant_id or not api_key:
            await smart_edit_message(callback.message, "⚠️ Сервис Platega временно отключен.")
            await state.clear()
            return
            
        user_id, user_data = callback.from_user.id, get_user(callback.from_user.id)
        price_rub, months = Decimal(str(data.get('final_price', plan['price']))), int(plan['months'])
        logger.info(f"Оплата (Platega): пользователь {user_id}, план {plan_id}, сумма {price_rub} RUB, действие {data.get('action')}")
        
        try:
            payment_id, metadata = await create_pending_payment(user_id=user_id, amount=float(price_rub), payment_method="Platega", action=data.get('action'), metadata_source=data, plan_id=plan_id, months=months)
            platega = PlategaAPI(merchant_id, api_key)
            description_str = get_transaction_comment(callback.from_user, 'new' if data.get('action') == 'new' else 'extend', months, data.get('host_name'))

            _, payment_url = await platega.create_payment(amount=float(price_rub), description=description_str, payment_id=payment_id, return_url=f"https://t.me/{TELEGRAM_BOT_USERNAME}", failed_url=f"https://t.me/{TELEGRAM_BOT_USERNAME}", payment_method=2)
            
            if payment_url:
                payment_image = get_setting("payment_image")
                await smart_edit_message(callback.message, "💳 <b>Оплата через Platega</b>\nНажмите на кнопку ниже для оплаты через СБП:", get_payment_keyboard("Platega", payment_url, back_callback="back_to_payment_options"), payment_image)
            else:
                await smart_edit_message(callback.message, "❌ Не удалось создать ссылку СБП. Выберите другой метод.")
                await state.clear()
        except Exception as e:
            logger.error(f"Platega Ошибка: {e}", exc_info=True)
            await smart_edit_message(callback.message, "⚠️ Внутренняя ошибка при создании платежа Platega.")
            await state.clear()
    # ===== Конец функции pay_platega_handler =====

    # ===== ОПЛАТА ПОДПИСКИ ЧЕРЕЗ PLATEGA (КРИПТА) =====
    @user_router.callback_query(PaymentProcess.waiting_for_payment_method, F.data == "pay_platega_crypto")
    @anti_spam
    async def pay_platega_crypto_handler(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer("⏳ Генерация ссылки на крипту...")
        data = await state.get_data()
        plan_id = data.get('plan_id')
        
        if not plan_id:
            logger.error(f"Platega Crypto: Отсутствует plan_id для {callback.fromuser.id}")
            await smart_edit_message(callback.message, "❌ Ошибка: Тариф не выбран.")
            await state.clear()
            return
        
        plan = get_plan_by_id(plan_id)
        if not plan:
            logger.error(f"Platega Crypto: Тариф {plan_id} не найден.")
            await smart_edit_message(callback.message, "❌ Выбранный тариф недоступен.")
            await state.clear()
            return
        
        merchant_id, api_key = get_setting("platega_merchant_id"), get_setting("platega_api_key")
        if not merchant_id or not api_key:
            await smart_edit_message(callback.message, "⚠️ Сервис Platega временно отключен.")
            await state.clear()
            return
            
        user_id, user_data = callback.from_user.id, get_user(callback.from_user.id)
        price_rub, months = Decimal(str(data.get('final_price', plan['price']))), int(plan['months'])
        logger.info(f"Оплата (Platega Crypto): пользователь {user_id}, план {plan_id}, сумма {price_rub} RUB, действие {data.get('action')}")
        
        try:
            payment_id, metadata = await create_pending_payment(user_id=user_id, amount=float(price_rub), payment_method="Platega Crypto", action=data.get('action'), metadata_source=data, plan_id=plan_id, months=months)
            platega = PlategaAPI(merchant_id, api_key)
            description_str = get_transaction_comment(callback.from_user, 'new' if data.get('action') == 'new' else 'extend', months, data.get('host_name'))

            _, payment_url = await platega.create_payment(amount=float(price_rub), description=description_str, payment_id=payment_id, return_url=f"https://t.me/{TELEGRAM_BOT_USERNAME}", failed_url=f"https://t.me/{TELEGRAM_BOT_USERNAME}", payment_method=13)
            
            if payment_url:
                payment_image = get_setting("payment_image")
                await smart_edit_message(callback.message, "🪙 <b>Оплата через Platega Crypto</b>\nНажмите на кнопку ниже для оплаты криптовалютой:", get_payment_keyboard("Platega", payment_url, back_callback="back_to_payment_options"), payment_image)
            else:
                await smart_edit_message(callback.message, "❌ Не удалось создать ссылку на крипту. Выберите другой метод.")
                await state.clear()
        except Exception as e:
            logger.error(f"Platega Crypto Ошибка: {e}", exc_info=True)
            await smart_edit_message(callback.message, "⚠️ Внутренняя ошибка при создании платежа Platega Crypto.")
            await state.clear()
    # ===== Конец функции pay_platega_crypto_handler =====

    # ===== ПОПОЛНЕНИЕ БАЛАНСА ЧЕРЕЗ PLATEGA =====
    # Формирует запрос на пополнение счета через систему Platega (СБП)
    @user_router.callback_query(TopUpProcess.waiting_for_topup_method, F.data == "topup_pay_platega")
    @anti_spam
    async def topup_platega_handler(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer("⏳ Генерация ссылки СБП...")
        data = await state.get_data()
        user_id, amount_rub = callback.from_user.id, Decimal(str(data.get('topup_amount', 0)))
        merchant_id, api_key = get_setting("platega_merchant_id"), get_setting("platega_api_key")
        
        if not merchant_id or not api_key or amount_rub <= 0:
            await smart_edit_message(callback.message, "⚠️ Оплата через Platega временно недоступна.")
            await state.clear()
            return
        
        try:
            payment_id, metadata = await create_pending_payment(user_id=user_id, amount=float(amount_rub), payment_method="Platega", action="top_up", metadata_source=data)
            logger.info(f"Пополнение (Platega): пользователь {user_id}, сумма {amount_rub} RUB")
            platega = PlategaAPI(merchant_id, api_key)
            description_str = get_transaction_comment(callback.from_user, 'topup', f"{amount_rub:.2f}")

            _, payment_url = await platega.create_payment(amount=float(amount_rub), description=description_str, payment_id=payment_id, return_url=f"https://t.me/{TELEGRAM_BOT_USERNAME}", failed_url=f"https://t.me/{TELEGRAM_BOT_USERNAME}", payment_method=2)
            
            if payment_url:
                payment_image = get_setting("payment_image")
                await smart_edit_message(callback.message, "💳 <b>Оплата через Platega</b>\nНажмите на кнопку ниже для пополнения через СБП:", get_payment_keyboard("Platega", payment_url, back_callback="back_to_topup_options"), payment_image)
            else:
                await smart_edit_message(callback.message, "❌ Ошибка создания ссылки СБП. Попробуйте позже.")
                await state.clear()
        except Exception as e:
            logger.error(f"Platega Пополнение Ошибка: {e}", exc_info=True)
            await smart_edit_message(callback.message, "⚠️ Произошла ошибка при инициализации платежа.")
            await state.clear()
    # ===== Конец функции topup_platega_handler =====

    # ===== ПОПОЛНЕНИЕ БАЛАНСА ЧЕРЕЗ PLATEGA (КРИПТА) =====
    @user_router.callback_query(TopUpProcess.waiting_for_topup_method, F.data == "topup_pay_platega_crypto")
    @anti_spam
    async def topup_platega_crypto_handler(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer("⏳ Генерация ссылки на крипту...")
        data = await state.get_data()
        user_id, amount_rub = callback.from_user.id, Decimal(str(data.get('topup_amount', 0)))
        merchant_id, api_key = get_setting("platega_merchant_id"), get_setting("platega_api_key")
        
        if not merchant_id or not api_key or amount_rub <= 0:
            await smart_edit_message(callback.message, "⚠️ Оплата через Platega временно недоступна.")
            await state.clear()
            return
        
        try:
            payment_id, metadata = await create_pending_payment(user_id=user_id, amount=float(amount_rub), payment_method="Platega Crypto", action="top_up", metadata_source=data)
            logger.info(f"Пополнение (Platega Crypto): пользователь {user_id}, сумма {amount_rub} RUB")
            platega = PlategaAPI(merchant_id, api_key)
            description_str = get_transaction_comment(callback.from_user, 'topup', f"{amount_rub:.2f}")

            _, payment_url = await platega.create_payment(amount=float(amount_rub), description=description_str, payment_id=payment_id, return_url=f"https://t.me/{TELEGRAM_BOT_USERNAME}", failed_url=f"https://t.me/{TELEGRAM_BOT_USERNAME}", payment_method=11)
            
            if payment_url:
                payment_image = get_setting("payment_image")
                await smart_edit_message(callback.message, "🪙 <b>Оплата через Platega Crypto</b>\nНажмите на кнопку ниже для пополнения криптовалютой:", get_payment_keyboard("Platega", payment_url, back_callback="back_to_topup_options"), payment_image)
            else:
                await smart_edit_message(callback.message, "❌ Ошибка создания ссылки на крипту. Попробуйте позже.")
                await state.clear()
        except Exception as e:
            logger.error(f"Platega Crypto Пополнение Ошибка: {e}", exc_info=True)
            await smart_edit_message(callback.message, "⚠️ Произошла ошибка при инициализации платежа.")
            await state.clear()
    # ===== Конец функции topup_platega_crypto_handler =====

    # ===== ПОПОЛНЕНИЕ БАЛАНСА ЧЕРЕЗ HELEKET =====
    # Создает транзакцию пополнения счета через платежный сервис Heleket
    @user_router.callback_query(TopUpProcess.waiting_for_topup_method, F.data == "topup_pay_heleket")
    @anti_spam
    async def topup_pay_heleket_like(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer("⏳ Создание счета...")
        data = await state.get_data()
        user_id, amount = callback.from_user.id, float(data.get('topup_amount', 0))
        if amount <= 0:
            await smart_edit_message(callback.message, "❌ Ошибка: Сумма пополнения некорректна.")
            await state.clear()
            return

        try:
            payment_id, metadata = await create_pending_payment(user_id=user_id, amount=float(amount), payment_method="Heleket", action="top_up", metadata_source=data)
            pay_url = await create_heleket_payment_request(payment_id=payment_id, price=float(amount), metadata=metadata)
            
            if pay_url:
                payment_image = get_setting("payment_image")
                await smart_edit_message(callback.message, "💎 <b>Оплата через Heleket</b>\nНажмите на кнопку ниже для оплаты криптовалютой:", get_payment_keyboard("Heleket", pay_url, back_callback="back_to_topup_options"), payment_image)
            else:
                await smart_edit_message(callback.message, "❌ Ошибка системы Heleket. Попробуйте другой способ.")
        except Exception as e:
            logger.error(f"Heleket Пополнение Ошибка: {e}", exc_info=True)
            await smart_edit_message(callback.message, "⚠️ Не удалось создать счет на пополнение.")
            await state.clear()
    # ===== Конец функции topup_pay_heleket_like =====

    # ===== ПОПОЛНЕНИЕ БАЛАНСА ЧЕРЕЗ CRYPTOBOT =====
    # Генерирует инвойс для пополнения счета в криптовалюте через CryptoBot
    @user_router.callback_query(TopUpProcess.waiting_for_topup_method, F.data == "topup_pay_cryptobot")
    async def topup_pay_cryptobot(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer("⏳ Создание счета в Crypto Pay...")
        data = await state.get_data()
        user_id, amount = callback.from_user.id, float(data.get('topup_amount', 0))
        if amount <= 0:
            await smart_edit_message(callback.message, "❌ Ошибка: Введена неверная сумма пополнения.")
            await state.clear()
            return
        
        try:
            payment_id, metadata = await create_pending_payment(user_id=user_id, amount=amount, payment_method="CryptoBot", action="top_up", metadata_source=data)
            logger.info(f"Пополнение (CryptoBot): пользователь {user_id}, сумма {amount} RUB")
            price_str = f"{Decimal(str(amount)).quantize(Decimal('0.01'))}"
            payload_str = ":".join([str(int(user_id)), "0", price_str, "top_up", "None", "", "None", "None", "CryptoBot", "None", "0"])

            result = await create_cryptobot_api_invoice(amount=amount, payload_str=payload_str)
            if result:
                pay_url, invoice_id = result
                payment_image = get_setting("payment_image")
                await smart_edit_message(callback.message, "💎 <b>Оплата через CryptoBot</b>\nНажмите на кнопку ниже для оплаты криптовалютой:", keyboards.create_cryptobot_payment_keyboard(pay_url, invoice_id, back_callback="back_to_topup_options"), payment_image)
            else:
                await smart_edit_message(callback.message, "❌ Не удалось создать счет в CryptoBot. Попробуйте другой метод.")
        except Exception as e:
            logger.error(f"CryptoBot Пополнение Ошибка: {e}", exc_info=True)
            await smart_edit_message(callback.message, "⚠️ Ошибка при создании криптовалютного счета.")
            await state.clear()
    # ===== Конец функции topup_pay_cryptobot =====

    # ===== ПОПОЛНЕНИЕ БАЛАНСА ЧЕРЕЗ TON CONNECT =====
    # Инициирует процесс оплаты в сети TON через TON Connect с генерацией QR-кода
    @user_router.callback_query(TopUpProcess.waiting_for_topup_method, F.data == "topup_pay_tonconnect")
    @anti_spam
    async def topup_pay_tonconnect(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer("⏳ Подготовка TON Connect...")
        data = await state.get_data()
        user_id, amount_rub = callback.from_user.id, Decimal(str(data.get('topup_amount', 0)))
        if amount_rub <= 0:
            await smart_edit_message(callback.message, "❌ Некорректная сумма пополнения.")
            await state.clear()
            return

        wallet_address = get_setting("ton_wallet_address")
        if not wallet_address:
            await smart_edit_message(callback.message, "⚠️ Оплата через TON Connect временно недоступна.")
            await state.clear()
            return

        usdt_rub_rate, ton_usdt_rate = await get_usdt_rub_rate(), await get_ton_usdt_rate()
        if not usdt_rub_rate or not ton_usdt_rate:
            await smart_edit_message(callback.message, "❌ Не удалось получить актуальный курс TON. Попробуйте позже.")
            await state.clear()
            return

        price_ton = (amount_rub / usdt_rub_rate / ton_usdt_rate).quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)
        amount_nanoton = int(price_ton * 1_000_000_000)
        
        # Use MSK timestamp
        valid_until = int(get_msk_time().timestamp()) + 600

        payment_id, _ = await create_pending_payment(user_id=user_id, amount=float(amount_rub), payment_method="TON Connect", action="top_up", metadata_source=data)
        transaction_payload = {'messages': [{'address': wallet_address, 'amount': str(amount_nanoton), 'payload': payment_id}], 'valid_until': valid_until}

        try:
            connect_url = await _start_ton_connect_process(user_id, transaction_payload)
            qr_img = qrcode.make(connect_url)
            bio = BytesIO(); qr_img.save(bio, "PNG"); qr_file = BufferedInputFile(bio.getvalue(), "ton_qr.png")
            try: await callback.message.delete()
            except: pass
            await callback.message.answer_photo(photo=qr_file, caption=(f"💎 <b>Оплата через TON Connect</b>\n\nСумма: `{price_ton}` TON\n\nНажмите кнопку ниже для подтверждения перевода."), reply_markup=keyboards.create_ton_connect_keyboard(connect_url, back_callback="back_to_topup_options"))
        except Exception as e:
            logger.error(f"Ошибка TON Connect при пополнении ({user_id}): {e}", exc_info=True)
            await smart_edit_message(callback.message, "❌ Не удалось инициализировать TON Connect.")
            await state.clear()
    # ===== Конец функции topup_pay_tonconnect =====

    # ===== ОТОБРАЖЕНИЕ РЕФЕРАЛЬНОЙ ПРОГРАММЫ =====
    # Выводит информацию о партнерской программе: ссылку, количество приглашенных и баланс
    @user_router.callback_query(F.data == "show_referral_program")
    @anti_spam
    @registration_required
    async def referral_program_handler(callback: types.CallbackQuery):
        await callback.answer()
        user_id = callback.from_user.id
        if not user_id: return
        
        try: count, balance_total = get_referral_count(user_id), float(get_referral_balance_all(user_id))
        except Exception: count, balance_total = 0, 0.0
            
        reward_type = (get_setting("referral_reward_type") or "percent_purchase").strip()
        if reward_type == "percent_purchase":
            percent = get_setting("referral_percentage") or "10"
            reward_desc = f"Вы получаете <b>{percent}%</b> от каждой покупки ваших друзей."
        elif reward_type == "fixed_purchase":
            amount = get_setting("fixed_referral_bonus_amount") or "50"
            reward_desc = f"Вы получаете <b>{amount}</b> RUB с каждой покупки ваших друзей."
        elif reward_type == "fixed_start_referrer":
            amount = get_setting("referral_on_start_referrer_amount") or "20"
            reward_desc = f"Вы получаете <b>{amount}</b> RUB за каждого приглашенного друга."
        else: reward_desc = "Условия программы уточняйте у администрации."
            
        bot_username = (await callback.bot.get_me()).username
        referral_link = f"https://t.me/{bot_username}?start=ref_{user_id}"
        
        referral_discount = get_setting("referral_discount") or "0"
        
        final_text = (
            "🌟 <b>Реферальная программа</b>\n\n"
            "Приглашайте друзей и получайте бонусы! 💸\n\n"
            f"💎 <b>Ваша награда:</b>\n"
            f"• {reward_desc}\n\n"
            f"🎁 <b>Бонус другу:</b>\n"
            f"• Скидка <b>{referral_discount}%</b> на первую покупку\n\n"
            f"📊 <b>Статистика:</b>\n"
            f"👤 Приглашено: <b>{count}</b>\n"
            f"💰 Заработано: <b>{balance_total:.2f} RUB</b>\n\n"
            f"🔗 <b>Реферальная ссылка:</b>\n<code>{referral_link}</code>"
        )
        referral_image = get_setting("referral_image")
        await smart_edit_message(callback.message, final_text, keyboards.create_referral_keyboard(referral_link), referral_image)
    # ===== Конец функции referral_program_handler =====


    # ===== ОТОБРАЖЕНИЕ РАЗДЕЛА "О ПРОЕКТЕ" =====
    # Выводит информационный текст о проекте, ссылки на условия использования и социальные сети
    @user_router.callback_query(F.data == "show_about")
    @registration_required
    async def about_handler(callback: types.CallbackQuery):
        await callback.answer()
        about_text = get_setting("about_text") or "ℹ️ Информация о проекте еще не заполнена в настройках."
        terms_url, privacy_url, channel_url = get_setting("terms_url"), get_setting("privacy_url"), get_setting("channel_url")
        about_image = get_setting("about_image")
        await smart_edit_message(callback.message, about_text, keyboards.create_about_keyboard(channel_url, terms_url, privacy_url), about_image)
    # ===== Конец функции about_handler =====

    # ===== ОТОБРАЖЕНИЕ РЕЗУЛЬТАТОВ SPEEDTEST =====
    # Выводит последние данные о скорости и пинге со всех доступных серверов SSH
    @user_router.callback_query(F.data == "user_speedtest_last")
    @registration_required
    async def user_speedtest_last_handler(callback: types.CallbackQuery):
        await callback.answer()
        try: targets = rw_repo.get_all_ssh_targets() or []
        except Exception: targets = []
        
        lines = []
        for t in targets:
            name = (t.get('target_name') or '').strip()
            if not name: continue
            try: last = rw_repo.get_latest_speedtest(name)
            except Exception: last = None
            
            if not last:
                lines.append(f"• <b>{name}</b>: 🚫 Нет данных")
                continue
            
            ping, down, up = last.get('ping_ms'), last.get('download_mbps'), last.get('upload_mbps')
            badge = '✅' if last.get('ok') else '❌'
            ping_s = f"{float(ping):.1f}" if isinstance(ping, (int, float)) else '—'
            down_s = f"{float(down):.0f}" if isinstance(down, (int, float)) else '—'
            up_s = f"{float(up):.0f}" if isinstance(up, (int, float)) else '—'
            
            ts_s = ""
            if last.get('created_at'):
                try:
                    ts_dt = datetime.fromisoformat(str(last['created_at']).replace('Z', '+00:00'))
                    if ts_dt.tzinfo:
                         ts_dt = ts_dt.astimezone(get_msk_time().tzinfo)
                    ts_s = ts_dt.strftime('%d.%m %H:%M')
                except: ts_s = str(last['created_at'])
            
            lines.append(f"• <b>{name}</b> — {badge} ⏱{ping_s}ms | ↓{down_s} | ↑{up_s} | 🕒{ts_s}")

        text = "⚡ <b>Актуальные показатели серверов:</b>\n\n" + ("\n\n".join(lines) if lines else "⚠️ Серверы для проверки не настроены.")
        speedtest_image = get_setting("speedtest_image")
        await smart_edit_message(callback.message, text, keyboards.create_back_to_menu_keyboard(), speedtest_image)
    # ===== Конец функции user_speedtest_last_handler =====

    # ===== УНИВЕРСАЛЬНЫЙ ПОМОЩНИК ПОДДЕРЖКИ =====
    # Отображает меню контактов поддержки на основе текущих настроек (бот или прямой контакт)
    async def _show_support_selection(message: types.Message):
        support_bot, support_user = get_setting("support_bot_username"), get_setting("support_user")
        support_text = get_setting("support_text") or "🆘 <b>Служба поддержки</b>\n\nВозникли вопросы или трудности? Наши специалисты всегда готовы помочь вам!"
        support_image = get_setting("support_image")
        
        if support_bot: kb = keyboards.create_support_bot_link_keyboard(support_bot)
        elif support_user: kb = keyboards.create_support_keyboard(support_user)
        else:
            await smart_edit_message(message, "⚠️ Контакты службы поддержки временно не настроены.", keyboards.create_back_to_menu_keyboard())
            return
            
        await smart_edit_message(message, support_text, kb, support_image)

    @user_router.callback_query(F.data == "show_help")
    @anti_spam
    @registration_required
    async def help_handler(callback: types.CallbackQuery):
        await callback.answer()
        await _show_support_selection(callback.message)

    @user_router.callback_query(F.data == "support_menu")
    @anti_spam
    @registration_required
    async def support_menu_handler(callback: types.CallbackQuery):
        await callback.answer()
        await _show_support_selection(callback.message)
    # ===== Конец функций поддержки =====

    @user_router.callback_query(F.data == "support_external")
    @anti_spam
    @registration_required
    async def support_external_handler(callback: types.CallbackQuery):
        await callback.answer()
        support_bot_username = get_setting("support_bot_username")
        if support_bot_username:
            await smart_edit_message(
                callback.message,
                get_setting("support_text") or "Раздел поддержки.",
                keyboards.create_support_bot_link_keyboard(support_bot_username)
            )
            return
        support_user = get_setting("support_user")
        if not support_user:
            await smart_edit_message(callback.message, "Внешний контакт поддержки не настроен.", keyboards.create_back_to_menu_keyboard())
            return
        await smart_edit_message(
            callback.message,
            "Для связи с поддержкой используйте кнопку ниже.",
            keyboards.create_support_keyboard(support_user)
        )

    @user_router.callback_query(F.data == "support_new_ticket")
    @anti_spam
    @registration_required
    async def support_new_ticket_handler(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer()
        support_bot_username = get_setting("support_bot_username")
        if support_bot_username:
            await smart_edit_message(
                callback.message,
                "Раздел поддержки вынесен в отдельного бота.",
                keyboards.create_support_bot_link_keyboard(support_bot_username)
            )
        else:
            await smart_edit_message(callback.message, "Контакты поддержки не настроены.", keyboards.create_back_to_menu_keyboard())

    # ===== УНИВЕРСАЛЬНОЕ УВЕДОМЛЕНИЕ О ВНЕШНЕЙ ПОДДЕРЖКЕ =====
    # Отправляет пользователю информацию о том, что поддержка осуществляется через внешнего бота
    async def _notify_external_support(event: types.Message | types.CallbackQuery):
        support_bot = get_setting("support_bot_username")
        text = "📢 <b>Центр поддержки</b>\n\nСоздание тикетов и общение с операторами теперь доступно в нашем специальном боте поддержки."
        kb = keyboards.create_support_bot_link_keyboard(support_bot) if support_bot else keyboards.create_back_to_menu_keyboard()
        
        if isinstance(event, types.CallbackQuery): await smart_edit_message(event.message, text if support_bot else "⚠️ Служба поддержки временно недоступна.", kb)
        else: await event.answer(text if support_bot else "⚠️ Служба поддержки временно недоступна.", reply_markup=kb)

    @user_router.message(SupportDialog.waiting_for_subject)
    @anti_spam
    @registration_required
    async def support_subject_received(message: types.Message, state: FSMContext):
        await state.clear()
        await _notify_external_support(message)

    @user_router.message(SupportDialog.waiting_for_message)
    @registration_required
    async def support_message_received(message: types.Message, state: FSMContext, bot: Bot):
        await state.clear()
        await _notify_external_support(message)

    @user_router.callback_query(F.data == "support_my_tickets")
    @registration_required
    async def support_my_tickets_handler(callback: types.CallbackQuery):
        await callback.answer()
        await _notify_external_support(callback)

    @user_router.callback_query(F.data.startswith("support_view_"))
    @registration_required
    async def support_view_ticket_handler(callback: types.CallbackQuery):
        await callback.answer()
        await _notify_external_support(callback)

    @user_router.callback_query(F.data.startswith("support_reply_"))
    @registration_required
    async def support_reply_prompt_handler(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer(); await state.clear()
        await _notify_external_support(callback)

    @user_router.message(SupportDialog.waiting_for_reply)
    @registration_required
    async def support_reply_received(message: types.Message, state: FSMContext, bot: Bot):
        await state.clear()
        await _notify_external_support(message)

    # ===== РЕЛЕЙ СООБЩЕНИЙ ИЗ ФОРУМА (АДМИН) =====
    # Пересылает сообщения из топиков форума пользователю для обеспечения обратной связи через бота
    @user_router.message(F.is_topic_message == True)
    async def forum_thread_message_handler(message: types.Message, bot: Bot):
        try:
            support_bot, me = get_setting("support_bot_username"), await bot.get_me()
            if support_bot and (me.username or "").lower() != support_bot.lower(): return
            if not message.message_thread_id: return
            
            ticket = get_ticket_by_thread(str(message.chat.id), int(message.message_thread_id))
            if not ticket: return
            
            user_id = int(ticket.get('user_id'))
            if message.from_user and message.from_user.id == me.id: return

            is_adm_set = is_admin(message.from_user.id)
            is_adm_chat = False
            try:
                member = await bot.get_chat_member(chat_id=message.chat.id, user_id=message.from_user.id)
                is_adm_chat = member.status in [ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR]
            except Exception: pass
            
            if not (is_adm_set or is_adm_chat): return
            
            content = (message.text or message.caption or "").strip()
            if content: add_support_message(ticket_id=int(ticket['ticket_id']), sender='admin', content=content)
            
            header = await bot.send_message(chat_id=user_id, text=f"💬 <b>Ответ поддержки по тикету #{ticket['ticket_id']}</b>")
            try: await bot.copy_message(chat_id=user_id, from_chat_id=message.chat.id, message_id=message.message_id, reply_to_message_id=header.message_id)
            except Exception:
                if content: await bot.send_message(chat_id=user_id, text=content)
        except Exception as e: logger.warning(f"Служба поддержки: Ошибка пересылки сообщения: {e}")

    @user_router.callback_query(F.data.startswith("support_close_"))
    @anti_spam
    @registration_required
    async def support_close_ticket_handler(callback: types.CallbackQuery):
        await callback.answer()
        await _notify_external_support(callback)
    # ===== Конец функций внешней поддержки =====

    # ===== УПРАВЛЕНИЕ КЛЮЧАМИ ПОЛЬЗОВАТЕЛЯ =====
    # Отображает список существующих VPN-ключей пользователя и предоставляет инструменты управления
    @user_router.callback_query(F.data == "manage_keys")
    @anti_spam
    @registration_required
    async def manage_keys_handler(callback: types.CallbackQuery):
        await callback.answer()
        user_id = callback.from_user.id
        user_keys = get_user_keys(user_id)
        keys_list_image = get_setting("keys_list_image")
        text = "🔑 <b>Ваши ключи доступа</b>\n\nНиже представлен список ваших активных и истекших ключей:" if user_keys else "🏷 <b>У вас пока нет активных ключей.</b>\nПриобретите подписку в главном меню, чтобы начать пользоваться."
        await smart_edit_message(callback.message, text, keyboards.create_keys_management_keyboard(user_keys), keys_list_image)
    # ===== Конец функции manage_keys_handler =====

    # ===== ПОЛУЧЕНИЕ ПРОБНОГО ПЕРИОДА =====
    # Проверяет доступность и выдает бесплатный пробный ключ пользователю на выбранном сервере
    @user_router.callback_query(F.data == "get_trial")
    @anti_spam
    @registration_required
    async def trial_period_handler(callback: types.CallbackQuery, state: FSMContext):
        user_id = callback.from_user.id
        user_db_data = get_user(user_id)
        if user_db_data and user_db_data.get('trial_used'):
            await callback.answer("⚠️ Вы уже активировали пробный период ранее.", show_alert=True)
            return

        hosts = get_all_hosts(visible_only=True)
        if not hosts:
            await smart_edit_message(callback.message, "😔 К сожалению, сейчас нет свободных серверов для пробного периода. Попробуйте позже.", keyboards.create_back_to_menu_keyboard())
            return

        forced_host = get_setting("trial_host_id")
        if forced_host:
             if any(h['host_name'] == forced_host for h in hosts):
                 await callback.answer("⏳ Активирую пробный период...")
                 await process_trial_key_creation(callback.message, forced_host)
                 return
            
        if len(hosts) == 1:
            await callback.answer("⏳ Подготовка пробного периода...")
            await process_trial_key_creation(callback.message, hosts[0]['host_name'])
        else:
            await callback.answer()
            await smart_edit_message(
                callback.message,
                "🎁 <b>Бесплатный пробный период</b>\n\nВыберите сервер, на котором хотите протестировать наш сервис:",
                keyboards.create_host_selection_keyboard(hosts, action="trial"),
                get_setting("buy_server_image")
            )
    # ===== Конец функции trial_period_handler =====

    # ===== ОБРАБОТКА ВЫБОРА СЕРВЕРА ДЛЯ ТРИАЛА =====
    # Принимает выбор локации и инициирует процедуру создания пробного ключа
    @user_router.callback_query(F.data.startswith("select_host_trial_"))
    @anti_spam
    @registration_required
    async def trial_host_selection_handler(callback: types.CallbackQuery):
        await callback.answer()
        await process_trial_key_creation(callback.message, callback.data[len("select_host_trial_"):])
    # ===== Конец функции trial_host_selection_handler =====

    # ===== ПРОЦЕДУРА СОЗДАНИЯ ПРОБНОГО КЛЮЧА =====
    # Логика генерации уникального email, регистрации ключа на хосте и уведомления пользователя
    async def process_trial_key_creation(message: types.Message, host_name: str):
        user_id = message.chat.id
        await smart_edit_message(message, f"⚙️ <b>Подготовка конфигурации...</b>\nСоздаю бесплатный доступ на {get_setting('trial_duration_days')} дня на сервере «{host_name}»")

        try:
            user_data = get_user(user_id) or {}
            #raw_user, attempt = (user_data.get('username') or f'user{user_id}').lower(), 1
            #slug = re.sub(r"[^a-z0-9._-]", "_", raw_user).strip("_")[:16] or f"user{user_id}"
            # Строгая очистка имени для slug
            raw_user = (user_data.get('username') or f'user{user_id}').lower()
            # 1. Замена точек на подчеркивание (my.name -> my_name) и удаление пробелов (my name -> myname)
            clean_step1 = raw_user.replace(".", "_").replace(" ", "")
            # 2. Оставляем только a-z, 0-9, -, _
            clean_step2 = re.sub(r"[^a-z0-9_-]", "", clean_step1)
            # 3. Удаляем спецсимволы в начале строки и обрезаем
            slug = clean_step2.lstrip("_-")[:16]
            # 4. Если пусто - используем резервное имя
            if not slug: slug = f"user{user_id}"
            
            attempt = 1
            while True:
                candidate_email = f"trial_{slug}{f'-{attempt}' if attempt > 1 else ''}@bot.local"
                if not rw_repo.get_key_by_email(candidate_email) or attempt > 100: break
                attempt += 1

            trial_traffic, trial_hwid = int(get_setting("trial_traffic_limit_gb") or 0), int(get_setting("trial_hwid_limit") or 0)
            result = await remnawave_api.create_or_update_key_on_host(host_name=host_name, email=candidate_email, days_to_add=int(get_setting("trial_duration_days")), telegram_id=user_id, traffic_limit_gb=trial_traffic if trial_traffic > 0 else None, hwid_limit=trial_hwid if trial_hwid > 0 else None)
            
            if not result:
                await smart_edit_message(message, "❌ <b>Ошибка сервера</b>\nНе удалось сгенерировать конфигурацию. Попробуйте выбрать другой сервер.")
                return

            set_trial_used(user_id)
            new_key_id = rw_repo.record_key_from_payload(user_id=user_id, payload=result, host_name=host_name)
            
            try: await message.delete()
            except: pass
            
            expiry_dt = datetime.fromtimestamp(result['expiry_timestamp_ms'] / 1000)
            final_text = get_purchase_success_text("new", get_next_key_number(user_id) - 1, expiry_dt, result['connection_string'], email=candidate_email)
            ready_img = get_setting("key_ready_image")
            
            if ready_img and os.path.exists(ready_img):
                await message.answer_photo(photo=FSInputFile(ready_img), caption=final_text, reply_markup=keyboards.create_dynamic_key_info_keyboard(new_key_id, result['connection_string']))
            else: await message.answer(text=final_text, reply_markup=keyboards.create_dynamic_key_info_keyboard(new_key_id, result['connection_string']))
        except Exception as e:
            logger.error(f"Ошибка создания пробного периода ({user_id} на {host_name}): {e}", exc_info=True)
            await smart_edit_message(message, "⚠️ <b>Произошла ошибка</b>\nНе удалось завершить создание пробного ключа.")
    # ===== Конец функции process_trial_key_creation =====

    # ===== ВНУТРЕННЕЕ ОБНОВЛЕНИЕ ИНФОРМАЦИИ О КЛЮЧЕ =====
    # Обеспечивает двухэтапное обновление инфо-панели: сначала кэшированные данные, затем актуальные из API
    async def refresh_key_info_internal(bot: Bot, chat_id: int, message_to_edit: types.Message, key_id: int, user_id: int, prompt_message_id: int = None, state: FSMContext = None):
        key_data = rw_repo.get_key_by_id(key_id)
        if not key_data or key_data['user_id'] != user_id:
            error_text = "❌ <b>Доступ запрещен</b>\nДанный ключ не найден в вашей библиотеке."
            if prompt_message_id:
                try: await bot.edit_message_text(chat_id=chat_id, message_id=prompt_message_id, text=error_text)
                except: pass
            else: await smart_edit_message(message_to_edit, error_text)
            return

        try:
            # 1. Мгновенное обновление (кэш)
            expiry, created, email, conn = datetime.fromisoformat(key_data['expiry_date']), datetime.fromisoformat(key_data['created_date']), key_data.get('key_email'), key_data.get('subscription_url') or "⏳ Загрузка..."
            all_keys = get_user_keys(user_id); key_num = next((i + 1 for i, k in enumerate(all_keys) if k['key_id'] == key_id), 0)
            text_cached = get_key_info_text(key_num, expiry, created, conn, email=email, hwid_limit="...", hwid_usage="...", traffic_limit="...", traffic_used="...", comment=key_data.get('comment_key'))
            
            info_img, kb = get_setting("key_info_image"), keyboards.create_dynamic_key_info_keyboard(key_id, conn if conn != "⏳ Загрузка..." else "")
            if state and (await state.get_data()).get('last_callback_query_id'):
                try: await bot.answer_callback_query((await state.get_data())['last_callback_query_id'], text="✅ Обновлено!")
                except: pass

            target_msg = message_to_edit
            if prompt_message_id:
                try:
                    if info_img and os.path.exists(info_img): await bot.edit_message_media(chat_id=chat_id, message_id=prompt_message_id, media=InputMediaPhoto(media=FSInputFile(info_img), caption=text_cached), reply_markup=kb)
                    else: await bot.edit_message_text(chat_id=chat_id, message_id=prompt_message_id, text=text_cached, reply_markup=kb)
                except: pass
            else:
                updated = await smart_edit_message(message_to_edit, text_cached, kb, info_img)
                if updated: target_msg = updated

            # 2. Фоновое обновление (API)
            details, sub = await asyncio.gather(remnawave_api.get_key_details_from_host(key_data), remnawave_api.get_subscription_info(key_data['remnawave_user_uuid'], host_name=key_data.get('host_name')) if key_data.get('remnawave_user_uuid') else asyncio.sleep(0, None))
            
            conn = details.get('connection_string') or conn if details else conn
            hw_lim, hw_usg = (details['user'].get('hwidDeviceLimit'), (await remnawave_api.get_connected_devices_count(details['user']['uuid'], host_name=key_data.get('host_name'))).get('total', 0)) if details and details.get('user') else (None, 0)
            tr_lim, tr_usg = (sub.get('trafficLimit'), sub.get('trafficUsed')) if sub and isinstance(sub, dict) else (None, None)

            text_final = get_key_info_text(key_num, expiry, created, conn, email=email, hwid_limit=hw_lim, hwid_usage=hw_usg, traffic_limit=tr_lim, traffic_used=tr_usg, comment=key_data.get('comment_key'))
            kb_final = keyboards.create_dynamic_key_info_keyboard(key_id, conn)

            if prompt_message_id:
                try:
                    if info_img and os.path.exists(info_img): await bot.edit_message_media(chat_id=chat_id, message_id=prompt_message_id, media=InputMediaPhoto(media=FSInputFile(info_img), caption=text_final), reply_markup=kb_final)
                    else: await bot.edit_message_text(chat_id=chat_id, message_id=prompt_message_id, text=text_final, reply_markup=kb_final)
                except: pass
            else: await smart_edit_message(target_msg, text_final, kb_final, info_img)
        except Exception as e: logger.error(f"Ошибка обновления данных ключа ({key_id}): {e}")
    # ===== Конец функции refresh_key_info_internal =====

    # ===== ОТОБРАЖЕНИЕ КАРТОЧКИ КЛЮЧА =====
    # Инициализирует отображение детальной информации об отдельном ключе
    @user_router.callback_query(F.data.startswith("show_key_"))
    @anti_spam
    @registration_required
    async def show_key_handler(callback: types.CallbackQuery, state: FSMContext, bot: Bot):
        await callback.answer(); await state.clear()
        try: kid = int(callback.data.split("_")[2])
        except (IndexError, ValueError): return
        await refresh_key_info_internal(bot=bot, chat_id=callback.message.chat.id, message_to_edit=callback.message, key_id=kid, user_id=callback.from_user.id)
    # ===== Конец функции show_key_handler =====

    # ===== НАЧАЛО ПЕРЕНОСА КЛЮЧА НА ДРУГОЙ СЕРВЕР =====
    # Предоставляет выбор доступных серверов для миграции текущего ключа
    @user_router.callback_query(F.data.startswith("switch_server_"))
    @anti_spam
    @registration_required
    async def switch_server_start(callback: types.CallbackQuery):
        await callback.answer()
        try: kid = int(callback.data[len("switch_server_"):])
        except ValueError: return await callback.answer("⚠️ Ошибка ID.", show_alert=True)

        key = rw_repo.get_key_by_id(kid)
        if not key or key.get('user_id') != callback.from_user.id: return await callback.answer("❌ Ключ не найден.", show_alert=True)

        hosts = [h for h in (get_all_hosts(visible_only=True) or []) if h.get('host_name') != key.get('host_name')]
        if not hosts: return await callback.answer("🌍 Другие серверы сейчас недоступны.", show_alert=True)

        await smart_edit_message(callback.message, "🔄 <b>Смена сервера</b>\n\nВыберите новую локацию для вашего ключа. Все ваши настройки и время подписки будут сохранены.", keyboards.create_host_selection_keyboard(hosts, action=f"switch_{kid}"))
    # ===== Конец функции switch_server_start =====

    # ===== ВЫПОЛНЕНИЕ ПЕРЕНОСА КЛЮЧА =====
    # Осуществляет удаление ключа со старого хоста и его создание на новом, сохраняя параметры
    @user_router.callback_query(F.data.startswith("select_host_switch_"))
    @anti_spam
    @registration_required
    async def select_host_for_switch(callback: types.CallbackQuery):
        await callback.answer()
        try:
            parts = callback.data[len("select_host_switch_"):].split("_", 1)
            kid, new_host = int(parts[0]), parts[1]
        except (ValueError, IndexError): return await callback.answer("⚠️ Ошибка данных.", show_alert=True)

        key = rw_repo.get_key_by_id(kid)
        if not key or key.get('user_id') != callback.from_user.id: return await callback.answer("❌ Ключ не найден.", show_alert=True)
        
        old_host = key.get('host_name')
        if not old_host or new_host == old_host: return await callback.answer("⚠️ Некоректный выбор сервера.", show_alert=True)

        try:
            expiry_ms = int(datetime.fromisoformat(key['expiry_date']).timestamp() * 1000)
        except: expiry_ms = int((get_msk_time().replace(tzinfo=None) + timedelta(days=1)).timestamp() * 1000)

        await smart_edit_message(callback.message, f"🚀 <b>Перенос конфигурации...</b>\nМиграция на сервер «{new_host}». Пожалуйста, подождите.")

        try:
            hw_lim = key.get('hwid_limit')
            tr_lim_gb = int(key['traffic_limit_bytes'] / (1024**3)) if key.get('traffic_limit_bytes') else None
            
            res = await remnawave_api.create_or_update_key_on_host(new_host, key.get('key_email'), expiry_timestamp_ms=expiry_ms, telegram_id=callback.from_user.id, hwid_limit=hw_lim, traffic_limit_gb=tr_lim_gb)
            if not res:
                await smart_edit_message(callback.message, f"❌ <b>Ошибка миграции</b>\nНе удалось активировать ключ на сервере «{new_host}».")
                return

            try: await remnawave_api.delete_client_on_host(old_host, key.get('key_email'))
            except: pass

            update_key_host_and_info(key_id=kid, new_host_name=new_host, new_remnawave_uuid=res['client_uuid'], new_expiry_ms=res['expiry_timestamp_ms'])
            
            # Попытка мгновенного обновления карточки после переноса
            try:
                updated = rw_repo.get_key_by_id(kid)
                details, sub = await asyncio.gather(remnawave_api.get_key_details_from_host(updated), remnawave_api.get_subscription_info(res['client_uuid'], host_name=new_host) if res.get('client_uuid') else asyncio.sleep(0, None))
                
                if details and details.get('connection_string'):
                    conn = details['connection_string']
                    hw_usg = (await remnawave_api.get_connected_devices_count(details['user']['uuid'], new_host)).get('total', 0) if details.get('user') else 0
                    tr_usg = sub.get('trafficUsed') if sub and isinstance(sub, dict) else None
                    
                    all_u_keys = get_user_keys(callback.from_user.id); k_num = next((i + 1 for i, k in enumerate(all_u_keys) if k['key_id'] == kid), 0)
                    txt = get_key_info_text(k_num, datetime.fromisoformat(updated['expiry_date']), datetime.fromisoformat(updated['created_date']), conn, hwid_limit=hw_lim, hwid_usage=hw_usg, traffic_limit=tr_lim_gb, traffic_used=tr_usg)
                    await smart_edit_message(callback.message, txt, keyboards.create_dynamic_key_info_keyboard(kid))
                    return
            except: pass

            await smart_edit_message(callback.message, f"✅ <b>Готово!</b>\nКлюч успешно перенесен на сервер «{new_host}».", keyboards.create_back_to_menu_keyboard())
        except Exception as e:
            logger.error(f"Ошибка смены хоста (ID {kid} на {new_host}): {e}", exc_info=True)
            await smart_edit_message(callback.message, "⚠️ <b>Сбой при переносе</b>\nНе удалось завершить операцию. Попробуйте позже.")
    # ===== Конец функции select_host_for_switch =====

    # ===== ГЕНЕРАЦИЯ QR-КОДА КОНФИГУРАЦИИ =====
    # Запрашивает актуальную строку подключения и формирует графический QR-код для быстрого импорта
    @user_router.callback_query(F.data.startswith("show_qr_"))
    @anti_spam
    @registration_required
    async def show_qr_handler(callback: types.CallbackQuery):
        await callback.answer("⏳ Генерация QR-кода...")
        try: kid = int(callback.data.split("_")[2])
        except: return
        
        key = rw_repo.get_key_by_id(kid)
        if not key or key['user_id'] != callback.from_user.id: return
        
        try:
            details = await remnawave_api.get_key_details_from_host(key)
            if details and details.get('connection_string'):
                qr_img = qrcode.make(details['connection_string'])
                bio = BytesIO(); qr_img.save(bio, "PNG"); bio.seek(0)
                
                # Заменяем сообщение на фото с QR-кодом
                await callback.message.edit_media(
                    media=InputMediaPhoto(
                        media=BufferedInputFile(bio.read(), filename="vpn_qr.png"),
                        caption=f"📸 <b>QR-код для конфигурации #{kid}</b>\n\nОтсканируйте его в вашем VPN-клиенте для быстрого импорта."
                    ),
                    reply_markup=keyboards.create_qr_keyboard(kid)
                )
            else: await callback.answer("❌ Не удалось получить данные подключения.", show_alert=True)
        except Exception as e:
            logger.error(f"Ошибка QR: {e}")
            await callback.answer("⚠️ Ошибка при создании QR-кода.", show_alert=True)
    # ===== Конец функции show_qr_handler =====
 
    # ===== УПРАВЛЕНИЕ УСТРОЙСТВАМИ (HWID) =====
    # Отображает список подключенных устройств и позволяет удалять их
    async def _render_devices_list(message: types.Message, key_id: int, user_id: int, page: int = 0):
        key = rw_repo.get_key_by_id(key_id)
        if not key or key['user_id'] != user_id:
            return

        host_name = key.get('host_name')
        user_uuid = key.get('remnawave_user_uuid')
        
        if not user_uuid:
            await smart_edit_message(message, "⚠️ Для этого ключа нет данных о пользователе.", keyboards.create_key_info_keyboard(key_id))
            return
        
        # Banner Image
        photo_path = rw_repo.get_setting("devices_list_image")
        
        devices = await remnawave_api.get_user_devices(user_uuid, host_name=host_name)
        
        if not devices:
            text = "🖥 <b>Подключённые устройства</b>\n\nСписок устройств пуст."
            await smart_edit_message(message, text, keyboards.create_devices_list_keyboard([], key_id), photo_path=photo_path)
            return

        ITEMS_PER_PAGE = 5
        total_pages = (len(devices) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
        if page >= total_pages: page = total_pages - 1
        if page < 0: page = 0
        
        start_index = page * ITEMS_PER_PAGE
        end_index = start_index + ITEMS_PER_PAGE
        current_devices = devices[start_index:end_index]

        hwid_limit = key.get('hwid_limit')
        if hwid_limit is None:
            user_info = await remnawave_api.get_user_by_uuid(user_uuid, host_name=host_name)
            if user_info:
                hwid_limit = user_info.get('hwidDeviceLimit')
        limit_str = str(hwid_limit) if hwid_limit else "∞"

        text = f"🖥 <b>Подключённые устройства</b>\n\nВсего: <b>{len(devices)} из {limit_str}</b> доступных!\n\n"
        for i, dev in enumerate(current_devices):
            ua = dev.get('userAgent', 'Unknown') 
            
            abs_index = start_index + i + 1
            hwid = dev.get('hwid', 'N/A')

            platform = dev.get('platform') or ""
            model = dev.get('deviceModel') or ""
            os_ver = dev.get('osVersion') or dev.get('appVersion') or dev.get('version') or ""
            
            device_emoji = get_device_emoji(ua, platform, model)
            
            if not platform: platform = "Неизвестно"
            
            dev_str = f"{platform}"
            if model and model.lower() != platform.lower():
                dev_str += f" ({model})"
            if os_ver:
                dev_str += f" — {os_ver}"
            
            text += f"{abs_index}. {device_emoji} {dev_str}\n"
            text += f"👤 <b>Agent:</b> <code>{ua}</code>\n\n"

        text += "\n💡 После удаления не подключайтесь с этого устройства, иначе оно снова займет свободный слот."

        await smart_edit_message(message, text, keyboards.create_devices_list_keyboard(devices, key_id, page, total_pages), photo_path=photo_path)

    @user_router.callback_query(F.data.startswith("key_devices_"))
    @anti_spam
    @registration_required
    async def key_devices_handler(callback: types.CallbackQuery):
        await callback.answer()
        parts = callback.data.split("_")
        try: 
            kid = int(parts[2])
            page = int(parts[3]) if len(parts) > 3 else 0
        except: return

        await _render_devices_list(callback.message, kid, callback.from_user.id, page)

    @user_router.callback_query(F.data.startswith("del_dev_"))
    @anti_spam
    @registration_required
    async def delete_device_handler(callback: types.CallbackQuery): 
        parts = callback.data.split("_")
        if len(parts) < 4: return
        
        device_id = parts[2]
        try: kid = int(parts[3])
        except: return

        key = rw_repo.get_key_by_id(kid)
        if not key or key['user_id'] != callback.from_user.id: return
        
        host_name = key.get('host_name')
        user_uuid = key.get('remnawave_user_uuid')
        
        if not user_uuid:
            await callback.answer("⚠️ Ошибка данных пользователя.", show_alert=True)
            return
            
        await callback.answer("⏳ Удаление устройства...")
         
        hwid_target = device_id
        
        if hwid_target == "None" or not hwid_target:
             await callback.answer("⚠️ Некорректный ID устройства.", show_alert=True)
        else:
            success = await remnawave_api.delete_user_device(user_uuid, hwid_target, host_name=host_name)
            
            if success:
                await callback.answer("✅ Устройство удалено!", show_alert=True)
            else:
                await callback.answer("❌ Не удалось удалить. Возможно, оно уже удалено.", show_alert=True)
        
        await _render_devices_list(callback.message, kid, callback.from_user.id, 0)
    # ===== Конец функций управления устройствами =====

    @user_router.callback_query(F.data == "ignore")
    async def ignore_callback_handler(callback: types.CallbackQuery):
        await callback.answer()

    # ===== ОТОБРАЖЕНИЕ ИНСТРУКЦИЙ ПО ТИПАМ ОС =====
    # Набор обработчиков для вывода обучающих материалов по настройке VPN на различных устройствах
    @user_router.callback_query(F.data.startswith("howto_vless_"))
    @anti_spam
    @registration_required
    async def show_instruction_handler_with_key(callback: types.CallbackQuery):
        await callback.answer()
        try: kid = int(callback.data.split("_")[2])
        except: return
        msg = get_setting("howto_intro_text") or "📖 <b>Инструкции по подключению</b>\n\nВыберите вашу операционную систему для получения подробного руководства:"
        await smart_edit_message(callback.message, msg, keyboards.create_howto_vless_keyboard_key(kid), get_setting("howto_image"))

    @user_router.callback_query(F.data == "howto_vless")
    @anti_spam
    @registration_required
    async def show_instruction_handler(callback: types.CallbackQuery):
        await callback.answer()
        msg = get_setting("howto_intro_text") or "📖 <b>Инструкции по подключению</b>\n\nВыберите вашу операционную систему для получения подробного руководства:"
        await smart_edit_message(callback.message, msg, keyboards.create_howto_vless_keyboard(), get_setting("howto_image"))

    @user_router.callback_query(F.data.in_(["howto_android", "howto_ios", "howto_windows", "howto_linux"]))
    @anti_spam
    @registration_required
    async def os_instructions_router(callback: types.CallbackQuery):
        await send_instruction_response(callback, callback.data.split("_")[1])
    # ===== Конец функций инструкций =====



    # ===== НАЧАЛО ПОКУПКИ НОВОГО КЛЮЧА =====
    # Инициирует процесс выбора сервера для создания новой VPN-подписки
    @user_router.callback_query(F.data == "buy_new_key")
    @anti_spam
    @registration_required
    async def buy_new_key_handler(callback: types.CallbackQuery):
        await callback.answer()
        hosts = rw_repo.get_all_hosts(visible_only=True) or []
        if not hosts: return await smart_edit_message(callback.message, "❌ <b>Нет доступных локаций</b>\nВ данный момент все серверы на техническом обслуживании. Попробуйте позже.", keyboards.create_back_to_menu_keyboard())
        
        if len(hosts) == 1:
            await callback.answer("⏳ Загрузка тарифов...")
            return await _show_plans_for_host(callback, hosts[0]['host_name'])

        await smart_edit_message(callback.message, "🌍 <b>Выбор сервера</b>\n\nВыберите страну и локацию для вашей новой подписки:", keyboards.create_host_selection_keyboard(hosts, action="new"), get_setting("buy_server_image"))
    # ===== Конец функции buy_new_key_handler =====

    # ===== ВЫБОР ТАРИФА ДЛЯ ПОКУПКИ =====
    # Отображает список доступных тарифных планов для выбранного сервера
    @user_router.callback_query(F.data.startswith("select_host_new_"))
    @anti_spam
    @registration_required
    async def select_host_for_purchase_handler(callback: types.CallbackQuery):
        await callback.answer()
        host_name = callback.data[len("select_host_new_"):]
        await _show_plans_for_host(callback, host_name)

    async def _show_plans_for_host(callback: types.CallbackQuery, host_name: str, action: str = "new", key_id: int = 0, tier_price: float = 0.0):
        plans = get_plans_for_host(host_name)
        if not plans: return await smart_edit_message(callback.message, f"❌ Для сервера «{host_name}» еще не настроены тарифные планы.")
        
        host_data = get_host(host_name)
        if action == "extend":
            plan_text = f"🔄 <b>Продление подписки</b>\n\n{host_data['description']}" if host_data and host_data.get('description') else f"💳 <b>Продление для сервера «{host_name}»</b>\nВыберите тарифный план:"
            img_setting = "extend_plan_image"
        else:
            plan_text = host_data.get('description') if host_data and host_data.get('description') else f"💳 <b>Выбор тарифа: {host_name}</b>\n\nВыберите подходящий период подписки:"
            img_setting = "buy_plan_image"
        
        display_plans = [p.copy() for p in plans]
        try:
             sale_percent = get_seller_discount_percent(callback.from_user.id)
             if sale_percent > 0:
                 for p in display_plans:
                     price = Decimal(str(p['price']))
                     p['price'] = float((price - (price * sale_percent / 100)).quantize(Decimal("0.01")))
        except Exception as e:
             logger.error(f"[SELLER_{callback.from_user.id}] - ошибка: {e}")

        if tier_price > 0:
            for p in display_plans:
                months = int(p.get('months') or 1)
                p['price'] = float(p['price']) + (tier_price * months)

        await smart_edit_message(callback.message, plan_text, keyboards.create_plans_keyboard(display_plans, action=action, host_name=host_name, key_id=key_id), get_setting(img_setting))
    # ===== Конец функции select_host_for_purchase_handler =====

    # ===== ПРОДЛЕНИЕ СУЩЕСТВУЮЩЕГО КЛЮЧА =====
    # Переводит пользователя к выбору тарифа для увеличения срока действия активной подписки
    @user_router.callback_query(F.data.startswith("extend_key_"))
    @anti_spam
    @registration_required
    async def extend_key_handler(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer()
        try: kid = int(callback.data.split("_")[2])
        except: return await smart_edit_message(callback.message, "⚠️ Ошибка идентификации ключа.")

        key = rw_repo.get_key_by_id(kid)
        if not key or key['user_id'] != callback.from_user.id: return await smart_edit_message(callback.message, "❌ Ключ не найден или доступ к нему ограничен.")
        
        host_name = key.get('host_name')
        if not host_name: return await smart_edit_message(callback.message, "⚠️ Ошибка сервера ключа. Обратитесь в поддержку.")

        host_data = get_host(host_name)
        if host_data and host_data.get('device_mode') == 'tiers':
            if host_data.get('tier_lock_extend'):
                key_hwid = key.get('hwid_limit')
                if not key_hwid and key.get('remnawave_user_uuid'):
                    try:
                        from shop_bot.modules import remnawave_api
                        user_info = await remnawave_api.get_user_by_uuid(key['remnawave_user_uuid'], host_name=host_name)
                        if user_info:
                            key_hwid = user_info.get('hwidDeviceLimit')
                    except Exception as e:
                        import logging
                        logging.error(f"Не удалось получить hwidDeviceLimit: {e}")
                key_hwid = int(key_hwid) if key_hwid is not None else 1
                
                preset_found = False
                if key_hwid > 1:
                    tiers = get_device_tiers(host_name)
                    for t in tiers:
                        if t['device_count'] == int(key_hwid):
                            await state.update_data(
                                tier_device_count=t['device_count'],
                                tier_price=float((t['device_count'] - 1) * t['price']),
                                selected_tier_id=t['tier_id'],
                                _extend_tier_preset=True
                            )
                            preset_found = True
                            break
                if not preset_found:
                    await state.update_data(
                        tier_device_count=1, tier_price=0.0, selected_tier_id=0,
                        _extend_tier_preset=True
                    )
            else:
                await state.update_data(
                    tier_device_count=1, tier_price=0.0, selected_tier_id=0,
                    _extend_tier_preset=False
                )

        data = await state.get_data()
        tp = data.get('tier_price', 0.0) or 0.0
        await _show_plans_for_host(callback, host_name, action="extend", key_id=kid, tier_price=float(tp))
    # ===== Конец функции extend_key_handler =====

    # ===== ПЕРЕХОД К ОПЛАТЕ (ВВОД EMAIL) =====
    # Сохраняет выбор тарифа и запрашивает email пользователя, если это требуется настройками
    @user_router.callback_query(F.data.startswith("buy_"))
    @anti_spam
    @registration_required
    async def plan_selection_handler(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer()
        old_data = await state.get_data()
        tier_keep = {}
        if old_data.get('_extend_tier_preset'):
            tier_keep = {k: old_data[k] for k in ('tier_device_count', 'tier_price', 'selected_tier_id') if k in old_data}
        await state.clear()
        parts = callback.data.split("_")[1:]
        await state.update_data(action=parts[-2], key_id=int(parts[-1]), plan_id=int(parts[-3]), host_name="_".join(parts[:-3]), **tier_keep)

        host_name = "_".join(parts[:-3])
        plan_id = int(parts[-3])
        action = parts[-2]
        key_id = int(parts[-1])
        host_data = get_host(host_name)
        if host_data and host_data.get('device_mode') == 'tiers':
            if action == 'extend' and not host_data.get('tier_lock_extend'):
                tier_keep = {}
                await state.update_data(tier_device_count=None, tier_price=None, selected_tier_id=None)
            tiers = get_device_tiers(host_name)
            if tiers and not tier_keep.get('tier_device_count'):
                await _show_device_tiers(callback.message, tiers, host_name, plan_id, action, key_id)
                return

        await _proceed_to_email_or_pay(callback.message, state)
    # ===== Конец функции plan_selection_handler =====

    async def _show_device_tiers(message, tiers, host_name, plan_id, action, key_id, selected_tier_id=None):
        img_setting = "extend_plan_image" if action == "extend" else "buy_plan_image"
        await smart_edit_message(
            message,
            "📱 <b>Выберите количество устройств</b>\n\nЦена зависит от выбранного количества:",
            keyboards.create_device_tiers_keyboard(tiers, host_name, plan_id, action, key_id, selected_tier_id=selected_tier_id),
            get_setting(img_setting)
        )

    async def _proceed_to_email_or_pay(message, state):
        if get_setting("skip_email") == "1":
            await state.update_data(customer_email=None)
            await show_payment_options(message, state)
        else:
            await smart_edit_message(message, "📧 <b>Ваш Email</b>\n\nПожалуйста, введите адрес электронной почты. На него будет отправлен чек после успешной оплаты.", keyboards.create_skip_email_keyboard(), get_setting("enter_email_image"))
            await state.set_state(PaymentProcess.waiting_for_email)

    @user_router.callback_query(F.data.startswith("select_tier_"))
    @anti_spam
    @registration_required
    async def device_tier_selection_handler(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer()
        try:
            tier_id = int(callback.data.split("_")[2])
        except:
            return
        data = await state.get_data()
        host_name = data.get('host_name', '')
        if tier_id == 0:
            await state.update_data(tier_device_count=1, tier_price=0.0, selected_tier_id=0)
        else:
            tier = get_device_tier_by_id(tier_id)
            if not tier:
                return
            calculated_price = float((tier['device_count'] - 1) * tier['price'])
            await state.update_data(tier_device_count=tier['device_count'], tier_price=calculated_price, selected_tier_id=tier_id)
        tiers = get_device_tiers(host_name)
        await _show_device_tiers(callback.message, tiers, host_name, data.get('plan_id', 0), data.get('action', 'new'), data.get('key_id', 0), selected_tier_id=tier_id)

    @user_router.callback_query(F.data == "confirm_tier")
    @anti_spam
    @registration_required
    async def confirm_tier_handler(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer()
        data = await state.get_data()
        if not data.get('tier_device_count'):
            return
        await _proceed_to_email_or_pay(callback.message, state)

    @user_router.callback_query(PaymentProcess.waiting_for_email, F.data == "back_to_plans")
    async def back_to_plans_handler(callback: types.CallbackQuery, state: FSMContext):
        data = await state.get_data(); await state.clear()
        action, host, kid = data.get('action'), data.get('host_name'), data.get('key_id', 0)

        if action == 'new' and host:
            plans = get_plans_for_host(host)
            host_data = get_host(host)
            text = host_data.get('description') if host_data and host_data.get('description') else "💳 <b>Выбор тарифа</b>"
            
            # Seller Discount Display
            display_plans = [p.copy() for p in plans]
            try:
                 sale_percent = get_seller_discount_percent(callback.from_user.id)
                 if sale_percent > 0:
                     logger.info(f"[SELLER_{callback.from_user.id}] - скидка {sale_percent}%")
                     for p in display_plans:
                         original_price = p['price']
                         price = Decimal(str(p['price']))
                         discounted = float((price - (price * sale_percent / 100)).quantize(Decimal("0.01")))
                         p['price'] = discounted
                         logger.info(f"[SELLER_{callback.from_user.id}] - Тариф '{p.get('plan_name')}': {original_price} -> {discounted}")
            except Exception as e:
                 logger.error(f"[SELLER_{callback.from_user.id}] - ошибка: {e}")

            await smart_edit_message(callback.message, text, keyboards.create_plans_keyboard(display_plans, action="new", host_name=host), get_setting("buy_plan_image"))
        elif action == 'extend' and kid:
            key = rw_repo.get_key_by_id(kid)
            if key:
                host = key.get('host_name')
                plans = get_plans_for_host(host)
                
                # Seller Discount Display
                display_plans = [p.copy() for p in plans]
                try:
                     sale_percent = get_seller_discount_percent(callback.from_user.id)
                     if sale_percent > 0:
                         logger.info(f"[SELLER_{callback.from_user.id}] - скидка {sale_percent}%")
                         for p in display_plans:
                             original_price = p['price']
                             price = Decimal(str(p['price']))
                             discounted = float((price - (price * sale_percent / 100)).quantize(Decimal("0.01")))
                             p['price'] = discounted
                             logger.info(f"[SELLER_{callback.from_user.id}] - Тариф '{p.get('plan_name')}': {original_price} -> {discounted}")
                except Exception as e:
                     logger.error(f"[SELLER_{callback.from_user.id}] - ошибка: {e}")

                await smart_edit_message(callback.message, f"🔄 <b>Продление: {host}</b>", keyboards.create_plans_keyboard(display_plans, action="extend", host_name=host, key_id=kid), get_setting("extend_plan_image"))
        else: await back_to_main_menu_handler(callback)

    @user_router.message(PaymentProcess.waiting_for_email)
    @anti_spam
    async def process_email_handler(message: types.Message, state: FSMContext):
        if is_valid_email(message.text):
            await state.update_data(customer_email=message.text)
            await message.answer(f"✅ <b>Email сохранен:</b> {message.text}")
            await show_payment_options(message, state)
        else: await message.answer("❌ <b>Некорректный формат</b>\nПожалуйста, введите валидный адрес (например, example@mail.com).")

    @user_router.callback_query(PaymentProcess.waiting_for_email, F.data == "skip_email")
    async def skip_email_handler(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer(); await state.update_data(customer_email=None)
        await show_payment_options(callback.message, state)

    # ===== ВЫБОР МЕТОДА ОПЛАТЫ =====
    # Вычисляет итоговую стоимость и выводит кнопки доступных платежных шлюзов
    async def show_payment_options(message: types.Message, state: FSMContext, bot: Bot = None, prompt_message_id: int = None):
        data = await state.get_data(); user = get_user(message.chat.id)
        plan = get_plan_by_id(data.get('plan_id'))
        if not plan: return await (message.edit_text if isinstance(message, types.Message) else message.answer)("❌ Ошибка: Тариф не найден.")
        
        price = calculate_order_price(plan, user, data.get('promo_code'), data.get('promo_discount', 0))
        months = int(plan.get('months') or 1)
        price += Decimal(str(data.get('tier_price', 0))) * months
        await state.update_data(final_price=float(price))
        
        balance = get_balance(message.chat.id)
        
        promo_text = ""
        if data.get('promo_code'):
            disc_val = data.get('promo_discount',0)
            promo_text = (
                f"\n✅ Промокод активирован!\n"
                f"🎟 Промокод {data['promo_code']} применен!\n"
                f"🛍 Ваша скидка: {disc_val:.2f} RUB\n"
            )
            
        text = f"💰 К оплате: {price:.2f} RUB\n{promo_text}\n{CHOOSE_PAYMENT_METHOD_MESSAGE}"
        
        back_cb = "back_to_email_prompt" if get_setting("skip_email") != "1" else (f"select_host_new_{data.get('host_name')}" if data.get('action') == 'new' else "manage_keys")
        kb = keyboards.create_payment_method_keyboard(PAYMENT_METHODS, action=data.get('action'), key_id=data.get('key_id'), show_balance=(balance >= float(price)), main_balance=balance, price=float(price), promo_applied=bool(data.get('promo_code')), back_callback=back_cb)
        payment_img = get_setting("payment_method_image")

        if prompt_message_id and bot:
            try:
                if payment_img and os.path.exists(payment_img):
                    await bot.edit_message_media(chat_id=message.chat.id, message_id=prompt_message_id, media=InputMediaPhoto(media=FSInputFile(payment_img), caption=text), reply_markup=kb)
                else:
                    await bot.edit_message_text(chat_id=message.chat.id, message_id=prompt_message_id, text=text, reply_markup=kb)
                await state.set_state(PaymentProcess.waiting_for_payment_method)
                return
            except: pass

        await smart_edit_message(message, text, kb, payment_img)
        await state.set_state(PaymentProcess.waiting_for_payment_method)

    @user_router.callback_query(PaymentProcess.waiting_for_payment_method, F.data == "back_to_email_prompt")
    async def back_to_email_prompt_handler(callback: types.CallbackQuery, state: FSMContext):
        await smart_edit_message(callback.message, "📧 <b>Ввод Email</b>\nВведите адрес почты или пропустите этот шаг:", keyboards.create_skip_email_keyboard(), get_setting("enter_email_image"))
        await state.set_state(PaymentProcess.waiting_for_email)

    @user_router.callback_query(PaymentProcess.waiting_for_payment_method, F.data == "back_to_payment_options")
    async def back_to_payment_options_handler(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer()
        await show_payment_options(callback.message, state)

    @user_router.callback_query(TopUpProcess.waiting_for_topup_method, F.data == "back_to_topup_options")
    async def back_to_topup_options_handler(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer()
        data = await state.get_data()
        final_amount = data.get('topup_amount')
        if not final_amount:
            await state.clear()
            await back_to_main_menu_handler(callback)
            return
        await smart_edit_message(
            callback.message,
            (
                f"✅ Сумма принята: {final_amount:.2f} RUB\n"
                "Выберите удобный способ оплаты:"
            ),
            keyboards.create_topup_payment_method_keyboard(PAYMENT_METHODS),
            get_setting("payment_method_image")
        )

    # ===== ПРИМЕНЕНИЕ ПРОМОКОДА =====
    # Обрабатывает ввод и валидацию скидочных купонов перед оплатой
    @user_router.callback_query(PaymentProcess.waiting_for_payment_method, F.data == "enter_promo_code")
    async def prompt_promo_code(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer()
        await state.update_data(promo_prompt_mid=callback.message.message_id)
        await smart_edit_message(callback.message, "🎟 <b>Ввод промокода</b>\nПришлите код ответным сообщением для получения скидки:", keyboards.create_cancel_keyboard("cancel_promo"), get_setting("payment_method_image"))
        await state.set_state(PaymentProcess.waiting_for_promo_code)

    @user_router.callback_query(PaymentProcess.waiting_for_promo_code, F.data == "cancel_promo")
    async def cancel_promo_entry(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer("Отмена"); await show_payment_options(callback.message, state)

    @user_router.message(PaymentProcess.waiting_for_promo_code)
    async def handle_promo_code_input(message: types.Message, state: FSMContext, bot: Bot):
        try: await message.delete()
        except: pass

        code = (message.text or '').strip()
        data = await state.get_data()
        prompt_mid = data.get('promo_prompt_mid')
        chat_id = message.chat.id

        if code.lower() in ["отмена", "cancel", "стоп"]: return await show_payment_options(message, state, bot=bot, prompt_message_id=prompt_mid)
        
        promo, err = check_promo_code_available(code, message.from_user.id)
        
        if not err and promo and promo.get('promo_type') in ('universal', 'balance'):
            err = "wrong_type"
            
        if err:
            err_msgs = {
                "not_found": "❓ Код не найден.", 
                "expired": "❌ Срок действия истек.", 
                "user_limit_reached": "❌ Вы уже использовали этот код.",
                "wrong_type": "❌ Этот промокод нельзя использовать при оплате (он предназначен для прямого ввода в профиле)."
            }
            err_text = f"🎟 <b>Ввод промокода</b>\n\n{err_msgs.get(err, '❌ Промокод недействителен.')}\n\nПопробуйте другой код:"
            if prompt_mid:
                try:
                    payment_img = get_setting("payment_method_image")
                    if payment_img and os.path.exists(payment_img):
                        await bot.edit_message_media(chat_id=chat_id, message_id=prompt_mid, media=InputMediaPhoto(media=FSInputFile(payment_img), caption=err_text), reply_markup=keyboards.create_cancel_keyboard("cancel_promo"))
                    else:
                        await bot.edit_message_text(chat_id=chat_id, message_id=prompt_mid, text=err_text, reply_markup=keyboards.create_cancel_keyboard("cancel_promo"))
                except: pass
            return
        
        plan = get_plan_by_id(data.get('plan_id'))
        disc = Decimal(str(promo.get('discount_amount') or 0))
        if promo.get('discount_percent'): disc = (Decimal(str(plan['price'])) * Decimal(str(promo['discount_percent'])) / 100).quantize(Decimal("0.01"))
        
        await state.update_data(promo_code=promo['code'], promo_discount=float(disc))
        await show_payment_options(message, state, bot=bot, prompt_message_id=prompt_mid)

    # ===== ПЛАТЕЖ ЧЕРЕЗ YOOKASSA =====
    # Создает счет в ЮKassa и отправляет кнопку-ссылку пользователю
    @user_router.callback_query(PaymentProcess.waiting_for_payment_method, F.data == "pay_yookassa")
    @anti_spam
    async def create_yookassa_payment_handler(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer("⏳ Создание счета..."); data = await state.get_data()
        shop_id, secret = get_setting("yookassa_shop_id"), get_setting("yookassa_secret_key")
        if not shop_id or not secret: return await callback.message.answer("⚠️ Оплата через ЮKassa временно недоступна.")
            
        Configuration.account_id, Configuration.secret_key = shop_id, secret
        plan = get_plan_by_id(data.get('plan_id'))
        if not plan: return await state.clear()
            
        price = Decimal(str(data.get('final_price', plan['price'])))
        email = data.get('customer_email') or get_setting("receipt_email")

        try:
            pid, meta = await create_pending_payment(user_id=callback.from_user.id, amount=float(price), payment_method="YooKassa", action=data['action'], metadata_source=data, plan_id=plan['plan_id'], months=plan['months'])
            logger.info(f"Оплата (YooKassa): пользователь {callback.from_user.id}, план {plan['plan_id']}, сумма {price} RUB")
            comment = get_transaction_comment(callback.from_user, data['action'], plan['months'], data.get('host_name'))
            
            payload = {"amount": {"value": f"{price:.2f}", "currency": "RUB"}, "confirmation": {"type": "redirect", "return_url": f"https://t.me/{TELEGRAM_BOT_USERNAME}"}, "capture": True, "description": comment, "metadata": meta}
            if email and is_valid_email(email): payload['receipt'] = {"customer": {"email": email}, "items": [{"description": comment, "quantity": "1.00", "amount": {"value": f"{price:.2f}", "currency": "RUB"}, "vat_code": "1", "payment_subject": "service", "payment_mode": "full_payment"}]}
            
            pay_obj = Payment.create(payload, pid)
            await smart_edit_message(callback.message, "💳 <b>Оплата через ЮKassa</b>\nНажмите на кнопку ниже для оплаты картой или через СБП:", get_payment_keyboard("YooKassa", pay_obj.confirmation.confirmation_url, back_callback="back_to_payment_options"), get_setting("payment_image"))
        except Exception as e:
            logger.error(f"YooKassa Ошибка: {e}", exc_info=True)
            await callback.message.answer("⚠️ Ошибка создания платежа."); await state.clear()

    # ===== ПЛАТЕЖ ЧЕРЕЗ CRYPTOBOT =====
    # Генерирует инвойс для оплаты через Crypto Pay и выдает ссылку пользователю
    @user_router.callback_query(PaymentProcess.waiting_for_payment_method, F.data == "pay_cryptobot")
    @anti_spam
    async def create_cryptobot_invoice_handler(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer("⏳ Создание инвойса..."); data = await state.get_data()
        plan = get_plan_by_id(data.get('plan_id'))
        if not plan: return await state.clear()

        price = Decimal(str(data.get('final_price', plan['price'])))
        try:
            pid, meta = await create_pending_payment(user_id=callback.from_user.id, amount=float(price), payment_method="CryptoBot", action=data['action'], metadata_source=data, plan_id=plan['plan_id'], months=plan['months'])
            logger.info(f"Оплата (CryptoBot): пользователь {callback.from_user.id}, план {plan['plan_id']}, сумма {price} RUB")
            
            payload = ":".join([str(callback.from_user.id), str(plan['months']), f"{price:.2f}", str(data['action']), str(data.get('key_id') or "None"), str(data.get('host_name') or ""), str(plan['plan_id']), str(data.get('customer_email') or "None"), "CryptoBot", str(data.get('promo_code') or "None"), f"{data.get('promo_discount', 0):.2f}", str(data.get('tier_device_count') or 'None')])
            res = await create_cryptobot_api_invoice(amount=float(price), payload_str=payload)
            
            if res:
                await smart_edit_message(callback.message, "💎 <b>Оплата через CryptoBot</b>\nНажмите на кнопку ниже для оплаты криптовалютой:", keyboards.create_cryptobot_payment_keyboard(res[0], res[1], back_callback="back_to_payment_options"), get_setting("payment_image"))
            else:
                logger.error(f"CryptoBot: Ошибка создания платежа (пустой ответ API) для {callback.from_user.id}")
                await callback.message.answer("❌ Ошибка CryptoBot API.")
        except Exception as e:
            logger.error(f"CryptoBot Ошибка: {e}", exc_info=True)
            await callback.message.answer("⚠️ Ошибка создания счета."); await state.clear()

    # ===== ПРОВЕРКА СТАТУСА CRYPTOBOT =====
    # Позволяет вручную обновить статус инвойса и завершить покупку при подтверждении транзакции
    @user_router.callback_query(F.data.startswith("check_crypto_invoice:"))
    @anti_spam
    async def check_crypto_invoice_handler(callback: types.CallbackQuery, state: FSMContext, bot: Bot):
        await callback.answer("⏳ Проверка...")
        try: inv_id = int(callback.data.split(":")[1])
        except: return await callback.message.answer("⚠️ Ошибка ID инвойса.")

        token = (get_setting("cryptobot_token") or "").strip()
        if not token: return await callback.message.answer("❌ API токен не настроен.")

        try:
            async with aiohttp.ClientSession() as sess:
                async with sess.post("https://pay.crypt.bot/api/getInvoices", headers={"Crypto-Pay-API-Token": token}, json={"invoice_ids": [inv_id]}) as resp:
                    data = await resp.json()
            
            invoices = data.get("result", {}).get("items", []) if data.get("ok") else []
            if not invoices or invoices[0].get("status") != "paid": return await callback.message.answer("⏳ Оплата еще не подтверждена. Попробуйте через минуту.")
            
            payload = invoices[0].get("payload")
            if not payload: return await callback.message.answer("⚠️ Оплата получена, но данные повреждены. Свяжитесь с поддержкой.")
            
            p = payload.split(":"); 
            if len(p) < 9: return await callback.message.answer("⚠️ Неверный формат данных платежа.")

            stable_payment_id = f"cryptobot_{inv_id}"
            metadata = {"user_id": p[0], "months": p[1], "price": p[2], "action": p[3], "key_id": p[4], "host_name": p[5], "plan_id": p[6], "customer_email": (p[7] if p[7] != 'None' else None), "payment_method": p[8], "transaction_id": str(inv_id), "payment_id": stable_payment_id}
            if len(p) >= 12:
                metadata["tier_device_count"] = p[11] if p[11] != 'None' else None
            
            await process_successful_payment(bot, metadata)
            await callback.message.answer("✅ <b>Оплата подтверждена!</b> Ваш ключ/баланс успешно обновлены.")
        except Exception as e:
            logger.error(f"Ошибка ручной проверки Crypto: {e}")
            await callback.answer("⚠️ Сбой при обработке платежа. Обратитесь в поддержку.")
    # ===== Конец функций CryptoBot =====

    # ===== ОПЛАТА ЧЕРЕЗ TON CONNECT =====
    # Инициирует процесс привязки кошелька и подготовки транзакции в сети TON
    @user_router.callback_query(PaymentProcess.waiting_for_payment_method, F.data == "pay_tonconnect")
    @anti_spam
    async def create_ton_invoice_handler(callback: types.CallbackQuery, state: FSMContext):
        data = await state.get_data(); uid = callback.from_user.id
        wallet, plan = get_setting("ton_wallet_address"), get_plan_by_id(data.get('plan_id'))
        if not wallet or not plan: return await smart_edit_message(callback.message, "❌ Оплата через TON временно недоступна.")

        await callback.answer("⏳ Подготовка TON Connect..."); user = get_user(uid)
        price_rub, months = Decimal(str(data.get('final_price', plan['price']))), int(plan['months'])
        rt_usdt, rt_ton = await get_usdt_rub_rate(), await get_ton_usdt_rate()

        if not rt_usdt or not rt_ton: return await smart_edit_message(callback.message, "❌ Не удалось получить актуальный курс TON.")
        
        price_ton = (price_rub / rt_usdt / rt_ton).quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)
        try:
            pid, _ = await create_pending_payment(user_id=uid, amount=float(price_rub), payment_method="TON Connect", action=data.get('action'), metadata_source=data, plan_id=data.get('plan_id'), months=months)
            logger.info(f"Оплата (TON Connect): пользователь {uid}, план {data.get('plan_id')}, сумма {price_rub} RUB")
            conn_url = await _start_ton_connect_process(uid, {'messages': [{'address': wallet, 'amount': str(int(price_ton * 10**9)), 'payload': pid}], 'valid_until': int(get_msk_time().timestamp()) + 600})
            
            bio = BytesIO(); qrcode.make(conn_url).save(bio, "PNG"); bio.seek(0)
            await callback.message.delete()
            await callback.message.answer_photo(photo=BufferedInputFile(bio.getvalue(), "ton_qr.png"), caption=f"💎 <b>Оплата через TON Connect</b>\n\nСумма: <code>{price_ton}</code> <b>TON</b>\n\n1. На мобильном: нажмите <b>«Открыть кошелек»</b>\n2. На ПК: отсканируйте <b>QR-код</b>\n\nПосле оплаты транзакция подтвердится автоматически.", reply_markup=keyboards.create_ton_connect_keyboard(conn_url, back_callback="back_to_payment_options"))
        except Exception as e:
            logger.error(f"Ошибка TON Connect ({uid}): {e}")
            await callback.message.answer("⚠️ Ошибка генерации ссылки."); await state.clear()
    # ===== Конец функции create_ton_invoice_handler =====

    # ===== ОПЛАТА С ЛИЧНОГО БАЛАНСА =====
    # Списывает средства с внутреннего счета пользователя для мгновенной активации услуг
    @user_router.callback_query(PaymentProcess.waiting_for_payment_method, F.data == "pay_balance")
    @anti_spam
    async def pay_with_main_balance_handler(callback: types.CallbackQuery, state: FSMContext, bot: Bot):
        await callback.answer(); data = await state.get_data(); plan = get_plan_by_id(data.get('plan_id'))
        if not plan: return await state.clear()

        price = float(data.get('final_price', plan['price']))
        logger.info(f"Оплата (Баланс): пользователь {callback.from_user.id}, план {plan['plan_id']}, сумма {price} RUB")
        if not deduct_from_balance(callback.from_user.id, price): return await callback.answer("⚖️ Недостаточно средств на балансе.", show_alert=True)

        meta = {"user_id": callback.from_user.id, "months": int(plan['months']), "price": price, "action": data.get('action'), "key_id": data.get('key_id'), "host_name": data.get('host_name'), "plan_id": data.get('plan_id'), "customer_email": data.get('customer_email'), "payment_method": "Balance", "chat_id": callback.message.chat.id, "message_id": callback.message.message_id, "promo_code": (data.get('promo_code') or '').strip(), "promo_discount": float(data.get('promo_discount', 0)), "tier_device_count": data.get('tier_device_count'), "tier_price": data.get('tier_price', 0)}
        
        await state.clear(); await process_successful_payment(bot, meta)
    # ===== Конец функции pay_with_main_balance_handler =====

    

    # ===== УПРАВЛЕНИЕ КОММЕНТАРИЯМИ К КЛЮЧУ =====
    # Инициирует процесс добавления или редактирования личного комментария для идентификации ключа
    @user_router.callback_query(F.data.startswith("key_comments_"))
    @anti_spam
    @registration_required
    async def key_comments_handler(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer()
        try: kid = int(callback.data.split("_")[2])
        except: return
        key = rw_repo.get_key_by_id(kid)
        if not key or key['user_id'] != callback.from_user.id: return await smart_edit_message(callback.message, "❌ Ключ не найден.")

        cur = key.get('comment_key')
        txt = f"<b>✏️ Комментарий к ключу #{kid}</b>\n\n" + (f"💬 Текущий: <b>{html.quote(cur)}</b>\n\n" if cur else "") + "Комментарий помогает различать ключи в общем списке. Виден только вам.\n\n💡 <i>Напр.: Телефон, Мама, Ноутбук</i>\n\n👇 <b>Введите новый текст:</b>"
        
        kb = InlineKeyboardBuilder().button(text="🗑 Удалить", callback_data=f"delete_comment_{kid}").button(text="⬅️ Назад", callback_data=f"show_key_{kid}").adjust(2).as_markup()
        msg = await smart_edit_message(callback.message, txt, kb, get_setting("key_comments_image"))
        
        await state.update_data(editing_key_id=kid, prompt_message_id=msg.message_id if msg else None, last_callback_query_id=callback.id)
        await state.set_state(KeyCommentState.waiting_for_comment)
    # ===== Конец функции key_comments_handler =====

    # ===== УДАЛЕНИЕ КОММЕНТАРИЯ =====
    # Очищает поле комментария в базе данных для указанного ключа
    @user_router.callback_query(F.data.startswith("delete_comment_"))
    @anti_spam
    @registration_required
    async def delete_key_comment_handler(callback: types.CallbackQuery, state: FSMContext, bot: Bot):
        try: kid = int(callback.data.split("_")[2])
        except: return
        key = rw_repo.get_key_by_id(kid)
        if not key or key.get('user_id') != callback.from_user.id: return await callback.answer("❌ Ключ не найден.", show_alert=True)

        rw_repo.update_key(kid, comment_key=""); await callback.answer("🗑 Удалено!"); await state.clear()
        await refresh_key_info_internal(bot=bot, chat_id=callback.message.chat.id, message_to_edit=callback.message, key_id=kid, user_id=callback.from_user.id)
    # ===== Конец функции delete_key_comment_handler =====

    # ===== ОБРАБОТКА ВВОДА КОММЕНТАРИЯ =====
    # Валидирует и сохраняет новый текст комментария, обновляя панель информации о ключе
    @user_router.message(KeyCommentState.waiting_for_comment)
    async def key_comment_input_handler(message: types.Message, state: FSMContext, bot: Bot):
        data = await state.get_data(); kid = data.get('editing_key_id')
        if not kid: return await state.clear()

        val = (message.text or "").strip()
        if not val or len(val) > 20: return await message.answer("⚠️ Текст должен быть от 1 до 20 символов.")

        rw_repo.update_key(kid, comment_key=val); prompt_id = data.get('prompt_message_id')
        await state.clear()
        try: await message.delete()
        except: pass
        
        await refresh_key_info_internal(bot=bot, chat_id=message.chat.id, message_to_edit=message, key_id=kid, user_id=message.from_user.id, prompt_message_id=prompt_id, state=state)
    # ===== Конец функции key_comment_input_handler =====

# ===== ОБРАБОТЧИКИ УНИВЕРСАЛЬНЫХ ПРОМОКОДОВ =====
    @user_router.callback_query(F.data == "promo_uni")
    async def promo_uni_handler(callback: types.CallbackQuery, state: FSMContext):
        await state.set_state(PromoUniProcess.waiting_for_promo_code)
        kb = InlineKeyboardBuilder().button(text="⬅️ Назад", callback_data="show_profile").as_markup()
        msg = await smart_edit_message(callback.message, "🎁 <b>Активация бонусного промокода</b>\n\nВведите ваш универсальный промокод:", reply_markup=kb)
        if msg: await state.update_data(promo_uni_prompt_mid=msg.message_id)
        await callback.answer()

    @user_router.message(PromoUniProcess.waiting_for_promo_code)
    async def process_uni_promo_code(message: types.Message, state: FSMContext):
        try: await message.delete()
        except: pass

        code = (message.text or '').strip().upper()
        uid = message.from_user.id
        data = await state.get_data()
        prompt_mid = data.get('promo_uni_prompt_mid')
        chat_id = message.chat.id
        bot = message.bot
        
        async def _show_result(text, clear_state=False, reply_markup=None):
            if clear_state: await state.clear()
            kb = reply_markup or InlineKeyboardBuilder().button(text="⬅️ Назад", callback_data="show_profile").as_markup()
            if prompt_mid:
                try: await bot.edit_message_text(chat_id=chat_id, message_id=prompt_mid, text=text, reply_markup=kb)
                except: await message.answer(text, reply_markup=kb)
            else:
                await message.answer(text, reply_markup=kb)

        promo, err_msg = check_promo_code_available(code, uid)
        err_map = {
            "not_found": "Этот промокод не найден или неправильно написан.",
            "not_active": "Этот код временно деактивирован.",
            "expired": "Срок действия этого промокода истёк.",
            "user_limit_reached": "Вы уже использовали этот промокод.",
            "total_limit_reached": "Лимит активаций для этого промокода исчерпан."
        }
        
        if not promo or promo.get('promo_type') not in ('universal', 'balance'):
            msg = err_map.get(err_msg, err_msg) if err_msg else "Возможно, это скидочный код для оплаты, или он уже был использован."
            await _show_result(f"🎁 <b>Активация бонусного промокода</b>\n\n❌ {msg}\n\nПопробуйте отправить другой код:", False)
            return
            
        if promo.get('promo_type') == 'balance':
            reward = int(promo.get('reward_value', 0))
            success = adjust_user_balance(uid, float(reward))
            if success:
                redeem_universal_promo(code, uid)
                await _show_result(f"✅ <b>Баланс успешно пополнен!</b>\nВам начислено {reward} ₽", True, InlineKeyboardBuilder().button(text="⬅️ Назад", callback_data="show_profile").as_markup())
            else:
                await _show_result("🎁 <b>Активация бонусного промокода</b>\n\n❌ Произошла ошибка при пополнении баланса.\n\nПопробуйте отправить другой код:", False)
            return
            
        keys = get_user_keys(uid)
        if not keys:
            kb_buy = InlineKeyboardBuilder().button(text="🛒 Купить подписку", callback_data="buy_new_key").button(text="⬅️ Назад в профиль", callback_data="show_profile").adjust(1).as_markup()
            await _show_result("🎁 <b>Активация бонусного промокода</b>\n\n❌ У вас нет активных подписок.\nПромокод на дни добавляет дни только к существующим подпискам.", True, kb_buy)
            return
            
        if len(keys) == 1:
            await state.clear()
            await _apply_uni_promo(message, uid, keys[0]['key_id'], code, promo, prompt_mid=prompt_mid)
        else:
            await state.clear()
            kb = keyboards.create_uni_promo_keys_keyboard(keys, code)
            await _show_result(" Выберите подписку (ключ), к которой нужно применить промокод:", True, kb)

    @user_router.callback_query(F.data.startswith("apply_uni_"))
    async def apply_uni_promo_callback(callback: types.CallbackQuery):
        parts = callback.data.split("_")
        code = parts[2]
        try: key_id = int(parts[3])
        except (IndexError, ValueError):
            await callback.answer("Ошибка данных", show_alert=True)
            return
            
        uid = callback.from_user.id
        promo, err_msg = check_promo_code_available(code, uid)
        err_map = {
            "not_found": "Этот промокод не найден или неправильно написан.",
            "not_active": "Этот код временно деактивирован.",
            "expired": "Срок действия этого промокода истёк.",
            "user_limit_reached": "Вы уже использовали этот промокод.",
            "total_limit_reached": "Лимит активаций для этого промокода исчерпан."
        }
        if not promo or promo.get('promo_type') not in ('universal', 'balance'):
            msg = err_map.get(err_msg, err_msg) if err_msg else "Данный промокод уже недействителен."
            await smart_edit_message(callback.message, f"❌ <b>Недействительный промокод.</b>\n{msg}", reply_markup=keyboards.create_profile_keyboard())
            await callback.answer()
            return
            
        if promo.get('promo_type') == 'balance':
            reward = int(promo.get('reward_value', 0))
            success = adjust_user_balance(uid, float(reward))
            if success:
                redeem_universal_promo(code, uid)
                await smart_edit_message(
                    callback.message,
                    f"✅ <b>Баланс успешно пополнен!</b>\nВам начислено {reward} ₽",
                    reply_markup=InlineKeyboardBuilder().button(text="⬅️ Назад", callback_data="show_profile").as_markup()
                )
            else:
                await smart_edit_message(
                    callback.message,
                    "🎁 <b>Активация бонусного промокода</b>\n\n❌ Произошла ошибка при пополнении баланса.",
                    reply_markup=InlineKeyboardBuilder().button(text="⬅️ Назад", callback_data="show_profile").as_markup()
                )
            await callback.answer()
            return
            
        await _apply_uni_promo(callback.message, uid, key_id, code, promo, is_callback=True)
        await callback.answer()
        
    async def _apply_uni_promo(msg_or_cb_message, uid: int, key_id: int, code: str, promo: dict, is_callback: bool=False, prompt_mid: int=None):
        bot = msg_or_cb_message.bot
        proc_msg = None
        
        async def _edit(text, kb=None):
            if is_callback:
                await smart_edit_message(msg_or_cb_message, text, reply_markup=kb)
            elif prompt_mid:
                try: await bot.edit_message_text(chat_id=msg_or_cb_message.chat.id, message_id=prompt_mid, text=text, reply_markup=kb)
                except: pass
            else:
                try: await proc_msg.edit_text(text, reply_markup=kb)
                except: pass

        if not is_callback and not prompt_mid:
            proc_msg = await msg_or_cb_message.answer("⏳ <b>Активация промокода...</b>")

        await _edit("⏳ <b>Активация промокода...</b>")
        
        try:
            from shop_bot.modules import remnawave_api
            from shop_bot.data_manager.remnawave_repository import redeem_universal_promo
            
            key = rw_repo.get_key_by_id(key_id)
            if not key or key['user_id'] != uid:
                await _edit("🎁 <b>Активация бонусного промокода</b>\n\n❌ Ключ не найден.", InlineKeyboardBuilder().button(text="⬅️ Назад", callback_data="show_profile").as_markup())
                return
                
            days_to_add = int(promo.get('reward_value') or 0)
            if days_to_add <= 0:
                await _edit("🎁 <b>Активация бонусного промокода</b>\n\n❌ Ошибка промокода: количество дней 0.", InlineKeyboardBuilder().button(text="⬅️ Назад", callback_data="show_profile").as_markup())
                return
                
            host = key.get('host_name')
            c_email = key.get('key_email')
            
            res = await remnawave_api.create_or_update_key_on_host(
                host_name=host,
                email=c_email,
                days_to_add=days_to_add,
                telegram_id=uid
            )
            if not res:
                await _edit("🎁 <b>Активация бонусного промокода</b>\n\n❌ Ошибка на стороне VPN-сервера.", InlineKeyboardBuilder().button(text="⬅️ Назад", callback_data="show_profile").as_markup())
                return
                
            if not rw_repo.update_key(key_id, remnawave_user_uuid=res['client_uuid'], expire_at_ms=res['expiry_timestamp_ms']):
                await _edit("🎁 <b>Активация бонусного промокода</b>\n\n❌ Ошибка обновления данных ключа локально.", InlineKeyboardBuilder().button(text="⬅️ Назад", callback_data="show_profile").as_markup())
                return
                
            redeem_res = redeem_universal_promo(code, uid)
            if not redeem_res:
                await _edit("🎁 <b>Активация бонусного промокода</b>\n\n❌ Ошибка: Вы уже использовали этот код.", InlineKeyboardBuilder().button(text="⬅️ Назад", callback_data="show_profile").as_markup())
                return
                
            success_txt = f"✅ <b>Промокод успешно активирован!</b>\n🎉 Добавлено дней: {days_to_add}\nВаша подписка продлена."
            await _edit(success_txt, InlineKeyboardBuilder().button(text="⬅️ Назад", callback_data="show_profile").as_markup())
            
        except Exception as e:
            logger.error(f"Ошибка применения uni промокода {uid}: {e}")
            await _edit("🎁 <b>Активация бонусного промокода</b>\n\n❌ Произошла ошибка при активации.", InlineKeyboardBuilder().button(text="⬅️ Назад", callback_data="show_profile").as_markup())

    return user_router

# ===== УВЕДОМЛЕНИЕ АДМИНИСТРАТОРА О ПОКУПКЕ =====
# Отправляет детальный отчет о совершенной транзакции в админ-чат
async def notify_admin_of_purchase(bot: Bot, metadata: dict):
    try:
        aid = get_setting("admin_telegram_id")
        if not aid: return
        
        user_id, host, months, price, action = metadata.get('user_id'), metadata.get('host_name'), metadata.get('months'), metadata.get('price'), metadata.get('action')
        
        user_data = get_user(user_id)
        username = user_data.get('username') if user_data else None
        username_str = f"@{username}" if username else "N/A"

        method = {'Balance': 'Баланс', 'Card': 'Карта', 'Crypto': 'Крипто', 'USDT': 'USDT', 'TON': 'TON'}.get(metadata.get('payment_method'), metadata.get('payment_method') or 'N/A')
        plan = get_plan_by_id(metadata.get('plan_id')); plan_name = plan.get('plan_name', 'N/A') if plan else 'N/A'
        
        txt = (
            "📥 <b>Новая оплата</b>\n"
            f"👤 Пользователь: <code>{user_id}</code>\n"
            f"💌 Username: {username_str}\n"
            f"🌍 Локация: <b>{host}</b>\n"
            f"📦 Тариф: {plan_name} ({months} мес.)\n"
            f"💳 Метод: {method}\n"
            f"💰 Сумма: {float(price):.2f} RUB\n"
            f"⚙️ Тип: {'Новый ключ ➕' if action == 'new' else 'Продление ♻️'}"
        )
        
        promo = (metadata.get('promo_code') or '').strip()
        if promo:
            disc = float(metadata.get('promo_applied_amount') or metadata.get('promo_discount') or 0)
            txt += f"\n🎟 Промокод: <code>{promo}</code> (-{disc:.2f} RUB)"
            
            stats = []
            if metadata.get('promo_usage_total_limit'): stats.append(f"Общий: {metadata.get('promo_usage_total_used') or 0}/{metadata.get('promo_usage_total_limit')}")
            if metadata.get('promo_usage_per_user_limit'): stats.append(f"На юзера: {metadata.get('promo_usage_per_user_used') or 0}/{metadata.get('promo_usage_per_user_limit')}")
            if stats: txt += "\n📊 " + " | ".join(stats)

        await bot.send_message(int(aid), txt, parse_mode="HTML")
    except Exception as e: logger.warning(f"Ошибка уведомления админа: {e}")
# ===== Конец функции notify_admin_of_purchase =====

# ===== ФИНАЛЬНАЯ ОБРАБОТКА УСПЕШНОГО ПЛАТЕЖА =====
# Маршрутизирует выполнение заказа: пополнение баланса, создание нового ключа или продление существующего
async def process_successful_payment(bot: Bot, metadata: dict):
    logger.info(f"💳 Обработка платежа: {metadata.get('user_id')} | {metadata.get('action')}")
    
    pay_id = metadata.get('payment_id')
    if pay_id and check_transaction_exists(pay_id):
        logger.warning(f"Повторная попытка обработки платежа {pay_id}. Операция отклонена.")
        return

    try:
        action, uid, price = metadata.get('action'), int(metadata.get('user_id')), float(metadata.get('price'))
        def _to_int(v, d=0):
            try: return int(v) if v not in (None, '', 'None', 'null') else d
            except: return d
        
        months, kid, host, plan_id, email = _to_int(metadata.get('months')), _to_int(metadata.get('key_id')), metadata.get('host_name', ''), _to_int(metadata.get('plan_id')), metadata.get('customer_email')
        pay_method = metadata.get('payment_method')
        
        if metadata.get('chat_id') and metadata.get('message_id'):
            try: await bot.delete_message(chat_id=metadata['chat_id'], message_id=metadata['message_id'])
            except: pass

        # --- ПОПОЛНЕНИЕ БАЛАНСА ---
        if action == "top_up":
            if not add_to_balance(uid, float(price)):
                logger.error(f"Ошибка баланса: Не удалось пополнить счет для {uid}")
                return
            
            user_info = get_user(uid); username = (user_info.get('username') if user_info else '') or f"@{uid}"
            log_transaction(username=username, transaction_id=None, payment_id=str(uuid.uuid4()), user_id=uid, status='paid', amount_rub=float(price), amount_currency=None, currency_name=None, payment_method=pay_method or 'Unknown', metadata=json.dumps({"action": "top_up"}))
            
            # Реферальные начисления за пополнение
            if (pay_method or '').lower() != 'balance':
                ref_id = user_info.get('referred_by')
                if ref_id:
                    # Проверка индивидуального реферального процента для seller
                    seller_ref_percent = get_seller_referral_percent(int(ref_id))
                    
                    if seller_ref_percent > 0:
                        # Используем индивидуальный процент продавца
                        reward = (Decimal(str(price)) * seller_ref_percent / 100).quantize(Decimal("0.01"))
                    else:
                        # Используем системный процент
                        rtype = (get_setting("referral_reward_type") or "percent_purchase").strip()
                        reward = Decimal("0")
                        if rtype == "fixed_purchase": reward = Decimal(get_setting("fixed_referral_bonus_amount") or "50")
                        elif rtype == "percent_purchase": reward = (Decimal(str(price)) * Decimal(get_setting("referral_percentage") or "0") / 100).quantize(Decimal("0.01"))
                    
                    if float(reward) > 0:
                        if add_to_balance(int(ref_id), float(reward)):
                            add_to_referral_balance_all(int(ref_id), float(reward))
                            try: await bot.send_message(int(ref_id), f"💰 <b>Реферальный бонус!</b>\nПользователь {username} пополнил баланс.\nВам начислено: <code>{float(reward):.2f} RUB</code>")
                            except: pass

            balance = get_balance(uid)
            await bot.send_message(uid, f"✅ <b>Баланс пополнен!</b>\nСумма: <code>{float(price):.2f} RUB</code>\nТекущий баланс: <code>{balance:.2f} RUB</code>", reply_markup=keyboards.create_profile_keyboard())
            
            admins = [u for u in (get_all_users() or []) if is_admin(u.get('telegram_id') or 0)]
            
            # Получаем чистый username для уведомления
            raw_username = user_info.get('username') if user_info else None
            username_display_str = f"@{raw_username}" if raw_username else "N/A"
            method_display = {'Balance': 'Баланс', 'Card': 'Карта', 'Crypto': 'Крипто', 'USDT': 'USDT', 'TON': 'TON'}.get(pay_method, pay_method or 'Unknown')

            for a in admins:
                try: 
                    await bot.send_message(a['telegram_id'], 
                        f"📥 <b>Пополнение баланса</b>\n"
                        f"👤 Пользователь: <code>{uid}</code>\n"
                        f"💌 Username: {username_display_str}\n"
                        f"💳 Метод: {method_display}\n"
                        f"💰 Сумма: {float(price):.2f} RUB\n"
                        f"⚙️ Тип: ➕ Баланс ‼️"
                    )
                except: pass
            return

        # --- ВЫДАЧА ИЛИ ПРОДЛЕНИЕ КЛЮЧА ---
        proc_msg = await bot.send_message(uid, f"⏳ <b>Оплата принята!</b>\nФормируем конфигурацию на сервере «{host}»...")
        
        try:
            if action == "new":
                u_data = get_user(uid) or {}; slug = re.sub(r"[^a-z0-9._-]", "_", (u_data.get('username') or f'user{uid}').lower()).strip("_")[:16] or f"user{uid}"
                cand = slug; attempt = 1
                while rw_repo.get_key_by_email(f"{cand}@bot.local") and attempt < 100:
                    cand = f"{slug}-{attempt}"; attempt += 1
                c_email = f"{cand}@bot.local"
            else:
                key = rw_repo.get_key_by_id(kid)
                if not key: return await proc_msg.edit_text("❌ Ключ для продления не найден.")
                c_email = key['key_email']

            hw_lim, tr_lim_gb, days = None, None, int(months * 30)
            tier_dc = metadata.get('tier_device_count')
            if tier_dc:
                hw_lim = int(tier_dc)
            if plan_id:
                plan = get_plan_by_id(plan_id)
                if plan:
                    if not tier_dc:
                        hw_lim = int(plan.get('hwid_limit', 0))
                    tr_lim_gb = int(plan.get('traffic_limit_gb', 0))
                    if plan.get('duration_days'): days = int(plan['duration_days'])

            # Получаем внешний сквад для seller (если пользователь - seller)
            external_squad = get_seller_external_squad(uid)
            
            res = await remnawave_api.create_or_update_key_on_host(
                host_name=host, 
                email=c_email, 
                days_to_add=days, 
                telegram_id=uid, 
                hwid_limit=hw_lim, 
                traffic_limit_gb=tr_lim_gb,
                external_squad_uuid=external_squad
            )
            if not res: return await proc_msg.edit_text("❌ Ошибка на стороне VPN-сервера. Обратитесь в поддержку.")

            if action == "new":
                kid = rw_repo.record_key_from_payload(user_id=uid, payload=res, host_name=host)
                if not kid: return await proc_msg.edit_text("❌ Ошибка сохранения данных в БД.")
            else:
                if not rw_repo.update_key(kid, remnawave_user_uuid=res['client_uuid'], expire_at_ms=res['expiry_timestamp_ms']): return await proc_msg.edit_text("❌ Ошибка обновления данных ключа.")

            # Реферальные начисления за покупку
            if (pay_method or '').lower() != 'balance':
                u_data = get_user(uid) or {}; ref_id = u_data.get('referred_by')
                if ref_id:
                    # Проверка индивидуального реферального процента для seller
                    seller_ref_percent = get_seller_referral_percent(int(ref_id))
                    
                    if seller_ref_percent > 0:
                        # Используем индивидуальный процент продавца
                        reward = (Decimal(str(price)) * seller_ref_percent / 100).quantize(Decimal("0.01"))
                    else:
                        # Используем системный процент
                        rtype = (get_setting("referral_reward_type") or "percent_purchase").strip()
                        reward = Decimal("0")
                        if rtype == "fixed_purchase": reward = Decimal(get_setting("fixed_referral_bonus_amount") or "50")
                        elif rtype == "percent_purchase": reward = (Decimal(str(price)) * Decimal(get_setting("referral_percentage") or "0") / 100).quantize(Decimal("0.01"))
                    
                    if float(reward) > 0:
                        if add_to_balance(int(ref_id), float(reward)):
                            add_to_referral_balance_all(int(ref_id), float(reward))
                            try: await bot.send_message(int(ref_id), f"💰 <b>Реферальный бонус!</b>\nВаш реферал совершил покупку.\nВам начислено: <code>{float(reward):.2f} RUB</code>")
                            except: pass

            update_user_stats(uid, (0.0 if (pay_method or '').lower() == 'balance' else price), months)
            
            p_log_id = metadata.get('payment_id') or str(uuid.uuid4())
            # Подготовка метаданных для истории
            tx_meta = {"plan_id": plan_id, "host": host, "host_name": host}
            if plan_id:
                p_obj = get_plan_by_id(plan_id)
                if p_obj:
                    tx_meta['plan_name'] = p_obj.get('plan_name')
            
            log_transaction(username=(get_user(uid) or {}).get('username', 'N/A'), transaction_id=None, payment_id=p_log_id, user_id=uid, status='paid', amount_rub=float(price), amount_currency=None, currency_name=None, payment_method=pay_method or 'Unknown', metadata=json.dumps(tx_meta))
            
            # Промокоды
            promo_val = (metadata.get('promo_code') or '').strip()
            if promo_val:
                try: 
                    p_info = redeem_promo_code(promo_val, uid, applied_amount=float(metadata.get('promo_discount') or 0), order_id=p_log_id)
                    if p_info and p_info.get('usage_limit_total') and (p_info.get('used_total') or 0) >= p_info['usage_limit_total']:
                        update_promo_code_status(promo_val, is_active=False)
                except: pass
            
            await proc_msg.delete()
            msk_tz = timezone(timedelta(hours=3))
            conn, exp = res.get('connection_string'), datetime.fromtimestamp(res['expiry_timestamp_ms'] / 1000, tz=msk_tz)
            u_keys = get_user_keys(uid); k_num = next((i + 1 for i, k in enumerate(u_keys) if k['key_id'] == kid), len(u_keys))
            txt = get_purchase_success_text(action=("extend" if action == "extend" else "new"), key_number=k_num, expiry_date=exp, connection_string=(conn or ""), email=c_email)
            
            ready_img = get_setting("key_ready_image")
            if ready_img and os.path.exists(ready_img):
                from aiogram.types import FSInputFile
                await bot.send_photo(chat_id=uid, photo=FSInputFile(ready_img), caption=txt, reply_markup=keyboards.create_dynamic_key_info_keyboard(kid, conn), parse_mode="HTML")
            else: await bot.send_message(chat_id=uid, text=txt, reply_markup=keyboards.create_dynamic_key_info_keyboard(kid, conn), parse_mode="HTML")

            try: await notify_admin_of_purchase(bot, metadata)
            except: pass
        except Exception as e:
            logger.error(f"Ошибка логики VPN ({uid}): {e}", exc_info=True)
            await bot.send_message(uid, "❌ <b>Ошибка при выдаче ключа</b>\nВаша оплата зафиксирована, но произошел сбой при создании конфигурации. Свяжитесь с поддержкой.")
    except Exception as e: logger.error(f"Глобальная ошибка обработки платежа: {e}", exc_info=True)
# ===== Конец функции process_successful_payment =====