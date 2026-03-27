import logging
import asyncio
import time
import uuid
import re
import html as html_escape
import hashlib
import json
from datetime import datetime, timedelta

from aiogram import Bot, Router, F, types
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.keyboard import InlineKeyboardBuilder

from shop_bot.bot import keyboards
from shop_bot.data_manager import speedtest_runner
from shop_bot.data_manager import resource_monitor
from shop_bot.data_manager import remnawave_repository as rw_repo
from shop_bot.data_manager.remnawave_repository import (
    get_all_users,
    get_setting,
    get_user,
    get_keys_for_user,
    create_gift_key,
    get_all_hosts,
    get_all_ssh_targets,
    add_to_balance,
    deduct_from_balance,
    ban_user,
    unban_user,
    delete_key_by_email,
    get_admin_stats,
    get_keys_for_host,
    is_admin,
    get_referral_count,
    get_referral_balance_all,
    get_referrals_for_user,
    create_promo_code,
    list_promo_codes,
    update_promo_code_status,
)
from shop_bot.config import get_msk_time
from shop_bot.data_manager.database import (
    update_key_email,
    set_referral_balance,
    set_referral_balance_all,
)
from shop_bot.data_manager import backup_manager
from shop_bot.bot.handlers import show_main_menu, smart_edit_message
from shop_bot.modules.remnawave_api import create_or_update_key_on_host, delete_client_on_host

logger = logging.getLogger(__name__)

class Broadcast(StatesGroup):
    waiting_for_message = State()
    waiting_for_button_option = State()
    waiting_for_button_text = State()
    waiting_for_button_url = State()
    waiting_for_confirmation = State()

class AdminUserSearch(StatesGroup):
    waiting_for_query = State()
    waiting_for_pick_query = State()


def get_admin_router() -> Router:
    admin_router = Router()


    def _format_user_mention(u: types.User) -> str:
        try:
            if u.username:
                uname = u.username.lstrip('@')
                return f"@{uname}"

            full_name = (u.full_name or u.first_name or "Администратор").strip()

            try:
                safe_name = html_escape.escape(full_name)
            except Exception:
                safe_name = full_name
            return f"<a href='tg://user?id={u.id}'>{safe_name}</a>"
        except Exception:
            return str(getattr(u, 'id', '—'))


    def _resolve_target_from_hash(cb_data: str) -> str | None:
        try:
            digest = cb_data.split(':', 1)[1]
        except Exception:
            return None
        try:
            targets = get_all_ssh_targets() or []
        except Exception:
            targets = []
        for t in targets:
            name = t.get('target_name')
            try:
                h = hashlib.sha1((name or '').encode('utf-8', 'ignore')).hexdigest()
            except Exception:
                h = hashlib.sha1(str(name).encode('utf-8', 'ignore')).hexdigest()
            if h == digest:
                return name
        return None

    async def show_admin_menu(message: types.Message, edit_message: bool = False):

        stats = get_admin_stats() or {}
        today_new = stats.get('today_new_users', 0)
        today_income = float(stats.get('today_income', 0) or 0)
        today_keys = stats.get('today_issued_keys', 0)
        total_users = stats.get('total_users', 0)
        total_income = float(stats.get('total_income', 0) or 0)
        total_keys = stats.get('total_keys', 0)
        active_keys = stats.get('active_keys', 0)

        text = (
            "📊 <b>Панель Администратора</b>\n\n"
            "<b>За сегодня:</b>\n"
            f"👥 Новых пользователей: {today_new}\n"
            f"💰 Доход: {today_income:.2f} RUB\n"
            f"🔑 Выдано ключей: {today_keys}\n\n"
            "<b>За все время:</b>\n"
            f"👥 Всего пользователей: {total_users}\n"
            f"💰 Общий доход: {total_income:.2f} RUB\n"
            f"🔑 Всего ключей: {total_keys}\n\n"
            "<b>Состояние ключей:</b>\n"
            f"✅ Активных: {active_keys}"
        )

        try:
            keyboard = keyboards.create_dynamic_admin_menu_keyboard()
        except Exception as e:
            logger.warning(f"Не удалось создать динамическую админ-клавиатуру, используем статическую: {e}")
            keyboard = keyboards.create_admin_menu_keyboard()

        if edit_message:
            await smart_edit_message(message, text, keyboard)
        else:
            await smart_edit_message(message, text, keyboard)

    async def show_admin_promo_menu(message: types.Message, edit_message: bool = False):
        text = (
            "🎟 <b>Управление промокодами</b>\n\n"
            "Здесь можно создавать новые промокоды, просматривать список и отключать их."
        )
        keyboard = keyboards.create_admin_promo_menu_keyboard()
        if edit_message:
            try:
                await message.edit_text(text, reply_markup=keyboard)
            except Exception:
                await message.answer(text, reply_markup=keyboard)
        else:
            await message.answer(text, reply_markup=keyboard)

    def _parse_datetime_input(raw: str) -> datetime | None:
        value = (raw or "").strip()
        if not value or value.lower() in {"skip", "нет", "не", "none"}:
            return None
        for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                return datetime.strptime(value, fmt)
            except Exception:
                continue
        raise ValueError("Неверный формат даты. Используйте 'ГГГГ-ММ-ДД' или 'ГГГГ-ММ-ДД ЧЧ:ММ'.")

    def _format_promo_line(promo: dict) -> str:
        code = promo.get("code") or "—"
        discount_percent = promo.get("discount_percent")
        discount_amount = promo.get("discount_amount")
        try:
            if discount_percent:
                discount_text = f"{float(discount_percent):.2f}%"
            else:
                discount_text = f"{float(discount_amount or 0):.2f} RUB"
        except Exception:
            discount_text = str(discount_percent or discount_amount or "—")

        status_parts: list[str] = []
        is_active = bool(promo.get("is_active"))
        status_parts.append("🟢 активен" if is_active else "🔴 отключён")

        try:
            usage_limit_total = int(promo.get("usage_limit_total") or 0)
        except Exception:
            usage_limit_total = 0
        used_total = int(promo.get("used_total") or 0)
        if usage_limit_total:
            status_parts.append(f"{used_total}/{usage_limit_total}")
            if used_total >= usage_limit_total:
                status_parts.append("лимит исчерпан")

        try:
            usage_limit_per_user = int(promo.get("usage_limit_per_user") or 0)
        except Exception:
            usage_limit_per_user = 0
        if usage_limit_per_user:
            status_parts.append(f"пользователь ≤ {usage_limit_per_user}")

        valid_until = promo.get("valid_until")
        if valid_until:
            status_parts.append(f"до {str(valid_until)[:16]}")

        status_text = ", ".join(status_parts)
        return f"• <code>{code}</code> — скидка: {discount_text} | статус: {status_text}"

    def _build_promo_list_keyboard(codes: list[dict], page: int = 0, page_size: int = 10) -> types.InlineKeyboardMarkup:
        builder = InlineKeyboardBuilder()
        total = len(codes)
        start = page * page_size
        end = start + page_size
        page_items = codes[start:end]
        if not page_items:
            builder.button(text="Промокодов нет", callback_data="noop")
        for promo in page_items:
            code = promo.get("code") or "—"
            is_active = bool(promo.get("is_active"))
            label = f"{'🟢' if is_active else '🔴'} {code}"
            builder.button(text=label, callback_data=f"admin_promo_toggle_{code}")
        have_prev = start > 0
        have_next = end < total
        if have_prev:
            builder.button(text="⬅️ Назад", callback_data=f"admin_promo_page_{page-1}")
        if have_next:
            builder.button(text="Вперёд ➡️", callback_data=f"admin_promo_page_{page+1}")
        builder.button(text="⬅️ В меню", callback_data="admin_promo_menu")
        rows = [1] * len(page_items)
        tail: list[int] = []
        if have_prev or have_next:
            tail.append(2 if (have_prev and have_next) else 1)
        tail.append(1)
        builder.adjust(*(rows + tail if rows else tail))
        return builder.as_markup()

    @admin_router.callback_query(F.data == "admin_menu")
    async def open_admin_menu_handler(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        await show_admin_menu(callback.message, edit_message=True)


    class AdminPromoCreate(StatesGroup):
        waiting_for_promo_type = State()
        waiting_for_code = State()
        waiting_for_discount_type = State()
        waiting_for_discount_value = State()
        waiting_for_reward_value = State()
        waiting_for_total_limit = State()
        waiting_for_per_user_limit = State()
        waiting_for_valid_from = State()
        waiting_for_valid_until = State()
        waiting_for_description = State()
        confirming = State()

    @admin_router.callback_query(F.data == "admin_promo_menu")
    async def admin_promo_menu_handler(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        await state.clear()
        await show_admin_promo_menu(callback.message, edit_message=True)

    @admin_router.callback_query(F.data == "admin_promo_create")
    async def admin_promo_create_start(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        await state.clear()
        await state.set_state(AdminPromoCreate.waiting_for_promo_type)
        await callback.message.edit_text(
            "🔐 Создание промокода\n\nВыберите тип промокода:",
            reply_markup=keyboards.create_admin_promo_type_keyboard()
        )

    @admin_router.callback_query(
        AdminPromoCreate.waiting_for_promo_type,
        F.data.in_(["admin_promo_type_discount", "admin_promo_type_days", "admin_promo_type_balance"])
    )
    async def admin_promo_type_selected(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        type_mapping = {
            "admin_promo_type_discount": "discount",
            "admin_promo_type_days": "universal",
            "admin_promo_type_balance": "balance"
        }
        ptype = type_mapping[callback.data]
        await state.update_data(promo_type=ptype)
        await state.set_state(AdminPromoCreate.waiting_for_code)
        
        type_names = {"discount": "Скидка", "universal": "Дни", "balance": "Баланс"}
        await callback.message.edit_text(
            f"🔐 Создание промокода (<b>{type_names[ptype]}</b>)\n\nВыберите способ указания кода:",
            reply_markup=keyboards.create_admin_promo_code_keyboard(),
            parse_mode='HTML'
        )

    @admin_router.callback_query(
        AdminPromoCreate.waiting_for_code,
        F.data == "admin_promo_code_auto"
    )
    async def admin_promo_code_auto(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        code = uuid.uuid4().hex[:8].upper()
        
        data = await state.get_data()
        ptype = data.get("promo_type", "discount")
        await state.update_data(promo_code=code)
        
        if ptype == "discount":
            await state.set_state(AdminPromoCreate.waiting_for_discount_type)
            text = f"Код: <code>{code}</code>\n\nВыберите тип скидки:"
            markup = keyboards.create_admin_promo_discount_keyboard()
        elif ptype == "universal":
            await state.set_state(AdminPromoCreate.waiting_for_reward_value)
            text = f"Код: <code>{code}</code>\n\nВведите количество дней начисления:"
            markup = keyboards.create_admin_cancel_keyboard()
        else:
            await state.set_state(AdminPromoCreate.waiting_for_reward_value)
            text = f"Код: <code>{code}</code>\n\nВведите сумму пополнения баланса (RUB):"
            markup = keyboards.create_admin_cancel_keyboard()
            
        try:
            await callback.message.edit_text(text, reply_markup=markup, parse_mode='HTML')
        except Exception:
            await callback.message.answer(text, reply_markup=markup, parse_mode='HTML')

    @admin_router.callback_query(
        AdminPromoCreate.waiting_for_code,
        F.data == "admin_promo_code_custom"
    )
    async def admin_promo_code_custom(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        await callback.message.edit_text(
            "Введите желаемый код (только латиница/цифры) или напишите <b>авто</b> для генерации:",
            reply_markup=keyboards.create_admin_cancel_keyboard(),
            parse_mode='HTML'
        )

    @admin_router.message(AdminPromoCreate.waiting_for_code)
    async def admin_promo_create_code(message: types.Message, state: FSMContext):
        if not is_admin(message.from_user.id):
            return
        raw = (message.text or '').strip()
        if not raw:
            await message.answer("❌ Введите код или напишите 'авто'.")
            return
        code = uuid.uuid4().hex[:8].upper() if raw.lower() == 'авто' or raw.lower() == 'auto' else raw.strip().upper()
        if not re.fullmatch(r"[A-Z0-9_-]{3,32}", code):
            await message.answer("❌ Код должен состоять из латиницы/цифр и быть длиной 3-32 символа.")
            return

        data = await state.get_data()
        ptype = data.get("promo_type", "discount")
        await state.update_data(promo_code=code)
        
        if ptype == "discount":
            await state.set_state(AdminPromoCreate.waiting_for_discount_type)
            text = "Выберите тип скидки:"
            markup = keyboards.create_admin_promo_discount_keyboard()
        elif ptype == "universal":
            await state.set_state(AdminPromoCreate.waiting_for_reward_value)
            text = "Введите количество дней начисления:"
            markup = keyboards.create_admin_cancel_keyboard()
        else:
            await state.set_state(AdminPromoCreate.waiting_for_reward_value)
            text = "Введите сумму пополнения баланса (RUB):"
            markup = keyboards.create_admin_cancel_keyboard()
            
        await message.answer(text, reply_markup=markup)

    @admin_router.message(AdminPromoCreate.waiting_for_reward_value)
    async def admin_promo_set_reward_value(message: types.Message, state: FSMContext):
        if not is_admin(message.from_user.id):
            return
        raw = (message.text or '').strip()
        try:
            value = int(raw)
        except Exception:
            await message.answer("❌ Введите целое число.")
            return
        if value <= 0:
            await message.answer("❌ Значение должно быть положительным.")
            return
        await state.update_data(reward_value=value)
        await state.set_state(AdminPromoCreate.waiting_for_total_limit)
        await message.answer(
            "Введите общий лимит активаций или выберите на кнопках:",
            reply_markup=keyboards.create_admin_promo_limit_keyboard("total")
        )

    @admin_router.callback_query(
        AdminPromoCreate.waiting_for_discount_type,
        F.data.in_({"admin_promo_discount_percent", "admin_promo_discount_amount"})
    )
    async def admin_promo_set_discount_type(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        discount_type = 'percent' if callback.data.endswith('percent') else 'amount'
        await state.update_data(discount_type=discount_type)
        await state.set_state(AdminPromoCreate.waiting_for_discount_value)
        prompt = "Введите процент скидки (например, 10.5):" if discount_type == 'percent' else "Введите размер скидки в RUB (например, 150):"
        await callback.message.edit_text(prompt, reply_markup=keyboards.create_admin_cancel_keyboard())

    @admin_router.message(AdminPromoCreate.waiting_for_discount_value)
    async def admin_promo_set_discount_value(message: types.Message, state: FSMContext):
        if not is_admin(message.from_user.id):
            return
        data = await state.get_data()
        discount_type = data.get('discount_type')
        raw = (message.text or '').strip().replace(',', '.')
        try:
            value = float(raw)
        except Exception:
            await message.answer("❌ Введите число.")
            return
        if value <= 0:
            await message.answer("❌ Значение должно быть положительным.")
            return
        if discount_type == 'percent' and value >= 100:
            await message.answer("❌ Процент скидки должен быть меньше 100.")
            return
        await state.update_data(discount_value=value)
        await state.set_state(AdminPromoCreate.waiting_for_total_limit)
        await message.answer(
            "Введите общий лимит активаций или выберите на кнопках:",
            reply_markup=keyboards.create_admin_promo_limit_keyboard("total")
        )

    @admin_router.message(AdminPromoCreate.waiting_for_total_limit)
    async def admin_promo_set_total_limit(message: types.Message, state: FSMContext):
        if not is_admin(message.from_user.id):
            return
        raw = (message.text or '').strip().lower()
        limit_total: int | None
        if raw in {'0', '∞', 'inf', 'infinity', 'безлимит', 'нет'} or not raw:
            limit_total = None
        else:
            try:
                limit_total = int(raw)
            except Exception:
                await message.answer("❌ Введите целое число или 0 для безлимита.")
                return
            if limit_total <= 0:
                limit_total = None
        await state.update_data(usage_limit_total=limit_total)
        await state.set_state(AdminPromoCreate.waiting_for_per_user_limit)
        await message.answer(
            "Введите лимит на пользователя или выберите на кнопках:",
            reply_markup=keyboards.create_admin_promo_limit_keyboard("user")
        )

    @admin_router.callback_query(
        AdminPromoCreate.waiting_for_total_limit,
        F.data.startswith("admin_promo_limit_total_")
    )
    async def admin_promo_total_limit_buttons(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.")
            return
        await callback.answer()
        tail = callback.data.replace("admin_promo_limit_total_", "", 1)
        if tail == "custom":
            await callback.message.edit_text(
                "Введите общий лимит активаций (целое число) или 0/∞ для безлимита:",
                reply_markup=keyboards.create_admin_cancel_keyboard()
            )
            return
        limit_total = None if tail == "inf" else int(tail)
        await state.update_data(usage_limit_total=limit_total)
        await state.set_state(AdminPromoCreate.waiting_for_per_user_limit)
        await callback.message.edit_text(
            "Введите лимит на пользователя или выберите на кнопках:",
            reply_markup=keyboards.create_admin_promo_limit_keyboard("user")
        )

    @admin_router.callback_query(
        AdminPromoCreate.waiting_for_per_user_limit,
        F.data.startswith("admin_promo_limit_user_")
    )
    async def admin_promo_user_limit_buttons(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.")
            return
        await callback.answer()
        tail = callback.data.replace("admin_promo_limit_user_", "", 1)
        if tail == "custom":
            await callback.message.edit_text(
                "Введите лимит на пользователя (целое число) или 0/∞ для безлимита:",
                reply_markup=keyboards.create_admin_cancel_keyboard()
            )
            return
        limit_user = None if tail == "inf" else int(tail)
        await state.update_data(usage_limit_per_user=limit_user)
        await state.set_state(AdminPromoCreate.waiting_for_valid_from)
        await callback.message.edit_text(
            "Укажите дату начала действия или выберите на кнопках:",
            reply_markup=keyboards.create_admin_promo_valid_from_keyboard()
        )

    @admin_router.message(AdminPromoCreate.waiting_for_per_user_limit)
    async def admin_promo_set_per_user_limit(message: types.Message, state: FSMContext):
        if not is_admin(message.from_user.id):
            return
        raw = (message.text or '').strip().lower()
        limit_user: int | None
        if raw in {'0', '∞', 'inf', 'infinity', 'безлимит', 'нет'} or not raw:
            limit_user = None
        else:
            try:
                limit_user = int(raw)
            except Exception:
                await message.answer("❌ Введите целое число или 0 для безлимита.")
                return
            if limit_user <= 0:
                limit_user = None
        await state.update_data(usage_limit_per_user=limit_user)
        await state.set_state(AdminPromoCreate.waiting_for_valid_from)
        await message.answer(
            "Укажите дату начала действия (ГГГГ-ММ-ДД или ГГГГ-ММ-ДД ЧЧ:ММ). Напишите 'skip', чтобы пропустить:",
            reply_markup=keyboards.create_admin_cancel_keyboard()
        )

    @admin_router.message(AdminPromoCreate.waiting_for_valid_from)
    async def admin_promo_set_valid_from(message: types.Message, state: FSMContext):
        if not is_admin(message.from_user.id):
            return
        raw = (message.text or '').strip()
        try:
            valid_from = _parse_datetime_input(raw)
        except ValueError as e:
            await message.answer(f"❌ {e}")
            return
        await state.update_data(valid_from=valid_from)
        await state.set_state(AdminPromoCreate.waiting_for_valid_until)
        await message.answer(
            "Укажите дату окончания действия или выберите на кнопках:",
            reply_markup=keyboards.create_admin_promo_valid_until_keyboard()
        )

    @admin_router.callback_query(
        AdminPromoCreate.waiting_for_valid_from,
        F.data.in_({
            "admin_promo_valid_from_now",
            "admin_promo_valid_from_today",
            "admin_promo_valid_from_tomorrow",
            "admin_promo_valid_from_skip",
            "admin_promo_valid_from_custom",
        })
    )
    async def admin_promo_valid_from_buttons(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.")
            return
        await callback.answer()
        now = get_msk_time().replace(tzinfo=None)
        if callback.data.endswith("custom"):
            await callback.message.edit_text(
                "Укажите дату начала (ГГГГ-ММ-ДД или ГГГГ-ММ-ДД ЧЧ:ММ):",
                reply_markup=keyboards.create_admin_cancel_keyboard()
            )
            return
        if callback.data.endswith("skip"):
            valid_from = None
        elif callback.data.endswith("today"):
            valid_from = datetime(now.year, now.month, now.day)
        elif callback.data.endswith("tomorrow"):
            valid_from = datetime(now.year, now.month, now.day) + timedelta(days=1)
        else:
            valid_from = now
        await state.update_data(valid_from=valid_from)
        await state.set_state(AdminPromoCreate.waiting_for_valid_until)
        await callback.message.edit_text(
            "Укажите дату окончания действия или выберите на кнопках:",
            reply_markup=keyboards.create_admin_promo_valid_until_keyboard()
        )

    @admin_router.message(AdminPromoCreate.waiting_for_valid_until)
    async def admin_promo_set_valid_until(message: types.Message, state: FSMContext):
        if not is_admin(message.from_user.id):
            return
        raw = (message.text or '').strip()
        try:
            valid_until = _parse_datetime_input(raw)
        except ValueError as e:
            await message.answer(f"❌ {e}")
            return
        data = await state.get_data()
        valid_from = data.get('valid_from')
        if valid_from and valid_until and valid_until <= valid_from:
            await message.answer("❌ Дата окончания должна быть позже даты начала.")
            return
        await state.update_data(valid_until=valid_until)
        await state.set_state(AdminPromoCreate.waiting_for_description)
        await message.answer(
            "Добавьте описание/комментарий или пропустите:",
            reply_markup=keyboards.create_admin_promo_description_keyboard()
        )

    @admin_router.callback_query(
        AdminPromoCreate.waiting_for_valid_until,
        F.data.in_({
            "admin_promo_valid_until_plus1d",
            "admin_promo_valid_until_plus7d",
            "admin_promo_valid_until_plus30d",
            "admin_promo_valid_until_skip",
            "admin_promo_valid_until_custom",
        })
    )
    async def admin_promo_valid_until_buttons(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.")
            return
        await callback.answer()
        if callback.data.endswith("custom"):
            await callback.message.edit_text(
                "Укажите дату окончания (ГГГГ-ММ-ДД или ГГГГ-ММ-ДД ЧЧ:ММ):",
                reply_markup=keyboards.create_admin_cancel_keyboard()
            )
            return
        if callback.data.endswith("skip"):
            valid_until = None
        else:
            data = await state.get_data()
            base = data.get('valid_from') or get_msk_time().replace(tzinfo=None)
            if callback.data.endswith("plus1d"):
                valid_until = base + timedelta(days=1)
            elif callback.data.endswith("plus7d"):
                valid_until = base + timedelta(days=7)
            else:
                valid_until = base + timedelta(days=30)
        await state.update_data(valid_until=valid_until)
        await state.set_state(AdminPromoCreate.waiting_for_description)
        await callback.message.edit_text(
            "Добавьте описание/комментарий или пропустите:",
            reply_markup=keyboards.create_admin_promo_description_keyboard()
        )

    @admin_router.message(AdminPromoCreate.waiting_for_description)
    async def admin_promo_description(message: types.Message, state: FSMContext):
        if not is_admin(message.from_user.id):
            return
        desc = (message.text or '').strip()
        description = None if not desc or desc.lower() in {'skip', 'пропустить', 'нет'} else desc
        await state.update_data(description=description)
        data = await state.get_data()
        code = data.get('promo_code')
        discount_type = data.get('discount_type')
        discount_value = data.get('discount_value')
        total_limit = data.get('usage_limit_total')
        per_user_limit = data.get('usage_limit_per_user')
        valid_from = data.get('valid_from')
        valid_until = data.get('valid_until')
        summary_lines = [
            "Проверьте данные промокода:",
            f"Код: <code>{code}</code>",
            f"Тип скидки: {'процент' if discount_type == 'percent' else 'фиксированная'}",
            f"Значение: {discount_value:.2f}{'%' if discount_type == 'percent' else ' RUB'}",
            f"Лимит всего: {total_limit if total_limit is not None else 'без ограничений'}",
            f"Лимит на пользователя: {per_user_limit if per_user_limit is not None else 'без ограничений'}",
            f"Действует с: {valid_from.isoformat(' ') if valid_from else '—'}",
            f"Действует до: {valid_until.isoformat(' ') if valid_until else '—'}",
            f"Описание: {description or '—'}",
        ]
        summary_text = "\n".join(summary_lines)
        builder = InlineKeyboardBuilder()
        builder.button(text="✅ Создать", callback_data="admin_promo_confirm")
        builder.button(text="❌ Отмена", callback_data="admin_cancel")
        builder.adjust(1, 1)
        await state.set_state(AdminPromoCreate.confirming)
        await message.answer(summary_text, reply_markup=builder.as_markup(), parse_mode='HTML')

    @admin_router.callback_query(
        AdminPromoCreate.waiting_for_description,
        F.data.in_({"admin_promo_desc_skip", "admin_promo_desc_custom"})
    )
    async def admin_promo_desc_buttons(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.")
            return
        await callback.answer()
        if callback.data.endswith("custom"):
            await callback.message.edit_text(
                "Введите описание промокода (опционально) или нажмите Отмена:",
                reply_markup=keyboards.create_admin_cancel_keyboard()
            )
            return

        await state.update_data(description=None)
        data = await state.get_data()
        code = data.get('promo_code')
        ptype = data.get('promo_type', 'discount')
        reward_value = data.get('reward_value', 0)
        total_limit = data.get('usage_limit_total')
        per_user_limit = data.get('usage_limit_per_user')
        valid_from = data.get('valid_from')
        valid_until = data.get('valid_until')
        
        summary_lines = [
            "Проверьте данные промокода:",
            f"Код: <code>{code}</code>",
        ]
        
        if ptype == "discount":
            discount_type = data.get('discount_type')
            discount_value = data.get('discount_value')
            summary_lines.extend([
                f"Тип скидки: {'процент' if discount_type == 'percent' else 'фиксированная'}",
                f"Значение: {discount_value:.2f}{'%' if discount_type == 'percent' else ' RUB'}"
            ])
        elif ptype == "universal":
            summary_lines.append(f"Тип: Дни ({reward_value} дн.)")
        elif ptype == "balance":
            summary_lines.append(f"Тип: Баланс ({reward_value} RUB)")

        summary_lines.extend([
            f"Лимит всего: {total_limit if total_limit is not None else 'без ограничений'}",
            f"Лимит на пользователя: {per_user_limit if per_user_limit is not None else 'без ограничений'}",
            f"Действует с: {valid_from.isoformat(' ') if valid_from else '—'}",
            f"Действует до: {valid_until.isoformat(' ') if valid_until else '—'}",
            f"Описание: —",
        ])
        summary_text = "\n".join(summary_lines)
        builder = InlineKeyboardBuilder()
        builder.button(text="✅ Создать", callback_data="admin_promo_confirm")
        builder.button(text="❌ Отмена", callback_data="admin_cancel")
        builder.adjust(1, 1)
        await state.set_state(AdminPromoCreate.confirming)
        await callback.message.edit_text(summary_text, reply_markup=builder.as_markup(), parse_mode='HTML')

    @admin_router.callback_query(AdminPromoCreate.confirming, F.data == "admin_promo_confirm")
    async def admin_promo_confirm(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        data = await state.get_data()
        code = data.get('promo_code')
        ptype = data.get('promo_type', 'discount')
        reward_value = data.get('reward_value', 0)
        discount_type = data.get('discount_type')
        discount_value = data.get('discount_value')
        total_limit = data.get('usage_limit_total')
        per_user_limit = data.get('usage_limit_per_user')
        valid_from = data.get('valid_from')
        valid_until = data.get('valid_until')
        description = data.get('description')
        kwargs = {
            'code': code,
            'promo_type': ptype,
            'reward_value': reward_value,
            'discount_percent': discount_value if ptype == 'discount' and discount_type == 'percent' else None,
            'discount_amount': discount_value if ptype == 'discount' and discount_type == 'amount' else None,
            'usage_limit_total': total_limit,
            'usage_limit_per_user': per_user_limit,
            'valid_from': valid_from,
            'valid_until': valid_until,
            'created_by': callback.from_user.id,
            'description': description,
        }
        try:
            ok = create_promo_code(**kwargs)
        except ValueError as e:
            await callback.message.edit_text(f"❌ Не удалось создать промокод: {e}", reply_markup=keyboards.create_admin_promo_menu_keyboard())
            await state.clear()
            return
        if not ok:
            await callback.message.edit_text(
                "❌ Не удалось создать промокод (возможно, код уже существует).",
                reply_markup=keyboards.create_admin_promo_menu_keyboard()
            )
            await state.clear()
            return
        await state.clear()
        await callback.message.edit_text(
            f"✅ Промокод <code>{code}</code> создан!\n\nПередайте его пользователю или опубликуйте в канале.",
            reply_markup=keyboards.create_admin_promo_menu_keyboard(),
            parse_mode='HTML'
        )

    @admin_router.callback_query(F.data == "admin_promo_list")
    async def admin_promo_list(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        await state.update_data(promo_page=0)
        codes = list_promo_codes(include_inactive=True) or []
        text_lines = ["🎟 <b>Доступные промокоды</b>"]
        if not codes:
            text_lines.append("Пока нет созданных промокодов.")
        else:
            for promo in codes[:10]:
                text_lines.append(_format_promo_line(promo))
        await callback.message.edit_text(
            "\n".join(text_lines),
            reply_markup=_build_promo_list_keyboard(codes, page=0),
            parse_mode='HTML'
        )

    @admin_router.callback_query(F.data.startswith("admin_promo_page_"))
    async def admin_promo_change_page(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.")
            return
        await callback.answer()
        try:
            page = int(callback.data.split('_')[-1])
        except Exception:
            page = 0
        codes = list_promo_codes(include_inactive=True) or []
        await state.update_data(promo_page=page)
        text_lines = ["🎟 <b>Доступные промокоды</b>"]
        if not codes:
            text_lines.append("Пока нет созданных промокодов.")
        else:
            start = page * 10
            for promo in codes[start:start + 10]:
                text_lines.append(_format_promo_line(promo))
        await callback.message.edit_text(
            "\n".join(text_lines),
            reply_markup=_build_promo_list_keyboard(codes, page=page),
            parse_mode='HTML'
        )

    @admin_router.callback_query(F.data.startswith("admin_promo_toggle_"))
    async def admin_promo_toggle(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.")
            return
        code = callback.data.split("admin_promo_toggle_")[-1]
        codes = list_promo_codes(include_inactive=True) or []
        target = next((p for p in codes if (p.get('code') or '').upper() == code.upper()), None)
        if not target:
            await callback.answer("Промокод не найден", show_alert=True)
            return
        new_status = not bool(target.get('is_active'))
        update_promo_code_status(code, is_active=new_status)
        await callback.answer("Статус обновлён")
        page = (await state.get_data()).get('promo_page', 0)
        codes = list_promo_codes(include_inactive=True) or []
        text_lines = ["🎟 <b>Доступные промокоды</b>"]
        if not codes:
            text_lines.append("Пока нет созданных промокодов.")
        else:
            start = page * 10
            for promo in codes[start:start + 10]:
                text_lines.append(_format_promo_line(promo))
        await callback.message.edit_text(
            "\n".join(text_lines),
            reply_markup=_build_promo_list_keyboard(codes, page=page),
            parse_mode='HTML'
        )


    @admin_router.callback_query(F.data == "admin_speedtest")
    async def admin_speedtest_entry(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()

        targets = get_all_ssh_targets() or []
        try:
            await callback.message.edit_text(
                "🔌 <b>SSH цели для Speedtest</b>\nВыберите цель:",
                reply_markup=keyboards.create_admin_ssh_targets_keyboard(targets)
            )
        except Exception:
            await callback.message.answer(
                "🔌 <b>SSH цели для Speedtest</b>\nВыберите цель:",
                reply_markup=keyboards.create_admin_ssh_targets_keyboard(targets)
            )


    @admin_router.callback_query(F.data == "admin_speedtest_ssh_targets")
    async def admin_speedtest_ssh_targets(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        targets = get_all_ssh_targets() or []
        try:
            await callback.message.edit_text(
                "🔌 <b>SSH цели для Speedtest</b>\nВыберите цель:",
                reply_markup=keyboards.create_admin_ssh_targets_keyboard(targets)
            )
        except Exception:
            await callback.message.answer(
                "🔌 <b>SSH цели для Speedtest</b>\nВыберите цель:",
                reply_markup=keyboards.create_admin_ssh_targets_keyboard(targets)
            )


    @admin_router.callback_query(F.data.startswith("admin_speedtest_pick_host_"))
    async def admin_speedtest_run(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        host_name = callback.data.replace("admin_speedtest_pick_host_", "", 1)


        try:
            from shop_bot.data_manager.remnawave_repository import get_admin_ids
            admin_ids = list({*(get_admin_ids() or []), int(callback.from_user.id)})
        except Exception:
            admin_ids = [int(callback.from_user.id)]
        initiator = _format_user_mention(callback.from_user)
        start_text = f"🚀 Запущен тест скорости для хоста: <b>{host_name}</b>\n(инициатор: {initiator})"
        for aid in admin_ids:
            try:
                await callback.bot.send_message(aid, start_text)
            except Exception:
                pass


        try:
            wait_msg = await callback.message.answer(f"⏳ Выполняю тест скорости для <b>{host_name}</b>…")
        except Exception:
            wait_msg = None


        try:
            result = await speedtest_runner.run_both_for_host(host_name)
        except Exception as e:
            result = {"ok": False, "error": str(e), "details": {}}


        def fmt_part(title: str, d: dict | None) -> str:
            if not d:
                return f"<b>{title}:</b> —"
            if not d.get("ok"):
                return f"<b>{title}:</b> ❌ {d.get('error') or 'ошибка'}"
            ping = d.get('ping_ms')
            down = d.get('download_mbps')
            up = d.get('upload_mbps')
            srv = d.get('server_name') or '—'
            return (f"<b>{title}:</b> ✅\n"
                    f"• ping: {ping if ping is not None else '—'} ms\n"
                    f"• ↓ {down if down is not None else '—'} Mbps\n"
                    f"• ↑ {up if up is not None else '—'} Mbps\n"
                    f"• сервер: {srv}")

        details = result.get('details') or {}
        text_res = (
            f"🏁 Тест скорости завершён для <b>{host_name}</b>\n\n"
            + fmt_part("SSH", details.get('ssh')) + "\n\n"
            + fmt_part("NET", details.get('net'))
        )



        if result.get('ok'):
            logger.info(f"Bot/Admin: спидтест для SSH-цели '{host_name}' завершён успешно")
        else:
            logger.warning(f"Bot/Admin: спидтест для SSH-цели '{host_name}' завершился с ошибкой: {result.get('error')}")


        if result.get('ok'):
            logger.info(f"Bot/Admin: спидтест (legacy) для SSH-цели '{host_name}' завершён успешно")
        else:
            logger.warning(f"Bot/Admin: спидтест (legacy) для SSH-цели '{host_name}' завершился с ошибкой: {result.get('error')}")

        if wait_msg:
            try:
                await wait_msg.edit_text(text_res)
            except Exception:
                await callback.message.answer(text_res)
        else:
            await callback.message.answer(text_res)


        for aid in admin_ids:
            if wait_msg and aid == callback.from_user.id:
                continue
            try:
                await callback.bot.send_message(aid, text_res)
            except Exception:
                pass


    @admin_router.callback_query(F.data.startswith("stt:"))
    async def admin_speedtest_run_target_hashed(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        target_name = _resolve_target_from_hash(callback.data)
        if not target_name:
            await callback.message.answer("❌ Цель не найдена")
            return


        logger.info(f"Bot/Admin: запуск спидтеста для SSH-цели '{target_name}' (инициатор id={callback.from_user.id})")
        try:
            from shop_bot.data_manager.remnawave_repository import get_admin_ids
            admin_ids = list({*(get_admin_ids() or []), int(callback.from_user.id)})
        except Exception:
            admin_ids = [int(callback.from_user.id)]
        initiator = _format_user_mention(callback.from_user)
        start_text = f"🚀 Запущен тест скорости (SSH-цель): <b>{target_name}</b>\n(инициатор: {initiator})"
        for aid in admin_ids:
            try:
                await callback.bot.send_message(aid, start_text)
            except Exception:
                pass


        try:
            wait_msg = await callback.message.answer(f"⏳ Выполняю тест скорости для SSH-цели <b>{target_name}</b>…")
        except Exception:
            wait_msg = None


        try:
            result = await speedtest_runner.run_and_store_ssh_speedtest_for_target(target_name)
        except Exception as e:
            result = {"ok": False, "error": str(e)}

        if not result.get("ok"):
            text_res = f"🏁 Тест скорости (SSH-цель) завершён для <b>{target_name}</b>\n❌ {result.get('error') or 'ошибка'}"
        else:
            ping = result.get('ping_ms')
            down = result.get('download_mbps')
            up = result.get('upload_mbps')
            srv = result.get('server_name') or '—'
            text_res = (
                f"🏁 Тест скорости (SSH-цель) завершён для <b>{target_name}</b>\n\n"
                f"<b>SSH:</b> ✅\n"
                f"• ping: {ping if ping is not None else '—'} ms\n"
                f"• ↓ {down if down is not None else '—'} Mbps\n"
                f"• ↑ {up if up is not None else '—'} Mbps\n"
                f"• сервер: {srv}"
            )

        if wait_msg:
            try:
                await wait_msg.edit_text(text_res)
            except Exception:
                await callback.message.answer(text_res)
        else:
            await callback.message.answer(text_res)

        for aid in admin_ids:
            if wait_msg and aid == callback.from_user.id:
                continue
            try:
                await callback.bot.send_message(aid, text_res)
            except Exception:
                pass


    @admin_router.callback_query(F.data.startswith("admin_speedtest_pick_target_"))
    async def admin_speedtest_run_target(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        target_name = callback.data.replace("admin_speedtest_pick_target_", "", 1)


        logger.info(f"Bot/Admin: запуск спидтеста (legacy) для SSH-цели '{target_name}' (инициатор id={callback.from_user.id})")
        try:
            from shop_bot.data_manager.remnawave_repository import get_admin_ids
            admin_ids = list({*(get_admin_ids() or []), int(callback.from_user.id)})
        except Exception:
            admin_ids = [int(callback.from_user.id)]
        initiator = _format_user_mention(callback.from_user)
        start_text = f"🚀 Запущен тест скорости (SSH-цель): <b>{target_name}</b>\n(инициатор: {initiator})"
        for aid in admin_ids:
            try:
                await callback.bot.send_message(aid, start_text)
            except Exception:
                pass


        try:
            wait_msg = await callback.message.answer(f"⏳ Выполняю тест скорости для SSH-цели <b>{target_name}</b>…")
        except Exception:
            wait_msg = None


        try:
            result = await speedtest_runner.run_and_store_ssh_speedtest_for_target(target_name)
        except Exception as e:
            result = {"ok": False, "error": str(e)}


        if not result.get("ok"):
            text_res = f"🏁 Тест скорости (SSH-цель) завершён для <b>{target_name}</b>\n❌ {result.get('error') or 'ошибка'}"
        else:
            ping = result.get('ping_ms')
            down = result.get('download_mbps')
            up = result.get('upload_mbps')
            srv = result.get('server_name') or '—'
            text_res = (
                f"🏁 Тест скорости (SSH-цель) завершён для <b>{target_name}</b>\n\n"
                f"<b>SSH:</b> ✅\n"
                f"• ping: {ping if ping is not None else '—'} ms\n"
                f"• ↓ {down if down is not None else '—'} Mbps\n"
                f"• ↑ {up if up is not None else '—'} Mbps\n"
                f"• сервер: {srv}"
            )


        if wait_msg:
            try:
                await wait_msg.edit_text(text_res)
            except Exception:
                await callback.message.answer(text_res)
        else:
            await callback.message.answer(text_res)


        for aid in admin_ids:
            if wait_msg and aid == callback.from_user.id:
                continue
            try:
                await callback.bot.send_message(aid, text_res)
            except Exception:
                pass


    @admin_router.callback_query(F.data == "admin_speedtest_back_to_users")
    async def admin_speedtest_back(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        await show_admin_menu(callback.message, edit_message=True)


    @admin_router.callback_query(F.data == "admin_speedtest_run_all")
    async def admin_speedtest_run_all(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()

        try:
            from shop_bot.data_manager.remnawave_repository import get_admin_ids
            admin_ids = list({*(get_admin_ids() or []), int(callback.from_user.id)})
        except Exception:
            admin_ids = [int(callback.from_user.id)]
        initiator = _format_user_mention(callback.from_user)
        start_text = f"🚀 Запущен тест скорости для всех хостов\n(инициатор: {initiator})"
        for aid in admin_ids:
            try:
                await callback.bot.send_message(aid, start_text)
            except Exception:
                pass

        hosts = get_all_hosts() or []
        summary_lines = []
        for h in hosts:
            name = h.get('host_name')
            try:
                res = await speedtest_runner.run_both_for_host(name)
                ok = res.get('ok')
                det = res.get('details') or {}
                dm = det.get('ssh', {}).get('download_mbps') or det.get('net', {}).get('download_mbps')
                um = det.get('ssh', {}).get('upload_mbps') or det.get('net', {}).get('upload_mbps')
                summary_lines.append(f"• {name}: {'✅' if ok else '❌'} ↓ {dm or '—'} ↑ {um or '—'}")
            except Exception as e:
                summary_lines.append(f"• {name}: ❌ {e}")
        text = "🏁 Тест для всех завершён:\n" + "\n".join(summary_lines)
        await callback.message.answer(text)
        for aid in admin_ids:

            if aid == callback.from_user.id or aid == callback.message.chat.id:
                continue
            try:
                await callback.bot.send_message(aid, text)
            except Exception:
                pass


    @admin_router.callback_query(F.data == "admin_speedtest_run_all_targets")
    async def admin_speedtest_run_all_targets(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()

        try:
            from shop_bot.data_manager.remnawave_repository import get_admin_ids
            admin_ids = list({*(get_admin_ids() or []), int(callback.from_user.id)})
        except Exception:
            admin_ids = [int(callback.from_user.id)]
        initiator = _format_user_mention(callback.from_user)
        start_text = f"🚀 Запущен тест скорости для всех SSH-целей\n(инициатор: {initiator})"
        logger.info(f"Bot/Admin: запуск спидтеста ДЛЯ ВСЕХ SSH-целей (инициатор id={callback.from_user.id})")
        for aid in admin_ids:
            try:
                await callback.bot.send_message(aid, start_text)
            except Exception:
                pass

        targets = get_all_ssh_targets() or []
        summary_lines = []
        ok_total = 0
        for t in targets:
            name = (t.get('target_name') or '').strip()
            if not name:
                continue
            try:
                res = await speedtest_runner.run_and_store_ssh_speedtest_for_target(name)
                ok = bool(res.get('ok'))
                dm = res.get('download_mbps')
                um = res.get('upload_mbps')
                summary_lines.append(f"• {name}: {'✅' if ok else '❌'} ↓ {dm or '—'} ↑ {um or '—'}")
                if ok:
                    ok_total += 1
            except Exception as e:
                summary_lines.append(f"• {name}: ❌ {e}")
        text = "🏁 SSH-цели: тест для всех завершён:\n" + ("\n".join(summary_lines) if summary_lines else "(нет целей)")
        logger.info(f"Bot/Admin: завершён спидтест ДЛЯ ВСЕХ SSH-целей: ок={ok_total}, всего={len(targets)}")
        await callback.message.answer(text)
        for aid in admin_ids:
            if aid == callback.from_user.id or aid == callback.message.chat.id:
                continue
            try:
                await callback.bot.send_message(aid, text)
            except Exception:
                pass


    @admin_router.callback_query(F.data == "admin_backup_db")
    async def admin_backup_db(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        try:
            wait = await callback.message.answer("⏳ Создаю бэкап базы данных…")
        except Exception:
            wait = None
        zip_path = backup_manager.create_backup_file()
        if not zip_path:
            if wait:
                await wait.edit_text("❌ Не удалось создать бэкап БД")
            else:
                await callback.message.answer("❌ Не удалось создать бэкап БД")
            return

        try:
            sent = await backup_manager.send_backup_to_admins(callback.bot, zip_path)
        except Exception:
            sent = 0
        txt = f"✅ Бэкап создан: <b>{zip_path.name}</b>\nОтправлено администраторам: {sent}"
        if wait:
            try:
                await wait.edit_text(txt)
            except Exception:
                await callback.message.answer(txt)
        else:
            await callback.message.answer(txt)


    class AdminRestoreDB(StatesGroup):
        waiting_file = State()

    @admin_router.callback_query(F.data == "admin_restore_db")
    async def admin_restore_db_prompt(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        await state.set_state(AdminRestoreDB.waiting_file)
        kb = InlineKeyboardBuilder()
        kb.button(text="❌ Отмена", callback_data="admin_cancel")
        kb.adjust(1)
        text = (
            "⚠️ <b>Восстановление базы данных</b>\n\n"
            "Отправьте файл <code>.zip</code> с бэкапом или файл <code>.db</code> в ответ на это сообщение.\n"
            "Текущая БД предварительно будет сохранена."
        )
        try:
            await callback.message.edit_text(text, reply_markup=kb.as_markup())
        except Exception:
            await callback.message.answer(text, reply_markup=kb.as_markup())

    @admin_router.message(AdminRestoreDB.waiting_file)
    async def admin_restore_db_receive(message: types.Message, state: FSMContext):
        if not is_admin(message.from_user.id):
            return
        doc = message.document
        if not doc:
            await message.answer("❌ Пришлите файл .zip или .db")
            return
        filename = (doc.file_name or "uploaded.db").lower()
        if not (filename.endswith('.zip') or filename.endswith('.db')):
            await message.answer("❌ Поддерживаются только файлы .zip или .db")
            return
        try:
            ts = get_msk_time().strftime('%Y%m%d-%H%M%S')
            dest = backup_manager.BACKUPS_DIR / f"uploaded-{ts}-{filename}"
            dest.parent.mkdir(parents=True, exist_ok=True)
            await message.bot.download(doc, destination=dest)
        except Exception as e:
            await message.answer(f"❌ Не удалось скачать файл: {e}")
            return
        ok = backup_manager.restore_from_file(dest)
        await state.clear()
        if ok:
            await message.answer("✅ Восстановление выполнено успешно.\nБот и панель продолжают работу с новой БД.")
        else:
            await message.answer("❌ Восстановление не удалось. Проверьте файл и повторите.")


    @admin_router.callback_query(F.data.startswith("admin_speedtest_autoinstall_"))
    async def admin_speedtest_autoinstall(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        host_name = callback.data.replace("admin_speedtest_autoinstall_", "", 1)
        try:
            wait = await callback.message.answer(f"🛠 Пытаюсь установить speedtest на <b>{host_name}</b>…")
        except Exception:
            wait = None
        from shop_bot.data_manager.speedtest_runner import auto_install_speedtest_on_host
        try:
            res = await auto_install_speedtest_on_host(host_name)
        except Exception as e:
            res = {"ok": False, "log": f"Ошибка: {e}"}
        text = ("✅ Автоустановка завершена успешно" if res.get("ok") else "❌ Автоустановка завершилась с ошибкой")
        text += f"\n<pre>{(res.get('log') or '')[:3500]}</pre>"
        if wait:
            try:
                await wait.edit_text(text)
            except Exception:
                await callback.message.answer(text)


    @admin_router.callback_query(F.data.startswith("admin_speedtest_autoinstall_target_"))
    async def admin_speedtest_autoinstall_target(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        target_name = callback.data.replace("admin_speedtest_autoinstall_target_", "", 1)
        try:
            wait = await callback.message.answer(f"🛠 Пытаюсь установить speedtest на SSH-цели <b>{target_name}</b>…")
        except Exception:
            wait = None
        from shop_bot.data_manager.speedtest_runner import auto_install_speedtest_on_target
        logger.info(f"Bot/Admin: автоустановка speedtest на SSH-цели '{target_name}' (инициатор id={callback.from_user.id})")
        try:
            res = await auto_install_speedtest_on_target(target_name)
        except Exception as e:
            res = {"ok": False, "log": f"Ошибка: {e}"}
        text = ("✅ Автоустановка завершена успешно" if res.get("ok") else "❌ Автоустановка завершилась с ошибкой")
        text += f"\n<pre>{(res.get('log') or '')[:3500]}</pre>"
        if res.get('ok'):
            logger.info(f"Bot/Admin: автоустановка завершена успешно для '{target_name}'")
        else:
            logger.warning(f"Bot/Admin: автоустановка завершилась с ошибкой для '{target_name}'")
        if wait:
            try:
                await wait.edit_text(text)
            except Exception:
                await callback.message.answer(text)
        else:
            await callback.message.answer(text)


    @admin_router.callback_query(F.data.startswith("stti:"))
    async def admin_speedtest_autoinstall_target_hashed(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        target_name = _resolve_target_from_hash(callback.data)
        if not target_name:
            await callback.message.answer("❌ Цель не найдена")
            return
        try:
            wait = await callback.message.answer(f"🛠 Пытаюсь установить speedtest на SSH-цели <b>{target_name}</b>…")
        except Exception:
            wait = None
        from shop_bot.data_manager.speedtest_runner import auto_install_speedtest_on_target
        try:
            res = await auto_install_speedtest_on_target(target_name)
        except Exception as e:
            res = {"ok": False, "log": f"Ошибка: {e}"}
        text = ("✅ Автоустановка завершена успешно" if res.get("ok") else "❌ Автоустановка завершилась с ошибкой")
        text += f"\n<pre>{(res.get('log') or '')[:3500]}</pre>"
        if wait:
            try:
                await wait.edit_text(text)
            except Exception:
                await callback.message.answer(text)
        else:
            await callback.message.answer(text)



    @admin_router.callback_query(F.data.startswith("admin_users"))
    async def admin_users_handler(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        users = get_all_users()
        page = 0
        if callback.data.startswith("admin_users_page_"):
            try:
                page = int(callback.data.split("_")[-1])
            except Exception:
                page = 0
        await callback.message.edit_text(
            "👥 <b>Пользователи</b>",
            reply_markup=keyboards.create_admin_users_keyboard(users, page=page)
        )

    @admin_router.callback_query(F.data == "admin_search_user")
    async def admin_search_user_start(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        await state.set_state(AdminUserSearch.waiting_for_query)
        kb = InlineKeyboardBuilder()
        kb.button(text="⬅️ Отмена", callback_data="admin_users")
        await callback.message.edit_text(
            "Введите ID пользователя или юзернейм (например, @durov):",
            reply_markup=kb.as_markup()
        )

    @admin_router.message(AdminUserSearch.waiting_for_query)
    async def admin_search_user_process(message: types.Message, state: FSMContext):
        if not is_admin(message.from_user.id):
            return
        
        query = message.text.strip()
        user_info = None
        
        from shop_bot.data_manager.remnawave_repository import get_user_by_username
        
        if query.startswith("@"):
            uname = query[1:]
            user_info = get_user_by_username(uname)
        else:
            try:
                user_id = int(query)
                user_info = get_user(user_id)
            except ValueError:
                pass
            
            if not user_info:
                user_info = get_user_by_username(query)
                
        await state.clear()
        
        if not user_info:
            kb = InlineKeyboardBuilder()
            kb.button(text="⬅️ Назад", callback_data="admin_users")
            await message.answer("❌ Пользователь не найден.", reply_markup=kb.as_markup())
            return
            
        user_id = user_info.get('telegram_id') or user_info.get('user_id') or user_info.get('id')
        
        if user_info.get('username'):
            uname = user_info.get('username').lstrip('@')
            user_tag = f"<a href='https://t.me/{uname}'>@{uname}</a>"
        else:
            user_tag = f"<a href='tg://user?id={user_id}'>Профиль</a>"
            
        is_banned = user_info.get('is_banned', False)
        total_spent = user_info.get('total_spent', 0)
        balance = user_info.get('balance', 0)
        referred_by = user_info.get('referred_by')
        keys = get_keys_for_user(user_id)
        keys_count = len(keys)
        text = (
            f"👤 <b>Пользователь {user_id}</b>\n\n"
            f"Имя пользователя: {user_tag}\n"
            f"Всего потратил: {float(total_spent):.2f} RUB\n"
            f"Баланс: {float(balance):.2f} RUB\n"
            f"Забанен: {'да' if is_banned else 'нет'}\n"
            f"Приглашён: {referred_by if referred_by else '—'}\n"
            f"Ключей: {keys_count}"
        )
        await message.answer(
            text,
            reply_markup=keyboards.create_admin_user_actions_keyboard(user_id, is_banned=is_banned)
        )

    @admin_router.callback_query(F.data.startswith("admin_search_pick_user_"))
    async def admin_search_pick_user_start(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        action = callback.data.replace("admin_search_pick_user_", "")
        await state.update_data(search_pick_action=action)
        await state.set_state(AdminUserSearch.waiting_for_pick_query)
        kb = InlineKeyboardBuilder()
        kb.button(text="⬅️ Отмена", callback_data="admin_menu")
        await callback.message.edit_text(
            "Введите ID пользователя или юзернейм (например, @durov):",
            reply_markup=kb.as_markup()
        )

    @admin_router.message(AdminUserSearch.waiting_for_pick_query)
    async def admin_search_pick_process(message: types.Message, state: FSMContext):
        if not is_admin(message.from_user.id):
            return
        
        data = await state.get_data()
        action = data.get("search_pick_action")
        
        query = message.text.strip()
        user_info = None
        
        from shop_bot.data_manager.remnawave_repository import get_user_by_username
        
        if query.startswith("@"):
            uname = query[1:]
            user_info = get_user_by_username(uname)
        else:
            try:
                user_id = int(query)
                user_info = get_user(user_id)
            except ValueError:
                pass
            
            if not user_info:
                user_info = get_user_by_username(query)
                
        if not user_info:
            kb = InlineKeyboardBuilder()
            kb.button(text="⬅️ В меню", callback_data="admin_menu")
            await message.answer("❌ Пользователь не найден.", reply_markup=kb.as_markup())
            return
            
        user_id = user_info.get('telegram_id') or user_info.get('user_id') or user_info.get('id')
        username = user_info.get('username') or '—'
        title = f"{user_id} • @{username}" if username != '—' else f"{user_id}"
        builder = InlineKeyboardBuilder()
        builder.button(text=f"✅ Выбрать {title}", callback_data=f"admin_{action}_pick_user_{user_id}")
        builder.button(text="⬅️ Отмена", callback_data="admin_menu")
        builder.adjust(1)
        
        await message.answer(f"Пользователь найден. Продолжить действие?", reply_markup=builder.as_markup())

    @admin_router.callback_query(F.data.startswith("admin_view_user_"))
    async def admin_view_user_handler(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        try:
            user_id = int(callback.data.split("_")[-1])
        except Exception:
            await callback.message.answer("❌ Неверный формат user_id")
            return
        user = get_user(user_id)
        if not user:
            await callback.message.answer("❌ Пользователь не найден")
            return

        username = user.get('username') or '—'

        if user.get('username'):
            uname = user.get('username').lstrip('@')
            user_tag = f"<a href='https://t.me/{uname}'>@{uname}</a>"
        else:
            user_tag = f"<a href='tg://user?id={user_id}'>Профиль</a>"
        is_banned = user.get('is_banned', False)
        total_spent = user.get('total_spent', 0)
        balance = user.get('balance', 0)
        referred_by = user.get('referred_by')
        keys = get_keys_for_user(user_id)
        keys_count = len(keys)
        text = (
            f"👤 <b>Пользователь {user_id}</b>\n\n"
            f"Имя пользователя: {user_tag}\n"
            f"Всего потратил: {float(total_spent):.2f} RUB\n"
            f"Баланс: {float(balance):.2f} RUB\n"
            f"Забанен: {'да' if is_banned else 'нет'}\n"
            f"Приглашён: {referred_by if referred_by else '—'}\n"
            f"Ключей: {keys_count}"
        )
        await callback.message.edit_text(
            text,
            reply_markup=keyboards.create_admin_user_actions_keyboard(user_id, is_banned=is_banned)
        )


    @admin_router.callback_query(F.data.startswith("admin_ban_user_"))
    async def admin_ban_user(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        try:
            user_id = int(callback.data.split("_")[-1])
        except Exception:
            await callback.message.answer("❌ Неверный формат user_id")
            return
        try:
            ban_user(user_id)
            await callback.message.answer(f"🚫 Пользователь {user_id} забанен")
            try:

                from shop_bot.data_manager.remnawave_repository import get_setting as _get_setting
                support = (_get_setting("support_bot_username") or _get_setting("support_user") or "").strip()
                kb = InlineKeyboardBuilder()
                url = None
                if support:
                    if support.startswith("@"):
                        url = f"tg://resolve?domain={support[1:]}"
                    elif support.startswith("tg://"):
                        url = support
                    elif support.startswith("http://") or support.startswith("https://"):
                        try:
                            part = support.split("/")[-1].split("?")[0]
                            if part:
                                url = f"tg://resolve?domain={part}"
                        except Exception:
                            url = support
                    else:
                        url = f"tg://resolve?domain={support}"
                if url:
                    kb.button(text="🆘 Написать в поддержку", url=url)
                else:
                    kb.button(text="🆘 Поддержка", callback_data="show_help")
                await callback.bot.send_message(
                    user_id,
                    "🚫 Ваш аккаунт заблокирован администратором. Если это ошибка — напишите в поддержку.",
                    reply_markup=kb.as_markup()
                )
            except Exception:
                pass
        except Exception as e:
            await callback.message.answer(f"❌ Не удалось забанить пользователя: {e}")
            return

        user = get_user(user_id) or {}
        username = user.get('username') or '—'
        if user.get('username'):
            uname = user.get('username').lstrip('@')
            user_tag = f"<a href='https://t.me/{uname}'>@{uname}</a>"
        else:
            user_tag = f"<a href='tg://user?id={user_id}'>Профиль</a>"
        total_spent = user.get('total_spent', 0)
        balance = user.get('balance', 0)
        referred_by = user.get('referred_by')
        keys = get_keys_for_user(user_id)
        keys_count = len(keys)
        text = (
            f"👤 <b>Пользователь {user_id}</b>\n\n"
            f"Имя пользователя: {user_tag}\n"
            f"Всего потратил: {float(total_spent):.2f} RUB\n"
            f"Баланс: {float(balance):.2f} RUB\n"
            f"Забанен: да\n"
            f"Приглашён: {referred_by if referred_by else '—'}\n"
            f"Ключей: {keys_count}"
        )
        try:
            await callback.message.edit_text(
                text,
                reply_markup=keyboards.create_admin_user_actions_keyboard(user_id, is_banned=True)
            )
        except Exception:
            pass


    @admin_router.callback_query(F.data == "admin_toggle_stealth_login")
    async def admin_toggle_stealth_login(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        
        try:
            from shop_bot.data_manager.database import update_setting
            current_val = get_setting("stealth_login_enabled") or "0"
            new_val = "0" if current_val == "1" else "1"
            update_setting("stealth_login_enabled", new_val)
            
            await callback.message.edit_text(
                "👮 <b>Управление администраторами</b>",
                reply_markup=keyboards.create_admins_menu_keyboard()
            )
        except Exception as e:
            logger.error(f"Error toggling stealth login: {e}")

    @admin_router.callback_query(F.data == "admin_admins_menu")
    async def admin_admins_menu_entry(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        await callback.message.edit_text(
            "👮 <b>Управление администраторами</b>",
            reply_markup=keyboards.create_admins_menu_keyboard()
        )

    @admin_router.callback_query(F.data == "admin_view_admins")
    async def admin_view_admins(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        try:
            from shop_bot.data_manager.remnawave_repository import get_admin_ids
            ids = list(get_admin_ids() or [])
        except Exception:
            ids = []
        if not ids:
            text = "📋 Список администраторов пуст."
        else:
            lines = []
            for aid in ids:
                try:
                    u = get_user(int(aid)) or {}
                except Exception:
                    u = {}
                uname = (u.get('username') or '').strip()
                if uname:
                    uname_clean = uname.lstrip('@')
                    tag = f"<a href='https://t.me/{uname_clean}'>@{uname_clean}</a>"
                else:
                    tag = f"<a href='tg://user?id={aid}'>Профиль</a>"
                lines.append(f"• ID: {aid} — {tag}")
            text = "📋 <b>Администраторы</b>:\n" + "\n".join(lines)

        kb = InlineKeyboardBuilder()
        kb.button(text="⬅️ Назад", callback_data="admin_admins_menu")
        kb.button(text="⬅️ В админ-меню", callback_data="admin_menu")
        kb.adjust(1, 1)
        try:
            await callback.message.edit_text(text, reply_markup=kb.as_markup())
        except Exception:
            await callback.message.answer(text, reply_markup=kb.as_markup())

    @admin_router.callback_query(F.data.startswith("admin_unban_user_"))
    async def admin_unban_user(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        try:
            user_id = int(callback.data.split("_")[-1])
        except Exception:
            await callback.message.answer("❌ Неверный формат user_id")
            return
        try:
            unban_user(user_id)
            await callback.message.answer(f"✅ Пользователь {user_id} разбанен")
            try:

                kb = InlineKeyboardBuilder()
                kb.row(keyboards.get_main_menu_button())
                await callback.bot.send_message(
                    user_id,
                    "✅ Доступ к аккаунту восстановлен администратором.",
                    reply_markup=kb.as_markup()
                )
            except Exception:
                pass
        except Exception as e:
            await callback.message.answer(f"❌ Не удалось разбанить пользователя: {e}")
            return

        user = get_user(user_id) or {}
        username = user.get('username') or '—'

        if user.get('username'):
            uname = user.get('username').lstrip('@')
            user_tag = f"<a href='https://t.me/{uname}'>@{uname}</a>"
        else:
            user_tag = f"<a href='tg://user?id={user_id}'>Профиль</a>"
        total_spent = user.get('total_spent', 0)
        balance = user.get('balance', 0)
        referred_by = user.get('referred_by')
        keys = get_keys_for_user(user_id)
        keys_count = len(keys)
        text = (
            f"👤 <b>Пользователь {user_id}</b>\n\n"
            f"Имя пользователя: {user_tag}\n"
            f"Всего потратил: {float(total_spent):.2f} RUB\n"
            f"Баланс: {float(balance):.2f} RUB\n"
            f"Забанен: нет\n"
            f"Приглашён: {referred_by if referred_by else '—'}\n"
            f"Ключей: {keys_count}"
        )
        try:
            await callback.message.edit_text(
                text,
                reply_markup=keyboards.create_admin_user_actions_keyboard(user_id, is_banned=False)
            )
        except Exception:
            pass


    @admin_router.callback_query(F.data.startswith("admin_user_keys_"))
    async def admin_user_keys(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        try:
            user_id = int(callback.data.split("_")[-1])
        except Exception:
            await callback.message.answer("❌ Неверный формат user_id")
            return
        keys = get_keys_for_user(user_id)
        await callback.message.edit_text(
            f"🔑 Ключи пользователя {user_id}:",
            reply_markup=keyboards.create_admin_user_keys_keyboard(user_id, keys)
        )

    @admin_router.callback_query(F.data.startswith("admin_user_referrals_"))
    async def admin_user_referrals(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        try:
            user_id = int(callback.data.split("_")[-1])
        except Exception:
            await callback.message.answer("❌ Неверный формат user_id")
            return
        inviter = get_user(user_id)
        if not inviter:
            await callback.message.answer("❌ Пользователь не найден")
            return
        refs = get_referrals_for_user(user_id) or []
        ref_count = len(refs)
        try:
            total_ref_earned = float(get_referral_balance_all(user_id) or 0)
        except Exception:
            total_ref_earned = 0.0

        max_items = 30
        lines = []
        for r in refs[:max_items]:
            rid = r.get('telegram_id')
            uname = r.get('username') or '—'
            rdate = r.get('registration_date') or '—'
            spent = float(r.get('total_spent') or 0)
            lines.append(f"• @{uname} (ID: {rid}) — рег: {rdate}, потратил: {spent:.2f} RUB")
        more_suffix = "\n… и ещё {}".format(ref_count - max_items) if ref_count > max_items else ""
        text = (
            f"🤝 <b>Рефералы пользователя {user_id}</b>\n\n"
            f"Всего приглашено: {ref_count}\n"
            f"Заработано по рефералке (всего): {total_ref_earned:.2f} RUB\n\n"
            + ("\n".join(lines) if lines else "Пока нет рефералов")
            + more_suffix
        )

        kb = InlineKeyboardBuilder()
        kb.button(text="⬅️ К пользователю", callback_data=f"admin_view_user_{user_id}")
        kb.button(text="⬅️ В админ-меню", callback_data="admin_menu")
        kb.adjust(1, 1)
        try:
            await callback.message.edit_text(text, reply_markup=kb.as_markup())
        except Exception:
            await callback.message.answer(text, reply_markup=kb.as_markup())

    @admin_router.callback_query(F.data.startswith("admin_edit_key_"))
    async def admin_edit_key(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        try:
            key_id = int(callback.data.split("_")[-1])
        except Exception:
            await callback.message.answer("❌ Неверный формат key_id")
            return
        key = rw_repo.get_key_by_id(key_id)
        if not key:
            await callback.message.answer("❌ Ключ не найден")
            return
        text = (
            f"🔑 <b>Ключ #{key_id}</b>\n"
            f"Хост: {key.get('host_name') or '—'}\n"
            f"Email: {key.get('key_email') or '—'}\n"
            f"Истекает: {key.get('expiry_date') or '—'}\n"
        )
        try:
            await callback.message.edit_text(
                text,
                reply_markup=keyboards.create_admin_key_actions_keyboard(key_id, int(key.get('user_id')) if key and key.get('user_id') else None)
            )
        except Exception as e:
            logger.debug(f"edit_text не удался в отмене удаления для ключа #{key_id}: {e}")
            await callback.message.answer(
                text,
                reply_markup=keyboards.create_admin_key_actions_keyboard(key_id, int(key.get('user_id')) if key and key.get('user_id') else None)
            )



    @admin_router.callback_query(F.data.regexp(r"^admin_key_delete_\d+$"))
    async def admin_key_delete_prompt(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        logger.info(f"Получен запрос на удаление ключа: data='{callback.data}' от {callback.from_user.id}")
        try:
            key_id = int(callback.data.split("_")[-1])
        except Exception:
            await callback.message.answer("❌ Неверный формат key_id")
            return
        key = rw_repo.get_key_by_id(key_id)
        if not key:
            await callback.message.answer("❌ Ключ не найден")
            return
        email = key.get('key_email') or '—'
        host = key.get('host_name') or '—'
        try:
            await callback.message.edit_text(
                f"Вы уверены, что хотите удалить ключ #{key_id}?\nEmail: {email}\nСервер: {host}",
                reply_markup=keyboards.create_admin_delete_key_confirm_keyboard(key_id)
            )
        except Exception as e:
            logger.debug(f"edit_text не удался в запросе удаления для ключа #{key_id}: {e}")
            await callback.message.answer(
                f"Вы уверены, что хотите удалить ключ #{key_id}?\nEmail: {email}\nСервер: {host}",
                reply_markup=keyboards.create_admin_delete_key_confirm_keyboard(key_id)
            )


    class AdminExtendSingleKey(StatesGroup):
        waiting_days = State()

    @admin_router.callback_query(F.data.startswith("admin_key_extend_"))
    async def admin_key_extend_prompt(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        try:
            key_id = int(callback.data.split("_")[-1])
        except Exception:
            await callback.message.answer("❌ Неверный формат key_id")
            return
        await state.update_data(extend_key_id=key_id)
        await state.set_state(AdminExtendSingleKey.waiting_days)
        await callback.message.edit_text(
            f"Укажите, на сколько дней продлить ключ #{key_id} (число):",
            reply_markup=keyboards.create_admin_cancel_keyboard()
        )

    @admin_router.message(AdminExtendSingleKey.waiting_days)
    async def admin_key_extend_process(message: types.Message, state: FSMContext):
        if not is_admin(message.from_user.id):
            return
        data = await state.get_data()
        key_id = int(data.get("extend_key_id", 0))
        if not key_id:
            await state.clear()
            await message.answer("❌ Не удалось определить ключ.")
            return
        try:
            days = int((message.text or '').strip())
        except Exception:
            await message.answer("❌ Введите число дней")
            return
        if days <= 0:
            await message.answer("❌ Дней должно быть положительное число")
            return
        key = rw_repo.get_key_by_id(key_id)
        if not key:
            await message.answer("❌ Ключ не найден")
            await state.clear()
            return
        host = key.get('host_name')
        email = key.get('key_email')
        if not host or not email:
            await message.answer("❌ У ключа отсутствует сервер или email")
            await state.clear()
            return

        try:
            resp = await create_or_update_key_on_host(host, email, days_to_add=days)
        except Exception as e:
            logger.error(f"Продление ключа админом: не удалось обновить хост для ключа #{key_id}: {e}")
            resp = None
        if not resp or not resp.get('client_uuid') or not resp.get('expiry_timestamp_ms'):
            await message.answer("❌ Не удалось продлить ключ на сервере")
            return

        if not rw_repo.update_key(
            key_id,
            remnawave_user_uuid=resp['client_uuid'],
            expire_at_ms=int(resp['expiry_timestamp_ms']),
        ):
            await message.answer("❌ Не удалось обновить информацию о ключе.")
            return
        await state.clear()

        new_key = rw_repo.get_key_by_id(key_id)
        text = (
            f"🔑 <b>Ключ #{key_id}</b>\n"
            f"Хост: {new_key.get('host_name') or '—'}\n"
            f"Email: {new_key.get('key_email') or '—'}\n"
            f"Истекает: {new_key.get('expiry_date') or '—'}\n"
        )
        await message.answer(f"✅ Ключ продлён на {days} дн.")
        await message.answer(text, reply_markup=keyboards.create_admin_key_actions_keyboard(key_id, int(new_key.get('user_id')) if new_key and new_key.get('user_id') else None))


    class AdminAddAdmin(StatesGroup):
        waiting_for_input = State()

    @admin_router.callback_query(F.data == "admin_add_admin")
    async def admin_add_admin_entry(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        await state.set_state(AdminAddAdmin.waiting_for_input)
        await callback.message.edit_text(
            "Введите ID пользователя или его @username, которого нужно сделать администратором:\n\n"
            "Примеры: 123456789 или @username",
            reply_markup=keyboards.create_admin_cancel_keyboard()
        )

    @admin_router.message(AdminAddAdmin.waiting_for_input)
    async def admin_add_admin_process(message: types.Message, state: FSMContext):
        if not is_admin(message.from_user.id):
            return
        raw = (message.text or '').strip()
        target_id: int | None = None

        if raw.isdigit():
            try:
                target_id = int(raw)
            except Exception:
                target_id = None

        if target_id is None and raw.startswith('@'):
            uname = raw.lstrip('@')

            try:
                chat = await message.bot.get_chat(raw)
                target_id = int(chat.id)
            except Exception:
                target_id = None

            if target_id is None:
                try:
                    chat = await message.bot.get_chat(uname)
                    target_id = int(chat.id)
                except Exception:
                    target_id = None

            if target_id is None:
                try:
                    users = get_all_users() or []
                    uname_low = uname.lower()
                    for u in users:
                        u_un = (u.get('username') or '').lstrip('@').lower()
                        if u_un and u_un == uname_low:
                            target_id = int(u.get('telegram_id') or u.get('user_id') or u.get('id'))
                            break
                except Exception:
                    target_id = None
        if target_id is None:
            await message.answer("❌ Не удалось распознать ID/username. Отправьте корректное значение или нажмите Отмена.")
            return

        try:
            from shop_bot.data_manager.remnawave_repository import get_admin_ids, update_setting
            ids = set(get_admin_ids())
            ids.add(int(target_id))

            ids_str = ",".join(str(i) for i in sorted(ids))
            update_setting("admin_telegram_ids", ids_str)
            await message.answer(f"✅ Пользователь {target_id} добавлен в администраторы.")
        except Exception as e:
            await message.answer(f"❌ Ошибка при сохранении: {e}")
        await state.clear()

        try:
            await show_admin_menu(message)
        except Exception:
            pass


    class AdminRemoveAdmin(StatesGroup):
        waiting_for_input = State()

    @admin_router.callback_query(F.data == "admin_remove_admin")
    async def admin_remove_admin_entry(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        await state.set_state(AdminRemoveAdmin.waiting_for_input)
        await callback.message.edit_text(
            "Введите ID пользователя или его @username, которого нужно снять из админов:\n\n"
            "Примеры: 123456789 или @username",
            reply_markup=keyboards.create_admin_cancel_keyboard()
        )

    @admin_router.message(AdminRemoveAdmin.waiting_for_input)
    async def admin_remove_admin_process(message: types.Message, state: FSMContext):
        if not is_admin(message.from_user.id):
            return
        raw = (message.text or '').strip()
        target_id: int | None = None

        if raw.isdigit():
            try:
                target_id = int(raw)
            except Exception:
                target_id = None

        if target_id is None:
            uname = raw.lstrip('@')

            try:
                chat = await message.bot.get_chat(raw)
                target_id = int(chat.id)
            except Exception:
                target_id = None

            if target_id is None and uname:
                try:
                    chat = await message.bot.get_chat(uname)
                    target_id = int(chat.id)
                except Exception:
                    target_id = None

            if target_id is None and uname:
                try:
                    users = get_all_users() or []
                    uname_low = uname.lower()
                    for u in users:
                        u_un = (u.get('username') or '').lstrip('@').lower()
                        if u_un and u_un == uname_low:
                            target_id = int(u.get('telegram_id') or u.get('user_id') or u.get('id'))
                            break
                except Exception:
                    target_id = None
        if target_id is None:
            await message.answer("❌ Не удалось распознать ID/username. Отправьте корректное значение или нажмите Отмена.")
            return

        try:
            from shop_bot.data_manager.remnawave_repository import get_admin_ids, update_setting
            ids = set(get_admin_ids())
            if target_id not in ids:
                await message.answer(f"ℹ️ Пользователь {target_id} не является администратором.")
                await state.clear()
                try:
                    await show_admin_menu(message)
                except Exception:
                    pass
                return
            if len(ids) <= 1:
                await message.answer("❌ Нельзя снять последнего администратора.")
                return
            ids.discard(int(target_id))
            ids_str = ",".join(str(i) for i in sorted(ids))
            update_setting("admin_telegram_ids", ids_str)
            await message.answer(f"✅ Пользователь {target_id} снят с администраторов.")
        except Exception as e:
            await message.answer(f"❌ Ошибка при сохранении: {e}")
        await state.clear()

        try:
            await show_admin_menu(message)
        except Exception:
            pass


    @admin_router.callback_query(F.data.startswith("admin_key_delete_cancel_"))
    async def admin_key_delete_cancel(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        try:
            await callback.answer("Отменено")
        except Exception:
            pass
        logger.info(f"Получена отмена удаления ключа: data='{callback.data}' от {callback.from_user.id}")
        try:
            key_id = int(callback.data.split("_")[-1])
        except Exception:
            return
        key = rw_repo.get_key_by_id(key_id)
        if not key:
            return
        text = (
            f"🔑 <b>Ключ #{key_id}</b>\n"
            f"Хост: {key.get('host_name') or '—'}\n"
            f"Email: {key.get('key_email') or '—'}\n"
            f"Истекает: {key.get('expiry_date') or '—'}\n"
        )
        try:
            await callback.message.edit_text(
                text,
                reply_markup=keyboards.create_admin_key_actions_keyboard(key_id, int(key.get('user_id')) if key and key.get('user_id') else None)
            )
        except Exception as e:
            logger.debug(f"edit_text не удался в отмене удаления для ключа #{key_id}: {e}")
            await callback.message.answer(
                text,
                reply_markup=keyboards.create_admin_key_actions_keyboard(key_id, int(key.get('user_id')) if key and key.get('user_id') else None)
            )


    @admin_router.callback_query(F.data.startswith("admin_key_delete_confirm_"))
    async def admin_key_delete_confirm(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        try:
            await callback.answer("Удаляю…")
        except Exception:
            pass
        logger.info(f"Получено подтверждение удаления ключа: data='{callback.data}' от {callback.from_user.id}")
        try:
            key_id = int(callback.data.split('_')[-1])
        except Exception:
            await callback.message.answer("❌ Неверный формат key_id")
            return
        try:
            key = rw_repo.get_key_by_id(key_id)
        except Exception as e:
            logger.error(f"БД get_key_by_id не удался для #{key_id}: {e}")
            key = None
        if not key:
            await callback.message.answer("❌ Ключ не найден")
            return
        try:
            user_id = int(key.get('user_id'))
        except Exception as e:
            logger.error(f"Неверный user_id для ключа #{key_id}: {key.get('user_id')}, err={e}")
            await callback.message.answer("❌ Ошибка данных ключа: некорректный пользователь")
            return
        host = key.get('host_name')
        email = key.get('key_email')
        ok_host = True
        if host and email:
            try:
                ok_host = await delete_client_on_host(host, email)
            except Exception as e:
                ok_host = False
                logger.error(f"Не удалось удалить клиента на хосте '{host}' для ключа #{key_id}: {e}")
        ok_db = False
        try:
            ok_db = delete_key_by_email(email)
        except Exception as e:
            logger.error(f"Не удалось удалить ключ в БД для email '{email}': {e}")
        if ok_db:
            await callback.message.answer("✅ Ключ удалён" + (" (с хоста тоже)" if ok_host else " (но удалить на хосте не удалось)"))

            keys = get_keys_for_user(user_id)
            try:
                await callback.message.edit_text(
                    f"🔑 Ключи пользователя {user_id}:",
                    reply_markup=keyboards.create_admin_user_keys_keyboard(user_id, keys)
                )
            except Exception as e:
                logger.debug(f"edit_text не удался в обновлении списка подтверждения удаления для пользователя {user_id}: {e}")
                await callback.message.answer(
                    f"🔑 Ключи пользователя {user_id}:",
                    reply_markup=keyboards.create_admin_user_keys_keyboard(user_id, keys)
                )

            try:
                await callback.bot.send_message(
                    user_id,
                    "ℹ️ Администратор удалил один из ваших ключей. Если это ошибка — напишите в поддержку.",
                    reply_markup=keyboards.create_support_keyboard()
                )
            except Exception:
                pass
        else:
            await callback.message.answer("❌ Не удалось удалить ключ из базы данных")

    class AdminEditKeyEmail(StatesGroup):
        waiting_for_email = State()

    @admin_router.callback_query(F.data.startswith("admin_key_edit_email_"))
    async def admin_key_edit_email_start(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        try:
            key_id = int(callback.data.split("_")[-1])
        except Exception:
            await callback.message.answer("❌ Неверный формат key_id")
            return
        await state.update_data(edit_key_id=key_id)
        await state.set_state(AdminEditKeyEmail.waiting_for_email)
        await callback.message.edit_text(
            f"Введите новый email для ключа #{key_id}",
            reply_markup=keyboards.create_admin_cancel_keyboard()
        )

    @admin_router.message(AdminEditKeyEmail.waiting_for_email)
    async def admin_key_edit_email_commit(message: types.Message, state: FSMContext):
        if not is_admin(message.from_user.id):
            return
        data = await state.get_data()
        key_id = int(data.get('edit_key_id'))
        new_email = (message.text or '').strip()
        if not new_email:
            await message.answer("❌ Введите корректный email")
            return
        ok = update_key_email(key_id, new_email)
        if ok:
            await message.answer("✅ Email обновлён")
        else:
            await message.answer("❌ Не удалось обновить email (возможно, уже занят)")
        await state.clear()




    class AdminGiftKey(StatesGroup):
        picking_user = State()
        picking_host = State()
        picking_days = State()

    @admin_router.callback_query(F.data == "admin_gift_key")
    async def admin_gift_key_entry(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        users = get_all_users()
        await state.clear()
        await state.set_state(AdminGiftKey.picking_user)
        await callback.message.edit_text(
            "🎁 Выдача подарочного ключа\n\nВыберите пользователя:",
            reply_markup=keyboards.create_admin_users_pick_keyboard(users, page=0, action="gift")
        )


    @admin_router.callback_query(F.data.startswith("admin_gift_key_"))
    async def admin_gift_key_for_user(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        try:
            user_id = int(callback.data.split("_")[-1])
        except Exception:
            await callback.message.answer("❌ Неверный формат user_id")
            return
        await state.clear()
        await state.update_data(target_user_id=user_id)
        hosts = get_all_hosts()
        await state.set_state(AdminGiftKey.picking_host)
        await callback.message.edit_text(
            f"👤 Пользователь {user_id}. Выберите сервер:",
            reply_markup=keyboards.create_admin_hosts_pick_keyboard(hosts, action="gift")
        )

    @admin_router.callback_query(AdminGiftKey.picking_user, F.data.startswith("admin_gift_pick_user_page_"))
    async def admin_gift_pick_user_page(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        try:
            page = int(callback.data.split("_")[-1])
        except Exception:
            page = 0
        users = get_all_users()
        await callback.message.edit_text(
            "🎁 Выдача подарочного ключа\n\nВыберите пользователя:",
            reply_markup=keyboards.create_admin_users_pick_keyboard(users, page=page, action="gift")
        )

    @admin_router.callback_query(AdminGiftKey.picking_user, F.data.startswith("admin_gift_pick_user_"))
    async def admin_gift_pick_user(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        try:
            user_id = int(callback.data.split("_")[-1])
        except Exception:
            await callback.message.answer("❌ Неверный формат user_id")
            return
        await state.update_data(target_user_id=user_id)
        hosts = get_all_hosts()
        await state.set_state(AdminGiftKey.picking_host)
        await callback.message.edit_text(
            f"👤 Пользователь {user_id}. Выберите сервер:",
            reply_markup=keyboards.create_admin_hosts_pick_keyboard(hosts, action="gift")
        )

    @admin_router.callback_query(AdminGiftKey.picking_host, F.data == "admin_gift_back_to_users")
    async def admin_gift_back_to_users(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        users = get_all_users()
        await state.set_state(AdminGiftKey.picking_user)
        await callback.message.edit_text(
            "🎁 Выдача подарочного ключа\n\nВыберите пользователя:",
            reply_markup=keyboards.create_admin_users_pick_keyboard(users, page=0, action="gift")
        )

    @admin_router.callback_query(AdminGiftKey.picking_host, F.data.startswith("admin_gift_pick_host_"))
    async def admin_gift_pick_host(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        host_name = callback.data.split("admin_gift_pick_host_")[-1]
        await state.update_data(host_name=host_name)
        await state.set_state(AdminGiftKey.picking_days)
        await callback.message.edit_text(
            f"🌍 Сервер: {host_name}. Введите срок действия ключа в днях (целое число):",
            reply_markup=keyboards.create_admin_cancel_keyboard()
        )

    @admin_router.callback_query(AdminGiftKey.picking_days, F.data == "admin_gift_back_to_hosts")
    async def admin_gift_back_to_hosts(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        data = await state.get_data()
        user_id = int(data.get('target_user_id'))
        hosts = get_all_hosts()
        await state.set_state(AdminGiftKey.picking_host)
        await callback.message.edit_text(
            f"👤 Пользователь {user_id}. Выберите сервер:",
            reply_markup=keyboards.create_admin_hosts_pick_keyboard(hosts, action="gift")
        )
    @admin_router.message(AdminGiftKey.picking_days)
    async def admin_gift_pick_days(message: types.Message, state: FSMContext):
        if not is_admin(message.from_user.id):
            return
        data = await state.get_data()
        user_id = int(data.get('target_user_id'))
        host_name = data.get('host_name')
        try:
            days = int(message.text.strip())
        except Exception:
            await message.answer("❌ Введите целое число дней")
            return
        if days <= 0:
            await message.answer("❌ Срок должен быть положительным")
            return

        user = get_user(user_id) or {}
        username = (user.get('username') or f'user{user_id}').lower()
        username_slug = re.sub(r"[^a-z0-9._-]", "_", username).strip("_")[:16] or f"user{user_id}"
        base_local = f"gift_{username_slug}"
        candidate_local = base_local
        attempt = 1
        while True:
            candidate_email = f"{candidate_local}@bot.local"
            existing = rw_repo.get_key_by_email(candidate_email)
            if not existing:
                break
            attempt += 1
            candidate_local = f"{base_local}-{attempt}"
            if attempt > 100:
                candidate_local = f"{base_local}-{int(time.time())}"
                candidate_email = f"{candidate_local}@bot.local"
                break
        generated_email = candidate_email


        try:
            host_resp = await create_or_update_key_on_host(host_name, generated_email, days_to_add=days)
        except Exception as e:
            host_resp = None
            logging.error(f"Gift flow: failed to create client on host '{host_name}' for user {user_id}: {e}")

        if not host_resp or not host_resp.get("client_uuid") or not host_resp.get("expiry_timestamp_ms"):
            await message.answer("❌ Не удалось выдать ключ на сервере. Проверьте настройки хоста и доступность панели Remnawave.")
            await state.clear()
            await show_admin_menu(message)
            return

        client_uuid = host_resp["client_uuid"]
        expiry_ms = int(host_resp["expiry_timestamp_ms"])
        connection_link = host_resp.get("connection_string")

        key_id = rw_repo.record_key_from_payload(
            user_id=user_id,
            payload=host_resp,
            host_name=host_name,
        )
        if key_id:
            username_readable = (user.get('username') or '').strip()
            user_part = f"{user_id} (@{username_readable})" if username_readable else f"{user_id}"
            text_admin = (
                f"✅ 🎁 Подарочный ключ #{key_id} выдан пользователю {user_part} (сервер: {host_name}, {days} дн.)\n"
                f"Email: {generated_email}"
            )
            await message.answer(text_admin)
            try:
                notify_text = (
                    f"🎁 Администратор выдал вам подарочный ключ #{key_id}\n"
                    f"Сервер: {host_name}\n"
                    f"Срок: {days} дн.\n"
                )
                if connection_link:
                    cs = html_escape.escape(connection_link)
                    notify_text += f"\n🔗 Подписка:\n<pre><code>{cs}</code></pre>"
                await message.bot.send_message(user_id, notify_text, parse_mode='HTML', disable_web_page_preview=True)
            except Exception:
                pass
        else:
            await message.answer("❌ Не удалось сохранить ключ в базе данных.")
        await state.clear()
        await show_admin_menu(message)




    class AdminMainRefill(StatesGroup):
        waiting_for_pair = State()
        waiting_for_amount = State()

    @admin_router.callback_query(F.data == "admin_add_balance")
    async def admin_add_balance_entry(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        users = get_all_users()
        await callback.message.edit_text(
            "➕ Начисление баланса\n\nВыберите пользователя:",
            reply_markup=keyboards.create_admin_users_pick_keyboard(users, page=0, action="add_balance")
        )

    @admin_router.callback_query(F.data.startswith("admin_add_balance_"))
    async def admin_add_balance_user(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        try:
            user_id = int(callback.data.split("_")[-1])
        except Exception:
            await callback.message.answer("❌ Неверный формат user_id")
            return
        await state.update_data(target_user_id=user_id)
        await state.set_state(AdminMainRefill.waiting_for_amount)
        await callback.message.edit_text(
            f"Пользователь {user_id}. Введите сумму начисления (в рублях):",
            reply_markup=keyboards.create_admin_cancel_keyboard()
        )


    @admin_router.callback_query(F.data.startswith("admin_add_balance_pick_user_page_"))
    async def admin_add_balance_pick_user_page(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        try:
            page = int(callback.data.split("_")[-1])
        except Exception:
            page = 0
        users = get_all_users()
        await callback.message.edit_text(
            "➕ Начисление баланса\n\nВыберите пользователя:",
            reply_markup=keyboards.create_admin_users_pick_keyboard(users, page=page, action="add_balance")
        )


    @admin_router.callback_query(F.data.startswith("admin_add_balance_pick_user_"))
    async def admin_add_balance_pick_user(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        try:
            user_id = int(callback.data.split("_")[-1])
        except Exception:
            await callback.message.answer("❌ Неверный формат user_id")
            return
        await state.update_data(target_user_id=user_id)
        await state.set_state(AdminMainRefill.waiting_for_amount)
        await callback.message.edit_text(
            f"Пользователь {user_id}. Введите сумму начисления (в рублях):",
            reply_markup=keyboards.create_admin_cancel_keyboard()
        )

    @admin_router.message(AdminMainRefill.waiting_for_amount)
    async def handle_main_amount(message: types.Message, state: FSMContext):
        if not is_admin(message.from_user.id):
            return
        data = await state.get_data()
        user_id = int(data.get('target_user_id'))
        try:
            amount = float(message.text.strip().replace(',', '.'))
        except Exception:
            await message.answer("❌ Введите число — сумму в рублях")
            return
        if amount <= 0:
            await message.answer("❌ Сумма должна быть положительной")
            return
        try:
            ok = add_to_balance(user_id, amount)
            if ok:
                await message.answer(f"✅ Начислено {amount:.2f} RUB на баланс пользователю {user_id}")
                try:
                    await message.bot.send_message(user_id, f"💰 Вам начислено {amount:.2f} RUB на баланс администратором.")
                except Exception:
                    pass
            else:
                await message.answer("❌ Пользователь не найден или ошибка БД")
        except Exception as e:
            await message.answer(f"❌ Ошибка начисления: {e}")
        await state.clear()
        await show_admin_menu(message)


    @admin_router.callback_query(F.data.startswith("admin_key_back_"))
    async def admin_key_back(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        try:
            key_id = int(callback.data.split("_")[-1])
        except Exception:
            await callback.message.answer("❌ Неверный формат key_id")
            return
        key = rw_repo.get_key_by_id(key_id)
        if not key:
            await callback.message.answer("❌ Ключ не найден")
            return

        host_from_state = None
        try:
            data = await state.get_data()
            host_from_state = (data or {}).get('hostkeys_host')
        except Exception:
            host_from_state = None

        if host_from_state:
            host_name = host_from_state
            keys = get_keys_for_host(host_name)
            await callback.message.edit_text(
                f"🔑 Ключи на хосте {host_name}:",
                reply_markup=keyboards.create_admin_keys_for_host_keyboard(host_name, keys)
            )
        else:
            user_id = int(key.get('user_id'))
            keys = get_keys_for_user(user_id)
            await callback.message.edit_text(
                f"🔑 Ключи пользователя {user_id}:",
                reply_markup=keyboards.create_admin_user_keys_keyboard(user_id, keys)
            )


    @admin_router.callback_query(F.data == "noop")
    async def admin_noop(callback: types.CallbackQuery):
        await callback.answer()

    @admin_router.callback_query(F.data == "admin_cancel")
    async def admin_cancel_handler(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer("Отменено")
        await state.clear()
        await show_admin_menu(callback.message, edit_message=True)


    class AdminMainDeduct(StatesGroup):
        waiting_for_amount = State()


    @admin_router.callback_query(F.data == "admin_deduct_balance")
    async def admin_deduct_balance_entry(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        users = get_all_users()
        await callback.message.edit_text(
            "➖ Списание баланса\n\nВыберите пользователя:",
            reply_markup=keyboards.create_admin_users_pick_keyboard(users, page=0, action="deduct_balance")
        )


    @admin_router.callback_query(F.data.startswith("admin_deduct_balance_"))
    async def admin_deduct_balance_user(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        try:
            user_id = int(callback.data.split("_")[-1])
        except Exception:
            await callback.message.answer("❌ Неверный формат user_id")
            return
        await state.update_data(target_user_id=user_id)
        await state.set_state(AdminMainDeduct.waiting_for_amount)
        await callback.message.edit_text(
            f"Пользователь {user_id}. Введите сумму списания (в рублях):",
            reply_markup=keyboards.create_admin_cancel_keyboard()
        )


    @admin_router.callback_query(F.data.startswith("admin_deduct_balance_pick_user_page_"))
    async def admin_deduct_balance_pick_user_page(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        try:
            page = int(callback.data.split("_")[-1])
        except Exception:
            page = 0
        users = get_all_users()
        await callback.message.edit_text(
            "➖ Списание баланса\n\nВыберите пользователя:",
            reply_markup=keyboards.create_admin_users_pick_keyboard(users, page=page, action="deduct_balance")
        )


    @admin_router.callback_query(F.data.startswith("admin_deduct_balance_pick_user_"))
    async def admin_deduct_balance_pick_user(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        try:
            user_id = int(callback.data.split("_")[-1])
        except Exception:
            await callback.message.answer("❌ Неверный формат user_id")
            return
        await state.update_data(target_user_id=user_id)
        await state.set_state(AdminMainDeduct.waiting_for_amount)
        await callback.message.edit_text(
            f"Пользователь {user_id}. Введите сумму списания (в рублях):",
            reply_markup=keyboards.create_admin_cancel_keyboard()
        )

    @admin_router.message(AdminMainDeduct.waiting_for_amount)
    async def handle_deduct_amount(message: types.Message, state: FSMContext):
        if not is_admin(message.from_user.id):
            return
        data = await state.get_data()
        user_id = int(data.get('target_user_id'))
        try:
            amount = float(message.text.strip().replace(',', '.'))
        except Exception:
            await message.answer("❌ Введите число — сумму в рублях")
            return
        if amount <= 0:
            await message.answer("❌ Сумма должна быть положительной")
            return
        try:
            ok = deduct_from_balance(user_id, amount)
            if ok:
                await message.answer(f"✅ Списано {amount:.2f} RUB с баланса пользователя {user_id}")
                try:
                    await message.bot.send_message(
                        user_id,
                        f"➖ С вашего баланса списано {amount:.2f} RUB администратором.\nЕсли это ошибка — напишите в поддержку.",
                        reply_markup=keyboards.create_support_keyboard()
                    )
                except Exception:
                    pass
            else:
                await message.answer("❌ Пользователь не найден или недостаточно средств")
        except Exception as e:
            await message.answer(f"❌ Ошибка списания: {e}")
        await state.clear()
        await show_admin_menu(message)


    class AdminHostKeys(StatesGroup):
        picking_host = State()

    @admin_router.callback_query(F.data == "admin_host_keys")
    async def admin_host_keys_entry(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        await state.clear()
        await state.set_state(AdminHostKeys.picking_host)
        hosts = get_all_hosts()
        await callback.message.edit_text(
            "🌍 Выберите хост для просмотра ключей:",
            reply_markup=keyboards.create_admin_hosts_pick_keyboard(hosts, action="hostkeys")
        )

    @admin_router.callback_query(AdminHostKeys.picking_host, F.data.startswith("admin_hostkeys_pick_host_"))
    async def admin_host_keys_pick_host(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        host_name = callback.data.split("admin_hostkeys_pick_host_")[-1]

        try:
            await state.update_data(hostkeys_host=host_name)
        except Exception:
            pass
        keys = get_keys_for_host(host_name)
        await callback.message.edit_text(
            f"🔑 Ключи на хосте {host_name}:",
            reply_markup=keyboards.create_admin_keys_for_host_keyboard(host_name, keys)
        )

    @admin_router.callback_query(AdminHostKeys.picking_host, F.data.startswith("admin_hostkeys_page_"))
    async def admin_hostkeys_page(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        try:
            page = int(callback.data.split("_")[-1])
        except Exception:
            page = 0
        data = await state.get_data()
        host_name = data.get('hostkeys_host')
        if not host_name:

            hosts = get_all_hosts()
            await callback.message.edit_text(
                "🌍 Выберите хост для просмотра ключей:",
                reply_markup=keyboards.create_admin_hosts_pick_keyboard(hosts, action="hostkeys")
            )
            return
        keys = get_keys_for_host(host_name)
        await callback.message.edit_text(
            f"🔑 Ключи на хосте {host_name}:",
            reply_markup=keyboards.create_admin_keys_for_host_keyboard(host_name, keys, page=page)
        )

    @admin_router.callback_query(AdminHostKeys.picking_host, F.data == "admin_hostkeys_back_to_hosts")
    async def admin_hostkeys_back_to_hosts(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()

        try:
            await state.update_data(hostkeys_host=None)
        except Exception:
            pass
        hosts = get_all_hosts()
        await callback.message.edit_text(
            "🌍 Выберите хост для просмотра ключей:",
            reply_markup=keyboards.create_admin_hosts_pick_keyboard(hosts, action="hostkeys")
        )

    @admin_router.callback_query(F.data == "admin_hostkeys_back_to_users")
    async def admin_hostkeys_back_to_users(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        await show_admin_menu(callback.message, edit_message=True)


    class AdminQuickDeleteKey(StatesGroup):
        waiting_for_identifier = State()

    @admin_router.callback_query(F.data == "admin_delete_key")
    async def admin_delete_key_entry(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        await state.set_state(AdminQuickDeleteKey.waiting_for_identifier)
        await callback.message.edit_text(
            "🗑 Введите <code>key_id</code> или <code>email</code> ключа для удаления:",
            reply_markup=keyboards.create_admin_cancel_keyboard()
        )

    @admin_router.message(AdminQuickDeleteKey.waiting_for_identifier)
    async def admin_delete_key_process(message: types.Message, state: FSMContext):
        if not is_admin(message.from_user.id):
            return
        text = (message.text or '').strip()
        key = None

        try:
            key_id = int(text)
            key = rw_repo.get_key_by_id(key_id)
        except Exception:

            key = rw_repo.get_key_by_email(text)
        if not key:
            await message.answer("❌ Ключ не найден. Пришлите корректный key_id или email.")
            return
        key_id = int(key.get('key_id'))
        email = key.get('key_email') or '—'
        host = key.get('host_name') or '—'
        await state.clear()
        await message.answer(
            f"Подтвердите удаление ключа #{key_id}\nEmail: {email}\nСервер: {host}",
            reply_markup=keyboards.create_admin_delete_key_confirm_keyboard(key_id)
        )


    class AdminExtendKey(StatesGroup):
        waiting_for_pair = State()

    @admin_router.callback_query(F.data == "admin_extend_key")
    async def admin_extend_key_entry(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        await state.set_state(AdminExtendKey.waiting_for_pair)
        await callback.message.edit_text(
            "➕ Введите: <code>key_id дни</code> (сколько дней добавить к ключу)",
            reply_markup=keyboards.create_admin_cancel_keyboard()
        )

    @admin_router.message(AdminExtendKey.waiting_for_pair)
    async def admin_extend_key_process(message: types.Message, state: FSMContext):
        if not is_admin(message.from_user.id):
            return
        parts = (message.text or '').strip().split()
        if len(parts) != 2:
            await message.answer("❌ Формат: <code>key_id дни</code>")
            return
        try:
            key_id = int(parts[0])
            days = int(parts[1])
        except Exception:
            await message.answer("❌ Оба значения должны быть числами")
            return
        if days <= 0:
            await message.answer("❌ Количество дней должно быть положительным")
            return
        key = rw_repo.get_key_by_id(key_id)
        if not key:
            await message.answer("❌ Ключ не найден")
            return
        host = key.get('host_name')
        email = key.get('key_email')
        if not host or not email:
            await message.answer("❌ У ключа отсутствуют данные о хосте или email")
            return

        resp = None
        try:
            resp = await create_or_update_key_on_host(host, email, days_to_add=days)
        except Exception as e:
            logger.error(f"Поток продления: не удалось обновить клиента на хосте '{host}' для ключа #{key_id}: {e}")
        if not resp or not resp.get('client_uuid') or not resp.get('expiry_timestamp_ms'):
            await message.answer("❌ Не удалось продлить ключ на сервере")
            return

        if not rw_repo.update_key(
            key_id,
            remnawave_user_uuid=resp['client_uuid'],
            expire_at_ms=int(resp['expiry_timestamp_ms']),
        ):
            await message.answer("❌ Не удалось обновить информацию о ключе.")
            return
        await state.clear()
        await message.answer(f"✅ Ключ #{key_id} продлён на {days} дн.")

        try:
            await message.bot.send_message(int(key.get('user_id')), f"ℹ️ Администратор продлил ваш ключ #{key_id} на {days} дн.")
        except Exception:
            pass

    @admin_router.callback_query(F.data == "start_broadcast")
    async def start_broadcast_handler(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id):
            await callback.answer("У вас нет прав.", show_alert=True)
            return
        await callback.answer()
        await callback.message.edit_text(
            "Пришлите сообщение, которое вы хотите разослать всем пользователям.\n"
            "Вы можете использовать форматирование (<b>жирный</b>, <i>курсив</i>).\n"
            "Также поддерживаются фото, видео и документы.\n",
            reply_markup=keyboards.create_broadcast_cancel_keyboard()
        )
        await state.set_state(Broadcast.waiting_for_message)

    @admin_router.message(Broadcast.waiting_for_message)
    async def broadcast_message_received_handler(message: types.Message, state: FSMContext):

        await state.update_data(message_to_send=message.model_dump_json())
        await message.answer(
            "Сообщение получено. Хотите добавить к нему кнопку со ссылкой?",
            reply_markup=keyboards.create_broadcast_options_keyboard()
        )
        await state.set_state(Broadcast.waiting_for_button_option)

    @admin_router.callback_query(Broadcast.waiting_for_button_option, F.data == "broadcast_add_button")
    async def add_button_prompt_handler(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer()
        await callback.message.edit_text(
            "Хорошо. Теперь отправьте мне текст для кнопки.",
            reply_markup=keyboards.create_broadcast_cancel_keyboard()
        )
        await state.set_state(Broadcast.waiting_for_button_text)

    @admin_router.message(Broadcast.waiting_for_button_text)
    async def button_text_received_handler(message: types.Message, state: FSMContext):
        await state.update_data(button_text=message.text)
        await message.answer(
            "Текст кнопки получен. Теперь отправьте ссылку (URL), куда она будет вести.",
            reply_markup=keyboards.create_broadcast_cancel_keyboard()
        )
        await state.set_state(Broadcast.waiting_for_button_url)

    @admin_router.message(Broadcast.waiting_for_button_url)
    async def button_url_received_handler(message: types.Message, state: FSMContext, bot: Bot):
        url_to_check = message.text

        if not (url_to_check.startswith("http://") or url_to_check.startswith("https://")):
            await message.answer(
                "❌ Ссылка должна начинаться с http:// или https://. Попробуйте еще раз.")
            return
        await state.update_data(button_url=url_to_check)
        await show_broadcast_preview(message, state, bot)

    @admin_router.callback_query(Broadcast.waiting_for_button_option, F.data == "broadcast_skip_button")
    async def skip_button_handler(callback: types.CallbackQuery, state: FSMContext, bot: Bot):
        await callback.answer()
        await state.update_data(button_text=None, button_url=None)
        await show_broadcast_preview(callback.message, state, bot)

    async def show_broadcast_preview(message: types.Message, state: FSMContext, bot: Bot):
        data = await state.get_data()
        message_json = data.get('message_to_send')
        original_message = types.Message.model_validate_json(message_json)

        button_text = data.get('button_text')
        button_url = data.get('button_url')

        preview_keyboard = None
        if button_text and button_url:
            builder = InlineKeyboardBuilder()
            builder.button(text=button_text, url=button_url)
            preview_keyboard = builder.as_markup()

        await message.answer(
            "Вот так будет выглядеть ваше сообщение. Отправляем?",
            reply_markup=keyboards.create_broadcast_confirmation_keyboard()
        )

        await bot.copy_message(
            chat_id=message.chat.id,
            from_chat_id=original_message.chat.id,
            message_id=original_message.message_id,
            reply_markup=preview_keyboard
        )

        await state.set_state(Broadcast.waiting_for_confirmation)

    @admin_router.callback_query(Broadcast.waiting_for_confirmation, F.data == "confirm_broadcast")
    async def confirm_broadcast_handler(callback: types.CallbackQuery, state: FSMContext, bot: Bot):
        await callback.message.edit_text("⏳ Начинаю рассылку... Это может занять некоторое время.")

        data = await state.get_data()
        message_json = data.get('message_to_send')
        original_message = types.Message.model_validate_json(message_json)

        button_text = data.get('button_text')
        button_url = data.get('button_url')

        final_keyboard = None
        if button_text and button_url:
            builder = InlineKeyboardBuilder()
            builder.button(text=button_text, url=button_url)
            final_keyboard = builder.as_markup()

        await state.clear()

        users = get_all_users()
        logger.info(f"Рассылка: Начинаем итерацию по {len(users)} пользователям.")

        sent_count = 0
        failed_count = 0
        banned_count = 0

        for user in users:
            user_id = user['telegram_id']
            if user.get('is_banned'):
                banned_count += 1
                continue
            try:
                await bot.copy_message(
                    chat_id=user_id,
                    from_chat_id=original_message.chat.id,
                    message_id=original_message.message_id,
                    reply_markup=final_keyboard
                )
                sent_count += 1
                await asyncio.sleep(0.1)
            except Exception as e:
                failed_count += 1
                logger.warning(f"Не удалось отправить сообщение рассылки пользователю {user_id}: {e}")

        await callback.message.answer(
            f"✅ Рассылка завершена!\n\n"
            f"👍 Отправлено: {sent_count}\n"
            f"👎 Не удалось отправить: {failed_count}\n"
            f"🚫 Пропущено (забанены): {banned_count}"
        )
        await show_admin_menu(callback.message)

    @admin_router.callback_query(StateFilter(Broadcast), F.data == "cancel_broadcast")
    async def cancel_broadcast_handler(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer("Рассылка отменена.")
        await state.clear()
        await show_admin_menu(callback.message, edit_message=True)


    @admin_router.message(Command(commands=["approve_withdraw"]))
    async def approve_withdraw_handler(message: types.Message):
        if not is_admin(message.from_user.id):
            return
        try:
            user_id = int(message.text.split("_")[-1])
            user = get_user(user_id)
            balance = user.get('referral_balance', 0)
            if balance < 100:
                await message.answer("Баланс пользователя менее 100 руб.")
                return
            set_referral_balance(user_id, 0)
            set_referral_balance_all(user_id, 0)
            await message.answer(f"✅ Выплата {balance:.2f} RUB пользователю {user_id} подтверждена.")
            await message.bot.send_message(
                user_id,
                f"✅ Ваша заявка на вывод {balance:.2f} RUB одобрена. Деньги будут переведены в ближайшее время."
            )
        except Exception as e:
            await message.answer(f"Ошибка: {e}")

    @admin_router.message(Command(commands=["decline_withdraw"]))
    async def decline_withdraw_handler(message: types.Message):
        if not is_admin(message.from_user.id):
            return
        try:
            user_id = int(message.text.split("_")[-1])
            await message.answer(f"❌ Заявка пользователя {user_id} отклонена.")
            await message.bot.send_message(
                user_id,
                "❌ Ваша заявка на вывод отклонена. Проверьте корректность реквизитов и попробуйте снова."
            )
        except Exception as e:
            await message.answer(f"Ошибка: {e}")


    @admin_router.callback_query(F.data == "admin_monitor")
    async def admin_monitor_menu(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("Доступ только для админов", show_alert=True)
            return
        try:
            hosts = get_all_hosts() or []
            targets = get_all_ssh_targets() or []
        except Exception:
            hosts, targets = [], []
        kb = InlineKeyboardBuilder()
        kb.button(text="📟 Панель (локально)", callback_data="admin_monitor_local")
        for h in hosts:
            name = h.get('host_name')
            if name:
                kb.button(text=f"🖥 {name}", callback_data=f"rmh:{name}")
        for t in targets:
            tname = t.get('target_name')
            if not tname:
                continue
            try:
                digest = hashlib.sha1((tname or '').encode('utf-8','ignore')).hexdigest()
            except Exception:
                digest = hashlib.sha1(str(tname).encode('utf-8','ignore')).hexdigest()
            kb.button(text=f"🔌 {tname}", callback_data=f"rmt:{digest}")
        kb.button(text="⬅️ В админ-меню", callback_data="admin_menu")
        rows = [1]
        total_items = len(hosts) + len(targets)
        if total_items > 0:
            rows.extend([2] * ((total_items + 1) // 2))
        rows.append(1)
        kb.adjust(*rows)
        await callback.message.edit_text("<b>Мониторинг ресурсов</b>\nВыберите объект:", reply_markup=kb.as_markup())

    @admin_router.callback_query(F.data == "admin_monitor_local")
    async def admin_monitor_local(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("Доступ только для админов", show_alert=True)
            return
        
        await callback.answer("🔄 Получение данных...")
        

        try:
            hosts = get_all_hosts() or []
            if hosts and len(hosts) > 0:

                current_host = hosts[0]
                data = resource_monitor.get_remote_metrics_for_host(current_host.get('host_name'))
                is_remote = True
            else:

                data = resource_monitor.get_local_metrics()
                is_remote = False
        except Exception:

            data = resource_monitor.get_local_metrics()
            is_remote = False
        
        try:
            if is_remote:

                cpu_p = data.get('cpu_percent')
                mem_p = data.get('memory_percent')
                disk_p = data.get('disk_percent')
                load1 = (data.get('loadavg') or [None])[0] if data.get('loadavg') else None
                net_sent = data.get('network_sent', 0)
                net_recv = data.get('network_recv', 0)
                scope = 'host'
                name = current_host.get('host_name')
            else:

                cpu_p = (data.get('cpu') or {}).get('percent')
                mem_p = (data.get('memory') or {}).get('percent')
                disks = data.get('disks') or []
                disk_p = max((d.get('percent') or 0) for d in disks) if disks else None
                load1 = (data.get('cpu') or {}).get('loadavg',[None])[0] if (data.get('cpu') or {}).get('loadavg') else None
                net_sent = (data.get('net') or {}).get('bytes_sent', 0)
                net_recv = (data.get('net') or {}).get('bytes_recv', 0)
                scope = 'local'
                name = 'panel'
            
            rw_repo.insert_resource_metric(
                scope, name,
                cpu_percent=cpu_p, mem_percent=mem_p, disk_percent=disk_p,
                load1=load1,
                net_bytes_sent=net_sent,
                net_bytes_recv=net_recv,
                raw_json=json.dumps(data, ensure_ascii=False)
            )
        except Exception:
            pass
        
        if not data.get('ok'):
            host_name = current_host.get('host_name') if is_remote else 'локально'
            txt = [
                f"🚨 <b>Панель ({host_name}) - ОШИБКА</b>",
                "",
                f"❌ <code>{data.get('error', 'Неизвестная ошибка')}</code>"
            ]
        else:
            if is_remote:

                cpu = {'percent': data.get('cpu_percent', 0), 'count_logical': data.get('cpu_count', '—')}
                mem = {
                    'percent': data.get('memory_percent', 0),
                    'used': (data.get('memory_used_mb', 0)) * 1024 * 1024,
                    'total': (data.get('memory_total_mb', 0)) * 1024 * 1024
                }
                net = {
                    'bytes_sent': data.get('network_sent', 0),
                    'bytes_recv': data.get('network_recv', 0),
                    'packets_sent': data.get('network_packets_sent', 0),
                    'packets_recv': data.get('network_packets_recv', 0)
                }
                sw = {}
                disks = []
                hostname = data.get('uname', '—')
                platform = '—'
            else:

                cpu = data.get('cpu') or {}
                mem = data.get('memory') or {}
                sw = data.get('swap') or {}
                net = data.get('net') or {}
                disks = data.get('disks', [])
                hostname = data.get('hostname', '—')
                platform = data.get('platform', '—')
            

            cpu_percent = cpu.get('percent', 0) or 0
            mem_percent = mem.get('percent', 0) or 0
            disk_percent = disk_p or 0
            
            def get_status_emoji(value, warning=70, critical=90):
                if value >= critical:
                    return "🔴"
                elif value >= warning:
                    return "🟡"
                else:
                    return "🟢"
            
            def format_bytes(bytes_val):
                if bytes_val is None:
                    return "—"
                for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
                    if bytes_val < 1024.0:
                        return f"{bytes_val:.1f} {unit}"
                    bytes_val /= 1024.0
                return f"{bytes_val:.1f} PB"
            
            def format_uptime(seconds):
                if not seconds:
                    return "—"
                days = int(seconds // 86400)
                hours = int((seconds % 86400) // 3600)
                minutes = int((seconds % 3600) // 60)
                if days > 0:
                    return f"{days}д {hours}ч {minutes}м"
                elif hours > 0:
                    return f"{hours}ч {minutes}м"
                else:
                    return f"{minutes}м"
            
            host_name = current_host.get('host_name') if is_remote else 'локально'
            txt = [
                f"🖥️ <b>Панель ({host_name})</b>",
                "",
                f"🖥 <b>Хост:</b> <code>{hostname}</code>",
                f"⏱ <b>Время работы:</b> <code>{format_uptime(data.get('uptime_sec'))}</code>",
                f"🖥 <b>Платформа:</b> <code>{platform}</code>",
                "",
                "📊 <b>Производительность:</b>",
                f"{get_status_emoji(cpu_percent)} <b>Процессор:</b> {cpu_percent}% ({cpu.get('count_logical', '—')} логич, {cpu.get('count_physical', '—')} физич)",
                f"{get_status_emoji(mem_percent)} <b>Память:</b> {mem_percent}% ({format_bytes(mem.get('used'))} / {format_bytes(mem.get('total'))})",
                f"{get_status_emoji(disk_percent)} <b>Диск:</b> {disk_percent}%",
                f"🔄 <b>Swap:</b> {sw.get('percent', '—')}% ({format_bytes(sw.get('used'))} / {format_bytes(sw.get('total'))})" if sw else "",
                "",
                "🌐 <b>Сеть:</b>",
                f"⬆️ Отправлено: <code>{format_bytes(net.get('bytes_sent', 0))}</code>",
                f"⬇️ Получено: <code>{format_bytes(net.get('bytes_recv', 0))}</code>",
            ]
            

            if disks:
                txt.append("")
                txt.append("💾 <b>Диски:</b>")
                for disk in disks[:3]:
                    mountpoint = disk.get('mountpoint') or disk.get('device', '—')
                    percent = disk.get('percent', 0) or 0
                    used = format_bytes(disk.get('used'))
                    total = format_bytes(disk.get('total'))
                    txt.append(f"  {get_status_emoji(percent, 80, 95)} <code>{mountpoint}</code>: {percent}% ({used} / {total})")
                if len(disks) > 3:
                    txt.append(f"  ... и еще {len(disks) - 3} дисков")
        

        kb = InlineKeyboardBuilder()
        kb.button(text="🔄 Обновить", callback_data="admin_monitor_local")
        kb.button(text="📊 Полная статистика", callback_data="admin_monitor_detailed")
        kb.button(text="⬅️ Назад", callback_data="admin_monitor")
        kb.adjust(2, 1)
        
        await callback.message.edit_text("\n".join(txt), parse_mode='HTML', reply_markup=kb.as_markup())

    @admin_router.callback_query(F.data.startswith("rmh:"))
    async def admin_monitor_host(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("Доступ только для админов", show_alert=True)
            return
        
        host_name = (callback.data or '').split(':',1)[1]
        await callback.answer("🔄 Подключение к хосту...")
        data = resource_monitor.get_remote_metrics_for_host(host_name)
        
        try:
            mem_p = (data.get('memory') or {}).get('percent')
            disks = data.get('disks') or []
            disk_p = max((d.get('percent') or 0) for d in disks) if disks else None
            rw_repo.insert_resource_metric(
                'host', host_name,
                mem_percent=mem_p,
                disk_percent=disk_p,
                load1=(data.get('loadavg') or [None])[0],
                raw_json=json.dumps(data, ensure_ascii=False)
            )
        except Exception:
            pass
        
        if not data.get('ok'):
            txt = [
                f"🖥️ <b>Хост: {host_name}</b>",
                "",
                "🚨 <b>ОШИБКА ПОДКЛЮЧЕНИЯ</b>",
                f"❌ <code>{data.get('error', 'Неизвестная ошибка')}</code>"
            ]
        else:
            mem = data.get('memory') or {}
            loadavg = data.get('loadavg') or []
            cpu_count = data.get('cpu_count', 1)
            

            cpu_percent = None
            if loadavg and cpu_count:
                cpu_percent = min((loadavg[0] / cpu_count) * 100, 100)
            
            mem_percent = mem.get('percent', 0) or 0
            disk_percent = max((d.get('percent') or 0) for d in data.get('disks', [])) if data.get('disks') else 0
            
            def get_status_emoji(value, warning=70, critical=90):
                if value is None:
                    return "⚪"
                if value >= critical:
                    return "🔴"
                elif value >= warning:
                    return "🟡"
                else:
                    return "🟢"
            
            def format_uptime(seconds):
                if not seconds:
                    return "—"
                days = int(seconds // 86400)
                hours = int((seconds % 86400) // 3600)
                minutes = int((seconds % 3600) // 60)
                if days > 0:
                    return f"{days}д {hours}ч {minutes}м"
                elif hours > 0:
                    return f"{hours}ч {minutes}м"
                else:
                    return f"{minutes}м"
            
            def format_loadavg(loads):
                if not loads:
                    return "—"
                return " / ".join(f"{load:.2f}" for load in loads)
            
            txt = [
                f"🖥️ <b>Хост: {host_name}</b>",
                "",
                f"🖥 <b>Система:</b> <code>{data.get('uname', '—')}</code>",
                f"⏱ <b>Время работы:</b> <code>{format_uptime(data.get('uptime_sec'))}</code>",
                f"🔢 <b>Ядер процессора:</b> <code>{cpu_count}</code>",
                "",
                "📊 <b>Производительность:</b>",
                f"{get_status_emoji(cpu_percent)} <b>Процессор:</b> {cpu_percent:.1f}%" if cpu_percent is not None else "⚪ <b>Процессор:</b> —",
                f"📈 <b>Средняя загрузка:</b> <code>{format_loadavg(loadavg)}</code>",
                f"{get_status_emoji(mem_percent)} <b>Память:</b> {mem_percent}% ({mem.get('used_mb', '—')} / {mem.get('total_mb', '—')} МБ)",
                f"{get_status_emoji(disk_percent)} <b>Диск:</b> {disk_percent}%",
            ]
            

            disks = data.get('disks', [])
            if disks:
                txt.append("")
                txt.append("💾 <b>Диски:</b>")
                for disk in disks[:3]:
                    device = disk.get('device') or disk.get('mountpoint', '—')
                    percent = disk.get('percent', 0) or 0
                    used = disk.get('used', '—')
                    size = disk.get('size', '—')
                    txt.append(f"  {get_status_emoji(percent, 80, 95)} <code>{device}</code>: {percent}% ({used} / {size})")
                if len(disks) > 3:
                    txt.append(f"  ... и еще {len(disks) - 3} дисков")
        

        kb = InlineKeyboardBuilder()
        kb.button(text="🔄 Обновить", callback_data=callback.data)
        kb.button(text="⬅️ Назад", callback_data="admin_monitor")
        kb.adjust(2)
        
        await callback.message.edit_text("\n".join(txt), parse_mode='HTML', reply_markup=kb.as_markup())

    @admin_router.callback_query(F.data.startswith("rmt:"))
    async def admin_monitor_target(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("Доступ только для админов", show_alert=True)
            return
        

        try:
            digest = callback.data.split(':',1)[1]
        except Exception:
            digest = ''
        tname = None
        try:
            for t in get_all_ssh_targets() or []:
                name = t.get('target_name')
                if not name:
                    continue
                try:
                    h = hashlib.sha1((name or '').encode('utf-8','ignore')).hexdigest()
                except Exception:
                    h = hashlib.sha1(str(name).encode('utf-8','ignore')).hexdigest()
                if h == digest:
                    tname = name; break
        except Exception:
            tname = None
        if not tname:
            await callback.answer("Цель не найдена", show_alert=True)
            return
        
        await callback.answer("🔄 Подключение по SSH...")
        data = resource_monitor.get_remote_metrics_for_target(tname)
        
        try:
            mem_p = (data.get('memory') or {}).get('percent')
            disks = data.get('disks') or []
            disk_p = max((d.get('percent') or 0) for d in disks) if disks else None
            rw_repo.insert_resource_metric(
                'target', tname,
                mem_percent=mem_p,
                disk_percent=disk_p,
                load1=(data.get('loadavg') or [None])[0],
                raw_json=json.dumps(data, ensure_ascii=False)
            )
        except Exception:
            pass
        
        if not data.get('ok'):
            txt = [
                f"🔌 <b>SSH-цель: {tname}</b>",
                "",
                "🚨 <b>ОШИБКА ПОДКЛЮЧЕНИЯ</b>",
                f"❌ <code>{data.get('error', 'Неизвестная ошибка')}</code>"
            ]
        else:
            mem = data.get('memory') or {}
            loadavg = data.get('loadavg') or []
            cpu_count = data.get('cpu_count', 1)
            

            cpu_percent = None
            if loadavg and cpu_count:
                cpu_percent = min((loadavg[0] / cpu_count) * 100, 100)
            
            mem_percent = mem.get('percent', 0) or 0
            disk_percent = max((d.get('percent') or 0) for d in data.get('disks', [])) if data.get('disks') else 0
            
            def get_status_emoji(value, warning=70, critical=90):
                if value is None:
                    return "⚪"
                if value >= critical:
                    return "🔴"
                elif value >= warning:
                    return "🟡"
                else:
                    return "🟢"
            
            def format_uptime(seconds):
                if not seconds:
                    return "—"
                days = int(seconds // 86400)
                hours = int((seconds % 86400) // 3600)
                minutes = int((seconds % 3600) // 60)
                if days > 0:
                    return f"{days}д {hours}ч {minutes}м"
                elif hours > 0:
                    return f"{hours}ч {minutes}м"
                else:
                    return f"{minutes}м"
            
            def format_loadavg(loads):
                if not loads:
                    return "—"
                return " / ".join(f"{load:.2f}" for load in loads)
            
            txt = [
                f"🔌 <b>SSH-цель: {tname}</b>",
                "",
                f"🖥 <b>Система:</b> <code>{data.get('uname', '—')}</code>",
                f"⏱ <b>Время работы:</b> <code>{format_uptime(data.get('uptime_sec'))}</code>",
                f"🔢 <b>Ядер процессора:</b> <code>{cpu_count}</code>",
                "",
                "📊 <b>Производительность:</b>",
                f"{get_status_emoji(cpu_percent)} <b>Процессор:</b> {cpu_percent:.1f}%" if cpu_percent is not None else "⚪ <b>Процессор:</b> —",
                f"📈 <b>Средняя загрузка:</b> <code>{format_loadavg(loadavg)}</code>",
                f"{get_status_emoji(mem_percent)} <b>Память:</b> {mem_percent}% ({mem.get('used_mb', '—')} / {mem.get('total_mb', '—')} МБ)",
                f"{get_status_emoji(disk_percent)} <b>Диск:</b> {disk_percent}%",
            ]
            

            disks = data.get('disks', [])
            if disks:
                txt.append("")
                txt.append("💾 <b>Диски:</b>")
                for disk in disks[:3]:
                    device = disk.get('device') or disk.get('mountpoint', '—')
                    percent = disk.get('percent', 0) or 0
                    used = disk.get('used', '—')
                    size = disk.get('size', '—')
                    txt.append(f"  {get_status_emoji(percent, 80, 95)} <code>{device}</code>: {percent}% ({used} / {size})")
                if len(disks) > 3:
                    txt.append(f"  ... и еще {len(disks) - 3} дисков")
        

        kb = InlineKeyboardBuilder()
        kb.button(text="🔄 Обновить", callback_data=callback.data)
        kb.button(text="⬅️ Назад", callback_data="admin_monitor")
        kb.adjust(2)
        
        await callback.message.edit_text("\n".join(txt), parse_mode='HTML', reply_markup=kb.as_markup())

    @admin_router.callback_query(F.data == "admin_monitor_detailed")
    async def admin_monitor_detailed(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id):
            await callback.answer("Доступ только для админов", show_alert=True)
            return
        
        await callback.answer("🔄 Получение детальной статистики...")
        data = resource_monitor.get_local_metrics()
        
        if not data.get('ok'):
            txt = [
                "🚨 <b>Детальная статистика - ОШИБКА</b>",
                "",
                f"❌ <code>{data.get('error', 'Неизвестная ошибка')}</code>"
            ]
        else:
            cpu = data.get('cpu') or {}
            mem = data.get('memory') or {}
            sw = data.get('swap') or {}
            net = data.get('net') or {}
            disks = data.get('disks') or []
            
            def format_bytes(bytes_val):
                if bytes_val is None:
                    return "—"
                for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
                    if bytes_val < 1024.0:
                        return f"{bytes_val:.1f} {unit}"
                    bytes_val /= 1024.0
                return f"{bytes_val:.1f} PB"
            
            def format_uptime(seconds):
                if not seconds:
                    return "—"
                days = int(seconds // 86400)
                hours = int((seconds % 86400) // 3600)
                minutes = int((seconds % 3600) // 60)
                if days > 0:
                    return f"{days}д {hours}ч {minutes}м"
                elif hours > 0:
                    return f"{hours}ч {minutes}м"
                else:
                    return f"{minutes}м"
            
            txt = [
                "📊 <b>Детальная статистика панели</b>",
                "",
                "🖥️ <b>Системная информация:</b>",
                f"• <b>Хост:</b> <code>{data.get('hostname', '—')}</code>",
                f"• <b>Платформа:</b> <code>{data.get('platform', '—')}</code>",
                f"• <b>Python:</b> <code>{data.get('python', '—')}</code>",
                f"• <b>Время работы:</b> <code>{format_uptime(data.get('uptime_sec'))}</code>",
                "",
                "⚙️ <b>Процессор:</b>",
                f"• <b>Загрузка:</b> {cpu.get('percent', '—')}%",
                f"• <b>Логических ядер:</b> {cpu.get('count_logical', '—')}",
                f"• <b>Физических ядер:</b> {cpu.get('count_physical', '—')}",
                f"• <b>Средняя загрузка:</b> {', '.join(map(str, cpu.get('loadavg', []))) or '—'}",
                "",
                "🧠 <b>Память:</b>",
                f"• <b>Загрузка памяти:</b> {mem.get('percent', '—')}%",
                f"• <b>Использовано:</b> {format_bytes(mem.get('used'))}",
                f"• <b>Доступно:</b> {format_bytes(mem.get('available'))}",
                f"• <b>Всего:</b> {format_bytes(mem.get('total'))}",
                f"• <b>Загрузка swap:</b> {sw.get('percent', '—')}%",
                f"• <b>Swap использовано:</b> {format_bytes(sw.get('used'))}",
                f"• <b>Swap всего:</b> {format_bytes(sw.get('total'))}",
                "",
                "🌐 <b>Сеть:</b>",
                f"• <b>Отправлено:</b> {format_bytes(net.get('bytes_sent'))} ({net.get('packets_sent', 0):,} пакетов)",
                f"• <b>Получено:</b> {format_bytes(net.get('bytes_recv'))} ({net.get('packets_recv', 0):,} пакетов)",
                f"• <b>Ошибки входящие:</b> {net.get('errin', 0):,}",
                f"• <b>Ошибки исходящие:</b> {net.get('errout', 0):,}",
                f"• <b>Потеряно входящих:</b> {net.get('dropin', 0):,}",
                f"• <b>Потеряно исходящих:</b> {net.get('dropout', 0):,}",
            ]
            

            temps = data.get('temperatures', {})
            if temps:
                txt.append("")
                txt.append("🌡️ <b>Температура:</b>")
                for sensor_name, temp_info in temps.items():
                    current = temp_info.get('current', 0)
                    high = temp_info.get('high', 0)
                    critical = temp_info.get('critical', 0)
                    status_emoji = "🔴" if current >= critical else "🟡" if current >= high else "🟢"
                    txt.append(f"• {status_emoji} <b>{sensor_name}:</b> {current:.1f}°C (критично: {critical:.1f}°C)")
            

            top_processes = data.get('top_processes', [])
            if top_processes:
                txt.append("")
                txt.append("🔄 <b>Топ процессов по процессору:</b>")
                for i, proc in enumerate(top_processes[:5], 1):
                    name = proc.get('name', '—')
                    cpu_p = proc.get('cpu_percent', 0)
                    mem_p = proc.get('memory_percent', 0)
                    pid = proc.get('pid', '—')
                    txt.append(f"  {i}. <code>{name}</code> (PID: {pid})")
                    txt.append(f"     Процессор: {cpu_p:.1f}%, Память: {mem_p:.1f}%")
            

            if disks:
                txt.append("")
                txt.append("💾 <b>Диски:</b>")
                for i, disk in enumerate(disks, 1):
                    mountpoint = disk.get('mountpoint') or disk.get('device', '—')
                    fstype = disk.get('fstype', '—')
                    percent = disk.get('percent', 0) or 0
                    used = format_bytes(disk.get('used'))
                    free = format_bytes(disk.get('free'))
                    total = format_bytes(disk.get('total'))
                    
                    status_emoji = "🔴" if percent >= 95 else "🟡" if percent >= 80 else "🟢"
                    
                    txt.append(f"  {i}. {status_emoji} <code>{mountpoint}</code>")
                    txt.append(f"     Тип: {fstype}")
                    txt.append(f"     Использовано: {percent}% ({used} / {total})")
                    txt.append(f"     Свободно: {free}")
                    if i < len(disks):
                        txt.append("")
        

        kb = InlineKeyboardBuilder()
        kb.button(text="🔄 Обновить", callback_data="admin_monitor_detailed")
        kb.button(text="⬅️ К мониторингу", callback_data="admin_monitor")
        kb.adjust(2)
        
        await callback.message.edit_text("\n".join(txt), parse_mode='HTML', reply_markup=kb.as_markup())

    return admin_router



