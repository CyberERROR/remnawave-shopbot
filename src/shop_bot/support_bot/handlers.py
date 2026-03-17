import logging
from aiogram import Bot, Router, F, types, html
from aiogram.types import FSInputFile
import os
import time
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.enums import ChatMemberStatus
from aiogram.exceptions import TelegramBadRequest

from shop_bot.data_manager.remnawave_repository import (
    get_setting,
    create_support_ticket,
    add_support_message,
    get_user_tickets,
    get_ticket,
    get_ticket_messages,
    set_ticket_status,
    update_ticket_thread_info,
    get_ticket_by_thread,
    get_or_create_open_ticket,
    update_ticket_subject,
    delete_ticket,
    is_admin,
    get_admin_ids,
    get_user,
    ban_user,
    unban_user,
)

logger = logging.getLogger(__name__)

NEW_TICKET_PHOTO_URL = "https://github.com/CyberERROR/remnawave-shopbot/blob/main/docs/screenshots/suppshor.png?raw=true"

class SupportDialog(StatesGroup):
    waiting_for_subject = State()
    waiting_for_message = State()
    waiting_for_reply = State()


class AdminDialog(StatesGroup):
    waiting_for_note = State()
    waiting_for_reply = State()


def _get_username_display(user, user_id: int = None) -> str:
    if hasattr(user, 'username') and user.username:
        return f"@{user.username}"
    if hasattr(user, 'full_name') and user.full_name:
        return user.full_name
    return str(user_id if user_id else (user.id if hasattr(user, 'id') else 'Unknown'))


def _parse_star_subject(subject: str) -> tuple[bool, str]:
    if not subject:
        return False, 'Обращение без темы'
    is_star = subject.strip().startswith('⭐')
    display_subj = subject.lstrip('⭐️ ').strip() if is_star else subject
    return is_star, display_subj or 'Обращение без темы'


def _build_topic_name(ticket_id: int, subject: str, author_tag: str) -> str:
    is_star, display_subj = _parse_star_subject(subject)
    trimmed = display_subj[:40]
    important_prefix = '🔴 Важно: ' if is_star else ''
    return f"#{ticket_id} {important_prefix}{trimmed} • от {author_tag}"


def _get_author_tag(message: types.Message) -> str:
    if not message.from_user:
        return 'пользователь'
    return _get_username_display(message.from_user, message.from_user.id)


def _build_notification_text(ticket_id: int, user_id: int, username_display: str, subject: str, message_content: str, created_new: bool) -> str:
    subj_display = subject or "—"
    header = "🆘 <b>Новое обращение:</b>\n\n" if created_new else "✅ <b>Сообщение добавлено в тикет</b>\n\n"
    return (
        f"{header}"
        f"👤 <b>USER:</b> (<code>{user_id}</code> - {username_display})\n"
        f"📝 <b>ID тикета:</b> <code>#{ticket_id}</code>\n"
        f"💬 <b>Тема:</b> <i>{subj_display}</i>\n\n"
        f"💌 Сообщения:\n"
        f"<blockquote>{message_content}</blockquote>"
    )


def get_support_router() -> Router:
    router = Router()

    # ==========================================
    # 1) UNIVERSAL MESSAGES (CONSTANTS)
    # ==========================================
    TXT_TICKET_NOT_FOUND = "❌ <b>Тикет не найден.</b>"
    TXT_ACCESS_DENIED = "❌ <b>Тикет не найден или доступ запрещён.</b>"
    TXT_CANNOT_REPLY = "❌ <b>Нельзя ответить на этот тикет.</b>"
    TXT_ALREADY_CLOSED = "🔒 <b>Тикет уже закрыт.</b>"
    TXT_ALREADY_OPEN = "⚠️ <b>Тикет уже открыт.</b>"
    TXT_BAN_RESTRICTED = "<b>🚫 Доступ ограничен</b>\n\nВаш аккаунт заблокирован. Вы не можете обращаться в поддержку."
    TXT_BAN_ERROR = "❌ Не удалось изменить статус блокировки: {}"
    
    # ==========================================
    # 2) UNIVERSAL FUNCTIONS (HELPERS)
    # ==========================================
    
    async def _safe_edit(call: types.CallbackQuery, text: str, reply_markup=None):
        try:
            await call.message.edit_text(text, reply_markup=reply_markup)
        except Exception:
            pass

    def _extract_content(message: types.Message) -> str:
        text = (message.text or message.caption or "").strip()
        if message.photo: return f"[Фото] {text}".strip()
        if message.video: return f"[Видео] {text}".strip()
        return text

    def _support_contact_markup() -> types.InlineKeyboardMarkup | None:
        support = (get_setting("support_bot_username") or get_setting("support_user") or "").strip()
        if not support:
            return None
        url: str | None = None
        if support.startswith("@"):
            url = f"tg://resolve?domain={support[1:]}"
        elif support.startswith("tg://"):
            url = support
        elif support.startswith("http://") or support.startswith("https://"):
            try:
                part = support.split("/")[-1].split("?")[0]
                if part:
                    url = f"tg://resolve?domain={part}"
                else:
                    url = support
            except Exception:
                url = support
        else:
            url = f"tg://resolve?domain={support}"
        if not url:
            return None
        return types.InlineKeyboardMarkup(inline_keyboard=[[types.InlineKeyboardButton(text="🆘 Написать в поддержку", url=url)]])

    def _user_main_reply_kb() -> types.ReplyKeyboardMarkup:
        return types.ReplyKeyboardMarkup(
            keyboard=[
                [types.KeyboardButton(text="✍️ Новое обращение")],
                [types.KeyboardButton(text="📨 Мои обращения")],
            ],
            resize_keyboard=True
        )

    def _admin_kb_build(status, ticket_id, user_id, is_banned) -> types.InlineKeyboardMarkup:
        first_row: list[types.InlineKeyboardButton] = []
        if status == 'open':
            first_row.append(types.InlineKeyboardButton(text="✅ Закрыть", callback_data=f"admin_close_{ticket_id}"))
        else:
            first_row.append(types.InlineKeyboardButton(text="🔓 Переоткрыть", callback_data=f"admin_reopen_{ticket_id}"))
        inline_kb = [
            first_row,
            [types.InlineKeyboardButton(text="🗑 Удалить", callback_data=f"admin_delete_{ticket_id}")],
            [
                types.InlineKeyboardButton(text="⭐ Важно", callback_data=f"admin_star_{ticket_id}"),
                types.InlineKeyboardButton(text="👤 Пользователь", callback_data=f"admin_user_{ticket_id}"),
                types.InlineKeyboardButton(text="📝 Заметка", callback_data=f"admin_note_{ticket_id}"),
            ],
            [types.InlineKeyboardButton(text="🗒 Заметки", callback_data=f"admin_notes_{ticket_id}")],
        ]
        if user_id:
            if is_banned:
                inline_kb.append([
                    types.InlineKeyboardButton(text="✅ Разбанить", callback_data=f"admin_unban_user_{ticket_id}")
                ])
            else:
                inline_kb.append([
                    types.InlineKeyboardButton(text="🚫 Забанить", callback_data=f"admin_ban_user_{ticket_id}")
                ])
        return types.InlineKeyboardMarkup(inline_keyboard=inline_kb)

    def _admin_actions_kb(ticket_id: int) -> types.InlineKeyboardMarkup:
        try:
            t = get_ticket(ticket_id)
            status = (t and t.get('status')) or 'open'
        except Exception:
            status = 'open'
        try:
            user_id = int((t or {}).get('user_id')) if t else None
        except Exception:
            user_id = None
        is_banned = None
        if user_id:
            try:
                user_info = get_user(user_id) or {}
                is_banned = bool(user_info.get('is_banned'))
            except Exception:
                is_banned = None
        return _admin_kb_build(status, ticket_id, user_id, is_banned)

    def _admin_dm_reply_kb(ticket_id: int) -> types.InlineKeyboardMarkup:
        return types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="💬 Ответить", callback_data=f"admin_reply_dm_{ticket_id}")]
        ])

    def _is_user_banned(user_id: int) -> bool:
        if not user_id:
            return False
        try:
            user = get_user(int(user_id)) or {}
        except Exception:
            return False
        return bool(user.get('is_banned'))
    
    async def _check_banned(event: types.Message | types.CallbackQuery, state: FSMContext = None) -> bool:
        user_id = event.from_user.id
        if not _is_user_banned(user_id):
            return False
        
        markup = _support_contact_markup()
        if isinstance(event, types.CallbackQuery):
             try:
                 await event.answer(TXT_BAN_RESTRICTED, show_alert=True)
             except Exception:
                 pass
        else:
             if markup:
                 await event.answer(TXT_BAN_RESTRICTED, reply_markup=markup)
             else:
                 await event.answer(TXT_BAN_RESTRICTED)
        
        if state:
            await state.clear()
        return True

    def _get_latest_open_ticket(user_id: int) -> dict | None:
        try:
            tickets = get_user_tickets(user_id) or []
            open_tickets = [t for t in tickets if t.get('status') == 'open']
            if not open_tickets:
                return None
            return max(open_tickets, key=lambda t: int(t['ticket_id']))
        except Exception:
            return None

    async def _check_active_ticket(message: types.Message | types.CallbackQuery, user_id: int) -> bool:
        existing = _get_latest_open_ticket(user_id)
        if existing:
            text = (
                f"⚠️ <b>Активный тикет найден</b>\n\n"
                f"У вас уже есть открытый тикет <b>#{existing['ticket_id']}</b>.\n"
                f"Пожалуйста, продолжайте переписку в нём."
            )
            if isinstance(message, types.CallbackQuery):
                await message.message.edit_text(text)
            else:
                await message.answer(text)
            return True
        return False

    def _cancel_creation_kb() -> types.InlineKeyboardMarkup:
        return types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="❌ Отменить создание", callback_data="support_cancel_creation")]
        ])

    async def _send_subject_prompt(message: types.Message | types.CallbackQuery, state: FSMContext):
        current_state = await state.get_state()
        if current_state in [SupportDialog.waiting_for_subject, SupportDialog.waiting_for_message]:
            text = "⚠️ <b>Вы уже создаете обращение.</b>\nПожалуйста, завершите текущий процесс или отмените его."
            if isinstance(message, types.CallbackQuery):
                await message.answer(text, show_alert=True)
            else:
                await message.answer(text, reply_markup=_cancel_creation_kb())
            return

        text = (
            "📝 <b>Шаг 1/2: Тема обращения</b>\n\n"
            "Напишите <b>краткий заголовок</b> (3-5 слов).\n"
            "<i>Пример: «Не работает VPN», «Проблема с оплатой»</i>\n\n"
            "Подробно описать проблему нужно будет в <b>следующем меню</b>."
        )
        if isinstance(message, types.CallbackQuery):
            await message.message.edit_text(text, reply_markup=_cancel_creation_kb())
        else:
            await message.answer(text, reply_markup=_cancel_creation_kb())
        await state.update_data(start_time=time.time())
        await state.set_state(SupportDialog.waiting_for_subject)
    
    async def _send_user_tickets_list(event: types.Message | types.CallbackQuery, user_id: int):
        tickets = get_user_tickets(user_id)
        text = "<b>📨 Ваши обращения:</b>" if tickets else "<b>📂 У вас пока нет обращений.</b>"
        rows = []
        if tickets:
            for t in tickets:
                status_text = "🟢 Открыт" if t.get('status') == 'open' else "🔒 Закрыт"
                is_star = (t.get('subject') or '').startswith('⭐ ')
                star = '⭐ ' if is_star else ''
                title = f"{star}#{t['ticket_id']} • {status_text}"
                if t.get('subject'):
                    title += f" • {t['subject'][:20]}"
                rows.append([types.InlineKeyboardButton(text=title, callback_data=f"support_view_{t['ticket_id']}")])
        
        reply_markup = types.InlineKeyboardMarkup(inline_keyboard=rows)
        if isinstance(event, types.CallbackQuery):
            await event.message.edit_text(text, reply_markup=reply_markup)
        else:
            await event.answer(text, reply_markup=reply_markup)

    async def _send_ticket_confirmation(message: types.Message, ticket_id: int, subject: str, content_text: str, created_new: bool):
        if created_new:
            text = (
                f"✅ <b>Обращение #{ticket_id} создано!</b>\n\n"
                f"📝 <b>Сообщения:</b>\n"
                f"💬 <b>Тема:</b> <i>{subject}</i>\n"
                f"<blockquote>{content_text}</blockquote>\n\n"
                f"💌 Ожидайте ответа поддержки. Мы скоро свяжемся с вами."
            )
        else:
            text = (
                f"✅ <b>Сообщение добавлено в ваш открытый тикет</b>\n\n"
                f"📝 <b>ID тикета:</b> <code>#{ticket_id}</code>\n\n"
                f"✉️ Сообщения:\n"
                f"<blockquote>{content_text}</blockquote>\n\n"
                f"💌 Ожидайте ответа поддержки. Мы скоро свяжемся с вами."
            )
        try:
            await message.answer(text, reply_markup=_user_main_reply_kb())
        except Exception:
            pass

    async def _send_ticket_closed_notification(bot: Bot, user_id: int, ticket_id: int, is_user_action: bool = False, message_obj: types.Message = None):
        text = (
            f"✅ <b>Ваш тикет #{ticket_id} был закрыт</b>\n\n"
            f"✉️ <i>Если у вас появятся другие вопросы или ваш вопрос не решен</i>\n\n"
            f"💌 <b>Вы можете создать новое обращение при необходимости.</b>"
        )
        
        try:
             if is_user_action and message_obj:
                 await message_obj.edit_text(
                     text,
                     reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[[types.InlineKeyboardButton(text="⬅️ К списку", callback_data="support_my_tickets")]])
                 )
             else:
                 await bot.send_message(chat_id=user_id, text=text)
        except Exception:
             pass

    async def _send_admin_reply_to_user(bot: Bot, user_id: int, ticket_id: int, message: types.Message, content: str):
        full_text = (
            f"💬 <b>Ответ от технической поддержки.</b>\n"
            f"📝 <b>ID тикета:</b> <code>#{ticket_id}</code>\n\n"
            f"💌 <b>Ответ на ваше обращение:</b>\n"
            f"<blockquote>{content}</blockquote>" 
        )
        try:
            if message.text:
                await bot.send_message(chat_id=user_id, text=full_text)
            else:
                 await bot.copy_message(
                    chat_id=user_id,
                    from_chat_id=message.chat.id,
                    message_id=message.message_id,
                    caption=full_text
                )
        except Exception as e:
            logger.warning(f"Failed to send reply to user {user_id}: {e}")
            raise e

    async def _notify_admins(bot: Bot, message: types.Message, ticket_id: int, subject: str = None, created_new: bool = False):
        username_display = _get_username_display(message.from_user, message.from_user.id)
        message_content = message.text or message.caption or ("[Фото]" if message.photo else "[Видео]" if message.video else "")
        notification_text = _build_notification_text(ticket_id, message.from_user.id, username_display, subject, message_content, created_new)
        
        
        for aid in get_admin_ids():
            try:
                if message.text or (message.caption and not created_new):
                    send_method = bot.send_photo if created_new else bot.send_message
                    
                    photo_to_send = NEW_TICKET_PHOTO_URL
                    if created_new and message.photo:
                        photo_to_send = message.photo[-1].file_id

                    await send_method(
                        chat_id=int(aid),
                        **(({"photo": photo_to_send, "caption": notification_text} if created_new else {"text": notification_text})),
                        reply_markup=_admin_dm_reply_kb(ticket_id)
                    )
                else:
                    if created_new:
                        photo_to_send = NEW_TICKET_PHOTO_URL
                        if message.photo:
                            photo_to_send = message.photo[-1].file_id
                            
                        await bot.send_photo(
                            chat_id=int(aid),
                            photo=photo_to_send,
                            caption=notification_text,
                            reply_markup=_admin_dm_reply_kb(ticket_id)
                        )
                    else:
                        await bot.copy_message(
                            chat_id=int(aid),
                            from_chat_id=message.chat.id,
                            message_id=message.message_id,
                            caption=notification_text,
                            reply_markup=_admin_dm_reply_kb(ticket_id)
                        )
            except Exception as e:
                logger.warning(f"Не удалось уведомить админа {aid} о тикете {ticket_id}: {e}")

    async def _notify_user_about_ban(bot: Bot, user_id: int, text: str) -> None:
        try:
            markup = _support_contact_markup()
            if markup:
                await bot.send_message(user_id, text, reply_markup=markup)
            else:
                await bot.send_message(user_id, text)
        except Exception:
            pass

    async def _ensure_forum_topic(bot: Bot, ticket_id: int, subject: str, message_from: types.User) -> tuple[int | None, int | None]:
        ticket = get_ticket(ticket_id)
        if not ticket:
            return None, None
            
        forum_chat_id = ticket.get('forum_chat_id')
        thread_id = ticket.get('message_thread_id')
        support_forum_chat_id = get_setting("support_forum_chat_id")
        
        if support_forum_chat_id and not (forum_chat_id and thread_id):
            try:
                chat_id = int(support_forum_chat_id)
                author_tag = _get_username_display(message_from, message_from.id)
                topic_name = _build_topic_name(ticket_id, subject or 'Обращение без темы', author_tag)
                
                forum_topic = await bot.create_forum_topic(chat_id=chat_id, name=topic_name)
                thread_id = forum_topic.message_thread_id
                forum_chat_id = chat_id
                update_ticket_thread_info(ticket_id, str(chat_id), int(thread_id))
                return int(forum_chat_id), int(thread_id)
            except Exception as e:
                error_msg = str(e).lower()
                if 'not a forum' in error_msg or 'chat_not_found' in error_msg:
                    logger.debug(f"Форум не настроен: {error_msg}")
                else:
                    logger.warning(f"Не удалось создать тему форума: {e}")
                return None, None

        if forum_chat_id and thread_id:
            try:
                author_tag = _get_username_display(message_from, message_from.id)
                topic_name = _build_topic_name(ticket_id, subject or 'Обращение без темы', author_tag)
                await bot.edit_forum_topic(chat_id=int(forum_chat_id), message_thread_id=int(thread_id), name=topic_name)
                return int(forum_chat_id), int(thread_id)
            except Exception as e:
                logger.warning(f"Не удалось переименовать тему форума: {e}")
                return int(forum_chat_id), int(thread_id)
        
        return None, None

    async def _mirror_to_forum(bot: Bot, message: types.Message, ticket_id: int, forum_chat_id: int, thread_id: int, subject: str = None, created_new: bool = False):
        try:
            username_display = _get_username_display(message.from_user, message.from_user.id)
            text_header = _build_notification_text(ticket_id, message.from_user.id, username_display, subject, "", created_new).split("💌 Сообщения:")[0] + "💌 Сообщения:"
            kb = _admin_actions_kb(ticket_id) if created_new else None

            await bot.send_message(
                chat_id=int(forum_chat_id),
                text=text_header,
                message_thread_id=int(thread_id),
                reply_markup=kb
            )
            await bot.copy_message(
                chat_id=int(forum_chat_id), 
                from_chat_id=message.chat.id, 
                message_id=message.message_id, 
                message_thread_id=int(thread_id)
            )
        except Exception as e:
            logger.warning(f"Не удалось отзеркалить сообщение пользователя в форум: {e}")

    async def _manage_forum_topic(bot: Bot, ticket: dict, action: str):
        """Action: close, reopen, delete"""
        chat_id = ticket.get('forum_chat_id')
        tid = ticket.get('message_thread_id')
        if not (chat_id and tid): return
        try:
            if action == 'close': await bot.close_forum_topic(chat_id=int(chat_id), message_thread_id=int(tid))
            elif action == 'reopen': await bot.reopen_forum_topic(chat_id=int(chat_id), message_thread_id=int(tid))
            elif action == 'delete': await bot.delete_forum_topic(chat_id=int(chat_id), message_thread_id=int(tid))
        except Exception:
            pass

    async def _process_ticket_message_flow(bot: Bot, message: types.Message, state: FSMContext, ticket_id: int, subject: str, created_new: bool):
        content = _extract_content(message)
        add_support_message(ticket_id, sender="user", content=content)
        
        forum_chat_id, thread_id = await _ensure_forum_topic(bot, ticket_id, subject, message.from_user)
        if forum_chat_id and thread_id:
             await _mirror_to_forum(bot, message, ticket_id, forum_chat_id, thread_id, subject=subject, created_new=created_new)

        await _send_ticket_confirmation(message, ticket_id, subject, content, created_new)
        await _notify_admins(bot, message, ticket_id, subject=subject, created_new=created_new)
        if state: await state.clear()

    async def _change_ticket_status_common(bot: Bot, call: types.CallbackQuery, ticket_id: int, new_status: str, is_admin: bool):
        action_name = "закрыть" if new_status == 'closed' else "переоткрыть"
        ticket = get_ticket(ticket_id)
        if not ticket:
             if is_admin: return 
             await _safe_edit(call, TXT_ACCESS_DENIED)
             return

        if not is_admin:
            if ticket.get('user_id') != call.from_user.id:
                await _safe_edit(call, TXT_ACCESS_DENIED)
                return
            if ticket.get('status') == new_status:
                await _safe_edit(call, TXT_ALREADY_CLOSED if new_status == 'closed' else TXT_ALREADY_OPEN)
                return

        if set_ticket_status(ticket_id, new_status):
            await _manage_forum_topic(bot, ticket, 'close' if new_status == 'closed' else 'reopen')
            if is_admin:
                 user_id = int(ticket.get('user_id'))
                 await _send_ticket_closed_notification(bot, user_id, ticket_id, is_user_action=False)
                 status_text = f"✅ <b>Тикет #{ticket_id} закрыт.</b>" if new_status == 'closed' else f"🔓 <b>Тикет #{ticket_id} переоткрыт.</b>"
                 try:
                    await call.message.edit_text(status_text, reply_markup=_admin_actions_kb(ticket_id))
                 except Exception:
                    await call.answer("Без изменений")
            else:
                 username = _get_username_display(call.from_user, call.from_user.id)
                 try:
                    if ticket.get('forum_chat_id') and ticket.get('message_thread_id'):
                        await bot.send_message(chat_id=int(ticket['forum_chat_id']), text=f"✅ Пользователь {username} закрыл тикет #{ticket_id}.", message_thread_id=int(ticket['message_thread_id']))
                        await bot.send_message(chat_id=int(ticket['forum_chat_id']), text="Панель управления тикетом:", message_thread_id=int(ticket['message_thread_id']), reply_markup=_admin_actions_kb(ticket_id))
                 except Exception: pass
                 
                 await _send_ticket_closed_notification(bot, call.from_user.id, ticket_id, is_user_action=True, message_obj=call.message)
                 try: await call.message.answer("Меню поддержки:", reply_markup=_user_main_reply_kb())
                 except Exception: pass
        else:
            if is_admin: await call.message.answer(f"❌ Не удалось {action_name} тикет.")
            else: await call.message.edit_text(f"<b>❌ Ошибка</b>\nНе удалось {action_name} тикет.")

    async def _get_ticket_and_check_admin(callback: types.CallbackQuery, bot: Bot) -> tuple[dict | None, int | None]:
        try:
            ticket_id = int(callback.data.split("_")[-1])
        except Exception:
            return None, None
        
        ticket = get_ticket(ticket_id)
        if not ticket:
            await _safe_edit(callback, TXT_TICKET_NOT_FOUND)
            return None, None
            
        forum_chat_id = int(ticket.get('forum_chat_id') or callback.message.chat.id)
        
        is_admin_by_setting = is_admin(callback.from_user.id)
        is_admin_in_chat = False
        try:
            member = await bot.get_chat_member(chat_id=forum_chat_id, user_id=callback.from_user.id)
            is_admin_in_chat = member.status in [ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR]
        except Exception:
            pass
            
        if not (is_admin_by_setting or is_admin_in_chat):
             return None, None
             
        return ticket, ticket_id

    async def _start_ticket_creation_flow(event: types.Message | types.CallbackQuery, state: FSMContext):
         if isinstance(event, types.CallbackQuery):
             await event.answer()
         if await _check_banned(event, state):
             return
         if await _check_active_ticket(event, event.from_user.id):
             return
         await _send_subject_prompt(event, state)

    @router.message(CommandStart(), F.chat.type == "private")
    async def start_handler(message: types.Message, state: FSMContext, bot: Bot):
        args = (message.text or "").split(maxsplit=1)
        arg = None
        if len(args) > 1:
            arg = args[1].strip()
        if arg == "new":
            await _start_ticket_creation_flow(message, state)
            return
        if await _check_banned(message, state):
            return

        support_text = get_setting("support_text") or "<b>👨‍💻 Поддержка</b>\n\nЗдесь вы можете создать обращение или посмотреть историю своих заявок."
        await message.answer(
            support_text,
            reply_markup=types.ReplyKeyboardMarkup(
                keyboard=[
                    [types.KeyboardButton(text="✍️ Новое обращение")],
                    [types.KeyboardButton(text="📨 Мои обращения")],
                ],
                resize_keyboard=True
            ),
        )

    @router.callback_query(F.data == "support_new_ticket")
    async def support_new_ticket_handler(callback: types.CallbackQuery, state: FSMContext):
        await _start_ticket_creation_flow(callback, state)

    @router.callback_query(F.data == "support_cancel_creation")
    async def support_cancel_creation_handler(callback: types.CallbackQuery, state: FSMContext):
        await state.clear()
        await callback.answer("✅ Создание обращения отменено.")
        
        support_text = get_setting("support_text") or "<b>👨‍💻 Поддержка</b>\n\nЗдесь вы можете создать обращение или посмотреть историю своих заявок."
        await callback.message.answer(
            support_text,
            reply_markup=_user_main_reply_kb()
        )
        try:
            await callback.message.delete()
        except Exception:
            pass

    @router.message(SupportDialog.waiting_for_subject, F.chat.type == "private")
    async def support_subject_received(message: types.Message, state: FSMContext):
        data = await state.get_data()
        start_time = data.get("start_time", 0)
        if time.time() - start_time > 900:
            await state.clear()
            await _start_ticket_creation_flow(message, state)
            return

        if await _check_banned(message, state):
            return

        subject = (message.text or "").strip()
        if subject in ["✍️ Новое обращение", "📨 Мои обращения"]:
            await _send_subject_prompt(message, state)
            return

        if len(subject) > 50:
            await message.answer(
                "⚠️ <b>Заголовок слишком длинный!</b>\n\n"
                "Пожалуйста, опишите тему кратко (до 50 символов).\n"
                "Подробности вы сможете написать на следующем шаге."
            )
            return

        await state.update_data(subject=subject, start_time=time.time())
        await message.answer(
            "✉️ <b>Шаг 2/2: Описание проблемы</b>\n\n"
            "Теперь максимально подробно опишите ситуацию <b>одним сообщением</b>.\n"
            "<i>Можете прикрепить скриншот или видео.</i>",
            reply_markup=_cancel_creation_kb()
        )
        await state.set_state(SupportDialog.waiting_for_message)

    @router.message(SupportDialog.waiting_for_message, F.chat.type == "private")
    async def support_message_received(message: types.Message, state: FSMContext, bot: Bot):
        data = await state.get_data()
        start_time = data.get("start_time", 0)
        if time.time() - start_time > 900:
            await state.clear()
            await _start_ticket_creation_flow(message, state)
            return

        if await _check_banned(message, state):
            return
        
        text_content = (message.text or "").strip()
        if text_content in ["✍️ Новое обращение", "📨 Мои обращения"]:
            await _send_subject_prompt(message, state)
            return

        user_id = message.from_user.id
        raw_subject = (data.get("subject") or "").strip()
        subject = raw_subject if raw_subject else "Обращение без темы"
        ticket_id, created_new = get_or_create_open_ticket(user_id, subject)
        if not ticket_id:
            await message.answer("❌ Не удалось создать обращение. Попробуйте позже.")
            await state.clear()
            return

        await _process_ticket_message_flow(bot, message, state, ticket_id, subject, created_new)
        
    @router.callback_query(F.data == "support_my_tickets")
    async def support_my_tickets_handler(callback: types.CallbackQuery, state: FSMContext):
        current_state = await state.get_state()
        if current_state in [SupportDialog.waiting_for_subject, SupportDialog.waiting_for_message]:
            await callback.answer("⚠️ Пожалуйста, сначала завершите создание обращения.", show_alert=True)
            return
        await callback.answer()
        await _send_user_tickets_list(callback, callback.from_user.id)

    @router.callback_query(F.data.startswith("support_view_"))
    async def support_view_ticket_handler(callback: types.CallbackQuery):
        await callback.answer()
        ticket_id = int(callback.data.split("_")[-1])
        ticket = get_ticket(ticket_id)
        if not ticket or ticket.get('user_id') != callback.from_user.id:
            await _safe_edit(callback, TXT_ACCESS_DENIED)
            return
        messages = get_ticket_messages(ticket_id)
        human_status = "🟢 Открыт" if ticket.get('status') == 'open' else "🔒 Закрыт"
        is_star = (ticket.get('subject') or '').startswith('⭐ ')
        star_line = "⭐ Важно" if is_star else "—"
        parts = [
            f"<b>🧾 Тикет #{ticket_id}</b>",
            f"<b>Статус:</b> {human_status}",
            f"<b>Тема:</b> <i>{ticket.get('subject') or '—'}</i>",
            f"<b>Важность:</b> {star_line}",
            ""
        ]
        for m in messages:
            if m.get('sender') == 'note':
                continue
            
            is_user = m.get('sender') == 'user'
            icon = "👤" if is_user else "👨‍💻"
            who = "<b>Вы</b>" if is_user else "<b>Поддержка</b>"
            created = m.get('created_at')
            content = m.get('content', '')
            
            parts.append(f"{icon} {who} ({created}):")
            parts.append(f"<blockquote>{content}</blockquote>")
            
        final_text = "\n".join(parts)
        is_open = (ticket.get('status') == 'open')
        buttons = []
        if is_open:
            buttons.append([types.InlineKeyboardButton(text="💬 Ответить", callback_data=f"support_reply_{ticket_id}")])
            buttons.append([types.InlineKeyboardButton(text="✅ Закрыть", callback_data=f"support_close_{ticket_id}")])
        buttons.append([types.InlineKeyboardButton(text="⬅️ К списку", callback_data="support_my_tickets")])
        await callback.message.edit_text(final_text, reply_markup=types.InlineKeyboardMarkup(inline_keyboard=buttons))

    @router.callback_query(F.data.startswith("support_reply_"))
    async def support_reply_prompt_handler(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer()
        ticket_id = int(callback.data.split("_")[-1])
        ticket = get_ticket(ticket_id)
        if await _check_banned(callback, state):
             markup = _support_contact_markup()
             if markup:
                await callback.message.edit_text(TXT_BAN_RESTRICTED, reply_markup=markup)
             else:
                await callback.message.edit_text(TXT_BAN_RESTRICTED)
             return
        if not ticket or ticket.get('user_id') != callback.from_user.id or ticket.get('status') != 'open':
            await _safe_edit(callback, TXT_CANNOT_REPLY)
            return
        await state.update_data(reply_ticket_id=ticket_id)
        await callback.message.edit_text(
            "<b>💬 Введите ваш ответ</b>\n\n"
            "Напишите сообщение, которое вы хотите отправить.\n"
            "<i>Вы можете прикрепить фото или видео.</i>"
        )
        await state.set_state(SupportDialog.waiting_for_reply)

    @router.message(SupportDialog.waiting_for_reply, F.chat.type == "private")
    async def support_reply_received(message: types.Message, state: FSMContext, bot: Bot):
        if await _check_banned(message, state):
            return
        data = await state.get_data()
        ticket_id = data.get('reply_ticket_id')
        ticket = get_ticket(ticket_id)
        if not ticket or ticket.get('user_id') != message.from_user.id or ticket.get('status') != 'open':
            await message.answer(TXT_CANNOT_REPLY)
            await state.clear()
            return
        
        await _process_ticket_message_flow(bot, message, state, ticket_id, ticket.get('subject'), created_new=False)

    @router.message(F.is_topic_message == True)
    async def forum_thread_message_handler(message: types.Message, bot: Bot, state: FSMContext):
        try:
            if not message.message_thread_id:
                return
            forum_chat_id = message.chat.id
            thread_id = message.message_thread_id
            ticket = get_ticket_by_thread(str(forum_chat_id), int(thread_id))
            if not ticket:
                return
            user_id = int(ticket.get('user_id'))
            try:
                current_state = await state.get_state()
                if current_state == AdminDialog.waiting_for_note.state:
                    note_body = (message.text or message.caption or '').strip()
                    author_id = message.from_user.id if message.from_user else None
                    if author_id:
                        username = _get_username_display(message.from_user, author_id)
                        note_text = f"[Заметка от {username} (ID: {author_id})]\n{note_body}"
                    else:
                        note_text = note_body
                    add_support_message(int(ticket['ticket_id']), sender='note', content=note_text)
                    await message.answer("✅ <b>Внутренняя заметка сохранена.</b>")
                    await state.clear()
                    return
            except Exception:
                pass
            me = await bot.get_me()
            if message.from_user and message.from_user.id == me.id:
                return

            is_admin_by_setting = is_admin(message.from_user.id)
            is_admin_in_chat = False
            try:
                member = await bot.get_chat_member(chat_id=forum_chat_id, user_id=message.from_user.id)
                is_admin_in_chat = member.status in [ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR]
            except Exception:
                pass
            if not (is_admin_by_setting or is_admin_in_chat):
                return
            content = (message.text or message.caption or "").strip()
            if content:
                add_support_message(ticket_id=int(ticket['ticket_id']), sender='admin', content=content)
            await _send_admin_reply_to_user(bot, user_id, int(ticket['ticket_id']), message, content)
        except Exception as e:
            logger.warning(f"Не удалось передать сообщение темы форума: {e}")

    @router.callback_query(F.data.startswith("support_close_"))
    async def support_close_ticket_handler(callback: types.CallbackQuery, bot: Bot):
        await callback.answer()
        ticket_id = int(callback.data.split("_")[-1])
        await _change_ticket_status_common(bot, callback, ticket_id, 'closed', is_admin=False)

    @router.callback_query(F.data.startswith("admin_close_"))
    async def admin_close_ticket(callback: types.CallbackQuery, bot: Bot):
        await callback.answer()
        ticket, ticket_id = await _get_ticket_and_check_admin(callback, bot)
        if not ticket: return
        await _change_ticket_status_common(bot, callback, ticket_id, 'closed', is_admin=True)

    @router.callback_query(F.data.startswith("admin_reopen_"))
    async def admin_reopen_ticket(callback: types.CallbackQuery, bot: Bot):
        await callback.answer()
        ticket, ticket_id = await _get_ticket_and_check_admin(callback, bot)
        if not ticket: return
        await _change_ticket_status_common(bot, callback, ticket_id, 'open', is_admin=True)

    @router.callback_query(F.data.startswith("admin_delete_"))
    async def admin_delete_ticket(callback: types.CallbackQuery, bot: Bot):
        await callback.answer()
        ticket, ticket_id = await _get_ticket_and_check_admin(callback, bot)
        if not ticket: return
        
        await _safe_edit(callback, f"🗑 Удаляю тикет #{ticket_id}...")
        await _manage_forum_topic(bot, ticket, 'delete')
        
        if delete_ticket(ticket_id):
            await callback.answer(f"🗑 Тикет #{ticket_id} удалён.", show_alert=False)
        else:
            await callback.answer("❌ Не удалось удалить тикет.", show_alert=True)

    @router.callback_query(F.data.startswith("admin_star_"))
    async def admin_toggle_star(callback: types.CallbackQuery, bot: Bot):
        await callback.answer()
        ticket, ticket_id = await _get_ticket_and_check_admin(callback, bot)
        if not ticket:
            return
        forum_chat_id = int(ticket.get('forum_chat_id') or callback.message.chat.id)
        subject = (ticket.get('subject') or '').strip()
        is_starred = subject.startswith("⭐ ")
        if is_starred:
            base_subject = subject[2:].strip()
            new_subject = base_subject if base_subject else "Обращение без темы"
        else:
            base_subject = subject if subject else "Обращение без темы"
            new_subject = f"⭐ {base_subject}"
        if update_ticket_subject(ticket_id, new_subject):
            try:
                thread_id = ticket.get('message_thread_id')
                if thread_id and ticket.get('forum_chat_id'):
                    user_id = int(ticket.get('user_id')) if ticket.get('user_id') else None
                    author_tag = None
                    if user_id:
                        try:
                            user = await bot.get_chat(user_id)
                            username = getattr(user, 'username', None)
                            author_tag = f"@{username}" if username else f"ID {user_id}"
                        except Exception:
                            author_tag = f"ID {user_id}"
                    else:
                        author_tag = "пользователь"
                    is_star2, display_subj2 = _parse_star_subject(new_subject)
                    topic_name = _build_topic_name(ticket_id, new_subject, author_tag)
                    await bot.edit_forum_topic(chat_id=int(ticket['forum_chat_id']), message_thread_id=int(thread_id), name=topic_name)
            except Exception:
                pass
            try:
                thread_id = ticket.get('message_thread_id')
                forum_chat_id = ticket.get('forum_chat_id')
                if thread_id and forum_chat_id:
                    state_text = "включена" if not is_starred else "снята"
                    msg = await bot.send_message(
                        chat_id=int(forum_chat_id),
                        message_thread_id=int(thread_id),
                        text=f"⭐ Важность {state_text} для тикета #{ticket_id}."
                    )
                    if not is_starred:
                        try:
                            await bot.pin_chat_message(chat_id=int(forum_chat_id), message_id=msg.message_id, disable_notification=True)
                        except Exception:
                            pass
                    else:
                        try:
                            await bot.unpin_all_forum_topic_messages(chat_id=int(forum_chat_id), message_thread_id=int(thread_id))
                        except Exception:
                            pass
            except Exception:
                pass
            state_text = "включена" if not is_starred else "снята"
            await callback.message.answer(f"✅ <b>Пометка важности {state_text}.</b>\nНазвание темы обновлено.")
        else:
            await callback.message.answer("❌ Не удалось обновить тему тикета.")

    @router.callback_query(F.data.startswith("admin_user_"))
    async def admin_show_user(callback: types.CallbackQuery, bot: Bot):
        await callback.answer()
        ticket, ticket_id = await _get_ticket_and_check_admin(callback, bot)
        if not ticket:
            return
        forum_chat_id = int(ticket.get('forum_chat_id') or callback.message.chat.id)

        is_banned = None
        try:
            uinfo = get_user(int(ticket.get('user_id'))) or {}
            is_banned = bool(uinfo.get('is_banned'))
        except Exception:
            pass

        statuses = {
            'open': '🟢 Открыт',
            'closed': '🔴 Закрыт'
        }
        st_text = statuses.get(ticket.get('status'), ticket.get('status'))
        
        user_id_val = ticket.get('user_id')
        username_val = "Неизвестно"
        try:
            if user_id_val:
                u_obj = await bot.get_chat(int(user_id_val))
                username_val = _get_username_display(u_obj, user_id_val)
        except Exception:
            pass
            
        ban_status_text = "🚫 ЗАБАНЕН" if is_banned else "✅ Активен"

        text = (
            f"👤 Информация о пользователе тикета #{ticket_id}\n"
            f"User ID: <code>{user_id_val}</code>\n"
            f"Username: {username_val}\n"
            f"Статус тикета: {st_text}\n"
            f"Статус аккаунта: {ban_status_text}"
        )
        await callback.message.edit_text(text, reply_markup=_admin_actions_kb(ticket_id))

    @router.callback_query(F.data.startswith("admin_reply_dm_"))
    async def admin_reply_dm_handler(callback: types.CallbackQuery, state: FSMContext, bot: Bot):
        await callback.answer()
        ticket, ticket_id = await _get_ticket_and_check_admin(callback, bot)
        if not ticket:
             return
             
        await state.update_data(admin_reply_ticket_id=ticket_id)
        await callback.message.answer(
            f"💬 Введите ответ для пользователя по тикету #{ticket_id}:",
            reply_markup=types.ForceReply(selective=True)
        )
        await state.set_state(AdminDialog.waiting_for_reply)

    @router.message(AdminDialog.waiting_for_reply)
    async def admin_reply_message_handler(message: types.Message, state: FSMContext, bot: Bot):
        data = await state.get_data()
        ticket_id = data.get('admin_reply_ticket_id')
        if not ticket_id:
            await message.answer("❌ <b>Ошибка: контекст ответа потерян.</b>")
            await state.clear()
            return
            
        content = (message.text or message.caption or "").strip()
        if not content:
            await message.answer("⚠️ <b>Сообщение не может быть пустым.</b>")
            return
            
        ticket = get_ticket(ticket_id)
        if not ticket:
            await message.answer(TXT_TICKET_NOT_FOUND)
            await state.clear()
            return

        user_id = int(ticket['user_id'])
        
        add_support_message(ticket_id=ticket_id, sender='admin', content=content)
        
        try:
            await _send_admin_reply_to_user(bot, user_id, ticket_id, message, content)
        except Exception as e:
            logger.warning(f"Failed to send reply to user {user_id}: {e}")
            await message.answer("❌ Не удалось доставить сообщение пользователю (возможно, он заблокировал бота).")
            
        try:
            forum_chat_id = ticket.get('forum_chat_id')
            thread_id = ticket.get('message_thread_id')
            if forum_chat_id and thread_id:
                 admin_tag = _get_username_display(message.from_user, message.from_user.id)
                 await bot.send_message(
                    chat_id=int(forum_chat_id),
                    text=f"👨‍💻 Ответ администратора {admin_tag} через ЛС бота:\n\n{content}",
                    message_thread_id=int(thread_id)
                 )
        except Exception as e:
            logger.warning(f"Failed to mirror admin reply to forum: {e}")

        await message.answer("✅ <b>Сообщение успешно отправлено пользователю.</b>")
        await state.clear()

    @router.callback_query(F.data.startswith("admin_ban_user_"))
    async def admin_ban_user(callback: types.CallbackQuery, bot: Bot):
        await callback.answer()
        ticket, ticket_id = await _get_ticket_and_check_admin(callback, bot)
        if not ticket:
            return
        forum_chat_id = int(ticket.get('forum_chat_id') or callback.message.chat.id)
        try:
            user_id = int(ticket.get('user_id'))
        except Exception:
            await callback.message.answer("❌ Не удалось определить пользователя тикета.")
            return
        try:
            ban_user(user_id)
        except Exception as e:
            await callback.message.answer(f"❌ Не удалось забанить пользователя: {e}")
            return
        await callback.message.answer(f"🚫 <b>Пользователь {user_id} забанен.</b>")

        await _notify_user_about_ban(bot, user_id, "🚫 Ваш аккаунт был заблокирован администратором. Если это ошибка — свяжитесь с поддержкой.")
        try:
            await callback.message.edit_reply_markup(reply_markup=_admin_actions_kb(ticket_id))
        except TelegramBadRequest:
            pass

    @router.callback_query(F.data.startswith("admin_unban_user_"))
    async def admin_unban_user(callback: types.CallbackQuery, bot: Bot):
        await callback.answer()
        ticket, ticket_id = await _get_ticket_and_check_admin(callback, bot)
        if not ticket:
             return
        forum_chat_id = int(ticket.get('forum_chat_id') or callback.message.chat.id)
        try:
            user_id = int(ticket.get('user_id'))
        except Exception:
            await callback.message.answer("❌ Не удалось определить пользователя тикета.")
            return
        try:
            unban_user(user_id)
        except Exception as e:
            await callback.message.answer(f"❌ Не удалось разбанить пользователя: {e}")
            return
        await callback.message.answer(f"✅ <b>Пользователь {user_id} разбанен.</b>")

        try:
            await bot.send_message(user_id, "✅ Ваш аккаунт был разблокирован. Вы снова можете пользоваться ботом.")
        except Exception:
            pass
        try:
            await callback.message.edit_reply_markup(reply_markup=_admin_actions_kb(ticket_id))
        except TelegramBadRequest:
            pass

    @router.callback_query(F.data.startswith("admin_note_"))
    async def admin_note_prompt(callback: types.CallbackQuery, state: FSMContext, bot: Bot):
        await callback.answer()
        ticket, ticket_id = await _get_ticket_and_check_admin(callback, bot)
        if not ticket:
             return
        forum_chat_id = int(ticket.get('forum_chat_id') or callback.message.chat.id)
        await state.update_data(note_ticket_id=ticket_id)
        await callback.message.answer("📝 <b>Отправьте внутреннюю заметку одним сообщением.</b>\nОна не будет отправлена пользователю.")
        await state.set_state(AdminDialog.waiting_for_note)

    @router.callback_query(F.data.startswith("admin_notes_"))
    async def admin_list_notes(callback: types.CallbackQuery, bot: Bot):
        await callback.answer()
        ticket, ticket_id = await _get_ticket_and_check_admin(callback, bot)
        if not ticket:
            return
        forum_chat_id = int(ticket.get('forum_chat_id') or callback.message.chat.id)
        notes = [m for m in get_ticket_messages(ticket_id) if m.get('sender') == 'note']
        if not notes:
            await callback.message.answer("🗒 <b>Внутренних заметок пока нет.</b>")
            return
        lines = [f"🗒 Заметки по тикету #{ticket_id}:"]
        for m in notes:
            created = m.get('created_at')
            content = (m.get('content') or '').strip()
            lines.append(f"— ({created})\n{content}")
        text = "\n\n".join(lines)
        await callback.message.answer(text)

    @router.message(AdminDialog.waiting_for_note, F.is_topic_message == True)
    async def admin_note_receive(message: types.Message, state: FSMContext):
        data = await state.get_data()
        ticket_id = data.get('note_ticket_id')
        if not ticket_id:
            await message.answer("❌ Не найден контекст тикета для заметки.")
            await state.clear()
            return
        author_id = message.from_user.id if message.from_user else None
        username = _get_username_display(message.from_user, author_id) if message.from_user else None
        note_body = (message.text or message.caption or '').strip()
        note_text = f"[Заметка от {username} (ID: {author_id})]\n{note_body}" if author_id else note_body
        add_support_message(int(ticket_id), sender='note', content=note_text)
        await message.answer("✅ <b>Внутренняя заметка сохранена.</b>")
        await state.clear()

    @router.message(F.text == "▶️ Начать", F.chat.type == "private")
    async def start_text_button(message: types.Message, state: FSMContext):
        await _start_ticket_creation_flow(message, state)

    @router.message(F.text == "✍️ Новое обращение", F.chat.type == "private")
    async def new_ticket_text_button(message: types.Message, state: FSMContext):
        await _start_ticket_creation_flow(message, state)

    @router.message(F.text == "📨 Мои обращения", F.chat.type == "private")
    async def my_tickets_text_button(message: types.Message, state: FSMContext):
        current_state = await state.get_state()
        if current_state in [SupportDialog.waiting_for_subject, SupportDialog.waiting_for_message]:
            await message.answer("⚠️ <b>Вы уже создаете обращение.</b>\nПожалуйста, завершите текущий процесс.")
            return
        await _send_user_tickets_list(message, message.from_user.id)

    @router.message(F.chat.type == "private")
    async def relay_user_message_to_forum(message: types.Message, bot: Bot, state: FSMContext):
        current_state = await state.get_state()
        if current_state is not None:
            return

        user_id = message.from_user.id if message.from_user else None
        if not user_id:
            return

        if await _check_banned(message, state):
            return

        content = (message.text or message.caption or '')
        ticket_id, created_new = get_or_create_open_ticket(user_id, None)
        if not ticket_id:
            return
        
        ticket = get_ticket(ticket_id)
        subject = ticket.get('subject') if ticket else None
        
        await _process_ticket_message_flow(bot, message, state, ticket_id, subject, created_new=created_new)

    return router
