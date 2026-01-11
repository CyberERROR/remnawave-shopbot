import logging
from aiogram import Bot, Router, F, types, html
from aiogram.types import FSInputFile
import os
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

class SupportDialog(StatesGroup):
    waiting_for_subject = State()
    waiting_for_message = State()
    waiting_for_reply = State()


class AdminDialog(StatesGroup):
    waiting_for_note = State()
    waiting_for_reply = State()


def get_support_router() -> Router:
    router = Router()

    def _user_main_reply_kb() -> types.ReplyKeyboardMarkup:
        return types.ReplyKeyboardMarkup(
            keyboard=[
                [types.KeyboardButton(text="✍️ Новое обращение")],
                [types.KeyboardButton(text="📨 Мои обращения")],
            ],
            resize_keyboard=True
        )

    def _is_user_banned(user_id: int) -> bool:
        if not user_id:
            return False
        try:
            user = get_user(int(user_id)) or {}
        except Exception:
            return False
        return bool(user.get('is_banned'))

    def _get_latest_open_ticket(user_id: int) -> dict | None:
        try:
            tickets = get_user_tickets(user_id) or []
            open_tickets = [t for t in tickets if t.get('status') == 'open']
            if not open_tickets:
                return None
            return max(open_tickets, key=lambda t: int(t['ticket_id']))
        except Exception:
            return None

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

    def _admin_dm_reply_kb(ticket_id: int) -> types.InlineKeyboardMarkup:
        return types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="💬 Ответить", callback_data=f"admin_reply_dm_{ticket_id}")]
        ])

    async def _is_admin(bot: Bot, chat_id: int, user_id: int) -> bool:
        is_admin_by_setting = is_admin(user_id)
        is_admin_in_chat = False
        try:
            member = await bot.get_chat_member(chat_id=chat_id, user_id=user_id)
            is_admin_in_chat = member.status in [ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR]
        except Exception:
            pass
        return bool(is_admin_by_setting or is_admin_in_chat)

    @router.message(CommandStart(), F.chat.type == "private")
    async def start_handler(message: types.Message, state: FSMContext, bot: Bot):
        args = (message.text or "").split(maxsplit=1)
        arg = None
        if len(args) > 1:
            arg = args[1].strip()
        if arg == "new":
            existing = _get_latest_open_ticket(message.from_user.id)
            if existing:
                await message.answer(
                    f"<b>⚠️ Активный тикет найден</b>\n\n"
                    f"У вас уже есть открытый тикет <b>#{existing['ticket_id']}</b>.\n"
                    f"Пожалуйста, продолжайте переписку в нём."
                )
            else:
                await message.answer(
                    "<b>📝 Шаг 1/2: Тема обращения</b>\n\n"
                    "Напишите <b>краткий заголовок</b> (3-5 слов).\n"
                    "<i>Пример: «Не работает VPN», «Проблема с оплатой»</i>"
                )
                await state.set_state(SupportDialog.waiting_for_subject)
            return
        if _is_user_banned(message.from_user.id):
            banned_text = (
                "<b>🚫 Доступ ограничен</b>\n\n"
                "Ваш аккаунт заблокирован. Вы не можете обращаться в поддержку."
            )
            markup = _support_contact_markup()
            if markup:
                await message.answer(banned_text, reply_markup=markup)
            else:
                await message.answer(banned_text)
            await state.clear()
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
        await callback.answer()
        if _is_user_banned(callback.from_user.id):
            text = (
                "<b>🚫 Доступ ограничен</b>\n\n"
                "Ваш аккаунт заблокирован. Вы не можете обращаться в поддержку."
            )
            try:
                await callback.answer(text, show_alert=True)
            except Exception:
                pass
            markup = _support_contact_markup()
            if markup:
                await callback.message.answer(text, reply_markup=markup)
            else:
                await callback.message.answer(text)
            await state.clear()
            return
        existing = _get_latest_open_ticket(callback.from_user.id)
        if existing:
            await callback.message.edit_text(
                f"<b>⚠️ Активный тикет найден</b>\n\n"
                f"У вас уже есть открытый тикет <b>#{existing['ticket_id']}</b>.\n"
                f"Пожалуйста, продолжайте переписку в нём."
            )
        else:
            await callback.message.edit_text(
                "<b>📝 Шаг 1/2: Тема обращения</b>\n\n"
                "Напишите <b>краткий заголовок</b> (3-5 слов).\n"
                "<i>Пример: «Не работает VPN», «Проблема с оплатой»</i>"
            )
            await state.set_state(SupportDialog.waiting_for_subject)

    @router.message(SupportDialog.waiting_for_subject, F.chat.type == "private")
    async def support_subject_received(message: types.Message, state: FSMContext):
        if _is_user_banned(message.from_user.id):
            banned_text = (
                "<b>🚫 Доступ ограничен</b>\n\n"
                "Ваш аккаунт заблокирован. Вы не можете обращаться в поддержку."
            )
            markup = _support_contact_markup()
            if markup:
                await message.answer(banned_text, reply_markup=markup)
            else:
                await message.answer(banned_text)
            await state.clear()
            return
        subject = (message.text or "").strip()
        await state.update_data(subject=subject)
        await message.answer(
            "<b>✉️ Шаг 2/2: Описание проблемы</b>\n\n"
            "Теперь максимально подробно опишите ситуацию <b>одним сообщением</b>.\n"
            "<i>Можете прикрепить скриншот или видео.</i>"
        )
        await state.set_state(SupportDialog.waiting_for_message)

    @router.message(SupportDialog.waiting_for_message, F.chat.type == "private")
    async def support_message_received(message: types.Message, state: FSMContext, bot: Bot):
        if _is_user_banned(message.from_user.id):
            banned_text = (
                "<b>🚫 Доступ ограничен</b>\n\n"
                "Ваш аккаунт заблокирован. Вы не можете обращаться в поддержку."
            )
            markup = _support_contact_markup()
            if markup:
                await message.answer(banned_text, reply_markup=markup)
            else:
                await message.answer(banned_text)
            await state.clear()
            return
        user_id = message.from_user.id
        data = await state.get_data()
        raw_subject = (data.get("subject") or "").strip()
        subject = raw_subject if raw_subject else "Обращение без темы"
        ticket_id, created_new = get_or_create_open_ticket(user_id, subject)
        if not ticket_id:
            await message.answer("❌ Не удалось создать обращение. Попробуйте позже.")
            await state.clear()
            return
        # Prepare content text
        content_text = (message.text or message.caption or "").strip()
        
        media_file_path = None
        media_type = None
        
        if message.photo:
            media_type = 'photo'
            content_text = f"[Фото] {content_text}".strip()
        elif message.video:
            media_type = 'video'
            content_text = f"[Видео] {content_text}".strip()
            
        add_support_message(ticket_id, sender="user", content=content_text)
        
        # Download media if exists
        if media_type:
            try:
                if message.photo:
                    file_id = message.photo[-1].file_id
                    ext = "jpg"
                else:
                    file_id = message.video.file_id
                    ext = "mp4" # Basic assumption, or get from mime_type
                
                file_info = await bot.get_file(file_id)
                folder_path = os.path.join("src", "shop_bot", "support_bot", "content")
                os.makedirs(folder_path, exist_ok=True)
                
                media_file_path = os.path.join(folder_path, f"{ticket_id}_{file_info.file_unique_id}.{ext}")
                await bot.download_file(file_info.file_path, media_file_path)
            except Exception as e:
                logger.error(f"Failed to download media for ticket {ticket_id}: {e}")
                media_file_path = None

        ticket = get_ticket(ticket_id)
        support_forum_chat_id = get_setting("support_forum_chat_id")
        thread_id = None
        if support_forum_chat_id and not (ticket and ticket.get('message_thread_id')):
            try:
                chat_id = int(support_forum_chat_id)
                author_tag = (
                    (message.from_user.username and f"@{message.from_user.username}")
                    or (message.from_user.full_name if message.from_user else None)
                    or str(user_id)
                )
                subj_full = (subject or 'Обращение без темы')
                is_star = subj_full.strip().startswith('⭐')
                display_subj = (subj_full.lstrip('⭐️ ').strip() if is_star else subj_full)
                trimmed_subject = display_subj[:40]
                important_prefix = '🔴 Важно: ' if is_star else ''
                topic_name = f"#{ticket_id} {important_prefix}{trimmed_subject} • от {author_tag}"
                forum_topic = await bot.create_forum_topic(chat_id=chat_id, name=topic_name)
                thread_id = forum_topic.message_thread_id
                update_ticket_thread_info(ticket_id, str(chat_id), int(thread_id))
                subj_display = (subject or '—')
                header_text = (
                    "🆘 Новое обращение\n"
                    f"Тикет: #{ticket_id}\n"
                    f"Пользователь: @{message.from_user.username or message.from_user.full_name} (ID: {user_id})\n"
                    f"Тема: {subj_display} — от @{message.from_user.username or message.from_user.full_name} (ID: {user_id})\n\n"
                    f"Сообщение:\n{message.text or message.caption or ''}"
                )
                if media_file_path:
                    media_input = FSInputFile(media_file_path)
                    if media_type == 'photo':
                        await bot.send_photo(chat_id=chat_id, photo=media_input, caption=header_text, message_thread_id=thread_id, reply_markup=_admin_actions_kb(ticket_id))
                    elif media_type == 'video':
                        await bot.send_video(chat_id=chat_id, video=media_input, caption=header_text, message_thread_id=thread_id, reply_markup=_admin_actions_kb(ticket_id))
                else:
                    await bot.send_message(chat_id=chat_id, text=header_text, message_thread_id=thread_id, reply_markup=_admin_actions_kb(ticket_id))
            except Exception as e:
                logger.warning(f"Не удалось создать тему форума или отправить сообщение для тикета {ticket_id}: {e}")
        try:
            ticket = get_ticket(ticket_id)
            forum_chat_id = ticket and ticket.get('forum_chat_id')
            thread_id = ticket and ticket.get('message_thread_id')
            if forum_chat_id and thread_id:
                username = (message.from_user.username and f"@{message.from_user.username}") or message.from_user.full_name or str(message.from_user.id)
                # For existing thread copy_message works best but if we have local file...
                # Actually, copy_message works perfectly for media too, so we don't strictly NEED the local file for the FORUM mirror if we use copy_message.
                # However, for DM notifications to admins, copy_message might not work if admins are in different chats (private chats).
                # But wait, copy_message works across chats if the bot has access.
                # But requirement was: "save to folder... send... delete".
                
                # Mirroring to forum using copy_message (original behavior preserved/enhanced)
                await bot.send_message(
                    chat_id=int(forum_chat_id),
                    text=(
                        f"🆕 Новое обращение от {username} (ID: {message.from_user.id}) по тикету #{ticket_id}:" if created_new
                        else f"✉️ Новое сообщение по тикету #{ticket_id} от {username} (ID: {message.from_user.id}):"
                    ),
                    message_thread_id=int(thread_id)
                )
                await bot.copy_message(
                    chat_id=int(forum_chat_id),
                    from_chat_id=message.chat.id,
                    message_id=message.message_id,
                    message_thread_id=int(thread_id)
                )
        except Exception as e:
            logger.warning(f"Не удалось отзеркалить сообщение пользователя в форум: {e}")
        await state.clear()
        if created_new:
            await message.answer(
                f"<b>✅ Обращение #{ticket_id} создано!</b>\n\n"
                "Ожидайте ответа поддержки. Мы скоро свяжемся с вами.",
                reply_markup=_user_main_reply_kb()
            )
        else:
            await message.answer(
                f"<b>✉️ Сообщение добавлено в тикет #{ticket_id}</b>",
                reply_markup=_user_main_reply_kb()
            )

        try:
            for aid in get_admin_ids():
                try:
                    username_val = message.from_user.username
                    user_tag = f"@{username_val}" if username_val else "@неуказан"
                    user_info_str = f"({message.from_user.id}. {user_tag})"

                    if created_new:
                        header_line = f"🆘 Новое обращение {user_info_str}"
                    else:
                        header_line = f"✉️ Пользователь дополнил сообщение {user_info_str}"

                    # If message is text, send as formatted message (copy_message fails with caption for text)
                    if message.text:
                         notification_text = (
                            f"{header_line}\n"
                            f"ID тикета: #{ticket_id}\n"
                            f"Сообщение:\n{message.text}"
                        )
                         await bot.send_message(
                            chat_id=int(aid),
                            text=notification_text,
                            reply_markup=_admin_dm_reply_kb(ticket_id)
                        )
                    else:
                        # For media, use copy_message which supports caption
                        caption_part = message.caption or ""
                        
                        if not caption_part and message.photo:
                             caption_part = "[Фото]"
                        elif not caption_part and message.video:
                             caption_part = "[Видео]"

                        notification_text = (
                            f"{header_line}\n"
                            f"ID тикета: #{ticket_id}\n"
                            f"Сообщение:\n{caption_part}"
                        )
                        await bot.copy_message(
                            chat_id=int(aid),
                            from_chat_id=message.chat.id,
                            message_id=message.message_id,
                            caption=notification_text,
                            reply_markup=_admin_dm_reply_kb(ticket_id)
                        )
                except Exception:
                    pass
        except Exception as e:
            logger.warning(f"Не удалось уведомить админов о тикете {ticket_id}: {e}")
            
        # Cleanup
        if media_file_path and os.path.exists(media_file_path):
            try:
                os.remove(media_file_path)
            except Exception:
                pass

    @router.callback_query(F.data == "support_my_tickets")
    async def support_my_tickets_handler(callback: types.CallbackQuery):
        await callback.answer()
        tickets = get_user_tickets(callback.from_user.id)
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
        await callback.message.edit_text(text, reply_markup=types.InlineKeyboardMarkup(inline_keyboard=rows))

    @router.callback_query(F.data.startswith("support_view_"))
    async def support_view_ticket_handler(callback: types.CallbackQuery):
        await callback.answer()
        ticket_id = int(callback.data.split("_")[-1])
        ticket = get_ticket(ticket_id)
        if not ticket or ticket.get('user_id') != callback.from_user.id:
            await callback.message.edit_text("Тикет не найден или доступ запрещён.")
            return
        messages = get_ticket_messages(ticket_id)
        human_status = "🟢 Открыт" if ticket.get('status') == 'open' else "🔒 Закрыт"
        is_star = (ticket.get('subject') or '').startswith('⭐ ')
        star_line = "⭐ Важно" if is_star else "—"
        parts = [
            f"<b>🧾 Тикет #{ticket_id}</b>",
            f"<b>Статус:</b> {human_status}",
            f"<b>Тема:</b> {ticket.get('subject') or '—'}",
            f"<b>Важность:</b> {star_line}",
            ""
        ]
        for m in messages:
            if m.get('sender') == 'note':
                continue
            who = "<b>Вы</b>" if m.get('sender') == 'user' else '<b>Поддержка</b>'
            created = m.get('created_at')
            parts.append(f"{who} ({created}):\n{m.get('content','')}\n")
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
        if _is_user_banned(callback.from_user.id):
            text = (
                "<b>🚫 Доступ ограничен</b>\n\n"
                "Ваш аккаунт заблокирован. Вы не можете обращаться в поддержку."
            )
            try:
                await callback.answer(text, show_alert=True)
            except Exception:
                pass
            markup = _support_contact_markup()
            if markup:
                await callback.message.edit_text(text, reply_markup=markup)
            else:
                await callback.message.edit_text(text)
            await state.clear()
            return
        if not ticket or ticket.get('user_id') != callback.from_user.id or ticket.get('status') != 'open':
            await callback.message.edit_text("Нельзя ответить на этот тикет.")
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
        if _is_user_banned(message.from_user.id):
            banned_text = (
                "<b>🚫 Доступ ограничен</b>\n\n"
                "Ваш аккаунт заблокирован. Вы не можете обращаться в поддержку."
            )
            markup = _support_contact_markup()
            if markup:
                await message.answer(banned_text, reply_markup=markup)
            else:
                await message.answer(banned_text)
            await state.clear()
            return
        data = await state.get_data()
        ticket_id = data.get('reply_ticket_id')
        ticket = get_ticket(ticket_id)
        if not ticket or ticket.get('user_id') != message.from_user.id or ticket.get('status') != 'open':
            await message.answer("Нельзя ответить на этот тикет.")
            await state.clear()
            return
        add_support_message(ticket_id, sender='user', content=(message.text or message.caption or ''))
        await state.clear()
        await message.answer(
            "<b>✅ Сообщение отправлено</b>\n"
            "Ваш ответ передан поддержке."
        )
        try:
            forum_chat_id = ticket.get('forum_chat_id')
            thread_id = ticket.get('message_thread_id')
            if not (forum_chat_id and thread_id):
                support_forum_chat_id = get_setting("support_forum_chat_id")
                if support_forum_chat_id:
                    try:
                        chat_id = int(support_forum_chat_id)
                        subj_full = (ticket.get('subject') or 'Обращение без темы')
                        is_star = subj_full.strip().startswith('⭐')
                        display_subj = (subj_full.lstrip('⭐️ ').strip() if is_star else subj_full)
                        trimmed_subject = display_subj[:40]
                        author_tag = (
                            (message.from_user.username and f"@{message.from_user.username}")
                            or (message.from_user.full_name if message.from_user else None)
                            or str(message.from_user.id)
                        )
                        important_prefix = '🔴 Важно: ' if is_star else ''
                        topic_name = f"#{ticket_id} {important_prefix}{trimmed_subject} • от {author_tag}"
                        forum_topic = await bot.create_forum_topic(chat_id=chat_id, name=topic_name)
                        thread_id = forum_topic.message_thread_id
                        forum_chat_id = chat_id
                        update_ticket_thread_info(ticket_id, str(chat_id), int(thread_id))
                        subj_display = (ticket.get('subject') or '—')
                        header = (
                            "📌 Тред создан автоматически\n"
                            f"Тикет: #{ticket_id}\n"
                            f"Пользователь: ID {ticket.get('user_id')}\n"
                            f"Тема: {subj_display} — от ID {ticket.get('user_id')}"
                        )
                        await bot.send_message(chat_id=chat_id, text=header, message_thread_id=thread_id, reply_markup=_admin_actions_kb(ticket_id))
                    except Exception as e:
                        logger.warning(f"Не удалось автоматически создать тему форума для тикета {ticket_id}: {e}")
            if forum_chat_id and thread_id:
                try:
                    subj_full = (ticket.get('subject') or 'Обращение без темы')
                    is_star = subj_full.strip().startswith('⭐')
                    display_subj = (subj_full.lstrip('⭐️ ').strip() if is_star else subj_full)
                    trimmed = display_subj[:40]
                    author_tag = (
                        (message.from_user.username and f"@{message.from_user.username}")
                        or (message.from_user.full_name if message.from_user else None)
                        or str(message.from_user.id)
                    )
                    important_prefix = '🔴 Важно: ' if is_star else ''
                    topic_name = f"#{ticket_id} {important_prefix}{trimmed} • от {author_tag}"
                    await bot.edit_forum_topic(chat_id=int(forum_chat_id), message_thread_id=int(thread_id), name=topic_name)
                except Exception as e:
                    logger.warning(f"Не удалось переименовать существующую тему для тикета {ticket_id}: {e}")
                username = (message.from_user.username and f"@{message.from_user.username}") or message.from_user.full_name or str(message.from_user.id)
                await bot.send_message(
                    chat_id=int(forum_chat_id),
                    text=f"✉️ Новое сообщение по тикету #{ticket_id} от {username} (ID: {message.from_user.id}):",
                    message_thread_id=int(thread_id)
                )
                await bot.copy_message(chat_id=int(forum_chat_id), from_chat_id=message.chat.id, message_id=message.message_id, message_thread_id=int(thread_id))
        except Exception as e:
            logger.warning(f"Не удалось отзеркалить ответ пользователя в форум: {e}")
        admin_id = get_setting("admin_telegram_id")
        if admin_id:
            try:
                username_val = message.from_user.username
                user_tag = f"@{username_val}" if username_val else "@неуказан"
                user_info_str = f"({message.from_user.id}. {user_tag})"
                header_line = f"✉️ Ответ пользователя {user_info_str}"
                
                if message.text:
                    notification_text = (
                        f"{header_line}\n"
                        f"ID тикета: #{ticket_id}\n"
                        f"Сообщение:\n{message.text}"
                    )
                    await bot.send_message(
                        int(admin_id),
                        notification_text,
                        reply_markup=_admin_dm_reply_kb(ticket_id)
                    )
                else:
                    caption_part = message.caption or ""
                    if not caption_part and message.photo:
                        caption_part = "[Фото]"
                    elif not caption_part and message.video:
                        caption_part = "[Видео]"

                    notification_text = (
                        f"{header_line}\n"
                        f"ID тикета: #{ticket_id}\n"
                        f"Сообщение:\n{caption_part}"
                    )
                    await bot.copy_message(
                        chat_id=int(admin_id),
                        from_chat_id=message.chat.id,
                        message_id=message.message_id,
                        caption=notification_text,
                        reply_markup=_admin_dm_reply_kb(ticket_id)
                    )
            except Exception as e:
                logger.warning(f"Не удалось уведомить админа о сообщении тикета #{ticket_id}: {e}")

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
                        username = None
                        if message.from_user.username:
                            username = f"@{message.from_user.username}"
                        else:
                            username = message.from_user.full_name or str(author_id)
                        note_text = f"[Заметка от {username} (ID: {author_id})]\n{note_body}"
                    else:
                        note_text = note_body
                    add_support_message(int(ticket['ticket_id']), sender='note', content=note_text)
                    await message.answer("📝 Внутренняя заметка сохранена.")
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
            header = await bot.send_message(
                chat_id=user_id,
                text=f"💬 Ответ поддержки по тикету #{ticket['ticket_id']}"
            )
            try:
                await bot.copy_message(
                    chat_id=user_id,
                    from_chat_id=message.chat.id,
                    message_id=message.message_id,
                    reply_to_message_id=header.message_id
                )
            except Exception:
                if content:
                    await bot.send_message(chat_id=user_id, text=content)
        except Exception as e:
            logger.warning(f"Не удалось передать сообщение темы форума: {e}")

    @router.callback_query(F.data.startswith("support_close_"))
    async def support_close_ticket_handler(callback: types.CallbackQuery, bot: Bot):
        await callback.answer()
        ticket_id = int(callback.data.split("_")[-1])
        ticket = get_ticket(ticket_id)
        if not ticket or ticket.get('user_id') != callback.from_user.id:
            await callback.message.edit_text("Тикет не найден или доступ запрещён.")
            return
        if ticket.get('status') == 'closed':
            await callback.message.edit_text("Тикет уже закрыт.")
            return
        ok = set_ticket_status(ticket_id, 'closed')
        if ok:
            try:
                forum_chat_id = ticket.get('forum_chat_id')
                thread_id = ticket.get('message_thread_id') or getattr(callback.message, 'message_thread_id', None)
                if forum_chat_id and thread_id:
                    try:
                        username = (callback.from_user.username and f"@{callback.from_user.username}") or callback.from_user.full_name or str(callback.from_user.id)
                        await bot.send_message(
                            chat_id=int(forum_chat_id),
                            text=f"✅ Пользователь {username} закрыл тикет #{ticket_id}.",
                            message_thread_id=int(thread_id)
                        )
                        await bot.send_message(
                            chat_id=int(forum_chat_id),
                            text="Панель управления тикетом:",
                            message_thread_id=int(thread_id),
                            reply_markup=_admin_actions_kb(ticket_id)
                        )
                    except Exception:
                        pass
                    try:
                        await bot.close_forum_topic(chat_id=int(forum_chat_id), message_thread_id=int(thread_id))
                    except Exception:
                        pass
            except Exception as e:
                logger.warning(f"Не удалось закрыть тему форума для тикета {ticket_id} из бота: {e}")
            await callback.message.edit_text(
                "<b>✅ Тикет успешно закрыт</b>\n\n"
                "Спасибо за обращение! Если возникнут новые вопросы — создавайте новый тикет.",
                reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[[types.InlineKeyboardButton(text="⬅️ К списку", callback_data="support_my_tickets")]])
            )
            try:
                await callback.message.answer("Меню поддержки:", reply_markup=_user_main_reply_kb())
            except Exception:
                pass
        else:
            await callback.message.edit_text("<b>❌ Ошибка</b>\nНе удалось закрыть тикет.")

    @router.callback_query(F.data.startswith("admin_close_"))
    async def admin_close_ticket(callback: types.CallbackQuery, bot: Bot):
        await callback.answer()
        try:
            ticket_id = int(callback.data.split("_")[-1])
        except Exception:
            return
        ticket = get_ticket(ticket_id)
        if not ticket:
            await callback.message.edit_text("Тикет не найден.")
            return
        forum_chat_id = int(ticket.get('forum_chat_id') or callback.message.chat.id)
        if not await _is_admin(bot, forum_chat_id, callback.from_user.id):
            return
        if set_ticket_status(ticket_id, 'closed'):
            try:
                thread_id = ticket.get('message_thread_id') or getattr(callback.message, 'message_thread_id', None)
                if thread_id:
                    await bot.close_forum_topic(chat_id=forum_chat_id, message_thread_id=int(thread_id))
            except Exception:
                pass
            try:
                await callback.message.edit_text(
                    f"✅ Тикет #{ticket_id} закрыт.",
                    reply_markup=_admin_actions_kb(ticket_id)
                )
            except TelegramBadRequest as e:
                if "message is not modified" in str(e):
                    await callback.answer("Без изменений", show_alert=False)
                else:
                    raise
            try:
                user_id = int(ticket.get('user_id'))
                await bot.send_message(chat_id=user_id, text=f"✅ Ваш тикет #{ticket_id} был закрыт администратором. Спасибо за обращение!")
            except Exception:
                pass
        else:
            await callback.message.answer("❌ Не удалось закрыть тикет.")

    @router.callback_query(F.data.startswith("admin_reopen_"))
    async def admin_reopen_ticket(callback: types.CallbackQuery, bot: Bot):
        await callback.answer()
        try:
            ticket_id = int(callback.data.split("_")[-1])
        except Exception:
            return
        ticket = get_ticket(ticket_id)
        if not ticket:
            await callback.message.edit_text("Тикет не найден.")
            return
        forum_chat_id = int(ticket.get('forum_chat_id') or callback.message.chat.id)
        if not await _is_admin(bot, forum_chat_id, callback.from_user.id):
            return
        if set_ticket_status(ticket_id, 'open'):
            try:
                thread_id = ticket.get('message_thread_id') or getattr(callback.message, 'message_thread_id', None)
                if thread_id:
                    await bot.reopen_forum_topic(chat_id=forum_chat_id, message_thread_id=int(thread_id))
            except Exception:
                pass
            try:
                await callback.message.edit_text(
                    f"🔓 Тикет #{ticket_id} переоткрыт.",
                    reply_markup=_admin_actions_kb(ticket_id)
                )
            except TelegramBadRequest as e:
                if "message is not modified" in str(e):
                    await callback.answer("Без изменений", show_alert=False)
                else:
                    raise
            try:
                user_id = int(ticket.get('user_id'))
                await bot.send_message(chat_id=user_id, text=f"🔓 Ваш тикет #{ticket_id} был переоткрыт администратором. Вы можете продолжить переписку.")
            except Exception:
                pass
        else:
            await callback.message.answer("❌ Не удалось переоткрыть тикет.")

    @router.callback_query(F.data.startswith("admin_delete_"))
    async def admin_delete_ticket(callback: types.CallbackQuery, bot: Bot):
        await callback.answer()
        try:
            ticket_id = int(callback.data.split("_")[-1])
        except Exception:
            return
        ticket = get_ticket(ticket_id)
        if not ticket:

            try:
                await callback.answer("Тикет уже удалён или не найден.", show_alert=False)
            except Exception:
                pass
            return
        forum_chat_id = int(ticket.get('forum_chat_id') or callback.message.chat.id)
        if not await _is_admin(bot, forum_chat_id, callback.from_user.id):
            return

        try:
            await callback.message.edit_text(
                f"🗑 Удаляю тикет #{ticket_id}..."
            )
        except Exception:

            pass


        try:
            thread_id = ticket.get('message_thread_id') or getattr(callback.message, 'message_thread_id', None)
            if thread_id:
                try:
                    await bot.delete_forum_topic(chat_id=forum_chat_id, message_thread_id=int(thread_id))
                except Exception:
                    try:
                        await bot.close_forum_topic(chat_id=forum_chat_id, message_thread_id=int(thread_id))
                    except Exception:
                        pass
        except Exception:
            pass


        ok = delete_ticket(ticket_id)
        if ok:

            try:
                await callback.answer(f"🗑 Тикет #{ticket_id} удалён.", show_alert=False)
            except Exception:
                pass
        else:
            try:
                await callback.answer("❌ Не удалось удалить тикет.", show_alert=True)
            except Exception:
                pass

    @router.callback_query(F.data.startswith("admin_star_"))
    async def admin_toggle_star(callback: types.CallbackQuery, bot: Bot):
        await callback.answer()
        try:
            ticket_id = int(callback.data.split("_")[-1])
        except Exception:
            return
        ticket = get_ticket(ticket_id)
        if not ticket:
            return
        forum_chat_id = int(ticket.get('forum_chat_id') or callback.message.chat.id)
        if not await _is_admin(bot, forum_chat_id, callback.from_user.id):
            return
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
                    subj_full = (new_subject or 'Обращение без темы')
                    is_star2 = subj_full.strip().startswith('⭐')
                    display_subj2 = (subj_full.lstrip('⭐️ ').strip() if is_star2 else subj_full)
                    trimmed = display_subj2[:40]
                    important_prefix2 = '🔴 Важно: ' if is_star2 else ''
                    topic_name = f"#{ticket_id} {important_prefix2}{trimmed} • от {author_tag}"
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
            await callback.message.answer(f"⭐ Пометка важности {state_text}. Название темы обновлено.")
        else:
            await callback.message.answer("❌ Не удалось обновить тему тикета.")

    @router.callback_query(F.data.startswith("admin_user_"))
    async def admin_show_user(callback: types.CallbackQuery, bot: Bot):
        await callback.answer()
        try:
            ticket_id = int(callback.data.split("_")[-1])
        except Exception:
            return
        ticket = get_ticket(ticket_id)
        if not ticket:
            await callback.message.edit_text("Тикет не найден.")
            return
        forum_chat_id = int(ticket.get('forum_chat_id') or callback.message.chat.id)
        if not await _is_admin(bot, forum_chat_id, callback.from_user.id):
            return

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
                username_val = f"@{u_obj.username}" if u_obj.username else (u_obj.full_name or str(user_id_val))
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
        try:
            ticket_id = int(callback.data.split("_")[-1])
        except Exception:
            return

        ticket = get_ticket(ticket_id)
        if not ticket:
            await callback.message.answer("Тикет не найден.")
            return
            
        # Проверка прав админа (хотя уведомления приходят только админам, перепроверка security-wise)
        if not is_admin(callback.from_user.id):
             await callback.message.answer("⛔ Нет доступа.")
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
            await message.answer("Ошибка контекст ответа потерян.")
            await state.clear()
            return
            
        content = (message.text or message.caption or "").strip()
        if not content:
            await message.answer("Сообщение не может быть пустым.")
            return
            
        ticket = get_ticket(ticket_id)
        if not ticket:
            await message.answer("Тикет не найден.")
            await state.clear()
            return

        user_id = int(ticket['user_id'])
        
        # 1. Сохраняем в БД как сообщение от админа
        add_support_message(ticket_id=ticket_id, sender='admin', content=content)
        
        # 2. Отправляем пользователю в ЛС
        try:
            header = await bot.send_message(
                chat_id=user_id,
                text=f"💬 Ответ поддержки по тикету #{ticket_id}"
            )
            try:
                await bot.copy_message(
                    chat_id=user_id,
                    from_chat_id=message.chat.id,
                    message_id=message.message_id,
                    reply_to_message_id=header.message_id
                )
            except Exception:
                await bot.send_message(chat_id=user_id, text=content)
        except Exception as e:
            logger.warning(f"Failed to send reply to user {user_id}: {e}")
            await message.answer("❌ Не удалось доставить сообщение пользователю (возможно, он заблокировал бота).")
            # Но в базу мы сохранили, так что продолжаем (или можно откатить, но обычно сохраняют)
            
        # 3. Дублируем в форумный тред (если есть) для истории
        try:
            forum_chat_id = ticket.get('forum_chat_id')
            thread_id = ticket.get('message_thread_id')
            if forum_chat_id and thread_id:
                 # От своего имени (бота) пишем, что админ (с таким-то ID/именем) ответил через бота
                 admin_tag = f"@{message.from_user.username}" if message.from_user.username else message.from_user.full_name
                 await bot.send_message(
                    chat_id=int(forum_chat_id),
                    text=f"👨‍💻 Ответ администратора {admin_tag} через ЛС бота:\n\n{content}",
                    message_thread_id=int(thread_id)
                 )
        except Exception as e:
            logger.warning(f"Failed to mirror admin reply to forum: {e}")

        await message.answer("✅ Сообщение отправлено.")
        await state.clear()

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

    async def _notify_user_about_ban(bot: Bot, user_id: int, text: str) -> None:
        try:
            markup = _support_contact_markup()
            if markup:
                await bot.send_message(user_id, text, reply_markup=markup)
            else:
                await bot.send_message(user_id, text)
        except Exception:
            pass

    @router.callback_query(F.data.startswith("admin_ban_user_"))
    async def admin_ban_user(callback: types.CallbackQuery, bot: Bot):
        await callback.answer()
        try:
            ticket_id = int(callback.data.split("_")[-1])
        except Exception:
            return
        ticket = get_ticket(ticket_id)
        if not ticket:
            return
        forum_chat_id = int(ticket.get('forum_chat_id') or callback.message.chat.id)
        if not await _is_admin(bot, forum_chat_id, callback.from_user.id):
            return
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
        await callback.message.answer(f"🚫 Пользователь {user_id} забанен.")

        await _notify_user_about_ban(bot, user_id, "🚫 Ваш аккаунт был заблокирован администратором. Если это ошибка — свяжитесь с поддержкой.")
        try:
            await callback.message.edit_reply_markup(reply_markup=_admin_actions_kb(ticket_id))
        except TelegramBadRequest:
            pass

    @router.callback_query(F.data.startswith("admin_unban_user_"))
    async def admin_unban_user(callback: types.CallbackQuery, bot: Bot):
        await callback.answer()
        try:
            ticket_id = int(callback.data.split("_")[-1])
        except Exception:
            return
        ticket = get_ticket(ticket_id)
        if not ticket:
            return
        forum_chat_id = int(ticket.get('forum_chat_id') or callback.message.chat.id)
        if not await _is_admin(bot, forum_chat_id, callback.from_user.id):
            return
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
        await callback.message.answer(f"✅ Пользователь {user_id} разбанен.")

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
        try:
            ticket_id = int(callback.data.split("_")[-1])
        except Exception:
            return
        ticket = get_ticket(ticket_id)
        if not ticket:
            return
        forum_chat_id = int(ticket.get('forum_chat_id') or callback.message.chat.id)
        if not await _is_admin(bot, forum_chat_id, callback.from_user.id):
            return
        await state.update_data(note_ticket_id=ticket_id)
        await callback.message.answer("📝 Отправьте внутреннюю заметку одним сообщением. Она не будет отправлена пользователю.")
        await state.set_state(AdminDialog.waiting_for_note)

    @router.callback_query(F.data.startswith("admin_notes_"))
    async def admin_list_notes(callback: types.CallbackQuery, bot: Bot):
        await callback.answer()
        try:
            ticket_id = int(callback.data.split("_")[-1])
        except Exception:
            return
        ticket = get_ticket(ticket_id)
        if not ticket:
            return
        forum_chat_id = int(ticket.get('forum_chat_id') or callback.message.chat.id)
        if not await _is_admin(bot, forum_chat_id, callback.from_user.id):
            return
        notes = [m for m in get_ticket_messages(ticket_id) if m.get('sender') == 'note']
        if not notes:
            await callback.message.answer("🗒 Внутренних заметок пока нет.")
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
        username = None
        if message.from_user:
            if message.from_user.username:
                username = f"@{message.from_user.username}"
            else:
                username = message.from_user.full_name or str(author_id)
        note_body = (message.text or message.caption or '').strip()
        note_text = f"[Заметка от {username} (ID: {author_id})]\n{note_body}" if author_id else note_body
        add_support_message(int(ticket_id), sender='note', content=note_text)
        await message.answer("📝 Внутренняя заметка сохранена.")
        await state.clear()

    @router.message(F.text == "▶️ Начать", F.chat.type == "private")
    async def start_text_button(message: types.Message, state: FSMContext):
        existing = _get_latest_open_ticket(message.from_user.id)
        if existing:
            await message.answer(
                f"<b>⚠️ Активный тикет найден</b>\n\n"
                f"У вас уже есть открытый тикет <b>#{existing['ticket_id']}</b>.\n"
                f"Пожалуйста, продолжайте переписку в нём."
            )
        else:
            await message.answer(
                "<b>📝 Шаг 1/2: Тема обращения</b>\n\n"
                "Напишите <b>краткий заголовок</b> (3-5 слов).\n"
                "<i>Пример: «Не работает VPN», «Проблема с оплатой»</i>"
            )
            await state.set_state(SupportDialog.waiting_for_subject)

    @router.message(F.text == "✍️ Новое обращение", F.chat.type == "private")
    async def new_ticket_text_button(message: types.Message, state: FSMContext):
        existing = _get_latest_open_ticket(message.from_user.id)
        if existing:
            await message.answer(
                f"<b>⚠️ Активный тикет найден</b>\n\n"
                f"У вас уже есть открытый тикет <b>#{existing['ticket_id']}</b>.\n"
                f"Пожалуйста, продолжайте переписку в нём."
            )
        else:
            await message.answer(
                "<b>📝 Шаг 1/2: Тема обращения</b>\n\n"
                "Напишите <b>краткий заголовок</b> (3-5 слов).\n"
                "<i>Пример: «Не работает VPN», «Проблема с оплатой»</i>"
            )
            await state.set_state(SupportDialog.waiting_for_subject)

    @router.message(F.text == "📨 Мои обращения", F.chat.type == "private")
    async def my_tickets_text_button(message: types.Message):
        tickets = get_user_tickets(message.from_user.id)
        text = "<b>📨 Ваши обращения:</b>" if tickets else "<b>📂 У вас пока нет обращений.</b>"
        rows = []
        if tickets:
            for t in tickets:
                status_text = "🟢 Открыт" if t.get('status') == 'open' else "🔒 Закрыт"
                title = f"#{t['ticket_id']} • {status_text}"
                if t.get('subject'):
                    title += f" • {t['subject'][:20]}"
                rows.append([types.InlineKeyboardButton(text=title, callback_data=f"support_view_{t['ticket_id']}")])
        await message.answer(text, reply_markup=types.InlineKeyboardMarkup(inline_keyboard=rows))

    @router.message(F.chat.type == "private")
    async def relay_user_message_to_forum(message: types.Message, bot: Bot, state: FSMContext):
        current_state = await state.get_state()
        if current_state is not None:
            return

        user_id = message.from_user.id if message.from_user else None
        if not user_id:
            return

        if _is_user_banned(user_id):
            banned_text = (
                "<b>🚫 Доступ ограничен</b>\n\n"
                "Ваш аккаунт заблокирован. Вы не можете обращаться в поддержку."
            )
            markup = _support_contact_markup()
            if markup:
                await message.answer(banned_text, reply_markup=markup)
            else:
                await message.answer(banned_text)
            await state.clear()
            return

        content = (message.text or message.caption or '')
        ticket_id, created_new = get_or_create_open_ticket(user_id, None)
        if not ticket_id:
            return
        add_support_message(ticket_id, sender='user', content=content)
        ticket = get_ticket(ticket_id)

        try:
            forum_chat_id = ticket.get('forum_chat_id')
            thread_id = ticket.get('message_thread_id')
            if not (forum_chat_id and thread_id):
                support_forum_chat_id = get_setting("support_forum_chat_id")
                if support_forum_chat_id:
                    try:
                        chat_id = int(support_forum_chat_id)
                        subj_full = (ticket.get('subject') or 'Обращение без темы')
                        is_star = subj_full.strip().startswith('⭐')
                        display_subj = (subj_full.lstrip('⭐️ ').strip() if is_star else subj_full)
                        trimmed = display_subj[:40]
                        author_tag = (
                            (message.from_user.username and f"@{message.from_user.username}")
                            or (message.from_user.full_name if message.from_user else None)
                            or str(message.from_user.id)
                        )
                        important_prefix = '🔴 Важно: ' if is_star else ''
                        topic_name = f"#{ticket_id} {important_prefix}{trimmed} • от {author_tag}"
                        forum_topic = await bot.create_forum_topic(chat_id=chat_id, name=topic_name)
                        thread_id = forum_topic.message_thread_id
                        forum_chat_id = chat_id
                        update_ticket_thread_info(ticket_id, str(chat_id), int(thread_id))
                        subj_display = (ticket.get('subject') or '—')
                        header = (
                            ("🆘 Новое обращение\n" if created_new else "📌 Тред создан автоматически\n") +
                            f"Тикет: #{ticket_id}\n" \
                            f"Пользователь: @{message.from_user.username or message.from_user.full_name} (ID: {message.from_user.id})\n" \
                            f"Тема: {subj_display} — от @{message.from_user.username or message.from_user.full_name} (ID: {message.from_user.id})"
                        )
                        await bot.send_message(chat_id=chat_id, text=header, message_thread_id=thread_id, reply_markup=_admin_actions_kb(ticket_id))
                    except Exception as e:
                        logger.warning(f"Не удалось автоматически создать тему форума для тикета {ticket_id}: {e}")
            if forum_chat_id and thread_id:
                try:
                    subj_full = (ticket.get('subject') or 'Обращение без темы')
                    is_star = subj_full.strip().startswith('⭐')
                    display_subj = (subj_full.lstrip('⭐️ ').strip() if is_star else subj_full)
                    trimmed = display_subj[:40]
                    author_tag = (
                        (message.from_user.username and f"@{message.from_user.username}")
                        or (message.from_user.full_name if message.from_user else None)
                        or str(message.from_user.id)
                    )
                    important_prefix = '🔴 Важно: ' if is_star else ''
                    topic_name = f"#{ticket_id} {important_prefix}{trimmed} • от {author_tag}"
                    await bot.edit_forum_topic(chat_id=int(forum_chat_id), message_thread_id=int(thread_id), name=topic_name)
                except Exception as e:
                    logger.warning(f"Не удалось переименовать тему для тикета свободного сообщения {ticket_id}: {e}")
                username = (message.from_user.username and f"@{message.from_user.username}") or message.from_user.full_name or str(message.from_user.id)
                await bot.send_message(
                    chat_id=int(forum_chat_id),
                    text=(
                        f"🆘 Новое обращение от {username} (ID: {message.from_user.id}) по тикету #{ticket_id}:" if created_new
                        else f"✉️ Новое сообщение по тикету #{ticket_id} от {username} (ID: {message.from_user.id}):"
                    ),
                    message_thread_id=int(thread_id)
                )
                await bot.copy_message(chat_id=int(forum_chat_id), from_chat_id=message.chat.id, message_id=message.message_id, message_thread_id=int(thread_id))
        except Exception as e:
            logger.warning(f"Не удалось отзеркалить свободное сообщение пользователя в форум для тикета {ticket_id}: {e}")

        try:
            for aid in get_admin_ids():
                try:
                    username_val = message.from_user.username
                    user_tag = f"@{username_val}" if username_val else "@неуказан"
                    user_info_str = f"({message.from_user.id}. {user_tag})"

                    if created_new:
                        header_line = f"🆘 Новое обращение {user_info_str}"
                    else:
                        header_line = f"✉️ Сообщение добавлено в тикет {user_info_str}"

                    content_part = message.caption or ""
                    if not content_part and message.photo:
                         content_part = "[Фото]"
                    elif not content_part and message.video:
                         content_part = "[Видео]"

                    if message.text:
                         notification_text = (
                            f"{header_line}\n"
                            f"ID тикета: #{ticket_id}\n"
                            f"Сообщение:\n{message.text}"
                        )
                         await bot.send_message(
                            chat_id=int(aid),
                            text=notification_text,
                            reply_markup=_admin_dm_reply_kb(ticket_id)
                        )
                    else:
                        notification_text = (
                            f"{header_line}\n"
                            f"ID тикета: #{ticket_id}\n"
                            f"Сообщение:\n{content_part}"
                        )
                        await bot.copy_message(
                            chat_id=int(aid),
                            from_chat_id=message.chat.id,
                            message_id=message.message_id,
                            caption=notification_text,
                            reply_markup=_admin_dm_reply_kb(ticket_id)
                        )
                except Exception:
                    pass
        except Exception as e:
            logger.warning(f"Не удалось уведомить админов о тикете {ticket_id}: {e}")

        try:
            if created_new:
                await message.answer(f"✅ Обращение создано: #{ticket_id}. Мы ответим вам как можно скорее.")
            else:
                await message.answer(f"✉️ Сообщение добавлено в ваш открытый тикет #{ticket_id}.")
        except Exception:
            pass

    return router
