import os
import json
import asyncio
import logging
import uuid
import threading
from datetime import datetime, timezone, timedelta
from flask import render_template, request, jsonify, current_app
from werkzeug.utils import secure_filename
from aiogram.types import FSInputFile
from shop_bot.data_manager import remnawave_repository as rw_repo
from . import server_plan

logger = logging.getLogger(__name__)

# ===== НАСТРОЙКИ И ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ =====
# Конфигурация путей, расширений файлов и глобальных объектов
# ===== Конец настроек =====

def get_msk_time() -> datetime:
    return datetime.now(timezone(timedelta(hours=3)))

def parse_expire_dt(expire_at) -> datetime:
    if not expire_at: return None
    try:
        if isinstance(expire_at, (int, float)):
            return datetime.fromtimestamp(expire_at / 1000, tz=timezone.utc)
        if isinstance(expire_at, str):
            if expire_at.isdigit():
                return datetime.fromtimestamp(int(expire_at) / 1000, tz=timezone.utc)
            try:
                dt = datetime.fromisoformat(expire_at.replace('Z', '+00:00'))
            except ValueError:
                dt = datetime.strptime(expire_at, "%Y-%m-%d %H:%M:%S")
            
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone(timedelta(hours=3)))
            return dt
    except: pass
    return None

ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif', 'mp4', 'webm'}
UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), 'img')

os.makedirs(UPLOAD_FOLDER, exist_ok=True)

broadcast_progress = {}
broadcast_lock = threading.Lock()
scheduler = None

ssh_sessions = {}
ssh_sessions_lock = threading.Lock()

# ===== ПРОВЕРКА ДОПУСТИМОГО ФАЙЛА =====
# Проверяет, имеет ли файл разрешенное расширение
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS
# ===== Конец функции allowed_file =====

# ===== ОПРЕДЕЛЕНИЕ ТИПА МЕДИА =====
# Возвращает тип контента (фото, анимация, видео) на основе расширения
def get_media_type(filename):
    ext = filename.rsplit('.', 1)[1].lower()
    if ext in {'png', 'jpg', 'jpeg'}: return 'photo'
    if ext == 'gif': return 'animation'
    if ext in {'mp4', 'webm'}: return 'video'
    return None
# ===== Конец функции get_media_type =====

# ===== ПОЛУЧЕНИЕ SSH СЕРВЕРА =====
# Извлекает данные хоста или SSH-цели по имени
def get_ssh_server(name, server_type):
    if server_type == 'host':
        hosts = rw_repo.list_squads(active_only=False)
        server = next((h for h in hosts if h.get('host_name') == name), None)
        if not server: return None, (jsonify({'ok': False, 'error': 'Хост не найден'}), 404)
        return server, None
    if server_type == 'ssh':
        ssh_targets = rw_repo.get_all_ssh_targets()
        server = next((t for t in ssh_targets if t.get('target_name') == name), None)
        if not server: return None, (jsonify({'ok': False, 'error': 'SSH-цель не найдена'}), 404)
        return server, None
    return None, (jsonify({'ok': False, 'error': 'Неверный тип сервера'}), 400)
# ===== Конец функции get_ssh_server =====

# ===== ПОЛУЧЕНИЕ SSH УЧЕТНЫХ ДАННЫХ =====
# Подготавливает параметры подключения к серверу
def get_ssh_credentials(server):
    host = server.get('ssh_host')
    port = server.get('ssh_port', 22)
    username = server.get('ssh_user') or server.get('ssh_username', 'root')
    password = server.get('ssh_password')
    if not host or not password: return None, (jsonify({'ok': False, 'error': 'Параметры SSH не настроены'}), 400)
    return (host, port, username, password), None
# ===== Конец функции get_ssh_credentials =====

# ===== ПОЛУЧЕНИЕ ЭКЗЕМПЛЯРА БОТА =====
# Возвращает текущий объект бота с проверкой доступности
def get_bot_instance_safe():
    from shop_bot.webhook_server.app import _bot_controller
    bot = _bot_controller.get_bot_instance() if _bot_controller else None
    if not bot: return None, (jsonify({'ok': False, 'error': 'Бот недоступен'}), 500)
    return bot, None
# ===== Конец функции get_bot_instance_safe =====

# ===== ПОЛУЧЕНИЕ ID АДМИНИСТРАТОРА =====
# Извлекает Telegram ID администратора из базы данных
def get_admin_id_safe():
    admin_id = rw_repo.get_setting('admin_telegram_id')
    if not admin_id: return None, (jsonify({'ok': False, 'error': 'ID администратора не настроен'}), 400)
    return admin_id, None
# ===== Конец функции get_admin_id_safe =====

# ===== ВАЛИДАЦИЯ ПАРАМЕТРОВ ПРОМОКОДА =====
# Проверяет корректность введенных данных для создания промокода
def validate_promo_params(form_data):
    try:
        discount_type = form_data.get('discount_type', 'percent')
        discount_value = form_data.get('discount_value')
        usage_limit_total = form_data.get('usage_limit_total')
        usage_limit_per_user = form_data.get('usage_limit_per_user')
        valid_from = form_data.get('valid_from')
        valid_until = form_data.get('valid_until')
        description = form_data.get('description', '')

        if not discount_value: return None, (jsonify({'ok': False, 'error': 'Значение скидки обязательно'}), 400)

        try: discount_value = float(discount_value)
        except ValueError: return None, (jsonify({'ok': False, 'error': 'Некорректное значение скидки'}), 400)

        if discount_value <= 0: return None, (jsonify({'ok': False, 'error': 'Скидка должна быть положительной'}), 400)

        discount_percent = discount_value if discount_type == 'percent' else None
        discount_amount = discount_value if discount_type == 'fixed' else None
        usage_limit_total_int = int(usage_limit_total) if usage_limit_total else None
        usage_limit_per_user_int = int(usage_limit_per_user) if usage_limit_per_user else None
        
        valid_from_dt = datetime.fromisoformat(valid_from) if valid_from else None
        valid_until_dt = datetime.fromisoformat(valid_until) if valid_until else None

        return {
            'discount_percent': discount_percent,
            'discount_amount': discount_amount,
            'usage_limit_total': usage_limit_total_int,
            'usage_limit_per_user': usage_limit_per_user_int,
            'valid_from': valid_from_dt,
            'valid_until': valid_until_dt,
            'description': description
        }, None
    except Exception as e: return None, (jsonify({'ok': False, 'error': str(e)}), 400)
# ===== Конец функции validate_promo_params =====

# ===== СОХРАНЕНИЕ РЕЗУЛЬТАТОВ РАССЫЛКИ =====
# Записывает статистику последней рассылки в базу данных (МСК)
def save_broadcast_results(sent, failed, skipped):
    try:
        moscow_time = get_msk_time()
        
        results = {
            'sent': sent,
            'failed': failed,
            'skipped': skipped,
            'timestamp': moscow_time.isoformat()
        }
        rw_repo.set_other_value('newsletter', json.dumps(results, ensure_ascii=False))
    except Exception as e: logger.error(f"Не удалось сохранить результаты рассылки: {e}")
# ===== Конец функции save_broadcast_results =====

# ===== ЗАГРУЗКА РЕЗУЛЬТАТОВ РАССЫЛКИ =====
# Получает данные статистики последней рассылки
def load_broadcast_results():
    try:
        data = rw_repo.get_other_value('newsletter')
        if data: return json.loads(data)
    except Exception as e: logger.error(f"Не удалось загрузить результаты рассылки: {e}")
    return {'sent': 0, 'failed': 0, 'skipped': 0, 'timestamp': None}
# ===== Конец функции load_broadcast_results =====
    
# ===== ПОЛУЧЕНИЕ СПИСКА ЗАБАНЕННЫХ =====
def get_banned_users_data():
    try:
        data = rw_repo.get_other_value('id_newsletter')
        if data: return json.loads(data)
    except Exception as e: logger.error(f"Error loading id_newsletter: {e}")
    return {"count": 0, "id": []}
# ===== Конец функции get_banned_users_data =====

# ===== СОХРАНЕНИЕ СПИСКА ЗАБАНЕННЫХ =====
def save_banned_users_data(banned_ids):
    try:
        unique_ids = list(set(banned_ids))
        data = {"count": len(unique_ids), "id": unique_ids}
        rw_repo.set_other_value('id_newsletter', json.dumps(data, ensure_ascii=False))
    except Exception as e: logger.error(f"Error saving id_newsletter: {e}")
# ===== Конец функции save_banned_users_data =====

# ===== ВЫПОЛНЕНИЕ SSH КОМАНДЫ =====
# Выполняет одну команду через Paramiko и возвращает результат
def execute_ssh_command(host, port, username, password, command, timeout=10):
    try:
        import paramiko
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=host, port=port, username=username, password=password, timeout=timeout, look_for_keys=False, allow_agent=False)
        stdin, stdout, stderr = client.exec_command(command, timeout=timeout)
        output = stdout.read().decode('utf-8').strip()
        error = stderr.read().decode('utf-8').strip()
        exit_status = stdout.channel.recv_exit_status()
        client.close()
        return {'ok': exit_status == 0, 'output': output, 'error': error, 'exit_status': exit_status}
    except Exception as e:
        logger.error(f"Ошибка команды SSH ({host}:{port}): {e}")
        return {'ok': False, 'output': '', 'error': str(e), 'exit_status': -1}
# ===== Конец функции execute_ssh_command =====

# ===== АСИНХРОННАЯ ОТПРАВКА РАССЫЛКИ =====
# Выполняет рассылку сообщений пользователям с поддержкой медиа и кнопок
async def send_broadcast_async(bot, users, text, media_path=None, media_type=None, buttons=None, mode='all', task_id=None, skip_banned=False):
    sent, failed, skipped, total = 0, 0, 0, len(users)
    
    # Загружаем список забаненных
    banned_data = get_banned_users_data()
    banned_set = set(banned_data.get('id', []))
    
    if task_id:
        with broadcast_lock:
            broadcast_progress[task_id] = {
                'status': 'running', 'total': total, 'sent': 0, 'failed': 0, 'skipped': 0, 'progress': 0,
                'start_time': get_msk_time().isoformat()
            }
    
    for index, user in enumerate(users):
        user_id = user.get('telegram_id')
        if not user_id: continue
            
        # Пропускаем, если пользователь забанен и включен тумблер
        if skip_banned and user_id in banned_set:
            skipped += 1
            if task_id:
                with broadcast_lock:
                    if task_id in broadcast_progress:
                        broadcast_progress[task_id].update({'skipped': skipped, 'progress': int((index + 1) / total * 100)})
            continue

        if user.get('is_banned', False):
            skipped += 1
            banned_set.add(user_id)
            if task_id:
                with broadcast_lock:
                    if task_id in broadcast_progress:
                        broadcast_progress[task_id].update({'skipped': skipped, 'progress': int((index + 1) / total * 100)})
            continue
        
        try:
            keyboard = None
            if buttons:
                from aiogram.utils.keyboard import InlineKeyboardBuilder
                builder = InlineKeyboardBuilder()
                for btn in buttons:
                    btn_text, btn_url = btn.get('text', '').strip(), btn.get('url', '').strip()
                    if btn_text and btn_url and (btn_url.startswith('http://') or btn_url.startswith('https://')):
                        builder.button(text=btn_text, url=btn_url)
                builder.adjust(1)
                keyboard = builder.as_markup() if builder.export() else None
            
            if media_path and media_type:
                media_file = FSInputFile(media_path)
                if media_type == 'photo':
                    await bot.send_photo(chat_id=user_id, photo=media_file, caption=text, parse_mode='HTML', reply_markup=keyboard)
                elif media_type == 'video':
                    await bot.send_video(chat_id=user_id, video=media_file, caption=text, parse_mode='HTML', reply_markup=keyboard)
                elif media_type == 'animation':
                    await bot.send_animation(chat_id=user_id, animation=media_file, caption=text, parse_mode='HTML', reply_markup=keyboard)
            else: await bot.send_message(chat_id=user_id, text=text, parse_mode='HTML', reply_markup=keyboard)
            
            sent += 1
            # Если успешно отправили, убираем из списка забаненных (если был там)
            if user_id in banned_set:
                banned_set.remove(user_id)
            
            await asyncio.sleep(0.05)
            
        except Exception as e:
            # logger.warning(f"Не удалось отправить рассылку пользователю {user_id}: {e}")
            failed += 1
            # Добавляем в список забаненных
            banned_set.add(user_id)
        
        if task_id and ((index + 1) % 10 == 0 or (index + 1) == total):
            with broadcast_lock:
                if task_id in broadcast_progress:
                    broadcast_progress[task_id].update({'sent': sent, 'failed': failed, 'skipped': skipped, 'progress': int((index + 1) / total * 100)})
    
    if task_id:
        with broadcast_lock:
            if task_id in broadcast_progress:
                broadcast_progress[task_id].update({
                    'status': 'completed', 'sent': sent, 'failed': failed, 'skipped': skipped, 'progress': 100,
                    'end_time': get_msk_time().isoformat()
                })
    
    save_broadcast_results(sent, failed, skipped)
    # Сохраняем обновленный список забаненных
    save_banned_users_data(list(banned_set))
    
    if media_path and os.path.exists(media_path):
        try:
            os.remove(media_path)
            logger.info(f"Медиафайл удален: {media_path}")
        except Exception as e: logger.error(f"Не удалось удалить медиафайл {media_path}: {e}")
    
    return {'sent': sent, 'failed': failed, 'skipped': skipped}
# ===== Конец функции send_broadcast_async =====

# ===== РЕГИСТРАЦИЯ РОУТОВ МОДУЛЯ =====
# Инициализирует планировщик и подключает Flask-эндпоинты
def register_other_routes(flask_app, login_required, get_common_template_data):
    global scheduler
    if scheduler is None:
        scheduler = server_plan.ServerScheduler(ssh_executor=execute_ssh_command, log_func=lambda msg: logger.info(msg))
        scheduler.start()

    # ===== СТРАНИЦА "ПРОЧЕЕ" =====
    # Отображает основной интерфейс раздела дополнительных функций
    @flask_app.route('/other')
    @login_required
    def other_page():
        common_data = get_common_template_data()
        return render_template('other.html', **common_data)
    # ===== Конец роута other_page =====
    
    # ===== СТАТИСТИКА РАССЫЛКИ =====
    # Собирает данные о пользователях и ключах для формирования отчета рассылки
    @flask_app.route('/other/broadcast/stats')
    @login_required
    def broadcast_stats():
        try:
            all_users = rw_repo.get_all_users() or []
            total_users = len(all_users)
            users_with_active_keys, users_with_expired_keys, users_without_trial = 0, 0, 0
            
            expiring_counts = {1: 0, 3: 0, 5: 0, 10: 0}

            for user in all_users:
                user_id = user.get('telegram_id')
                keys = rw_repo.get_keys_for_user(user_id) or []
                has_active_key, has_expired_key = False, False
                min_days_remaining = None

                for key in keys:
                    expire_dt = parse_expire_dt(key.get('expire_at'))
                    if expire_dt:
                        now = get_msk_time()
                        if expire_dt > now:
                            has_active_key = True
                            days_rem = (expire_dt - now).days
                            if min_days_remaining is None or days_rem < min_days_remaining:
                                min_days_remaining = days_rem
                        else:
                            has_expired_key = True
                
                if has_active_key: 
                    users_with_active_keys += 1
                    if min_days_remaining is not None:
                        for day_limit in [1, 3, 5, 10]:
                            if min_days_remaining <= day_limit:
                                expiring_counts[day_limit] += 1

                if has_expired_key: users_with_expired_keys += 1
                if not user.get('trial_used', 0): users_without_trial += 1
            
            last_results = load_broadcast_results()
            
            # Получаем количество забаненных (failed list)
            banned_data = get_banned_users_data()
            banned_count = banned_data.get('count', 0)
            
            return jsonify({
                'ok': True, 'total_users': total_users, 'users_with_keys': users_with_active_keys,
                'users_with_expired_keys': users_with_expired_keys, 'users_without_trial': users_without_trial,
                'expiring_counts': expiring_counts,
                'last_results': last_results,
                'banned_count': banned_count
            })
        except Exception as e:
            logger.error(f"Ошибка получения статистики рассылки: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    # ===== Конец роута broadcast_stats =====
    
    # ===== ОЧИСТКА СПИСКА ЗАБАНЕННЫХ =====
    @flask_app.route('/other/broadcast/clear-banned', methods=['POST'])
    @login_required
    def broadcast_clear_banned():
        try:
            save_banned_users_data([])
            return jsonify({'ok': True, 'message': 'Список забаненных пользователей очищен'})
        except Exception as e:
            logger.error(f"Ошибка очистки списка забаненных: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    # ===== Конец роута broadcast_clear_banned =====
    
    # ===== ПРЕДПРОСМОТР РАССЫЛКИ =====
    # Отправляет тестовое сообщение администратору для проверки внешнего вида
    @flask_app.route('/other/broadcast/preview', methods=['POST'])
    @login_required
    def broadcast_preview():
        try:
            text, buttons_json, media_filename = request.form.get('text', ''), request.form.get('buttons', '[]'), request.form.get('media_filename', '')
            buttons = json.loads(buttons_json) if buttons_json else []
            
            admin_id, error = get_admin_id_safe()
            if error: return error
            
            bot, error = get_bot_instance_safe()
            if error: return error
            
            keyboard = None
            if buttons:
                from aiogram.utils.keyboard import InlineKeyboardBuilder
                builder = InlineKeyboardBuilder()
                for btn in buttons:
                    btn_text, btn_url = btn.get('text', '').strip(), btn.get('url', '').strip()
                    if btn_text and btn_url and (btn_url.startswith('http://') or btn_url.startswith('https://')):
                        builder.button(text=btn_text, url=btn_url)
                builder.adjust(1)
                keyboard = builder.as_markup() if builder.export() else None
            
            media_path, media_type = None, None
            if media_filename:
                media_path = os.path.join(UPLOAD_FOLDER, media_filename)
                if os.path.exists(media_path): media_type = get_media_type(media_filename)
            
            loop = current_app.config.get('EVENT_LOOP')
            if not loop or not loop.is_running(): return jsonify({'ok': False, 'error': 'Цикл событий недоступен'}), 500
            
            async def send_preview():
                preview_text = f"{text}\n\n📨 <b>Предпросмотр</b>"
                if media_path and media_type:
                    media_file = FSInputFile(media_path)
                    if media_type == 'photo': await bot.send_photo(chat_id=int(admin_id), photo=media_file, caption=preview_text, parse_mode='HTML', reply_markup=keyboard)
                    elif media_type == 'video': await bot.send_video(chat_id=int(admin_id), video=media_file, caption=preview_text, parse_mode='HTML', reply_markup=keyboard)
                    elif media_type == 'animation': await bot.send_animation(chat_id=int(admin_id), animation=media_file, caption=preview_text, parse_mode='HTML', reply_markup=keyboard)
                else: await bot.send_message(chat_id=int(admin_id), text=preview_text, parse_mode='HTML', reply_markup=keyboard)
            
            asyncio.run_coroutine_threadsafe(send_preview(), loop).result(timeout=10)
            return jsonify({'ok': True, 'message': 'Предпросмотр отправлен администратору'})
        except Exception as e:
            logger.error(f"Ошибка отправки предпросмотра: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    # ===== Конец роута broadcast_preview =====
    
    # ===== ЗАГРУЗКА МЕДИА ДЛЯ РАССЫЛКИ =====
    # Загружает файл изображения или видео на сервер для последующей рассылки
    @flask_app.route('/other/broadcast/upload', methods=['POST'])
    @login_required
    def broadcast_upload():
        try:
            if 'file' not in request.files: return jsonify({'ok': False, 'error': 'Файл не предоставлен'}), 400
            file = request.files['file']
            if file.filename == '': return jsonify({'ok': False, 'error': 'Файл не выбран'}), 400
            if not allowed_file(file.filename): return jsonify({'ok': False, 'error': 'Недопустимый тип файла'}), 400
            
            filename = secure_filename(file.filename)
            unique_filename = f"{uuid.uuid4().hex}_{filename}"
            filepath = os.path.join(UPLOAD_FOLDER, unique_filename)
            file.save(filepath)
            
            media_type = get_media_type(filename)
            return jsonify({'ok': True, 'filename': unique_filename, 'media_type': media_type, 'path': filepath})
        except Exception as e:
            logger.error(f"Ошибка загрузки медиа: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    # ===== Конец роута broadcast_upload =====
    
    # ===== ЗАПУСК МАССОВОЙ РАССЫЛКИ =====
    # Формирует список получателей и запускает асинхронный процесс отправки
    @flask_app.route('/other/broadcast/send', methods=['POST'])
    @login_required
    def broadcast_send():
        try:
            text, mode, buttons_json, media_filename = request.form.get('text', ''), request.form.get('mode', 'all'), request.form.get('buttons', '[]'), request.form.get('media_filename', '')
            skip_banned = request.form.get('skip_banned') == 'true'
            
            buttons = json.loads(buttons_json) if buttons_json else []
            if not text: return jsonify({'ok': False, 'error': 'Текст обязателен'}), 400
            
            bot, error = get_bot_instance_safe()
            if error: return error
            
            all_users = rw_repo.get_all_users() or []
            
            if mode == 'test':
                admin_id, error = get_admin_id_safe()
                if error: return error
                all_users = [{'telegram_id': int(admin_id), 'is_banned': False}]
            elif mode == 'with_keys':
                filtered_users = []
                for user in all_users:
                    user_id = user.get('telegram_id')
                    keys = rw_repo.get_keys_for_user(user_id) or []
                    has_active_key = False
                    for key in keys:
                        expire_dt = parse_expire_dt(key.get('expire_at'))
                        if expire_dt and expire_dt > get_msk_time():
                            has_active_key = True
                            break
                    if has_active_key: filtered_users.append(user)
                all_users = filtered_users
            elif mode == 'expired_keys':
                filtered_users = []
                for user in all_users:
                    user_id = user.get('telegram_id')
                    keys = rw_repo.get_keys_for_user(user_id) or []
                    has_active_key, has_expired_key = False, False
                    for key in keys:
                        expire_dt = parse_expire_dt(key.get('expire_at'))
                        if expire_dt:
                            now = get_msk_time()
                            if expire_dt > now:
                                has_active_key = True
                                break
                            else:
                                has_expired_key = True
                    if not has_active_key and has_expired_key: filtered_users.append(user)
                all_users = filtered_users
            elif mode == 'expiring_keys':
                expiring_days = request.form.get('expiring_days', '3')
                try: days_threshold = int(expiring_days)
                except ValueError: days_threshold = 3
                
                filtered_users = []
                for user in all_users:
                    user_id = user.get('telegram_id')
                    keys = rw_repo.get_keys_for_user(user_id) or []
                    has_expiring_key = False
                    for key in keys:
                        expire_dt = parse_expire_dt(key.get('expire_at'))
                        if expire_dt:
                            now = get_msk_time()
                            days_until_expiry = (expire_dt - now).days
                            if 0 <= days_until_expiry <= days_threshold:
                                has_expiring_key = True
                                break
                    if has_expiring_key: filtered_users.append(user)
                all_users = filtered_users
            elif mode == 'without_trial' or mode == 'not_used_trial':
                all_users = [u for u in all_users if not u.get('trial_used', 0)]
            
            media_path, media_type = None, None
            if media_filename:
                media_path = os.path.join(UPLOAD_FOLDER, media_filename)
                if os.path.exists(media_path): media_type = get_media_type(media_filename)
            
            loop = current_app.config.get('EVENT_LOOP')
            if not loop or not loop.is_running(): return jsonify({'ok': False, 'error': 'Цикл событий недоступен'}), 500
            
            task_id = str(uuid.uuid4())
            asyncio.run_coroutine_threadsafe(send_broadcast_async(bot, all_users, text, media_path, media_type, buttons, mode, task_id, skip_banned), loop)
            return jsonify({'ok': True, 'task_id': task_id, 'total_users': len(all_users)})
        except Exception as e:
            logger.error(f"Ошибка запуска рассылки: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    # ===== Конец роута broadcast_send =====
    
    # =====СТАТУС ТЕКУЩЕЙ РАССЫЛКИ =====
    # Возвращает текущий прогресс выполнения активной задачи рассылки
    @flask_app.route('/other/broadcast/status/<task_id>', methods=['GET'])
    @login_required
    def broadcast_status(task_id):
        with broadcast_lock:
            if task_id not in broadcast_progress: return jsonify({'ok': False, 'error': 'Задача не найдена'}), 404
            progress = broadcast_progress[task_id].copy()
        return jsonify({'ok': True, 'progress': progress})
    # ===== Конец роута broadcast_status =====
    
    # ===== УДАЛЕНИЕ МЕДИАФАЙЛА РАССЫЛКИ =====
    # Удаляет временный файл медиа с сервера
    @flask_app.route('/other/broadcast/delete-media/<filename>', methods=['DELETE'])
    @login_required
    def broadcast_delete_media(filename):
        try:
            filepath = os.path.join(UPLOAD_FOLDER, secure_filename(filename))
            if os.path.exists(filepath):
                os.remove(filepath)
                return jsonify({'ok': True})
            return jsonify({'ok': False, 'error': 'Файл не найден'}), 404
        except Exception as e:
            logger.error(f"Ошибка удаления медиафайла: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500

    @flask_app.route('/other/themes/save', methods=['POST'])
    @login_required
    def broadcast_themes_save():
        logger.info("Route /other/themes/save called")
        try:
            title = request.form.get('title', '').strip()
            content = request.form.get('content', '').strip()
            if not title or not content:
                return jsonify({'ok': False, 'error': 'Название и сообщение обязательны'}), 400
            
            data = rw_repo.get_other_value('theme_newsletter')
            themes = json.loads(data) if data else {}
            
            if len(themes) >= 5 and title not in themes:
                return jsonify({'ok': False, 'error': 'Максимум 5 шаблонов'}), 400
            
            themes[title] = content
            rw_repo.set_other_value('theme_newsletter', json.dumps(themes, ensure_ascii=False))
            logger.info(f"Theme '{title}' saved successfully")
            return jsonify({'ok': True, 'message': 'Шаблон сохранен'})
        except Exception as e:
            logger.error(f"Ошибка сохранения шаблона: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500

    @flask_app.route('/other/themes/list')
    @login_required
    def broadcast_themes_list():
        logger.info("Route /other/themes/list called")
        try:
            data = rw_repo.get_other_value('theme_newsletter')
            themes = json.loads(data) if data else {}
            return jsonify({'ok': True, 'themes': themes})
        except Exception as e:
            logger.error(f"Ошибка получения списка шаблонов: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    @flask_app.route('/other/themes/delete', methods=['POST'])
    @login_required
    def broadcast_themes_delete():
        try:
            title = request.form.get('title', '').strip()
            if not title:
                return jsonify({'ok': False, 'error': 'Название обязательно'}), 400
            
            data = rw_repo.get_other_value('theme_newsletter')
            themes = json.loads(data) if data else {}
            
            if title in themes:
                del themes[title]
                rw_repo.set_other_value('theme_newsletter', json.dumps(themes, ensure_ascii=False))
                return jsonify({'ok': True, 'message': 'Шаблон удален'})
            return jsonify({'ok': False, 'error': 'Шаблон не найден'}), 404
        except Exception as e:
            logger.error(f"Ошибка удаления шаблона: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    # ===== Конец роута broadcast_delete_media =====
    
    # ===== СПИСОК ПРОМОКОДОВ =====
    # Возвращает список всех существующих промокодов
    @flask_app.route('/other/promo/list')
    @login_required
    def promo_list():
        try:
            promos = rw_repo.list_promo_codes(include_inactive=True)
            return jsonify({'ok': True, 'promos': promos})
        except Exception as e:
            logger.error(f"Ошибка получения списка промокодов: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    # ===== Конец роута promo_list =====
    
    # ===== СОЗДАНИЕ ПРОМОКОДА =====
    # Генерирует или сохраняет новый промокод с заданными параметрами
    @flask_app.route('/other/promo/create', methods=['POST'])
    @login_required
    def promo_create():
        try:
            code = request.form.get('code', '').strip().upper()
            if not code:
                import string, random
                code = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(8))
            
            params, error = validate_promo_params(request.form)
            if error: return error
            
            admin_id, error = get_admin_id_safe()
            created_by = int(admin_id) if not error else None
            
            if rw_repo.create_promo_code(code=code, created_by=created_by, **params):
                return jsonify({'ok': True, 'code': code, 'message': 'Промокод успешно создан'})
            return jsonify({'ok': False, 'error': 'Такой код уже существует'}), 400
        except ValueError as e: return jsonify({'ok': False, 'error': str(e)}), 400
        except Exception as e:
            logger.error(f"Ошибка создания промокода: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    # ===== Конец роута promo_create =====
    
    # ===== ПЕРЕКЛЮЧЕНИЕ СТАТУСА ПРОМОКОДА =====
    # Активирует или деактивирует промокод
    @flask_app.route('/other/promo/toggle/<code>', methods=['POST'])
    @login_required
    def promo_toggle(code):
        try:
            promo = rw_repo.get_promo_code(code)
            if not promo: return jsonify({'ok': False, 'error': 'Промокод не найден'}), 404
            
            new_status = not promo.get('is_active', 1)
            if rw_repo.update_promo_code_status(code, is_active=new_status):
                return jsonify({'ok': True, 'is_active': new_status})
            return jsonify({'ok': False, 'error': 'Не удалось обновить статус'}), 500
        except Exception as e:
            logger.error(f"Ошибка переключения статуса промокода: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    # ===== Конец роута promo_toggle =====
    
    # ===== УДАЛЕНИЕ ПРОМОКОДА =====
    # Полностью удаляет промокод из базы данных
    @flask_app.route('/other/promo/delete/<code>', methods=['DELETE'])
    @login_required
    def promo_delete(code):
        try:
            if rw_repo.delete_promo_code(code):
                return jsonify({'ok': True, 'message': 'Промокод успешно удален'})
            return jsonify({'ok': False, 'error': 'Промокод не найден'}), 404
        except Exception as e:
            logger.error(f"Ошибка удаления промокода: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    # ===== Конец роута promo_delete =====
    
    # ===== ОБНОВЛЕНИЕ ПРОМОКОДА =====
    # Пересоздает промокод с новыми параметрами (сохраняя сам код)
    @flask_app.route('/other/promo/update/<code>', methods=['POST'])
    @login_required
    def promo_update(code):
        try:
            params, error = validate_promo_params(request.form)
            if error: return error
            
            rw_repo.delete_promo_code(code)
            admin_id, error = get_admin_id_safe()
            created_by = int(admin_id) if not error else None
            
            if rw_repo.create_promo_code(code=code, created_by=created_by, **params):
                return jsonify({'ok': True, 'message': 'Промокод успешно обновлен'})
            return jsonify({'ok': False, 'error': 'Не удалось обновить промокод'}), 500
        except Exception as e:
            logger.error(f"Ошибка обновления промокода: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    # ===== Конец роута promo_update =====
    

    
    # ===== СПИСОК СЕРВЕРОВ =====
    # Получает список всех хостов и SSH-целей с настроенным доступом
    @flask_app.route('/other/servers/list')
    @login_required
    def servers_list():
        try:
            hosts, ssh_targets = rw_repo.list_squads(active_only=False), rw_repo.get_all_ssh_targets()
            filtered_hosts = [h for h in hosts if h.get('ssh_host') and h.get('ssh_password')]
            filtered_ssh_targets = [t for t in ssh_targets if t.get('ssh_host') and t.get('ssh_password')]
            return jsonify({'ok': True, 'hosts': filtered_hosts, 'ssh_targets': filtered_ssh_targets})
        except Exception as e:
            logger.error(f"Ошибка получения списка серверов: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    # ===== Конец роута servers_list =====
    
    # ===== СОРТИРОВКА SSH СЕРВЕРОВ =====
    # Сохраняет новый порядок отображения SSH-целей
    @flask_app.route('/other/servers/ssh/reorder', methods=['POST'])
    @login_required
    def ssh_servers_reorder():
        try:
            data = request.get_json()
            if not data: return jsonify({'ok': False, 'error': 'Некорректный JSON'}), 400
            order = data.get('order', [])
            if not isinstance(order, list): return jsonify({'ok': False, 'error': 'Неверный формат порядка'}), 400
            for index, target_name in enumerate(order): rw_repo.update_ssh_target_sort_order(target_name, index)
            return jsonify({'ok': True, 'message': 'Порядок SSH-серверов сохранён'})
        except Exception as e:
            logger.error(f"Ошибка сортировки SSH-серверов: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    # ===== Конец роута ssh_servers_reorder =====

    # ===== СОРТИРОВКА ХОСТОВ =====
    # Сохраняет новый порядок отображения основных хостов
    @flask_app.route('/other/servers/hosts/reorder', methods=['POST'])
    @login_required
    def hosts_reorder():
        try:
            data = request.get_json()
            if not data: return jsonify({'ok': False, 'error': 'Некорректный JSON'}), 400
            order = data.get('order', [])
            if not isinstance(order, list): return jsonify({'ok': False, 'error': 'Неверный формат порядка'}), 400
            for index, host_name in enumerate(order): rw_repo.update_host_sort_order(host_name, index)
            return jsonify({'ok': True, 'message': 'Порядок хостов сохранён'})
        except Exception as e:
            logger.error(f"Ошибка сортировки хостов: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    # ===== Конец роута hosts_reorder =====
    
    # ===== ИНФОРМАЦИЯ О СЕРВЕРЕ (UPTIME/LOAD) =====
    # Собирает данные о нагрузке системы через SSH: CPU, RAM, SWAP и Uptime
    @flask_app.route('/other/servers/uptime/<server_type>/<name>')
    @login_required
    def server_uptime(server_type, name):
        try:
            server, error = get_ssh_server(name, server_type)
            if error: return error
            creds, error = get_ssh_credentials(server)
            if error: return error
            host, port, username, password = creds
            
            delimiter = "___"
            command = (
                f"cat /proc/uptime || echo '0 0'; echo '{delimiter}'; "
                f"top -bn1 | grep 'Cpu(s)' | awk '{{print $2}}' || echo '0.0'; echo '{delimiter}'; "
                f"nproc || echo '1'; echo '{delimiter}'; "
                f"free -m | grep Mem | awk '{{print $3 \" \" $2}}' || echo '0 0'; echo '{delimiter}'; "
                f"free -m | grep Swap | awk '{{print $3 \" \" $2}}' || echo '0 0'; echo '{delimiter}'; "
                f"cat /proc/sys/vm/swappiness || echo '-1'; echo '{delimiter}'; "
                f"awk 'NR>2 && $1 !~ /lo/ {{rx += $2; tx += $10}} END {{print rx \" \" tx}}' /proc/net/dev || echo '0 0'; echo '{delimiter}'; "
                f"sleep 1; awk 'NR>2 && $1 !~ /lo/ {{rx += $2; tx += $10}} END {{print rx \" \" tx}}' /proc/net/dev || echo '0 0'"
            )
            result = execute_ssh_command(host, port, username, password, command, timeout=20)
            
            if result['ok']:
                try:
                    output_raw = result['output'].strip()
                    parts = output_raw.split(delimiter)
                    if len(parts) < 8:
                        logger.error(f"Ошибка формата данных для {name}: получено {len(parts)} из 8 частей")
                        return jsonify({'ok': False, 'error': 'Неполные данные о системе'}), 500
                    
                    uptime_parts = parts[0].strip().split()
                    uptime_seconds = float(uptime_parts[0]) if (uptime_parts and uptime_parts[0]) else 0
                    
                    cpu_str = parts[1].strip().replace(',', '.').replace('%', '') 
                    cpu_usage = float(cpu_str) if (cpu_str and cpu_str != '') else 0.0
                    
                    cpu_cores = int(parts[2].strip()) if parts[2].strip().isdigit() else 1
                    
                    ram_str = parts[3].strip().split()
                    ram_used, ram_total = (int(ram_str[0]), int(ram_str[1])) if len(ram_str) >= 2 else (0, 0)
                    ram_percent = (ram_used / ram_total * 100) if ram_total > 0 else 0
                    
                    swap_str = parts[4].strip().split()
                    swap_used, swap_total = (int(swap_str[0]), int(swap_str[1])) if len(swap_str) >= 2 else (0, 0)
                    swap_percent = (swap_used / swap_total * 100) if swap_total > 0 else 0
                    
                    swappiness_str = parts[5].strip()
                    swappiness = int(swappiness_str) if (swappiness_str and swappiness_str.replace('-','').isdigit()) else -1

                    net1_str = parts[6].strip().split()
                    rx1, tx1 = (int(float(net1_str[0])), int(float(net1_str[1]))) if len(net1_str) >= 2 else (0, 0)
                    
                    net2_str = parts[7].strip().split()
                    rx2, tx2 = (int(float(net2_str[0])), int(float(net2_str[1]))) if len(net2_str) >= 2 else (0, 0)
                    
                    # Разница за 1 секунду (байты)
                    rx_diff = max(0, rx2 - rx1)
                    tx_diff = max(0, tx2 - tx1)
                    
                    # Конвертация в Мегабайты и Мегабиты (используем базу 10^6 для сетевых скоростей)
                    net_rx_mbs = round(rx_diff / 1000000, 2)
                    net_tx_mbs = round(tx_diff / 1000000, 2)
                    
                    net_rx_mbps = round((rx_diff * 8) / 1000000, 2)
                    net_tx_mbps = round((tx_diff * 8) / 1000000, 2)

                    return jsonify({
                        'ok': True, 'uptime_seconds': uptime_seconds, 'uptime_formatted': format_uptime(uptime_seconds),
                        'cpu_percent': round(cpu_usage, 1), 'cpu_cores': cpu_cores,
                        'ram_used': ram_used, 'ram_total': ram_total, 'ram_percent': round(ram_percent, 1),
                        'swap_used': swap_used, 'swap_total': swap_total, 'swap_percent': round(swap_percent, 1),
                        'swappiness': swappiness,
                        'net_rx_mbps': net_rx_mbps, 'net_tx_mbps': net_tx_mbps,
                        'net_rx_mbs': net_rx_mbs, 'net_tx_mbs': net_tx_mbs
                    })
                except Exception as parse_error:
                    logger.exception(f"Ошибка парсинга системной инфо для {name}: {parse_error}")
                    return jsonify({'ok': False, 'error': f'Ошибка обработки данных: {parse_error}'}), 500
            else:
                error_msg = result.get('error', 'Неизвестная ошибка')
                if 'timed out' in error_msg.lower() or 'timeout' in error_msg.lower():
                    logger.warning(f"Таймаут SSH для {server_type}/{name}: {error_msg}")
                    return jsonify({'ok': False, 'error': 'Таймаут подключения к серверу'}), 503
                logger.error(f"Ошибка SSH команды для {server_type}/{name}: {error_msg}")
                return jsonify({'ok': False, 'error': error_msg}), 503
        except Exception as e:
            logger.error(f"Ошибка получения uptime для {server_type}/{name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    # ===== Конец роута server_uptime =====
    
    # ===== ФОРМАТИРОВАНИЕ ВРЕМЕНИ РАБОТЫ =====
    # Преобразует секунды в человекочитаемый формат (д, ч, м)
    def format_uptime(seconds):
        days, hours, minutes = int(seconds // 86400), int((seconds % 86400) // 3600), int((seconds % 3600) // 60)
        parts = []
        if days > 0: parts.append(f"{days}д")
        if hours > 0: parts.append(f"{hours}ч")
        if minutes > 0 or not parts: parts.append(f"{minutes}м")
        return ' '.join(parts)
    # ===== Конец функции format_uptime =====
    
    # ===== ПЕРЕЗАГРУЗКА СЕРВЕРА =====
    # Отправляет команду на перезагрузку через SSH
    @flask_app.route('/other/servers/reboot/<server_type>/<name>', methods=['POST'])
    @login_required
    def server_reboot(server_type, name):
        try:
            server, error = get_ssh_server(name, server_type)
            if error: return error
            creds, error = get_ssh_credentials(server)
            if error: return error
            host, port, username, password = creds
            
            logger.info(f"Перезагрузка сервера {server_type}/{name} ({host}:{port})")
            execute_ssh_command(host, port, username, password, 'sudo reboot', timeout=5)
            return jsonify({'ok': True, 'message': f'Команда перезагрузки отправлена на {name}'})
        except Exception as e:
            logger.error(f"Ошибка перезагрузки {server_type}/{name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    # ===== Конец роута server_reboot =====
    
    # ===== ПРОВЕРКА СОСТОЯНИЯ РАЗВЕРТЫВАНИЯ =====
    # Проверяет наличие Docker, рабочей директории и конфигурации на сервере
    @flask_app.route('/other/servers/deploy/check-status/<name>', methods=['GET'])
    @login_required
    def deploy_check_status(name):
        try:
            server, error = get_ssh_server(name, 'ssh')
            if error: return error
            creds, error = get_ssh_credentials(server)
            if error: return error
            host, port, username, password = creds
            
            status = {'docker_installed': False, 'directory_exists': False, 'compose_file_exists': False, 'suggested_step': 1}
            
            logger.info(f"Проверка Docker на {name} ({host}:{port})")
            docker_check = execute_ssh_command(host, port, username, password, 'docker --version', timeout=10)
            status['docker_installed'] = docker_check['ok']
            
            if status['docker_installed']:
                dir_check = execute_ssh_command(host, port, username, password, 'test -d /opt/remnanode && echo "exists"', timeout=10)
                status['directory_exists'] = 'exists' in dir_check.get('output', '')
                if status['directory_exists']:
                    compose_check = execute_ssh_command(host, port, username, password, 'test -f /opt/remnanode/docker-compose.yml && echo "exists"', timeout=10)
                    status['compose_file_exists'] = 'exists' in compose_check.get('output', '')
            
            if not status['docker_installed']: status['suggested_step'] = 1
            elif not status['directory_exists']: status['suggested_step'] = 2
            elif not status['compose_file_exists']: status['suggested_step'] = 3
            else: status['suggested_step'] = 5  
            
            return jsonify({'ok': True, 'status': status})
        except Exception as e:
            logger.error(f"Ошибка проверки статуса развертывания на {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    # ===== Конец роута deploy_check_status =====
    
    # ===== УСТАНОВКА DOCKER =====
    # Устанавливает Docker на удаленный сервер в зависимости от типа ОС
    @flask_app.route('/other/servers/deploy/install-docker/<name>', methods=['POST'])
    @login_required
    def deploy_install_docker(name):
        try:
            os_type = request.form.get('os_type', 'ubuntu')
            server, error = get_ssh_server(name, 'ssh')
            if error: return error
            creds, error = get_ssh_credentials(server)
            if error: return error
            host, port, username, password = creds
            
            docker_install_cmd = 'curl -fsSL https://get.docker.com | sh' if os_type == 'debian' else 'sudo curl -fsSL https://get.docker.com | sh'
            logger.info(f"Установка Docker на {name} ({host}:{port}) - режим {os_type}")
            
            result = execute_ssh_command(host, port, username, password, docker_install_cmd, timeout=300)
            if result['ok']:
                return jsonify({'ok': True, 'message': 'Docker успешно установлен', 'output': result['output']})
            return jsonify({'ok': False, 'error': result['error'] or 'Не удалось установить Docker', 'output': result['output']}), 500
        except Exception as e:
            logger.error(f"Ошибка установки Docker на {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    # ===== Конец роута deploy_install_docker =====
    
    # ===== СОЗДАНИЕ ДИРЕКТОРИИ РАЗВЕРТЫВАНИЯ =====
    # Подготавливает рабочую директорию /opt/remnanode на сервере
    @flask_app.route('/other/servers/deploy/create-directory/<name>', methods=['POST'])
    @login_required
    def deploy_create_directory(name):
        try:
            server, error = get_ssh_server(name, 'ssh')
            if error: return error
            creds, error = get_ssh_credentials(server)
            if error: return error
            host, port, username, password = creds
            
            logger.info(f"Создание директории на {name} ({host}:{port})")
            result = execute_ssh_command(host, port, username, password, 'mkdir -p /opt/remnanode && cd /opt/remnanode && pwd', timeout=30)
            
            if result['ok']:
                return jsonify({'ok': True, 'message': 'Директория успешно создана', 'output': result['output']})
            return jsonify({'ok': False, 'error': result['error'] or 'Не удалось создать директорию', 'output': result['output']}), 500
        except Exception as e:
            logger.error(f"Ошибка создания директории на {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    # ===== Конец роута deploy_create_directory =====
    
    # ===== СОХРАНЕНИЕ DOCKER-COMPOSE =====
    # Записывает содержимое docker-compose.yml в рабочую директорию сервера
    @flask_app.route('/other/servers/deploy/save-compose/<name>', methods=['POST'])
    @login_required
    def deploy_save_compose(name):
        try:
            content = request.form.get('content', '').strip()
            if not content: return jsonify({'ok': False, 'error': 'Содержимое обязательно'}), 400
            
            server, error = get_ssh_server(name, 'ssh')
            if error: return error
            creds, error = get_ssh_credentials(server)
            if error: return error
            host, port, username, password = creds
            
            logger.info(f"Сохранение docker-compose.yml на {name} ({host}:{port})")
            result = execute_ssh_command(host, port, username, password, f"cd /opt/remnanode && cat > docker-compose.yml << 'EOF'\n{content}\nEOF", timeout=30)
            
            if result['ok'] or result['exit_status'] == 0:
                return jsonify({'ok': True, 'message': 'docker-compose.yml успешно сохранен'})
            return jsonify({'ok': False, 'error': result['error'] or 'Не удалось сохранить docker-compose.yml', 'output': result['output']}), 500
        except Exception as e:
            logger.error(f"Ошибка сохранения docker-compose.yml на {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    # ===== Конец роута deploy_save_compose =====
    
    # ===== ПРОСМОТР DOCKER-COMPOSE =====
    # Считывает содержимое docker-compose.yml с удаленного сервера
    @flask_app.route('/other/servers/deploy/view-compose/<name>', methods=['GET'])
    @login_required
    def deploy_view_compose(name):
        try:
            server, error = get_ssh_server(name, 'ssh')
            if error: return error
            creds, error = get_ssh_credentials(server)
            if error: return error
            host, port, username, password = creds
            
            logger.info(f"Чтение docker-compose.yml с {name} ({host}:{port})")
            result = execute_ssh_command(host, port, username, password, 'cd /opt/remnanode && cat docker-compose.yml', timeout=30)
            
            if result['ok']: return jsonify({'ok': True, 'content': result['output']})
            return jsonify({'ok': False, 'error': result['error'] or 'Файл не найден или ошибка чтения', 'output': result['output']}), 500
        except Exception as e:
            logger.error(f"Ошибка чтения docker-compose.yml с {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    # ===== Конец роута deploy_view_compose =====
    
    # ===== УПРАВЛЕНИЕ КОНТЕЙНЕРАМИ =====
    # Выполняет команды docker compose (start, restart, logs) на сервере
    @flask_app.route('/other/servers/deploy/manage-containers/<name>', methods=['POST'])
    @login_required
    def deploy_manage_containers(name):
        try:
            action = request.form.get('action', 'start')  
            server, error = get_ssh_server(name, 'ssh')
            if error: return error
            creds, error = get_ssh_credentials(server)
            if error: return error
            host, port, username, password = creds
            
            if action == 'start': command, timeout = 'cd /opt/remnanode && docker compose up -d', 120
            elif action == 'restart': command, timeout = 'cd /opt/remnanode && docker compose restart remnanode', 60
            elif action == 'logs': command, timeout = 'cd /opt/remnanode && docker compose logs -t --tail=100 remnanode', 30
            else: return jsonify({'ok': False, 'error': 'Неверное действие'}), 400
            
            logger.info(f"Управление контейнерами на {name} ({host}:{port}) - действие: {action}")
            result = execute_ssh_command(host, port, username, password, command, timeout=timeout)
            
            if result['ok'] or result['exit_status'] == 0:
                return jsonify({'ok': True, 'message': f'Действие {action} успешно выполнено', 'output': result['output']})
            return jsonify({'ok': False, 'error': result['error'] or f'Не удалось выполнить {action}', 'output': result['output']}), 500
        except Exception as e:
            logger.error(f"Ошибка управления контейнерами на {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    # ===== Конец роута deploy_manage_containers =====
    
    # ===== ПОЛНОЕ УДАЛЕНИЕ НОДЫ И DOCKER =====
    # Очищает рабочую директорию и удаляет Docker-пакеты с сервера
    @flask_app.route('/other/servers/deploy/remove-all/<name>', methods=['POST'])
    @login_required
    def deploy_remove_all(name):
        try:
            server, error = get_ssh_server(name, 'ssh')
            if error: return error
            creds, error = get_ssh_credentials(server)
            if error: return error
            host, port, username, password = creds
            
            command = (
                '(if [ -f /opt/remnanode/docker-compose.yml ]; then cd /opt/remnanode && sudo docker compose down 2>/dev/null || true; fi; '
                'sudo rm -rf /opt/remnanode; '
                'if command -v docker &> /dev/null; then '
                'sudo apt-get purge -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin docker-ce-rootless-extras 2>/dev/null || true; '
                'sudo rm -rf /var/lib/docker /var/lib/containerd ~/.docker 2>/dev/null || true; fi; '
                'echo "Cleanup completed")'
            )
            
            logger.warning(f"УДАЛЕНИЕ ВСЕХ ДАННЫХ Docker и ноды на {name} ({host}:{port})")
            result = execute_ssh_command(host, port, username, password, command, timeout=180)
            
            if result.get('output') and 'Cleanup completed' in result.get('output', ''):
                return jsonify({'ok': True, 'message': 'Нода и Docker полностью удалены', 'output': result['output']})
            if result.get('ok') or result.get('exit_status') == 0:
                return jsonify({'ok': True, 'message': 'Команда удаления выполнена', 'output': result.get('output', '')})
            
            logger.error(f"Удаление не удалось на {name}: {result.get('error')}, вывод: {result.get('output')}")
            return jsonify({'ok': False, 'error': result.get('error') or 'Не удалось выполнить удаление', 'output': result.get('output', '')}), 500
        except Exception as e:
            logger.error(f"Ошибка при полном удалении на {name}: {e}", exc_info=True)
            return jsonify({'ok': False, 'error': str(e)}), 500
    # ===== Конец роута deploy_remove_all =====
    
    # ===== СТРИМИНГ ЛОГОВ БОТА =====
    # Обеспечивает передачу логов в реальном времени через SSE (Server-Sent Events)
    @flask_app.route('/other/logs/stream')
    @login_required
    def logs_stream():
        def generate():
            import subprocess, shutil, time, socket, http.client
            tail_lines = "100"
            
            if os.name == 'nt':
                yield f"data: [INFO] --- Windows Logs Simulation Mode ---\n\n"
                while True:
                    yield f"data: [INFO] {get_msk_time().isoformat()} - Heartbeat\n\n"
                    time.sleep(2)
                return

            cli_cmd = ['docker-compose', 'logs', '-f', f'--tail={tail_lines}'] if shutil.which('docker-compose') else (['docker', 'compose', 'logs', '-f', f'--tail={tail_lines}'] if shutil.which('docker') else None)
            
            if cli_cmd and os.path.exists('/root/remnawave-shopbot'):
                yield f"data: [INFO] Docker CLI найден. Попытка стриминга через команду...\n\n"
                try:
                    process = subprocess.Popen(cli_cmd, cwd='/root/remnawave-shopbot', stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True, bufsize=1)
                    for line in iter(process.stdout.readline, ''):
                        if line: yield f"data: {line.rstrip()}\n\n"
                    process.stdout.close()
                    yield f"data: [EXIT] Процесс CLI завершен.\n\n"
                    return 
                except Exception as e: yield f"data: [WARN] Ошибка CLI: {e}. Пробуем Docker Socket...\n\n"
            
            socket_path = '/var/run/docker.sock'
            if os.path.exists(socket_path):
                yield f"data: [INFO] Docker socket найден в {socket_path}. Подключение...\n\n"
                try:
                    hostname = socket.gethostname()
                    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    sock.connect(socket_path)
                    
                    request = f"GET /containers/{hostname}/logs?stdout=1&stderr=1&follow=1&tail={tail_lines} HTTP/1.1\r\nHost: localhost\r\n\r\n"
                    sock.sendall(request.encode('ascii'))
                    
                    fp = sock.makefile('rb')
                    while True:
                        line = fp.readline()
                        if line in (b'\r\n', b'\n', b''): break
                        
                    while True:
                        header = fp.read(8)
                        if not header or len(header) < 8: break
                        import struct
                        payload_size = struct.unpack('>I', header[4:])[0]
                        if payload_size > 0:
                            payload = fp.read(payload_size)
                            if not payload: break
                            try:
                                text = payload.decode('utf-8', errors='replace')
                                for line in text.splitlines(): yield f"data: {line}\n\n"
                            except: pass
                    sock.close()
                    yield f"data: [EXIT] Стрим через сокет завершен.\n\n"
                    return
                except Exception as e: yield f"data: [ERROR] Ошибка подключения к сокету: {e}\n\n"
            else: yield f"data: [WARN] Docker socket не найден в {socket_path}.\n\n"

            log_files = ['logs/bot.log', 'bot.log']
            found_log = False
            for log_file in log_files:
                if os.path.exists(log_file):
                    found_log = True
                    yield f"data: [INFO] Чтение локального файла логов: {log_file}\n\n"
                    try:
                        from collections import deque
                        with open(log_file, 'r', encoding='utf-8', errors='replace') as f:
                            for line in deque(f, int(tail_lines)): yield f"data: {line.strip()}\n\n"
                            f.seek(0, os.SEEK_END)
                            while True:
                                line = f.readline()
                                if not line:
                                    time.sleep(0.5)
                                    continue
                                yield f"data: {line.strip()}\n\n"
                    except Exception as e: yield f"data: [ERROR] Ошибка чтения файла: {e}\n\n"
                    break
            
            if not found_log: yield f"data: [WARN] Методы получения логов недоступны.\n\n"

        response = current_app.response_class(generate(), mimetype='text/event-stream')
        response.headers['Cache-Control'] = 'no-cache'
        response.headers['X-Accel-Buffering'] = 'no'
        response.headers['Connection'] = 'keep-alive'
        return response
    # ===== Конец роута logs_stream =====

    # ===== ИСТОРИЯ ЛОГОВ =====
    # Возвращает последние N строк логов из Docker или локальных файлов
    @flask_app.route('/other/logs/history')
    @login_required
    def logs_history():
        try:
            lines_count = int(request.args.get('lines', 50))
            lines_count = min(lines_count, 50) # Принудительное ограничение
            offset = int(request.args.get('offset', 0))
        except ValueError: return jsonify({'ok': False, 'error': 'Некорректные параметры'})

        import subprocess, shutil
        if shutil.which('docker-compose') or shutil.which('docker'):
            total_fetch = offset + lines_count
            cli_cmd = ['docker-compose', 'logs', f'--tail={total_fetch}'] if shutil.which('docker-compose') else ['docker', 'compose', 'logs', f'--tail={total_fetch}']
                
            if cli_cmd and os.path.exists('/root/remnawave-shopbot'):
                try:
                    result = subprocess.run(cli_cmd, cwd='/root/remnawave-shopbot', capture_output=True, text=True, timeout=5)
                    if result.returncode == 0:
                        all_lines = result.stdout.splitlines()
                        target_lines = all_lines[:len(all_lines) - offset]
                        chunk = target_lines[-lines_count:] if lines_count < len(target_lines) else target_lines
                        return jsonify({'ok': True, 'lines': chunk})
                except Exception as e: logger.error(f"Ошибка получения истории из Docker: {e}")

        log_files = ['logs/bot.log', 'bot.log']
        for log_file in log_files:
            if os.path.exists(log_file):
                try: 
                    with open(log_file, 'r', encoding='utf-8', errors='replace') as f: all_lines = f.readlines()
                    target_lines = all_lines[:len(all_lines) - offset]
                    chunk = target_lines[-lines_count:] if lines_count < len(target_lines) else target_lines
                    return jsonify({'ok': True, 'lines': [l.rstrip() for l in chunk]})
                except Exception as e: return jsonify({'ok': False, 'error': str(e)})

        return jsonify({'ok': False, 'error': 'Логи недоступны'})
    # ===== Конец роута logs_history =====
    
    # ===== ОЧИСТКА ЛОГОВ (ЛОКАЛЬНЫХ ИЛИ DOCKER) =====
    # Пытается очистить локальные файлы логов или логи контейнера Docker
    @flask_app.route('/other/logs/clear', methods=['POST'])
    @login_required
    def logs_clear():
        try:
            import subprocess
            cleared_any, log_files = False, ['logs/bot.log', 'bot.log']
            for log_file in log_files:
                if os.path.exists(log_file):
                    try:
                        with open(log_file, 'w', encoding='utf-8') as f: pass
                        logger.info(f"Локальный лог {log_file} очищен"); cleared_any = True
                    except Exception as e: logger.error(f"Не удалось очистить {log_file}: {e}")
            
            if cleared_any: return jsonify({'ok': True, 'message': 'Локальные логи успешно очищены'})
            if os.name == 'nt':
                logger.info("Обнаружена Windows, имитация очистки логов")
                return jsonify({'ok': True, 'message': 'Логи очищены (имитация)'})
            
            result = subprocess.run("truncate -s 0 /var/lib/docker/containers/*/*-json.log", shell=True, capture_output=True, text=True)
            if result.returncode == 0: return jsonify({'ok': True, 'message': 'Логи Docker успешно очищены'})
            return jsonify({'ok': False, 'error': f"Ошибка: {result.stderr or 'Доступ запрещен'}"}), 500
        except Exception as e:
            logger.error(f"Ошибка очистки логов: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    # ===== Конец роута logs_clear =====

    # ===== ПЕРЕЗАПУСК БОТА =====
    # Выполняет полный перезапуск сервиса через docker-compose или завершает процесс
    @flask_app.route('/other/restart', methods=['POST'])
    @login_required
    def logs_restart():
        try:
            import subprocess
            cmd = None
            try:
                subprocess.run(["docker-compose", "--version"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                cmd = "docker-compose restart"
            except FileNotFoundError:
                try:
                    subprocess.run(["docker", "compose", "version"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    cmd = "docker compose restart"
                except FileNotFoundError: pass
            
            if not cmd:
                logger.warning("Docker CLI не найден. Используется выход из процесса для перезапуска.")
                def suicide():
                    import time, sys
                    time.sleep(1)
                    logger.critical("Выполнение самозавершения через sys.exit(1)")
                    os._exit(1)
                threading.Thread(target=suicide).start()
                return jsonify({'ok': True, 'message': 'Перезапускаем процесс...'})

            subprocess.Popen(cmd, shell=True) 
            return jsonify({'ok': True, 'message': 'Команда перезапуска отправлена. Подождите 10-20 секунд.'})
        except Exception as e:
            logger.error(f"Ошибка перезапуска бота: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    # ===== Конец роута logs_restart =====

    # =====СТАТУС WARP (WIREPROXY) =====
    # Проверяет активность сервиса wireproxy и наличие бинарного файла
    @flask_app.route('/other/servers/warp/status/<name>', methods=['GET'])
    @login_required
    def warp_status(name):
        try:
            server, error = get_ssh_server(name, 'ssh')
            if error: return error
            creds, error = get_ssh_credentials(server)
            if error: return error
            host, port, username, password = creds
            
            command = (
                "systemctl is-active wireproxy; "
                "if systemctl list-unit-files | grep -q wireproxy; then echo 'SERVICE_EXISTS'; else echo 'SERVICE_MISSING'; fi; "
                "if [ -f /usr/local/bin/wireproxy ] || [ -f /usr/bin/wireproxy ]; then echo 'BINARY_FOUND'; else echo 'BINARY_MISSING'; fi; "
                "systemctl cat wireproxy 2>/dev/null | grep -E 'MemoryMax|MemoryHigh' || true"
            )
            result = execute_ssh_command(host, port, username, password, command, timeout=15)
            status = {'installed': False, 'active': False, 'service_exists': False, 'binary_exists': False, 'memory_max': 'N/A', 'memory_high': 'N/A'}
            
            if result['ok']:
                lines = result['output'].splitlines()
                if len(lines) >= 3:
                    status['active'] = lines[0].strip() == 'active'
                    status['service_exists'] = 'SERVICE_EXISTS' in result['output']
                    status['binary_exists'] = 'BINARY_FOUND' in result['output']
                    status['installed'] = status['binary_exists']
                    import re
                    all_max = re.findall(r'MemoryMax=([^\s]+)', result['output'])
                    all_high = re.findall(r'MemoryHigh=([^\s]+)', result['output'])
                    if all_max: status['memory_max'] = all_max[-1]
                    if all_high: status['memory_high'] = all_high[-1]
            
            return jsonify({'ok': True, 'status': status})
        except Exception as e:
            logger.error(f"Ошибка проверки статуса WARP на {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    # ===== Конец роута warp_status =====

    # ===== УСТАНОВКА WARP (WIREPROXY) =====
    # Устанавливает WARP через скрипт fscarmen и настраивает лимиты памяти
    @flask_app.route('/other/servers/warp/install/<name>', methods=['POST'])
    @login_required
    def warp_install(name):
        try:
            server, error = get_ssh_server(name, 'ssh')
            if error: return error
            creds, error = get_ssh_credentials(server)
            if error: return error
            host, port, username, password = creds
            
            install_cmd = "printf '1\\n1\\n40000\\n' | bash <(curl -fsSL https://gitlab.com/fscarmen/warp/-/raw/main/menu.sh) w"
            logger.info(f"Установка WARP на {name} ({host}:{port})")
            result = execute_ssh_command(host, port, username, password, install_cmd, timeout=300)
            
            if result['ok'] or "Socks5 configured" in result['output']:
                try:
                    config_cmd = (
                        "mkdir -p /etc/systemd/system/wireproxy.service.d && "
                        "printf '[Service]\\nEnvironment=\"WG_LOG_LEVEL=error\"\\nStandardOutput=null\\nStandardError=journal\\nMemoryMax=800M\\nMemoryHigh=1G\\n' > /etc/systemd/system/wireproxy.service.d/override.conf && "
                        "systemctl daemon-reload && systemctl restart wireproxy"
                    )
                    logger.info(f"Применение настроек по умолчанию для WARP на {name}")
                    config_res = execute_ssh_command(host, port, username, password, config_cmd, timeout=30)
                    if config_res['ok']: result['output'] += "\n[Config] Applied default settings (800M/1G)"
                    else: result['output'] += f"\n[Config] Failed to apply defaults: {config_res['error']}"
                except Exception as e: logger.error(f"Не удалось применить конфигурацию на {name}: {e}")
            
            if result['ok'] or "Socks5 configured" in result['output']:
                 return jsonify({'ok': True, 'message': 'WARP успешно установлен', 'output': result['output']})
            return jsonify({'ok': False, 'error': result['error'] or 'Ошибка установки', 'output': result['output']}), 500
        except Exception as e:
            logger.error(f"Ошибка установки WARP на {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    # ===== Конец роута warp_install =====

    # ===== УДАЛЕНИЕ WARP =====
    # Удаляет сервис wireproxy через скрипт fscarmen
    @flask_app.route('/other/servers/warp/uninstall/<name>', methods=['POST'])
    @login_required
    def warp_uninstall(name):
        try:
            server, error = get_ssh_server(name, 'ssh')
            if error: return error
            creds, error = get_ssh_credentials(server)
            if error: return error
            host, port, username, password = creds
            
            logger.info(f"Удаление WARP на {name}")
            result = execute_ssh_command(host, port, username, password, "printf 'y\\n' | bash <(curl -fsSL https://gitlab.com/fscarmen/warp/-/raw/main/menu.sh) u", timeout=120)
            
            if result['ok']: return jsonify({'ok': True, 'message': 'WARP успешно удален', 'output': result['output']})
            return jsonify({'ok': False, 'error': result['error'] or 'Ошибка удаления', 'output': result['output']}), 500
        except Exception as e:
            logger.error(f"Ошибка удаления WARP на {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    # ===== Конец роута warp_uninstall =====

    # ===== КОНФИГУРАЦИЯ WARP =====
    # Настраивает лимиты памяти в override.conf для wireproxy
    @flask_app.route('/other/servers/warp/config/<name>', methods=['POST'])
    @login_required
    def warp_config(name):
        try:
            server, error = get_ssh_server(name, 'ssh')
            if error: return error
            creds, error = get_ssh_credentials(server)
            if error: return error
            host, port, username, password = creds
            
            memory_max, memory_high = request.form.get('memory_max', '800M'), request.form.get('memory_high', '1G')
            override_dir, override_file = '/etc/systemd/system/wireproxy.service.d', '/etc/systemd/system/wireproxy.service.d/override.conf'
            
            check_result = execute_ssh_command(host, port, username, password, f"test -f {override_file} && echo 'EXISTS' || echo 'NOT_EXISTS'", timeout=10)
            
            if check_result['ok'] and 'EXISTS' in check_result['output']:
                cmd = (f"mkdir -p {override_dir} && "
                       f"if grep -q '^MemoryMax=' {override_file}; then sed -i 's/^MemoryMax=.*/MemoryMax={memory_max}/' {override_file}; else sed -i '/^\\[Service\\]/a MemoryMax={memory_max}' {override_file}; fi && "
                       f"if grep -q '^MemoryHigh=' {override_file}; then sed -i 's/^MemoryHigh=.*/MemoryHigh={memory_high}/' {override_file}; else sed -i '/^\\[Service\\]/a MemoryHigh={memory_high}' {override_file}; fi && "
                       "systemctl daemon-reload && systemctl restart wireproxy")
            else:
                content = f"[Service]\nMemoryMax={memory_max}\nMemoryHigh={memory_high}\n"
                safe_content = content.replace("'", "'\"'\"'")
                cmd = (f"mkdir -p {override_dir} && printf '%s' '{safe_content}' > {override_file} && "
                       "systemctl daemon-reload && systemctl restart wireproxy")
            
            logger.info(f"Настройка WARP на {name}: {memory_max}/{memory_high}")
            result = execute_ssh_command(host, port, username, password, cmd, timeout=60)
            
            if result['ok']: return jsonify({'ok': True, 'message': 'Конфигурация обновлена и сервис перезапущен', 'output': result['output']})
            return jsonify({'ok': False, 'error': result['error'] or 'Ошибка конфигурации', 'output': result['output']}), 500
        except Exception as e:
             logger.error(f"Ошибка настройки WARP на {name}: {e}")
             return jsonify({'ok': False, 'error': str(e)}), 500
    # ===== Конец роута warp_config =====

    # ===== ПЕРЕЗАПУСК WARP =====
    # Перезапускает системный сервис wireproxy
    @flask_app.route('/other/servers/warp/restart/<name>', methods=['POST'])
    @login_required
    def warp_restart(name):
        try:
             ssh_targets = rw_repo.get_all_ssh_targets()
             server = next((t for t in ssh_targets if t.get('target_name') == name), None)
             if not server: return jsonify({'ok': False, 'error': 'SSH цель не найдена'}), 404
             
             host, port, username, password = server.get('ssh_host'), server.get('ssh_port', 22), server.get('ssh_username', 'root'), server.get('ssh_password')
             if not host or not password: return jsonify({'ok': False, 'error': 'SSH данные не настроены'}), 400
             
             result = execute_ssh_command(host, port, username, password, "systemctl restart wireproxy", timeout=30)
             if result['ok']: return jsonify({'ok': True, 'message': 'Сервис wireproxy перезапущен'})
             return jsonify({'ok': False, 'error': result['error']}), 500
        except Exception as e:
             logger.error(f"Ошибка перезапуска WARP на {name}: {e}")
             return jsonify({'ok': False, 'error': str(e)}), 500
    # ===== Конец роута warp_restart =====

    # ===== ЗАПУСК WARP =====
    # Запускает системный сервис wireproxy
    @flask_app.route('/other/servers/warp/start/<name>', methods=['POST'])
    @login_required
    def warp_start(name):
        try:
            server, error = get_ssh_server(name, 'ssh')
            if error: return error
            creds, error = get_ssh_credentials(server)
            if error: return error
            host, port, username, password = creds
            
            result = execute_ssh_command(host, port, username, password, "systemctl start wireproxy", timeout=30)
            if result['ok']: return jsonify({'ok': True, 'message': 'Сервис запущен'})
            return jsonify({'ok': False, 'error': result['error'] or 'Ошибка запуска'}), 500
        except Exception as e:
            logger.error(f"Ошибка запуска WARP на {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    # ===== Конец роута warp_start =====

    # ===== ОСТАНОВКА WARP =====
    # Останавливает системный сервис wireproxy
    @flask_app.route('/other/servers/warp/stop/<name>', methods=['POST'])
    @login_required
    def warp_stop(name):
        try:
            server, error = get_ssh_server(name, 'ssh')
            if error: return error
            creds, error = get_ssh_credentials(server)
            if error: return error
            host, port, username, password = creds
            
            result = execute_ssh_command(host, port, username, password, "systemctl stop wireproxy", timeout=30)
            if result['ok']: return jsonify({'ok': True, 'message': 'Сервис остановлен'})
            return jsonify({'ok': False, 'error': result['error'] or 'Ошибка остановки'}), 500
        except Exception as e:
            logger.error(f"Ошибка остановки WARP на {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    # ===== Конец роута warp_stop =====

    # ===== УСТАНОВКА SWAP =====
    # Создает и подключает SWAP-файл заданного размера на сервере
    @flask_app.route('/other/servers/swap/install/<name>', methods=['POST'])
    @login_required
    def swap_install(name):
        try:
            size_mb = request.form.get('size_mb', '2048')
            if not size_mb.isdigit(): return jsonify({'ok': False, 'error': 'Некорректный размер'}), 400
            
            server, error = get_ssh_server(name, 'ssh')
            if error: return error
            creds, error = get_ssh_credentials(server)
            if error: return error
            host, port, username, password = creds

            cmd = (f"fallocate -l {size_mb}M /swapfile || dd if=/dev/zero of=/swapfile bs=1M count={size_mb}; "
                   "chmod 600 /swapfile; mkswap /swapfile; swapon /swapfile; "
                   "grep -q '/swapfile' /etc/fstab || echo '/swapfile none swap sw 0 0' >> /etc/fstab")
            
            logger.info(f"Установка SWAP ({size_mb}MB) на {name}")
            result = execute_ssh_command(host, port, username, password, cmd, timeout=120)
            
            if result['ok']: return jsonify({'ok': True, 'message': 'SWAP установлен'})
            return jsonify({'ok': False, 'error': result['error'] or 'Не удалось установить SWAP'}), 500
        except Exception as e:
            logger.error(f"Ошибка установки SWAP на {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    # ===== Конец роута swap_install =====

    # ===== УДАЛЕНИЕ SWAP =====
    # Отключает и удаляет SWAP-файл с сервера
    @flask_app.route('/other/servers/swap/delete/<name>', methods=['DELETE'])
    @login_required
    def swap_delete(name):
        try:
            server, error = get_ssh_server(name, 'ssh')
            if error: return error
            creds, error = get_ssh_credentials(server)
            if error: return error
            host, port, username, password = creds

            cmd = "swapoff /swapfile; rm /swapfile; sed -i '/\\/swapfile/d' /etc/fstab"
            logger.info(f"Удаление SWAP на {name}")
            result = execute_ssh_command(host, port, username, password, cmd, timeout=60)
            
            if result['ok']: return jsonify({'ok': True, 'message': 'SWAP удален'})
            return jsonify({'ok': False, 'error': result['error'] or 'Не удалось удалить SWAP'}), 500
        except Exception as e:
            logger.error(f"Ошибка удаления SWAP на {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    # ===== Конец роута swap_delete =====
            
    # ===== ИЗМЕНЕНИЕ РАЗМЕРА SWAP =====
    # Пересоздает SWAP-файл с новым указанным размером
    @flask_app.route('/other/servers/swap/resize/<name>', methods=['POST'])
    @login_required
    def swap_resize(name):
        try:
            size_mb = request.form.get('size_mb', '2048')
            if not size_mb.isdigit(): return jsonify({'ok': False, 'error': 'Некорректный размер'}), 400
                 
            server, error = get_ssh_server(name, 'ssh')
            if error: return error
            creds, error = get_ssh_credentials(server)
            if error: return error
            host, port, username, password = creds

            cmd = (f"if grep -q '/swapfile' /proc/swaps; then swapoff /swapfile || exit 1; fi && rm -f /swapfile && "
                   f"fallocate -l {size_mb}M /swapfile || dd if=/dev/zero of=/swapfile bs=1M count={size_mb} && "
                   "chmod 600 /swapfile && mkswap /swapfile && swapon /swapfile && "
                   "grep -q '/swapfile' /etc/fstab || echo '/swapfile none swap sw 0 0' >> /etc/fstab")
            
            logger.info(f"Изменение размера SWAP до {size_mb}MB на {name}")
            result = execute_ssh_command(host, port, username, password, cmd, timeout=180)
            
            if result['ok']: return jsonify({'ok': True, 'message': 'Размер SWAP изменен'})
            return jsonify({'ok': False, 'error': result['error'] or 'Не удалось изменить размер SWAP'}), 500
        except Exception as e:
            logger.error(f"Ошибка изменения размера SWAP на {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    # ===== Конец роута swap_resize =====

    # ===== ИЗМЕНЕНИЕ SWAPPINESS =====
    # Обновляет параметр vm.swappiness в системе и в файле sysctl.conf
    @flask_app.route('/other/servers/swap/swappiness/<name>', methods=['POST'])
    @login_required
    def swap_swappiness(name):
        try:
            swappiness = request.form.get('swappiness', '60')
            if not swappiness.isdigit() or not (0 <= int(swappiness) <= 100): return jsonify({'ok': False, 'error': 'Некорректное значение (0-100)'}), 400
            
            server, error = get_ssh_server(name, 'ssh')
            if error: return error
            creds, error = get_ssh_credentials(server)
            if error: return error
            host, port, username, password = creds

            cmd = (f"sysctl vm.swappiness={swappiness}; "
                   f"if grep -q 'vm.swappiness' /etc/sysctl.conf; then sed -i 's/^vm.swappiness.*/vm.swappiness={swappiness}/' /etc/sysctl.conf; "
                   f"else echo 'vm.swappiness={swappiness}' >> /etc/sysctl.conf; fi")
            
            logger.info(f"Изменение swappiness на {swappiness} на {name}")
            result = execute_ssh_command(host, port, username, password, cmd, timeout=30)
            
            if result['ok']: return jsonify({'ok': True, 'message': 'Параметр swappiness обновлен'})
            return jsonify({'ok': False, 'error': result['error'] or 'Не удалось обновить swappiness'}), 500
        except Exception as e:
            logger.error(f"Ошибка изменения swappiness на {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    # ===== Конец роута swap_swappiness =====

    # ===== ПОЛУЧЕНИЕ JSON КОНФИГА SYSTEMD (WARP) =====
    # Читает содержимое файла override.conf для wireproxy
    @flask_app.route('/other/servers/warp/systemd/get/<name>', methods=['GET'])
    @login_required
    def warp_systemd_get(name):
        try:
            server, error = get_ssh_server(name, 'ssh')
            if error: return error
            creds, error = get_ssh_credentials(server)
            if error: return error
            host, port, username, password = creds
            
            result = execute_ssh_command(host, port, username, password, "if [ -f /etc/systemd/system/wireproxy.service.d/override.conf ]; then cat /etc/systemd/system/wireproxy.service.d/override.conf; else echo ''; fi", timeout=15)
            if result['ok']: return jsonify({'ok': True, 'content': result['output']})
            return jsonify({'ok': False, 'error': result['error'] or 'Не удалось прочитать конфиг'}), 500
        except Exception as e:
            logger.error(f"Ошибка чтения системного конфига на {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    # ===== Конец роута warp_systemd_get =====

    # ===== СОХРАНЕНИЕ JSON КОНФИГА SYSTEMD (WARP) =====
    # Перезаписывает override.conf и перезагружает сервис
    @flask_app.route('/other/servers/warp/systemd/save/<name>', methods=['POST'])
    @login_required
    def warp_systemd_save(name):
        try:
            content = request.form.get('content', '')
            server, error = get_ssh_server(name, 'ssh')
            if error: return error
            creds, error = get_ssh_credentials(server)
            if error: return error
            host, port, username, password = creds
            
            override_dir, override_file = '/etc/systemd/system/wireproxy.service.d', '/etc/systemd/system/wireproxy.service.d/override.conf'
            safe_content = content.replace("'", "'\"'\"'")
            cmd = (f"mkdir -p {override_dir} && printf '%s' '{safe_content}' > {override_file} && "
                   "systemctl daemon-reload && systemctl restart wireproxy")
            
            logger.info(f"Сохранение системного конфига на {name}")
            result = execute_ssh_command(host, port, username, password, cmd, timeout=60)
            
            if result['ok']: return jsonify({'ok': True, 'message': 'Конфигурация сохранена и сервис перезапущен'})
            return jsonify({'ok': False, 'error': result['error'] or 'Не удалось сохранить конфиг'}), 500
        except Exception as e:
            logger.error(f"Ошибка сохранения системного конфига на {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    # ===== Конец роута warp_systemd_save =====

    # ===== ИСПОЛЬЗОВАНИЕ ДИСКА ЛОГАМИ =====
    # Показывает объем дискового пространства, занятого системными логами
    @flask_app.route('/other/servers/warp/logs/usage/<name>', methods=['GET'])
    @login_required
    def warp_logs_usage(name):
        try:
            server, error = get_ssh_server(name, 'ssh')
            if error: return error
            creds, error = get_ssh_credentials(server)
            if error: return error
            host, port, username, password = creds
            
            result = execute_ssh_command(host, port, username, password, "journalctl --disk-usage", timeout=15)
            if result['ok']: return jsonify({'ok': True, 'usage': result['output']})
            return jsonify({'ok': False, 'error': result['error']}), 500
        except Exception as e:
            logger.error(f"Ошибка проверки использования логов на {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    # ===== Конец роута warp_logs_usage =====

    # ===== ОЧИСТКА СИСТЕМНЫХ ЛОГОВ (VACUUM) =====
    # Выполняет очистку логов journalctl по размеру или времени
    @flask_app.route('/other/servers/warp/logs/clean/<name>', methods=['POST'])
    @login_required
    def warp_logs_clean(name):
        try:
            max_size, max_age = request.form.get('max_size', '0'), request.form.get('max_age', '0')
            if not max_size.isdigit() or not max_age.isdigit(): return jsonify({'ok': False, 'error': 'Некорректные значения'}), 400
            
            s_int, a_int = int(max_size), int(max_age)
            if s_int == 0 and a_int == 0: return jsonify({'ok': False, 'error': 'Укажите размер или возраст'}), 400
            
            server, error = get_ssh_server(name, 'ssh')
            if error: return error
            creds, error = get_ssh_credentials(server)
            if error: return error
            host, port, username, password = creds
            
            cmd_parts = ['sudo journalctl -u wireproxy.service']
            if s_int > 0: cmd_parts.append(f'--vacuum-size={s_int}M')
            if a_int > 0: cmd_parts.append(f'--vacuum-time={a_int}d')
            
            logger.info(f"Очистка логов wireproxy на {name}: {' '.join(cmd_parts)}")
            result = execute_ssh_command(host, port, username, password, ' '.join(cmd_parts), timeout=60)
            
            if result['ok']: return jsonify({'ok': True, 'message': 'Логи wireproxy очищены', 'output': result['output']})
            return jsonify({'ok': False, 'error': result['error'] or 'Ошибка очистки логов', 'output': result['output']}), 500
        except Exception as e:
            logger.error(f"Ошибка очистки логов на {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    # ===== Конец роута warp_logs_clean =====
    
    # ===== ИНТЕРАКТИВНОЕ ВЫПОЛНЕНИЕ КОМАНД (SSH SHELL) =====
    # Поддерживает постоянную сессию и потоковую передачу вывода через SSE
    @flask_app.route('/other/servers/execute/<server_type>/<name>', methods=['POST'])
    @login_required
    def server_execute_command(server_type, name):
        try:
            import paramiko, time, re
            from flask import Response, stream_with_context
            command = request.form.get('command', '')
            server, error = get_ssh_server(name, server_type)
            if error: return error
            creds, error = get_ssh_credentials(server)
            if error: return error
            host, port, username, password = creds
            
            session_key, ansi_escape = f"{server_type}:{name}", re.compile(r'\x1B\[[0-9;]*[a-zA-Z]|\x1B\(B|\x1B\[m|\x1B\]0;[^\x07]*\x07')
            
            def generate():
                global ssh_sessions
                client, channel = None, None
                try:
                    with ssh_sessions_lock:
                        session = ssh_sessions.get(session_key)
                        if session:
                            client, channel = session.get('client'), session.get('channel')
                            try:
                                transport = client.get_transport()
                                if not (transport and transport.is_active() and not channel.closed): client, channel = None, None
                            except: client, channel = None, None
                    
                    if not client or not channel:
                        client = paramiko.SSHClient()
                        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                        yield f"data: [INFO] Подключение к {host}:{port}...\n\n"
                        client.connect(hostname=host, port=port, username=username, password=password, timeout=30, look_for_keys=False, allow_agent=False)
                        yield f"data: [INFO] Соединение установлено. Запуск оболочки...\n\n"
                        channel = client.invoke_shell(term='xterm', width=200, height=50)
                        channel.settimeout(0.1)
                        with ssh_sessions_lock: ssh_sessions[session_key] = {'client': client, 'channel': channel, 'created': time.time()}
                        time.sleep(0.5)
                        while channel.recv_ready(): channel.recv(4096)
                    
                    channel.send(command + '\n')
                    start_time, idle_count, max_idle, timeout = time.time(), 0, 50, 30
                    
                    while True:
                        try:
                            if channel.recv_ready():
                                data = channel.recv(4096)
                                if data:
                                    idle_count = 0
                                    try:
                                        text = ansi_escape.sub('', data.decode('utf-8', errors='replace'))
                                        for line in text.splitlines():
                                            if line.rstrip(): yield f"data: {line.rstrip()}\n\n"
                                    except Exception as ex: logger.error(f"Ошибка декодирования вывода: {ex}")
                            else: idle_count += 1
                            
                            if channel.closed:
                                yield f"data: [INFO] Сессия закрыта\n\n"
                                with ssh_sessions_lock:
                                    if session_key in ssh_sessions: del ssh_sessions[session_key]
                                break
                            
                            if idle_count >= max_idle or (time.time() - start_time > timeout): break
                            time.sleep(0.1)
                        except Exception as loop_ex:
                            logger.error(f"Ошибка цикла SSH: {loop_ex}")
                            break
                except Exception as e:
                    logger.error(f"Ошибка выполнения команды на {server_type}/{name}: {e}")
                    yield f"data: [ERROR] {str(e)}\n\n"
                    with ssh_sessions_lock:
                        if session_key in ssh_sessions: del ssh_sessions[session_key]
                finally: yield "data: [DONE]\n\n"
            
            return Response(stream_with_context(generate()), mimetype='text/event-stream', headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no', 'Connection': 'keep-alive'})
        except Exception as e:
            logger.error(f"Ошибка в server_execute_command для {server_type}/{name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    # ===== Конец роута server_execute_command =====
    
    # ===== ЗАКРЫТИЕ SSH СЕССИИ =====
    # Принудительно завершает Paramiko-соединение для указанного сервера
    @flask_app.route('/other/servers/execute/close/<server_type>/<name>', methods=['POST'])
    @login_required
    def close_ssh_session(server_type, name):
        try:
            session_key = f"{server_type}:{name}"
            with ssh_sessions_lock:
                session = ssh_sessions.pop(session_key, None)
                if session:
                    try:
                        if session.get('channel'): session['channel'].close()
                        if session.get('client'): session['client'].close()
                    except: pass
                    return jsonify({'ok': True, 'message': 'Сессия закрыта'})
                return jsonify({'ok': True, 'message': 'Активная сессия не найдена'})
        except Exception as e:
            logger.error(f"Ошибка закрытия SSH сессии для {server_type}/{name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    # ===== Конец функции close_ssh_session =====

    # ===== СОХРАНЕНИЕ НАСТРОЕК ПЛАНИРОВЩИКА =====
    # Записывает параметры расписания (интервал, единицы, активность) в БД
    @flask_app.route('/other/servers/scheduler/save/<target_name>', methods=['POST'])
    @login_required
    def save_scheduler_config(target_name):
        try:
            value = request.form.get('value')
            unit = request.form.get('unit')
            enabled = request.form.get('enabled') == 'true'
            
            if not value or not value.isdigit():
                 return jsonify({'ok': False, 'error': 'Некорректное значение'}), 400
            
            value = int(value)
            if unit not in ['minutes', 'hours', 'days']:
                 return jsonify({'ok': False, 'error': 'Некорректная единица измерения'}), 400
            
            config = {
                'value': value,
                'unit': unit,
                'enabled': enabled,
                'last_run': None 
            }
            
            ssh_targets = rw_repo.get_all_ssh_targets()
            target = next((t for t in ssh_targets if t.get('target_name') == target_name), None)
            if not target:
                return jsonify({'ok': False, 'error': 'Цель не найдена'}), 404
            
            json_config = json.dumps(config)
            rw_repo.update_ssh_target_scheduler(target_name, json_config)
            
            return jsonify({'ok': True})
            
        except Exception as e:
            logger.error(f"Ошибка сохранения настроек планировщика для {target_name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500