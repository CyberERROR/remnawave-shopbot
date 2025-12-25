import os
import json
import asyncio
import logging
import uuid
from datetime import datetime
from flask import render_template, request, jsonify, current_app
from werkzeug.utils import secure_filename
from aiogram.types import FSInputFile
from shop_bot.data_manager import remnawave_repository as rw_repo

logger = logging.getLogger(__name__)

ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif', 'mp4', 'webm'}
UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), 'templates', 'partials')
RESULTS_FILE = os.path.join(UPLOAD_FOLDER, 'total.json')

os.makedirs(UPLOAD_FOLDER, exist_ok=True)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def get_media_type(filename):
    ext = filename.rsplit('.', 1)[1].lower()
    if ext in {'png', 'jpg', 'jpeg'}:
        return 'photo'
    elif ext == 'gif':
        return 'animation'
    elif ext in {'mp4', 'webm'}:
        return 'video'
    return None

def save_broadcast_results(sent, failed, skipped):
    try:
        from datetime import timezone, timedelta
        moscow_tz = timezone(timedelta(hours=3))
        moscow_time = datetime.now(moscow_tz)
        
        results = {
            'sent': sent,
            'failed': failed,
            'skipped': skipped,
            'timestamp': moscow_time.isoformat()
        }
        with open(RESULTS_FILE, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Failed to save broadcast results: {e}")

def load_broadcast_results():
    try:
        if os.path.exists(RESULTS_FILE):
            with open(RESULTS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load broadcast results: {e}")
    return {'sent': 0, 'failed': 0, 'skipped': 0, 'timestamp': None}

async def send_broadcast_async(bot, users, text, media_path=None, media_type=None, buttons=None, mode='all'):
    sent = 0
    failed = 0
    skipped = 0
    
    for user in users:
        user_id = user.get('telegram_id')
        if not user_id:
            continue
            
        is_banned = user.get('is_banned', False)
        if is_banned:
            skipped += 1
            continue
        
        try:
            keyboard = None
            if buttons:
                from aiogram.utils.keyboard import InlineKeyboardBuilder
                builder = InlineKeyboardBuilder()
                for btn in buttons:
                    btn_text = btn.get('text', '').strip()
                    btn_url = btn.get('url', '').strip()
                    if btn_text and btn_url and (btn_url.startswith('http://') or btn_url.startswith('https://')):
                        builder.button(text=btn_text, url=btn_url)
                builder.adjust(1)
                keyboard = builder.as_markup() if builder.export() else None
            
            if media_path and media_type:
                media_file = FSInputFile(media_path)
                if media_type == 'photo':
                    await bot.send_photo(
                        chat_id=user_id,
                        photo=media_file,
                        caption=text,
                        parse_mode='HTML',
                        reply_markup=keyboard
                    )
                elif media_type == 'video':
                    await bot.send_video(
                        chat_id=user_id,
                        video=media_file,
                        caption=text,
                        parse_mode='HTML',
                        reply_markup=keyboard
                    )
                elif media_type == 'animation':
                    await bot.send_animation(
                        chat_id=user_id,
                        animation=media_file,
                        caption=text,
                        parse_mode='HTML',
                        reply_markup=keyboard
                    )
            else:
                await bot.send_message(
                    chat_id=user_id,
                    text=text,
                    parse_mode='HTML',
                    reply_markup=keyboard
                )
            
            sent += 1
            await asyncio.sleep(0.05)
            
        except Exception as e:
            logger.warning(f"Failed to send broadcast to {user_id}: {e}")
            failed += 1
    
    save_broadcast_results(sent, failed, skipped)
    
    if media_path and os.path.exists(media_path):
        try:
            os.remove(media_path)
            logger.info(f"Removed media file: {media_path}")
        except Exception as e:
            logger.error(f"Failed to remove media file {media_path}: {e}")
    
    return {'sent': sent, 'failed': failed, 'skipped': skipped}

def register_other_routes(flask_app, login_required, get_common_template_data):
    @flask_app.route('/other')
    @login_required
    def other_page():
        common_data = get_common_template_data()
        return render_template('other.html', **common_data)
    
    @flask_app.route('/other/broadcast/stats')
    @login_required
    def broadcast_stats():
        try:
            from datetime import datetime
            
            all_users = rw_repo.get_all_users() or []
            total_users = len(all_users)
            
            users_with_active_keys = 0
            users_with_expired_keys = 0
            users_without_trial = 0
            
            for user in all_users:
                user_id = user.get('telegram_id')
                keys = rw_repo.get_keys_for_user(user_id) or []
                
                has_active_key = False
                has_expired_key = False
                for key in keys:
                    expire_at = key.get('expire_at')
                    if expire_at:
                        try:
                            expire_dt = datetime.fromisoformat(expire_at.replace('Z', '+00:00'))
                            now = datetime.now(expire_dt.tzinfo or None)
                            if expire_dt > now:
                                has_active_key = True
                            elif expire_dt <= now:
                                has_expired_key = True
                        except:
                            pass
                
                if has_active_key:
                    users_with_active_keys += 1
                if has_expired_key:
                    users_with_expired_keys += 1
                
                trial_used = user.get('trial_used', 0)
                if not trial_used:
                    users_without_trial += 1
            
            last_results = load_broadcast_results()
            
            return jsonify({
                'ok': True,
                'total_users': total_users,
                'users_with_keys': users_with_active_keys,
                'users_with_expired_keys': users_with_expired_keys,
                'users_without_trial': users_without_trial,
                'last_results': last_results
            })
        except Exception as e:
            logger.error(f"Error getting broadcast stats: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    
    @flask_app.route('/other/broadcast/preview', methods=['POST'])
    @login_required
    def broadcast_preview():
        try:
            text = request.form.get('text', '')
            buttons_json = request.form.get('buttons', '[]')
            buttons = json.loads(buttons_json) if buttons_json else []
            
            admin_id = rw_repo.get_setting('admin_telegram_id')
            if not admin_id:
                return jsonify({'ok': False, 'error': 'Admin ID not configured'}), 400
            
            from shop_bot.webhook_server.app import _bot_controller
            bot = _bot_controller.get_bot_instance() if _bot_controller else None
            if not bot:
                return jsonify({'ok': False, 'error': 'Bot not available'}), 500
            
            keyboard = None
            if buttons:
                from aiogram.utils.keyboard import InlineKeyboardBuilder
                builder = InlineKeyboardBuilder()
                for btn in buttons:
                    btn_text = btn.get('text', '').strip()
                    btn_url = btn.get('url', '').strip()
                    if btn_text and btn_url and (btn_url.startswith('http://') or btn_url.startswith('https://')):
                        builder.button(text=btn_text, url=btn_url)
                builder.adjust(1)
                keyboard = builder.as_markup() if builder.export() else None
            
            loop = current_app.config.get('EVENT_LOOP')
            if not loop or not loop.is_running():
                return jsonify({'ok': False, 'error': 'Event loop not available'}), 500
            
            async def send_preview():
                await bot.send_message(
                    chat_id=int(admin_id),
                    text=f"📨 <b>Предпросмотр рассылки</b>\n\n{text}",
                    parse_mode='HTML',
                    reply_markup=keyboard
                )
            
            asyncio.run_coroutine_threadsafe(send_preview(), loop).result(timeout=10)
            
            return jsonify({'ok': True, 'message': 'Preview sent to admin'})
        except Exception as e:
            logger.error(f"Error sending preview: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    
    @flask_app.route('/other/broadcast/upload', methods=['POST'])
    @login_required
    def broadcast_upload():
        try:
            if 'file' not in request.files:
                return jsonify({'ok': False, 'error': 'No file provided'}), 400
            
            file = request.files['file']
            if file.filename == '':
                return jsonify({'ok': False, 'error': 'No file selected'}), 400
            
            if not allowed_file(file.filename):
                return jsonify({'ok': False, 'error': 'Invalid file type'}), 400
            
            filename = secure_filename(file.filename)
            unique_filename = f"{uuid.uuid4().hex}_{filename}"
            filepath = os.path.join(UPLOAD_FOLDER, unique_filename)
            
            file.save(filepath)
            
            media_type = get_media_type(filename)
            
            return jsonify({
                'ok': True,
                'filename': unique_filename,
                'media_type': media_type,
                'path': filepath
            })
        except Exception as e:
            logger.error(f"Error uploading media: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    
    @flask_app.route('/other/broadcast/send', methods=['POST'])
    @login_required
    def broadcast_send():
        try:
            text = request.form.get('text', '')
            mode = request.form.get('mode', 'all')
            buttons_json = request.form.get('buttons', '[]')
            media_filename = request.form.get('media_filename', '')
            
            buttons = json.loads(buttons_json) if buttons_json else []
            
            if not text:
                return jsonify({'ok': False, 'error': 'Text is required'}), 400
            
            from shop_bot.webhook_server.app import _bot_controller
            bot = _bot_controller.get_bot_instance() if _bot_controller else None
            if not bot:
                return jsonify({'ok': False, 'error': 'Bot not available'}), 500
            
            all_users = rw_repo.get_all_users() or []
            
            if mode == 'test':
                admin_id = rw_repo.get_setting('admin_telegram_id')
                if admin_id:
                    all_users = [{'telegram_id': int(admin_id), 'is_banned': False}]
                else:
                    return jsonify({'ok': False, 'error': 'Admin ID not configured'}), 400
            elif mode == 'with_keys':
                from datetime import datetime
                filtered_users = []
                for user in all_users:
                    user_id = user.get('telegram_id')
                    keys = rw_repo.get_keys_for_user(user_id) or []
                    has_active_key = False
                    for key in keys:
                        expire_at = key.get('expire_at')
                        if expire_at:
                            try:
                                expire_dt = datetime.fromisoformat(expire_at.replace('Z', '+00:00'))
                                if expire_dt > datetime.now(expire_dt.tzinfo or None):
                                    has_active_key = True
                                    break
                            except:
                                pass
                    if has_active_key:
                        filtered_users.append(user)
                all_users = filtered_users
            elif mode == 'expired_keys':
                from datetime import datetime
                filtered_users = []
                for user in all_users:
                    user_id = user.get('telegram_id')
                    keys = rw_repo.get_keys_for_user(user_id) or []
                    has_active_key = False
                    has_expired_key = False
                    for key in keys:
                        expire_at = key.get('expire_at')
                        if expire_at:
                            try:
                                expire_dt = datetime.fromisoformat(expire_at.replace('Z', '+00:00'))
                                now = datetime.now(expire_dt.tzinfo or None)
                                if expire_dt > now:
                                    has_active_key = True
                                    break
                                elif expire_dt <= now:
                                    has_expired_key = True
                            except:
                                pass
                    if not has_active_key and has_expired_key:
                        filtered_users.append(user)
                all_users = filtered_users
            elif mode == 'without_trial':
                all_users = [u for u in all_users if not u.get('trial_used', 0)]
            
            media_path = None
            media_type = None
            if media_filename:
                media_path = os.path.join(UPLOAD_FOLDER, media_filename)
                if os.path.exists(media_path):
                    media_type = get_media_type(media_filename)
            
            loop = current_app.config.get('EVENT_LOOP')
            if not loop or not loop.is_running():
                return jsonify({'ok': False, 'error': 'Event loop not available'}), 500
            
            future = asyncio.run_coroutine_threadsafe(
                send_broadcast_async(bot, all_users, text, media_path, media_type, buttons, mode),
                loop
            )
            
            results = future.result(timeout=600)
            
            return jsonify({
                'ok': True,
                'results': results
            })
        except Exception as e:
            logger.error(f"Error sending broadcast: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    
    @flask_app.route('/other/broadcast/delete-media/<filename>', methods=['DELETE'])
    @login_required
    def broadcast_delete_media(filename):
        try:
            filepath = os.path.join(UPLOAD_FOLDER, secure_filename(filename))
            if os.path.exists(filepath):
                os.remove(filepath)
                return jsonify({'ok': True})
            return jsonify({'ok': False, 'error': 'File not found'}), 404
        except Exception as e:
            logger.error(f"Error deleting media: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    
    # ==================== Промокоды ====================
    
    @flask_app.route('/other/promo/list')
    @login_required
    def promo_list():
        try:
            promos = rw_repo.list_promo_codes(include_inactive=True)
            return jsonify({'ok': True, 'promos': promos})
        except Exception as e:
            logger.error(f"Error getting promo codes: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    
    @flask_app.route('/other/promo/create', methods=['POST'])
    @login_required
    def promo_create():
        try:
            import string
            import random
            from datetime import datetime
            
            code = request.form.get('code', '').strip().upper()
            discount_type = request.form.get('discount_type', 'percent')
            discount_value = request.form.get('discount_value')
            usage_limit_total = request.form.get('usage_limit_total')
            usage_limit_per_user = request.form.get('usage_limit_per_user')
            valid_from = request.form.get('valid_from')
            valid_until = request.form.get('valid_until')
            description = request.form.get('description', '')
            
            # Генерация кода если не указан
            if not code:
                chars = string.ascii_uppercase + string.digits
                code = ''.join(random.choice(chars) for _ in range(8))
            
            # Валидация
            if not discount_value:
                return jsonify({'ok': False, 'error': 'Discount value is required'}), 400
            
            try:
                discount_value = float(discount_value)
            except ValueError:
                return jsonify({'ok': False, 'error': 'Invalid discount value'}), 400
            
            if discount_value <= 0:
                return jsonify({'ok': False, 'error': 'Discount must be positive'}), 400
            
            # Подготовка параметров
            discount_percent = discount_value if discount_type == 'percent' else None
            discount_amount = discount_value if discount_type == 'fixed' else None
            
            usage_limit_total_int = int(usage_limit_total) if usage_limit_total else None
            usage_limit_per_user_int = int(usage_limit_per_user) if usage_limit_per_user else None
            
            valid_from_dt = datetime.fromisoformat(valid_from) if valid_from else None
            valid_until_dt = datetime.fromisoformat(valid_until) if valid_until else None
            
            # Получаем ID админа
            admin_id = rw_repo.get_setting('admin_telegram_id')
            created_by = int(admin_id) if admin_id else None
            
            # Создаем промокод
            success = rw_repo.create_promo_code(
                code=code,
                discount_percent=discount_percent,
                discount_amount=discount_amount,
                usage_limit_total=usage_limit_total_int,
                usage_limit_per_user=usage_limit_per_user_int,
                valid_from=valid_from_dt,
                valid_until=valid_until_dt,
                created_by=created_by,
                description=description
            )
            
            if success:
                return jsonify({'ok': True, 'code': code, 'message': 'Promo code created'})
            else:
                return jsonify({'ok': False, 'error': 'Code already exists'}), 400
        except ValueError as e:
            return jsonify({'ok': False, 'error': str(e)}), 400
        except Exception as e:
            logger.error(f"Error creating promo code: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    
    @flask_app.route('/other/promo/toggle/<code>', methods=['POST'])
    @login_required
    def promo_toggle(code):
        try:
            promo = rw_repo.get_promo_code(code)
            if not promo:
                return jsonify({'ok': False, 'error': 'Promo code not found'}), 404
            
            new_status = not promo.get('is_active', 1)
            success = rw_repo.update_promo_code_status(code, is_active=new_status)
            
            if success:
                return jsonify({'ok': True, 'is_active': new_status})
            return jsonify({'ok': False, 'error': 'Failed to update status'}), 500
        except Exception as e:
            logger.error(f"Error toggling promo code: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    
    @flask_app.route('/other/promo/delete/<code>', methods=['DELETE'])
    @login_required
    def promo_delete(code):
        try:
            success = rw_repo.delete_promo_code(code)
            if success:
                return jsonify({'ok': True, 'message': 'Promo code deleted'})
            return jsonify({'ok': False, 'error': 'Promo code not found'}), 404
        except Exception as e:
            logger.error(f"Error deleting promo code: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    
    @flask_app.route('/other/promo/update/<code>', methods=['POST'])
    @login_required
    def promo_update(code):
        try:
            from datetime import datetime
            
            # Проверяем существование
            existing = rw_repo.get_promo_code(code)
            if not existing:
                return jsonify({'ok': False, 'error': 'Promo code not found'}), 404
            
            # Удаляем старый и создаем новый с теми же параметрами
            discount_type = request.form.get('discount_type', 'percent')
            discount_value = request.form.get('discount_value')
            usage_limit_total = request.form.get('usage_limit_total')
            usage_limit_per_user = request.form.get('usage_limit_per_user')
            valid_from = request.form.get('valid_from')
            valid_until = request.form.get('valid_until')
            description = request.form.get('description', '')
            
            if not discount_value:
                return jsonify({'ok': False, 'error': 'Discount value is required'}), 400
            
            try:
                discount_value = float(discount_value)
            except ValueError:
                return jsonify({'ok': False, 'error': 'Invalid discount value'}), 400
            
            # Удаляем старый
            rw_repo.delete_promo_code(code)
            
            # Создаем новый
            discount_percent = discount_value if discount_type == 'percent' else None
            discount_amount = discount_value if discount_type == 'fixed' else None
            
            usage_limit_total_int = int(usage_limit_total) if usage_limit_total else None
            usage_limit_per_user_int = int(usage_limit_per_user) if usage_limit_per_user else None
            
            valid_from_dt = datetime.fromisoformat(valid_from) if valid_from else None
            valid_until_dt = datetime.fromisoformat(valid_until) if valid_until else None
            
            admin_id = rw_repo.get_setting('admin_telegram_id')
            created_by = int(admin_id) if admin_id else None
            
            success = rw_repo.create_promo_code(
                code=code,
                discount_percent=discount_percent,
                discount_amount=discount_amount,
                usage_limit_total=usage_limit_total_int,
                usage_limit_per_user=usage_limit_per_user_int,
                valid_from=valid_from_dt,
                valid_until=valid_until_dt,
                created_by=created_by,
                description=description
            )
            
            if success:
                return jsonify({'ok': True, 'message': 'Promo code updated'})
            return jsonify({'ok': False, 'error': 'Failed to update'}), 500
        except Exception as e:
            logger.error(f"Error updating promo code: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    
    # ==================== Сервера ====================
    
    def execute_ssh_command(host, port, username, password, command, timeout=10):
        """Выполнение SSH команды на удаленном сервере"""
        try:
            import paramiko
            
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            client.connect(
                hostname=host,
                port=port,
                username=username,
                password=password,
                timeout=timeout,
                look_for_keys=False,
                allow_agent=False
            )
            
            stdin, stdout, stderr = client.exec_command(command, timeout=timeout)
            output = stdout.read().decode('utf-8').strip()
            error = stderr.read().decode('utf-8').strip()
            exit_status = stdout.channel.recv_exit_status()
            
            client.close()
            
            return {
                'ok': exit_status == 0,
                'output': output,
                'error': error,
                'exit_status': exit_status
            }
        except Exception as e:
            logger.error(f"SSH command failed for {host}:{port} - {e}")
            return {
                'ok': False,
                'output': '',
                'error': str(e),
                'exit_status': -1
            }
    
    @flask_app.route('/other/servers/list')
    @login_required
    def servers_list():
        try:
            hosts = rw_repo.list_squads(active_only=False)
            ssh_targets = rw_repo.get_all_ssh_targets()
            
            return jsonify({
                'ok': True,
                'hosts': hosts,
                'ssh_targets': ssh_targets
            })
        except Exception as e:
            logger.error(f"Error getting servers list: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    
    @flask_app.route('/other/servers/uptime/<server_type>/<name>')
    @login_required
    def server_uptime(server_type, name):
        try:
            if server_type == 'host':
                # Получаем хост из БД
                hosts = rw_repo.list_squads(active_only=False)
                server = next((h for h in hosts if h.get('host_name') == name), None)
                if not server:
                    return jsonify({'ok': False, 'error': 'Host not found'}), 404
                
                host = server.get('ssh_host')
                port = server.get('ssh_port', 22)
                username = server.get('ssh_user', 'root')
                password = server.get('ssh_password')
            elif server_type == 'ssh':
                # Получаем SSH-цель из БД
                ssh_targets = rw_repo.get_all_ssh_targets()
                server = next((t for t in ssh_targets if t.get('target_name') == name), None)
                if not server:
                    return jsonify({'ok': False, 'error': 'SSH target not found'}), 404
                
                host = server.get('ssh_host')
                port = server.get('ssh_port', 22)
                username = server.get('ssh_username', 'root')
                password = server.get('ssh_password')
            else:
                return jsonify({'ok': False, 'error': 'Invalid server type'}), 400
            
            if not host or not password:
                return jsonify({'ok': False, 'error': 'SSH credentials not configured'}), 400
            
            # Получаем uptime
            result = execute_ssh_command(host, port, username, password, 'cat /proc/uptime')
            
            if result['ok']:
                uptime_output = result['output']
                try:
                    uptime_seconds = float(uptime_output.split()[0])
                    return jsonify({
                        'ok': True,
                        'uptime_seconds': uptime_seconds,
                        'uptime_formatted': format_uptime(uptime_seconds)
                    })
                except:
                    return jsonify({'ok': False, 'error': 'Failed to parse uptime'}), 500
            else:
                return jsonify({'ok': False, 'error': result['error']}), 500
        except Exception as e:
            logger.error(f"Error getting uptime for {server_type}/{name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    
    def format_uptime(seconds):
        """Форматирование uptime в человекочитаемый вид"""
        days = int(seconds // 86400)
        hours = int((seconds % 86400) // 3600)
        minutes = int((seconds % 3600) // 60)
        
        parts = []
        if days > 0:
            parts.append(f"{days}д")
        if hours > 0:
            parts.append(f"{hours}ч")
        if minutes > 0 or len(parts) == 0:
            parts.append(f"{minutes}м")
        
        return ' '.join(parts)
    
    @flask_app.route('/other/servers/reboot/<server_type>/<name>', methods=['POST'])
    @login_required
    def server_reboot(server_type, name):
        try:
            if server_type == 'host':
                hosts = rw_repo.list_squads(active_only=False)
                server = next((h for h in hosts if h.get('host_name') == name), None)
                if not server:
                    return jsonify({'ok': False, 'error': 'Host not found'}), 404
                
                host = server.get('ssh_host')
                port = server.get('ssh_port', 22)
                username = server.get('ssh_user', 'root')
                password = server.get('ssh_password')
            elif server_type == 'ssh':
                ssh_targets = rw_repo.get_all_ssh_targets()
                server = next((t for t in ssh_targets if t.get('target_name') == name), None)
                if not server:
                    return jsonify({'ok': False, 'error': 'SSH target not found'}), 404
                
                host = server.get('ssh_host')
                port = server.get('ssh_port', 22)
                username = server.get('ssh_username', 'root')
                password = server.get('ssh_password')
            else:
                return jsonify({'ok': False, 'error': 'Invalid server type'}), 400
            
            if not host or not password:
                return jsonify({'ok': False, 'error': 'SSH credentials not configured'}), 400
            
            # Выполняем перезагрузку
            logger.info(f"Rebooting server {server_type}/{name} ({host}:{port})")
            result = execute_ssh_command(host, port, username, password, 'sudo reboot', timeout=5)
            
            # reboot может не вернуть результат, т.к. сервер перезагружается
            return jsonify({
                'ok': True,
                'message': f'Reboot command sent to {name}'
            })
        except Exception as e:
            logger.error(f"Error rebooting {server_type}/{name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
