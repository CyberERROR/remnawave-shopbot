import os
import json
import asyncio
import logging
import uuid
import threading
from datetime import datetime
from flask import render_template, request, jsonify, current_app
from werkzeug.utils import secure_filename
from aiogram.types import FSInputFile
from shop_bot.data_manager import remnawave_repository as rw_repo

logger = logging.getLogger(__name__)

ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif', 'mp4', 'webm'}
UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), 'img')
RESULTS_FILE = os.path.join(os.path.dirname(__file__), 'total.json')

os.makedirs(UPLOAD_FOLDER, exist_ok=True)

broadcast_progress = {}
broadcast_lock = threading.Lock()

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

async def send_broadcast_async(bot, users, text, media_path=None, media_type=None, buttons=None, mode='all', task_id=None):
    sent = 0
    failed = 0
    skipped = 0
    total = len(users)
    
    if task_id:
        with broadcast_lock:
            broadcast_progress[task_id] = {
                'status': 'running',
                'total': total,
                'sent': 0,
                'failed': 0,
                'skipped': 0,
                'progress': 0,
                'start_time': datetime.now().isoformat()
            }
    
    for index, user in enumerate(users):
        user_id = user.get('telegram_id')
        if not user_id:
            continue
            
        is_banned = user.get('is_banned', False)
        if is_banned:
            skipped += 1
            
            if task_id:
                with broadcast_lock:
                    if task_id in broadcast_progress:
                        broadcast_progress[task_id].update({
                            'skipped': skipped,
                            'progress': int((index + 1) / total * 100)
                        })
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
        
        if task_id and ((index + 1) % 10 == 0 or (index + 1) == total):
            with broadcast_lock:
                if task_id in broadcast_progress:
                    broadcast_progress[task_id].update({
                        'sent': sent,
                        'failed': failed,
                        'skipped': skipped,
                        'progress': int((index + 1) / total * 100)
                    })
    
    if task_id:
        with broadcast_lock:
            if task_id in broadcast_progress:
                broadcast_progress[task_id].update({
                    'status': 'completed',
                    'sent': sent,
                    'failed': failed,
                    'skipped': skipped,
                    'progress': 100,
                    'end_time': datetime.now().isoformat()
                })
    
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
            media_filename = request.form.get('media_filename', '')
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
            
            media_path = None
            media_type = None
            if media_filename:
                media_path = os.path.join(UPLOAD_FOLDER, media_filename)
                if os.path.exists(media_path):
                    media_type = get_media_type(media_filename)
            
            loop = current_app.config.get('EVENT_LOOP')
            if not loop or not loop.is_running():
                return jsonify({'ok': False, 'error': 'Event loop not available'}), 500
            
            async def send_preview():
                preview_text = f"📨 <b>Предпросмотр рассылки</b>\n\n{text}"
                
                if media_path and media_type:
                    media_file = FSInputFile(media_path)
                    if media_type == 'photo':
                        await bot.send_photo(
                            chat_id=int(admin_id),
                            photo=media_file,
                            caption=preview_text,
                            parse_mode='HTML',
                            reply_markup=keyboard
                        )
                    elif media_type == 'video':
                        await bot.send_video(
                            chat_id=int(admin_id),
                            video=media_file,
                            caption=preview_text,
                            parse_mode='HTML',
                            reply_markup=keyboard
                        )
                    elif media_type == 'animation':
                        await bot.send_animation(
                            chat_id=int(admin_id),
                            animation=media_file,
                            caption=preview_text,
                            parse_mode='HTML',
                            reply_markup=keyboard
                        )
                else:
                    await bot.send_message(
                        chat_id=int(admin_id),
                        text=preview_text,
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
            elif mode == 'without_trial' or mode == 'not_used_trial':
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
            
            task_id = str(uuid.uuid4())
            
            asyncio.run_coroutine_threadsafe(
                send_broadcast_async(bot, all_users, text, media_path, media_type, buttons, mode, task_id),
                loop
            )
            
            return jsonify({
                'ok': True,
                'task_id': task_id,
                'total_users': len(all_users)
            })
        except Exception as e:
            logger.error(f"Error starting broadcast: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    
    @flask_app.route('/other/broadcast/status/<task_id>', methods=['GET'])
    @login_required
    def broadcast_status(task_id):
        """Получение прогресса рассылки"""
        with broadcast_lock:
            if task_id not in broadcast_progress:
                return jsonify({'ok': False, 'error': 'Task not found'}), 404
            
            progress = broadcast_progress[task_id].copy()
        
        return jsonify({
            'ok': True,
            'progress': progress
        })
    
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
            
            if not code:
                chars = string.ascii_uppercase + string.digits
                code = ''.join(random.choice(chars) for _ in range(8))
            
            if not discount_value:
                return jsonify({'ok': False, 'error': 'Discount value is required'}), 400
            
            try:
                discount_value = float(discount_value)
            except ValueError:
                return jsonify({'ok': False, 'error': 'Invalid discount value'}), 400
            
            if discount_value <= 0:
                return jsonify({'ok': False, 'error': 'Discount must be positive'}), 400
            
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
            
            existing = rw_repo.get_promo_code(code)
            if not existing:
                return jsonify({'ok': False, 'error': 'Promo code not found'}), 404
            
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
            
            rw_repo.delete_promo_code(code)
            
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
            
            filtered_hosts = [
                h for h in hosts 
                if h.get('ssh_host') and h.get('ssh_password')
            ]
            
            filtered_ssh_targets = [
                t for t in ssh_targets 
                if t.get('ssh_host') and t.get('ssh_password')
            ]
            
            return jsonify({
                'ok': True,
                'hosts': filtered_hosts,
                'ssh_targets': filtered_ssh_targets
            })
        except Exception as e:
            logger.error(f"Error getting servers list: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    
    @flask_app.route('/other/servers/uptime/<server_type>/<name>')
    @login_required
    def server_uptime(server_type, name):
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
            
            delimiter = "___"
            command = (
                f"cat /proc/uptime || echo '0 0'; echo '{delimiter}'; "
                f"top -bn1 | grep 'Cpu(s)' | awk '{{print $2}}' || echo '0.0'; echo '{delimiter}'; "
                f"nproc || echo '1'; echo '{delimiter}'; "
                f"free -m | grep Mem | awk '{{print $3 \" \" $2}}' || echo '0 0'; echo '{delimiter}'; "
                f"free -m | grep Swap | awk '{{print $3 \" \" $2}}' || echo '0 0'; echo '{delimiter}'; "
                f"cat /proc/sys/vm/swappiness || echo '-1'"
            )
            result = execute_ssh_command(host, port, username, password, command, timeout=20)
            
            if result['ok']:
                try:
                    output_raw = result['output'].strip()
                    logger.debug(f"Raw system info output for {name}: {repr(output_raw)}")
                    parts = output_raw.split(delimiter)
                    
                    if len(parts) < 6:
                        logger.error(f"Insufficient parts in output for {name}. Expected 6, got {len(parts)}. Parts: {parts}")
                        return jsonify({'ok': False, 'error': f'Incomplete system info (got {len(parts)}/6 parts)'}), 500
                    
                    uptime_parts = parts[0].strip().split()
                    if len(uptime_parts) > 0 and uptime_parts[0]:
                        try:
                            uptime_seconds = float(uptime_parts[0])
                        except (ValueError, IndexError):
                            logger.warning(f"Failed to parse uptime from '{parts[0]}' for {name}")
                            uptime_seconds = 0
                    else:
                        uptime_seconds = 0
                    
                    cpu_str = parts[1].strip().replace(',', '.').replace('%', '') 
                    try:
                        cpu_usage = float(cpu_str) if cpu_str and cpu_str != '' else 0.0
                    except ValueError:
                        logger.warning(f"Failed to parse CPU from '{parts[1]}' for {name}")
                        cpu_usage = 0.0
                    
                    cores_str = parts[2].strip()
                    if cores_str.isdigit():
                        cpu_cores = int(cores_str)
                    else:
                        logger.warning(f"Failed to parse cores from '{parts[2]}' for {name}")
                        cpu_cores = 1
                    
                    ram_str = parts[3].strip().split()
                    if len(ram_str) >= 2:
                        try:
                            ram_used = int(ram_str[0])
                            ram_total = int(ram_str[1])
                        except (ValueError, IndexError):
                            logger.warning(f"Failed to parse RAM from '{parts[3]}' for {name}")
                            ram_used = 0
                            ram_total = 0
                    else:
                        ram_used = 0
                        ram_total = 0
                    ram_percent = (ram_used / ram_total * 100) if ram_total > 0 else 0
                    
                    swap_str = parts[4].strip().split()
                    if len(swap_str) >= 2:
                        try:
                            swap_used = int(swap_str[0])
                            swap_total = int(swap_str[1])
                        except (ValueError, IndexError):
                            logger.warning(f"Failed to parse SWAP from '{parts[4]}' for {name}")
                            swap_used = 0
                            swap_total = 0
                    else:
                        
                        swap_used = 0
                        swap_total = 0
                    swap_percent = (swap_used / swap_total * 100) if swap_total > 0 else 0
                    
                    swappiness_str = parts[5].strip()
                    if swappiness_str and swappiness_str.replace('-','').isdigit():
                        try:
                            swappiness = int(swappiness_str)
                        except ValueError:
                            logger.warning(f"Failed to parse swappiness from '{parts[5]}' for {name}")
                            swappiness = -1
                    else:
                        swappiness = -1

                    return jsonify({
                        'ok': True,
                        'uptime_seconds': uptime_seconds,
                        'uptime_formatted': format_uptime(uptime_seconds),
                        'cpu_percent': round(cpu_usage, 1),
                        'cpu_cores': cpu_cores,
                        'ram_used': ram_used,
                        'ram_total': ram_total,
                        'ram_percent': round(ram_percent, 1),
                        'swap_used': swap_used,
                        'swap_total': swap_total,
                        'swap_percent': round(swap_percent, 1),
                        'swappiness': swappiness
                    })
                except Exception as parse_error:
                    logger.exception(f"Failed to parse system info for {name}. Output was: {result['output']}")
                    return jsonify({'ok': False, 'error': f'Failed to parse system info: {str(parse_error)}'}), 500
            else:
                
                error_msg = result.get('error', 'Unknown error')
                if 'timed out' in error_msg.lower() or 'timeout' in error_msg.lower():
                    logger.warning(f"SSH timeout for {server_type}/{name}: {error_msg}")
                    return jsonify({'ok': False, 'error': 'Server connection timeout', 'details': error_msg}), 503
                else:
                    logger.error(f"SSH command failed for {server_type}/{name}: {error_msg}")
                    return jsonify({'ok': False, 'error': error_msg}), 503
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
            
            logger.info(f"Rebooting server {server_type}/{name} ({host}:{port})")
            result = execute_ssh_command(host, port, username, password, 'sudo reboot', timeout=5)
            
            return jsonify({
                'ok': True,
                'message': f'Reboot command sent to {name}'
            })
        except Exception as e:
            logger.error(f"Error rebooting {server_type}/{name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    
    @flask_app.route('/other/servers/deploy/check-status/<name>', methods=['GET'])
    @login_required
    def deploy_check_status(name):
        """Проверка состояния развертывания: Docker, директория, docker-compose.yml"""
        try:
            ssh_targets = rw_repo.get_all_ssh_targets()
            server = next((t for t in ssh_targets if t.get('target_name') == name), None)
            if not server:
                return jsonify({'ok': False, 'error': 'SSH target not found'}), 404
            
            host = server.get('ssh_host')
            port = server.get('ssh_port', 22)
            username = server.get('ssh_username', 'root')
            password = server.get('ssh_password')
            
            if not host or not password:
                return jsonify({'ok': False, 'error': 'SSH credentials not configured'}), 400
            
            status = {
                'docker_installed': False,
                'directory_exists': False,
                'compose_file_exists': False,
                'suggested_step': 1
            }
            
            logger.info(f"Checking Docker on {name} ({host}:{port})")
            docker_check = execute_ssh_command(
                host, port, username, password,
                'docker --version',
                timeout=10
            )
            status['docker_installed'] = docker_check['ok']
            
            if status['docker_installed']:
                dir_check = execute_ssh_command(
                    host, port, username, password,
                    'test -d /opt/remnanode && echo "exists"',
                    timeout=10
                )
                status['directory_exists'] = 'exists' in dir_check.get('output', '')
                
                if status['directory_exists']:
                    compose_check = execute_ssh_command(
                        host, port, username, password,
                        'test -f /opt/remnanode/docker-compose.yml && echo "exists"',
                        timeout=10
                    )
                    status['compose_file_exists'] = 'exists' in compose_check.get('output', '')
            
            if not status['docker_installed']:
                status['suggested_step'] = 1
            elif not status['directory_exists']:
                status['suggested_step'] = 2
            elif not status['compose_file_exists']:
                status['suggested_step'] = 3
            else:
                status['suggested_step'] = 5  
            
            return jsonify({
                'ok': True,
                'status': status
            })
        except Exception as e:
            logger.error(f"Error checking deployment status on {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    
    @flask_app.route('/other/servers/deploy/install-docker/<name>', methods=['POST'])
    @login_required
    def deploy_install_docker(name):
        """Установка Docker на SSH-цели"""
        try:
            ssh_targets = rw_repo.get_all_ssh_targets()
            server = next((t for t in ssh_targets if t.get('target_name') == name), None)
            if not server:
                return jsonify({'ok': False, 'error': 'SSH target not found'}), 404
            
            host = server.get('ssh_host')
            port = server.get('ssh_port', 22)
            username = server.get('ssh_username', 'root')
            password = server.get('ssh_password')
            
            if not host or not password:
                return jsonify({'ok': False, 'error': 'SSH credentials not configured'}), 400
            
            logger.info(f"Installing Docker on {name} ({host}:{port})")
            result = execute_ssh_command(
                host, port, username, password,
                'sudo curl -fsSL https://get.docker.com | sh',
                timeout=300  
            )
            
            if result['ok']:
                return jsonify({
                    'ok': True,
                    'message': 'Docker successfully installed',
                    'output': result['output']
                })
            else:
                return jsonify({
                    'ok': False,
                    'error': result['error'] or 'Failed to install Docker',
                    'output': result['output']
                }), 500
        except Exception as e:
            logger.error(f"Error installing Docker on {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    
    @flask_app.route('/other/servers/deploy/create-directory/<name>', methods=['POST'])
    @login_required
    def deploy_create_directory(name):
        """Создание директории для Remnawave ноды"""
        try:
            ssh_targets = rw_repo.get_all_ssh_targets()
            server = next((t for t in ssh_targets if t.get('target_name') == name), None)
            if not server:
                return jsonify({'ok': False, 'error': 'SSH target not found'}), 404
            
            host = server.get('ssh_host')
            port = server.get('ssh_port', 22)
            username = server.get('ssh_username', 'root')
            password = server.get('ssh_password')
            
            if not host or not password:
                return jsonify({'ok': False, 'error': 'SSH credentials not configured'}), 400
            
            logger.info(f"Creating directory on {name} ({host}:{port})")
            result = execute_ssh_command(
                host, port, username, password,
                'mkdir -p /opt/remnanode && cd /opt/remnanode && pwd',
                timeout=30
            )
            
            if result['ok']:
                return jsonify({
                    'ok': True,
                    'message': 'Directory created successfully',
                    'output': result['output']
                })
            else:
                return jsonify({
                    'ok': False,
                    'error': result['error'] or 'Failed to create directory',
                    'output': result['output']
                }), 500
        except Exception as e:
            logger.error(f"Error creating directory on {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    
    @flask_app.route('/other/servers/deploy/save-compose/<name>', methods=['POST'])
    @login_required
    def deploy_save_compose(name):
        """Сохранение docker-compose.yml файла"""
        try:
            content = request.form.get('content', '').strip()
            if not content:
                return jsonify({'ok': False, 'error': 'Content is required'}), 400
            
            ssh_targets = rw_repo.get_all_ssh_targets()
            server = next((t for t in ssh_targets if t.get('target_name') == name), None)
            if not server:
                return jsonify({'ok': False, 'error': 'SSH target not found'}), 404
            
            host = server.get('ssh_host')
            port = server.get('ssh_port', 22)
            username = server.get('ssh_username', 'root')
            password = server.get('ssh_password')
            
            if not host or not password:
                return jsonify({'ok': False, 'error': 'SSH credentials not configured'}), 400
            
            safe_content = content.replace("'", "'\\''")
            
            logger.info(f"Saving docker-compose.yml on {name} ({host}:{port})")
            result = execute_ssh_command(
                host, port, username, password,
                f"cd /opt/remnanode && cat > docker-compose.yml << 'EOF'\n{content}\nEOF",
                timeout=30
            )
            
            if result['ok'] or result['exit_status'] == 0:
                return jsonify({
                    'ok': True,
                    'message': 'docker-compose.yml saved successfully'
                })
            else:
                return jsonify({
                    'ok': False,
                    'error': result['error'] or 'Failed to save docker-compose.yml',
                    'output': result['output']
                }), 500
        except Exception as e:
            logger.error(f"Error saving docker-compose.yml on {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    
    @flask_app.route('/other/servers/deploy/view-compose/<name>', methods=['GET'])
    @login_required
    def deploy_view_compose(name):
        """Просмотр содержимого docker-compose.yml"""
        try:
            ssh_targets = rw_repo.get_all_ssh_targets()
            server = next((t for t in ssh_targets if t.get('target_name') == name), None)
            if not server:
                return jsonify({'ok': False, 'error': 'SSH target not found'}), 404
            
            host = server.get('ssh_host')
            port = server.get('ssh_port', 22)
            username = server.get('ssh_username', 'root')
            password = server.get('ssh_password')
            
            if not host or not password:
                return jsonify({'ok': False, 'error': 'SSH credentials not configured'}), 400
            
            logger.info(f"Reading docker-compose.yml from {name} ({host}:{port})")
            result = execute_ssh_command(
                host, port, username, password,
                'cd /opt/remnanode && cat docker-compose.yml',
                timeout=30
            )
            
            if result['ok']:
                return jsonify({
                    'ok': True,
                    'content': result['output']
                })
            else:
                return jsonify({
                    'ok': False,
                    'error': result['error'] or 'File not found or error reading',
                    'output': result['output']
                }), 500
        except Exception as e:
            logger.error(f"Error reading docker-compose.yml from {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    
    @flask_app.route('/other/servers/deploy/manage-containers/<name>', methods=['POST'])
    @login_required
    def deploy_manage_containers(name):
        """Управление контейнерами (start, restart, logs)"""
        try:
            action = request.form.get('action', 'start')  
            
            ssh_targets = rw_repo.get_all_ssh_targets()
            server = next((t for t in ssh_targets if t.get('target_name') == name), None)
            if not server:
                return jsonify({'ok': False, 'error': 'SSH target not found'}), 404
            
            host = server.get('ssh_host')
            port = server.get('ssh_port', 22)
            username = server.get('ssh_username', 'root')
            password = server.get('ssh_password')
            
            if not host or not password:
                return jsonify({'ok': False, 'error': 'SSH credentials not configured'}), 400
            
            if action == 'start':
                command = 'cd /opt/remnanode && docker compose up -d'
                timeout = 120
            elif action == 'restart':
                command = 'cd /opt/remnanode && docker compose restart remnanode'
                timeout = 60
            elif action == 'logs':
                
                command = 'cd /opt/remnanode && docker compose logs -t --tail=100 remnanode'
                timeout = 30
            else:
                return jsonify({'ok': False, 'error': 'Invalid action'}), 400
            
            logger.info(f"Managing containers on {name} ({host}:{port}) - action: {action}")
            result = execute_ssh_command(host, port, username, password, command, timeout=timeout)
            
            if result['ok'] or result['exit_status'] == 0:
                return jsonify({
                    'ok': True,
                    'message': f'Action {action} executed successfully',
                    'output': result['output']
                })
            else:
                return jsonify({
                    'ok': False,
                    'error': result['error'] or f'Failed to execute {action}',
                    'output': result['output']
                }), 500
        except Exception as e:
            logger.error(f"Error managing containers on {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    
    @flask_app.route('/other/servers/deploy/remove-all/<name>', methods=['POST'])
    @login_required
    def deploy_remove_all(name):
        """Полное удаление ноды и Docker"""
        try:
            ssh_targets = rw_repo.get_all_ssh_targets()
            server = next((t for t in ssh_targets if t.get('target_name') == name), None)
            if not server:
                return jsonify({'ok': False, 'error': 'SSH target not found'}), 404
            
            host = server.get('ssh_host')
            port = server.get('ssh_port', 22)
            username = server.get('ssh_username', 'root')
            password = server.get('ssh_password')
            
            if not host or not password:
                return jsonify({'ok': False, 'error': 'SSH credentials not configured'}), 400
            
            command = (
                '('
                'if [ -f /opt/remnanode/docker-compose.yml ]; then '
                    'cd /opt/remnanode && sudo docker compose down 2>/dev/null || true; '
                'fi; '
                'sudo rm -rf /opt/remnanode; '
                'if command -v docker &> /dev/null; then '
                    'sudo apt-get purge -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin docker-ce-rootless-extras 2>/dev/null || true; '
                    'sudo rm -rf /var/lib/docker /var/lib/containerd ~/.docker 2>/dev/null || true; '
                'fi; '
                'echo "Cleanup completed"'
                ')'
            )
            
            logger.warning(f"REMOVING ALL Docker and node data on {name} ({host}:{port})")
            result = execute_ssh_command(host, port, username, password, command, timeout=180)
            
            if result.get('output') and 'Cleanup completed' in result.get('output', ''):
                return jsonify({
                    'ok': True,
                    'message': 'Нода и Docker полностью удалены',
                    'output': result['output']
                })
            elif result.get('ok') or result.get('exit_status') == 0:
                return jsonify({
                    'ok': True,
                    'message': 'Команда удаления выполнена',
                    'output': result.get('output', '')
                })
            else:
                logger.error(f"Remove all failed on {name}: {result.get('error')}, output: {result.get('output')}")
                return jsonify({
                    'ok': False,
                    'error': result.get('error') or 'Failed to remove',
                    'output': result.get('output', '')
                }), 500
        except Exception as e:
            logger.error(f"Error removing all on {name}: {e}", exc_info=True)
            return jsonify({'ok': False, 'error': str(e)}), 500
    
    @flask_app.route('/other/logs/stream')
    @login_required
    def logs_stream():
        """Стриминг логов. Пытается использовать Docker CLI, Socket или локальные файлы."""
        def generate():
            import subprocess
            import shutil
            import time
            import socket
            import http.client
            
            if os.name == 'nt':
                yield f"data: [INFO] --- Windows Logs Simulation Mode ---\n\n"
                while True:
                    yield f"data: [INFO] {datetime.now().isoformat()} - Heartbeat\n\n"
                    time.sleep(2)
                return

            cli_cmd = None
            if shutil.which('docker-compose'):
                cli_cmd = ['docker-compose', 'logs', '-f', '--tail=100']
            elif shutil.which('docker'):
                cli_cmd = ['docker', 'compose', 'logs', '-f', '--tail=100']
            
            if cli_cmd and os.path.exists('/root/remnawave-shopbot'):
                yield f"data: [INFO] Docker CLI found. Trying to stream via command...\n\n"
                try:
                    process = subprocess.Popen(
                        cli_cmd,
                        cwd='/root/remnawave-shopbot',
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                        universal_newlines=True,
                        bufsize=1
                    )
                    for line in iter(process.stdout.readline, ''):
                        if line: yield f"data: {line.rstrip()}\n\n"
                    process.stdout.close()
                    yield f"data: [EXIT] CLI process exited.\n\n"
                    return 
                except Exception as e:
                    yield f"data: [WARN] CLI failed: {e}. Trying Docker Socket...\n\n"
            
            socket_path = '/var/run/docker.sock'
            if os.path.exists(socket_path):
                yield f"data: [INFO] Docker socket found at {socket_path}. Connecting...\n\n"
                try:
                    
                    hostname = socket.gethostname()
                    
                    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    sock.connect(socket_path)
                    
                    request = f"GET /containers/{hostname}/logs?stdout=1&stderr=1&follow=1&tail=100 HTTP/1.1\r\nHost: localhost\r\n\r\n"
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
                                
                                for line in text.splitlines():
                                    yield f"data: {line}\n\n"
                            except:
                                pass
                                
                    sock.close()
                    yield f"data: [EXIT] Socket stream ended.\n\n"
                    return
                except Exception as e:
                     yield f"data: [ERROR] Socket connection failed: {e}\n\n"
            else:
                 yield f"data: [WARN] Docker socket not found at {socket_path}.\n\n"

            log_files = ['logs/bot.log', 'bot.log']
            found_log = False
            for log_file in log_files:
                if os.path.exists(log_file):
                    found_log = True
                    yield f"data: [INFO] Reading local log file: {log_file} (tail mode)\n\n"
                    try:
                        from collections import deque
                        
                        with open(log_file, 'r', encoding='utf-8', errors='replace') as f:
                            
                            for line in deque(f, 100):
                                yield f"data: {line.strip()}\n\n"
                            
                            f.seek(0, os.SEEK_END)
                            
                            while True:
                                line = f.readline()
                                if not line:
                                    time.sleep(0.5)
                                    continue
                                yield f"data: {line.strip()}\n\n"
                                
                    except Exception as e:
                        yield f"data: [ERROR] Error reading file: {e}\n\n"
                    break
            
            if not found_log:
                yield f"data: [CRITICAL] No log methods work. CLI missing, Socket missing, Log files missing.\n\n"

        return current_app.response_class(generate(), mimetype='text/event-stream')

    @flask_app.route('/other/logs/clear', methods=['POST'])
    @login_required
    def logs_clear():
        """Очистка логов (локальных или docker)"""
        try:
            import subprocess
            
            cleared_any = False
            
            log_files = ['logs/bot.log', 'bot.log']
            for log_file in log_files:
                if os.path.exists(log_file):
                    try:
                        
                        with open(log_file, 'w', encoding='utf-8') as f:
                            pass
                        logger.info(f"Cleared local log file: {log_file}")
                        cleared_any = True
                    except Exception as e:
                        logger.error(f"Failed to clear {log_file}: {e}")
            
            if cleared_any:
                return jsonify({'ok': True, 'message': 'Local logs cleared successfully'})

            cmd = "truncate -s 0 /var/lib/docker/containers/*/*-json.log"
            
            if os.name == 'nt':
                logger.info("Windows detected using dummy log clear")
                return jsonify({'ok': True, 'message': 'Logs cleared (Simulation)'})
            
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            
            if result.returncode == 0:
                return jsonify({'ok': True, 'message': 'Docker logs cleared successfully'})
            else:
                return jsonify({'ok': False, 'error': f"Failed: {result.stderr or 'Permission denied'}"}), 500
                
        except Exception as e:
            logger.error(f"Error clearing logs: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500

    @flask_app.route('/other/restart', methods=['POST'])
    @login_required
    def logs_restart():
        """Полный перезапуск бота через docker-compose restart"""
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
                except FileNotFoundError:
                    pass
            
            if not cmd:
                
                logger.warning("Docker CLI not found. Falling back to process exit.")
                
                def suicide():
                    import time
                    import sys
                    time.sleep(1)
                    logger.critical("Initiating self-restart via sys.exit(1)")
                    os._exit(1)

                threading.Thread(target=suicide).start()
                return jsonify({'ok': True, 'message': 'Перезапускаем процесс...'})

            proc = subprocess.Popen(cmd, shell=True) 
            return jsonify({'ok': True, 'message': 'Перезапуск бота отправлен. Пожалуйста, подождите 10-20 секунд.'})

        except Exception as e:
            logger.error(f"Error restarting bot: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500

    @flask_app.route('/other/servers/warp/status/<name>', methods=['GET'])
    @login_required
    def warp_status(name):
        """Проверка статуса WARP (wireproxy)"""
        try:
            ssh_targets = rw_repo.get_all_ssh_targets()
            server = next((t for t in ssh_targets if t.get('target_name') == name), None)
            if not server:
                return jsonify({'ok': False, 'error': 'SSH target not found'}), 404
            
            host = server.get('ssh_host')
            port = server.get('ssh_port', 22)
            username = server.get('ssh_username', 'root')
            password = server.get('ssh_password')
            
            if not host or not password:
                return jsonify({'ok': False, 'error': 'SSH credentials not configured'}), 400
            
            command = (
                "systemctl is-active wireproxy; "
                "if systemctl list-unit-files | grep -q wireproxy; then echo 'SERVICE_EXISTS'; else echo 'SERVICE_MISSING'; fi; "
                "if [ -f /usr/local/bin/wireproxy ] || [ -f /usr/bin/wireproxy ]; then echo 'BINARY_FOUND'; else echo 'BINARY_MISSING'; fi; "
                "systemctl cat wireproxy 2>/dev/null | grep -E 'MemoryMax|MemoryHigh' || true"
            )
            
            result = execute_ssh_command(host, port, username, password, command, timeout=15)
            
            status = {
                'installed': False,
                'active': False,
                'service_exists': False,
                'binary_exists': False,
                'memory_max': 'N/A',
                'memory_high': 'N/A'
            }
            
            if result['ok']:
                lines = result['output'].splitlines()
                if len(lines) >= 3:
                    is_active = lines[0].strip() == 'active'
                    service_exists = 'SERVICE_EXISTS' in result['output']
                    binary_exists = 'BINARY_FOUND' in result['output']
                    
                    status['active'] = is_active
                    status['service_exists'] = service_exists
                    status['binary_exists'] = binary_exists
                    status['installed'] = service_exists or binary_exists
                    
                    import re
                    
                    all_max = re.findall(r'MemoryMax=([^\s]+)', result['output'])
                    all_high = re.findall(r'MemoryHigh=([^\s]+)', result['output'])
                    
                    if all_max: status['memory_max'] = all_max[-1]
                    if all_high: status['memory_high'] = all_high[-1]
            
            return jsonify({'ok': True, 'status': status})
            
        except Exception as e:
            logger.error(f"Error checking WARP status on {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500

    @flask_app.route('/other/servers/warp/install/<name>', methods=['POST'])
    @login_required
    def warp_install(name):
        """Установка WARP (wireproxy)"""
        try:
            ssh_targets = rw_repo.get_all_ssh_targets()
            server = next((t for t in ssh_targets if t.get('target_name') == name), None)
            if not server:
                return jsonify({'ok': False, 'error': 'SSH target not found'}), 404
            
            host = server.get('ssh_host')
            port = server.get('ssh_port', 22)
            username = server.get('ssh_username', 'root')
            password = server.get('ssh_password')
            
            if not host or not password:
                return jsonify({'ok': False, 'error': 'SSH credentials not configured'}), 400
            
            install_cmd = "printf '1\\n1\\n40000\\n' | bash <(curl -fsSL https://gitlab.com/fscarmen/warp/-/raw/main/menu.sh) w"
            
            logger.info(f"Installing WARP on {name} ({host}:{port})")
            
            result = execute_ssh_command(host, port, username, password, install_cmd, timeout=300)
            
            if result['ok'] or "Socks5 configured" in result['output']:
                try:
                    
                    config_cmd = (
                        "mkdir -p /etc/systemd/system/wireproxy.service.d && "
                        "printf '[Service]\\nEnvironment=\"WG_LOG_LEVEL=error\"\\nStandardOutput=null\\nStandardError=journal\\nMemoryMax=800M\\nMemoryHigh=1G\\n' > /etc/systemd/system/wireproxy.service.d/override.conf && "
                        "systemctl daemon-reload && "
                        "systemctl restart wireproxy"
                    )
                    
                    logger.info(f"Applying default config to WARP on {name}")
                    config_res = execute_ssh_command(host, port, username, password, config_cmd, timeout=30)
                    if config_res['ok']:
                        result['output'] += "\n[Config] Applied default settings (800M/1G)"
                    else:
                        result['output'] += f"\n[Config] Failed to apply defaults: {config_res['error']}"
                        
                except Exception as e:
                    logger.error(f"Failed to apply default config on {name}: {e}")
            
            if result['ok'] or "Socks5 configured" in result['output']:
                 return jsonify({
                    'ok': True, 
                    'message': 'WARP успешно установлен',
                    'output': result['output']
                })
            else:
                return jsonify({
                    'ok': False, 
                    'error': result['error'] or 'Ошибка установки',
                    'output': result['output']
                }), 500
                
        except Exception as e:
            logger.error(f"Error installing WARP on {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500

    @flask_app.route('/other/servers/warp/uninstall/<name>', methods=['POST'])
    @login_required
    def warp_uninstall(name):
        """Удаление WARP"""
        try:
            ssh_targets = rw_repo.get_all_ssh_targets()
            server = next((t for t in ssh_targets if t.get('target_name') == name), None)
            if not server:
                return jsonify({'ok': False, 'error': 'SSH target not found'}), 404
            
            host = server.get('ssh_host')
            port = server.get('ssh_port', 22)
            username = server.get('ssh_username', 'root')
            password = server.get('ssh_password')
            
            if not host or not password:
                return jsonify({'ok': False, 'error': 'SSH credentials not configured'}), 400
            
            uninstall_cmd = "printf 'y\\n' | bash <(curl -fsSL https://gitlab.com/fscarmen/warp/-/raw/main/menu.sh) u"
            
            logger.info(f"Uninstalling WARP on {name}")
            result = execute_ssh_command(host, port, username, password, uninstall_cmd, timeout=120)
            
            if result['ok']:
                 return jsonify({
                    'ok': True, 
                    'message': 'WARP успешно удален',
                    'output': result['output']
                })
            else:
                return jsonify({
                    'ok': False, 
                    'error': result['error'] or 'Ошибка удаления',
                    'output': result['output']
                }), 500
                
        except Exception as e:
            logger.error(f"Error uninstalling WARP on {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500

    @flask_app.route('/other/servers/warp/config/<name>', methods=['POST'])
    @login_required
    def warp_config(name):
        try:
            ssh_targets = rw_repo.get_all_ssh_targets()
            server = next((t for t in ssh_targets if t.get('target_name') == name), None)
            if not server:
                return jsonify({'ok': False, 'error': 'SSH target not found'}), 404
            
            host = server.get('ssh_host')
            port = server.get('ssh_port', 22)
            username = server.get('ssh_username', 'root')
            password = server.get('ssh_password')
            
            if not host or not password:
                return jsonify({'ok': False, 'error': 'SSH credentials not configured'}), 400
                
            memory_max = request.form.get('memory_max', '800M')
            memory_high = request.form.get('memory_high', '1G')
            
            override_dir = '/etc/systemd/system/wireproxy.service.d'
            override_file = f'{override_dir}/override.conf'
            
            check_cmd = f"test -f {override_file} && echo 'EXISTS' || echo 'NOT_EXISTS'"
            check_result = execute_ssh_command(host, port, username, password, check_cmd, timeout=10)
            
            if check_result['ok'] and 'EXISTS' in check_result['output']:
                cmd = (
                    f"mkdir -p {override_dir} && "
                    f"if grep -q '^MemoryMax=' {override_file}; then "
                    f"sed -i 's/^MemoryMax=.*/MemoryMax={memory_max}/' {override_file}; "
                    f"else "
                    f"sed -i '/^\\[Service\\]/a MemoryMax={memory_max}' {override_file}; "
                    f"fi && "
                    f"if grep -q '^MemoryHigh=' {override_file}; then "
                    f"sed -i 's/^MemoryHigh=.*/MemoryHigh={memory_high}/' {override_file}; "
                    f"else "
                    f"sed -i '/^\\[Service\\]/a MemoryHigh={memory_high}' {override_file}; "
                    f"fi && "
                    "systemctl daemon-reload && "
                    "systemctl restart wireproxy"
                )
            else:
                override_content = f"""[Service]
MemoryMax={memory_max}
MemoryHigh={memory_high}
"""
                safe_content = override_content.replace("'", "'\"'\"'")
                cmd = (
                    f"mkdir -p {override_dir} && "
                    f"printf '%s' '{safe_content}' > {override_file} && "
                    "systemctl daemon-reload && "
                    "systemctl restart wireproxy"
                )
            
            logger.info(f"Configuring WARP on {name}: {memory_max}/{memory_high}")
            result = execute_ssh_command(host, port, username, password, cmd, timeout=60)
            
            if result['ok']:
                 return jsonify({
                    'ok': True, 
                    'message': 'Конфигурация обновлена и сервис перезапущен',
                    'output': result['output']
                })
            else:
                 return jsonify({
                    'ok': False, 
                    'error': result['error'] or 'Ошибка конфигурации',
                    'output': result['output']
                }), 500
                
        except Exception as e:
             logger.error(f"Error configuring WARP on {name}: {e}")
             return jsonify({'ok': False, 'error': str(e)}), 500

    @flask_app.route('/other/servers/warp/restart/<name>', methods=['POST'])
    @login_required
    def warp_restart(name):
        """Перезапуск сервиса wireproxy"""
        try:
             ssh_targets = rw_repo.get_all_ssh_targets()
             server = next((t for t in ssh_targets if t.get('target_name') == name), None)
             if not server:
                 return jsonify({'ok': False, 'error': 'SSH target not found'}), 404
             
             host = server.get('ssh_host')
             port = server.get('ssh_port', 22)
             username = server.get('ssh_username', 'root')
             password = server.get('ssh_password')
             
             if not host or not password:
                 return jsonify({'ok': False, 'error': 'SSH credentials not configured'}), 400
             
             cmd = "systemctl restart wireproxy"
             
             result = execute_ssh_command(host, port, username, password, cmd, timeout=30)
             
             if result['ok']:
                  return jsonify({'ok': True, 'message': 'Сервис wireproxy перезапущен'})
             else:
                  return jsonify({'ok': False, 'error': result['error']}), 500
        except Exception as e:
             logger.error(f"Error restarting WARP on {name}: {e}")
             return jsonify({'ok': False, 'error': str(e)}), 500

    @flask_app.route('/other/servers/warp/start/<name>', methods=['POST'])
    @login_required
    def warp_start(name):
        """Запуск WARP"""
        try:
            ssh_targets = rw_repo.get_all_ssh_targets()
            server = next((t for t in ssh_targets if t.get('target_name') == name), None)
            if not server:
                return jsonify({'ok': False, 'error': 'SSH target not found'}), 404
            
            host = server.get('ssh_host')
            port = server.get('ssh_port', 22)
            username = server.get('ssh_username', 'root')
            password = server.get('ssh_password')
            
            if not host or not password:
                return jsonify({'ok': False, 'error': 'SSH credentials not configured'}), 400
                
            cmd = "systemctl start wireproxy"
            result = execute_ssh_command(host, port, username, password, cmd, timeout=30)
            
            if result['ok']:
                 return jsonify({'ok': True, 'message': 'Сервис запущен'})
            else:
                return jsonify({'ok': False, 'error': result['error'] or 'Ошибка запуска'}), 500
                
        except Exception as e:
            logger.error(f"Error starting WARP on {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500

    @flask_app.route('/other/servers/warp/stop/<name>', methods=['POST'])
    @login_required
    def warp_stop(name):
        """Остановка WARP"""
        try:
            ssh_targets = rw_repo.get_all_ssh_targets()
            server = next((t for t in ssh_targets if t.get('target_name') == name), None)
            if not server:
                return jsonify({'ok': False, 'error': 'SSH target not found'}), 404
            
            host = server.get('ssh_host')
            port = server.get('ssh_port', 22)
            username = server.get('ssh_username', 'root')
            password = server.get('ssh_password')
            
            if not host or not password:
                return jsonify({'ok': False, 'error': 'SSH credentials not configured'}), 400
                
            cmd = "systemctl stop wireproxy"
            result = execute_ssh_command(host, port, username, password, cmd, timeout=30)
            
            if result['ok']:
                 return jsonify({'ok': True, 'message': 'Сервис остановлен'})
            else:
                return jsonify({'ok': False, 'error': result['error'] or 'Ошибка остановки'}), 500
                
        except Exception as e:
            logger.error(f"Error stopping WARP on {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500

    @flask_app.route('/other/servers/swap/install/<name>', methods=['POST'])
    @login_required
    def swap_install(name):
        """Установка SWAP файла"""
        try:
            size_mb = request.form.get('size_mb', '2048')
            if not size_mb.isdigit():
                 return jsonify({'ok': False, 'error': 'Invalid size'}), 400
            
            ssh_targets = rw_repo.get_all_ssh_targets()
            server = next((t for t in ssh_targets if t.get('target_name') == name), None)
            if not server:
                return jsonify({'ok': False, 'error': 'SSH target not found'}), 404
            
            host = server.get('ssh_host')
            port = server.get('ssh_port', 22)
            username = server.get('ssh_username', 'root')
            password = server.get('ssh_password')
            
            if not host or not password:
                return jsonify({'ok': False, 'error': 'SSH credentials not configured'}), 400

            cmd = (
                f"fallocate -l {size_mb}M /swapfile || dd if=/dev/zero of=/swapfile bs=1M count={size_mb}; "
                "chmod 600 /swapfile; "
                "mkswap /swapfile; "
                "swapon /swapfile; "
                "grep -q '/swapfile' /etc/fstab || echo '/swapfile none swap sw 0 0' >> /etc/fstab"
            )
            
            logger.info(f"Installing SWAP ({size_mb}MB) on {name}")
            result = execute_ssh_command(host, port, username, password, cmd, timeout=120)
            
            if result['ok']:
                 return jsonify({'ok': True, 'message': 'SWAP установлен'})
            else:
                 return jsonify({'ok': False, 'error': result['error'] or 'Failed to install SWAP'}), 500
                 
        except Exception as e:
            logger.error(f"Error installing SWAP on {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500

    @flask_app.route('/other/servers/swap/delete/<name>', methods=['DELETE'])
    @login_required
    def swap_delete(name):
        """Удаление SWAP файла"""
        try:
            ssh_targets = rw_repo.get_all_ssh_targets()
            server = next((t for t in ssh_targets if t.get('target_name') == name), None)
            if not server:
                return jsonify({'ok': False, 'error': 'SSH target not found'}), 404
            
            host = server.get('ssh_host')
            port = server.get('ssh_port', 22)
            username = server.get('ssh_username', 'root')
            password = server.get('ssh_password')
            
            if not host or not password:
                return jsonify({'ok': False, 'error': 'SSH credentials not configured'}), 400

            cmd = (
                "swapoff /swapfile; "
                "rm /swapfile; "
                "sed -i '/\/swapfile/d' /etc/fstab"
            )
            
            logger.info(f"Deleting SWAP on {name}")
            result = execute_ssh_command(host, port, username, password, cmd, timeout=60)
            
            if result['ok']:
                 return jsonify({'ok': True, 'message': 'SWAP удален'})
            else:
                 return jsonify({'ok': False, 'error': result['error'] or 'Failed to delete SWAP'}), 500
                 
        except Exception as e:
            logger.error(f"Error deleting SWAP on {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
            
    @flask_app.route('/other/servers/swap/resize/<name>', methods=['POST'])
    @login_required
    def swap_resize(name):
        """Изменение размера SWAP (удаление + установка)"""
        try:
            size_mb = request.form.get('size_mb', '2048')
            if not size_mb.isdigit():
                 return jsonify({'ok': False, 'error': 'Invalid size'}), 400
                 
            ssh_targets = rw_repo.get_all_ssh_targets()
            server = next((t for t in ssh_targets if t.get('target_name') == name), None)
            if not server:
                return jsonify({'ok': False, 'error': 'SSH target not found'}), 404
            
            host = server.get('ssh_host')
            port = server.get('ssh_port', 22)
            username = server.get('ssh_username', 'root')
            password = server.get('ssh_password')
            
            if not host or not password:
                return jsonify({'ok': False, 'error': 'SSH credentials not configured'}), 400

            cmd = (
                "if grep -q '/swapfile' /proc/swaps; then "
                "  swapoff /swapfile || exit 1; "
                "fi && "
                "rm -f /swapfile && "
                f"fallocate -l {size_mb}M /swapfile || dd if=/dev/zero of=/swapfile bs=1M count={size_mb} && "
                "chmod 600 /swapfile && "
                "mkswap /swapfile && "
                "swapon /swapfile && "
                "grep -q '/swapfile' /etc/fstab || echo '/swapfile none swap sw 0 0' >> /etc/fstab"
            )
            
            logger.info(f"Resizing SWAP to {size_mb}MB on {name}")
            result = execute_ssh_command(host, port, username, password, cmd, timeout=180)
            
            if result['ok']:
                 return jsonify({'ok': True, 'message': 'Размер SWAP изменен'})
            else:
                 return jsonify({'ok': False, 'error': result['error'] or 'Failed to resize SWAP'}), 500
                 
        except Exception as e:
            logger.error(f"Error resizing SWAP on {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500

    @flask_app.route('/other/servers/swap/swappiness/<name>', methods=['POST'])
    @login_required
    def swap_swappiness(name):
        """Изменение swappiness"""
        try:
            swappiness = request.form.get('swappiness', '60')
            if not swappiness.isdigit() or not (0 <= int(swappiness) <= 100):
                 return jsonify({'ok': False, 'error': 'Invalid swappiness value (0-100)'}), 400
            
            ssh_targets = rw_repo.get_all_ssh_targets()
            server = next((t for t in ssh_targets if t.get('target_name') == name), None)
            if not server:
                return jsonify({'ok': False, 'error': 'SSH target not found'}), 404
            
            host = server.get('ssh_host')
            port = server.get('ssh_port', 22)
            username = server.get('ssh_username', 'root')
            password = server.get('ssh_password')
            
            if not host or not password:
                return jsonify({'ok': False, 'error': 'SSH credentials not configured'}), 400

            cmd = (
                f"sysctl vm.swappiness={swappiness}; "
                f"if grep -q 'vm.swappiness' /etc/sysctl.conf; then "
                f"sed -i 's/^vm.swappiness.*/vm.swappiness={swappiness}/' /etc/sysctl.conf; "
                "else "
                f"echo 'vm.swappiness={swappiness}' >> /etc/sysctl.conf; "
                "fi"
            )
            
            logger.info(f"Changing swappiness to {swappiness} on {name}")
            result = execute_ssh_command(host, port, username, password, cmd, timeout=30)
            
            if result['ok']:
                 return jsonify({'ok': True, 'message': 'Parametr swappiness обновлен'})
            else:
                 return jsonify({'ok': False, 'error': result['error'] or 'Failed to change swappiness'}), 500
                 
        except Exception as e:
            logger.error(f"Error changing swappiness on {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500

    @flask_app.route('/other/servers/warp/systemd/get/<name>', methods=['GET'])
    @login_required
    def warp_systemd_get(name):
        try:
            ssh_targets = rw_repo.get_all_ssh_targets()
            server = next((t for t in ssh_targets if t.get('target_name') == name), None)
            if not server:
                return jsonify({'ok': False, 'error': 'SSH target not found'}), 404
            
            host = server.get('ssh_host')
            port = server.get('ssh_port', 22)
            username = server.get('ssh_username', 'root')
            password = server.get('ssh_password')
            
            if not host or not password:
                return jsonify({'ok': False, 'error': 'SSH credentials not configured'}), 400
            
            override_file = '/etc/systemd/system/wireproxy.service.d/override.conf'
            cmd = f"if [ -f {override_file} ]; then cat {override_file}; else echo ''; fi"
            
            result = execute_ssh_command(host, port, username, password, cmd, timeout=15)
            
            if result['ok']:
                return jsonify({'ok': True, 'content': result['output']})
            else:
                return jsonify({'ok': False, 'error': result['error'] or 'Failed to read config'}), 500
                
        except Exception as e:
            logger.error(f"Error reading systemd config on {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500

    @flask_app.route('/other/servers/warp/systemd/save/<name>', methods=['POST'])
    @login_required
    def warp_systemd_save(name):
        try:
            content = request.form.get('content', '')
            
            ssh_targets = rw_repo.get_all_ssh_targets()
            server = next((t for t in ssh_targets if t.get('target_name') == name), None)
            if not server:
                return jsonify({'ok': False, 'error': 'SSH target not found'}), 404
            
            host = server.get('ssh_host')
            port = server.get('ssh_port', 22)
            username = server.get('ssh_username', 'root')
            password = server.get('ssh_password')
            
            if not host or not password:
                return jsonify({'ok': False, 'error': 'SSH credentials not configured'}), 400
            
            override_dir = '/etc/systemd/system/wireproxy.service.d'
            override_file = f'{override_dir}/override.conf'
            
            safe_content = content.replace("'", "'\"'\"'")
            
            cmd = (
                f"mkdir -p {override_dir} && "
                f"printf '%s' '{safe_content}' > {override_file} && "
                "systemctl daemon-reload && "
                "systemctl restart wireproxy"
            )
            
            logger.info(f"Saving systemd config on {name}")
            result = execute_ssh_command(host, port, username, password, cmd, timeout=60)
            
            if result['ok']:
                return jsonify({'ok': True, 'message': 'Конфигурация сохранена и сервис перезапущен'})
            else:
                return jsonify({'ok': False, 'error': result['error'] or 'Failed to save config'}), 500
                
        except Exception as e:
            logger.error(f"Error saving systemd config on {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500

    @flask_app.route('/other/servers/warp/logs/usage/<name>', methods=['GET'])
    @login_required
    def warp_logs_usage(name):
        try:
            ssh_targets = rw_repo.get_all_ssh_targets()
            server = next((t for t in ssh_targets if t.get('target_name') == name), None)
            if not server:
                return jsonify({'ok': False, 'error': 'SSH target not found'}), 404
            
            host = server.get('ssh_host')
            port = server.get('ssh_port', 22)
            username = server.get('ssh_username', 'root')
            password = server.get('ssh_password')
            
            if not host or not password:
                return jsonify({'ok': False, 'error': 'SSH credentials not configured'}), 400
            
            cmd = "journalctl --disk-usage"
            result = execute_ssh_command(host, port, username, password, cmd, timeout=15)
            
            if result['ok']:
                return jsonify({'ok': True, 'usage': result['output']})
            else:
                return jsonify({'ok': False, 'error': result['error']}), 500
                
        except Exception as e:
            logger.error(f"Error checking log usage for {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500

    @flask_app.route('/other/servers/warp/logs/clean/<name>', methods=['POST'])
    @login_required
    def warp_logs_clean(name):
        try:
            max_size = request.form.get('max_size', '0')
            max_age = request.form.get('max_age', '0')
            
            if not max_size.isdigit() or not max_age.isdigit():
                return jsonify({'ok': False, 'error': 'Invalid values'}), 400
            
            max_size_int = int(max_size)
            max_age_int = int(max_age)
            
            if max_size_int == 0 and max_age_int == 0:
                return jsonify({'ok': False, 'error': 'Укажите хотя бы один параметр (размер или возраст)'}), 400
            
            ssh_targets = rw_repo.get_all_ssh_targets()
            server = next((t for t in ssh_targets if t.get('target_name') == name), None)
            if not server:
                return jsonify({'ok': False, 'error': 'SSH target not found'}), 404
            
            host = server.get('ssh_host')
            port = server.get('ssh_port', 22)
            username = server.get('ssh_username', 'root')
            password = server.get('ssh_password')
            
            if not host or not password:
                return jsonify({'ok': False, 'error': 'SSH credentials not configured'}), 400
            
            cmd_parts = ['sudo journalctl -u wireproxy.service']
            
            if max_size_int > 0:
                cmd_parts.append(f'--vacuum-size={max_size_int}M')
            
            if max_age_int > 0:
                cmd_parts.append(f'--vacuum-time={max_age_int}d')
            
            cmd = ' '.join(cmd_parts)
            
            logger.info(f"Cleaning wireproxy logs on {name}: {cmd}")
            result = execute_ssh_command(host, port, username, password, cmd, timeout=60)
            
            if result['ok']:
                return jsonify({
                    'ok': True,
                    'message': 'Логи wireproxy очищены',
                    'output': result['output']
                })
            else:
                return jsonify({
                    'ok': False,
                    'error': result['error'] or 'Ошибка очистки логов',
                    'output': result['output']
                }), 500
                
        except Exception as e:
            logger.error(f"Error cleaning logs for {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
    
    @flask_app.route('/other/servers/execute/<server_type>/<name>', methods=['POST'])
    @login_required
    def server_execute_command(server_type, name):
        try:
            import paramiko
            from flask import Response, stream_with_context
            import time
            
            command = request.form.get('command', '').strip()
            if not command:
                return jsonify({'ok': False, 'error': 'Command is required'}), 400
            
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
            
            def generate():
                client = None
                channel = None
                try:
                    import re
                    
                    ansi_escape = re.compile(r'\x1B\[[0-9;]*[a-zA-Z]|\x1B\(B|\x1B\[m')
                    
                    client = paramiko.SSHClient()
                    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                    
                    yield f"data: [INFO] Connecting to {host}:{port}...\n\n"
                    
                    client.connect(
                        hostname=host,
                        port=port,
                        username=username,
                        password=password,
                        timeout=30,
                        look_for_keys=False,
                        allow_agent=False
                    )
                    
                    yield f"data: [INFO] Connected. Executing command...\n\n"
                    yield f"data: [INFO] Нажмите кнопку 'Остановить' чтобы прервать выполнение команды\n\n"
                    yield f"data: [INFO] Отображаются последние 300 строк вывода\n\n"
                    
                    channel = client.get_transport().open_session()
                    channel.get_pty()
                    channel.settimeout(0.1)
                    channel.exec_command(command)
                    
                    while True:
                        try:
                            if channel.recv_ready():
                                data = channel.recv(4096)
                                if data:
                                    try:
                                        text = data.decode('utf-8', errors='replace')
                                        text = ansi_escape.sub('', text)
                                        
                                        for line in text.splitlines():
                                            line = line.strip()
                                            if line and line != '':
                                                yield f"data: {line}\n\n"
                                    except Exception as ex:
                                        logger.error(f"Error decoding stdout: {ex}")
                            
                            if channel.recv_stderr_ready():
                                data = channel.recv_stderr(4096)
                                if data:
                                    try:
                                        text = data.decode('utf-8', errors='replace')
                                        text = ansi_escape.sub('', text)
                                        
                                        for line in text.splitlines():
                                            line = line.strip()
                                            if line and line != '':
                                                yield f"data: [STDERR] {line}\n\n"
                                    except Exception as ex:
                                        logger.error(f"Error decoding stderr: {ex}")
                            
                            if channel.exit_status_ready():
                                while channel.recv_ready():
                                    data = channel.recv(4096)
                                    if data:
                                        try:
                                            text = data.decode('utf-8', errors='replace')
                                            text = ansi_escape.sub('', text)
                                            
                                            for line in text.splitlines():
                                                line = line.strip()
                                                if line and line != '':
                                                    yield f"data: {line}\n\n"
                                        except:
                                            pass
                                
                                exit_status = channel.recv_exit_status()
                                yield f"data: [INFO] Command finished with exit code: {exit_status}\n\n"
                                break
                            
                            time.sleep(0.1)
                        except:
                            break
                    
                except Exception as e:
                    logger.error(f"Error executing command on {server_type}/{name}: {e}")
                    yield f"data: [ERROR] {str(e)}\n\n"
                finally:
                    if channel:
                        try:
                            channel.close()
                        except:
                            pass
                    if client:
                        try:
                            client.close()
                        except:
                            pass
                    yield "data: [DONE]\n\n"
            
            return Response(
                stream_with_context(generate()),
                mimetype='text/event-stream',
                headers={
                    'Cache-Control': 'no-cache',
                    'X-Accel-Buffering': 'no',
                    'Connection': 'keep-alive'
                }
            )
            
        except Exception as e:
            logger.error(f"Error in server_execute_command for {server_type}/{name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500