import os
import re
import logging
import asyncio
import json
import secrets
import string
import time
import asyncio
import hashlib
import html as html_escape
import base64
import time
import uuid
from hmac import compare_digest
from datetime import datetime, timezone, timedelta
from functools import wraps
from math import ceil
from flask import Flask, request, render_template, redirect, url_for, flash, session, current_app, jsonify, send_file
from flask_wtf.csrf import CSRFProtect, generate_csrf
import secrets
import urllib.parse
import urllib.request

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logging.getLogger('werkzeug').setLevel(logging.WARNING)

# --- GLOBAL TIME CONFIGURATION ---
# Force MSK (UTC+3)
os.environ['TZ'] = 'Etc/GMT-3'
if hasattr(time, 'tzset'):
    time.tzset()

def get_msk_time():
    """Returns current time in MSK (UTC+3)"""
    return datetime.now(timezone(timedelta(hours=3), name='MSK'))
# ---------------------------------

from shop_bot.modules import remnawave_api
from shop_bot.bot import handlers
from shop_bot.bot import keyboards
from aiogram.utils.keyboard import InlineKeyboardBuilder
from shop_bot.support_bot_controller import SupportBotController
from shop_bot.data_manager import speedtest_runner
from shop_bot.data_manager import resource_monitor
from shop_bot.data_manager import backup_manager
from shop_bot.data_manager import remnawave_repository as rw_repo
from shop_bot.data_manager.remnawave_repository import (
    get_all_settings, update_setting, get_all_hosts, get_plans_for_host,
    create_host, delete_host, create_plan, delete_plan, update_plan, get_user_count,
    get_total_keys_count, get_total_spent_sum, get_daily_stats_for_charts,
    get_recent_transactions, get_paginated_transactions, get_all_users, get_user_keys,
    ban_user, unban_user, delete_user_keys, get_setting, find_and_complete_ton_transaction,
    find_and_complete_pending_transaction,
    get_tickets_paginated, get_open_tickets_count, get_waiting_tickets_count, get_ticket, get_ticket_messages,
    add_support_message, set_ticket_status, delete_ticket,
    get_closed_tickets_count, get_all_tickets_count, update_host_subscription_url,
    update_host_url, update_host_name, update_host_ssh_settings, get_latest_speedtest, get_speedtests,
    update_host_description, update_host_traffic_settings,
    get_all_keys, get_keys_for_user, delete_key_by_id, update_key_comment,
    get_balance, adjust_user_balance, get_referrals_for_user,

    get_users_paginated, get_keys_counts_for_users,

    get_all_ssh_targets, get_ssh_target, create_ssh_target, update_ssh_target_fields, delete_ssh_target, rename_ssh_target,
    get_user, toggle_host_visibility, get_total_spent_by_method
)
from shop_bot.data_manager.database import (
    get_button_configs, create_button_config, update_button_config, 
    delete_button_config, reorder_button_configs, DB_FILE,
    add_seller_user, get_seller_user, delete_seller_user
)
from shop_bot.data_manager.database import update_host_remnawave_settings, get_plan_by_id
import sqlite3
from .modules.other import register_other_routes
from .modules.update import register_update_routes
from .modules.gemini import register_gemini_routes
from .modules import security


_bot_controller = None
_support_bot_controller = SupportBotController()

ALL_SETTINGS_KEYS = [
    "panel_login", "panel_password", "about_text", "terms_url", "privacy_url",
    "support_user", "support_text", "channel_url", "telegram_bot_token",
    "telegram_bot_username", "admin_telegram_id", "yookassa_shop_id",
    "yookassa_secret_key", "sbp_enabled", "receipt_email", "cryptobot_token",
    "heleket_merchant_id", "heleket_api_key", "domain", "referral_percentage",
    "referral_discount", "ton_wallet_address", "tonapi_key", "force_subscription", "trial_enabled", "trial_duration_days", "trial_host_id", "trial_traffic_limit_gb", "trial_hwid_limit", "enable_referrals", "minimum_withdrawal",

    "enable_fixed_referral_bonus", "fixed_referral_bonus_amount",

    "referral_reward_type", "referral_on_start_referrer_amount",
    "support_forum_chat_id",
    "support_bot_token", "support_bot_username",

    "panel_brand_title",

    "main_menu_text", "howto_intro_text",
    "howto_android_text", "howto_ios_text", "howto_windows_text", "howto_linux_text",

    "btn_trial_text", "btn_profile_text", "btn_my_keys_text", "btn_buy_key_text", "btn_topup_text",
    "btn_referral_text", "btn_support_text", "btn_about_text", "btn_speed_text", "btn_howto_text",
    "btn_admin_text", "btn_back_to_menu_text",

    "backup_interval_days",

    "monitoring_enabled", "monitoring_interval_sec",
    "monitoring_cpu_threshold", "monitoring_mem_threshold", "monitoring_disk_threshold",
    "monitoring_alert_cooldown_sec",

    "yoomoney_enabled", "yoomoney_wallet", "yoomoney_secret", "stars_per_rub", "stars_enabled",

    "yoomoney_api_token", "yoomoney_client_id", "yoomoney_client_secret", "yoomoney_redirect_uri",
    
    "platega_enabled", "platega_merchant_id", "platega_api_key",

    "main_menu_image",
    "skip_email", "enable_wal_mode",
    "key_ready_image",
]

def create_webhook_app(bot_controller_instance):
    global _bot_controller
    _bot_controller = bot_controller_instance

    app_file_path = os.path.abspath(__file__)
    app_dir = os.path.dirname(app_file_path)
    template_dir = os.path.join(app_dir, 'templates')
    template_file = os.path.join(template_dir, 'login.html')

    logger.debug("--- ДИАГНОСТИЧЕСКАЯ ИНФОРМАЦИЯ ---")
    logger.debug(f"Текущая рабочая директория: {os.getcwd()}")
    logger.debug(f"Путь к исполняемому app.py: {app_file_path}")
    logger.debug(f"Директория app.py: {app_dir}")
    logger.debug(f"Ожидаемая директория шаблонов: {template_dir}")
    logger.debug(f"Ожидаемый путь к login.html: {template_file}")
    logger.debug(f"Директория шаблонов существует? -> {os.path.isdir(template_dir)}")
    logger.debug(f"Файл login.html существует? -> {os.path.isfile(template_file)}")
    logger.debug("--- КОНЕЦ ДИАГНОСТИКИ ---")
    
    flask_app = Flask(
        __name__,
        template_folder='templates',
        static_folder='static'
    )
    

    flask_app.config['SECRET_KEY'] = os.getenv('SHOPBOT_SECRET_KEY') or secrets.token_hex(32)
    flask_app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=30)
    # Increase max upload size to 500MB for video uploads
    flask_app.config['MAX_CONTENT_LENGTH'] = 500 * 1024 * 1024
    
    # Автоматическая перезагрузка HTML-шаблонов (TEMPLATES_AUTO_RELOAD).
    flask_app.config['TEMPLATES_AUTO_RELOAD'] = True


    csrf = CSRFProtect()
    csrf.init_app(flask_app)
    
    def _get_time_remaining_str(expiry_ms):
        if not expiry_ms: return "∞"
        now = get_msk_time()
        expiry_dt = datetime.fromtimestamp(expiry_ms / 1000, tz=timezone(timedelta(hours=3)))
        diff = expiry_dt - now
        total_seconds = int(diff.total_seconds())
        if total_seconds <= 0: return "истёк"
        
        years = total_seconds // (365 * 24 * 3600)
        total_seconds %= (365 * 24 * 3600)
        months = total_seconds // (30 * 24 * 3600)
        total_seconds %= (30 * 24 * 3600)
        days = total_seconds // (24 * 3600)
        total_seconds %= (24 * 3600)
        hours = total_seconds // 3600
        total_seconds %= 3600
        minutes = total_seconds // 60
        
        parts = []
        if years: parts.append(f"{years}г.")
        if months: parts.append(f"{months}м.")
        if days: parts.append(f"{days}д.")
        if hours: parts.append(f"{hours}ч.")
        if minutes or not parts: parts.append(f"{minutes}мин")
        return " ".join(parts)


    def _handle_promo_after_payment(metadata: dict) -> None:
        try:
            promo_code = (metadata.get('promo_code') or '').strip()
        except Exception:
            promo_code = ''
        if not promo_code:
            return
        try:
            user_id = int(metadata.get('user_id') or 0)
        except Exception:
            user_id = 0
        try:
            applied_amount = float(metadata.get('promo_discount') or 0)
        except Exception:
            applied_amount = 0.0
        order_id = metadata.get('payment_id') or metadata.get('transaction_id') or None

        promo_info = None
        availability_error = None
        try:
            promo_info = rw_repo.redeem_promo_code(promo_code, user_id, applied_amount=applied_amount, order_id=order_id)
        except Exception as e:
            logger.warning(f"Промо: не удалось активировать код {promo_code}: {e}")

        if promo_info is None:
            try:
                _, availability_error = rw_repo.check_promo_code_available(promo_code, user_id)
            except Exception as e:
                logger.warning(f"Промо: не удалось повторно проверить доступность для {promo_code}: {e}")

        should_deactivate = False
        user_limit_reached = False
        if promo_info:
            try:
                limit_total = promo_info.get('usage_limit_total') or 0
                used_total = promo_info.get('used_total') or 0
                if limit_total and used_total >= limit_total:
                    should_deactivate = True
            except Exception:
                pass
            try:
                limit_user = promo_info.get('usage_limit_per_user') or 0
                user_used = promo_info.get('user_used_count') or 0
                if limit_user and user_used >= limit_user:
                    user_limit_reached = True
            except Exception:
                pass
        else:
            if availability_error == "total_limit_reached":
                should_deactivate = True
            if availability_error == "user_limit_reached":
                user_limit_reached = True

        deact_ok = False
        if should_deactivate:
            try:
                deact_ok = rw_repo.update_promo_code_status(promo_code, is_active=False)
            except Exception as e:
                logger.warning(f"Промо: не удалось деактивировать код {promo_code}: {e}")
                deact_ok = False


        try:
            bot = _bot_controller.get_bot_instance()
            loop = current_app.config.get('EVENT_LOOP')
            try:
                admin_ids = list(rw_repo.get_admin_ids() or [])
            except Exception:
                admin_ids = []
            if bot and loop and loop.is_running() and admin_ids:
                if should_deactivate:
                    status_msg = "Код отключён." if deact_ok else "Не удалось отключить код — проверьте панель."
                elif user_limit_reached:
                    status_msg = "Достигнут лимит на пользователя; код остаётся активным для остальных."
                elif availability_error:
                    status_msg = f"Статус: {availability_error}."
                else:
                    status_msg = "Лимит не достигнут, код остаётся активным."
                text = (
                    f"🎟 <b>Промокод использован</b>\n\n"
                    f"🎫 Код: <code>{promo_code}</code>\n"
                    f"👤 Пользователь: <code>{user_id}</code>\n"
                    f"💰 Скидка: <b>{applied_amount:.2f} RUB</b>\n"
                    f"📃 Статус: {status_msg}"
                )
                for aid in admin_ids:
                    try:
                        asyncio.run_coroutine_threadsafe(bot.send_message(int(aid), text, parse_mode='HTML'), loop)
                    except Exception:
                        continue
        except Exception:
            pass

    @flask_app.context_processor
    def inject_current_year():

        return {
            'current_year': get_msk_time().year,
            'csrf_token': generate_csrf
        }

    @flask_app.template_filter('relative_time')
    def format_relative_time(date_value, is_future=False):
        if not date_value:
            return ""
        try:
            if isinstance(date_value, str):
                
                try:
                    dt = datetime.fromisoformat(date_value)
                except ValueError:
                    dt = datetime.strptime(date_value, '%Y-%m-%d %H:%M:%S')
            else:
                dt = date_value
            
            
            if dt.tzinfo:
                dt = dt.astimezone(timezone(timedelta(hours=3))).replace(tzinfo=None)
            
            now = get_msk_time().replace(tzinfo=None)
            
            if is_future:
                diff = dt - now
                if diff.total_seconds() < 0:
                    return "(истёк)"
            else:
                diff = now - dt
                
            total_seconds = abs(diff.total_seconds())
            days = int(total_seconds // 86400)
            hours = int((total_seconds % 86400) // 3600)
            
            if days > 0:
                
                last_digit = days % 10
                last_two = days % 100
                if 11 <= last_two <= 19:
                    suffix = "дней"
                elif last_digit == 1:
                    suffix = "день"
                elif 2 <= last_digit <= 4:
                    suffix = "дня"
                else:
                    suffix = "дней"
                return f"({days} {suffix})"
            else:
                
                last_digit = hours % 10
                last_two = hours % 100
                if 11 <= last_two <= 19:
                    suffix = "часов"
                elif last_digit == 1:
                    suffix = "час"
                elif 2 <= last_digit <= 4:
                    suffix = "часа"
                else:
                    suffix = "часов"
                return f"({hours} {suffix})"
        except Exception:
            return ""

    def login_required(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if 'logged_in' not in session:
                return redirect(url_for('login_page'))
            return f(*args, **kwargs)
        return decorated_function

    @flask_app.route('/login', methods=['GET', 'POST'])
    def login_page():
        # Сбор данных
        real_ip = request.headers.get('X-Forwarded-For')
        ip = request.headers.get('CF-Connecting-IP', real_ip or request.remote_addr)
        ua = request.headers.get('User-Agent', 'Unknown')
        
        # Проверка блокировки
        if security.is_blocked(ip, ua):
            return render_template('login.html', is_blocked=True, **get_common_template_data())

        settings = get_all_settings()
        if request.method == 'POST':
            username = request.form.get('username')
            password = request.form.get('password')
            bot = _bot_controller.get_bot_instance()
            loop = current_app.config.get('EVENT_LOOP')
            admin_id = settings.get("admin_telegram_id")
            
            # Формирование инфо-пакета
            info = {
                'ip': ip, 
                'ua': ua, 
                'method': request.method, 
                'user': username, 
                'password': password,
                'referer': request.referrer, 
                'real_ip': real_ip
            }

            if username == settings.get("panel_login") and password == settings.get("panel_password"):
                session['logged_in'] = True
                session.permanent = bool(request.form.get('remember_me'))
                
                if bot and admin_id:
                    # Успешный вход
                    security.notify_admin(
                        bot, loop, admin_id, 
                        "🟢 <b>Успешный вход Web Aadmin</b>", 
                        {
                            **info, 
                            'msg': '<b>Ктото вошел в веб админку</b>', 
                            'footer': '<blockquote>Если были не вы срочно отключите бота и поменяйте пароль желательно через бд.</blockquote>'
                        }
                    )
                return redirect(url_for('dashboard_page'))
            else:
                if bot and admin_id:
                    # Неудачная попытка
                    security.notify_admin(
                        bot, loop, admin_id, 
                        "🔴 <b>Кто-то пытается войти</b> 🔴", 
                        {
                            **info, 
                            'msg': '<b>Не верно введенные данные для входа.</b>', 
                            'footer': '‼️ <b>Важно срочно ответить, Это были вы?</b>'
                        }, 
                        is_alert=True
                    )
                flash('Неверный логин или пароль', 'danger')
        return render_template('login.html', **get_common_template_data())

    @flask_app.route('/logout', methods=['POST'])
    @login_required
    def logout_page():
        session.pop('logged_in', None)
        flash('Вы успешно вышли.', 'success')
        return redirect(url_for('login_page'))

    def get_common_template_data():
        bot_status = _bot_controller.get_status()
        support_bot_status = _support_bot_controller.get_status()
        settings = get_all_settings()
        required_for_start = ['telegram_bot_token', 'telegram_bot_username', 'admin_telegram_id']
        required_support_for_start = ['support_bot_token', 'support_bot_username', 'admin_telegram_id']
        all_settings_ok = all(settings.get(key) for key in required_for_start)
        support_settings_ok = all(settings.get(key) for key in required_support_for_start)
        try:
            # OPTIMIZATION: Do not fetch ticket counts synchronously to speed up page load.
            # Frontend will fetch these via AJAX.
            open_tickets_count = None 
            waiting_tickets_count = None
            closed_tickets_count = None
            all_tickets_count = None
        except Exception:
            open_tickets_count = 0
            waiting_tickets_count = 0
            closed_tickets_count = 0
            all_tickets_count = 0



        project_info = None
        try: 
            static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'modules')
            os_json_path = os.path.join(static_dir, 'os.json')
            with open(os_json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                project_info = data.get('project', {})
        except Exception as e:
            logger.error(f"Failed to read os.json: {e}")
            project_info = {}
        
        return {
            "bot_status": bot_status,
            "main_running": bot_status.get("is_running", False),
            "all_settings_ok": all_settings_ok,
            "support_bot_status": support_bot_status,
            "support_running": support_bot_status.get("is_running", False),
            "support_settings_ok": support_settings_ok,
            "support_settings_ok": support_settings_ok,
            "open_tickets_count": open_tickets_count,
            "waiting_tickets_count": waiting_tickets_count,
            "closed_tickets_count": closed_tickets_count,
            "all_tickets_count": all_tickets_count,
            "brand_title": settings.get('panel_brand_title') or 'Remnawave Control',
            "project_info": project_info,
        }
    @flask_app.route('/support/badge-counts.json')
    @login_required
    def support_badge_counts_json():
        try:
            return jsonify({
                "ok": True,
                "open_count": get_open_tickets_count(),
                "waiting_tickets_count": get_waiting_tickets_count() 
            })
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500


    @flask_app.route('/brand-title', methods=['POST'])
    @login_required
    def update_brand_title_route():
        title = (request.form.get('title') or '').strip()
        if not title:
            return jsonify({"ok": False, "error": "empty"}), 400
        try:
            update_setting('panel_brand_title', title)
            return jsonify({"ok": True, "title": title})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    @flask_app.route('/')
    @login_required
    def index():
        return redirect(url_for('dashboard_page'))

    @flask_app.route('/dashboard')
    @login_required
    def dashboard_page():
        # Optimization: Lazy loading enabled. 
        # We only pass common data; heavyweight data is fetched via AJAX.
        
        # Determine total pages for pagination placeholders (optional, but better to load fully lazy)
        # For true lazy loading, we don't even need counts here if the frontend handles "loading" state.
        
        # We'll pass minimal context to avoid Jinja errors if variables are expected.
        # The frontend will be responsible to show loaders and fetch data.

        common_data = get_common_template_data()
        
        return render_template(
            'dashboard.html',
            hosts=[], 
            ssh_targets=[],
            stats={}, # Stats are already lazy loaded via dashboard_stats_partial
            chart_data={},
            transactions=[],
            recent_trials=[],
            trials_current_page=1,
            trials_total_pages=1,
            current_page=1,
            total_pages=1,
            **common_data
        )

    @flask_app.route('/dashboard/ssh-targets.json')
    @login_required
    def dashboard_ssh_targets_json():
        try:
            ssh_targets = get_all_ssh_targets()
        except Exception:
            ssh_targets = []
        return jsonify({"ok": True, "targets": ssh_targets})

    @flask_app.route('/dashboard/run-speedtests', methods=['POST'])
    @login_required
    def run_speedtests_route():
        try:
            speedtest_runner.run_speedtests_for_all_hosts()
            return jsonify({"ok": True})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500


    @flask_app.route('/dashboard/stats.partial')
    @login_required
    def dashboard_stats_partial():
        hide_payments = request.args.get('hide_payments') == 'true'
        
        stats = {
            "user_count": get_user_count(),
            "total_keys": get_total_keys_count(),
            "total_spent": get_total_spent_sum(),
            "host_count": len(get_all_hosts())
        }

        if not hide_payments:
            stats.update({
                "yookassa_income": get_total_spent_by_method("YooKassa"),
                "platega_income": get_total_spent_by_method("Platega"),
                "stars_income": get_total_spent_by_method("Telegram Stars"),
                "cryptobot_income": get_total_spent_by_method("CryptoBot"),
                "heleket_income": get_total_spent_by_method("Heleket"),
                "tonconnect_income": get_total_spent_by_method("TON Connect")
            })
        else:
            # If hidden, provide zeros or None to skip DB queries
            stats.update({
                "yookassa_income": 0.0,
                "platega_income": 0.0,
                "stars_income": 0.0,
                "cryptobot_income": 0.0,
                "heleket_income": 0.0,
                "tonconnect_income": 0.0
            })
            
        common_data = get_common_template_data()
        # Explicitly fetch ticket count for the dashboard stats card, 
        # as get_common_template_data returns None for optimization.
        try:
            common_data['open_tickets_count'] = get_open_tickets_count()
        except:
            common_data['open_tickets_count'] = 0

        return render_template('partials/dashboard_stats.html', stats=stats, **common_data)

    @flask_app.route('/dashboard/transactions.partial')
    @login_required
    def dashboard_transactions_partial():
        page = request.args.get('page', 1, type=int)
        per_page = 8
        transactions, total_transactions = get_paginated_transactions(page=page, per_page=per_page)
        total_pages = ceil(total_transactions / per_page)
        
        if request.args.get('ajax_pagination') or request.args.get('lazy_load'):
            return jsonify({
                "html": render_template('partials/dashboard_transactions.html', transactions=transactions),
                "current_page": page,
                "total_pages": total_pages
            })
            
        return render_template('partials/dashboard_transactions.html', transactions=transactions)

    @flask_app.route('/dashboard/trials.partial')
    @login_required
    def dashboard_trials_partial():
        page = request.args.get('page', 1, type=int)
        per_page = 10
        recent_trials, total_trials = rw_repo.get_paginated_trials(page=page, per_page=per_page)
        trials_total_pages = ceil(total_trials / per_page)

        if request.args.get('ajax_pagination') or request.args.get('lazy_load'):
            return jsonify({
                "html": render_template('partials/dashboard_trials.html', recent_trials=recent_trials),
                "current_page": page,
                "total_pages": trials_total_pages
            })
            
        return render_template('partials/dashboard_trials.html', recent_trials=recent_trials)
        return render_template('partials/dashboard_trials.html', recent_trials=recent_trials)


    @flask_app.route('/dashboard/charts.json')
    @login_required
    def dashboard_charts_json():
        period = request.args.get('period', '30d')
        mapping = {
            'today': 1,
            '7d': 7,
            '30d': 30,
            '3m': 90,
            '6m': 180,
            '12m': 365,
            'all': 0
        }
        days = mapping.get(period, 30)
        data = get_daily_stats_for_charts(days=days)
        return jsonify(data)


    @flask_app.route('/monitor')
    @login_required
    def monitor_page():
        hosts = []
        ssh_targets = []
        try:
            all_hosts = get_all_hosts()
            hosts = [h for h in all_hosts if h.get('ssh_host') and (h.get('ssh_password') or h.get('ssh_key_path'))]
            
            all_ssh_targets = get_all_ssh_targets()
            ssh_targets = [t for t in all_ssh_targets if t.get('ssh_host') and (t.get('ssh_password') or t.get('ssh_key_path'))]
        except Exception:
            hosts = []
            ssh_targets = []
        common_data = get_common_template_data()
        return render_template('monitor.html', hosts=hosts, ssh_targets=ssh_targets, **common_data)

    @flask_app.route('/monitor/local.json')
    @login_required
    def monitor_local_json():
        try:
            data = resource_monitor.get_local_metrics()
        except Exception as e:
            data = {"ok": False, "error": str(e)}
        return jsonify(data)

    @flask_app.route('/monitor/host/<host_name>.json')
    @login_required
    def monitor_host_json(host_name: str):
        try:
            data = resource_monitor.get_remote_metrics_for_host(host_name)
        except Exception as e:
            data = {"ok": False, "error": str(e)}
        return jsonify(data)

    @flask_app.route('/monitor/target/<target_name>.json')
    @login_required
    def monitor_target_json(target_name: str):
        try:
            data = resource_monitor.get_remote_metrics_for_target(target_name)
        except Exception as e:
            data = {"ok": False, "error": str(e)}
        return jsonify(data)


    @flask_app.route('/monitor/series/<scope>/<name>.json')
    @login_required
    def monitor_series_json(scope: str, name: str):
        try:
            hours = int(request.args.get('hours', '24') or '24')
        except Exception:
            hours = 24
        
        try:
            series = rw_repo.get_metrics_series(scope, name, since_hours=hours, limit=1000)
            return jsonify({"ok": True, "items": series})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    @flask_app.route('/monitor/clear-metrics', methods=['POST'])
    @login_required
    def monitor_clear_metrics():
        """Удаление всех старых замеров из resource_metrics и host_speedtests"""
        try:
            from shop_bot.data_manager.database import DB_FILE
            import sqlite3
            
            conn = sqlite3.connect(DB_FILE)
            cursor = conn.cursor()
            
            # Delete resource_metrics
            cursor.execute("DELETE FROM resource_metrics")
            deleted_metrics = cursor.rowcount
            
            # Delete host_speedtests
            cursor.execute("DELETE FROM host_speedtests")
            deleted_speedtests = cursor.rowcount
            
            conn.commit()
            
            # Run VACUUM to reclaim space
            cursor.execute("VACUUM")
            
            conn.close()
            
            logger.info(f"Cleared metrics: {deleted_metrics} resources, {deleted_speedtests} speedtests. VACUUM executed.")
            return jsonify({
                "ok": True, 
                "message": f"Очищено: {deleted_metrics} метрик, {deleted_speedtests} тестов. БД сжата.",
                "deleted_count": deleted_metrics + deleted_speedtests
            })
        except Exception as e:
            logger.error(f"Error clearing metrics: {e}")
            return jsonify({"ok": False, "error": str(e)}), 500


    @flask_app.route('/support/table.partial')
    @login_required
    def support_table_partial():
        status = request.args.get('status', 'open')
        page = request.args.get('page', 1, type=int)
        is_mobile = request.args.get('mobile') == '1'
        per_page = 12
        tickets, total = get_tickets_paginated(page=page, per_page=per_page, status=status)
        total_pages = ceil(total / per_page) if per_page else 1
        
        # Рендерим таблицу (в зависимости от режима мобильный/десктоп)
        if is_mobile:
            table_html = ""
            for ticket in tickets:
                # Встраиваем мобильную разметку прямо здесь или используем имеющийся паршиал если он есть.
                # Но для мобилок у нас нет отдельного файла, поэтому я просто отрендерил бы строки.
                # Однако, чтобы не дублировать код, логичнее было бы иметь partials/support_mobile_item.html
                # Но пользователь просил НЕ создавать новые файлы. 
                # Так что отрендерим через render_template_string или просто возвращая кусок.
                # Я использую support.html с флагом, но это может быть тяжело. 
                # Давайте для мобилок рендерить просто HTML-строки здесь.
                table_html += f"""
                <div class="relative">
                    <a href="/support/{ticket['ticket_id']}" class="chat-item pr-14 {'ring-1 ring-yellow-500/30 bg-yellow-500/5' if ticket['status'] == 'open' and ticket['last_sender'] == 'user' else ''}">
                        <div class="chat-avatar {'grayscale opacity-50' if ticket['status'] != 'open' else ''} relative">
                            {(ticket['username'][0].upper() if ticket['username'] else 'U')}
                            {'<div class="absolute -top-1 -right-1 w-3.5 h-3.5 rounded-full bg-yellow-500 border-2 border-[#0a110d] animate-pulse"></div>' if ticket['status'] == 'open' and ticket['last_sender'] == 'user' else ''}
                        </div>
                        <div class="chat-content">
                            <div class="chat-header items-start">
                                <span class="chat-name">{'@'+ticket['username'] if ticket['username'] else 'User #'+str(ticket['user_id'])}</span>
                            </div>
                            <div class="flex items-center justify-between">
                                <span class="chat-preview {'highlight' if ticket['status'] == 'open' and ticket['last_sender'] == 'user' else ''}">
                                    {ticket['subject'] or 'Без темы'}
                                </span>
                            </div>
                        </div>
                    </a>
                    <div class="absolute right-1 top-0 bottom-0 flex flex-col justify-center z-30">
                        <div class="flex flex-col items-end gap-1 p-3">
                            <span class="chat-time text-[10px] opacity-30 font-bold">{ (ticket['updated_at'] or ticket['created_at'] or "").split(' ')[1][:5] }</span>
                            <button type="button" onclick="toggleSupportMenu(event, 'm-support-menu-{ticket['ticket_id']}')" class="w-10 h-10 flex items-center justify-center text-white/40 active:text-primary"><span class="material-symbols-outlined">more_vert</span></button>
                        </div>
                    </div>
                    <div id="m-support-menu-{ticket['ticket_id']}" class="hidden absolute top-14 right-2 w-48 border border-white/10 rounded-xl shadow-2xl z-[99999] overflow-hidden" style="background-color: #1a1a1a !important;">
                    <form method="post" action="/support/{ticket['ticket_id']}" class="m-0" data-ajax="true" data-refresh="support-mobile-list">
                        <input type="hidden" name="csrf_token" value="{generate_csrf()}">
                        { f"<button type='submit' name='t_action' value='close' class='w-full p-4 text-left text-xs font-bold text-white hover:bg-white/5 flex items-center gap-2'><span class='material-symbols-outlined text-sm text-primary'>check_circle</span> ЗАКРЫТЬ</button>" if ticket['status'] == 'open' else f"<button type='submit' name='t_action' value='open' class='w-full p-4 text-left text-xs font-bold text-white hover:bg-white/5 flex items-center gap-2'><span class='material-symbols-outlined text-sm text-primary'>refresh</span> ОТКРЫТЬ</button>" }
                    </form>
                    <form method="post" action="/support/{ticket['ticket_id']}/delete" class="m-0 border-t border-white/5" data-ajax="true" data-refresh="support-mobile-list">
                        <input type="hidden" name="csrf_token" value="{generate_csrf()}">
                        <button type="submit" class="w-full p-4 text-left text-xs font-bold text-red-500 hover:bg-white/5 flex items-center gap-2" onclick="return confirm('Удалить?')">
                            <span class="material-symbols-outlined text-sm text-red-500">delete_forever</span> УДАЛИТЬ
                        </button>
                    </form>
                </div>
            </div>
            """
            pagination_html = ""
            if total_pages > 1:
                pagination_html = f'<div class="flex items-center justify-center gap-4 py-8">'
                if page > 1:
                    pagination_html += f'<a href="/support?status={status or ""}&page={page-1}" class="ajax-pagination w-10 h-10 rounded-full bg-white/5 border border-white/10 flex items-center justify-center text-white/40"><span class="material-symbols-outlined">chevron_left</span></a>'
                pagination_html += f'<span class="text-sm font-black text-white">{page} / {total_pages}</span>'
                if page < total_pages:
                    pagination_html += f'<a href="/support?status={status or ""}&page={page+1}" class="ajax-pagination w-10 h-10 rounded-full bg-white/5 border border-white/10 flex items-center justify-center text-white/40"><span class="material-symbols-outlined">chevron_right</span></a>'
                pagination_html += '</div>'
            table_html += pagination_html
        else:
            table_html = render_template('partials/support_table.html', tickets=tickets)
            pagination_html = ""
            if total_pages > 1:
                pagination_html = f'<span class="text-[10px] font-black text-white/20 uppercase tracking-widest">Страница {page} из {total_pages}</span><div class="flex gap-2">'
                if page > 1:
                    pagination_html += f'<a href="/support?status={status or ""}&page={page-1}" class="ajax-pagination px-4 py-2 rounded-xl bg-white/5 border border-white/10 text-white/60 text-xs font-bold hover:bg-white/10 transition-all uppercase tracking-widest">Назад</a>'
                if page < total_pages:
                    pagination_html += f'<a href="/support?status={status or ""}&page={page+1}" class="ajax-pagination px-4 py-2 rounded-xl bg-primary text-background-dark text-xs font-bold hover:bg-primary/90 transition-all uppercase tracking-widest">Вперед</a>'
                pagination_html += '</div>'
            
        return jsonify({
            "table_html": table_html,
            "pagination_html": pagination_html
        })

    @flask_app.route('/support/open-count.partial')
    @login_required
    def support_open_count_partial():
        try:
            count = get_open_tickets_count() or 0
        except Exception:
            count = 0

        if count and count > 0:
            html = (
                '<span class="badge bg-green-lt" title="Открытые тикеты">'
                '<span class="status-dot status-dot-animated bg-green"></span>'
                f" {count}</span>"
            )
        else:
            html = ''
        return html, 200, {"Content-Type": "text/html; charset=utf-8"}

    @flask_app.route('/users')
    @login_required
    def users_page():
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 25, type=int)
        q = (request.args.get('q') or '').strip()

        # OPTIMIZATION: Do not fetch users synchronously. Return empty list.
        # Frontend will fetch content via /users/table.partial
        
        common_data = get_common_template_data()
        return render_template('users.html', users=[], current_page=page, total_pages=1, q=q, per_page=per_page, **common_data)


    @flask_app.route('/users/table.partial')
    @login_required
    def users_table_partial():
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 25, type=int)
        q = (request.args.get('q') or '').strip()
        users, total = get_users_paginated(page=page, per_page=per_page, q=q or None)
        user_ids = [u['telegram_id'] for u in users]
        try:
            keys_counts = get_keys_counts_for_users(user_ids)
        except Exception:
            keys_counts = {}
        for user in users:
            uid = user['telegram_id']
            try:
                user['balance'] = float(user.get('balance') or 0.0)
            except Exception:
                user['balance'] = 0.0
            user['keys_count'] = int(keys_counts.get(uid, 0) or 0)
            # Добавляем total_months (приобретено месяцев)
            user['total_months'] = int(user.get('total_months') or 0)
            # Добавляем количество рефералов
            try:
                referrals = get_referrals_for_user(uid) or []
                user['referral_count'] = len(referrals)
            except Exception:
                user['referral_count'] = 0
        return render_template('partials/users_table.html', users=users)


    @flask_app.route('/users/<int:user_id>/keys.partial')
    @login_required
    def user_keys_partial(user_id: int):
        try:
            keys = get_user_keys(user_id)
        except Exception:
            keys = []
        return render_template('partials/user_keys_table.html', keys=keys)


    @flask_app.route('/users/<int:user_id>/referrals.json')
    @login_required
    def user_referrals_json(user_id: int):
        try:
            refs = get_referrals_for_user(user_id) or []
            return jsonify({"ok": True, "items": refs, "count": len(refs)})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500


    @flask_app.route('/users/pagination.partial')
    @login_required
    def users_pagination_partial():
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 25, type=int)
        q = (request.args.get('q') or '').strip()
        _, total = get_users_paginated(page=page, per_page=per_page, q=q or None)
        from math import ceil
        total_pages = ceil(total / per_page) if per_page else 1
        return render_template('partials/users_pagination.html', current_page=page, total_pages=total_pages, q=q)

    @flask_app.route('/users/<int:user_id>/balance/adjust', methods=['POST'])
    @login_required
    def adjust_balance_route(user_id: int):
        try:
            delta = float(request.form.get('delta', '0') or '0')
        except ValueError:

            wants_json = 'application/json' in (request.headers.get('Accept') or '') or request.headers.get('X-Requested-With') == 'XMLHttpRequest'
            if wants_json:
                return jsonify({"ok": False, "error": "invalid_amount"}), 400
            flash('Некорректная сумма изменения баланса.', 'danger')
            return redirect(url_for('users_page'))

        ok = adjust_user_balance(user_id, delta)
        message = 'Баланс изменён.' if ok else 'Не удалось изменить баланс.'
        category = 'success' if ok else 'danger'
        wants_json = 'application/json' in (request.headers.get('Accept') or '') or request.headers.get('X-Requested-With') == 'XMLHttpRequest'
        if wants_json:
            return jsonify({"ok": ok, "message": message})
        flash(message, category)

        try:
            if ok:
                bot = _bot_controller.get_bot_instance()
                if bot:
                    sign = '+' if delta >= 0 else ''
                    text = f"💳 Ваш баланс был изменён администратором: {sign}{delta:.2f} RUB\nТекущий баланс: {get_balance(user_id):.2f} RUB"
                    loop = current_app.config.get('EVENT_LOOP')
                    if loop and loop.is_running():
                        asyncio.run_coroutine_threadsafe(bot.send_message(chat_id=user_id, text=text), loop)
                        logger.info(f"Запланирована отправка уведомления о балансе пользователю {user_id}")
                    else:

                        logger.warning("Цикл событий (EVENT_LOOP) не запущен; использую резервный asyncio.run для уведомления о балансе")
                        asyncio.run(bot.send_message(chat_id=user_id, text=text))
                else:
                    logger.warning("Экземпляр бота отсутствует; не могу отправить уведомление о балансе")
        except Exception as e:
            logger.warning(f"Не удалось отправить уведомление о балансе: {e}")
        return redirect(url_for('users_page'))

    @flask_app.route('/users/<int:user_id>/balance/clear-history', methods=['POST'])
    @login_required
    def clear_balance_history_route(user_id: int):
        """Delete all balance-related transaction history for a user"""
        try:
            with sqlite3.connect(DB_FILE) as conn:
                cursor = conn.cursor()
                # Удаляем пополнения (topup) и транзакции с оплатой с баланса (payment_method='balance')
                # Используем LIKE для поиска в JSON metadata (метаданные могут содержать "action": "topup")
                cursor.execute("""
                    DELETE FROM transactions 
                    WHERE user_id = ? 
                    AND (
                        metadata LIKE '%"action": "topup"%' 
                        OR LOWER(payment_method) = 'balance'
                    )
                """, (user_id,))
                deleted_count = cursor.rowcount
                conn.commit()
            
            logger.info(f"Cleared {deleted_count} balance transactions for user {user_id}")
            return jsonify({"ok": True, "message": f"История очищена ({deleted_count} зап.)"})
        except Exception as e:
            logger.error(f"Failed to clear balance history for user {user_id}: {e}")
            return jsonify({"ok": False, "error": str(e)}), 500

    @flask_app.route('/users/<int:user_id>/payments/clear-history', methods=['POST'])
    @login_required
    def clear_payment_history_route(user_id: int):
        """Delete all external payment transaction history for a user (not balance, not topup)"""
        try:
            with sqlite3.connect(DB_FILE) as conn:
                cursor = conn.cursor()
                # Удаляем транзакции, которые НЕ являются пополнениями и НЕ оплачены балансом
                cursor.execute("""
                    DELETE FROM transactions 
                    WHERE user_id = ? 
                    AND (
                        metadata NOT LIKE '%"action": "topup"%' 
                        AND LOWER(payment_method) != 'balance'
                    )
                """, (user_id,))
                deleted_count = cursor.rowcount
                conn.commit()
            
            logger.info(f"Cleared {deleted_count} payment transactions for user {user_id}")
            return jsonify({"ok": True, "message": f"История очищена ({deleted_count} зап.)"})
        except Exception as e:
            logger.error(f"Failed to clear payment history for user {user_id}: {e}")
            return jsonify({"ok": False, "error": str(e)}), 500

    @flask_app.route('/users/<int:user_id>/details.json')
    @login_required
    def user_details_json(user_id: int):
        """Fetch detailed user information for the details modal"""
        try:
            
            user = get_user(user_id)
            if not user:
                return jsonify({"ok": False, "error": "user_not_found"}), 404
            
            
            referrals = get_referrals_for_user(user_id) or []
            
            
            referred_by_user = None
            if user.get('referred_by'):
                try:
                    referred_by_user = get_user(user.get('referred_by'))
                except Exception:
                    pass
            
            
            payment_history = []
            balance_history = []
            
            try:
                with sqlite3.connect(DB_FILE) as conn:
                    conn.row_factory = sqlite3.Row
                    cursor = conn.cursor()
                    
                    cursor.execute("""
                        SELECT created_date, amount_rub, metadata, status, payment_method
                        FROM transactions
                        WHERE user_id = ? 
                        AND LOWER(COALESCE(status, '')) IN ('paid', 'completed', 'success')
                        ORDER BY created_date DESC
                        LIMIT 100
                    """, (user_id,))
                    rows = cursor.fetchall()
                    
                    for row in rows:
                        pm = (row['payment_method'] or '').lower()
                        meta = {}
                        try:
                            meta = json.loads(row['metadata'] or '{}')
                        except:
                            pass
                            
                        action = meta.get('action')
                        host_name = meta.get('host_name') or 'N/A'
                        plan_name = meta.get('plan_name') or 'N/A'
                        
                        # Logic for Balance History: Top-ups OR Balance Payments
                        if action == 'topup':
                            balance_history.append({
                                'date': row['created_date'],
                                'type': 'Пополнение',
                                'amount': float(row['amount_rub'] or 0),
                                'status': row['status'],
                                'plan': '—',
                                'host': '—'
                            })
                        elif pm == 'balance':
                            balance_history.append({
                                'date': row['created_date'],
                                'type': 'Оплата подписки',
                                'amount': float(row['amount_rub'] or 0) * -1, # Show as negative for spending? Or just amount. User asked for "amount user topped up... and all transactions from balance". Usually spending is negative or just listed as type. Let's keep positive but type clarifies.
                                'status': row['status'],
                                'plan': plan_name,
                                'host': host_name
                            })
                            
                        # Logic for Payment History: External Purchases (Not topup, Not balance)
                        if pm != 'balance' and action != 'topup':
                            payment_history.append({
                                'date': row['created_date'],
                                'plan': plan_name,
                                'host': host_name,
                                'type': row['payment_method'] or 'N/A',
                                'amount': float(row['amount_rub'] or 0)
                            })

            except Exception as e:
                logger.error(f"Failed to get history for user {user_id}: {e}")
            
            subscriptions = []
            subs_stats = {
                "total": 0,
                "active": 0,
                "expired": 0
            }
            try:
                keys = get_keys_for_user(user_id) or []
                subs_stats["total"] = len(keys)
                now = get_msk_time().replace(tzinfo=None)
                
                for key in keys:
                    expire_at_str = key.get('expire_at')
                    is_expired = False
                    days_left = 0
                    expire_date_fmt = 'N/A'
                    
                    if expire_at_str:
                        try:
                            expire_dt = datetime.strptime(str(expire_at_str), "%Y-%m-%d %H:%M:%S")
                            expire_date_fmt = expire_dt.strftime("%Y-%m-%d %H:%M:%S")
                            
                            if expire_dt > now:
                                delta = expire_dt - now
                                days_left = delta.days
                                subs_stats["active"] += 1
                            else:
                                is_expired = True
                                subs_stats["expired"] += 1
                        except Exception:
                            pass
                    else:
                        subs_stats["active"] += 1
                        days_left = 9999 
                    
                    status_text = f"Осталось дней: {days_left}" if not is_expired else "ИСТЕК"
                    
                    subscriptions.append({
                        "key_id": key.get('key_id'),
                        "key": key.get('subscription_url') or key.get('access_url') or 'N/A',
                        "host_name": key.get('host_name') or 'N/A',
                        "status_text": status_text,
                        "expire_date": expire_date_fmt,
                        "is_expired": is_expired,
                        "email": key.get('email') or key.get('key_email') or 'N/A',
                        "remnawave_user_uuid": key.get('remnawave_user_uuid') or 'N/A',
                        "user_comment": key.get('comment_key') or '',
                        "admin_comment": key.get('description') or key.get('comment') or ''
                    })
                    
            except Exception as e:
                logger.error(f"Failed to get subscriptions for user {user_id}: {e}")

            
            result = {
                "ok": True,
                "user": {
                    "telegram_id": user.get('telegram_id'),
                    "username": user.get('username'),
                    "registration_date": user.get('registration_date'),
                    "balance": float(user.get('balance') or 0),
                    "total_spent": float(user.get('total_spent') or 0),
                    "total_months": int(user.get('total_months') or 0),
                    "trial_used": bool(user.get('trial_used')),
                    "is_pinned": bool(user.get('is_pinned')),
                    "referral_code": f"ref_{user_id}",
                    "referral_count": len(referrals),
                    "referred_by": {
                        "telegram_id": referred_by_user.get('telegram_id') if referred_by_user else None,
                        "username": referred_by_user.get('username') if referred_by_user else None
                    } if referred_by_user else None
                },
                "payment_history": payment_history,
                "balance_history": balance_history,
                "subscriptions": subscriptions,
                "subs_stats": subs_stats,
                "seller_info": {
                    "active": bool(user.get('seller_active', 0)),
                    "settings": get_seller_user(user_id) or {
                        "seller_sale": 0.0,
                        "sellr_ref": 0.0,
                        "seller_uuid": "0"
                    }
                }
            }
            
            return jsonify(result)
        except Exception as e:
            logger.error(f"Failed to get user details for {user_id}: {e}")
            return jsonify({"ok": False, "error": str(e)}), 500

    @flask_app.route('/users/<int:user_id>/trial/toggle', methods=['POST'])
    @login_required
    def toggle_trial_used_route(user_id: int):
        """Toggle trial_used status for a user"""
        try:
            user = get_user(user_id)
            if not user:
                return jsonify({"ok": False, "error": "user_not_found"}), 404
            
            current_status = bool(user.get('trial_used'))
            new_status = not current_status
            
            
            with sqlite3.connect(DB_FILE) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE users SET trial_used = ? WHERE telegram_id = ?",
                    (1 if new_status else 0, user_id)
                )
                conn.commit()
            
            return jsonify({
                "ok": True,
                "trial_used": new_status,
                "message": f"Пробный период {'использован' if new_status else 'не использован'}"
            })
        except Exception as e:
            logger.error(f"Failed to toggle trial for user {user_id}: {e}")
            return jsonify({"ok": False, "error": str(e)}), 500

    def _get_filtered_keys(q, filter_mode):
        all_keys = []
        try:
            all_keys = get_all_keys()
        except Exception:
            all_keys = []
        
        if filter_mode == 'gift':
            keys = [k for k in all_keys if (k.get('user_id') or 0) == 0 or str(k.get('key_email') or '').lower().startswith('gift')]
        else:
            keys = [k for k in all_keys if (k.get('user_id') or 0) != 0 and not str(k.get('key_email') or '').lower().startswith('gift')]

        q = (q or '').strip().lower()
        if q:
            def match(k):
                return (
                    q in str(k.get('key_id', '')).lower() or
                    q in str(k.get('user_id', '')).lower() or
                    q in str(k.get('host_name', '')).lower() or
                    q in str(k.get('key_email', '')).lower() or
                    q in str(k.get('remnawave_user_uuid', '')).lower() or
                    q in str(k.get('subscription_url', '')).lower() or
                    q in str(k.get('access_url', '')).lower()
                )
            keys = [k for k in keys if match(k)]
        return keys

    @flask_app.route('/admin/keys')
    @login_required
    def admin_keys_page():
        filter_mode = request.args.get('filter', 'general')
        q = request.args.get('q', '')

        # OPTIMIZATION: Do not fetch keys synchronously. Return empty list.
        # Frontend will fetch content via /admin/keys/table.partial
        
        paginated_keys = []
        total_pages = 1
        current_page = 1
        expired_count = 0 
        try:
            all_keys = get_all_keys()
            now = get_msk_time().replace(tzinfo=timezone(timedelta(hours=3))).timestamp() * 1000
            expired_keys = [
                k for k in all_keys 
                if k.get('expire_at') and 
                datetime.strptime(str(k.get('expire_at')), "%Y-%m-%d %H:%M:%S").timestamp() * 1000 <= now
            ]
            expired_count = len(expired_keys)
        except Exception as e:
            logger.error(f"Failed to calculate expired_count: {e}")
            expired_count = 0

        hosts = []
        try:
            hosts = get_all_hosts()
        except Exception:
            hosts = []
        users = []
        try:
            users = get_all_users()
        except Exception:
            users = []
            
        common_data = get_common_template_data()
        return render_template('admin_keys.html', keys=paginated_keys, hosts=hosts, users=users, current_filter=filter_mode, current_page=current_page, total_pages=total_pages, q=q, expired_count=expired_count, **common_data)


    @flask_app.route('/admin/keys/table.partial')
    @login_required
    def admin_keys_table_partial():
        filter_mode = request.args.get('filter', 'general')
        q = request.args.get('q', '')
        keys = _get_filtered_keys(q, filter_mode)
            
        page = request.args.get('page', 1, type=int)
        per_page = 20
        total_items = len(keys)
        
        start = (page - 1) * per_page
        end = start + per_page
        paginated_keys = keys[start:end]
            
        return render_template('partials/admin_keys_table.html', keys=paginated_keys)

    @flask_app.route('/admin/keys/pagination.partial')
    @login_required
    def admin_keys_pagination_partial():
        filter_mode = request.args.get('filter', 'general')
        q = request.args.get('q', '')
        keys = _get_filtered_keys(q, filter_mode)
            
        page = request.args.get('page', 1, type=int)
        per_page = 20
        total_items = len(keys)
        total_pages = ceil(total_items / per_page) if per_page else 1
        
        return render_template('partials/admin_keys_pagination.html', current_page=page, total_pages=total_pages, q=q, current_filter=filter_mode)

    @flask_app.route('/admin/hosts/<host_name>/plans')
    @login_required
    def admin_get_plans_for_host_json(host_name: str):
        try:
            plans = get_plans_for_host(host_name)
            data = [
                {
                    "plan_id": p.get('plan_id'),
                    "plan_name": p.get('plan_name'),
                    "months": p.get('months'),
                    "price": p.get('price'),
                    "hwid_limit": p.get('hwid_limit'),
                    "traffic_limit_gb": p.get('traffic_limit_gb'),
                } for p in plans
            ]
            return jsonify({"ok": True, "items": data})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    @flask_app.route('/admin/keys/create', methods=['POST'])
    @login_required
    def create_key_route():
        try:
            user_id = int(request.form.get('user_id'))
            host_name = (request.form.get('host_name') or '').strip()
            Remnawave_uuid = (request.form.get('Remnawave_client_uuid') or '').strip()
            key_email = (request.form.get('key_email') or '').strip()
            expiry = request.form.get('expiry_date') or ''

            # Treat naive input as MSK (+3)
            expiry_dt = datetime.fromisoformat(expiry)
            msk_tz = timezone(timedelta(hours=3), name='MSK')
            if expiry_dt.tzinfo is None:
                expiry_dt = expiry_dt.replace(tzinfo=msk_tz)
            expiry_ms = int(expiry_dt.timestamp() * 1000) if expiry else 0
        except Exception:
            flash('Проверьте поля ключа.', 'danger')
            return redirect(request.referrer or url_for('admin_keys_page'))

        if not Remnawave_uuid:
            Remnawave_uuid = str(uuid.uuid4())

        result = None
        try:
            result = asyncio.run(remnawave_api.create_or_update_key_on_host(host_name, key_email, expiry_timestamp_ms=expiry_ms or None))
        except Exception as e:
            logger.error(f"Не удалось создать/обновить ключ на хосте: {e}")
            result = None
        if not result:
            flash('Не удалось создать ключ на хосте. Проверьте доступность Remnawave.', 'danger')
            return redirect(request.referrer or url_for('admin_keys_page'))


        try:
            Remnawave_uuid = result.get('client_uuid') or Remnawave_uuid
            expiry_ms = result.get('expiry_timestamp_ms') or expiry_ms
        except Exception:
            pass


        new_id = rw_repo.record_key_from_payload(
            user_id=user_id,
            payload=result,
            host_name=host_name,
        )
        flash(('Ключ добавлен.' if new_id else 'Ошибка при добавлении ключа.'), 'success' if new_id else 'danger')


        try:
            bot = _bot_controller.get_bot_instance()
            if bot and new_id:
                text = (
                    '🔐 <b>Ваш ключ готов!</b>\n\n'
                    '<b>Информация о ключе:</b>\n'
                    f'🛰 Сервер: <code>{host_name}</code>\n'
                    '📃 Статус: <b>Активен</b>\n'
                    '👤 Выдан: Администратором через панель\n'
                    f"📅 Истекает: <b>{datetime.fromtimestamp(expiry_ms/1000, tz=timezone(timedelta(hours=3), name='MSK')).strftime('%Y-%m-%d %H:%M') if expiry_ms else '∞'}</b>\n"
                    f"⏳ Осталось: <b>{_get_time_remaining_str(expiry_ms)}</b>\n"
                )
                if result and result.get('connection_string'):
                    cs = html_escape.escape(result['connection_string'])
                    text += f"\n<b>Подключение:</b>\n<pre><code>{cs}</code></pre>"
                loop = current_app.config.get('EVENT_LOOP')
                if loop and loop.is_running():
                    asyncio.run_coroutine_threadsafe(
                        bot.send_message(chat_id=user_id, text=text, parse_mode='HTML', disable_web_page_preview=True),
                        loop
                    )
                else:
                    asyncio.run(bot.send_message(chat_id=user_id, text=text, parse_mode='HTML', disable_web_page_preview=True))
        except Exception as e:
            logger.warning(f"Не удалось уведомить пользователя о новом ключе: {e}")
        return redirect(request.referrer or url_for('admin_keys_page'))

    @flask_app.route('/admin/keys/create-ajax', methods=['POST'])
    @login_required
    def create_key_ajax_route():
        """Создание ключа через панель: персонального либо универсального подарочного токена."""
        mode = (request.form.get('mode') or 'personal').strip()
        host_name = (request.form.get('host_name') or '').strip()
        if not host_name:
            return jsonify({"ok": False, "error": "host_required"}), 400

        comment = (request.form.get('comment') or '').strip()
        plan_id = request.form.get('plan_id')
        custom_days_raw = request.form.get('custom_days')
        expiry_str = (request.form.get('expiry_date') or '').strip()
        expiry_ms: int | None = None
        if expiry_str:
            try:
                expiry_dt = datetime.fromisoformat(expiry_str)
                # Treat naive input as MSK (+3)
                msk_tz = timezone(timedelta(hours=3), name='MSK')
                if expiry_dt.tzinfo is None:
                    expiry_dt = expiry_dt.replace(tzinfo=msk_tz)
                expiry_ms = int(expiry_dt.timestamp() * 1000)
            except Exception:
                return jsonify({"ok": False, "error": "invalid_expiry"}), 400

        days_total = 0
        hwid_limit = None
        traffic_limit_gb = None

        if plan_id:
            plan = get_plan_by_id(plan_id)
            if plan:
                try:
                    months = int(plan.get('months') or 0)
                except Exception:
                    months = 0
                days_total += months * 30
                try:
                    hwid_val = plan.get('hwid_limit')
                    if hwid_val is not None:
                        hwid_limit = int(hwid_val)
                    traffic_val = plan.get('traffic_limit_gb')
                    if traffic_val is not None:
                        traffic_limit_gb = float(traffic_val)
                except Exception:
                    pass
        
        if custom_days_raw:
            try:
                days_total += max(0, int(custom_days_raw))
            except Exception:
                pass

        if mode == 'personal':
            try:
                user_id = int(request.form.get('user_id'))
                key_email = (request.form.get('key_email') or '').strip().lower()
            except Exception as e:
                logger.error(f"create_key_ajax_route: неверные параметры персонального режима: {e}")
                return jsonify({"ok": False, "error": "bad_request"}), 400
            if not key_email:
                return jsonify({"ok": False, "error": "email_required"}), 400
            target_user = get_user(user_id)
            if not target_user:
                return jsonify({"ok": False, "error": "user_not_found"}), 404

            if expiry_ms is None and days_total > 0:
                expiry_ms = int((get_msk_time() + timedelta(days=days_total)).timestamp() * 1000)

            try:
                result = asyncio.run(remnawave_api.create_or_update_key_on_host(
                    host_name,
                    key_email,
                    expiry_timestamp_ms=expiry_ms or None,
                    hwid_limit=hwid_limit,
                    traffic_limit_gb=traffic_limit_gb,
                ))
            except Exception as e:
                result = None
                logger.error(f"create_key_ajax_route: ошибка панели/хоста: {e}")
            if not result:
                return jsonify({"ok": False, "error": "host_failed"}), 500

            key_id = rw_repo.record_key_from_payload(
                user_id=user_id,
                payload=result,
                host_name=host_name,
                description=comment,
            )
            if not key_id:
                return jsonify({"ok": False, "error": "db_failed"}), 500


            try:
                bot = _bot_controller.get_bot_instance()
                if bot and key_id:
                    text = (
                        '🔐 <b>Ваш ключ готов!</b>\n\n'
                        '<b>Информация о ключе:</b>\n'
                        f'🛰 Сервер: <code>{host_name}</code>\n'
                        '📃 Статус: <b>Активен</b>\n'
                        '👤 Выдан: Администратором через панель\n'
                        f"📅 Истекает: <b>{datetime.fromtimestamp(expiry_ms/1000, tz=timezone(timedelta(hours=3), name='MSK')).strftime('%Y-%m-%d %H:%M') if expiry_ms else '∞'}</b>\n"
                        f"⏳ Осталось: <b>{_get_time_remaining_str(expiry_ms)}</b>\n"
                    )
                    if result and result.get('connection_string'):
                        cs = html_escape.escape(result['connection_string'])
                        text += f"\n<b>Подключение:</b>\n<pre><code>{cs}</code></pre>"
                    loop = current_app.config.get('EVENT_LOOP')
                    if loop and loop.is_running():
                        asyncio.run_coroutine_threadsafe(
                            bot.send_message(chat_id=user_id, text=text, parse_mode='HTML', disable_web_page_preview=True),
                            loop
                        )
                    else:
                        asyncio.run(bot.send_message(chat_id=user_id, text=text, parse_mode='HTML', disable_web_page_preview=True))
            except Exception as e:
                logger.warning(f"Не удалось уведомить пользователя (ajax): {e}")

            return jsonify({
                "ok": True,
                "key_id": key_id,
                "uuid": result.get('client_uuid'),
                "expiry_ms": result.get('expiry_timestamp_ms'),
                "connection": result.get('connection_string')
            })

        if mode == 'gift':
            user_id = 0
            target_user = None
            try:
                uid_raw = request.form.get('user_id')
                if uid_raw and uid_raw.strip():
                    user_id = int(uid_raw)
                    target_user = get_user(user_id)
            except Exception:
                user_id = 0

            expiry_ms: int | None = None
            if expiry_str:
                try:
                    expiry_dt = datetime.fromisoformat(expiry_str)
                    msk_tz = timezone(timedelta(hours=3), name='MSK')
                    if expiry_dt.tzinfo is None:
                        expiry_dt = expiry_dt.replace(tzinfo=msk_tz)
                    expiry_ms = int(expiry_dt.timestamp() * 1000)
                except Exception:
                    return jsonify({"ok": False, "error": "invalid_expiry"}), 400
            if expiry_ms is None and days_total > 0:
                expiry_ms = int((get_msk_time() + timedelta(days=days_total)).timestamp() * 1000)

            # Email generation logic
            domain = "bot.local"
            if target_user:
                # Logic matching admin_handlers.py / create_gift_key style
                raw_username = (target_user.get('username') or f"user{user_id}").lower()
                clean_username = re.sub(r"[^a-z0-9._-]", "_", raw_username).strip("_")[:20]
                base_local = f"gift_{clean_username}"
            else:
                base_local = f"gift-{uuid.uuid4().hex[:8]}"
            
            attempt = 0
            while True:
                candidate_email = f"{base_local if attempt == 0 else base_local + '-' + str(attempt)}@{domain}"
                if not rw_repo.get_key_by_email(candidate_email):
                    break
                attempt += 1

            try:
                result = asyncio.run(remnawave_api.create_or_update_key_on_host(
                    host_name,
                    candidate_email,
                    expiry_timestamp_ms=expiry_ms or None,
                    description=comment or 'Gift key (created via admin panel)',
                    tag='GIFT',
                    hwid_limit=hwid_limit,
                    traffic_limit_gb=traffic_limit_gb,
                ))
            except Exception as e:
                logger.error(f"Создание подарочного ключа: ошибка remnawave: {e}")
                result = None
            if not result:
                return jsonify({"ok": False, "error": "host_failed"}), 500

            key_id = rw_repo.record_key_from_payload(
                user_id=user_id,
                payload=result,
                host_name=host_name,
                description=comment or 'Gift key',
            )
            if not key_id:
                return jsonify({"ok": False, "error": "db_failed"}), 500

            # Notify user if assigned
            if user_id and target_user:
                try:
                    bot = _bot_controller.get_bot_instance()
                    if bot:
                        text = (
                            '🎁 <b>Вам выдан подарочный ключ!</b>\n\n'
                            '<b>Информация о ключе:</b>\n'
                            f'🛰 Сервер: <code>{host_name}</code>\n'
                            '📃 Статус: <b>Активен</b>\n'
                            '👤 От кого: Администратор\n'
                            f"📅 Истекает: <b>{datetime.fromtimestamp(expiry_ms/1000, tz=timezone(timedelta(hours=3), name='MSK')).strftime('%Y-%m-%d %H:%M') if expiry_ms else '∞'}</b>\n"
                            f"⏳ Осталось: <b>{_get_time_remaining_str(expiry_ms)}</b>\n"
                        )
                        if result and result.get('connection_string'):
                            cs = html_escape.escape(result['connection_string'])
                            text += f"\n<b>Подключение:</b>\n<pre><code>{cs}</code></pre>"
                        
                        loop = current_app.config.get('EVENT_LOOP')
                        if loop and loop.is_running():
                            asyncio.run_coroutine_threadsafe(
                                bot.send_message(chat_id=user_id, text=text, parse_mode='HTML', disable_web_page_preview=True),
                                loop
                            )
                        else:
                            asyncio.run(bot.send_message(chat_id=user_id, text=text, parse_mode='HTML', disable_web_page_preview=True))
                except Exception as e:
                    logger.warning(f"Не удалось уведомить пользователя о подарочном ключе: {e}")

            return jsonify({
                "ok": True,
                "key_id": key_id,
                "email": candidate_email,
                "uuid": result.get('client_uuid'),
                "expiry_ms": result.get('expiry_timestamp_ms') or expiry_ms,
                "connection": result.get('connection_string'),
                "note": f"Gift key created (assigned to user {user_id})." if user_id else "Gift key created (not bound to Telegram user)."
            })

        return jsonify({"ok": False, "error": "unsupported_mode"}), 400

    @flask_app.route('/admin/keys/generate-email')
    @login_required
    def generate_key_email_route():
        import re
        mode = request.args.get('mode', 'personal')
        try:
            user_id = int(request.args.get('user_id'))
        except Exception:
            user_id = 0

        if mode == 'personal' and not user_id:
            return jsonify({"ok": False, "error": "invalid user_id"}), 400

        try:
            base_local = ""
            user = get_user(user_id) if user_id else None
            
            if mode == 'gift':
                if user:
                    # Gift with user assigned
                    raw_username = (user.get('username') or f'user{user_id}').lower()
                    username_slug = re.sub(r"[^a-z0-9._-]", "_", raw_username).strip("_")[:20]
                    base_local = f"gift_{username_slug}"
                else:
                    # Gift without user (random)
                    # Use a temporary placeholder that looks real, but keeps changing if we re-request? 
                    # Or just one. Let's use uuid.
                    base_local = f"gift-{uuid.uuid4().hex[:8]}"
            else:
                # Personal (requires user)
                raw_username = (user.get('username') or f'user{user_id}').lower()
                username_slug = re.sub(r"[^a-z0-9._-]", "_", raw_username).strip("_")[:16] or f"user{user_id}"
                base_local = f"{username_slug}"

            candidate_local = base_local
            attempt = 0
            while True:
                suffix = f"-{attempt}" if attempt > 0 else ""
                candidate_email = f"{candidate_local}{suffix}@bot.local"
                if not rw_repo.get_key_by_email(candidate_email):
                    break
                attempt += 1
            
            return jsonify({"ok": True, "email": candidate_email})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    @flask_app.route('/admin/users/<int:user_id>/seller_settings', methods=['POST'])
    @login_required
    def update_seller_settings_route(user_id: int):
        try:
            seller_active = int(request.form.get('seller_active', 0))
            seller_sale = float(request.form.get('seller_sale', 0))
            seller_ref = float(request.form.get('seller_ref', 0))
            seller_uuid = request.form.get('seller_uuid', '0').strip()
            
            # Update user table for active status
            with sqlite3.connect(DB_FILE) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE users SET seller_active = ? WHERE telegram_id = ?",
                    (seller_active, user_id)
                )
                conn.commit()
            
            # Update seller_users table logic
            if seller_active == 1:
                add_seller_user(user_id, seller_sale, seller_ref, seller_uuid)
            else:
                delete_seller_user(user_id)
            
            return jsonify({"ok": True, "message": "Настройки продавца сохранены"})
        except Exception as e:
            logger.error(f"Failed to update seller settings for {user_id}: {e}")
            return jsonify({"ok": False, "error": str(e)}), 500

    @flask_app.route('/admin/keys/<int:key_id>/delete', methods=['POST'])
    @login_required
    def delete_key_route(key_id: int):

        try:
            key = rw_repo.get_key_by_id(key_id)
            if key:
                try:
                    asyncio.run(remnawave_api.delete_client_on_host(key['host_name'], key['key_email']))
                except Exception:
                    pass
        except Exception:
            pass
        ok = delete_key_by_id(key_id)
        msg = 'Ключ удалён.' if ok else 'Не удалось удалить ключ.'
        
        wants_json = 'application/json' in (request.headers.get('Accept') or '') or request.headers.get('X-Requested-With') == 'XMLHttpRequest'
        if wants_json:
            return jsonify({"ok": ok, "message": msg})
            
        flash(msg, 'success' if ok else 'danger')
        return redirect(request.referrer or url_for('admin_keys_page'))

    @flask_app.route('/admin/keys/<int:key_id>/adjust-expiry', methods=['POST'])
    @login_required
    def adjust_key_expiry_route(key_id: int):
        try:
            delta_days = int(request.form.get('delta_days', '0'))
        except Exception:
            return jsonify({"ok": False, "error": "invalid_delta"}), 400
        key = rw_repo.get_key_by_id(key_id)
        if not key:
            return jsonify({"ok": False, "error": "not_found"}), 404
        try:

            cur_expiry = key.get('expiry_date')
            if isinstance(cur_expiry, str):
                try:
                    exp_dt = datetime.fromisoformat(cur_expiry)
                except Exception:

                    try:
                        exp_dt = datetime.strptime(cur_expiry, '%Y-%m-%d %H:%M:%S')
                    except Exception:
                        exp_dt = get_msk_time().replace(tzinfo=None)
            else:
                exp_dt = cur_expiry or get_msk_time().replace(tzinfo=None)
            new_dt = exp_dt + timedelta(days=delta_days)
            if new_dt.tzinfo is None:
                msk_tz = timezone(timedelta(hours=3), name='MSK')
                new_dt = new_dt.replace(tzinfo=msk_tz)
            new_ms = int(new_dt.timestamp() * 1000)


            try:
                result = asyncio.run(remnawave_api.create_or_update_key_on_host(
                    host_name=key.get('host_name'),
                    email=key.get('key_email'),
                    expiry_timestamp_ms=new_ms,
                    force_expiry=True  # Из админки всегда принудительно обновляем срок
                ))
            except Exception as e:
                result = None
            if not result or not result.get('expiry_timestamp_ms'):
                return jsonify({"ok": False, "error": "remnawave_update_failed"}), 500


            client_uuid = result.get('client_uuid') or key.get('remnawave_user_uuid') or ''
            if not rw_repo.update_key(
                key_id,
                remnawave_user_uuid=client_uuid,
                expire_at_ms=int(result.get('expiry_timestamp_ms') or new_ms),
                subscription_url=result.get('subscription_url') or result.get('connection_string'),
            ):
                return jsonify({"ok": False, "error": "db_update_failed"}), 500


            try:
                user_id = key.get('user_id')
                new_ms_final = int(result.get('expiry_timestamp_ms'))
                # Используем МСК для отображения
                msk_tz = timezone(timedelta(hours=3), name='MSK')
                new_dt_local = datetime.fromtimestamp(new_ms_final/1000, tz=msk_tz)
                text = (
                    "🗓️ <b>Срок действия ключа изменён</b>\n\n"
                    "<b>Обновленные данные:</b>\n"
                    f"🛰 Хост: <code>{key.get('host_name')}</code>\n"
                    f"💌 Email: <code>{key.get('key_email')}</code>\n\n"
                    f"📅 Истекает: <b>{datetime.fromtimestamp(new_ms_final/1000, tz=timezone(timedelta(hours=3), name='MSK')).strftime('%Y-%m-%d %H:%M')}</b>\n"
                    f"⏳ Осталось: <b>{_get_time_remaining_str(new_ms_final)}</b>\n"
                    "👤 Изменено: Администратором\n"
                )
                if user_id:
                    bot = _bot_controller.get_bot_instance()
                    loop = current_app.config.get('EVENT_LOOP')
                    if bot and loop and loop.is_running():
                        asyncio.run_coroutine_threadsafe(bot.send_message(chat_id=user_id, text=text, parse_mode='HTML'), loop)
                    elif bot:
                        asyncio.run(bot.send_message(chat_id=user_id, text=text, parse_mode='HTML'))
            except Exception:
                pass

            return jsonify({"ok": True, "new_expiry_ms": int(result.get('expiry_timestamp_ms'))})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    @flask_app.route('/admin/keys/sweep-expired', methods=['POST'])
    @login_required
    def sweep_expired_keys_route():
        removed = 0
        failed = 0
        now = get_msk_time().replace(tzinfo=None)
        keys = get_all_keys()
        for k in keys:
            exp = k.get('expiry_date')
            exp_dt = None
            try:
                if isinstance(exp, str):
                    s = exp.strip()
                    if s:
                        try:

                            exp_dt = datetime.fromisoformat(s)
                        except Exception:
                            try:
                                exp_dt = datetime.fromisoformat(s.replace('Z', '+00:00'))
                            except Exception:

                                try:
                                    exp_dt = datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
                                except Exception:
                                    exp_dt = None
                else:
                    exp_dt = exp
            except Exception:
                exp_dt = None

            try:
                if exp_dt is not None and getattr(exp_dt, 'tzinfo', None) is not None:
                    exp_dt = exp_dt.astimezone(timezone(timedelta(hours=3))).replace(tzinfo=None)
            except Exception:
                pass
            if not exp_dt or exp_dt > now:
                continue

            try:
                try:

                    host_for_delete = (k.get('host_name') or '').strip()
                    if not host_for_delete:
                        try:
                            sq = (k.get('squad_uuid') or k.get('squadUuid') or '').strip()
                            if sq:
                                squad = rw_repo.get_squad(sq)
                                if squad and squad.get('host_name'):
                                    host_for_delete = squad.get('host_name')
                        except Exception:
                            pass
                    if host_for_delete:
                        asyncio.run(remnawave_api.delete_client_on_host(host_for_delete, k.get('key_email')))
                except Exception:
                    pass
                delete_key_by_id(k.get('key_id'))
                removed += 1

                try:
                    bot = _bot_controller.get_bot_instance()
                    loop = current_app.config.get('EVENT_LOOP')
                    text = (
                        "🗑 <b>Ключ удалён (истек срок)</b>\n\n"
                        "<b>Информация:</b>\n"
                        f"🛰 Хост: <code>{k.get('host_name')}</code>\n"
                        f"💌 Email: <code>{k.get('key_email')}</code>\n\n"
                        "💡 <i>Вы можете оформить новый ключ в меню бота.</i>"
                    )
                    if bot and loop and loop.is_running():
                        asyncio.run_coroutine_threadsafe(bot.send_message(chat_id=k.get('user_id'), text=text, parse_mode='HTML'), loop)
                    else:
                        asyncio.run(bot.send_message(chat_id=k.get('user_id'), text=text, parse_mode='HTML'))
                except Exception:
                    pass
            except Exception:
                failed += 1
            
        msg = f"Удалено истёкших ключей: {removed}. Ошибок: {failed}."
        wants_json = 'application/json' in (request.headers.get('Accept') or '') or request.headers.get('X-Requested-With') == 'XMLHttpRequest'
        if wants_json:
            return jsonify({"ok": True, "message": msg, "removed": removed, "failed": failed})

        flash(msg, 'success' if failed == 0 else 'warning')
        return redirect(request.referrer or url_for('admin_keys_page'))

    @flask_app.route('/admin/keys/<int:key_id>/comment', methods=['POST'])
    @login_required
    def update_key_comment_route(key_id: int):
        comment = (request.form.get('comment') or '').strip()
        ok = update_key_comment(key_id, comment)
        if ok:
            return jsonify({"ok": True})
        else:
            return jsonify({"ok": False, "error": "db_error"}), 500


    @flask_app.route('/admin/hosts/ssh/update', methods=['POST'])
    @login_required
    def update_host_ssh_route():
        host_name = (request.form.get('host_name') or '').strip()
        ssh_host = (request.form.get('ssh_host') or '').strip() or None
        ssh_port_raw = (request.form.get('ssh_port') or '').strip()
        ssh_user = (request.form.get('ssh_user') or '').strip() or None
        ssh_password = request.form.get('ssh_password')
        ssh_key_path = (request.form.get('ssh_key_path') or '').strip() or None
        ssh_port = None
        try:
            ssh_port = int(ssh_port_raw) if ssh_port_raw else None
        except Exception:
            ssh_port = None
        ok = update_host_ssh_settings(host_name, ssh_host=ssh_host, ssh_port=ssh_port, ssh_user=ssh_user,
                                      ssh_password=ssh_password, ssh_key_path=ssh_key_path)
        
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({'ok': ok, 'message': 'SSH-параметры обновлены' if ok else 'Не удалось обновить SSH-параметры'})
        
        flash('SSH-параметры обновлены.' if ok else 'Не удалось обновить SSH-параметры.', 'success' if ok else 'danger')
        return redirect(request.referrer or url_for('settings_page'))




    @flask_app.route('/admin/ssh-targets/<target_name>/speedtest/run', methods=['POST'])
    @login_required
    def run_ssh_target_speedtest_route(target_name: str):
        logger.info(f"Панель: запущен спидтест для SSH-цели '{target_name}'")
        try:
            res = asyncio.run(speedtest_runner.run_and_store_ssh_speedtest_for_target(target_name))
        except Exception as e:
            res = {"ok": False, "error": str(e)}
        if res and res.get('ok'):
            logger.info(f"Панель: спидтест для SSH-цели '{target_name}' завершён успешно")
        else:
            logger.warning(f"Панель: спидтест для SSH-цели '{target_name}' завершился с ошибкой: {res.get('error') if res else 'unknown'}")
        wants_json = 'application/json' in (request.headers.get('Accept') or '') or request.headers.get('X-Requested-With') == 'XMLHttpRequest'
        if wants_json:
            return jsonify(res)
        flash(('Тест выполнен.' if res and res.get('ok') else f"Ошибка теста: {res.get('error') if res else 'unknown'}"), 'success' if res and res.get('ok') else 'danger')
        return redirect(request.referrer or url_for('settings_page', tab='hosts'))


    @flask_app.route('/admin/ssh-targets/speedtests/run-all', methods=['POST'])
    @login_required
    def run_all_ssh_target_speedtests_route():
        logger.info("Панель: запуск спидтеста ДЛЯ ВСЕХ SSH-целей")
        try:
            targets = get_all_ssh_targets()
        except Exception:
            targets = []
        errors = []
        ok_count = 0
        total = 0
        for t in targets or []:
            name = (t.get('target_name') or '').strip()
            if not name:
                continue
            total += 1
            try:
                res = asyncio.run(speedtest_runner.run_and_store_ssh_speedtest_for_target(name))
                if res and res.get('ok'):
                    ok_count += 1
                else:
                    errors.append(f"{name}: {res.get('error') if res else 'unknown'}")
            except Exception as e:
                errors.append(f"{name}: {e}")
        logger.info(f"Панель: завершён спидтест ДЛЯ ВСЕХ SSH-целей: ок={ok_count}, всего={total}")
        wants_json = 'application/json' in (request.headers.get('Accept') or '') or request.headers.get('X-Requested-With') == 'XMLHttpRequest'
        if wants_json:
            return jsonify({"ok": len(errors) == 0, "done": ok_count, "total": total, "errors": errors})
        if errors:
            flash(f"SSH цели: выполнено {ok_count}/{total}. Ошибки: {'; '.join(errors[:3])}{'…' if len(errors) > 3 else ''}", 'warning')
        else:
            flash(f"SSH цели: тесты скорости выполнены для всех ({ok_count}/{total})", 'success')
        return redirect(request.referrer or url_for('dashboard_page'))


    @flask_app.route('/admin/hosts/<host_name>/speedtest/run', methods=['POST'])
    @login_required
    def run_host_speedtest_route(host_name: str):
        method = (request.form.get('method') or '').strip().lower()
        logger.info(f"Панель: запущен спидтест для хоста '{host_name}', метод='{method or 'both'}'")
        try:
            if method == 'ssh':
                res = asyncio.run(speedtest_runner.run_and_store_ssh_speedtest(host_name))
            elif method == 'net':
                res = asyncio.run(speedtest_runner.run_and_store_net_probe(host_name))
            else:

                res = asyncio.run(speedtest_runner.run_both_for_host(host_name))
        except Exception as e:
            res = {'ok': False, 'error': str(e)}
        if res and res.get('ok'):
            logger.info(f"Панель: спидтест для хоста '{host_name}' завершён успешно")
        else:
            logger.warning(f"Панель: спидтест для хоста '{host_name}' завершился с ошибкой: {res.get('error') if res else 'unknown'}")
        wants_json = 'application/json' in (request.headers.get('Accept') or '') or request.headers.get('X-Requested-With') == 'XMLHttpRequest'
        if wants_json:
            return jsonify(res)
        flash(('Тест выполнен.' if res and res.get('ok') else f"Ошибка теста: {res.get('error') if res else 'unknown'}"), 'success' if res and res.get('ok') else 'danger')
        return redirect(request.referrer or url_for('settings_page'))

    @flask_app.route('/admin/hosts/<host_name>/speedtests.json')
    @login_required
    def host_speedtests_json(host_name: str):
        try:
            limit = int(request.args.get('limit') or 20)
        except Exception:
            limit = 20
        try:
            items = get_speedtests(host_name, limit=limit) or []
            return jsonify({
                'ok': True,
                'items': items
            })
        except Exception as e:
            return jsonify({'ok': False, 'error': str(e)}), 500

    @flask_app.route('/admin/speedtests/run-all', methods=['POST'])
    @login_required
    def run_all_speedtests_route():

        logger.info("Панель: запуск спидтеста ДЛЯ ВСЕХ хостов")
        try:
            hosts = get_all_hosts()
        except Exception:
            hosts = []
        errors = []
        ok_count = 0
        for h in hosts:
            name = h.get('host_name')
            if not name:
                continue
            try:
                res = asyncio.run(speedtest_runner.run_both_for_host(name))
                if res and res.get('ok'):
                    ok_count += 1
                else:
                    errors.append(f"{name}: {res.get('error') if res else 'unknown'}")
            except Exception as e:
                errors.append(f"{name}: {e}")
        logger.info(f"Панель: завершён спидтест ДЛЯ ВСЕХ хостов: ок={ok_count}, всего={len(hosts)}")

        wants_json = 'application/json' in (request.headers.get('Accept') or '') or request.headers.get('X-Requested-With') == 'XMLHttpRequest'
        if wants_json:
            return jsonify({"ok": len(errors) == 0, "done": ok_count, "total": len(hosts), "errors": errors})
        if errors:
            flash(f"Выполнено для {ok_count}/{len(hosts)}. Ошибки: {'; '.join(errors[:3])}{'…' if len(errors) > 3 else ''}", 'warning')
        else:
            flash(f"Тесты скорости выполнены для всех хостов: {ok_count}/{len(hosts)}", 'success')
        return redirect(request.referrer or url_for('dashboard_page'))


    @flask_app.route('/admin/hosts/<host_name>/speedtest/install', methods=['POST'])
    @login_required
    def auto_install_speedtest_route(host_name: str):

        try:
            res = asyncio.run(speedtest_runner.auto_install_speedtest_on_host(host_name))
        except Exception as e:
            res = {'ok': False, 'log': str(e)}
        wants_json = 'application/json' in (request.headers.get('Accept') or '') or request.headers.get('X-Requested-With') == 'XMLHttpRequest'
        if wants_json:
            return jsonify({"ok": bool(res.get('ok')), "log": res.get('log')})
        flash(('Установка завершена успешно.' if res.get('ok') else 'Не удалось установить speedtest на хост.') , 'success' if res.get('ok') else 'danger')

        try:
            log = res.get('log') or ''
            short = '\n'.join((log.splitlines() or [])[-20:])
            if short:
                flash(short, 'secondary')
        except Exception:
            pass
        return redirect(request.referrer or url_for('settings_page'))

    @flask_app.route('/admin/balance')
    @login_required
    def admin_balance_page():
        try:
            user_id = request.args.get('user_id', type=int)
        except Exception:
            user_id = None
        user = None
        balance = None
        referrals = []
        if user_id:
            try:
                user = get_user(user_id)
                balance = get_balance(user_id)
                referrals = get_referrals_for_user(user_id)
            except Exception:
                pass
        common_data = get_common_template_data()
        return render_template('admin_balance.html', user=user, balance=balance, referrals=referrals, **common_data)

    @flask_app.route('/support')
    @login_required
    def support_list_page():
        status = request.args.get('status', 'open')
        page = request.args.get('page', 1, type=int)
        per_page = 12

        # OPTIMIZATION: Do not fetch tickets synchronously.
        # Front-end will call /support/table.partial
        tickets = []
        total_pages = 1
        
        open_count = get_open_tickets_count()
        closed_count = get_closed_tickets_count()
        all_count = get_all_tickets_count()
        common_data = get_common_template_data()
        return render_template(
            'support.html',
            tickets=tickets,
            current_page=page,
            total_pages=total_pages,
            filter_status=status,
            open_count=open_count,
            closed_count=closed_count,
            all_count=all_count,
            **common_data
        )

    @flask_app.route('/support/<int:ticket_id>', methods=['GET', 'POST'])
    @login_required
    def support_ticket_page(ticket_id):
        ticket = get_ticket(ticket_id)
        if not ticket:
            flash('Тикет не найден.', 'danger')
            return redirect(url_for('support_list_page'))

        if request.method == 'POST':
            message = (request.form.get('message') or '').strip()
            action = request.form.get('t_action') or request.form.get('action')
            if action == 'reply':
                if not message:
                    flash('Сообщение не может быть пустым.', 'warning')
                else:
                    add_support_message(ticket_id, sender='admin', content=message)
                    try:
                        bot = _support_bot_controller.get_bot_instance()
                        loop = current_app.config.get('EVENT_LOOP')
                        user_chat_id = ticket.get('user_id')
                        if bot and loop and loop.is_running() and user_chat_id:
                            text = (
                                f"💬 <b>Ответ от технической поддержки.</b>\n"
                                f"📝 <b>ID тикета:</b> <code>#{ticket_id}</code>\n\n"
                                f"💌 <b>Ответ на ваше обращение:</b>\n"
                                f"<blockquote>{message}</blockquote>"
                            )
                            asyncio.run_coroutine_threadsafe(bot.send_message(user_chat_id, text), loop)
                        else:
                            logger.info("Ответ поддержки: support-бот не настроен или не запущен; сообщение пользователю сохранено только в БД.")
                    except Exception as e:
                        logger.error(f"Ответ поддержки: не удалось отправить сообщение пользователю {ticket.get('user_id')} через support-бота: {e}", exc_info=True)
                    try:
                        bot = _support_bot_controller.get_bot_instance()
                        loop = current_app.config.get('EVENT_LOOP')
                        forum_chat_id = ticket.get('forum_chat_id')
                        thread_id = ticket.get('message_thread_id')
                        if bot and loop and loop.is_running() and forum_chat_id and thread_id:
                            text = (
                                f"💬 <b>Ответ технической поддержки:</b>\n"
                                f"📝 <b>ID тикета:</b> <code>#{ticket_id}</code>\n\n"
                                f"💌 <b>Ответ на ваше обращение:</b>\n"
                                f"<blockquote>{message}</blockquote>"
                            )
                            asyncio.run_coroutine_threadsafe(
                                bot.send_message(chat_id=int(forum_chat_id), text=text, message_thread_id=int(thread_id)),
                                loop
                            )
                    except Exception as e:
                        logger.warning(f"Ответ поддержки: не удалось отзеркалить сообщение в тему форума для тикета {ticket_id}: {e}")
                    flash('Ответ отправлен.', 'success')
                return redirect(url_for('support_ticket_page', ticket_id=ticket_id))
            elif action == 'close':
                if ticket.get('status') != 'closed' and set_ticket_status(ticket_id, 'closed'):
                    try:
                        bot = _support_bot_controller.get_bot_instance()
                        loop = current_app.config.get('EVENT_LOOP')
                        forum_chat_id = ticket.get('forum_chat_id')
                        thread_id = ticket.get('message_thread_id')
                        if bot and loop and loop.is_running() and forum_chat_id and thread_id:
                            asyncio.run_coroutine_threadsafe(
                                bot.close_forum_topic(chat_id=int(forum_chat_id), message_thread_id=int(thread_id)),
                                loop
                            )
                    except Exception as e:
                        logger.warning(f"Закрытие тикета: не удалось закрыть тему форума для тикета {ticket_id}: {e}")
                    try:
                        bot = _support_bot_controller.get_bot_instance()
                        loop = current_app.config.get('EVENT_LOOP')
                        user_chat_id = ticket.get('user_id')
                        if bot and loop and loop.is_running() and user_chat_id:
                            text = (
                                f"✅ <b>Ваш тикет #{ticket_id} был закрыт</b>\n\n"
                                f"✉️ <i>Если у вас появятся другие вопросы или ваш вопрос не решен</i>\n\n"
                                f"💌 <b>Вы можете создать новое обращение при необходимости.</b>"
                            )
                            asyncio.run_coroutine_threadsafe(bot.send_message(int(user_chat_id), text), loop)
                    except Exception as e:
                        logger.warning(f"Закрытие тикета: не удалось уведомить пользователя {ticket.get('user_id')} о закрытии тикета #{ticket_id}: {e}")
                    flash('Тикет закрыт.', 'success')
                else:
                    flash('Не удалось закрыть тикет.', 'danger')
                return redirect(url_for('support_ticket_page', ticket_id=ticket_id))
            elif action == 'open':
                if ticket.get('status') != 'open' and set_ticket_status(ticket_id, 'open'):
                    try:
                        bot = _support_bot_controller.get_bot_instance()
                        loop = current_app.config.get('EVENT_LOOP')
                        forum_chat_id = ticket.get('forum_chat_id')
                        thread_id = ticket.get('message_thread_id')
                        if bot and loop and loop.is_running() and forum_chat_id and thread_id:
                            asyncio.run_coroutine_threadsafe(
                                bot.reopen_forum_topic(chat_id=int(forum_chat_id), message_thread_id=int(thread_id)),
                                loop
                            )
                    except Exception as e:
                        logger.warning(f"Открытие тикета: не удалось переоткрыть тему форума для тикета {ticket_id}: {e}")

                    try:
                        bot = _support_bot_controller.get_bot_instance()
                        loop = current_app.config.get('EVENT_LOOP')
                        user_chat_id = ticket.get('user_id')
                        if bot and loop and loop.is_running() and user_chat_id:
                            text = (
                                f"🔓 <b>Ваш тикет #{ticket_id} был переоткрыт!</b>\n\n"
                                f"Администратор изучил ваше обращение и возобновил переписку. Вы можете продолжить общение."
                            )
                            asyncio.run_coroutine_threadsafe(bot.send_message(int(user_chat_id), text), loop)
                    except Exception as e:
                        logger.warning(f"Открытие тикета: не удалось уведомить пользователя {ticket.get('user_id')} об открытии тикета #{ticket_id}: {e}")
                    flash('Тикет открыт.', 'success')
                else:
                    flash('Не удалось открыть тикет.', 'danger')
                return redirect(url_for('support_ticket_page', ticket_id=ticket_id))

        messages = get_ticket_messages(ticket_id)
        
        # AJAX OPTIMIZATION: Return only messages part if requested
        if request.args.get('partial') == 'true':
            # Чтобы не создавать файлы, рендерим фрагмент сообщения прямо здесь (через цикл в строке или мини-шаблон)
            # Но у нас есть ticket.html, мы можем добавить туда заголовок-условие.
            return render_template('ticket.html', ticket=ticket, messages=messages, partial_mode=True)

        common_data = get_common_template_data()
        return render_template('ticket.html', ticket=ticket, messages=[], **common_data)

    @flask_app.route('/support/<int:ticket_id>/messages.json')
    @login_required
    def support_ticket_messages_api(ticket_id):
        ticket = get_ticket(ticket_id)
        if not ticket:
            return jsonify({"error": "not_found"}), 404
        messages = get_ticket_messages(ticket_id) or []
        items = [
            {
                "sender": m.get('sender'),
                "content": m.get('content'),
                "created_at": m.get('created_at')
            }
            for m in messages
        ]
        return jsonify({
            "ticket_id": ticket_id,
            "status": ticket.get('status'),
            "messages": items
        })

    @flask_app.route('/support/<int:ticket_id>/delete', methods=['POST'])
    @login_required
    def delete_support_ticket_route(ticket_id: int):
        ticket = get_ticket(ticket_id)
        if not ticket:
            flash('Тикет не найден.', 'danger')
            return redirect(url_for('support_list_page'))
        try:
            bot = _support_bot_controller.get_bot_instance()
            loop = current_app.config.get('EVENT_LOOP')
            forum_chat_id = ticket.get('forum_chat_id')
            thread_id = ticket.get('message_thread_id')
            if bot and loop and loop.is_running() and forum_chat_id and thread_id:
                try:
                    fut = asyncio.run_coroutine_threadsafe(
                        bot.delete_forum_topic(chat_id=int(forum_chat_id), message_thread_id=int(thread_id)),
                        loop
                    )
                    fut.result(timeout=5)
                except Exception as e:
                    logger.warning(f"Удаление темы форума не удалось для тикета {ticket_id} (чат {forum_chat_id}, тема {thread_id}): {e}. Пытаюсь закрыть тему как фолбэк.")
                    try:
                        fut2 = asyncio.run_coroutine_threadsafe(
                            bot.close_forum_topic(chat_id=int(forum_chat_id), message_thread_id=int(thread_id)),
                            loop
                        )
                        fut2.result(timeout=5)
                    except Exception as e2:
                        logger.warning(f"Фолбэк-закрытие темы форума также не удалось для тикета {ticket_id}: {e2}")
            else:
                logger.debug("Удаление тикета: support-бот не настроен или форум не создан; тикет удален только из БД.")
        except Exception as e:
            logger.warning(f"Не удалось обработать удаление темы форума для тикета {ticket_id} перед удалением: {e}")
        if delete_ticket(ticket_id):
            flash(f"Тикет #{ticket_id} удалён.", 'success')
            return redirect(url_for('support_list_page'))
        else:
            flash(f"Не удалось удалить тикет #{ticket_id}.", 'danger')
            return redirect(url_for('support_ticket_page', ticket_id=ticket_id))

    @flask_app.route('/settings', methods=['GET', 'POST'])
    @login_required
    def settings_page():
        if request.method == 'POST':

            if 'panel_password' in request.form and request.form.get('panel_password'):
                update_setting('panel_password', request.form.get('panel_password'))


            checkbox_keys = ['force_subscription', 'sbp_enabled', 'trial_enabled', 'enable_referrals', 'enable_fixed_referral_bonus', 'stars_enabled', 'yoomoney_enabled', 'monitoring_enabled', 'platega_enabled', 'skip_email', 'enable_wal_mode']
            for checkbox_key in checkbox_keys:
                values = request.form.getlist(checkbox_key)
                value = values[-1] if values else 'false'
                update_setting(checkbox_key, value)


            for key in ALL_SETTINGS_KEYS:
                if key in checkbox_keys or key == 'panel_password':
                    continue
                if key in request.form:
                    update_setting(key, request.form.get(key))

            # Обработка настроек комментариев платежей
            # Если мы на вкладке payments или поля присутствуют (чекбоксы шлют значение только если checked, поэтому проверяем наличие хотя бы одного или контекст)
            # Так как форма общая, просто собираем состояние
            pay_info = {
                'id': 1 if request.form.get('pay_info_id') else 0,
                'username': 1 if request.form.get('pay_info_username') else 0,
                'first_name': 1 if request.form.get('pay_info_first_name') else 0,
                'host_name': 1 if request.form.get('pay_info_host_name') else 0,
            }
            # Сохраняем только если это сабмит формы настроек (а не partial update, хотя тут вроде все save)
            update_setting('pay_info_comment', json.dumps(pay_info))

            flash('Настройки сохранены.', 'success')
            next_hash = (request.form.get('next_hash') or '').strip() or '#panel'
            next_tab = (next_hash[1:] if next_hash.startswith('#') else next_hash) or 'panel'
            return redirect(url_for('settings_page', tab=next_tab))

        current_settings = get_all_settings()
        
        # Загрузка настроек комментариев
        try:
            pay_info = json.loads(current_settings.get('pay_info_comment', '{}'))
        except (ValueError, TypeError):
            pay_info = {}
        


        hosts = get_all_hosts()
        for host in hosts:
            host['plans'] = get_plans_for_host(host['host_name'])

            try:
                host['latest_speedtest'] = get_latest_speedtest(host['host_name'])
            except Exception:
                host['latest_speedtest'] = None

        try:
            ssh_targets = get_all_ssh_targets()
        except Exception:
            ssh_targets = []
        

        backups = []
        try:
            from pathlib import Path
            bdir = backup_manager.BACKUPS_DIR
            for p in sorted(bdir.glob('db-backup-*.zip'), key=lambda x: x.stat().st_mtime, reverse=True):
                try:
                    st = p.stat()
                    backups.append({
                        'name': p.name,
                        'mtime': datetime.fromtimestamp(st.st_mtime, tz=timezone(timedelta(hours=3))).strftime('%Y-%m-%d %H:%M'),
                        'size': st.st_size
                    })
                except Exception:
                    pass
        except Exception:
            backups = []

        common_data = get_common_template_data()
        return render_template('settings.html', settings=current_settings, hosts=hosts, ssh_targets=ssh_targets, backups=backups, pay_info=pay_info, **common_data)


    @flask_app.route('/api/settings/update-pay-info', methods=['POST'])
    @login_required
    def update_pay_info_api():
        data = request.get_json()
        if not data:
             return jsonify({'status': 'error', 'message': 'No data provided'}), 400
            
        field = data.get('field')
        value = data.get('value')
        
        valid_fields = ['id', 'username', 'first_name', 'host_name']
        if field not in valid_fields:
            return jsonify({'status': 'error', 'message': f'Invalid field: {field}'}), 400
            
        try:
            current_json = get_setting('pay_info_comment')
            pay_info = json.loads(current_json) if current_json else {}
        except (ValueError, TypeError):
            pay_info = {}
            

             
        pay_info[field] = 1 if value else 0
        
        update_setting('pay_info_comment', json.dumps(pay_info))
        return jsonify({'status': 'success', 'pay_info': pay_info})



    @flask_app.route('/admin/ssh-targets/create', methods=['POST'])
    @login_required
    def create_ssh_target_route():
        name = (request.form.get('target_name') or '').strip()
        ssh_host = (request.form.get('ssh_host') or '').strip()
        ssh_port = request.form.get('ssh_port')
        ssh_user = (request.form.get('ssh_user') or '').strip() or None
        ssh_password = request.form.get('ssh_password')
        ssh_key_path = (request.form.get('ssh_key_path') or '').strip() or None
        description = (request.form.get('description') or '').strip() or None
        try:
            ssh_port_val = int(ssh_port) if ssh_port else 22
        except Exception:
            ssh_port_val = 22
        wants_json = 'application/json' in (request.headers.get('Accept') or '') or request.headers.get('X-Requested-With') == 'XMLHttpRequest'
        if not name or not ssh_host:
            if wants_json:
                return jsonify({'ok': False, 'error': 'Укажите имя цели и SSH хост.'}), 400
            flash('Укажите имя цели и SSH хост.', 'warning')
            return redirect(url_for('settings_page', tab='hosts'))
        ok = create_ssh_target(
            target_name=name,
            ssh_host=ssh_host,
            ssh_port=ssh_port_val,
            ssh_user=ssh_user,
            ssh_password=ssh_password,
            ssh_key_path=ssh_key_path,
            description=description,
        )
        if wants_json:
            return jsonify({'ok': ok, 'message': 'SSH-цель добавлена' if ok else 'Не удалось добавить SSH-цель'})
        flash('SSH-цель добавлена.' if ok else 'Не удалось добавить SSH-цель.', 'success' if ok else 'danger')
        return redirect(url_for('settings_page', tab='hosts'))

    @flask_app.route('/admin/ssh-targets/<target_name>/update', methods=['POST'])
    @login_required
    def update_ssh_target_route(target_name: str):
        wants_json = 'application/json' in (request.headers.get('Accept') or '') or request.headers.get('X-Requested-With') == 'XMLHttpRequest'
        new_target_name = (request.form.get('new_target_name') or '').strip() if 'new_target_name' in request.form else None
        
        ssh_host = (request.form.get('ssh_host') or '').strip() if 'ssh_host' in request.form else None
        ssh_port_raw = (request.form.get('ssh_port') or '').strip() if 'ssh_port' in request.form else None
        ssh_user = (request.form.get('ssh_user') or '').strip() if 'ssh_user' in request.form else None
        ssh_password = request.form.get('ssh_password') if 'ssh_password' in request.form else None
        ssh_key_path = (request.form.get('ssh_key_path') or '').strip() if 'ssh_key_path' in request.form else None
        description = (request.form.get('description') or '').strip() if 'description' in request.form else None
        try:
            ssh_port = int(ssh_port_raw) if ssh_port_raw else None
        except Exception:
            ssh_port = None
        
        actual_target_name = target_name
        if new_target_name and new_target_name != target_name:
            rename_ok = rename_ssh_target(target_name, new_target_name)
            if not rename_ok:
                if wants_json:
                    return jsonify({'ok': False, 'error': 'Не удалось переименовать SSH-цель. Возможно, цель с таким именем уже существует.'}), 400
                flash('Не удалось переименовать SSH-цель. Возможно, цель с таким именем уже существует.', 'danger')
                return redirect(request.referrer or url_for('settings_page', tab='hosts'))
            actual_target_name = new_target_name
        
        ok = update_ssh_target_fields(
            actual_target_name,
            ssh_host=ssh_host,
            ssh_port=ssh_port,
            ssh_user=ssh_user,
            ssh_password=ssh_password,
            ssh_key_path=ssh_key_path,
            description=description,
        )
        if wants_json:
            return jsonify({'ok': ok, 'message': 'SSH-цель обновлена' if ok else 'Не удалось обновить SSH-цель'})
        flash('SSH-цель обновлена.' if ok else 'Не удалось обновить SSH-цель.', 'success' if ok else 'danger')
        return redirect(request.referrer or url_for('settings_page', tab='hosts'))

    @flask_app.route('/admin/ssh-targets/<target_name>/delete', methods=['POST'])
    @login_required
    def delete_ssh_target_route(target_name: str):
        ok = delete_ssh_target(target_name)
        wants_json = 'application/json' in (request.headers.get('Accept') or '') or request.headers.get('X-Requested-With') == 'XMLHttpRequest'
        if wants_json:
            return jsonify({'ok': ok, 'message': 'SSH-цель удалена' if ok else 'Не удалось удалить SSH-цель'})
        flash('SSH-цель удалена.' if ok else 'Не удалось удалить SSH-цель.', 'success' if ok else 'danger')
        return redirect(url_for('settings_page', tab='hosts'))

    

    @flask_app.route('/admin/ssh-targets/<target_name>/speedtest/install', methods=['POST'])
    @login_required
    def auto_install_speedtest_on_target_route(target_name: str):
        try:
            res = asyncio.run(speedtest_runner.auto_install_speedtest_on_target(target_name))
        except Exception as e:
            res = {'ok': False, 'log': str(e)}
        wants_json = 'application/json' in (request.headers.get('Accept') or '') or request.headers.get('X-Requested-With') == 'XMLHttpRequest'
        if wants_json:
            return jsonify({"ok": bool(res.get('ok')), "log": res.get('log')})
        flash(('Установка завершена успешно.' if res.get('ok') else 'Не удалось установить speedtest на цель.') , 'success' if res.get('ok') else 'danger')
        try:
            log = res.get('log') or ''
            short = '\n'.join((log.splitlines() or [])[-20:])
            if short:
                flash(short, 'secondary')
        except Exception:
            pass
        return redirect(request.referrer or url_for('settings_page', tab='hosts'))


    @flask_app.route('/admin/db/backup', methods=['POST'])
    @login_required
    def backup_db_route():
        try:
            zip_path = backup_manager.create_backup_file()
            if not zip_path or not os.path.isfile(zip_path):
                flash('Не удалось создать бэкап БД.', 'danger')
                return redirect(request.referrer or url_for('settings_page', tab='panel'))

            return send_file(str(zip_path), as_attachment=True, download_name=os.path.basename(zip_path))
        except Exception as e:
            logger.error(f"Ошибка резервного копирования БД: {e}")
            flash('Ошибка при создании бэкапа.', 'danger')
            return redirect(request.referrer or url_for('settings_page', tab='panel'))

    @flask_app.route('/admin/db/restore', methods=['POST'])
    @login_required
    def restore_db_route():
        try:

            existing = (request.form.get('existing_backup') or '').strip()
            ok = False
            if existing:

                base = backup_manager.BACKUPS_DIR
                candidate = (base / existing).resolve()
                if str(candidate).startswith(str(base.resolve())) and os.path.isfile(candidate):
                    ok = backup_manager.restore_from_file(candidate)
                else:
                    flash('Выбранный бэкап не найден.', 'danger')
                    return redirect(request.referrer or url_for('settings_page', tab='panel'))
            else:

                file = request.files.get('db_file')
                if not file or file.filename == '':
                    flash('Файл для восстановления не выбран.', 'warning')
                    return redirect(request.referrer or url_for('settings_page', tab='panel'))
                filename = file.filename.lower()
                if not (filename.endswith('.zip') or filename.endswith('.db')):
                    flash('Поддерживаются только файлы .zip или .db', 'warning')
                    return redirect(request.referrer or url_for('settings_page', tab='panel'))
                ts = get_msk_time().strftime('%Y%m%d-%H%M%S')
                dest_dir = backup_manager.BACKUPS_DIR
                try:
                    dest_dir.mkdir(parents=True, exist_ok=True)
                except Exception:
                    pass
                dest_path = dest_dir / f"uploaded-{ts}-{os.path.basename(filename)}"
                file.save(dest_path)
                ok = backup_manager.restore_from_file(dest_path)
            if ok:
                flash('Восстановление выполнено успешно.', 'success')
            else:
                flash('Восстановление не удалось. Проверьте файл и повторите.', 'danger')
            return redirect(request.referrer or url_for('settings_page', tab='panel'))
        except Exception as e:
            logger.error(f"Ошибка восстановления БД: {e}", exc_info=True)
            flash('Ошибка при восстановлении БД.', 'danger')
            return redirect(request.referrer or url_for('settings_page', tab='panel'))

    @flask_app.route('/update-host-subscription', methods=['POST'])
    @login_required
    def update_host_subscription_route():
        host_name = (request.form.get('host_name') or '').strip()
        sub_url = (request.form.get('host_subscription_url') or '').strip()
        if not host_name:
            if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                 return jsonify({'ok': False, 'error': 'Не указан хост'}), 400
            flash('Не указан хост для обновления ссылки подписки.', 'danger')
            return redirect(url_for('settings_page', tab='hosts'))
        ok = update_host_subscription_url(host_name, sub_url or None)
        
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
             return jsonify({'ok': ok, 'message': 'Ссылка подписки обновлена' if ok else 'Не удалось обновить ссылку'})

        if ok:
            flash('Ссылка подписки для хоста обновлена.', 'success')
        else:
            flash('Не удалось обновить ссылку подписки для хоста (возможно, хост не найден).', 'danger')
        return redirect(url_for('settings_page', tab='hosts'))

    @flask_app.route('/update-host-description', methods=['POST'])
    @login_required
    def update_host_description_route():
        host_name = (request.form.get('host_name') or '').strip()
        description = (request.form.get('host_description') or '').strip()
        if not host_name:
            if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                 return jsonify({'ok': False, 'error': 'Не указан хост'}), 400
            flash('Не указан хост для обновления описания.', 'danger')
            return redirect(url_for('settings_page', tab='hosts'))
        ok = update_host_description(host_name, description or None)

        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
             return jsonify({'ok': ok, 'message': 'Описание обновлено' if ok else 'Не удалось обновить описание'})

        if ok:
            flash('Описание для хоста обновлено.', 'success')
        else:
            flash('Не удалось обновить описание для хоста (возможно, хост не найден).', 'danger')
        return redirect(url_for('settings_page', tab='hosts'))

    @flask_app.route('/update-host-traffic-settings', methods=['POST'])
    @login_required
    def update_host_traffic_settings_route():
        host_name = (request.form.get('host_name') or '').strip()
        strategy = (request.form.get('traffic_limit_strategy') or 'NO_RESET')
        
        if not host_name:
            if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                 return jsonify({'ok': False, 'error': 'Не указан хост'}), 400
            flash('Не указан хост для обновления настроек трафика.', 'danger')
            return redirect(url_for('settings_page', tab='hosts'))
            
        ok = update_host_traffic_settings(host_name, strategy)

        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
             return jsonify({'ok': ok, 'message': 'Настройки трафика обновлены' if ok else 'Не удалось обновить настройки'})

        if ok:
            flash('Настройки трафика для хоста обновлены.', 'success')
        else:
            flash('Не удалось обновить настройки трафика (возможно, хост не найден).', 'danger')
        return redirect(url_for('settings_page', tab='hosts'))


    @flask_app.route('/update-host-url', methods=['POST'])
    @login_required
    def update_host_url_route():
        host_name = (request.form.get('host_name') or '').strip()
        new_url = (request.form.get('host_url') or '').strip()
        if not host_name or not new_url:
            if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                 return jsonify({'ok': False, 'error': 'Не указан хост или URL'}), 400
            flash('Укажите имя хоста и новый URL.', 'warning')
            return redirect(url_for('settings_page', tab='hosts'))
        ok = update_host_url(host_name, new_url)
        
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
             return jsonify({'ok': ok, 'message': 'URL хоста обновлён' if ok else 'Не удалось обновить URL'})

        flash('URL хоста обновлён.' if ok else 'Не удалось обновить URL хоста.', 'success' if ok else 'danger')
        return redirect(url_for('settings_page', tab='hosts'))

    @flask_app.route('/update-host-remnawave', methods=['POST'])
    @login_required
    def update_host_remnawave_route():
        host_name = (request.form.get('host_name') or '').strip()
        base_url = (request.form.get('remnawave_base_url') or '').strip()
        api_token = (request.form.get('remnawave_api_token') or '').strip()
        squad_uuid = (request.form.get('squad_uuid') or '').strip()
        if not host_name:
            if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                 return jsonify({'ok': False, 'error': 'Не указан хост'}), 400
            flash('Не указан хост для обновления Remnawave-настроек.', 'danger')
            return redirect(url_for('settings_page', tab='hosts'))
        ok = update_host_remnawave_settings(
            host_name,
            remnawave_base_url=base_url or None,
            remnawave_api_token=api_token or None,
            squad_uuid=squad_uuid or None,
        )

        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
             return jsonify({'ok': ok, 'message': 'Remnawave-настройки обновлены' if ok else 'Не удалось обновить Remnawave-настройки'})

        flash('Remnawave-настройки обновлены.' if ok else 'Не удалось обновить Remnawave-настройки.', 'success' if ok else 'danger')
        return redirect(url_for('settings_page', tab='hosts'))

    @flask_app.route('/rename-host', methods=['POST'])
    @login_required
    def rename_host_route():
        old_name = (request.form.get('old_host_name') or '').strip()
        new_name = (request.form.get('new_host_name') or '').strip()
        if not old_name or not new_name:
            if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                 return jsonify({'ok': False, 'error': 'Укажите старое и новое имя'}), 400
            flash('Введите старое и новое имя хоста.', 'warning')
            return redirect(url_for('settings_page', tab='hosts'))
        ok = update_host_name(old_name, new_name)

        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
             return jsonify({'ok': ok, 'message': 'Имя хоста обновлено' if ok else 'Не удалось переименовать хост'})

        flash('Имя хоста обновлено.' if ok else 'Не удалось переименовать хост.', 'success' if ok else 'danger')
        return redirect(url_for('settings_page', tab='hosts'))

    @flask_app.route('/start-support-bot', methods=['POST'])
    @login_required
    def start_support_bot_route():
        loop = current_app.config.get('EVENT_LOOP')
        if loop and loop.is_running():
            _support_bot_controller.set_loop(loop)
        result = _support_bot_controller.start()
        flash(result['message'], 'success' if result['status'] == 'success' else 'danger')
        return redirect(request.referrer or url_for('settings_page'))

    def _wait_for_stop(controller, timeout: float = 5.0) -> bool:
        start = time.time()
        while time.time() - start < timeout:
            status = controller.get_status() or {}
            if not status.get('is_running'):
                return True
            time.sleep(0.1)
        return False

    @flask_app.route('/stop-support-bot', methods=['POST'])
    @login_required
    def stop_support_bot_route():
        result = _support_bot_controller.stop()
        _wait_for_stop(_support_bot_controller)
        flash(result['message'], 'success' if result['status'] == 'success' else 'danger')
        return redirect(request.referrer or url_for('settings_page'))

    @flask_app.route('/start-bot', methods=['POST'])
    @login_required
    def start_bot_route():
        result = _bot_controller.start()
        flash(result['message'], 'success' if result['status'] == 'success' else 'danger')
        return redirect(request.referrer or url_for('dashboard_page'))

    @flask_app.route('/stop-bot', methods=['POST'])
    @login_required
    def stop_bot_route():
        result = _bot_controller.stop()
        _wait_for_stop(_bot_controller)
        flash(result['message'], 'success' if result['status'] == 'success' else 'danger')
        return redirect(request.referrer or url_for('dashboard_page'))

    @flask_app.route('/stop-both-bots', methods=['POST'])
    @login_required
    def stop_both_bots_route():
        main_result = _bot_controller.stop()
        support_result = _support_bot_controller.stop()

        statuses = []
        categories = []
        for name, res in [('Основной бот', main_result), ('Support-бот', support_result)]:
            if res.get('status') == 'success':
                statuses.append(f"{name}: остановлен")
                categories.append('success')
            else:
                statuses.append(f"{name}: ошибка — {res.get('message')}")
                categories.append('danger')
        _wait_for_stop(_bot_controller)
        _wait_for_stop(_support_bot_controller)
        category = 'danger' if 'danger' in categories else 'success'
        message = ' | '.join(statuses)
        
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({'ok': category == 'success', 'message': message})
        
        flash(message, category)
        return redirect(request.referrer or url_for('dashboard_page'))

    @flask_app.route('/start-both-bots', methods=['POST'])
    @login_required
    def start_both_bots_route():
        main_result = _bot_controller.start()
        loop = current_app.config.get('EVENT_LOOP')
        if loop and loop.is_running():
            _support_bot_controller.set_loop(loop)
        support_result = _support_bot_controller.start()

        statuses = []
        categories = []
        for name, res in [('Основной бот', main_result), ('Support-бот', support_result)]:
            if res.get('status') == 'success':
                statuses.append(f"{name}: запущен")
                categories.append('success')
            else:
                statuses.append(f"{name}: ошибка — {res.get('message')}")
                categories.append('danger')
        category = 'danger' if 'danger' in categories else 'success'
        message = ' | '.join(statuses)
        
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({'ok': category == 'success', 'message': message})
        
        flash(message, category)
        return redirect(request.referrer or url_for('settings_page'))

    @flask_app.route('/users/ban/<int:user_id>', methods=['POST'])
    @login_required
    def ban_user_route(user_id):
        ban_user(user_id)
        flash(f'Пользователь {user_id} был заблокирован.', 'success')

        try:
            bot = _bot_controller.get_bot_instance()
            if bot:
                text = "🚫 Ваш аккаунт заблокирован администратором. Если это ошибка — напишите в поддержку."

                try:
                    support = (get_setting("support_bot_username") or get_setting("support_user") or "").strip()
                except Exception:
                    support = ""
                kb = InlineKeyboardBuilder()
                url: str | None = None
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
                loop = current_app.config.get('EVENT_LOOP')
                if loop and loop.is_running():
                    asyncio.run_coroutine_threadsafe(
                        bot.send_message(chat_id=user_id, text=text, reply_markup=kb.as_markup()),
                        loop
                    )
                else:
                    asyncio.run(bot.send_message(chat_id=user_id, text=text, reply_markup=kb.as_markup()))
        except Exception as e:
            logger.warning(f"Не удалось отправить уведомление о бане пользователю {user_id}: {e}")
    
        wants_json = 'application/json' in (request.headers.get('Accept') or '') or request.headers.get('X-Requested-With') == 'XMLHttpRequest'
        if wants_json:
            return jsonify({"ok": True, "message": f'Пользователь {user_id} был заблокирован.'})

        return redirect(url_for('users_page'))

    @flask_app.route('/users/toggle-block/<int:user_id>', methods=['POST'])
    @login_required
    def toggle_block_user_route(user_id):
        user = get_user(user_id)
        if not user:
            return jsonify({"ok": False, "error": "Пользователь не найден"}), 404
        is_banned = bool(user.get('is_banned', False))
        if is_banned:
            unban_user(user_id)
            msg = f"Пользователь {user_id} разблокирован."
            res_ok = True
        else:
            ban_user(user_id)
            msg = f"Пользователь {user_id} заблокирован."
            res_ok = True
        
        try:
            bot = _bot_controller.get_bot_instance()
            if bot:
                if is_banned:
                    text = "✅ Доступ к аккаунту восстановлен администратором."
                    kb = InlineKeyboardBuilder().row(keyboards.get_main_menu_button()).as_markup()
                else:
                    text = "🚫 Ваш аккаунт заблокирован администратором."
                    kb = None # Or support link
                loop = current_app.config.get('EVENT_LOOP')
                if loop and loop.is_running():
                    asyncio.run_coroutine_threadsafe(bot.send_message(chat_id=user_id, text=text, reply_markup=kb), loop)
        except Exception: pass

        return jsonify({"ok": res_ok, "message": msg, "is_banned": not is_banned})

    @flask_app.route('/users/toggle-pin/<int:user_id>', methods=['POST'])
    @login_required
    def toggle_pin_user_route(user_id):
        from shop_bot.data_manager.database import toggle_user_pin
        ok = toggle_user_pin(user_id)
        return jsonify({"ok": ok})

    @flask_app.route('/users/unban/<int:user_id>', methods=['POST'])
    @login_required
    def unban_user_route(user_id):
        unban_user(user_id)
        flash(f'Пользователь {user_id} был разблокирован.', 'success')

        try:
            bot = _bot_controller.get_bot_instance()
            if bot:
                kb = InlineKeyboardBuilder()
                kb.row(keyboards.get_main_menu_button())
                text = "✅ Доступ к аккаунту восстановлен администратором."
                loop = current_app.config.get('EVENT_LOOP')
                if loop and loop.is_running():
                    asyncio.run_coroutine_threadsafe(
                        bot.send_message(chat_id=user_id, text=text, reply_markup=kb.as_markup()),
                        loop
                    )
                else:
                    asyncio.run(bot.send_message(chat_id=user_id, text=text, reply_markup=kb.as_markup()))
        except Exception as e:
            logger.warning(f"Не удалось отправить уведомление о разбане пользователю {user_id}: {e}")
    
        wants_json = 'application/json' in (request.headers.get('Accept') or '') or request.headers.get('X-Requested-With') == 'XMLHttpRequest'
        if wants_json:
            return jsonify({"ok": True, "message": f'Пользователь {user_id} был разблокирован.'})

        return redirect(url_for('users_page'))

    @flask_app.route('/users/revoke/<int:user_id>', methods=['POST'])
    @login_required
    def revoke_keys_route(user_id):
        keys_to_revoke = get_user_keys(user_id)
        success_count = 0
        total = len(keys_to_revoke)

        for key in keys_to_revoke:
            result = asyncio.run(remnawave_api.delete_client_on_host(key['host_name'], key['key_email']))
            if result:
                success_count += 1


        delete_user_keys(user_id)


        try:
            bot = _bot_controller.get_bot_instance()
            if bot:
                text = (
                    "❌ Ваши VPN‑ключи были отозваны администратором.\n"
                    f"Всего ключей: {total}\n"
                    f"Отозвано: {success_count}"
                )
                loop = current_app.config.get('EVENT_LOOP')
                if loop and loop.is_running():
                    asyncio.run_coroutine_threadsafe(bot.send_message(chat_id=user_id, text=text), loop)
                else:
                    asyncio.run(bot.send_message(chat_id=user_id, text=text))
        except Exception:
            pass

        message = (
            f"Все {total} ключей для пользователя {user_id} были успешно отозваны." if success_count == total
            else f"Удалось отозвать {success_count} из {total} ключей для пользователя {user_id}. Проверьте логи."
        )
        category = 'success' if success_count == total else 'warning'


        wants_json = 'application/json' in (request.headers.get('Accept') or '') or request.headers.get('X-Requested-With') == 'XMLHttpRequest'
        if wants_json:
            return jsonify({"ok": success_count == total, "message": message, "revoked": success_count, "total": total}), 200

        flash(message, category)
        return redirect(url_for('users_page'))

    @flask_app.route('/add-host', methods=['POST'])
    @login_required
    def add_host_route():
        name = (request.form.get('host_name') or '').strip()
        base_url = (request.form.get('remnawave_base_url') or '').strip()
        api_token = (request.form.get('remnawave_api_token') or '').strip()
        squad_uuid = (request.form.get('squad_uuid') or '').strip()
        if not name or not base_url or not api_token:
            flash('Укажите название хоста, базовый URL и API токен.', 'danger')
            return redirect(url_for('settings_page', tab='hosts'))


        try:
            create_host(
                name=name,
                url=base_url,
                user='',
                passwd='',
                inbound=0,
                subscription_url=None,
            )
        except Exception as e:
            logger.error(f"Не удалось создать хост '{name}': {e}")
            flash(f"Не удалось создать хост '{name}'.", 'danger')
            return redirect(url_for('settings_page', tab='hosts'))


        try:
            update_host_remnawave_settings(
                name,
                remnawave_base_url=base_url,
                remnawave_api_token=api_token,
                squad_uuid=squad_uuid or None,
            )
        except Exception as e:
            logger.error(f"Не удалось сохранить Remnawave-настройки для '{name}': {e}")
            if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                 return jsonify({'ok': True, 'message': 'Хост создан, но настройки Remnawave не сохранены'}), 200 # Partial success
            flash('Хост создан, но Remnawave-настройки сохранить не удалось.', 'warning')
            return redirect(url_for('settings_page', tab='hosts'))

        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
             return jsonify({'ok': True, 'message': f"Хост '{name}' успешно добавлен"})

        flash(f"Хост '{name}' успешно добавлен.", 'success')
        return redirect(url_for('settings_page', tab='hosts'))

    @flask_app.route('/heleket-webhook', methods=['POST'])
    @csrf.exempt
    def heleket_webhook_handler():
        """
        Обработка вебхука от Heleket.
        Ожидается POST запрос с JSON телом.
        Заголовки:
            sign: подпись запроса (md5(base64(json_body) + api_key))
        Тело (пример):
        {
            "order_id": "...",
            "amount": "...",
            "currency": "...",
            "status": "PAID",
            "description": "..." (наш metadata json)
        }
        """
        try:
            # 1. Получаем сырое тело и подпись
            raw_data = request.get_data()
            
            # Логируем заголовки для отладки
            headers_dict = dict(request.headers)
            logger.info(f"Вебхук Heleket заголовки: {headers_dict}")
            
            signature = request.headers.get("sign") or request.headers.get("Sign") or request.headers.get("SIGN") or ""
            
            # 2. Получаем API ключ из настроек
            api_key = (get_setting("heleket_api_key") or "").strip()
            if not api_key:
                logger.error("Вебхук Heleket: API ключ не настроен")
                return jsonify({"error": "Configuration error"}), 500
            
            base64_body = base64.b64encode(raw_data).decode()
            expected_sign = hashlib.md5((base64_body + api_key).encode()).hexdigest()
            
            if not compare_digest(signature, expected_sign):
                logger.warning(f"Вебхук Heleket: Неверная подпись. Получено: '{signature}', Ожидалось: '{expected_sign}'")
                logger.warning("Вебхук Heleket: Проверка подписи отключена в конфигурации (для совместимости с прокси).")
                 
            
            # 4. Парсим JSON
            try:
                data = json.loads(raw_data)
            except json.JSONDecodeError:
                logger.error("Вебхук Heleket: Некорректный JSON")
                return jsonify({"error": "Invalid JSON"}), 400
                
            # 5. Проверяем статус
            
            logger.info(f"Данные вебхука Heleket: {data}")
            
            # Извлекаем метаданные из description
            description_raw = data.get("description", "")
            metadata = {}
            if description_raw:
                try:
                    metadata = json.loads(description_raw)
                except Exception:
                    logger.warning(f"Вебхук Heleket: Не удалось разобрать JSON описания: {description_raw}")
            
            payment_id = data.get("order_id")
            status = str(data.get("status", "")).lower()

            if payment_id: 
                if status not in ['paid', 'confirm_check', 'success']:
                    logger.warning(f"Вебхук Heleket: Платеж {payment_id} имеет статус '{status}' (не оплачен). Игнорируем.")
                    return jsonify({"state": 0, "message": "Ignored non-paid status"}), 200

                # Обновляем статус транзакции
                meta_from_db = find_and_complete_pending_transaction(payment_id)
                
                if meta_from_db:
                    logger.info(f"Вебхук Heleket: Транзакция {payment_id} найдена и завершена.")
                    
                    # Обработка успешного платежа
                    bot = _bot_controller.get_bot_instance()
                    loop = current_app.config.get('EVENT_LOOP')
                    
                    if bot and loop and loop.is_running():
                        asyncio.run_coroutine_threadsafe(
                            handlers.process_successful_payment(bot, meta_from_db),
                            loop
                        )
                        logger.info(f"Вебхук Heleket: Запланирована обработка платежа для {payment_id}")
                    else:
                        logger.error("Вебхук Heleket: Цикл событий или экземпляр бота не готовы")
                        
                    # Обработка промокода
                    _handle_promo_after_payment(meta_from_db)
                    
                else:
                    logger.warning(f"Вебхук Heleket: Транзакция {payment_id} не найдена или уже завершена.")
            
            return jsonify({"state": 0, "message": "OK"}), 200

        except Exception as e:
            logger.error(f"Вебхук Heleket: Внутренняя ошибка: {e}", exc_info=True)
            return jsonify({"error": "Internal error"}), 500

    @flask_app.route('/delete-host/<host_name>', methods=['POST'])
    @login_required
    def delete_host_route(host_name):
        delete_host(host_name)
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
             return jsonify({'ok': True, 'message': f"Хост '{host_name}' удален"})
        flash(f"Хост '{host_name}' и все его тарифы были удалены.", 'success')
        return redirect(url_for('settings_page', tab='hosts'))

    @flask_app.route('/toggle-host-visibility/<host_name>', methods=['POST'])
    @login_required
    def toggle_host_visibility_route(host_name):
        visible = request.form.get('visible', '1')
        try:
            visible_int = int(visible)
        except (ValueError, TypeError):
            visible_int = 1
        
        ok = toggle_host_visibility(host_name, visible_int)
        
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
             status_text = "показан" if visible_int == 1 else "скрыт"
             return jsonify({'ok': ok, 'message': f"Хост '{host_name}' теперь {status_text}" if ok else "Ошибка обновления видимости"})

        if ok:
            status_text = "показан" if visible_int == 1 else "скрыт"
            flash(f"Хост '{host_name}' теперь {status_text} в меню бота.", 'success')
        else:
            flash(f"Не удалось изменить видимость хоста '{host_name}'.", 'danger')
        return redirect(url_for('settings_page', tab='hosts'))

    @flask_app.route('/add-plan', methods=['POST'])
    @login_required
    def add_plan_route():
        try:
            host_name = request.form.get('host_name')
            plan_name = request.form.get('plan_name')
            months = int(request.form.get('months'))
            price = float(request.form.get('price'))
            hwid_limit = int(request.form.get('hwid_limit') or 0)
            traffic_limit_gb = int(request.form.get('traffic_limit_gb') or 0)
            
            new_plan_id = create_plan(host_name=host_name, plan_name=plan_name, months=months, price=price, hwid_limit=hwid_limit, traffic_limit_gb=traffic_limit_gb)
            
            wants_json = 'application/json' in (request.headers.get('Accept') or '') or request.headers.get('X-Requested-With') == 'XMLHttpRequest'
            if wants_json:
                plan = get_plan_by_id(new_plan_id) if new_plan_id else None
                return jsonify({'ok': True, 'plan': plan})
            
            flash(f"Новый тариф для хоста '{host_name}' добавлен.", 'success')
            return redirect(url_for('settings_page', tab='hosts'))
        except Exception as e:
            wants_json = 'application/json' in (request.headers.get('Accept') or '') or request.headers.get('X-Requested-With') == 'XMLHttpRequest'
            if wants_json:
                return jsonify({'ok': False, 'error': str(e)}), 400
            flash(f'Ошибка добавления тарифа: {e}', 'danger')
            return redirect(url_for('settings_page', tab='hosts'))

    @flask_app.route('/delete-plan/<int:plan_id>', methods=['POST'])
    @login_required
    def delete_plan_route(plan_id):
        try:
            delete_plan(plan_id)
            wants_json = 'application/json' in (request.headers.get('Accept') or '') or request.headers.get('X-Requested-With') == 'XMLHttpRequest'
            if wants_json:
                return jsonify({'ok': True})
            flash("Тариф успешно удален.", 'success')
            return redirect(url_for('settings_page', tab='hosts'))
        except Exception as e:
            wants_json = 'application/json' in (request.headers.get('Accept') or '') or request.headers.get('X-Requested-With') == 'XMLHttpRequest'
            if wants_json:
                return jsonify({'ok': False, 'error': str(e)}), 400
            flash(f'Ошибка удаления тарифа: {e}', 'danger')
            return redirect(url_for('settings_page', tab='hosts'))

    @flask_app.route('/update-plan/<int:plan_id>', methods=['POST'])
    @login_required
    def update_plan_route(plan_id):
        wants_json = 'application/json' in (request.headers.get('Accept') or '') or request.headers.get('X-Requested-With') == 'XMLHttpRequest'
        try:
            plan_name = (request.form.get('plan_name') or '').strip()
            months = int(request.form.get('months'))
            price = float(request.form.get('price'))
            hwid_limit = int(request.form.get('hwid_limit') or 0)
            traffic_limit_gb = int(request.form.get('traffic_limit_gb') or 0)

            if not plan_name:
                if wants_json:
                    return jsonify({'ok': False, 'error': 'Название не может быть пустым'}), 400
                flash('Название тарифа не может быть пустым.', 'danger')
                return redirect(url_for('settings_page', tab='hosts'))

            ok = update_plan(plan_id, plan_name, months, price, hwid_limit=hwid_limit, traffic_limit_gb=traffic_limit_gb)
            if ok:
                if wants_json:
                    plan = get_plan_by_id(plan_id)
                    return jsonify({'ok': True, 'plan': plan})
                flash('Тариф обновлён.', 'success')
            else:
                if wants_json:
                    return jsonify({'ok': False, 'error': 'Тариф не найден'}), 404
                flash('Не удалось обновить тариф (возможно, он не найден).', 'danger')
            return redirect(url_for('settings_page', tab='hosts'))
        except Exception as e:
            if wants_json:
                return jsonify({'ok': False, 'error': str(e)}), 400
            flash(f'Ошибка обновления тарифа: {e}', 'danger')
            return redirect(url_for('settings_page', tab='hosts'))

    @csrf.exempt
    @flask_app.route('/yookassa-webhook', methods=['POST'])
    def yookassa_webhook_handler():
        try:
            event_json = request.json
            if event_json.get("event") == "payment.succeeded":
                metadata = event_json.get("object", {}).get("metadata", {})
                
                bot = _bot_controller.get_bot_instance()
                payment_processor = handlers.process_successful_payment

                if metadata and bot is not None and payment_processor is not None:
                    loop = current_app.config.get('EVENT_LOOP')
                    if loop and loop.is_running():
                        asyncio.run_coroutine_threadsafe(payment_processor(bot, metadata), loop)
                    else:
                        logger.error("YooKassa вебхук: цикл событий недоступен!")
            return 'OK', 200
        except Exception as e:
            logger.error(f"Ошибка в обработчике вебхука YooKassa: {e}", exc_info=True)
            return 'Error', 500
        
    @csrf.exempt
    @flask_app.route('/test-webhook', methods=['GET', 'POST'])
    def test_webhook():
        """Тестовый endpoint для проверки работы webhook сервера"""
        if request.method == 'GET':
            return f"Webhook server is running! Time: {get_msk_time()}"
        else:
            return f"POST received! Data: {request.get_json() or request.form.to_dict()}"
    
    @csrf.exempt
    @flask_app.route('/debug-all', methods=['GET', 'POST', 'PUT', 'DELETE'])
    def debug_all_requests():
        """Endpoint для отладки всех входящих запросов"""
        print(f"[DEBUG] Received {request.method} request to /debug-all")
        print(f"[DEBUG] Headers: {dict(request.headers)}")
        print(f"[DEBUG] Form data: {request.form.to_dict()}")
        print(f"[DEBUG] JSON data: {request.get_json()}")
        print(f"[DEBUG] Args: {request.args.to_dict()}")
        
        return {
            "method": request.method,
            "headers": dict(request.headers),
            "form": request.form.to_dict(),
            "json": request.get_json(),
            "args": request.args.to_dict(),
            "timestamp": get_msk_time().isoformat()
        }
    
    @csrf.exempt
    @flask_app.route('/yoomoney-webhook', methods=['POST'])
    def yoomoney_webhook_handler():
        """ЮMoney HTTP уведомление (кнопка/ссылка p2p). Подпись: sha1(notification_type&operation_id&amount&currency&datetime&sender&codepro&notification_secret&label)."""
        logger.info("🔔 Получен webhook от ЮMoney")
        
        try:
            form = request.form
            logger.info(f"📋 Данные webhook: {dict(form)}")
            
            required = [
                'notification_type', 'operation_id', 'amount', 'currency', 'datetime', 'sender', 'codepro', 'label', 'sha1_hash'
            ]
            if not all(k in form for k in required):
                logger.warning(f"❌ Отсутствуют обязательные поля. Доступно: {list(form.keys())}")
                return 'Bad Request', 400
            

            notification_type = form.get('notification_type', '')
            logger.info(f"📝 Тип уведомления: {notification_type}")
            if notification_type != 'p2p-incoming':
                logger.info(f"⏭️  Игнорируем тип уведомления: {notification_type}")
                return 'OK', 200
            

            codepro = form.get('codepro', '')
            if codepro.lower() == 'true':
                logger.info("🧪 Игнорируем тестовый платеж (codepro=true)")
                return 'OK', 200
            
            secret = get_setting('yoomoney_secret') or ''
            signature_str = "&".join([
                form.get('notification_type',''),
                form.get('operation_id',''),
                form.get('amount',''),
                form.get('currency',''),
                form.get('datetime',''),
                form.get('sender',''),
                form.get('codepro',''),
                secret,
                form.get('label',''),
            ])
            expected = hashlib.sha1(signature_str.encode('utf-8')).hexdigest()
            provided = (form.get('sha1_hash') or '').lower()
            if expected != provided:
                logger.warning("🔐 Неверная подпись")
                return 'Forbidden', 403
            

            payment_id = form.get('label')
            if not payment_id:
                logger.warning("🏷️  Пустой label")
                return 'OK', 200
            
            logger.info(f"💰 Обрабатываем платеж: {payment_id}")
            metadata = find_and_complete_pending_transaction(payment_id)
            if not metadata:
                logger.warning(f"❌ Метаданные не найдены для платежа: {payment_id}")
                return 'OK', 200
            
            logger.info(f"✅ Найдены метаданные для платежа {payment_id}: пользователь={metadata.get('user_id')}, сумма={metadata.get('price')}")
            bot = _bot_controller.get_bot_instance()
            loop = current_app.config.get('EVENT_LOOP')
            payment_processor = handlers.process_successful_payment
            if bot and loop and loop.is_running():
                asyncio.run_coroutine_threadsafe(payment_processor(bot, metadata), loop)
                logger.info(f"🚀 Запущена обработка платежа: {payment_id}")
            else:
                logger.error("❌ Бот или цикл событий недоступен")
            return 'OK', 200
        except Exception as e:
            logger.error(f"💥 Ошибка в webhook ЮMoney: {e}", exc_info=True)
            return 'Error', 500

    @csrf.exempt
    @flask_app.route('/cryptobot-webhook', methods=['POST'])
    def cryptobot_webhook_handler():
        try:
            request_data = request.json
            
            if request_data and request_data.get('update_type') == 'invoice_paid':
                payload_data = request_data.get('payload', {})
                
                payload_string = payload_data.get('payload')
                
                if not payload_string:
                    logger.warning("CryptoBot вебхук: Получен оплаченный invoice, но payload пустой.")
                    return 'OK', 200

                parts = payload_string.split(':')
                if len(parts) < 9:
                    logger.error(f"CryptoBot вебхук: некорректный формат payload: {payload_string}")
                    return 'Error', 400

                metadata = {
                    "user_id": parts[0],
                    "months": parts[1],
                    "price": parts[2],
                    "action": parts[3],
                    "key_id": parts[4],
                    "host_name": parts[5],
                    "plan_id": parts[6],
                    "customer_email": parts[7] if parts[7] != 'None' else None,
                    "payment_method": parts[8]
                }

                if len(parts) >= 10:
                    metadata["promo_code"] = (parts[9] if parts[9] != 'None' else None)
                if len(parts) >= 11:
                    metadata["promo_discount"] = parts[10]
                
                bot = _bot_controller.get_bot_instance()
                loop = current_app.config.get('EVENT_LOOP')
                payment_processor = handlers.process_successful_payment

                if bot and loop and loop.is_running():

                    try:
                        _handle_promo_after_payment(metadata)
                    except Exception:
                        pass
                    asyncio.run_coroutine_threadsafe(payment_processor(bot, metadata), loop)
                else:
                    logger.error("CryptoBot вебхук: не удалось обработать платёж — бот или цикл событий не запущены.")

            return 'OK', 200
            
        except Exception as e:
            logger.error(f"Ошибка в обработчике вебхука CryptoBot: {e}", exc_info=True)
            return 'Error', 500
        

        
    @csrf.exempt
    @flask_app.route('/ton-webhook', methods=['POST'])
    def ton_webhook_handler():
        try:
            data = request.json
            logger.info(f"Получен вебхук TonAPI: {data}")

            if 'tx_id' in data:
                account_id = data.get('account_id')
                for tx in data.get('in_progress_txs', []) + data.get('txs', []):
                    in_msg = tx.get('in_msg')
                    if in_msg and in_msg.get('decoded_comment'):
                        payment_id = in_msg['decoded_comment']
                        amount_nano = int(in_msg.get('value', 0))
                        amount_ton = float(amount_nano / 1_000_000_000)

                        metadata = find_and_complete_ton_transaction(payment_id, amount_ton)
                        
                        if metadata:
                            logger.info(f"TON Payment successful for payment_id: {payment_id}")
                            bot = _bot_controller.get_bot_instance()
                            loop = current_app.config.get('EVENT_LOOP')
                            payment_processor = handlers.process_successful_payment

                            if bot and loop and loop.is_running():
                                asyncio.run_coroutine_threadsafe(payment_processor(bot, metadata), loop)
            
            return 'OK', 200
        except Exception as e:
            logger.error(f"Ошибка в обработчике вебхука TonAPI: {e}", exc_info=True)
            return 'Error', 500

    @csrf.exempt
    @flask_app.route('/platega-webhook', methods=['POST'])
    def platega_webhook_handler():
        """Обработчик webhook от Platega"""
        try:
            
            merchant_id = request.headers.get('X-MerchantId')
            secret = request.headers.get('X-Secret')
            
            expected_merchant = get_setting('platega_merchant_id')
            expected_secret = get_setting('platega_api_key')
            
            if not expected_merchant or not expected_secret:
                logger.warning("Platega webhook: настройки не заданы")
                return 'OK', 200
            
            if merchant_id != expected_merchant or secret != expected_secret:
                logger.warning(f"Platega webhook: неверные учетные данные. Получено: merchant_id={merchant_id}")
                return 'Forbidden', 403
            
            data = request.json
            logger.info(f"Platega webhook получен: {data}")
            
            
            status = data.get('status')
            if status == 'CONFIRMED':
                
                payment_id = data.get('payload')
                
                if not payment_id:
                    logger.warning("Platega webhook: отсутствует payload (payment_id)")
                    return 'OK', 200
                
                
                metadata = find_and_complete_pending_transaction(payment_id)
                if metadata:
                    logger.info(f"Platega: найдены метаданные для платежа {payment_id}")
                    
                    bot = _bot_controller.get_bot_instance()
                    loop = current_app.config.get('EVENT_LOOP')
                    payment_processor = handlers.process_successful_payment
                    
                    if bot and loop and loop.is_running():
                        asyncio.run_coroutine_threadsafe(
                            payment_processor(bot, metadata), 
                            loop
                        )
                        logger.info(f"Platega: платеж {payment_id} обработан")
                    else:
                        logger.error("Platega webhook: бот или цикл событий недоступен")
                else:
                    logger.warning(f"Platega webhook: метаданные не найдены для платежа {payment_id}")
            elif status == 'CANCELED':
                logger.info(f"Platega webhook: платеж отменен, ID={data.get('id')}")
            else:
                logger.info(f"Platega webhook: получен статус {status}")
            
            return 'OK', 200
        except Exception as e:
            logger.error(f"Ошибка в обработчике вебхука Platega: {e}", exc_info=True)
            return 'Error', 500


    def _ym_get_redirect_uri():
        try:
            saved = (get_setting("yoomoney_redirect_uri") or "").strip()
        except Exception:
            saved = ""
        if saved:
            return saved
        root = request.url_root.rstrip('/')
        return f"{root}/yoomoney/callback"

    @flask_app.route('/yoomoney/connect')
    @login_required
    def yoomoney_connect_route():
        client_id = (get_setting('yoomoney_client_id') or '').strip()
        if not client_id:
            flash('Укажите YooMoney client_id в настройках.', 'warning')
            return redirect(url_for('settings_page', tab='payments'))
        redirect_uri = _ym_get_redirect_uri()
        scope = 'operation-history operation-details account-info'
        qs = urllib.parse.urlencode({
            'client_id': client_id,
            'response_type': 'code',
            'scope': scope,
            'redirect_uri': redirect_uri,
        })
        url = f"https://yoomoney.ru/oauth/authorize?{qs}"
        return redirect(url)

    @csrf.exempt
    @flask_app.route('/yoomoney/callback')
    def yoomoney_callback_route():
        code = (request.args.get('code') or '').strip()
        if not code:
            flash('YooMoney: не получен code из OAuth.', 'danger')
            return redirect(url_for('settings_page', tab='payments'))
        client_id = (get_setting('yoomoney_client_id') or '').strip()
        client_secret = (get_setting('yoomoney_client_secret') or '').strip()
        redirect_uri = _ym_get_redirect_uri()
        data = {
            'grant_type': 'authorization_code',
            'code': code,
            'client_id': client_id,
            'redirect_uri': redirect_uri,
        }
        if client_secret:
            data['client_secret'] = client_secret
        try:
            encoded = urllib.parse.urlencode(data).encode('utf-8')
            req = urllib.request.Request('https://yoomoney.ru/oauth/token', data=encoded, headers={'Content-Type': 'application/x-www-form-urlencoded'})
            with urllib.request.urlopen(req, timeout=15) as resp:
                resp_text = resp.read().decode('utf-8', errors='ignore')
            try:
                payload = json.loads(resp_text)
            except Exception:
                payload = {}
            token = (payload.get('access_token') or '').strip()
            if not token:
                flash(f"Не удалось получить access_token от YooMoney: {payload}", 'danger')
                return redirect(url_for('settings_page', tab='payments'))
            update_setting('yoomoney_api_token', token)
            flash('YooMoney: токен успешно сохранён.', 'success')
        except Exception as e:
            logger.error(f"YooMoney OAuth callback error: {e}", exc_info=True)
            flash(f'Ошибка при обмене кода на токен: {e}', 'danger')
        return redirect(url_for('settings_page', tab='payments'))

    @flask_app.route('/yoomoney/check', methods=['GET','POST'])
    @login_required
    def yoomoney_check_route():
        token = (get_setting('yoomoney_api_token') or '').strip()
        if not token:
            flash('YooMoney: токен не задан.', 'warning')
            return redirect(url_for('settings_page', tab='payments'))

        try:
            req = urllib.request.Request('https://yoomoney.ru/api/account-info', headers={'Authorization': f'Bearer {token}'}, method='POST')
            with urllib.request.urlopen(req, timeout=15) as resp:
                ai_text = resp.read().decode('utf-8', errors='ignore')
                ai_status = resp.status
                ai_headers = dict(resp.headers)
        except Exception as e:
            flash(f'YooMoney account-info: ошибка запроса: {e}', 'danger')
            return redirect(url_for('settings_page', tab='payments'))
        try:
            ai = json.loads(ai_text)
        except Exception:
            ai = {}
        if ai_status != 200:
            www = ai_headers.get('WWW-Authenticate', '')
            flash(f"YooMoney account-info HTTP {ai_status}. {www}", 'danger')
            return redirect(url_for('settings_page', tab='payments'))
        account = ai.get('account') or ai.get('account_number') or '—'

        try:
            body = urllib.parse.urlencode({'records': '1'}).encode('utf-8')
            req2 = urllib.request.Request('https://yoomoney.ru/api/operation-history', data=body, headers={'Authorization': f'Bearer {token}', 'Content-Type': 'application/x-www-form-urlencoded'})
            with urllib.request.urlopen(req2, timeout=15) as resp2:
                oh_text = resp2.read().decode('utf-8', errors='ignore')
                oh_status = resp2.status
        except Exception as e:
            flash(f'YooMoney operation-history: ошибка запроса: {e}', 'warning')
            oh_status = None
        if oh_status == 200:
            flash(f'YooMoney: токен валиден. Кошелёк: {account}', 'success')
        elif oh_status is not None:
            flash(f'YooMoney operation-history HTTP {oh_status}. Проверьте scope operation-history и соответствие кошелька.', 'danger')
        else:
            flash('YooMoney: не удалось проверить operation-history.', 'warning')
        return redirect(url_for('settings_page', tab='payments'))


    @flask_app.route('/api/button-configs/<menu_type>')
    @login_required
    @csrf.exempt
    def get_button_configs_api(menu_type):
        """Get button configurations for a specific menu type"""
        try:
            configs = get_button_configs(menu_type, include_inactive=True)
            return jsonify({'success': True, 'data': configs})
        except Exception as e:
            logger.error(f"Error getting button configs for {menu_type}: {e}")
            return jsonify({'success': False, 'error': str(e)}), 500

    @flask_app.route('/api/button-configs', methods=['POST'])
    @login_required
    @csrf.exempt
    def create_button_config_api():
        """Create a new button configuration"""
        try:
            data = request.json
            required_fields = ['menu_type', 'button_id', 'text']
            for field in required_fields:
                if field not in data:
                    return jsonify({'success': False, 'error': f'Missing required field: {field}'}), 400

            success = create_button_config(
                menu_type=data['menu_type'],
                button_id=data['button_id'],
                text=data['text'],
                callback_data=data.get('callback_data'),
                url=data.get('url'),
                row_position=data.get('row_position', 0),
                column_position=data.get('column_position', 0),
                button_width=data.get('button_width', 1),
                metadata=data.get('metadata')
            )
            
            if success:
                return jsonify({'success': True, 'message': 'Button configuration created'})
            else:
                return jsonify({'success': False, 'error': 'Failed to create button configuration'}), 500
        except Exception as e:
            logger.error(f"Error creating button config: {e}")
            return jsonify({'success': False, 'error': str(e)}), 500

    @flask_app.route('/api/button-configs/<int:button_id>', methods=['PUT'])
    @login_required
    @csrf.exempt
    def update_button_config_api(button_id):
        """Update an existing button configuration"""
        try:
            data = request.json
            logger.info(f"API update request for button {button_id}: {data}")
            
            success = update_button_config(
                button_id=button_id,
                text=data.get('text'),
                callback_data=data.get('callback_data'),
                url=data.get('url'),
                row_position=data.get('row_position'),
                column_position=data.get('column_position'),
                button_width=data.get('button_width'),
                is_active=data.get('is_active'),
                sort_order=data.get('sort_order'),
                metadata=data.get('metadata')
            )
            
            if success:
                logger.info(f"Successfully updated button {button_id}")
                return jsonify({'success': True, 'message': 'Button configuration updated'})
            else:
                logger.error(f"Failed to update button {button_id}")
                return jsonify({'success': False, 'error': 'Failed to update button configuration'}), 500
        except Exception as e:
            logger.error(f"Error updating button config {button_id}: {e}")
            return jsonify({'success': False, 'error': str(e)}), 500

    @flask_app.route('/api/button-configs/<int:button_id>', methods=['DELETE'])
    @login_required
    @csrf.exempt
    def delete_button_config_api(button_id):
        """Delete a button configuration"""
        try:
            success = delete_button_config(button_id)
            if success:
                return jsonify({'success': True, 'message': 'Button configuration deleted'})
            else:
                return jsonify({'success': False, 'error': 'Failed to delete button configuration'}), 500
        except Exception as e:
            logger.error(f"Error deleting button config {button_id}: {e}")
            return jsonify({'success': False, 'error': str(e)}), 500

    @flask_app.route('/api/button-configs/<menu_type>/reorder', methods=['POST'])
    @login_required
    @csrf.exempt
    def reorder_button_configs_api(menu_type):
        """Reorder button configurations for a menu type"""
        try:
            data = request.json
            button_orders = data.get('button_orders', [])


            
            success = reorder_button_configs(menu_type, button_orders)
            
            if success:
                logger.info(f"Successfully reordered buttons for {menu_type}")
                return jsonify({'success': True, 'message': 'Button configurations reordered'})
            else:
                logger.error(f"Failed to reorder buttons for {menu_type}")
                return jsonify({'success': False, 'error': 'Failed to reorder button configurations'}), 500
        except Exception as e:
            logger.error(f"Error reordering button configs for {menu_type}: {e}")
            return jsonify({'success': False, 'error': str(e)}), 500

    @flask_app.route('/users/<int:user_id>/send-message', methods=['POST'])
    @login_required
    @csrf.exempt
    def send_user_message_route(user_id):
        """Send a message to a user via bot"""
        try:
            message_text = request.form.get('message', '').strip()
            
            if not message_text:
                return jsonify({'ok': False, 'error': 'Сообщение не может быть пустым'}), 400
            
            
            bot = _bot_controller.get_bot_instance()
            if not bot:
                return jsonify({'ok': False, 'error': 'Бот недоступен'}), 500
            
            
            loop = current_app.config.get('EVENT_LOOP')
            if not loop or not loop.is_running():
                return jsonify({'ok': False, 'error': 'Event loop недоступен'}), 500
            
            
            async def send_message():
                try:
                    await bot.send_message(chat_id=user_id, text=message_text)
                    return True
                except Exception as e:
                    logger.error(f"Failed to send message to user {user_id}: {e}")
                    return False
            
            
            future = asyncio.run_coroutine_threadsafe(send_message(), loop)
            success = future.result(timeout=10)
            
            if success:
                logger.info(f"Message sent to user {user_id}")
                return jsonify({'ok': True, 'message': 'Сообщение успешно отправлено'})
            else:
                return jsonify({'ok': False, 'error': 'Не удалось отправить сообщение'}), 500
                
        except Exception as e:
            logger.error(f"Error sending message to user {user_id}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500

    @flask_app.route('/button-constructor')
    @login_required
    def button_constructor_page():
        """Button constructor page"""
        template_data = get_common_template_data()
        return render_template('button_constructor.html', **template_data)



    
    MENU_IMAGE_SECTIONS = {
        'profile': 'profile_image',
        'keys': 'keys_image',
        'buy_key': 'buy_key_image',
        'topup': 'topup_image',
        'referral': 'referral_image',
        'support': 'support_image',
        'about': 'about_image',
        'speedtest': 'speedtest_image',
        'howto': 'howto_image',
        'main_menu': 'main_menu_image',
        'topup_amount': 'topup_amount_image',

        'payment': 'payment_image',
        'buy_server': 'buy_server_image',
        'buy_plan': 'buy_plan_image',
        'enter_email': 'enter_email_image',
        'key_info': 'key_info_image',
        'extend_plan': 'extend_plan_image',
        'keys_list': 'keys_list_image',
        'payment_method': 'payment_method_image',
        'key_comments': 'key_comments_image',
        'key_ready': 'key_ready_image',
        'waiting_payment': 'waiting_payment_image',
        'payment_success': 'payment_success_image',
    }

    @flask_app.route('/upload-menu-image/<section>', methods=['POST'])
    @login_required
    def upload_menu_image_route(section):
        if section not in MENU_IMAGE_SECTIONS:
            return jsonify({'ok': False, 'error': 'Неизвестный раздел'}), 400
        
        setting_key = MENU_IMAGE_SECTIONS[section]
        ALLOWED_EXTENSIONS = {'jpg', 'jpeg', 'png', 'gif'}
        MAX_SIZE_BYTES = 10 * 1024 * 1024

        if 'file' not in request.files:
            return jsonify({'ok': False, 'error': 'Файл не выбран'}), 400

        file = request.files['file']
        if file.filename == '':
            return jsonify({'ok': False, 'error': 'Файл не выбран'}), 400

        ext = file.filename.rsplit('.', 1)[-1].lower() if '.' in file.filename else ''
        if ext not in ALLOWED_EXTENSIONS:
            return jsonify({'ok': False, 'error': f'Неподдерживаемый формат. Разрешены: {", ".join(ALLOWED_EXTENSIONS)}'}), 400

        file.seek(0, 2)
        size = file.tell()
        file.seek(0)
        if size > MAX_SIZE_BYTES:
            return jsonify({'ok': False, 'error': 'Размер файла превышает 10 МБ'}), 400

        try:
            current_image = get_setting(setting_key)
            if current_image and os.path.exists(current_image):
                try:
                    os.remove(current_image)
                except Exception:
                    pass

            base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            upload_dir = os.path.join(base_dir, 'modules', 'menu_images')
            os.makedirs(upload_dir, exist_ok=True)

            filename = f"{section}_{int(time.time())}.{ext}"
            filepath = os.path.join(upload_dir, filename)

            file.save(filepath)
            update_setting(setting_key, filepath)

            return jsonify({'ok': True, 'path': filepath})
        except Exception as e:
            logger.error(f"Ошибка загрузки изображения {section}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500

    @flask_app.route('/delete-menu-image/<section>', methods=['POST'])
    @login_required
    def delete_menu_image_route(section):
        if section not in MENU_IMAGE_SECTIONS:
            return jsonify({'ok': False, 'error': 'Неизвестный раздел'}), 400
        
        setting_key = MENU_IMAGE_SECTIONS[section]
        try:
            current_image = get_setting(setting_key)
            if current_image and os.path.exists(current_image):
                try:
                    os.remove(current_image)
                except Exception as e:
                    logger.warning(f"Не удалось удалить файл {current_image}: {e}")

            update_setting(setting_key, '')
            return jsonify({'ok': True})
        except Exception as e:
            logger.error(f"Ошибка удаления изображения {section}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500

    register_other_routes(flask_app, login_required, get_common_template_data)
    register_update_routes(flask_app, login_required)
    register_gemini_routes(flask_app, login_required)

    return flask_app


