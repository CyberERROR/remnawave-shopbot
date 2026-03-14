from typing import Any
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
import aiohttp
from shop_bot.data_manager.remnawave_repository import get_setting, get_user_keys, get_msk_time, get_webapp_settings, get_user, get_referral_count, get_all_hosts, list_squads, get_plans_for_host
import os
from datetime import datetime, timedelta, timezone
from pydantic import BaseModel
import uuid
import asyncio
from aiogram import Bot
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo, FSInputFile, LabeledPrice
from aiogram.utils.keyboard import InlineKeyboardBuilder
import json
import traceback
from shop_bot.bot.keyboards import (
    create_payment_keyboard, 
    create_yoomoney_payment_keyboard, 
    create_cryptobot_payment_keyboard
)
from shop_bot.data_manager.remnawave_repository import (
    create_payload_pending, get_plan_by_id,
    deduct_from_balance, check_transaction_exists, add_to_balance, log_transaction,
    add_to_referral_balance_all, get_balance, get_all_users, is_admin, update_user_stats,
    redeem_promo_code, update_promo_code_status, record_key_from_payload, get_key_by_id,
    update_key, get_key_by_email
)
import shop_bot.data_manager.remnawave_repository as rw_repo
from shop_bot.data_manager.database import get_seller_user, get_device_tiers, get_host
from shop_bot.modules import remnawave_api
from shop_bot.config import get_purchase_success_text
import re
from decimal import Decimal
import logging
from urllib.parse import urlencode


logger = logging.getLogger(__name__)

# In-memory storage for temporary auth tokens: {token: user_id}
TEMP_AUTH_TOKENS = {}


# ===== Utility Functions =====
def get_transaction_comment(user_data: dict, action_type: str, value: any, host_name: str = None) -> str:
    from shop_bot.bot.handlers import get_transaction_comment as bot_get_comment
    from aiogram.types import User
    
    # Adapt dictionary to types.User if needed by bot function
    tg_user = User(
        id=user_data.get('id', 0),
        is_bot=False,
        first_name=user_data.get('first_name', 'User'),
        username=user_data.get('username')
    )
    return bot_get_comment(tg_user, action_type, value, host_name)

def calculate_webapp_price(price: float, user_id: int) -> float:
    try:
        user = get_user(user_id)
        if not user: return price
        
        # 1. Seller Discount
        if user.get('seller_active'):
            seller = get_seller_user(user_id)
            if seller and seller.get('seller_sale'):
                discount_percent = float(seller['seller_sale'])
                price -= price * (discount_percent / 100)
        
        # 2. Referral Discount (First purchase)
        if user.get('referred_by') and user.get('total_spent', 0) == 0:
            ref_discount = get_setting("referral_discount")
            if ref_discount:
                try:
                    d_val = float(ref_discount)
                    if d_val > 0:
                        price -= price * (d_val / 100)
                except: pass
                
    except Exception as e:
        logger.error(f"Error calculating price: {e}")
        
    return round(price, 2)

# ===== HELPER FUNCTIONS FOR PAYMENT PROCESS =====
async def notify_admin_of_purchase(bot: Bot, metadata: dict):
    from shop_bot.bot.handlers import notify_admin_of_purchase as bot_notify
    await bot_notify(bot, metadata)

async def process_successful_payment(bot: Bot, metadata: dict):
    from shop_bot.bot.handlers import process_successful_payment as bot_process
    await bot_process(bot, metadata)

async def _send_telegram_message(user_id: int, text: str, reply_markup=None, photo=None):
    token = get_setting("telegram_bot_token")
    if not token: return False
    bot = Bot(token=token)
    try:
        if photo:
            await bot.send_photo(chat_id=user_id, photo=photo, caption=text, reply_markup=reply_markup, parse_mode="HTML")
        else:
            await bot.send_message(chat_id=user_id, text=text, reply_markup=reply_markup, parse_mode="HTML")
        return True
    except Exception as e:
        logger.error(f"Error sending telegram message: {e}")
        return False
    finally:
        await bot.session.close()

async def _send_invoice_stars(user_id: int, title: str, description: str, payload: str, amount: int):
    token = get_setting("telegram_bot_token")
    if not token: return False
    bot = Bot(token=token)
    try:
        await bot.send_invoice(
            chat_id=user_id,
            title=title,
            description=description,
            payload=payload,
            provider_token="", 
            currency="XTR",
            prices=[LabeledPrice(label=title, amount=amount)]
        )
        return True
    except Exception as e:
        logger.error(f"Error sending Stars invoice: {e}")
        return False
    finally:
        await bot.session.close()


from shop_bot.modules.platega_api import PlategaAPI
from shop_bot.modules.heleket_api import create_heleket_payment_request
from shop_bot.bot.keyboards import (
    create_payment_keyboard, create_cryptobot_payment_keyboard,
    create_yoomoney_payment_keyboard
)
from shop_bot.bot.handlers import create_cryptobot_api_invoice, process_successful_payment
from yookassa import Configuration as YookassaConfiguration, Payment as YookassaPayment
from aiogram.types import BufferedInputFile
import io
import qrcode
from urllib.parse import urlencode

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
        "successURL": f"https://t.me/{get_setting('telegram_bot_username')}",
    }
    return base + "?" + urlencode(params)

app = FastAPI()

ico_dir = os.path.join(os.path.dirname(__file__), "module", "ico")
if os.path.exists(ico_dir):
    app.mount("/module/ico", StaticFiles(directory=ico_dir), name="ico")

def _format_remaining_details(remaining: timedelta) -> str:
    total_seconds = int(remaining.total_seconds())
    if total_seconds <= 0:
        return "0мин"

    minutes = (total_seconds // 60) % 60
    hours = (total_seconds // 3600) % 24
    days = remaining.days % 365
    years = remaining.days // 365

    parts = []
    if years > 0:
        parts.append(f"{years}г.")
    if days > 0:
        parts.append(f"{days}д.")
    if hours > 0:
        parts.append(f"{hours}ч.")
    if minutes > 0:
        parts.append(f"{minutes}мин")

    # Берем только первые две значимые части для краткости
    result_parts = parts[:2]
    return " ".join(result_parts) if result_parts else "меньше минуты"

def _format_bytes(size: Any) -> str:
    if size is None: return "0 B"
    if isinstance(size, str):
        if any(x in size for x in ['B', 'KB', 'MB', 'GB', 'TB', 'iB']):
            return size
        try: size = float(size)
        except: return "0 B"
    
    if size <= 0: return "0 B"
    power = 1024
    n = 0
    power_labels = {0 : 'B', 1: 'KB', 2: 'MB', 3: 'GB', 4: 'TB'}
    while size >= power and n < 4:
        size /= power
        n += 1
    return f"{size:.2f} {power_labels[n]}"

def _process_template_placeholders(html: str, user_id: int, webapp_settings: dict, context_data: dict) -> str:
    title = webapp_settings.get("webapp_title") or get_setting("panel_brand_title") or "CABINET VPN"
    support_username = get_setting("support_bot_username") or ""
    
    replacements = {
        "{{ panel_brand_title }}": title,
        "{{ user_profile_card }}": context_data.get("profile_card", ""),
        "{{ key_info_section }}": context_data.get("key_section", ""),
        "{{ profile_keys_list }}": context_data.get("profile_keys_list", ""),
        "{{ setup_keys_list }}": context_data.get("setup_keys_list", ""),
        "{{ renew_keys_dropdown_options }}": context_data.get("renew_keys_options", ""),
        "{{ renew_plans_grid }}": context_data.get("renew_plans_html_data", ""),
        "{{ support_bot_username }}": support_username,
        "{{ min_price }}": context_data.get("min_price", "0 ₽"),
        "{{ webapp_logo }}": context_data.get("webapp_logo", ""),
        "{{ webapp_icon }}": context_data.get("webapp_icon", ""),
        "{{ logo_hidden }}": "hidden" if not context_data.get("webapp_logo") else "",
        "{{ user_id }}": str(user_id),
        "{{ tg_fullscreen_css }}": """
    <style>
        .tg-miniapp #main-page,
        .tg-miniapp #purchase-page,
        .tg-miniapp #renew-page,
        .tg-miniapp #setup-page,
        .tg-miniapp #profile-page,
        .tg-miniapp #support-page {
            padding-top: max(env(safe-area-inset-top), 70px) !important;
        }
    </style>
        """ if webapp_settings.get("tg_fullscreen") else "",
    }
    
    # Selected key display variants
    display_val = context_data.get("renew_selected_display", "Нет активных ключей")
    replacements["{{ renew_selected_key_display }}"] = display_val
    replacements["{{\n                                renew_selected_key_display }}"] = display_val

    for placeholder, value in replacements.items():
        html = html.replace(placeholder, value)
    
    server_options, server_plans = _get_servers_and_plans_html(user_id)
    html = html.replace("{{ server_dropdown_options }}", server_options)
    html = html.replace("{{ server_plans_grid }}", server_plans)
    
    return html

def _process_key_data(key: dict) -> dict:
    # 1. Calculate expiry
    try:
        expire_dt = datetime.strptime(key['expiry_date'], "%Y-%m-%d %H:%M:%S")
        created_dt = datetime.strptime(key.get('created_at', key['expiry_date']), "%Y-%m-%d %H:%M:%S")
        expire_date_str = expire_dt.strftime("%d.%m.%Y")
    except (ValueError, TypeError):
        expire_dt = datetime.now()
        created_dt = datetime.now()
        expire_date_str = "Unknown"
    
    now = get_msk_time().replace(tzinfo=None)
    
    # 2. Days left & Detailed remaining
    delta = expire_dt - now
    days_left = delta.days
    if days_left < 0:
        days_left = 0
        
    remaining_str = _format_remaining_details(delta) if delta.total_seconds() > 0 else "Истек"

    # 3. Progress
    total_duration = (expire_dt - created_dt).total_seconds()
    elapsed_delta = now - created_dt
    elapsed = elapsed_delta.total_seconds()
    elapsed_str = _format_remaining_details(elapsed_delta) if elapsed > 0 else "0мин"
    
    if total_duration > 0:
        percent = (elapsed / total_duration) * 100
    else:
        percent = 100
        
    percent = max(0, min(100, percent))
    percent_str = f"{percent:.1f}%"
    
    # 4. Display Name
    key_name = key.get('name')
    if not key_name:
        # User requested: Key #email_username (sannilo@bot.local -> Ключ #sannilo)
        email = key.get('email') or key.get('key_email') or ""
        if email.endswith("@bot.local"):
            email = email[:-10]
        
        if email:
            key_name = f"Ключ #{email}"
        elif key.get('short_uuid'):
            key_name = f"Ключ #{key.get('short_uuid')}"
        else:
            key_name = f"Ключ #{key.get('key_id')}"
        
    # 5. Subscription URL
    sub_url = key.get('subscription_url') or key.get('key') or ""

    # 6. Limits
    traffic_limit = key.get('limit_bytes')
    traffic_used = key.get('used_bytes', 0)
    
    formatted_used = _format_bytes(traffic_used)
    
    traffic_str = "∞"
    if traffic_limit:
        try:
            t_lim_float = float(traffic_limit)
            if t_lim_float > 0:
                traffic_str = _format_bytes(t_lim_float)
            else:
                traffic_str = "∞"
        except (ValueError, TypeError):
            traffic_str = "∞"
    
    hwid_limit = key.get('limit_ips')
    hwid_usage = key.get('used_ips', 0)
    
    limit_display = "∞"
    if hwid_limit is not None:
        try:
            limit_val = int(hwid_limit)
            if limit_val > 0 and limit_val < 99:
                 limit_display = str(limit_val)
            else:
                 limit_display = "∞"
        except (ValueError, TypeError):
            limit_display = "∞"

    hwid_str = f"{hwid_usage} / {limit_display}"
    
    # Safety: Created Date String
    created_date_str = created_dt.strftime("%d.%m.%Y")

    if days_left > 5:
        status_text = "Активен"
        status_color = "text-emerald-500"
        status_bg = "bg-emerald-500/10"
    elif days_left > 0:
        status_text = "Скоро"
        status_color = "text-yellow-500"
        status_bg = "bg-yellow-500/10"
    else:
        status_text = "Истек"
        status_color = "text-red-500"
        status_bg = "bg-red-500/10"

    return {
        "key_id": key.get('key_id'),
        "name": key_name,
        "expire_date_str": expire_date_str,
        "days_left": days_left,
        "percent_str": percent_str,
        "sub_url": sub_url,
        "expiry_dt": expire_dt,
        "remaining_str": remaining_str,
        "created_date_str": created_date_str,
        "elapsed_str": elapsed_str,
        "traffic_info": f"{formatted_used} / {traffic_str}", 
        "hwid_info": f"{hwid_str} уст.",
        "status_text": status_text,
        "status_color": status_color,
        "status_bg": status_bg,
        "comment_key": key.get('comment_key') or "",
        "host_name": key.get('host_name') or "",
    }

def _get_key_html(key: dict) -> str:
    data = _process_key_data(key)
    
    html = f"""
        <section
            class="bg-white dark:bg-surface-dark border border-gray-200 dark:border-surface-highlight-dark rounded-2xl p-5 shadow-sm relative overflow-hidden group">
            <div class="absolute -top-10 -right-10 w-32 h-32 bg-primary/20 rounded-full blur-3xl dark:block hidden">
            </div>
            <div class="flex flex-col gap-1 mb-4">
                <!-- Row 1: Status & Date -->
                <div class="flex justify-between items-center h-6">
                    <div class="flex items-center gap-2">
                        <span class="relative flex h-3 w-3">
                            <span
                                class="animate-ping absolute inline-flex h-full w-full rounded-full bg-primary opacity-75"></span>
                            <span class="relative inline-flex rounded-full h-3 w-3 bg-primary"></span>
                        </span>
                        <span class="font-bold text-lg text-gray-900 dark:text-white leading-none">Активна</span>
                    </div>
                    <div class="font-semibold text-sm leading-none text-right">{data['expire_date_str']}</div>
                </div>

                <!-- Row 2: Key Name & Days Left Badge -->
                <div class="flex justify-between items-center h-6">
                    <div class="flex items-center gap-1.5 text-gray-500 dark:text-gray-400 text-sm">
                        <span class="material-icons-round text-base">vpn_key</span>
                        <span>{data['name']}</span>
                    </div>
                    <div
                        class="bg-surface-highlight-dark/10 dark:bg-surface-highlight-dark px-2 py-0.5 rounded text-[10px] font-medium text-gray-600 dark:text-gray-300">
                        {data['days_left']} дн.
                    </div>
                </div>
            </div>
            <div class="mt-6">
                <div class="flex justify-between text-xs mb-2">
                    <span class="text-gray-500 dark:text-gray-400">Использовано</span>
                    <span class="font-bold text-primary">{data['percent_str']}</span>
                </div>
                <div class="w-full bg-gray-100 dark:bg-black rounded-full h-2 overflow-hidden">
                    <div class="bg-primary h-2 rounded-full progress-bar shadow-[0_0_10px_rgba(16,185,129,0.5)]" style="width: {data['percent_str']}"></div>
                </div>
            </div>
        </section>
    """
    return html

def _get_profile_card_html(user: dict | None, referral_count: int, keys_count: int, referral_earned: float = 0.0) -> str:
    if not user:
        return ""
        
    user_id = user.get("telegram_id")
    balance = user.get("balance") or 0.0
    reg_date = user.get("registration_date")
    
    # Format currency: 1 240,50 ₽
    balance_str = f"{balance:,.2f}".replace(",", " ").replace(".", ",") + " ₽"
    earned_str = f"{referral_earned:,.2f}".replace(",", " ").replace(".", ",") + " ₽"
    
    # Format date and calculate time since
    reg_date_str = "Unknown"
    time_since_str = ""
    if reg_date:
        try:
             if isinstance(reg_date, str):
                 try:
                    dt = datetime.strptime(reg_date, "%Y-%m-%d %H:%M:%S")
                 except ValueError:
                    dt = datetime.fromisoformat(reg_date)
             else:
                 dt = reg_date
                 
             reg_date_str = dt.strftime("%d.%m.%Y")
             
             # Calculate relative time
             now = get_msk_time().replace(tzinfo=None)
             diff = now - dt.replace(tzinfo=None)
             days = max(0, diff.days)
             
             if days < 31:
                 time_since_str = f"{days} д."
             elif days < 365:
                 m = days // 30
                 d = days % 30
                 time_since_str = f"{m}м. {d}д." if d > 0 else f"{m}м."
             else:
                 y = days // 365
                 rem = days % 365
                 m = rem // 30
                 d = rem % 30
                 bits = [f"{y}г."]
                 if m > 0: bits.append(f"{m}м.")
                 if d > 0: bits.append(f"{d}д.")
                 time_since_str = " ".join(bits)
        except:
             pass

    return f"""
            <!-- Modern Balanced User Card -->
            <div class="glass-card border border-white/10 rounded-[2rem] p-6 relative overflow-hidden shadow-xl">
                <!-- Decoration -->
                <div class="absolute -top-10 -right-10 w-32 h-32 bg-primary/5 rounded-full blur-3xl"></div>

                <div class="flex flex-col gap-5 relative z-10">
                    <!-- Top: ID and Status -->
                    <div class="flex items-center justify-between">
                        <div class="flex items-center gap-3">
                            <div
                                class="w-10 h-10 bg-primary/10 rounded-xl flex items-center justify-center border border-primary/20">
                                <span class="material-icons-round text-primary">person</span>
                            </div>
                            <div>
                                <div class="text-[10px] text-gray-500 uppercase font-black tracking-widest">ID
                                    пользователя</div>
                                <div class="text-base font-black text-white tracking-tight">#{user_id}</div>
                            </div>
                        </div>
                        <div class="text-right">
                            <div class="text-[10px] text-gray-500 uppercase font-black tracking-widest">Баланс</div>
                            <div class="text-lg font-black text-primary tracking-tighter">{balance_str}</div>
                        </div>
                    </div>

                    <!-- Middle: Main Stats -->
                    <div class="grid grid-cols-3 gap-2">
                        <div
                            class="bg-white/5 border border-white/5 rounded-2xl p-2.5 flex flex-col items-center justify-center text-center transition-all hover:bg-white/[0.08]">
                            <span class="material-icons-round text-emerald-400 text-sm mb-1 opacity-80">group</span>
                            <div class="text-[9px] text-gray-400 uppercase font-black tracking-tight leading-none mb-1">Рефералы</div>
                            <div class="text-[11px] font-black text-white">{referral_count} чел.</div>
                        </div>
                        <div
                            class="bg-white/5 border border-white/5 rounded-2xl p-2.5 flex flex-col items-center justify-center text-center transition-all hover:bg-white/[0.08]">
                            <span class="material-icons-round text-yellow-400 text-sm mb-1 opacity-80">payments</span>
                            <div class="text-[9px] text-gray-400 uppercase font-black tracking-tight leading-none mb-1">Доход</div>
                            <div class="text-[11px] font-black text-white truncate w-full px-1">{earned_str}</div>
                        </div>
                        <div
                            class="bg-white/5 border border-white/5 rounded-2xl p-2.5 flex flex-col items-center justify-center text-center transition-all hover:bg-white/[0.08]">
                            <span class="material-icons-round text-primary text-sm mb-1 opacity-80">vpn_key</span>
                            <div class="text-[9px] text-gray-400 uppercase font-black tracking-tight leading-none mb-1">Ключи</div>
                            <div class="text-[11px] font-black text-white">{keys_count} шт.</div>
                        </div>
                    </div>

                    <!-- Bottom: Meta Info -->
                    <div class="flex items-center justify-center gap-2 pt-1">
                        <span class="material-icons-round text-[12px] text-gray-600">calendar_today</span>
                        <span class="text-[10px] text-gray-500 font-bold uppercase tracking-widest">Дата
                            регистрации:</span>
                        <span class="text-[10px] text-gray-300 font-black">{reg_date_str} ({time_since_str})</span>
                    </div>
                </div>
            </div>
    """

def _get_profile_keys_html(keys: list) -> str:
    if not keys:
        return _get_no_key_html()
    
    html = ""
    
    for key in keys:
        data = _process_key_data(key)
        
        html += f"""
        <div class="glass-card border border-white/10 rounded-2xl relative overflow-hidden shadow-lg transition-all hover:border-primary/30 group mb-3">
            <div class="absolute inset-0 bg-gradient-to-r from-primary/0 via-primary/5 to-primary/0 translate-x-[-100%] group-hover:translate-x-[100%] transition-transform duration-700 pointer-events-none"></div>

            <button class="key-toggle w-full p-3 flex items-center justify-between relative z-10 transition-colors hover:bg-white/5">
                <div class="flex items-center gap-3">
                    <div class="w-9 h-9 bg-white/5 rounded-xl flex items-center justify-center group-hover:bg-primary/10 transition-colors shrink-0">
                        <span class="material-icons-round text-gray-400 group-hover:text-primary transition-colors text-lg">vpn_key</span>
                    </div>
                    
                    <div class="text-left overflow-hidden">
                        <div class="text-xs font-bold text-white group-hover:text-primary transition-colors truncate">{data['name']}</div>
                        <div class="text-[9px] text-gray-500 font-medium uppercase tracking-wider truncate">
                           До {data['expire_date_str']} ({data['remaining_str']})
                        </div>
                    </div>
                </div>

                <div class="flex items-center gap-2 shrink-0">
                     <span class="text-[9px] {data['status_bg']} {data['status_color']} px-2 py-0.5 rounded-full font-bold uppercase tracking-wider">{data['status_text']}</span>
                     <div class="w-7 h-7 rounded-full bg-white/5 flex items-center justify-center group-hover:bg-primary/20 transition-colors">
                        <span class="material-icons-round text-gray-500 text-sm group-hover:text-white transition-colors rotate-icon">expand_more</span>
                     </div>
                </div>
            </button>

            <div class="key-content px-3 relative z-10 transition-all duration-300"> 
                 <div class="pb-3 pt-2 flex flex-col gap-2 border-t border-white/5">
                 
                     <!-- KEY INFO BLOCK -->
                     <div class="flex flex-col gap-1 px-1 py-1 text-[10px]">
                        <!-- Row 1: Time -->
                        <div class="flex flex-wrap justify-between items-center gap-x-2 gap-y-1 border-b border-white/5 pb-1.5 mb-1.5 opacity-90">
                            <div class="flex items-center gap-1">
                                <span class="text-gray-500 font-medium shrink-0">⏳ Осталось:</span>
                                <span class="text-gray-200 font-mono tracking-tight whitespace-nowrap">{data['remaining_str']}</span>
                            </div>
                            <div class="w-px h-3 bg-white/10"></div>
                            <div class="flex items-center gap-1">
                                <span class="text-gray-500 font-medium shrink-0">➕ Куплен:</span>
                                <span class="text-gray-200 font-mono tracking-tight whitespace-nowrap">{data['elapsed_str']}</span>
                            </div>
                        </div>
                        
                        <!-- Row 2: Limits -->
                        <div class="flex justify-between items-center opacity-90">
                            <div class="flex items-center gap-1.5">
                                <span class="text-gray-500 whitespace-nowrap">🛰 Лимит:</span>
                                <span class="text-gray-300 font-mono whitespace-nowrap">{data['traffic_info']}</span>
                            </div>
                            <div class="w-px h-3 bg-white/10 mx-1"></div>
                            <div class="flex items-center gap-1.5">
                                <span class="text-gray-500 whitespace-nowrap">📱 Лимит:</span>
                                <span class="text-gray-300 font-mono whitespace-nowrap">{data['hwid_info']}</span>
                            </div>
                        </div>
                     </div>
                 
                     <!-- COMMENTS BLOCK -->
                     <div id="comment-block-{data['key_id']}" class="{'hidden' if not data.get('comment_key') else 'flex'} items-center opacity-90 px-1 py-1 mb-2 mt-1 relative">
                         <div class="w-1/2 flex items-center pr-2">
                             <span class="text-[9px] text-gray-500 font-bold uppercase tracking-wider whitespace-nowrap">Комментарий:</span>
                         </div>
                         <div class="absolute left-1/2 -translate-x-1/2 w-px h-3 bg-white/10 shrink-0"></div>
                         <div class="w-1/2 pl-2 text-right overflow-hidden flex justify-end">
                             <span id="comment-text-{data['key_id']}" class="text-[10px] text-gray-300 break-words">{data.get('comment_key', '')}</span>
                         </div>
                     </div>

                     <div class="flex items-center gap-2 bg-black/20 rounded-xl p-2 border border-white/5 group/copy hover:border-primary/30 transition-colors">
                         <div class="flex-1 min-w-0">
                             <div class="text-[9px] text-gray-500 font-bold uppercase tracking-wider mb-0.5">Ссылка</div>
                             <div class="text-[10px] text-gray-300 font-mono truncate transition-colors group-hover/copy:text-white">{data['sub_url']}</div>
                         </div>
                         <button onclick="copyKey(this, '{data['sub_url']}')" 
                            class="w-7 h-7 rounded-lg bg-white/5 text-white flex items-center justify-center hover:bg-white/10 transition-all active:scale-95 shrink-0 shadow-sm">
                             <span class="material-icons-round text-sm">content_copy</span>
                         </button>
                     </div>

                     <button onclick="openLinkSafe('{data['sub_url']}')"
                        class="w-full bg-white text-black py-2.5 rounded-xl font-bold text-[10px] uppercase tracking-wider shadow-[0_4px_15px_rgba(255,255,255,0.1)] hover:shadow-[0_6px_20px_rgba(255,255,255,0.2)] active:scale-[0.98] transition-all flex items-center justify-center gap-2">
                         <span class="material-icons-round text-sm">bolt</span>
                         <span>Подключить</span>
                     </button>
                     
                     <div class="grid grid-cols-2 gap-2 mt-1">
                         <button onclick="openActionModal('devices', {data['key_id']}, '{data.get('host_name', '')}')"
                             class="w-full bg-white/5 text-white py-2 rounded-xl font-bold text-[10px] uppercase tracking-wider hover:bg-white/10 active:scale-[0.98] transition-all flex items-center justify-center gap-1.5 border border-white/5 hover:border-white/10">
                             <span class="material-icons-round text-sm">devices</span>
                             <span>Устройства</span>
                         </button>
                         <button onclick="openActionModal('comment', {data['key_id']}, '{data.get('comment_key', '')}')"
                             class="w-full bg-white/5 text-white py-2 rounded-xl font-bold text-[10px] uppercase tracking-wider hover:bg-white/10 active:scale-[0.98] transition-all flex items-center justify-center gap-1.5 border border-white/5 hover:border-white/10">
                             <span class="material-icons-round text-sm">edit_note</span>
                             <span>Комментарии</span>
                         </button>
                     </div>
                </div>
            </div>
        </div>
        """
    return html

def _get_setup_keys_html(keys: list) -> str:
    if not keys:
        return _get_no_key_html()
        
    html = ""
    for key in keys:
        data = _process_key_data(key)
        
        if data['days_left'] <= 0:
            continue
            
        html += f"""
        <div class="glass-card border border-white/10 rounded-2xl relative overflow-hidden shadow-lg transition-all hover:border-primary/30 group mb-3">
            <div class="absolute inset-0 bg-gradient-to-r from-primary/0 via-primary/5 to-primary/0 translate-x-[-100%] group-hover:translate-x-[100%] transition-transform duration-700 pointer-events-none"></div>

            <button class="key-toggle w-full p-3 flex items-center justify-between relative z-10 transition-colors hover:bg-white/5">
                <div class="flex items-center gap-3">
                    <div class="w-9 h-9 bg-white/5 rounded-xl flex items-center justify-center group-hover:bg-primary/10 transition-colors shrink-0">
                        <span class="material-icons-round text-gray-400 group-hover:text-primary transition-colors text-lg">vpn_key</span>
                    </div>
                    
                    <div class="text-left overflow-hidden">
                        <div class="text-xs font-bold text-white group-hover:text-primary transition-colors truncate">{data['name']}</div>
                        <div class="text-[9px] text-gray-500 font-medium uppercase tracking-wider truncate">
                           До {data['expire_date_str']} ({data['remaining_str']})
                        </div>
                    </div>
                </div>

                <div class="flex items-center gap-2 shrink-0">
                     <span class="text-[9px] {data['status_bg']} {data['status_color']} px-2 py-0.5 rounded-full font-bold uppercase tracking-wider">{data['status_text']}</span>
                     <div class="w-7 h-7 rounded-full bg-white/5 flex items-center justify-center group-hover:bg-primary/20 transition-colors">
                        <span class="material-icons-round text-gray-500 text-sm group-hover:text-white transition-colors rotate-icon">expand_more</span>
                     </div>
                </div>
            </button>

            <div class="key-content px-3 relative z-10 transition-all duration-300"> 
                 <div class="pb-3 pt-2 flex flex-col gap-2 border-t border-white/5">
                 
                     <!-- COMMENTS BLOCK -->
                     <div id="comment-block-{data['key_id']}" class="{'hidden' if not data.get('comment_key') else 'flex'} items-center opacity-90 px-1 py-1 mb-2 mt-1 relative">
                         <div class="w-1/2 flex items-center pr-2">
                             <span class="text-[9px] text-gray-500 font-bold uppercase tracking-wider whitespace-nowrap">Комментарий:</span>
                         </div>
                         <div class="absolute left-1/2 -translate-x-1/2 w-px h-3 bg-white/10 shrink-0"></div>
                         <div class="w-1/2 pl-2 text-right overflow-hidden flex justify-end">
                             <span id="comment-text-{data['key_id']}" class="text-[10px] text-gray-300 break-words">{data.get('comment_key', '')}</span>
                         </div>
                     </div>

                     <div class="flex items-center gap-2 bg-black/20 rounded-xl p-2 border border-white/5 group/copy hover:border-primary/30 transition-colors">
                         <div class="flex-1 min-w-0">
                             <div class="text-[9px] text-gray-500 font-bold uppercase tracking-wider mb-0.5">Ссылка</div>
                             <div class="text-[10px] text-gray-300 font-mono truncate transition-colors group-hover/copy:text-white">{data['sub_url']}</div>
                         </div>
                         <button onclick="copyKey(this, '{data['sub_url']}')" 
                            class="w-7 h-7 rounded-lg bg-white/5 text-white flex items-center justify-center hover:bg-white/10 transition-all active:scale-95 shrink-0 shadow-sm">
                             <span class="material-icons-round text-sm">content_copy</span>
                         </button>
                     </div>

                     <button onclick="openLinkSafe('{data['sub_url']}')"
                        class="w-full bg-white text-black py-2.5 rounded-xl font-bold text-[10px] uppercase tracking-wider shadow-[0_4px_15px_rgba(255,255,255,0.1)] hover:shadow-[0_6px_20px_rgba(255,255,255,0.2)] active:scale-[0.98] transition-all flex items-center justify-center gap-2">
                         <span class="material-icons-round text-sm">bolt</span>
                         <span>Открыть инструкцию</span>
                     </button>
                     
                     <div class="grid grid-cols-2 gap-2 mt-1">
                         <button onclick="openActionModal('devices', {data['key_id']}, '{data.get('host_name', '')}')"
                             class="w-full bg-white/5 text-white py-2 rounded-xl font-bold text-[10px] uppercase tracking-wider hover:bg-white/10 active:scale-[0.98] transition-all flex items-center justify-center gap-1.5 border border-white/5 hover:border-white/10">
                             <span class="material-icons-round text-sm">devices</span>
                             <span>Устройства</span>
                         </button>
                         <button onclick="openActionModal('comment', {data['key_id']}, '{data.get('comment_key', '')}')"
                             class="w-full bg-white/5 text-white py-2 rounded-xl font-bold text-[10px] uppercase tracking-wider hover:bg-white/10 active:scale-[0.98] transition-all flex items-center justify-center gap-1.5 border border-white/5 hover:border-white/10">
                             <span class="material-icons-round text-sm">edit_note</span>
                             <span>Комментарии</span>
                         </button>
                     </div>
                </div>
            </div>
        </div>
        """
    return html

def _get_renew_keys_html(keys: list, user_id: int | None = None) -> tuple[str, str, str]:
    if not keys:
        return "", "Нет активных ключей", _get_no_key_html()
        
    options_html = '<div class="p-1 flex flex-col gap-0.5">'
    selected_text = ""
    renew_plans_html = ""
    
    for index, key in enumerate(keys):
        data = _process_key_data(key)
        host_name = key.get('host_name', '')
        
        is_selected = (index == 0)
        check_class = "text-primary" if is_selected else "text-transparent"
        text_color = "text-white" if is_selected else "text-gray-300"
        icon_color = "text-primary" if is_selected else "text-gray-500"
        
        if is_selected:
            selected_text = f"{data['name']} • До {data['expire_date_str']}"

        options_html += f"""
        <button
            class="dropdown-option w-full p-2.5 flex items-center justify-between rounded-lg hover:bg-white/5 transition-colors"
            data-key="#{data['key_id']}" data-name="{data['name']}" data-date="{data['expire_date_str']}" data-host="{host_name}" data-index="{index}">
            <div class="flex items-center gap-2.5 overflow-hidden">
                <span class="material-icons-round {icon_color} text-sm shrink-0">vpn_key</span>
                <div class="text-left overflow-hidden">
                    <div class="text-xs font-bold {text_color} truncate">{data['name']}</div>
                    <div class="flex items-center gap-2">
                        <div class="text-[9px] text-gray-400">До {data['expire_date_str']}</div>
                        <span class="text-[8px] {data['status_bg']} {data['status_color']} px-1.5 py-0.5 rounded-full font-bold uppercase tracking-wider shrink-0">{data['status_text']}</span>
                    </div>
                </div>
            </div>
            <span class="material-icons-round {check_class} text-xs selected-icon shrink-0">check</span>
        </button>
        """
        
        display_style = "grid" if is_selected else "none"
        desc, grid_html = _build_plans_grid_html(host_name, user_id, f"renew-plans-{index}", display_style)
        
        renew_plans_html += f'<div id="renew-desc-content-{index}" style="display: none;">{desc}</div>'
        renew_plans_html += grid_html
    
    options_html += '</div>'
    
    return options_html, selected_text, renew_plans_html

def _get_no_key_html() -> str:
    return """
        <div class="glass-card border border-white/10 rounded-[2rem] p-5 flex flex-col items-center justify-center text-center shadow-lg mb-3">
            <div class="w-12 h-12 bg-white/5 rounded-2xl flex items-center justify-center mb-3">
                <span class="material-icons-round text-2xl text-gray-500">vpn_key_off</span>
            </div>
            <h3 class="text-sm font-black text-white mb-1 tracking-tight">Нет активных ключей</h3>
            <p class="text-[10px] text-gray-400 font-medium leading-tight max-w-[180px]">
                Купите ключ, чтобы начать пользоваться VPN
            </p>
        </div>
    """


def _build_plans_grid_html(host_name: str, user_id: int | None, container_id: str, display_style: str = "grid") -> str:
    import re
    try:
        hosts = get_all_hosts(visible_only=True)
        host = next((h for h in (hosts or []) if h['host_name'] == host_name), None)
    except:
        host = None

    desc = ""
    if host:
        desc = host.get('description') or "Выберите подходящий тариф:"
        desc = re.sub(r'(\s*\n\s*){2,}', '\n', desc).strip()

    try:
        plans = get_plans_for_host(host_name)
    except:
        plans = []

    active_plans = [p for p in plans if p.get('is_active')]

    html = f'<div id="{container_id}" class="server-plans-container grid grid-cols-2 gap-2 mt-1" style="display: {display_style};">'

    if not active_plans:
        html += '<div class="col-span-2 text-center text-[10px] text-gray-500 py-3 glass-card border border-white/5 rounded-xl">Нет доступных тарифов</div>'
    else:
        plan_count = len(active_plans)
        for plan_idx, plan in enumerate(active_plans):
            try:
                raw_price = float(plan.get('price', 0))
                final_price = int(calculate_webapp_price(raw_price, user_id))
                months = int(plan.get('months') or 1)
            except (ValueError, TypeError):
                continue

            month_label = "месяц" if months == 1 else ("месяца" if 1 < months < 5 else "месяцев")

            is_last_odd = (plan_idx == plan_count - 1) and (plan_count % 2 == 1)
            span_class = " col-span-2" if is_last_odd else ""

            html += f"""
            <button
                class="plan-btn glass-card border border-white/10 rounded-2xl p-3.5 flex flex-col items-center justify-center text-center transition-all active:scale-95 hover:border-primary/40 hover:bg-white/5 group{span_class}"
                data-host="{host_name}" data-plan-id="{plan['plan_id']}" data-price="{final_price}" data-plan-name="{plan.get('plan_name', '')}"
                data-months="{months}"
                onclick="selectPlan(this)">
                <span
                    class="plan-label text-[9px] font-bold text-gray-500 uppercase tracking-widest mb-0.5 group-hover:text-gray-300 transition-colors">{months} {month_label}</span>
                <div class="flex items-baseline gap-0.5">
                    <span class="plan-price text-xl font-bold text-white">{final_price}</span>
                    <span class="text-xs font-medium text-gray-400">₽</span>
                </div>
            </button>
            """
    html += '</div>'

    return desc, html


def _get_servers_and_plans_html(user_id: int | None = None):
    try:
        hosts = get_all_hosts(visible_only=True)
    except:
        hosts = []
        
    if not hosts:
        return "", '<div class="col-span-2 text-center text-xs text-gray-500 py-4 glass-card border border-white/5 rounded-xl">Нет доступных серверов</div>'
        
    server_options_html = '<div class="p-1 flex flex-col gap-0.5">'
    plans_html = ""
    
    for index, host in enumerate(hosts):
        host_name = host['host_name']
        
        is_selected = (index == 0)
            
        check_class = "text-primary" if is_selected else "text-transparent"
        text_color = "text-white" if is_selected else "text-gray-300"
        icon_color = "text-primary" if is_selected else "text-gray-500"
        
        server_options_html += f"""
        <button
            class="server-option w-full p-2.5 flex items-center justify-between rounded-lg hover:bg-white/5 transition-colors"
            data-server="{host_name}" data-index="{index}" onclick="selectServer(this)">
            <div class="flex items-center gap-2.5">
                <span class="material-icons-round {icon_color} text-sm">public</span>
                <div class="text-left">
                    <div class="text-xs font-bold {text_color}">{host_name}</div>
                </div>
            </div>
            <span class="material-icons-round {check_class} text-xs server-selected-icon">check</span>
        </button>
        """
        
        display_style = "grid" if is_selected else "none"
        desc, grid_html = _build_plans_grid_html(host_name, user_id, f"plans-{index}", display_style)
        
        plans_html += f'<div id="desc-content-{index}" style="display: none;">{desc}</div>'
        plans_html += grid_html

    server_options_html += '</div>'
    
    return server_options_html, plans_html


def _render_banned_page(webapp_settings: dict):
    title = webapp_settings.get("webapp_title") or get_setting("panel_brand_title") or "TOR VPN"
    logo = webapp_settings.get("webapp_logo") or ""
    icon = webapp_settings.get("webapp_icon") or ""
    
    html = f"""<!DOCTYPE html>
<html lang="ru" class="dark">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no, viewport-fit=cover">
    <title>{title}</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap" rel="stylesheet">
    <link href="https://fonts.googleapis.com/icon?family=Material+Icons+Round" rel="stylesheet">
    <script src="https://cdn.tailwindcss.com"></script>
    <script>
        tailwind.config = {{
            darkMode: 'class',
            theme: {{
                extend: {{
                    colors: {{
                        primary: '#10b981',
                        surface: {{
                            dark: '#121212',
                            card: '#1e1e1e',
                            highlight: '#2a2a2a'
                        }}
                    }}
                }}
            }}
        }}
    </script>
    <style>
        body {{ font-family: 'Inter', sans-serif; -webkit-tap-highlight-color: transparent; }}
        .glass {{ background: rgba(30, 30, 30, 0.7); backdrop-filter: blur(10px); border: 1px solid rgba(255, 255, 255, 0.05); }}
    </style>
</head>
<body class="bg-surface-dark text-white h-screen flex flex-col items-center justify-center p-6 select-none overflow-hidden">
    <div class="fixed inset-0 pointer-events-none">
        <div class="absolute top-[-10%] left-[-10%] w-[40%] h-[40%] bg-primary/10 rounded-full blur-[120px]"></div>
        <div class="absolute bottom-[-10%] right-[-10%] w-[40%] h-[40%] bg-primary/5 rounded-full blur-[120px]"></div>
    </div>

    <div class="relative z-10 flex flex-col items-center text-center max-w-sm w-full">
        {f'<img src="{logo}" class="h-20 mb-8 drop-shadow-[0_0_20px_rgba(16,185,129,0.3)]">' if logo else f'<div class="w-20 h-20 bg-primary/20 rounded-3xl flex items-center justify-center mb-8 border border-primary/30 shadow-[0_0_30px_rgba(16,185,129,0.2)]"><span class="material-icons-round text-primary text-4xl">block</span></div>'}
        
        <h1 class="text-3xl font-black mb-3 tracking-tight">Доступ ограничен</h1>
        <p class="text-gray-400 font-medium leading-relaxed mb-8">
            Ваш аккаунт был заблокирован за нарушение правил сервиса. Использование функций WebApp временно недоступно.
        </p>

        <div class="glass rounded-[2rem] p-6 w-full border border-red-500/20 shadow-2xl">
            <div class="flex items-center gap-4 text-left">
                <div class="w-12 h-12 bg-red-500/10 rounded-2xl flex items-center justify-center shrink-0 border border-red-500/20">
                    <span class="material-icons-round text-red-500">lock_person</span>
                </div>
                <div>
                    <div class="text-[10px] text-gray-500 uppercase font-black tracking-widest mb-1">Статус аккаунта</div>
                    <div class="text-lg font-black text-red-500 leading-none">ЗАБЛОКИРОВАН</div>
                </div>
            </div>
            
            <div class="mt-6 pt-6 border-t border-white/5">
                <p class="text-[11px] text-gray-500 font-semibold mb-4 text-center">Если вы считаете, что это ошибка, обратитесь в нашу поддержку</p>
                <a href="https://t.me/{get_setting('support_bot_username')}" target="_blank"
                   class="flex items-center justify-center gap-2 w-full bg-white text-black py-4 rounded-2xl font-black text-sm uppercase tracking-wider hover:opacity-90 active:scale-[0.98] transition-all shadow-xl">
                    <span class="material-icons-round text-lg">headset_mic</span>
                    <span>Написать в поддержку</span>
                </a>
            </div>
        </div>

        <div class="mt-8 opacity-40 text-[10px] font-black uppercase tracking-widest flex items-center gap-2">
            <span>{title}</span>
            <span class="w-1 h-1 bg-gray-600 rounded-full"></span>
            <span>Security Module</span>
        </div>
    </div>
</body>
</html>"""
    return HTMLResponse(content=html, status_code=403)


async def _render_main_page(user_id: int):
    webapp_settings = get_webapp_settings()
    
    # 1. Check if Webapp is enabled
    if not webapp_settings.get("webapp_enable"):
         return HTMLResponse(content="<h1>Webapp is disabled</h1>", status_code=403)
         
    # 2. Check if user is banned
    user = get_user(user_id)
    if user and user.get('is_banned'):
         return _render_banned_page(webapp_settings)
         
    # Можно использовать webapp_domen для проверок или редиректов если нужно
    # current_domain = webapp_settings.get("webapp_domen")

    key_section = _get_no_key_html()
    profile_card = ""
    profile_keys_list = _get_no_key_html()
    setup_keys_list = _get_no_key_html()
    renew_keys_options = ""
    renew_selected_key = "Нет активных ключей"
    renew_plans_html_data = _get_no_key_html()
    keys = []
    
    if user_id:
        keys = get_user_keys(user_id)
        # Sort all keys by expiry, soonest first
        try:
            keys.sort(key=lambda k: datetime.strptime(k['expiry_date'], "%Y-%m-%d %H:%M:%S"))
        except:
            pass
            
        now = get_msk_time().replace(tzinfo=None)
        
        # --- FETCH LIVE DATA ONLY FOR ACTIVE KEYS ---
        active_keys = []
        for k in keys:
            try:
                exp = datetime.strptime(k['expiry_date'], "%Y-%m-%d %H:%M:%S")
                if exp > now:
                    active_keys.append(k)
            except: pass

        if active_keys:
            try:
                # --- 1. Fetch Key Details (User info from Host) ---
                details_tasks = []
                for k in active_keys:
                    details_tasks.append(remnawave_api.get_key_details_from_host(k))
                
                details_results = await asyncio.gather(*details_tasks, return_exceptions=True)
                
                # --- 2. Fetch Subscription Info (Traffic Stats) using UUID from Details ---
                sub_tasks = []
                # Map results to keys to keep order
                key_details_map = {}
                
                for k, res in zip(active_keys, details_results):
                    if isinstance(res, Exception) or not res or not res.get('user'):
                        sub_tasks.append(asyncio.sleep(0, None)) # Skip
                        continue
                        
                    u = res['user']
                    key_details_map[k['key_id']] = u
                    
                    # Update limits from user object immediately
                    if u.get('trafficLimitBytes') is not None:
                        k['limit_bytes'] = u.get('trafficLimitBytes')
                    if u.get('hwidDeviceLimit') is not None:
                        k['limit_ips'] = u.get('hwidDeviceLimit')

                    if not k.get('email') and not k.get('key_email'):
                        api_email = u.get('username') or u.get('email') or ''
                        if api_email:
                            k['email'] = api_email
                            k['key_email'] = api_email
                        
                    # Determine UUID for subscription check
                    # BOT PRIORITY: Use DB UUID first, then API response
                    target_uuid = k.get('remnawave_user_uuid') or u.get('uuid')
                    host = k.get('host_name')
                    
                    if target_uuid:
                        sub_tasks.append(remnawave_api.get_subscription_info(str(target_uuid), host_name=host))
                    else:
                        sub_tasks.append(asyncio.sleep(0, None))

                sub_results = await asyncio.gather(*sub_tasks, return_exceptions=True)
                
                # --- 3. Process Subscription Results ---
                for k, sub_res in zip(active_keys, sub_results):
                    # Try to find traffic in subscription response
                    found_traffic = None
                    if not isinstance(sub_res, Exception) and sub_res and isinstance(sub_res, dict):
                        # check common keys
                        for key_name in ['trafficUsed', 'traffic', 'used_traffic']:
                            val = sub_res.get(key_name)
                            if val is not None:
                                found_traffic = val
                                break
                    
                    if found_traffic is not None:
                        k['used_bytes'] = found_traffic
                    
                    # Fallback: check User Details (u)
                    if 'used_bytes' not in k:
                        u = key_details_map.get(k['key_id'])
                        if u:
                             # Check keys in user object
                             for key_name in ['traffic', 'trafficUsed', 'used_traffic']:
                                 if u.get(key_name) is not None:
                                     try: k['used_bytes'] = int(u.get(key_name)); break
                                     except: pass
                             
                             # Final fallback: sum upload + download
                             if 'used_bytes' not in k:
                                 uploaded = int(u.get('upload') or 0)
                                 downloaded = int(u.get('download') or 0)
                                 k['used_bytes'] = uploaded + downloaded

                    # HWID Usage
                    u = key_details_map.get(k['key_id'])
                    target_uuid = None
                    if u:
                         target_uuid = u.get('uuid')
                    if not target_uuid:
                         target_uuid = k.get('remnawave_user_uuid')
                         
                    host = k.get('host_name')

                    if target_uuid and host:
                         try:
                              devs = await remnawave_api.get_connected_devices_count(target_uuid, host_name=host)
                              if devs and 'total' in devs:
                                   k['used_ips'] = int(devs['total'])
                         except: pass
            except Exception as e:
                logger.error(f"Error fetching live stats: {e}")

        # --- CALCULATE MIN PRICE ---
        min_price_val = 0.0
        try:
            all_hosts = get_all_hosts(visible_only=True)
            prices = []
            for h in all_hosts:
                plans = get_plans_for_host(h['host_name'])
                for p in plans:
                    if p.get('is_active'):
                        try:
                            raw_p = float(p.get('price', 0))
                            final_p = calculate_webapp_price(raw_p, user_id)
                            prices.append(final_p)
                        except: continue
            if prices:
                min_price_val = min(prices)
        except Exception as e:
            logger.error(f"Error calculating min price: {e}")

        # --- GENERATE SECTIONS ---
        if keys:
            # For the main monitoring section, show only the soonest active key
            if active_keys:
                key_section = _get_key_html(active_keys[0])
            
            # Renew, Profile and Setup sections get the full list of keys
            # (Setup will filter internally, Profile shows all, Renew now shows all)
            renew_keys_options, renew_selected_key, renew_plans_html_data = _get_renew_keys_html(keys, user_id)
            renew_selected_display = renew_selected_key
            
            profile_keys_list = _get_profile_keys_html(keys)
            setup_keys_list = _get_setup_keys_html(keys)
            
        # Profile Stats
        user = get_user(user_id)
        ref_count = get_referral_count(user_id)
        ref_earned = user.get("referral_balance_all") or 0.0
        profile_card = _get_profile_card_html(user, ref_count, len(keys), ref_earned)
    
    p = os.path.join(os.path.dirname(__file__), "app.html")
    with open(p, "r", encoding="utf-8") as f:
        content = f.read()
    
    context = {
        "profile_card": profile_card,
        "key_section": key_section,
        "profile_keys_list": profile_keys_list,
        "setup_keys_list": setup_keys_list,
        "renew_keys_options": renew_keys_options,
        "renew_plans_html_data": renew_plans_html_data,
        "renew_selected_display": renew_selected_display if 'renew_selected_display' in locals() else renew_selected_key,
        "min_price": f"{int(min_price_val)} ₽" if min_price_val > 0 else "0 ₽",
        "webapp_logo": webapp_settings.get("webapp_logo") or "",
        "webapp_icon": webapp_settings.get("webapp_icon") or ""
    }
    
    content = _process_template_placeholders(content, user_id, webapp_settings, context)
    return HTMLResponse(content=content)


@app.get("/", response_class=HTMLResponse)
async def index(request: Request, user_id: int | None = None, token: str | None = None):
    try:
        # 1. Authorize by Token (query param)
        if token:
            from shop_bot.data_manager import database
            user = database.get_user_by_auth_token(token)
            if user:
                user_id = user['telegram_id']
        
        # 2. If no user_id (and no valid token), serve login.html
        if user_id is None:
            p = os.path.join(os.path.dirname(__file__), "login.html")
            if os.path.exists(p):
                with open(p, "r", encoding="utf-8") as f:
                    content = f.read()
                
                # Process placeholders for login page too
                webapp_settings = get_webapp_settings()
                context = {
                    "webapp_logo": webapp_settings.get("webapp_logo") or "",
                    "webapp_icon": webapp_settings.get("webapp_icon") or ""
                }
                content = _process_template_placeholders(content, 0, webapp_settings, context)
                return HTMLResponse(content=content)
            else:
                return HTMLResponse(content="<h1>Login page not found</h1>", status_code=404)

        webapp_settings = get_webapp_settings()
        user = get_user(user_id)
        if user and user.get('is_banned'):
            return _render_banned_page(webapp_settings)

        return await _render_main_page(user_id)

    except Exception as e:
        error_details = traceback.format_exc()
        return HTMLResponse(content=f"<h1>500 Internal Server Error</h1><pre>{error_details}</pre>", status_code=500)

# ===== API Models =====

class SupportStatusRequest(BaseModel):
    user_id: int

class SupportTicketCreateRequest(BaseModel):
    user_id: int
    subject: str

class SupportMessageSendRequest(BaseModel):
    user_id: int
    ticket_id: int
    message: str

class PaymentMethodsRequest(BaseModel):
    user_id: int

class TokenRequest(BaseModel):
    init_data: str

class TelegramDirectAuthRequest(BaseModel):
    user_id: int

class DeviceTiersRequest(BaseModel):
    host_name: str

class CreatePaymentRequest(BaseModel):
    user_id: int
    payment_method: str
    plan_id: int
    host_name: str | None = None
    action: str
    key_id: int | None = None
    promo_code: str | None = None
    tier_device_count: int = 1
    tier_price: float = 0

class ApplyPromoRequest(BaseModel):
    user_id: int
    promo_code: str
    plan_id: int | None = None
    price: float | None = None

# ===== API Endpoints =====


def validate_telegram_data(init_data: str, bot_token: str) -> dict | None:
    from urllib.parse import parse_qsl, unquote
    import hmac
    import hashlib
    import json

    try:
        if not init_data or len(init_data) < 10:
            logger.warning("Telegram auth: init_data is empty or too short")
            return None

        parsed_data = dict(parse_qsl(init_data, keep_blank_values=True))
        if "hash" not in parsed_data:
            logger.warning("Telegram auth: hash not found in init_data")
            return None
        
        received_hash = parsed_data.pop("hash")
        
        data_check_string = "\n".join(
            f"{k}={v}" for k, v in sorted(parsed_data.items())
        )
        
        secret_key = hmac.new(b"WebAppData", bot_token.encode(), hashlib.sha256).digest()
        calculated_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
        
        if calculated_hash == received_hash:
            user_json = parsed_data.get("user")
            if user_json:
                return json.loads(user_json)
            logger.warning("Telegram auth: hash valid but no user field")
        else:
            logger.warning(f"Telegram auth: hash mismatch. Expected={calculated_hash[:16]}... Got={received_hash[:16]}...")
        return None
    except Exception as e:
        logger.error(f"Telegram auth validation error: {e}")
        return None

@app.get("/api/auth/request-token")
async def api_request_auth_token():
    token = str(uuid.uuid4())[:36]
    TEMP_AUTH_TOKENS[token] = None
    bot_username = get_setting("telegram_bot_username")
    auth_url = f"tg://resolve?domain={bot_username}&start=auth_{token}"
    return {"ok": True, "token": token, "auth_url": auth_url}

@app.get("/api/auth/check-token/{token}")
async def api_check_auth_token(token: str):
    from shop_bot.data_manager import database
    # 1. Check in memory (waiting for bot confirmation)
    if token in TEMP_AUTH_TOKENS and TEMP_AUTH_TOKENS[token] is not None:
        user_id = TEMP_AUTH_TOKENS.pop(token)
        
        # Check existing token first
        existing_token = database.get_auth_token_by_user_id(user_id)
        if existing_token:
            return {"ok": True, "authorized": True, "user_id": user_id, "token": existing_token}
            
        # Generate persistent token
        persistent_token = str(uuid.uuid4())
        database.update_user_auth_token(user_id, persistent_token)
        return {"ok": True, "authorized": True, "user_id": user_id, "token": persistent_token}
    
    # 2. Check in DB (already authorized)
    user = database.get_user_by_auth_token(token)
    if user:
        if user.get('is_banned'):
            return {"ok": True, "authorized": False, "error": "Banned"}
        return {"ok": True, "authorized": True, "user_id": user['telegram_id'], "token": token}
    
    # 2.1 Check if user has persistent token (deep link flow edge case)
    # If the token passed is not found, it might be expired or invalid, return False
        
    return {"ok": True, "authorized": False}

@app.post("/api/auth/token")
async def api_create_token(req: TokenRequest):
    """Generate or retrieve a persistent login token using verified Telegram data."""
    token_str = get_setting("telegram_bot_token")
    if not token_str:
        return {"ok": False, "error": "Server configuration error"}

    user_data = validate_telegram_data(req.init_data, token_str)
    
    if not user_data:
        return {"ok": False, "error": "Invalid auth data"}

    user_id = user_data.get("id")
    from shop_bot.data_manager import database
    
    # Check ban status
    user = get_user(user_id)
    if user and user.get('is_banned'):
        return {"ok": False, "error": "Access denied"}
    
    # Check if user already has a persistent token
    existing_token = database.get_auth_token_by_user_id(user_id)
    if existing_token:
         return {"ok": True, "token": existing_token}
    
    # Generate new persistent token
    token = str(uuid.uuid4())
    # Ensure it's unique (highly likely with UUID4)
    database.update_user_auth_token(user_id, token)

    return {"ok": True, "token": token}


@app.post("/api/auth/telegram-direct")
async def api_telegram_direct_auth(req: TelegramDirectAuthRequest):
    from shop_bot.data_manager import database
    try:
        user = get_user(req.user_id)
        if not user:
            return {"ok": False, "error": "User not registered"}
            
        if user.get('is_banned'):
            return {"ok": False, "error": "Access denied"}

        existing_token = database.get_auth_token_by_user_id(req.user_id)
        if existing_token:
            return {"ok": True, "token": existing_token}

        token = str(uuid.uuid4())
        database.update_user_auth_token(req.user_id, token)
        return {"ok": True, "token": token}
    except Exception as e:
        logger.error(f"Telegram direct auth error: {e}")
        return {"ok": False, "error": "Auth error"}


@app.post("/api/device-tiers")
async def api_device_tiers(req: DeviceTiersRequest):
    try:
        host_data = get_host(req.host_name)
        if not host_data:
            return {"ok": True, "device_mode": "plan", "tiers": [], "tier_lock_extend": 0}
        mode = host_data.get('device_mode', 'plan')
        lock = int(host_data.get('tier_lock_extend', 0) or 0)
        from shop_bot.data_manager import database
        base_devices = int(database.get_setting(f"base_device_{req.host_name}", "1"))
        tiers = []
        if mode == 'tiers':
            raw = get_device_tiers(req.host_name)
            tiers = [{"tier_id": t["tier_id"], "device_count": t["device_count"], "price": float(t["price"])} for t in raw]
        return {"ok": True, "device_mode": mode, "tiers": tiers, "tier_lock_extend": lock, "base_device_count": base_devices}
    except Exception as e:
        logger.error(f"API device-tiers error: {e}")
        return {"ok": False, "error": str(e)}

@app.post("/api/payment-methods")
async def api_get_payment_methods(req: PaymentMethodsRequest):
    user_id = req.user_id
    user = get_user(user_id)
    
    methods = []
    
    # 1. YooKassa
    if (get_setting("yookassa_shop_id") or "") and (get_setting("yookassa_secret_key") or ""):
        label = "Банковская карта"
        if (get_setting("sbp_enabled") or "false").strip().lower() == "true":
            label = "СБП / Банковская карта"
        methods.append({"id": "pay_yookassa", "name": label, "icon": "credit_card"})

    # 2. Platega
    if (get_setting("platega_enabled") or "false").strip().lower() == "true":
        methods.append({"id": "pay_platega", "name": "СБП / Platega", "icon": "payments"})
    if (get_setting("platega_crypto_enabled") or "false").strip().lower() == "true":
        methods.append({"id": "pay_platega_crypto", "name": "Крипта / Platega", "icon": "payments"})

    # 3. CryptoBot
    if get_setting("cryptobot_token"):
        methods.append({"id": "pay_cryptobot", "name": "Криптовалюта", "icon": "currency_bitcoin"})
    # 3.1 Heleket (alternative crypto)
    elif (get_setting("heleket_merchant_id") or "") and (get_setting("heleket_api_key") or ""):
        methods.append({"id": "pay_heleket", "name": "Криптовалюта", "icon": "currency_bitcoin"})

    # 4. TON Connect
    if (get_setting("ton_wallet_address") or "") and (get_setting("tonapi_key") or ""):
        methods.append({"id": "pay_tonconnect", "name": "TON Connect", "icon": "wallet"})

    # 5. Telegram Stars
    if (get_setting("stars_enabled") or "false").strip().lower() == "true":
        methods.append({"id": "pay_stars", "name": "Telegram Stars", "icon": "star"})

    # 6. YooMoney
    if (get_setting("yoomoney_enabled") or "false").strip().lower() == "true":
        methods.append({"id": "pay_yoomoney", "name": "ЮMoney (кошелёк)", "icon": "account_balance_wallet"})

    # 7. Balance
    balance = float(user.get('balance', 0)) if user else 0
    methods.append({"id": "pay_balance", "name": f"Баланс ({balance:.0f} RUB)", "icon": "account_balance", "balance": balance})

    return {"ok": True, "methods": methods, "balance": balance}


@app.post("/api/create-payment")
async def api_create_payment(req: CreatePaymentRequest):
    try:
        user_id = req.user_id
        plan_id = req.plan_id
        method_id = req.payment_method
        
        plan = get_plan_by_id(plan_id)
        if not plan:
            return {"ok": False, "error": "Тариф не найден"}

        user = get_user(user_id)
        if not user:
            return {"ok": False, "error": "Пользователь не найден (ID: " + str(user_id) + ")"}
        
        final_price = calculate_webapp_price(float(plan['price']), user_id) 
        
        months = int(plan.get('months') or 1)
        
        tier_device_count = req.tier_device_count
        tier_price_per_month = req.tier_price
        
        if req.action == 'extend' and req.key_id:
            host_data = get_host(req.host_name) if req.host_name else None
            if host_data and host_data.get('device_mode') == 'tiers' and int(host_data.get('tier_lock_extend', 0) or 0):
                if not tier_price_per_month: 
                    key = get_key_by_id(req.key_id)
                    if key and key.get('remnawave_user_uuid'):
                        try:
                            user_info = await remnawave_api.get_user_by_uuid(key['remnawave_user_uuid'], host_name=req.host_name)
                            if user_info:
                                hwid = int(user_info.get('hwidDeviceLimit') or 1)
                                if hwid > 1:
                                    from shop_bot.data_manager import database
                                    base_devices = int(database.get_setting(f"base_device_{req.host_name}", "1"))
                                    tiers = get_device_tiers(req.host_name)
                                    for t in tiers:
                                        if t['device_count'] == hwid:
                                            tier_device_count = hwid
                                            diff = hwid - base_devices
                                            if diff < 0: diff = 0
                                            tier_price_per_month = float(diff * t['price'])
                                            break
                        except Exception as e:
                            logger.error(f"Auto-detect hwid error: {e}")
        
        if tier_price_per_month > 0:
            final_price += tier_price_per_month * months
            
        # --- APPLY PROMO DISCOUNT ---
        if req.promo_code:
            promo, error = rw_repo.check_promo_code_available(req.promo_code, user_id)
            if promo and promo.get('promo_type') == 'discount':
                if promo.get('discount_percent'):
                    final_price -= final_price * (float(promo['discount_percent']) / 100)
                elif promo.get('discount_amount'):
                    final_price -= float(promo['discount_amount'])
                final_price = max(0, round(final_price, 2))
        
        action_name = req.action
        
        # --- YooKassa ---
        if method_id == "pay_yookassa":
            shop_id, secret = get_setting("yookassa_shop_id"), get_setting("yookassa_secret_key")
            if not shop_id or not secret: return {"ok": False, "error": "YooKassa не настроена"}
            YookassaConfiguration.account_id = shop_id
            YookassaConfiguration.secret_key = secret
            pid = str(uuid.uuid4())
            meta = {
                "user_id": user_id, "months": months, "price": float(final_price),
                "action": action_name, "key_id": req.key_id, "host_name": req.host_name,
                "plan_id": plan_id, "payment_method": "YooKassa", "payment_id": pid,
                "tier_device_count": tier_device_count
            }
            create_payload_pending(pid, user_id, float(final_price), meta)
            comment = get_transaction_comment({"id": user_id, "username": user.get("username")}, action_name, months, req.host_name)
            payload = {
                "amount": {"value": f"{final_price:.2f}", "currency": "RUB"},
                "confirmation": {"type": "redirect", "return_url": f"https://t.me/{get_setting('telegram_bot_username')}"},
                "capture": True, "description": comment, "metadata": meta
            }
            try:
                pay_obj = YookassaPayment.create(payload, pid)
                pay_url = pay_obj.confirmation.confirmation_url
                
                kb = create_payment_keyboard(pay_url)
                await _send_telegram_message(user_id, f"<b>Оплата через ЮKassa</b>\n\nСумма: <b>{final_price:.2f} RUB</b>\n\n<i>Вы можете оплатить счет здесь или в WebApp.</i>", kb)
                
                return {"ok": True, "payment_url": pay_url, "payment_id": pid, "message": "Счёт создан"}
            except Exception as e:
                logger.error(f"YooKassa error: {e}")
                return {"ok": False, "error": f"Ошибка YooKassa: {e}"}

        # --- Platega ---
        elif method_id == "pay_platega":
            mid, key = get_setting("platega_merchant_id"), get_setting("platega_api_key")
            if not mid or not key: return {"ok": False, "error": "Platega не настроена"}
            pid = str(uuid.uuid4())
            meta = {
                "user_id": user_id, "months": months, "price": float(final_price),
                "action": action_name, "key_id": req.key_id, "host_name": req.host_name,
                "plan_id": plan_id, "payment_method": "Platega", "payment_id": pid,
                "tier_device_count": tier_device_count
            }
            create_payload_pending(pid, user_id, float(final_price), meta)
            desc = f"Order {pid}"
            try:
                platega = PlategaAPI(mid, key)
                _, url = await platega.create_payment(float(final_price), desc, pid, f"https://t.me/{get_setting('telegram_bot_username')}", f"https://t.me/{get_setting('telegram_bot_username')}", 2)
                if url:
                    kb = create_payment_keyboard(url)
                    await _send_telegram_message(user_id, f"<b>Оплата через Platega</b>\n\nСумма: <b>{final_price:.2f} RUB</b>\n\n<i>Счет также доступен в WebApp.</i>", kb)
                    return {"ok": True, "payment_url": url, "payment_id": pid, "message": "Счёт создан"}
                return {"ok": False, "error": "Ошибка получения ссылки Platega"}
            except Exception as e:
                return {"ok": False, "error": f"Ошибка Platega: {e}"}

        # --- Platega Crypto ---
        elif method_id == "pay_platega_crypto":
            mid, key = get_setting("platega_merchant_id"), get_setting("platega_api_key")
            if not mid or not key: return {"ok": False, "error": "Platega не настроена"}
            pid = str(uuid.uuid4())
            meta = {
                "user_id": user_id, "months": months, "price": float(final_price),
                "action": action_name, "key_id": req.key_id, "host_name": req.host_name,
                "plan_id": plan_id, "payment_method": "Platega Crypto", "payment_id": pid,
                "tier_device_count": tier_device_count
            }
            create_payload_pending(pid, user_id, float(final_price), meta)
            desc = f"Order {pid}"
            try:
                platega = PlategaAPI(mid, key)
                _, url = await platega.create_payment(float(final_price), desc, pid, f"https://t.me/{get_setting('telegram_bot_username')}", f"https://t.me/{get_setting('telegram_bot_username')}", 13)
                if url:
                    kb = create_payment_keyboard(url)
                    await _send_telegram_message(user_id, f"<b>Оплата через Platega (Crypto)</b>\n\nСумма: <b>{final_price:.2f} RUB</b>\n\n<i>Счет также доступен в WebApp.</i>", kb)
                    return {"ok": True, "payment_url": url, "payment_id": pid, "message": "Счёт создан"}
                return {"ok": False, "error": "Ошибка получения ссылки Platega Crypto"}
            except Exception as e:
                 return {"ok": False, "error": f"Ошибка Platega Crypto: {e}"}

         # --- CryptoBot ---
        elif method_id == "pay_cryptobot":
             pid = str(uuid.uuid4())
             meta = {
                "user_id": user_id, "months": months, "price": float(final_price),
                "action": action_name, "key_id": req.key_id, "host_name": req.host_name,
                "plan_id": plan_id, "payment_method": "CryptoBot", "payment_id": pid,
                "tier_device_count": tier_device_count
            }
             create_payload_pending(pid, user_id, float(final_price), meta)
             # payload_str format MUST match what bot expects. Using a generic format for now or just ID
             # safe encoded payload
             payload_str = f"{pid}" 
             
             try:
                 # Note: create_cryptobot_api_invoice IS imported now
                 res = await create_cryptobot_api_invoice(amount=float(final_price), payload_str=payload_str)
                 if res:
                     # res[0] is url, res[1] is invoice_id
                     kb = create_cryptobot_payment_keyboard(res[0], res[1])
                     await _send_telegram_message(user_id, f"<b>Оплата через CryptoBot</b>\n\nСумма: <b>{final_price:.2f} RUB</b>\n\n<i>Счет также доступен в WebApp.</i>", kb)
                     return {"ok": True, "payment_url": res[0], "payment_id": pid, "message": "Счёт создан"}
                 return {"ok": False, "error": "Ошибка API CryptoBot"}
             except Exception as e:
                 return {"ok": False, "error": f"Ошибка CryptoBot: {e}"}
             
        # --- Heleket ---
        elif method_id == "pay_heleket":
            pid = str(uuid.uuid4())
            meta = {
                "user_id": user_id, "months": months, "price": float(final_price),
                "action": action_name, "key_id": req.key_id, "host_name": req.host_name,
                "plan_id": plan_id, "payment_method": "Heleket", "payment_id": pid,
                "tier_device_count": tier_device_count
            }
            create_payload_pending(pid, user_id, float(final_price), meta)
            
            try:
                result = await create_heleket_payment_request(
                    amount=float(final_price), 
                    currency="RUB", 
                    description=f"Payment for {req.host_name}",
                    return_url=f"https://t.me/{get_setting('telegram_bot_username')}",
                    user_id=user_id,
                    email=user.get('email', 'no-email')
                )
                
                if result and result.get('payment_url'):
                    pay_url = result['payment_url']
                    kb = create_payment_keyboard(pay_url)
                    await _send_telegram_message(user_id, f"<b>Оплата через Crypto (Heleket)</b>\n\nСумма: <b>{final_price:.2f} RUB</b>", kb)
                    return {"ok": True, "payment_url": pay_url, "payment_id": pid}
                else:
                     return {"ok": False, "error": "Ошибка создания платежа Heleket"}

            except Exception as e:
                logger.error(f"Heleket error: {e}")
                return {"ok": False, "error": f"Ошибка Heleket: {e}"}
                
        # --- YooMoney ---
        elif method_id == "pay_yoomoney":
             receiver = get_setting("yoomoney_receiver")
             if not receiver: return {"ok": False, "error": "YooMoney не настроен"}
             pid = str(uuid.uuid4())
             meta = {
                "user_id": user_id, "months": months, "price": float(final_price),
                "action": action_name, "key_id": req.key_id, "host_name": req.host_name,
                "plan_id": plan_id, "payment_method": "YooMoney", "payment_id": pid,
                "tier_device_count": tier_device_count
            }
             create_payload_pending(pid, user_id, float(final_price), meta)
             label = pid
             desc = get_transaction_comment({"id": user_id, "username": user.get("username")}, action_name, months, req.host_name)
             link = _build_yoomoney_link(receiver, Decimal(str(final_price)), label, desc)
             
             kb = create_yoomoney_payment_keyboard(link, pid)
             await _send_telegram_message(user_id, f"<b>Оплата через ЮMoney (кошелёк)</b>\n\nСумма: <b>{final_price:.2f} RUB</b>\n\n<i>Счет также доступен в WebApp.</i>", kb)
             
             return {"ok": True, "payment_url": link, "payment_id": pid, "message": "Счёт создан"}

        # --- TON Connect ---
        elif method_id == "pay_tonconnect":
             return {"ok": False, "error": "TON Connect пока недоступен через WebApp"}

        # --- Stars ---
        elif method_id == "pay_stars":
             try:
                stars_ratio = float(get_setting("stars_per_rub") or 0)
             except: stars_ratio = 0
             if stars_ratio <= 0: return {"ok": False, "error": "Stars отключены"}
             stars_amount = max(1, int((final_price * stars_ratio)))
             pid = str(uuid.uuid4())
             meta = {
                "user_id": user_id, "months": months, "price": float(final_price),
                "action": action_name, "key_id": req.key_id, "host_name": req.host_name,
                "plan_id": plan_id, "payment_method": "Telegram Stars", "payment_id": pid,
                "tier_device_count": tier_device_count
            }
             create_payload_pending(pid, user_id, float(final_price), meta)
             title = f"{'Подписка' if action_name == 'new' else 'Продление'} на {months} мес."
             desc = get_transaction_comment({"id": user_id, "username": user.get("username")}, action_name, months, req.host_name)
             await _send_invoice_stars(user_id, title, desc, pid, stars_amount)
             bot_username = get_setting('telegram_bot_username')
             return {"ok": True, "message": "Счёт Stars отправлен в бот", "payment_url": f"tg://resolve?domain={bot_username}"}

        # --- Balance ---
        elif method_id == "pay_balance":
            if not deduct_from_balance(user_id, float(final_price)):
                return {"ok": False, "error": "Недостаточно средств"}
            meta = {
                "user_id": user_id, "months": months, "price": float(final_price),
                "action": action_name, "key_id": req.key_id, "host_name": req.host_name,
                "plan_id": plan_id, "payment_method": "Balance", "promo_code": "", "promo_discount": 0,
                "tier_device_count": tier_device_count
            }
            token = get_setting("telegram_bot_token")
            if not token: return {"ok": False, "error": "Бот не настроен (нет токена)"}
            
            bot = Bot(token=token)
            try:
                # process_successful_payment is imported at module level now
                await process_successful_payment(bot, meta)
            except Exception as e:
                # Refund if processing failed?
                add_to_balance(user_id, float(final_price))
                logger.error(f"Balance payment processing error: {e}")
                return {"ok": False, "error": f"Ошибка обработки: {e}"}
            finally:
                await bot.session.close()
            return {"ok": True, "message": "Оплачено с баланса!", "paid": True}

        return {"ok": False, "error": "Метод не поддерживается"}
    except Exception as e:
        logger.error(f"API Create Payment Error: {e}")
        return {"ok": False, "error": str(e), "details": traceback.format_exc()}

@app.post("/api/apply-promo")
async def api_apply_promo(req: ApplyPromoRequest):
    try:
        user = get_user(req.user_id)
        if not user or user.get('is_banned'):
            return {"ok": False, "error": "Access denied"}
        user_id = req.user_id
        code = req.promo_code.strip().upper()
        
        promo, error = rw_repo.check_promo_code_available(code, user_id)
        if not promo:
            errors = {
                "not_found": "Промокод не найден",
                "inactive": "Промокод не активен",
                "not_started": "Акция еще не началась",
                "expired": "Срок действия промокода истек",
                "total_limit_reached": "Промокод закончился",
                "user_limit_reached": "Вы уже использовали этот промокод",
                "empty_code": "Введите промокод"
            }
            return {"ok": False, "error": errors.get(error, "Ошибка проверки промокода")}

        promo_type = promo.get('promo_type')
        
        # 1. DISCOUNT (For Payment Modal)
        if promo_type == 'discount':
            if req.price is None:
                return {"ok": False, "error": "Промокод действителен только при покупке"}
            
            new_price = float(req.price)
            if promo.get('discount_percent'):
                new_price -= new_price * (float(promo['discount_percent']) / 100)
            elif promo.get('discount_amount'):
                new_price -= float(promo['discount_amount'])
            
            return {
                "ok": True, 
                "promo_type": "discount", 
                "new_price": max(0, round(new_price, 2))
            }

        # 2. BALANCE or UNIVERSAL (For Profile)
        elif promo_type == 'balance':
            reward = float(promo.get('reward_value', 0))
            if rw_repo.adjust_user_balance(user_id, reward):
                rw_repo.redeem_universal_promo(code, user_id)
                return {"ok": True, "promo_type": "balance", "message": f"Зачислено {reward} ₽"}
            return {"ok": False, "error": "Ошибка начисления баланса"}

        elif promo_type == 'universal':
            days_to_add = int(promo.get('reward_value') or 0)
            keys = rw_repo.get_user_keys(user_id)
            if not keys:
                 return {"ok": False, "error": "У вас нет активных подписок для продления"}
             
            keys.sort(key=lambda x: x.get('expiry_date', ''))
            key = keys[0]
            key_id = key['key_id']
            
            host = key.get('host_name')
            c_email = key.get('key_email')
             
            res = await remnawave_api.create_or_update_key_on_host(
                host_name=host,
                email=c_email,
                days_to_add=days_to_add,
                telegram_id=user_id
            )
            if res:
                rw_repo.update_key(key_id, remnawave_user_uuid=res['client_uuid'], expire_at_ms=res['expiry_timestamp_ms'])
                rw_repo.redeem_universal_promo(code, user_id)
                return {"ok": True, "promo_type": "universal", "message": f"Добавлено {days_to_add} дн."}
            else:
                return {"ok": False, "error": "Ошибка активации на стороне сервера"}

    except Exception as e:
        logger.error(f"API apply-promo error: {e}")
        return {"ok": False, "error": str(e)}

class CheckPaymentRequest(BaseModel):
    payment_id: str

@app.post("/api/check-payment")
async def api_check_payment(req: CheckPaymentRequest):
    try:
        if not req.payment_id or req.payment_id == "undefined" or req.payment_id == "null":
            return {"ok": False, "error": "Invalid payment_id"}
            
        exists = check_transaction_exists(req.payment_id)
        if not exists:
            return {"ok": True, "paid": False}
        
        return {
            "ok": True, 
            "paid": True,
            "message": "Оплата успешно подтверждена"
        }
    except Exception as e:
        logger.error(f"Check payment error: {e}")
        return {"ok": False, "error": str(e)}

class KeyActionRequest(BaseModel):
    user_id: int
    key_id: int
    host_name: str | None = None

class DeleteDeviceRequest(BaseModel):
    user_id: int
    key_id: int
    device_id: str
    host_name: str | None = None

class CommentRequest(BaseModel):
    user_id: int
    key_id: int
    comment: str

@app.post("/api/key/devices")
async def api_key_devices(req: KeyActionRequest):
    try:
        user = get_user(req.user_id)
        if not user or user.get('is_banned'):
            return {"ok": False, "error": "Access denied"}
            
        from shop_bot.data_manager.remnawave_repository import get_key_by_id
        from shop_bot.modules import remnawave_api
        key = get_key_by_id(req.key_id)
        if not key or key.get("user_id") != req.user_id:
            return {"ok": False, "error": "Ключ не найден"}
            
        uuid_val = key.get("remnawave_user_uuid")
        if not uuid_val:
            return {"ok": False, "error": "Ключ не имеет привязки к серверу"}
            
        host = req.host_name or key.get("host_name")
        devices_data = await remnawave_api.get_connected_devices_count(uuid_val, host_name=host)
        if devices_data and "devices" in devices_data:
            return {"ok": True, "devices": devices_data["devices"]}
            
        return {"ok": True, "devices": []}
    except Exception as e:
        logger.error(f"Error fetching devices: {e}")
        return {"ok": False, "error": str(e)}

@app.post("/api/key/device/delete")
async def api_key_device_delete(req: DeleteDeviceRequest):
    try:
        user = get_user(req.user_id)
        if not user or user.get('is_banned'):
            return {"ok": False, "error": "Access denied"}
            
        from shop_bot.data_manager.remnawave_repository import get_key_by_id
        from shop_bot.modules import remnawave_api
        key = get_key_by_id(req.key_id)
        if not key or key.get("user_id") != req.user_id:
            return {"ok": False, "error": "Ключ не найден"}
            
        uuid_val = key.get("remnawave_user_uuid")
        if not uuid_val:
            return {"ok": False, "error": "Ключ не имеет привязки"}
            
        host = req.host_name or key.get("host_name")
        success = await remnawave_api.delete_user_device(uuid_val, req.device_id, host_name=host)
        if success:
            return {"ok": True}
        return {"ok": False, "error": "Не удалось удалить устройство"}
    except Exception as e:
        logger.error(f"Error deleting device: {e}")
        return {"ok": False, "error": str(e)}

@app.post("/api/key/comment")
async def api_key_comment(req: CommentRequest):
    try:
        user = get_user(req.user_id)
        if not user or user.get('is_banned'):
            return {"ok": False, "error": "Access denied"}
            
        from shop_bot.data_manager.remnawave_repository import get_key_by_id, update_key
        key = get_key_by_id(req.key_id)
        if not key or key.get("user_id") != req.user_id:
            return {"ok": False, "error": "Ключ не найден"}
            
        update_key(req.key_id, comment_key=req.comment)
        return {"ok": True}
    except Exception as e:
        logger.error(f"Error updating comment: {e}")
        return {"ok": False, "error": str(e)}

@app.post("/api/support/status")
async def api_support_status(req: SupportStatusRequest):
    try:
        user = get_user(req.user_id)
        if not user or user.get('is_banned'):
            return {"ok": False, "error": "Access denied"}
            
        from shop_bot.data_manager.remnawave_repository import get_user_tickets, get_ticket_messages
        tickets = get_user_tickets(req.user_id) or []
        open_tickets = [t for t in tickets if t.get('status') == 'open']
        if not open_tickets:
            return {"ok": True, "has_ticket": False}
        
        ticket = max(open_tickets, key=lambda t: int(t['ticket_id']))
        messages = get_ticket_messages(ticket['ticket_id']) or []
        
        formatted_messages = []
        for m in messages:
            if m.get('sender') == 'note':
                continue
            formatted_messages.append({
                "sender": m.get("sender"),
                "content": m.get("content"),
                "created_at": m.get("created_at")
            })
            
        return {
            "ok": True, 
            "has_ticket": True, 
            "ticket_id": ticket['ticket_id'],
            "subject": ticket.get('subject', 'Обращение без темы'),
            "status": ticket.get('status'),
            "messages": formatted_messages
        }
    except Exception as e:
        logger.error(f"Error in support status: {e}")
        return {"ok": False, "error": str(e)}

@app.post("/api/support/create")
async def api_support_create(req: SupportTicketCreateRequest):
    try:
        user = get_user(req.user_id)
        if not user or user.get('is_banned'):
            return {"ok": False, "error": "Access denied"}
            
        from shop_bot.data_manager.remnawave_repository import get_or_create_open_ticket, add_support_message, get_setting
        
        subject_text = req.subject.strip()[:64]
        if not subject_text:
            return {"ok": False, "error": "Тема обращения не может быть пустой"}
            
        ticket_id, created_new = get_or_create_open_ticket(req.user_id, subject_text)
        
        if not ticket_id:
            return {"ok": False, "error": "Не удалось создать тикет"}
            
        if not created_new:
            return {"ok": False, "error": "У вас уже есть открытый тикет"}
            
        from aiogram import Bot
        token = get_setting("support_bot_token")
        if token:
            bot = Bot(token=token)
            try:
                try:
                    user = await bot.get_chat(req.user_id)
                    username_display = f"@{user.username}" if getattr(user, 'username', None) else f"ID {req.user_id}"
                except Exception:
                    username_display = f"ID {req.user_id}"
                    
                notification_text = (
                    f"🆕 <b>Новое обращение (WebApp)!</b>\n\n"
                    f"👤 <b>USER:</b> (<code>{req.user_id}</code> - {username_display})\n"
                    f"📝 <b>ID тикета:</b> <code>#{ticket_id}</code>\n"
                    f"💬 <b>Тема:</b> <i>{subject_text}</i>\n\n"
                    f"💌 Сообщения:\n"
                    f"<blockquote>Тикет открыт через веб-приложение.</blockquote>"
                )
                
                admin_ids_str = get_setting("admin_ids") or ""
                admin_ids = [aid.strip() for aid in admin_ids_str.split(",") if aid.strip()]
                for aid in admin_ids:
                    try:
                        await bot.send_message(
                            chat_id=int(aid),
                            text=notification_text,
                            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                [InlineKeyboardButton(text="💬 Ответить", callback_data=f"admin_reply_dm_{ticket_id}")]
                            ])
                        )
                    except Exception:
                        pass
            finally:
                await bot.session.close()
                    
        return {"ok": True, "ticket_id": ticket_id}
    except Exception as e:
        logger.error(f"Error in support create: {e}")
        return {"ok": False, "error": str(e)}

@app.post("/api/support/send")
async def api_support_send(req: SupportMessageSendRequest):
    try:
        user = get_user(req.user_id)
        if not user or user.get('is_banned'):
            return {"ok": False, "error": "Access denied"}
            
        from shop_bot.data_manager.remnawave_repository import get_ticket, add_support_message, get_setting
        ticket = get_ticket(req.ticket_id)
        if not ticket or ticket.get('user_id') != req.user_id or ticket.get('status') != 'open':
            return {"ok": False, "error": "Тикет не найден или закрыт"}
            
        add_support_message(req.ticket_id, sender="user", content=req.message)
        
        from aiogram import Bot
        token = get_setting("support_bot_token")
        if token:
            bot = Bot(token=token)
            try:
                try:
                    user = await bot.get_chat(req.user_id)
                    username_display = f"@{user.username}" if getattr(user, 'username', None) else f"ID {req.user_id}"
                except Exception:
                    username_display = f"ID {req.user_id}"
                    
                notification_text = (
                    f"📨 <b>Новое сообщение (WebApp)!</b>\n\n"
                    f"👤 <b>USER:</b> (<code>{req.user_id}</code> - {username_display})\n"
                    f"📝 <b>ID тикета:</b> <code>#{req.ticket_id}</code>\n"
                    f"💬 <b>Тема:</b> <i>{ticket.get('subject', 'Без темы')}</i>\n\n"
                    f"💌 Сообщения:\n"
                    f"<blockquote>{req.message}</blockquote>"
                )
                
                forum_chat_id = ticket.get('forum_chat_id')
                thread_id = ticket.get('message_thread_id')
                
                if forum_chat_id and thread_id:
                    try:
                        await bot.send_message(
                            chat_id=int(forum_chat_id),
                            message_thread_id=int(thread_id),
                            text=notification_text
                        )
                    except Exception as e:
                        logger.warning(f"Error mirroring to forum: {e}")
                else:
                    admin_ids_str = get_setting("admin_ids") or ""
                    admin_ids = [aid.strip() for aid in admin_ids_str.split(",") if aid.strip()]
                    for aid in admin_ids:
                        try:
                            await bot.send_message(
                                chat_id=int(aid),
                                text=notification_text,
                                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                    [InlineKeyboardButton(text="💬 Ответить", callback_data=f"admin_reply_dm_{req.ticket_id}")]
                                ])
                            )
                        except Exception:
                            pass
            finally:
                await bot.session.close()
                        
        return {"ok": True}
    except Exception as e:
        logger.error(f"Error in support send: {e}")
        return {"ok": False, "error": str(e)}

@app.get("/api/user-status")
async def api_user_status(user_id: int):
    try:
        user = get_user(user_id)
        if not user or user.get('is_banned'):
            return {"ok": False, "error": "Access denied"}
            
        keys = get_user_keys(user_id)
        # Sort keys by key_id descending to get the latest one first
        formatted_keys = []
        if keys:
            keys.sort(key=lambda k: k.get('key_id', 0), reverse=True)
            formatted_keys = [_process_key_data(k) for k in keys]
        
        return {"ok": True, "keys": formatted_keys}
    except Exception as e:
        logger.error(f"User status error: {e}")
        return {"ok": False, "error": str(e)}

@app.get("/{path_param}")
async def dynamic_route(request: Request, path_param: str):
    try:
        if path_param.startswith("token="):
            token = path_param.split("=")[1]
            from shop_bot.data_manager import database
            user = database.get_user_by_auth_token(token)
            if user:
                webapp_settings = get_webapp_settings()
                if user.get('is_banned'):
                    return _render_banned_page(webapp_settings)
                return await _render_main_page(user['telegram_id'])
            else:
                 # Token not valid or expired -> Render Login Page
                 p = os.path.join(os.path.dirname(__file__), "login.html")
                 if os.path.exists(p):
                     with open(p, "r", encoding="utf-8") as f:
                         content = f.read()
                     
                     webapp_settings = get_webapp_settings()
                     context = {
                        "webapp_logo": webapp_settings.get("webapp_logo") or "",
                        "webapp_icon": webapp_settings.get("webapp_icon") or ""
                     }
                     content = _process_template_placeholders(content, 0, webapp_settings, context)
                     return HTMLResponse(content=content)
                 else:
                     return HTMLResponse(content="<h1>Login page not found</h1>", status_code=404)
        
        # Pass through to 404 naturally or handle other dynamic routes
        return HTMLResponse(content="<h1>404 Not Found</h1>", status_code=404)
    except Exception as e:
        logger.error(f"Dynamic route error: {e}")
        return HTMLResponse(content="<h1>Error</h1>", status_code=500)
