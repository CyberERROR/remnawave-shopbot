import time
import asyncio
import logging
import re
import hashlib
import ipaddress
from flask import request
from aiogram import Router, F
from aiogram.types import CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder

logger = logging.getLogger(__name__)

BANNED_DATA = {
    'ips': {},
    'ua_hashes': {}
}

def is_blocked(ip, ua):
    now = time.time()
    if ip in BANNED_DATA['ips']:
        if now < BANNED_DATA['ips'][ip]:
            return True
        else:
            del BANNED_DATA['ips'][ip]
    
    ua_hash = hashlib.md5(ua.encode()).hexdigest()[:12]
    if ua_hash in BANNED_DATA['ua_hashes']:
        if now < BANNED_DATA['ua_hashes'][ua_hash]:
            return True
        else:
            del BANNED_DATA['ua_hashes'][ua_hash]
            
    return False

def block_access(ip, ua_hash, duration_sec=300):
    now = time.time()
    expire = now + duration_sec
    BANNED_DATA['ips'][ip] = expire
    BANNED_DATA['ua_hashes'][ua_hash] = expire
    logger.info(f"Blocked IP {ip} and UA hash {ua_hash} for {duration_sec}s")

def get_client_ip():
    remote_addr = (request.remote_addr or '').strip()
    try:
        remote_ip = ipaddress.ip_address(remote_addr)
        trusted_proxy = remote_ip.is_loopback or remote_ip.is_private
    except ValueError:
        trusted_proxy = False
    if not trusted_proxy:
        return remote_addr
    cf_ip = (request.headers.get('CF-Connecting-IP') or '').strip()
    if cf_ip:
        return cf_ip
    forwarded = request.headers.get('X-Forwarded-For') or ''
    if forwarded:
        first = forwarded.split(',')[0].strip()
        if first:
            return first
    return remote_addr

def parse_ip_list(raw_value):
    networks = []
    for chunk in re.split(r'[,;\s]+', str(raw_value or '')):
        if not chunk:
            continue
        try:
            networks.append(ipaddress.ip_network(chunk, strict=False))
        except ValueError:
            logger.warning(f"IP list: invalid entry skipped: {chunk}")
    return networks

def is_ip_allowed(ip, networks):
    try:
        addr = ipaddress.ip_address(str(ip).strip())
    except ValueError:
        return False
    if addr.version == 6 and addr.ipv4_mapped:
        addr = addr.ipv4_mapped
    return any(addr in network for network in networks)

def parse_ua(ua):
    os_info = "Unknown OS"
    engine = "Unknown Engine"
    
    if "iPhone" in ua: os_info = "iPhone"
    elif "iPad" in ua: os_info = "iPad"
    elif "Android" in ua:
        v = re.search(r"Android ([\d.]+)", ua)
        os_info = f"Android {v.group(1)}" if v else "Android"
    elif "Windows NT 10.0" in ua: os_info = "Windows 10/11"
    elif "Windows NT 6.3" in ua: os_info = "Windows 8.1"
    elif "Windows NT 6.1" in ua: os_info = "Windows 7"
    elif "Macintosh" in ua: os_info = "macOS"
    elif "Linux" in ua: os_info = "Linux"
    
    if "AppleWebKit" in ua: engine = "WebKit"
    elif "Gecko" in ua and "Firefox" in ua: engine = "Gecko"
    elif "Trident" in ua: engine = "Trident"
    elif "Chrome" in ua: engine = "Blink"
    
    return os_info, engine

def format_security_msg(title, info):
    os_info, engine = parse_ua(info.get('ua', ''))
    
    lines = [title]
    if info.get('msg'):
        lines.append(info['msg'])
    lines.append("")
    
    is_success = "Успешный вход" in title
    
    mapping = [
        ('🛰 <b>iP:</b>', info.get('ip')),
        ('🔎 <b>Реальный ip:</b>', info.get('real_ip')),
        ('💻 <b>ОС и версия:</b>', os_info),
        ('📃 <b>Движок браузера:</b>', engine),
        ('🗽 <b>Метод запроса:</b>', info.get('method')), 
    ]
    if not is_success:
        mapping.append(('👤 <b>Введеный логин:</b>', info.get('user')))
        mapping.append(('🔐 <b>Введеный пароль:</b>', info.get('password')))
        
    mapping.extend([
        ('🌐 <b>UserAgent:</b>', info.get('ua')),
    ])
    
    for label, val in mapping:
        if val:
            if 'UserAgent:' in label:
                lines.append(f"{label}\n<blockquote><code>{val}</code></blockquote>")
            else:
                lines.append(f"{label} <code>{val}</code>")
            
    if info.get('footer'):
        lines.append("")
        lines.append(info['footer'])
        
    return "\n".join(lines)

async def send_notification(bot, chat_id, text, reply_markup=None):
    try:
        await bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup, parse_mode="HTML")
    except Exception as e:
        logger.error(f"Security notify error: {e}")

def notify_admin(bot, loop, admin_id, title, info, is_alert=False):
    text = format_security_msg(title, info)
    kb = None
    
    if is_alert:
        kb = InlineKeyboardBuilder()
        kb.button(text="✅ Да", callback_data="sec_it_was_me")
        ua_hash = hashlib.md5(info.get('ua', '').encode()).hexdigest()[:12]
        kb.button(text="❌ Нет", callback_data=f"sec_block:{info.get('ip')}:{ua_hash}")
        kb.adjust(2)
        kb = kb.as_markup()

    if loop and loop.is_running():
        asyncio.run_coroutine_threadsafe(send_notification(bot, admin_id, text, kb), loop)
    else:
        try:
            asyncio.run(send_notification(bot, admin_id, text, kb))
        except Exception:
            pass

def get_security_router():
    router = Router()
    
    @router.callback_query(F.data == "sec_it_was_me")
    async def handle_was_me(callback: CallbackQuery):
        await callback.message.edit_text(callback.message.text + "\n\n✅ <i>Подтверждено владельцем.</i>", reply_markup=None)
        await callback.answer("Принято")

    @router.callback_query(F.data.startswith("sec_block:"))
    async def handle_block(callback: CallbackQuery):
        try:
            parts = callback.data.split(":")
            if len(parts) >= 3:
                ip = parts[1]
                ua_hash = parts[2]
                
                block_access(ip, ua_hash)
                
                text = callback.message.text
                

                new_header = "🟡  <b>ЗАБЛОКИРОВАН</b>\n<blockquote>iP и UserAgent заблокированы, если даже пользователь поменяет ip все равно будет блокирован по UserAgent</blockquote>\n"
                new_footer = f"\n🚫 IP {ip} заблокирован на 5 минут."
                

                lines = text.split('\n')
                data_lines = []
                capture = False
                
                for line in lines:

                    if any(x in line for x in ["iP:", "ОС и версия:", "Движок браузера:", "Метод запроса:", "UserAgent:", "Введеный логин:", "Введеный пароль:", "Реальный ip:"]):

                        if "iP:" in line: line = line.replace("iP:", "<b>iP:</b>").replace(line.split("iP:")[1], f" <code>{line.split('iP:')[1].strip()}</code>")
                        elif "ОС и версия:" in line: line = line.replace("ОС и версия:", "<b>ОС и версия:</b>").replace(line.split("ОС и версия:")[1], f" <code>{line.split('ОС и версия:')[1].strip()}</code>")
                        elif "Движок браузера:" in line: line = line.replace("Движок браузера:", "<b>Движок браузера:</b>").replace(line.split("Движок браузера:")[1], f" <code>{line.split('Движок браузера:')[1].strip()}</code>")
                        elif "Метод запроса:" in line: line = line.replace("Метод запроса:", "<b>Метод запроса:</b>").replace(line.split("Метод запроса:")[1], f" <code>{line.split('Метод запроса:')[1].strip()}</code>")
                        elif "Введеный логин:" in line: line = line.replace("Введеный логин:", "<b>Введеный логин:</b>").replace(line.split("Введеный логин:")[1], f" <code>{line.split('Введеный логин:')[1].strip()}</code>")
                        elif "Введеный пароль:" in line: line = line.replace("Введеный пароль:", "<b>Введеный пароль:</b>").replace(line.split("Введеный пароль:")[1], f" <code>{line.split('Введеный пароль:')[1].strip()}</code>")
                        elif "Реальный ip:" in line: line = line.replace("Реальный ip:", "<b>Реальный ip:</b>").replace(line.split("Реальный ip:")[1], f" <code>{line.split('Реальный ip:')[1].strip()}</code>")
                        elif "UserAgent:" in line:
                            ua_val = line.split("UserAgent:")[1].replace("<code>", "").replace("</code>", "").strip()
                            line = f"<b>UserAgent:</b>\n<blockquote><code>{ua_val}</code></blockquote>"
                        
                        data_lines.append(line)

                
                final_text = new_header + "\n" + "\n".join(data_lines) + new_footer
                
                await callback.message.edit_text(final_text, reply_markup=None, parse_mode="HTML")
                await callback.answer("Доступ заблокирован")
            else:
                await callback.answer("Ошибка данных", show_alert=True)
                
        except Exception as e:
            logger.error(f"Block handle error: {e}")
            await callback.answer("Ошибка обработки", show_alert=True)
    
    return router
