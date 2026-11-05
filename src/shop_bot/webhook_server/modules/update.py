import json
import os
import requests
import shlex
import threading
import time
import uuid
from flask import request

from shop_bot.data_manager import remnawave_repository as rw_repo

RELEASES_CACHE_TTL = 600
RELEASES_LIMIT = 1

_releases_cache = {
    'expires_at': 0,
    'items': []
}
_UPDATE_SETTINGS = {
    'host': 'update_ssh_host',
    'port': 'update_ssh_port',
    'username': 'update_ssh_username',
    'password': 'update_ssh_password'
}
_UPDATE_COMMAND = 'cd /root/remnawave-shopbot && bash /root/remnawave-shopbot/install.sh'
_UPDATE_CHECK_COMMAND = 'printf "Сервер: "; hostname; printf "Пользователь: "; id -un; printf "Текущий каталог: "; pwd; if [ ! -d /root/remnawave-shopbot ]; then echo "Каталог /root/remnawave-shopbot не найден или недоступен этому пользователю"; ls -ld /root /root/remnawave-shopbot 2>&1; exit 10; fi; if [ ! -f /root/remnawave-shopbot/install.sh ]; then echo "Файл /root/remnawave-shopbot/install.sh не найден"; ls -l /root/remnawave-shopbot 2>&1; exit 11; fi; if ! command -v bash >/dev/null 2>&1; then echo "Команда bash не найдена"; exit 12; fi; printf "SSH-проверка пройдена: установщик найден"'
_UPDATE_LOG_FILE = '/tmp/remnawave-shopbot-update.log'
_UPDATE_STATUS_PREFIX = '/tmp/remnawave-shopbot-update'
_UPDATE_STATE_KEY = 'remnawave_shopbot_update_state'
_update_lock = threading.Lock()
_update_running = False


def get_update_state():
    raw_state = rw_repo.get_other_value(_UPDATE_STATE_KEY)
    if not raw_state:
        return {'status': 'idle', 'progress': 0, 'message': 'Обновление ещё не запускалось', 'log': ''}
    try:
        state = json.loads(raw_state)
        if not isinstance(state, dict):
            raise ValueError('invalid state')
        return state
    except Exception:
        return {'status': 'idle', 'progress': 0, 'message': 'Состояние обновления недоступно', 'log': ''}


def save_update_state(state):
    rw_repo.set_other_value(_UPDATE_STATE_KEY, json.dumps(state, ensure_ascii=False))


def _progress_from_log(log_text):
    phases = [
        ('загрузка', 15, 'Загрузка установщика'),
        ('обновление системы', 30, 'Подготовка системы'),
        ('клонирование', 45, 'Загрузка файлов проекта'),
        ('установка зависимостей', 65, 'Установка зависимостей'),
        ('docker compose', 82, 'Обновление контейнеров'),
        ('контейнер запущен', 94, 'Завершение запуска'),
        ('успешно', 97, 'Завершение обновления')
    ]
    text = (log_text or '').lower()
    matches = [(progress, label) for marker, progress, label in phases if marker in text]
    if matches:
        return max(matches, key=lambda item: item[0])
    return 8, 'Установщик выполняется'

def get_current_version():
    os_json_path = os.path.join(os.path.dirname(__file__), 'os.json')
    with open(os_json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data['project']['version']

def get_update_url():
    os_json_path = os.path.join(os.path.dirname(__file__), 'os.json')
    with open(os_json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data['project']['links']['update']

def parse_version(version_string):
    return tuple(map(int, version_string.split('.')))

def check_for_updates():
    try:
        current_version = get_current_version()
        update_url = get_update_url()
        
        separator = '&' if '?' in update_url else '?'
        cache_busting_url = f"{update_url}{separator}t={int(time.time())}"
        
        response = requests.get(
            cache_busting_url, 
            headers={"Cache-Control": "no-cache", "Pragma": "no-cache"}, 
            timeout=5
        )
        response.raise_for_status()
        
        remote_data = response.json()
        remote_version = remote_data['project']['version']
        
        if parse_version(remote_version) > parse_version(current_version):
            return {
                'update_available': True,
                'current_version': current_version,
                'latest_version': remote_version
            }
        else:
            return {
                'update_available': False,
                'current_version': current_version,
                'latest_version': remote_version
            }
    except Exception as e:
        return {
            'update_available': False,
            'current_version': get_current_version(),
            'latest_version': None,
            'error': str(e)
        }

def get_saved_ssh_settings():
    return {
        'host': (rw_repo.get_setting(_UPDATE_SETTINGS['host']) or '').strip(),
        'port': (rw_repo.get_setting(_UPDATE_SETTINGS['port']) or '22').strip(),
        'username': (rw_repo.get_setting(_UPDATE_SETTINGS['username']) or '').strip(),
        'configured': bool(rw_repo.get_setting(_UPDATE_SETTINGS['host']) and rw_repo.get_setting(_UPDATE_SETTINGS['username']) and rw_repo.get_setting(_UPDATE_SETTINGS['password']))
    }

def save_ssh_settings(host, port, username, password):
    if not host or not username or not password:
        return False, 'Заполните IP-адрес, логин и пароль'
    try:
        port_value = int(port or 22)
    except (TypeError, ValueError):
        return False, 'Порт должен быть числом'
    if not 1 <= port_value <= 65535:
        return False, 'Порт должен быть от 1 до 65535'
    rw_repo.update_setting(_UPDATE_SETTINGS['host'], host)
    rw_repo.update_setting(_UPDATE_SETTINGS['port'], str(port_value))
    rw_repo.update_setting(_UPDATE_SETTINGS['username'], username)
    rw_repo.update_setting(_UPDATE_SETTINGS['password'], password)
    return True, 'Данные SSH сохранены'

def run_ssh_update():
    global _update_running
    with _update_lock:
        state = get_update_state()
        if _update_running or state.get('status') == 'running':
            return {'ok': False, 'message': 'Обновление уже выполняется'}, 409
        settings = get_saved_ssh_settings()
        password = rw_repo.get_setting(_UPDATE_SETTINGS['password']) or ''
        if not settings['configured']:
            return {'ok': False, 'message': 'Сначала укажите данные SSH-сервера'}, 400

    from shop_bot.webhook_server.modules.other import execute_ssh_command
    check = execute_ssh_command(settings['host'], int(settings['port']), settings['username'], password, _UPDATE_CHECK_COMMAND, timeout=15)
    if not check.get('ok'):
        error = check.get('error') or check.get('output') or f'Проверка сервера не пройдена (код {check.get("exit_status", "неизвестен")})'
        return {'ok': False, 'message': f'Сервер не готов к обновлению: {error}'}, 502

    task_id = uuid.uuid4().hex
    log_file = f'{_UPDATE_STATUS_PREFIX}-{task_id}.log'
    status_file = f'{_UPDATE_STATUS_PREFIX}-{task_id}.status'
    script = f'{_UPDATE_COMMAND}; result=$?; printf "%s" "$result" > {shlex.quote(status_file)}; exit "$result"'
    launch_command = f'rm -f {shlex.quote(log_file)} {shlex.quote(status_file)}; nohup bash -lc {shlex.quote(script)} > {shlex.quote(log_file)} 2>&1 < /dev/null & echo $!'
    launch = execute_ssh_command(settings['host'], int(settings['port']), settings['username'], password, launch_command, timeout=20)
    if not launch.get('ok') or not launch.get('output', '').strip():
        error = launch.get('error') or launch.get('output') or 'Не удалось запустить установщик'
        return {'ok': False, 'message': f'Ошибка запуска обновления: {error}'}, 502

    state = {
        'task_id': task_id,
        'pid': launch['output'].strip().splitlines()[-1],
        'host': settings['host'],
        'port': int(settings['port']),
        'username': settings['username'],
        'status': 'running',
        'progress': 5,
        'message': 'Установщик запущен',
        'log': '',
        'started_at': int(time.time()),
        'updated_at': int(time.time())
    }
    save_update_state(state)
    with _update_lock:
        _update_running = True
    return {'ok': True, 'message': f'Обновление запущено на сервере, PID: {state["pid"]}', 'state': state}


def refresh_update_state():
    global _update_running
    state = get_update_state()
    if state.get('status') != 'running':
        return state

    settings = get_saved_ssh_settings()
    password = rw_repo.get_setting(_UPDATE_SETTINGS['password']) or ''
    if not settings['configured']:
        state.update({'status': 'error', 'message': 'Не заданы SSH-данные для отслеживания обновления'})
        save_update_state(state)
        with _update_lock:
            _update_running = False
        return state

    from shop_bot.webhook_server.modules.other import execute_ssh_command
    task_id = state.get('task_id') or ''
    log_file = f'{_UPDATE_STATUS_PREFIX}-{task_id}.log'
    status_file = f'{_UPDATE_STATUS_PREFIX}-{task_id}.status'
    poll_command = f'printf "STATUS:"; if [ -f {shlex.quote(status_file)} ]; then cat {shlex.quote(status_file)}; else echo RUNNING; fi; printf "\\nLOG:\\n"; tail -c 12000 {shlex.quote(log_file)} 2>/dev/null || true'
    result = execute_ssh_command(state['host'], int(state['port']), state['username'], password, poll_command, timeout=20)
    if not result.get('ok'):
        state.update({'message': f'Ожидание ответа сервера: {result.get("error") or "SSH временно недоступен"}', 'updated_at': int(time.time())})
        save_update_state(state)
        return state

    output = result.get('output') or ''
    status_text, _, log_text = output.partition('LOG:\n')
    status_value = status_text.replace('STATUS:', '').strip()
    state['log'] = log_text[-12000:]
    state['updated_at'] = int(time.time())
    if status_value == 'RUNNING':
        state['status'] = 'running'
        progress, message = _progress_from_log(state['log'])
        elapsed = max(0, int(time.time()) - int(state.get('started_at', time.time())))
        activity_progress = min(88, 8 + elapsed // 8 + len(state['log']) // 500)
        state['progress'] = min(92, max(progress, activity_progress))
        state['message'] = message if progress > 8 else 'Установщик выполняется'
    elif status_value == '0':
        state.update({'status': 'completed', 'progress': 100, 'message': 'Обновление успешно завершено'})
        with _update_lock:
            _update_running = False
    else:
        state.update({'status': 'failed', 'progress': min(int(state.get('progress', 5)), 94), 'message': f'Установщик завершился с кодом {status_value or "неизвестно"}'})
        with _update_lock:
            _update_running = False
    save_update_state(state)
    return state

def get_releases_url():
    os_json_path = os.path.join(os.path.dirname(__file__), 'os.json')
    with open(os_json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    releases_url = data['project']['links']['releases']
    parts = [part for part in releases_url.split('/') if part]
    if 'github.com' not in parts:
        return None
    index = parts.index('github.com')
    if len(parts) < index + 3:
        return None
    return f"https://api.github.com/repos/{parts[index + 1]}/{parts[index + 2]}/releases"

def fetch_releases(limit=RELEASES_LIMIT):
    now = time.time()
    if _releases_cache['items'] and _releases_cache['expires_at'] > now:
        return _releases_cache['items']

    releases_url = get_releases_url()
    if not releases_url:
        return []

    response = requests.get(
        releases_url,
        params={'per_page': limit},
        headers={
            'Accept': 'application/vnd.github+json',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        },
        timeout=5
    )
    response.raise_for_status()

    items = []
    for release in response.json():
        if release.get('draft'):
            continue
        items.append({
            'tag': release.get('tag_name') or '',
            'name': release.get('name') or release.get('tag_name') or '',
            'published_at': (release.get('published_at') or '')[:10],
            'url': release.get('html_url') or '',
            'prerelease': bool(release.get('prerelease')),
            'body': release.get('body') or ''
        })

    _releases_cache['items'] = items
    _releases_cache['expires_at'] = now + RELEASES_CACHE_TTL
    return items

def register_update_routes(flask_app, login_required):
    @flask_app.route('/update/check', methods=['GET'])
    @login_required
    def check_updates_route():
        result = check_for_updates()
        return result

    @flask_app.route('/update/settings', methods=['GET', 'POST'])
    @login_required
    def update_settings_route():
        if request.method == 'GET':
            return {'ok': True, **get_saved_ssh_settings()}
        host = (request.form.get('host') or '').strip()
        port = (request.form.get('port') or '22').strip()
        username = (request.form.get('username') or '').strip()
        password = request.form.get('password') or ''
        if not password:
            password = rw_repo.get_setting(_UPDATE_SETTINGS['password']) or ''
        ok, message = save_ssh_settings(host, port, username, password)
        return {'ok': ok, 'message': message}, (200 if ok else 400)

    @flask_app.route('/update/run', methods=['POST'])
    @login_required
    def run_update_route():
        return run_ssh_update()

    @flask_app.route('/update/status', methods=['GET'])
    @login_required
    def update_status_route():
        return refresh_update_state()

    @flask_app.route('/update/releases', methods=['GET'])
    @login_required
    def get_releases_route():
        try:
            releases = fetch_releases()
            return {'ok': True, 'releases': releases}
        except Exception as e:
            return {'ok': False, 'releases': [], 'error': str(e)}
