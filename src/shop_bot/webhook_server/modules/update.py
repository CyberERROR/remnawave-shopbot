import os
import time
import zipfile
from functools import lru_cache
from pathlib import Path

import requests
from flask import request

from shop_bot.data_manager import backup_manager
from shop_bot.data_manager.database import DATA_DIR

try:
    import tomllib
except ImportError:
    try:
        import tomli as tomllib
    except ImportError:
        tomllib = None

RELEASES_CACHE_TTL = 120
RELEASES_LIMIT = 1
UPDATE_CHECK_TTL = 120
PYPROJECT_FILENAME = 'pyproject.toml'
AGENT_FRESH_SECONDS = 90

_releases_cache = {
    'expires_at': 0,
    'items': []
}

_update_cache = {
    'expires_at': 0,
    'data': None
}


def _find_pyproject_path():
    current = os.path.dirname(os.path.abspath(__file__))
    for _ in range(6):
        candidate = os.path.join(current, PYPROJECT_FILENAME)
        if os.path.exists(candidate):
            return candidate
        parent = os.path.dirname(current)
        if parent == current:
            break
        current = parent
    candidate = os.path.join(os.getcwd(), PYPROJECT_FILENAME)
    return candidate if os.path.exists(candidate) else None


@lru_cache(maxsize=1)
def _load_project_meta():
    path = _find_pyproject_path()
    if not path or tomllib is None:
        return {}
    try:
        with open(path, 'rb') as f:
            return tomllib.load(f)
    except Exception:
        return {}


def get_project_info():
    meta = _load_project_meta()
    project = meta.get('project') or {}
    tool = (meta.get('tool') or {}).get('shopbot') or {}
    links = {}
    for key, value in (project.get('urls') or {}).items():
        if value:
            links[str(key).strip().lower()] = str(value).strip()
    authors = project.get('authors') or []
    author = ''
    if authors and isinstance(authors[0], dict):
        author = str(authors[0].get('name') or '').strip()
    return {
        'version': str(project.get('version') or '').strip(),
        'name': str(tool.get('name') or project.get('name') or '').strip(),
        'links': links,
        'author': author
    }


def get_current_version():
    return get_project_info().get('version') or ''


def parse_version(version_string):
    parts = []
    for chunk in str(version_string or '').strip().lstrip('vV').split('.'):
        digits = ''.join(char for char in chunk if char.isdigit())
        parts.append(int(digits) if digits else 0)
    return tuple(parts)


def is_newer_version(remote_version, current_version):
    try:
        return parse_version(remote_version) > parse_version(current_version)
    except Exception:
        return False


def get_releases_url():
    releases_url = get_project_info().get('links', {}).get('releases') or ''
    parts = [part for part in releases_url.split('/') if part]
    if 'github.com' not in parts:
        return None
    index = parts.index('github.com')
    if len(parts) < index + 3:
        return None
    return f"https://api.github.com/repos/{parts[index + 1]}/{parts[index + 2]}/releases"


def fetch_releases(limit=RELEASES_LIMIT, force=False):
    now = time.time()
    if not force and _releases_cache['items'] and _releases_cache['expires_at'] > now:
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
        timeout=8
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


def get_latest_release_version():
    releases = fetch_releases()
    for release in releases:
        if release.get('prerelease'):
            continue
        tag = (release.get('tag') or '').strip().lstrip('vV')
        if tag:
            return tag
    return None


def check_for_updates(force=False):
    try:
        now = time.time()
        if not force and _update_cache['data'] and _update_cache['expires_at'] > now:
            return _update_cache['data']
        current_version = get_current_version()
        latest_version = get_latest_release_version()
        if not latest_version:
            return {
                'update_available': False,
                'current_version': current_version,
                'latest_version': None,
                'error': 'releases_unavailable'
            }
        result = {
            'update_available': is_newer_version(latest_version, current_version),
            'current_version': current_version,
            'latest_version': latest_version
        }
        _update_cache['data'] = result
        _update_cache['expires_at'] = now + UPDATE_CHECK_TTL
        return result
    except Exception as e:
        return {
            'update_available': False,
            'current_version': get_current_version(),
            'latest_version': None,
            'error': str(e)
        }


def get_update_dir():
    try:
        path = Path(DATA_DIR) / 'update'
        path.mkdir(parents=True, exist_ok=True)
        return path
    except Exception:
        return None


def is_agent_alive():
    directory = get_update_dir()
    if not directory:
        return False
    heartbeat = directory / 'agent.seen'
    try:
        return (time.time() - heartbeat.stat().st_mtime) <= AGENT_FRESH_SECONDS
    except Exception:
        return False


def read_update_status():
    directory = get_update_dir()
    status = {
        'state': 'idle',
        'message': '',
        'updated_at': None,
        'pending': False,
        'agent_alive': False,
        'current_version': get_current_version()
    }
    if not directory:
        status['state'] = 'unavailable'
        return status
    status['pending'] = (directory / 'request').exists()
    status['agent_alive'] = is_agent_alive()
    status_file = directory / 'status'
    try:
        if status_file.exists():
            for line in status_file.read_text(encoding='utf-8').splitlines():
                if '=' in line:
                    key, value = line.split('=', 1)
                    status[key.strip()] = value.strip()
    except Exception:
        pass
    return status


def is_valid_backup(backup_path):
    if not backup_path:
        return False
    try:
        path = Path(backup_path)
        if not path.exists() or path.stat().st_size <= 0:
            return False
        with zipfile.ZipFile(path) as archive:
            return archive.testzip() is None
    except Exception:
        return False


def request_update(target_version=None):
    directory = get_update_dir()
    if not directory:
        return {'ok': False, 'error': 'Недоступен каталог данных для обновления.'}
    if not is_agent_alive():
        return {'ok': False, 'error': 'Хост-агент обновления не установлен или не отвечает.'}
    request_file = directory / 'request'
    if request_file.exists():
        return {'ok': False, 'error': 'Обновление уже выполняется.'}
    status = read_update_status()
    if status.get('state') == 'running':
        try:
            updated_at = float(status.get('updated_at') or 0)
        except Exception:
            updated_at = 0
        if (time.time() - updated_at) < 900:
            return {'ok': False, 'error': 'Обновление уже выполняется.'}
    try:
        backup_path = backup_manager.create_backup_file()
    except Exception:
        backup_path = None
    if not is_valid_backup(backup_path):
        return {'ok': False, 'error': 'Не удалось создать резервную копию базы. Обновление отменено.'}
    try:
        request_file.write_text(
            f'backup={backup_path.name}\ntarget={target_version or ""}\nrequested_at={int(time.time())}\n',
            encoding='utf-8'
        )
    except Exception:
        return {'ok': False, 'error': 'Не удалось передать запрос на обновление.'}
    _update_cache['data'] = None
    return {'ok': True, 'backup': backup_path.name}


def register_update_routes(flask_app, login_required):
    @flask_app.route('/update/check', methods=['GET'])
    @login_required
    def check_updates_route():
        force = request.args.get('fresh') == '1'
        return check_for_updates(force=force)

    @flask_app.route('/update/releases', methods=['GET'])
    @login_required
    def get_releases_route():
        try:
            force = request.args.get('fresh') == '1'
            releases = fetch_releases(force=force)
            return {'ok': True, 'releases': releases}
        except Exception as e:
            return {'ok': False, 'releases': [], 'error': str(e)}

    @flask_app.route('/update/status', methods=['GET'])
    @login_required
    def update_status_route():
        return read_update_status()

    @flask_app.route('/update/apply', methods=['POST'])
    @login_required
    def update_apply_route():
        check = check_for_updates(force=True)
        if not check.get('update_available'):
            return {'ok': False, 'error': 'Обновление не требуется.'}
        return request_update(check.get('latest_version'))
