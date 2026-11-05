import json
import os
import requests
import time

RELEASES_CACHE_TTL = 600
RELEASES_LIMIT = 1

_releases_cache = {
    'expires_at': 0,
    'items': []
}

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

    @flask_app.route('/update/releases', methods=['GET'])
    @login_required
    def get_releases_route():
        try:
            releases = fetch_releases()
            return {'ok': True, 'releases': releases}
        except Exception as e:
            return {'ok': False, 'releases': [], 'error': str(e)}
