import json
import os
import requests

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
        
        response = requests.get(update_url, timeout=5)
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

def register_update_routes(flask_app, login_required):
    @flask_app.route('/update/check', methods=['GET'])
    @login_required
    def check_updates_route():
        result = check_for_updates()
        return result
