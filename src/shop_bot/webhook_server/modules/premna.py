import asyncio
import re
from typing import Any

from flask import jsonify, render_template

from shop_bot.data_manager import remnawave_repository as rw_repo
from shop_bot.modules import remnawave_api


def _response_data(response: Any) -> dict[str, Any]:
    payload = response.json()
    if isinstance(payload, dict):
        data = payload.get('response', payload)
        return data if isinstance(data, dict) else {}
    return {}


def _as_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _format_bytes(value: Any) -> str:
    if isinstance(value, str):
        formatted_value = value.strip()
        if formatted_value and not formatted_value.replace('.', '', 1).isdigit():
            return formatted_value
    size = _as_int(value)
    units = ('Б', 'КБ', 'МБ', 'ГБ', 'ТБ', 'ПБ')
    index = 0
    while size >= 1024 and index < len(units) - 1:
        size /= 1024
        index += 1
    return f'{size:.1f} {units[index]}' if index else f'{size} {units[index]}'


def _traffic_value(data: dict[str, Any], key: str) -> dict[str, str]:
    item = data.get(key) or {}
    current = _format_bytes(item.get('current'))
    previous = _format_bytes(item.get('previous'))
    current_bytes = _bytes_from_formatted_value(current)
    previous_bytes = _bytes_from_formatted_value(previous)
    trend = 'equal'
    if previous_bytes < current_bytes:
        trend = 'down'
    elif previous_bytes > current_bytes:
        trend = 'up'
    return {
        'current': current,
        'previous': previous,
        'trend': trend,
    }


def _bytes_from_formatted_value(value: str) -> float:
    match = re.match(r'^\s*([\d.,]+)\s*([A-Za-zА-Яа-я]+)?', value)
    if not match:
        return 0
    try:
        amount = float(match.group(1).replace(',', '.'))
    except ValueError:
        return 0
    unit = (match.group(2) or 'B').lower()
    powers = {
        'b': 0, 'byte': 0, 'bytes': 0, 'б': 0,
        'kb': 1, 'kib': 1, 'кб': 1,
        'mb': 2, 'mib': 2, 'мб': 2,
        'gb': 3, 'gib': 3, 'гб': 3,
        'tb': 4, 'tib': 4, 'тб': 4,
        'pb': 5, 'pib': 5, 'пб': 5,
    }
    return amount * (1024 ** powers.get(unit, 0))


async def load_remnawave_nodes() -> list[dict[str, Any]]:
    try:
        remnawave_api._load_config()
        response = await remnawave_api._request('GET', '/api/nodes')
    except remnawave_api.RemnawaveAPIError:
        hosts = rw_repo.list_squads(active_only=False)
        host = next(
            (
                item for item in hosts
                if item.get('remnawave_base_url') and item.get('remnawave_api_token')
            ),
            None,
        )
        if not host:
            raise
        response = await remnawave_api._request_for_host(host.get('host_name'), 'GET', '/api/nodes')
    payload = response.json()
    data = payload.get('response', payload) if isinstance(payload, dict) else payload
    if isinstance(data, dict):
        data = data.get('nodes') or data.get('data') or []
    return [node for node in data if isinstance(node, dict)] if isinstance(data, list) else []


async def _load_statistics() -> dict[str, Any]:
    params = {'tz': 'Europe/Moscow'}
    try:
        remnawave_api._load_config()
        request_api = remnawave_api._request
    except remnawave_api.RemnawaveAPIError:
        hosts = rw_repo.list_squads(active_only=False)
        host = next(
            (
                item for item in hosts
                if item.get('remnawave_base_url') and item.get('remnawave_api_token')
            ),
            None,
        )
        if not host:
            raise
        host_name = host.get('host_name')
        request_api = lambda method, path, **kwargs: remnawave_api._request_for_host(
            host_name,
            method,
            path,
            **kwargs,
        )
    stats_response, bandwidth_response, nodes_response = await asyncio.gather(
        request_api('GET', '/api/system/stats', params=params),
        request_api('GET', '/api/system/stats/bandwidth', params=params),
        request_api('GET', '/api/nodes'),
    )
    stats = _response_data(stats_response)
    bandwidth = _response_data(bandwidth_response)
    nodes_payload = nodes_response.json()
    nodes = nodes_payload.get('response', nodes_payload) if isinstance(nodes_payload, dict) else []
    users = stats.get('users') or {}
    status_counts = users.get('statusCounts') or {}
    memory = stats.get('memory') or {}
    online = stats.get('onlineStats') or {}

    return {
        'users': {
            'online': _as_int(online.get('onlineNow')),
            'active': _as_int(status_counts.get('ACTIVE')),
            'total': _as_int(users.get('totalUsers')),
            'expired': _as_int(status_counts.get('EXPIRED')),
            'disabled': _as_int(status_counts.get('DISABLED')),
            'limited': _as_int(status_counts.get('LIMITED')),
            'never_online': _as_int(online.get('neverOnline')),
            'today': _as_int(online.get('lastDay')),
        },
        'nodes': len(nodes) if isinstance(nodes, list) else 0,
        'memory': {
            'used': _format_bytes(memory.get('used')),
            'total': _format_bytes(memory.get('total')),
        },
        'traffic': {
            'today': _traffic_value(bandwidth, 'bandwidthLastTwoDays'),
            'week': _traffic_value(bandwidth, 'bandwidthLastSevenDays'),
            'month': _traffic_value(bandwidth, 'bandwidthLast30Days'),
            'year': _traffic_value(bandwidth, 'bandwidthCurrentYear'),
        },
    }


def _get_statistics() -> tuple[dict[str, Any] | None, str | None]:
    try:
        return asyncio.run(_load_statistics()), None
    except Exception as error:
        return None, str(error)


def register_premna_routes(app, login_required, get_common_template_data):
    @app.route('/premna')
    @login_required
    def premna_page():
        statistics, error = _get_statistics()
        return render_template(
            'premna.html',
            statistics=statistics,
            statistics_error=error,
            **get_common_template_data(),
        )

    @app.route('/premna/statistics.json')
    @login_required
    def premna_statistics_json():
        statistics, error = _get_statistics()
        if error:
            return jsonify({'ok': False, 'error': error}), 502
        return jsonify({'ok': True, 'statistics': statistics})
