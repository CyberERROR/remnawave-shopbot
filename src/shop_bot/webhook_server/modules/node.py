from flask import render_template, Blueprint, request, jsonify, flash, redirect, url_for
import logging
import asyncio
import os
import json
import uuid
import threading
from datetime import datetime, timezone, timedelta
import re
from shop_bot.data_manager import remnawave_repository as rw_repo

node_bp = Blueprint('node', __name__)
logger = logging.getLogger(__name__)

ssh_sessions = {}
ssh_sessions_lock = threading.Lock()

def clean_ansi(text):
    if not text: return ""
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])|\x1B\(B|\x1B\][0-2];[^\x07]*\x07')
    return ansi_escape.sub('', text)

def get_msk_time() -> datetime:
    return datetime.now(timezone(timedelta(hours=3)))

def get_ssh_server(name, server_type):
    if server_type == 'host':
        hosts = rw_repo.list_squads(active_only=False)
        server = next((h for h in hosts if h.get('host_name') == name), None)
        if not server: return None, (jsonify({'ok': False, 'error': 'Хост не найден'}), 404)
        return server, None
    if server_type == 'ssh':
        ssh_targets = rw_repo.get_all_ssh_targets()
        server = next((t for t in ssh_targets if t.get('target_name') == name), None)
        if not server: return None, (jsonify({'ok': False, 'error': 'SSH-цель не найдена'}), 404)
        return server, None
    return None, (jsonify({'ok': False, 'error': 'Неверный тип сервера'}), 400)

def get_ssh_credentials(server):
    host = server.get('ssh_host')
    port = server.get('ssh_port', 22)
    username = server.get('ssh_user') or server.get('ssh_username', 'root')
    password = server.get('ssh_password')
    key_path = server.get('ssh_key_path')
    if not host or (not password and not key_path): 
        return None, (jsonify({'ok': False, 'error': 'Параметры SSH не настроены'}), 400)
    return (host, port, username, password, key_path), None

def execute_ssh_command(host, port, username, password, command, timeout=10, key_path=None):
    try:
        import paramiko
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        connect_kwargs = {
            'hostname': host,
            'port': port,
            'username': username,
            'timeout': timeout,
            'look_for_keys': False,
            'allow_agent': False
        }

        if key_path:
            actual_key_path = key_path
            if not os.path.exists(actual_key_path):
                filename = os.path.basename(key_path)
                base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                alt_path = os.path.join(base_dir, 'modules', 'keys', filename)
                if os.path.exists(alt_path):
                    actual_key_path = alt_path
                else:
                    logger.error(f"SSH Ключ не найден ни по основному ({key_path}), ни по запасному ({alt_path}) пути")
            if os.path.exists(actual_key_path):
                connect_kwargs['key_filename'] = actual_key_path
            else:
                connect_kwargs['password'] = password
        else:
            connect_kwargs['password'] = password

        import time
        retries = 3
        for attempt in range(retries):
            try:
                client.connect(**connect_kwargs)
                break
            except Exception as conn_err:
                if attempt == retries - 1:
                    raise conn_err
                time.sleep(1.5)

        stdin, stdout, stderr = client.exec_command(command, timeout=timeout)
        output = stdout.read().decode('utf-8').strip()
        error = stderr.read().decode('utf-8').strip()
        exit_status = stdout.channel.recv_exit_status()
        client.close()
        return {'ok': exit_status == 0, 'output': output, 'error': error, 'exit_status': exit_status}
    except Exception as e:
        logger.error(f"Ошибка команды SSH ({host}:{port}): {e}")
        return {'ok': False, 'output': '', 'error': str(e), 'exit_status': -1}
def format_uptime(seconds):
    days, hours, minutes = int(seconds // 86400), int((seconds % 86400) // 3600), int((seconds % 3600) // 60)
    parts = []
    if days > 0: parts.append(f"{days}д")
    if hours > 0: parts.append(f"{hours}ч")
    if minutes > 0 or not parts: parts.append(f"{minutes}м")
    return ' '.join(parts)

def register_node_routes(app, login_required, get_common_template_data):
    from shop_bot.data_manager.remnawave_repository import (
        get_all_ssh_targets,
        create_ssh_target,
        update_ssh_target_fields,
        rename_ssh_target,
        delete_ssh_target
    )
    from shop_bot.data_manager import speedtest_runner

    @app.route('/node')
    @login_required
    def node_page():
        ssh_targets = []
        try:
            ssh_targets = get_all_ssh_targets()
            logger.info(f"Node page: loaded {len(ssh_targets)} SSH targets")
        except Exception as e:
            logger.error(f"Node page: failed to load SSH targets: {e}")
            ssh_targets = []
        common_data = get_common_template_data()
        return render_template('node.html', ssh_targets=ssh_targets, **common_data)

    @app.route('/admin/ssh-targets/create', methods=['POST'], endpoint='create_ssh_target_route')
    @app.route('/node/ssh-targets/create', methods=['POST'])
    @login_required
    def node_create_ssh_target_route():
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
            return redirect(url_for('node_page'))
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
        return redirect(url_for('node_page'))

    @app.route('/admin/ssh-targets/<target_name>/update', methods=['POST'], endpoint='update_ssh_target_route')
    @app.route('/node/ssh-targets/<target_name>/update', methods=['POST'])
    @login_required
    def node_update_ssh_target_route(target_name: str):
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
                return redirect(request.referrer or url_for('node_page'))
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
        return redirect(request.referrer or url_for('node_page'))

    @app.route('/admin/ssh-targets/<target_name>/delete', methods=['POST'], endpoint='delete_ssh_target_route')
    @app.route('/node/ssh-targets/<target_name>/delete', methods=['POST'])
    @login_required
    def node_delete_ssh_target_route(target_name: str):
        ok = delete_ssh_target(target_name)
        wants_json = 'application/json' in (request.headers.get('Accept') or '') or request.headers.get('X-Requested-With') == 'XMLHttpRequest'
        if wants_json:
            return jsonify({'ok': ok, 'message': 'SSH-цель удалена' if ok else 'Не удалось удалить SSH-цель'})
        flash('SSH-цель удалена.' if ok else 'Не удалось удалить SSH-цель.', 'success' if ok else 'danger')
        return redirect(url_for('node_page'))

    @app.route('/admin/ssh-targets/<target_name>/speedtest/run', methods=['POST'], endpoint='run_ssh_target_speedtest_route')
    @app.route('/node/ssh-targets/<target_name>/speedtest/run', methods=['POST'])
    @login_required
    def node_run_ssh_target_speedtest_route(target_name: str):
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
        return redirect(request.referrer or url_for('node_page'))

    @app.route('/node/ssh-targets/speedtests/run-all', methods=['POST'])
    @login_required
    def node_run_all_ssh_target_speedtests_route():
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
            flash(f"SSH цели: выполнено {ok_count}/{total}.", 'success')
        return redirect(request.referrer or url_for('node_page'))

    @app.route('/admin/ssh-targets/<target_name>/speedtest/install', methods=['POST'], endpoint='auto_install_speedtest_on_target_route')
    @app.route('/node/ssh-targets/<target_name>/speedtest/install', methods=['POST'])
    @login_required
    def node_auto_install_speedtest_on_target_route(target_name: str):
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
        return redirect(request.referrer or url_for('node_page'))

    @app.route('/node/servers/list')
    @login_required
    def node_servers_list():
        try:
            hosts, ssh_targets = rw_repo.list_squads(active_only=False), rw_repo.get_all_ssh_targets()
            filtered_hosts = [h for h in hosts if h.get('ssh_host') and (h.get('ssh_password') or h.get('ssh_key_path'))]
            filtered_ssh_targets = [t for t in ssh_targets if t.get('ssh_host') and (t.get('ssh_password') or t.get('ssh_key_path'))]
            return jsonify({'ok': True, 'hosts': filtered_hosts, 'ssh_targets': filtered_ssh_targets})
        except Exception as e:
            logger.error(f"Ошибка получения списка серверов: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500

    @app.route('/node/servers/ssh/reorder', methods=['POST'])
    @login_required
    def node_ssh_servers_reorder():
        try:
            data = request.get_json()
            if not data: return jsonify({'ok': False, 'error': 'Некорректный JSON'}), 400
            order = data.get('order', [])
            if not isinstance(order, list): return jsonify({'ok': False, 'error': 'Неверный формат порядка'}), 400
            for index, target_name in enumerate(order): rw_repo.update_ssh_target_sort_order(target_name, index)
            return jsonify({'ok': True, 'message': 'Порядок SSH-серверов сохранён'})
        except Exception as e:
            logger.error(f"Ошибка сортировки SSH-серверов: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500

    @app.route('/node/servers/hosts/reorder', methods=['POST'])
    @login_required
    def node_hosts_reorder():
        try:
            data = request.get_json()
            if not data: return jsonify({'ok': False, 'error': 'Некорректный JSON'}), 400
            order = data.get('order', [])
            if not isinstance(order, list): return jsonify({'ok': False, 'error': 'Неверный формат порядка'}), 400
            for index, host_name in enumerate(order): rw_repo.update_host_sort_order(host_name, index)
            return jsonify({'ok': True, 'message': 'Порядок хостов сохранён'})
        except Exception as e:
            logger.error(f"Ошибка сортировки хостов: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500

    @app.route('/node/servers/uptime/<server_type>/<name>')
    @login_required
    def node_server_uptime(server_type, name):
        try:
            server, error = get_ssh_server(name, server_type)
            if error: return error
            creds, error = get_ssh_credentials(server)
            if error: return error
            host, port, username, password, key_path = creds
            delimiter = "___"
            command = (
                f"cat /proc/uptime || echo '0 0'; echo '{delimiter}'; "
                f"top -bn1 | grep 'Cpu(s)' | awk '{{print $2}}' || echo '0.0'; echo '{delimiter}'; "
                f"nproc || echo '1'; echo '{delimiter}'; "
                f"free -m | grep Mem | awk '{{print $3 \" \" $2}}' || echo '0 0'; echo '{delimiter}'; "
                f"free -m | grep Swap | awk '{{print $3 \" \" $2}}' || echo '0 0'; echo '{delimiter}'; "
                f"cat /proc/sys/vm/swappiness || echo '-1'; echo '{delimiter}'; "
                f"awk 'NR>2 && $1 !~ /lo/ {{rx += $2; tx += $10}} END {{print rx \" \" tx}}' /proc/net/dev || echo '0 0'; echo '{delimiter}'; "
                f"sleep 1; awk 'NR>2 && $1 !~ /lo/ {{rx += $2; tx += $10}} END {{print rx \" \" tx}}' /proc/net/dev || echo '0 0'"
            )
            result = execute_ssh_command(host, port, username, password, command, timeout=20, key_path=key_path)
            if result['ok']:
                try:
                    output_raw = result['output'].strip()
                    parts = output_raw.split(delimiter)
                    if len(parts) < 8:
                        logger.error(f"Ошибка формата данных для {name}: получено {len(parts)} из 8 частей")
                        return jsonify({'ok': False, 'error': 'Неполные данные о системе'}), 500
                    uptime_parts = parts[0].strip().split()
                    uptime_seconds = float(uptime_parts[0]) if (uptime_parts and uptime_parts[0]) else 0
                    cpu_str = parts[1].strip().replace(',', '.')
                    cpu_str = re.sub(r'[^0-9.]', '', cpu_str)
                    cpu_usage = float(cpu_str) if (cpu_str and cpu_str != '') else 0.0
                    cpu_cores = int(parts[2].strip()) if parts[2].strip().isdigit() else 1
                    ram_str = parts[3].strip().split()
                    ram_used, ram_total = (int(ram_str[0]), int(ram_str[1])) if len(ram_str) >= 2 else (0, 0)
                    ram_percent = (ram_used / ram_total * 100) if ram_total > 0 else 0
                    swap_str = parts[4].strip().split()
                    swap_used, swap_total = (int(swap_str[0]), int(swap_str[1])) if len(swap_str) >= 2 else (0, 0)
                    swap_percent = (swap_used / swap_total * 100) if swap_total > 0 else 0
                    swappiness_str = parts[5].strip()
                    swappiness = int(swappiness_str) if (swappiness_str and swappiness_str.replace('-','').isdigit()) else -1
                    net1_str = parts[6].strip().split()
                    rx1, tx1 = (int(float(net1_str[0])), int(float(net1_str[1]))) if len(net1_str) >= 2 else (0, 0)
                    net2_str = parts[7].strip().split()
                    rx2, tx2 = (int(float(net2_str[0])), int(float(net2_str[1]))) if len(net2_str) >= 2 else (0, 0)
                    rx_diff = max(0, rx2 - rx1)
                    tx_diff = max(0, tx2 - tx1)
                    net_rx_mbs = round(rx_diff / 1000000, 2)
                    net_tx_mbs = round(tx_diff / 1000000, 2)
                    net_rx_mbps = round((rx_diff * 8) / 1000000, 2)
                    net_tx_mbps = round((tx_diff * 8) / 1000000, 2)
                    return jsonify({
                        'ok': True, 'uptime_seconds': uptime_seconds, 'uptime_formatted': format_uptime(uptime_seconds),
                        'cpu_percent': round(cpu_usage, 1), 'cpu_cores': cpu_cores,
                        'ram_used': ram_used, 'ram_total': ram_total, 'ram_percent': round(ram_percent, 1),
                        'swap_used': swap_used, 'swap_total': swap_total, 'swap_percent': round(swap_percent, 1),
                        'swappiness': swappiness,
                        'net_rx_mbps': net_rx_mbps, 'net_tx_mbps': net_tx_mbps,
                        'net_rx_mbs': net_rx_mbs, 'net_tx_mbs': net_tx_mbs
                    })
                except Exception as parse_error:
                    logger.exception(f"Ошибка парсинга системной инфо для {name}: {parse_error}")
                    return jsonify({'ok': False, 'error': f'Ошибка обработки данных: {parse_error}'}), 500
            else:
                error_msg = result.get('error', 'Неизвестная ошибка')
                if 'timed out' in error_msg.lower() or 'timeout' in error_msg.lower():
                    logger.warning(f"Таймаут SSH для {server_type}/{name}: {error_msg}")
                    return jsonify({'ok': False, 'error': 'Таймаут подключения к серверу'}), 503
                logger.error(f"Ошибка SSH команды для {server_type}/{name}: {error_msg}")
                return jsonify({'ok': False, 'error': error_msg}), 503
        except Exception as e:
            logger.error(f"Ошибка получения uptime для {server_type}/{name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500

    @app.route('/node/servers/reboot/<server_type>/<name>', methods=['POST'])
    @login_required
    def node_server_reboot(server_type, name):
        try:
            server, error = get_ssh_server(name, server_type)
            if error: return error
            creds, error = get_ssh_credentials(server)
            if error: return error
            host, port, username, password, key_path = creds
            logger.info(f"Перезагрузка сервера {server_type}/{name} ({host}:{port})")
            execute_ssh_command(host, port, username, password, 'sudo reboot', timeout=5, key_path=key_path)
            return jsonify({'ok': True, 'message': f'Команда перезагрузки отправлена на {name}'})
        except Exception as e:
            logger.error(f"Ошибка перезагрузки {server_type}/{name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500

    @app.route('/node/servers/deploy/check-status/<name>', methods=['GET'])
    @login_required
    def node_deploy_check_status(name):
        try:
            server, error = get_ssh_server(name, 'ssh')
            if error: return error
            creds, error = get_ssh_credentials(server)
            if error: return error
            host, port, username, password, key_path = creds
            status = {'docker_installed': False, 'directory_exists': False, 'compose_file_exists': False, 'suggested_step': 1}
            logger.info(f"Проверка Docker на {name} ({host}:{port})")
            docker_check = execute_ssh_command(host, port, username, password, 'docker --version', timeout=10, key_path=key_path)
            status['docker_installed'] = docker_check['ok']
            if status['docker_installed']:
                dir_check = execute_ssh_command(host, port, username, password, 'test -d /opt/remnanode && echo "exists"', timeout=10, key_path=key_path)
                status['directory_exists'] = 'exists' in dir_check.get('output', '')
                if status['directory_exists']:
                    compose_check = execute_ssh_command(host, port, username, password, 'test -f /opt/remnanode/docker-compose.yml && echo "exists"', timeout=10, key_path=key_path)
                    status['compose_file_exists'] = 'exists' in compose_check.get('output', '')
            if not status['docker_installed']: status['suggested_step'] = 1
            elif not status['directory_exists']: status['suggested_step'] = 2
            elif not status['compose_file_exists']: status['suggested_step'] = 3
            else: status['suggested_step'] = 5  
            return jsonify({'ok': True, 'status': status})
        except Exception as e:
            logger.error(f"Ошибка проверки статуса развертывания на {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500

    @app.route('/node/servers/deploy/install-docker/<name>', methods=['POST'])
    @login_required
    def node_deploy_install_docker(name):
        try:
            os_type = request.form.get('os_type', 'ubuntu')
            server, error = get_ssh_server(name, 'ssh')
            if error: return error
            creds, error = get_ssh_credentials(server)
            if error: return error
            host, port, username, password, key_path = creds
            docker_install_cmd = 'curl -fsSL https://get.docker.com | sh' if os_type == 'debian' else 'sudo curl -fsSL https://get.docker.com | sh'
            logger.info(f"Установка Docker на {name} ({host}:{port}) - режим {os_type}")
            result_container = {'res': None, 'done': False}
            def run_install():
                try:
                    res = execute_ssh_command(host, port, username, password, docker_install_cmd, timeout=300, key_path=key_path)
                    result_container['res'] = res
                except Exception as e:
                    result_container['res'] = {'ok': False, 'error': str(e), 'output': ''}
                finally:
                    result_container['done'] = True
            thread = threading.Thread(target=run_install)
            thread.daemon = True
            thread.start()
            thread.join(timeout=40)
            if result_container['done']:
                result = result_container['res']
                if result.get('ok'):
                    return jsonify({'ok': True, 'message': 'Docker успешно установлен', 'output': result.get('output', '')})
                return jsonify({'ok': False, 'error': result.get('error') or 'Не удалось установить Docker', 'output': result.get('output', '')}), 500
            return jsonify({
                'ok': True, 
                'message': 'Установка Docker запущена и продолжается в фоновом режиме. Это может занять 2-5 минут. Пожалуйста, подождите немного перед следующим шагом.', 
                'output': 'Процесс переведен в фоновый режим для предотвращения таймаута соединения.'
            })
        except Exception as e:
            logger.error(f"Ошибка установки Docker на {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500

    @app.route('/node/servers/deploy/create-directory/<name>', methods=['POST'])
    @login_required
    def node_deploy_create_directory(name):
        try:
            server, error = get_ssh_server(name, 'ssh')
            if error: return error
            creds, error = get_ssh_credentials(server)
            if error: return error
            host, port, username, password, key_path = creds
            logger.info(f"Создание директории на {name} ({host}:{port})")
            result = execute_ssh_command(host, port, username, password, 'mkdir -p /opt/remnanode && cd /opt/remnanode && pwd', timeout=30, key_path=key_path)
            if result['ok']:
                return jsonify({'ok': True, 'message': 'Директория успешно создана', 'output': result['output']})
            return jsonify({'ok': False, 'error': result['error'] or 'Не удалось создать директорию', 'output': result['output']}), 500
        except Exception as e:
            logger.error(f"Ошибка создания директории на {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500

    @app.route('/node/servers/deploy/save-compose/<name>', methods=['POST'])
    @login_required
    def node_deploy_save_compose(name):
        try:
            content = request.form.get('content', '').strip()
            if not content: return jsonify({'ok': False, 'error': 'Содержимое обязательно'}), 400
            if "services:" not in content:
                key_candidate = content
                if "SECRET_KEY=" in key_candidate:
                    parts = key_candidate.split("SECRET_KEY=", 1)
                    key_candidate = parts[1].strip().strip('"').strip("'")
                content = (
                    "services:\n"
                    "  remnanode:\n"
                    "    container_name: remnanode\n"
                    "    hostname: remnanode\n"
                    "    image: remnawave/node:latest\n"
                    "    network_mode: host\n"
                    "    restart: always\n"
                    "    cap_add:\n"
                    "      - NET_ADMIN\n"
                    "    ulimits:\n"
                    "      nofile:\n"
                    "        soft: 1048576\n"
                    "        hard: 1048576\n"
                    "    environment:\n"
                    "      - NODE_PORT=2222\n"
                    f'      - SECRET_KEY="{key_candidate}"'
                )
            server, error = get_ssh_server(name, 'ssh')
            if error: return error
            creds, error = get_ssh_credentials(server)
            if error: return error
            host, port, username, password, key_path = creds
            logger.info(f"Сохранение docker-compose.yml на {name} ({host}:{port})")
            result = execute_ssh_command(host, port, username, password, f"cd /opt/remnanode && cat > docker-compose.yml << 'EOF'\n{content}\nEOF", timeout=30, key_path=key_path)
            if result['ok'] or result['exit_status'] == 0:
                return jsonify({'ok': True, 'message': 'docker-compose.yml успешно сохранен'})
            return jsonify({'ok': False, 'error': result['error'] or 'Не удалось сохранить docker-compose.yml', 'output': result['output']}), 500
        except Exception as e:
            logger.error(f"Ошибка сохранения docker-compose.yml на {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500

    @app.route('/node/servers/deploy/view-compose/<name>', methods=['GET'])
    @login_required
    def node_deploy_view_compose(name):
        try:
            server, error = get_ssh_server(name, 'ssh')
            if error: return error
            creds, error = get_ssh_credentials(server)
            if error: return error
            host, port, username, password, key_path = creds
            logger.info(f"Чтение docker-compose.yml с {name} ({host}:{port})")
            result = execute_ssh_command(host, port, username, password, 'cd /opt/remnanode && cat docker-compose.yml', timeout=30, key_path=key_path)
            if result['ok']: return jsonify({'ok': True, 'content': result['output']})
            return jsonify({'ok': False, 'error': result['error'] or 'Файл не найден или ошибка чтения', 'output': result['output']}), 500
        except Exception as e:
            logger.error(f"Ошибка чтения docker-compose.yml с {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500

    @app.route('/node/servers/deploy/manage-containers/<name>', methods=['POST'])
    @login_required
    def node_deploy_manage_containers(name):
        try:
            action = request.form.get('action', 'start')  
            server, error = get_ssh_server(name, 'ssh')
            if error: return error
            creds, error = get_ssh_credentials(server)
            if error: return error
            host, port, username, password, key_path = creds
            if action == 'start': command, timeout = 'cd /opt/remnanode && docker compose up -d', 120
            elif action == 'restart': command, timeout = 'cd /opt/remnanode && docker compose restart remnanode', 60
            elif action == 'logs': command, timeout = 'cd /opt/remnanode && docker compose logs -t --tail=100 remnanode', 30
            else: return jsonify({'ok': False, 'error': 'Неверное действие'}), 400
            logger.info(f"Управление контейнерами на {name} ({host}:{port}) - действие: {action}")
            result = execute_ssh_command(host, port, username, password, command, timeout=timeout, key_path=key_path)
            if result['ok'] or result['exit_status'] == 0:
                return jsonify({'ok': True, 'message': f'Действие {action} успешно выполнено', 'output': result['output']})
            return jsonify({'ok': False, 'error': result['error'] or f'Не удалось выполнить {action}', 'output': result['output']}), 500
        except Exception as e:
            logger.error(f"Ошибка управления контейнерами на {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500

    @app.route('/node/servers/deploy/remove-all/<name>', methods=['POST'])
    @login_required
    def node_deploy_remove_all(name):
        try:
            server, error = get_ssh_server(name, 'ssh')
            if error: return error
            creds, error = get_ssh_credentials(server)
            if error: return error
            host, port, username, password, key_path = creds
            command = (
                '(if [ -f /opt/remnanode/docker-compose.yml ]; then cd /opt/remnanode && sudo docker compose down 2>/dev/null || true; fi; '
                'sudo rm -rf /opt/remnanode; '
                'if command -v docker &> /dev/null; then '
                'sudo apt-get purge -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin docker-ce-rootless-extras 2>/dev/null || true; '
                'sudo rm -rf /var/lib/docker /var/lib/containerd ~/.docker 2>/dev/null || true; fi; '
                'echo "Cleanup completed")'
            )
            logger.warning(f"УДАЛЕНИЕ ВСЕХ ДАННЫХ Docker и ноды на {name} ({host}:{port})")
            result = execute_ssh_command(host, port, username, password, command, timeout=180, key_path=key_path)
            if result.get('output') and 'Cleanup completed' in result.get('output', ''):
                return jsonify({'ok': True, 'message': 'Нода и Docker полностью удалены', 'output': result['output']})
            if result.get('ok') or result.get('exit_status') == 0:
                return jsonify({'ok': True, 'message': 'Команда удаления выполнена', 'output': result.get('output', '')})
            logger.error(f"Удаление не удалось на {name}: {result.get('error')}, вывод: {result.get('output')}")
            return jsonify({'ok': False, 'error': result.get('error') or 'Не удалось выполнить удаление', 'output': result.get('output', '')}), 500
        except Exception as e:
            logger.error(f"Ошибка при полном удалении на {name}: {e}", exc_info=True)
            return jsonify({'ok': False, 'error': str(e)}), 500

    @app.route('/node/servers/warp/status/<name>', methods=['GET'])
    @login_required
    def node_warp_status(name):
        try:
            server, error = get_ssh_server(name, 'ssh')
            if error: return error
            creds, error = get_ssh_credentials(server)
            if error: return error
            host, port, username, password, key_path = creds
            command = (
                "systemctl is-active wireproxy; "
                "if systemctl list-unit-files | grep -q wireproxy; then echo 'SERVICE_EXISTS'; else echo 'SERVICE_MISSING'; fi; "
                "if [ -f /usr/local/bin/wireproxy ] || [ -f /usr/bin/wireproxy ]; then echo 'BINARY_FOUND'; else echo 'BINARY_MISSING'; fi; "
                "systemctl cat wireproxy 2>/dev/null | grep -E 'MemoryMax|MemoryHigh' || true"
            )
            result = execute_ssh_command(host, port, username, password, command, timeout=15, key_path=key_path)
            status = {'installed': False, 'active': False, 'service_exists': False, 'binary_exists': False, 'memory_max': 'N/A', 'memory_high': 'N/A'}
            if result['ok']:
                lines = result['output'].splitlines()
                if len(lines) >= 3:
                    status['active'] = lines[0].strip() == 'active'
                    status['service_exists'] = 'SERVICE_EXISTS' in result['output']
                    status['binary_exists'] = 'BINARY_FOUND' in result['output']
                    status['installed'] = status['binary_exists']
                    import re
                    all_max = re.findall(r'MemoryMax=([^\s]+)', result['output'])
                    all_high = re.findall(r'MemoryHigh=([^\s]+)', result['output'])
                    if all_max: status['memory_max'] = all_max[-1]
                    if all_high: status['memory_high'] = all_high[-1]
            return jsonify({'ok': True, 'status': status})
        except Exception as e:
            logger.error(f"Ошибка проверки статуса WARP на {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500

    @app.route('/node/servers/warp/install/<name>', methods=['POST'])
    @login_required
    def node_warp_install(name):
        try:
            server, error = get_ssh_server(name, 'ssh')
            if error: return error
            creds, error = get_ssh_credentials(server)
            if error: return error
            host, port, username, password, key_path = creds
            install_cmd = "printf '1\\n1\\n40000\\n' | bash <(curl -fsSL https://gitlab.com/fscarmen/warp/-/raw/main/menu.sh) w"
            logger.info(f"Установка WARP на {name} ({host}:{port})")
            result = execute_ssh_command(host, port, username, password, install_cmd, timeout=300, key_path=key_path)
            if result['ok'] or "Socks5 configured" in result['output']:
                try:
                    config_cmd = (
                        "mkdir -p /etc/systemd/system/wireproxy.service.d && "
                        "printf '[Service]\\nEnvironment=\"WG_LOG_LEVEL=error\"\\nStandardOutput=null\\nStandardError=journal\\nMemoryMax=800M\\nMemoryHigh=1G\\n' > /etc/systemd/system/wireproxy.service.d/override.conf && "
                        "systemctl daemon-reload && systemctl restart wireproxy"
                    )
                    logger.info(f"Применение настроек по умолчанию для WARP на {name}")
                    config_res = execute_ssh_command(host, port, username, password, config_cmd, timeout=30, key_path=key_path)
                    if config_res['ok']: result['output'] += "\n[Config] Applied default settings (800M/1G)"
                    else: result['output'] += f"\n[Config] Failed to apply defaults: {config_res['error']}"
                except Exception as e: logger.error(f"Не удалось применить конфигурацию на {name}: {e}")
            if result['ok'] or "Socks5 configured" in result['output']:
                 return jsonify({'ok': True, 'message': 'WARP успешно установлен', 'output': result['output']})
            return jsonify({'ok': False, 'error': result['error'] or 'Ошибка установки', 'output': result['output']}), 500
        except Exception as e:
            logger.error(f"Ошибка установки WARP на {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500

    @app.route('/node/servers/warp/uninstall/<name>', methods=['POST'])
    @login_required
    def node_warp_uninstall(name):
        try:
            server, error = get_ssh_server(name, 'ssh')
            if error: return error
            creds, error = get_ssh_credentials(server)
            if error: return error
            host, port, username, password, key_path = creds
            logger.info(f"Удаление WARP на {name}")
            result = execute_ssh_command(host, port, username, password, "printf 'y\\n' | bash <(curl -fsSL https://gitlab.com/fscarmen/warp/-/raw/main/menu.sh) u", timeout=120, key_path=key_path)
            if result['ok']: return jsonify({'ok': True, 'message': 'WARP успешно удален', 'output': result['output']})
            return jsonify({'ok': False, 'error': result['error'] or 'Ошибка удаления', 'output': result['output']}), 500
        except Exception as e:
            logger.error(f"Ошибка удаления WARP на {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500

    @app.route('/node/servers/warp/config/<name>', methods=['POST'])
    @login_required
    def node_warp_config(name):
        try:
            server, error = get_ssh_server(name, 'ssh')
            if error: return error
            creds, error = get_ssh_credentials(server)
            if error: return error
            host, port, username, password, key_path = creds
            memory_max, memory_high = request.form.get('memory_max', '800M'), request.form.get('memory_high', '1G')
            override_dir, override_file = '/etc/systemd/system/wireproxy.service.d', '/etc/systemd/system/wireproxy.service.d/override.conf'
            check_result = execute_ssh_command(host, port, username, password, f"test -f {override_file} && echo 'EXISTS' || echo 'NOT_EXISTS'", timeout=10, key_path=key_path)
            if check_result['ok'] and 'EXISTS' in check_result['output']:
                cmd = (f"mkdir -p {override_dir} && "
                       f"if grep -q '^MemoryMax=' {override_file}; then sed -i 's/^MemoryMax=.*/MemoryMax={memory_max}/' {override_file}; else sed -i '/^\\[Service\\]/a MemoryMax={memory_max}' {override_file}; fi && "
                       f"if grep -q '^MemoryHigh=' {override_file}; then sed -i 's/^MemoryHigh=.*/MemoryHigh={memory_high}/' {override_file}; else sed -i '/^\\[Service\\]/a MemoryHigh={memory_high}' {override_file}; fi && "
                       "systemctl daemon-reload && systemctl restart wireproxy")
            else:
                content = f"[Service]\nMemoryMax={memory_max}\nMemoryHigh={memory_high}\n"
                safe_content = content.replace("'", "'\"'\"'")
                cmd = (f"mkdir -p {override_dir} && printf '%s' '{safe_content}' > {override_file} && "
                       "systemctl daemon-reload && systemctl restart wireproxy")
            logger.info(f"Настройка WARP на {name}: {memory_max}/{memory_high}")
            result = execute_ssh_command(host, port, username, password, cmd, timeout=60, key_path=key_path)
            if result['ok']: return jsonify({'ok': True, 'message': 'Конфигурация обновлена и сервис перезапущен', 'output': result['output']})
            return jsonify({'ok': False, 'error': result['error'] or 'Ошибка конфигурации', 'output': result['output']}), 500
        except Exception as e:
             logger.error(f"Ошибка настройки WARP на {name}: {e}")
             return jsonify({'ok': False, 'error': str(e)}), 500

    @app.route('/node/servers/warp/restart/<name>', methods=['POST'])
    @login_required
    def node_warp_restart(name):
        try:
             ssh_targets = rw_repo.get_all_ssh_targets()
             server = next((t for t in ssh_targets if t.get('target_name') == name), None)
             if not server: return jsonify({'ok': False, 'error': 'SSH цель не найдена'}), 404
             host, port, username, password, key_path = server.get('ssh_host'), server.get('ssh_port', 22), server.get('ssh_username', 'root'), server.get('ssh_password'), server.get('ssh_key_path')
             if not host or (not password and not key_path): return jsonify({'ok': False, 'error': 'SSH данные не настроены'}), 400
             result = execute_ssh_command(host, port, username, password, "systemctl restart wireproxy", timeout=30, key_path=key_path)
             if result['ok']: return jsonify({'ok': True, 'message': 'Сервис wireproxy перезапущен'})
             return jsonify({'ok': False, 'error': result['error']}), 500
        except Exception as e:
             logger.error(f"Ошибка перезапуска WARP на {name}: {e}")
             return jsonify({'ok': False, 'error': str(e)}), 500

    @app.route('/node/servers/warp/start/<name>', methods=['POST'])
    @login_required
    def node_warp_start(name):
        try:
            server, error = get_ssh_server(name, 'ssh')
            if error: return error
            creds, error = get_ssh_credentials(server)
            if error: return error
            host, port, username, password, key_path = creds
            result = execute_ssh_command(host, port, username, password, "systemctl start wireproxy", timeout=30, key_path=key_path)
            if result['ok']: return jsonify({'ok': True, 'message': 'Сервис запущен'})
            return jsonify({'ok': False, 'error': result['error'] or 'Ошибка запуска'}), 500
        except Exception as e:
            logger.error(f"Ошибка запуска WARP на {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500

    @app.route('/node/servers/warp/stop/<name>', methods=['POST'])
    @login_required
    def node_warp_stop(name):
        try:
            server, error = get_ssh_server(name, 'ssh')
            if error: return error
            creds, error = get_ssh_credentials(server)
            if error: return error
            host, port, username, password, key_path = creds
            result = execute_ssh_command(host, port, username, password, "systemctl stop wireproxy", timeout=30, key_path=key_path)
            if result['ok']: return jsonify({'ok': True, 'message': 'Сервис остановлен'})
            return jsonify({'ok': False, 'error': result['error'] or 'Ошибка остановки'}), 500
        except Exception as e:
            logger.error(f"Ошибка остановки WARP на {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500

    @app.route('/node/servers/swap/install/<name>', methods=['POST'])
    @login_required
    def node_swap_install(name):
        try:
            size_mb = request.form.get('size_mb', '2048')
            if not size_mb.isdigit(): return jsonify({'ok': False, 'error': 'Некорректный размер'}), 400
            server, error = get_ssh_server(name, 'ssh')
            if error: return error
            creds, error = get_ssh_credentials(server)
            if error: return error
            host, port, username, password, key_path = creds
            cmd = (f"fallocate -l {size_mb}M /swapfile || dd if=/dev/zero of=/swapfile bs=1M count={size_mb}; "
                   "chmod 600 /swapfile; mkswap /swapfile; swapon /swapfile; "
                   "grep -q '/swapfile' /etc/fstab || echo '/swapfile none swap sw 0 0' >> /etc/fstab")
            logger.info(f"Установка SWAP ({size_mb}MB) на {name}")
            result = execute_ssh_command(host, port, username, password, cmd, timeout=120, key_path=key_path)
            if result['ok']: return jsonify({'ok': True, 'message': 'SWAP установлен'})
            return jsonify({'ok': False, 'error': result['error'] or 'Не удалось установить SWAP'}), 500
        except Exception as e:
            logger.error(f"Ошибка установки SWAP на {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500

    @app.route('/node/servers/swap/delete/<name>', methods=['DELETE'])
    @login_required
    def node_swap_delete(name):
        try:
            server, error = get_ssh_server(name, 'ssh')
            if error: return error
            creds, error = get_ssh_credentials(server)
            if error: return error
            host, port, username, password, key_path = creds
            cmd = "swapoff /swapfile; rm /swapfile; sed -i '/\\/swapfile/d' /etc/fstab"
            logger.info(f"Удаление SWAP на {name}")
            result = execute_ssh_command(host, port, username, password, cmd, timeout=60, key_path=key_path)
            if result['ok']: return jsonify({'ok': True, 'message': 'SWAP удален'})
            return jsonify({'ok': False, 'error': result['error'] or 'Не удалось удалить SWAP'}), 500
        except Exception as e:
            logger.error(f"Ошибка удаления SWAP на {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
            
    @app.route('/node/servers/swap/resize/<name>', methods=['POST'])
    @login_required
    def node_swap_resize(name):
        try:
            size_mb = request.form.get('size_mb', '2048')
            if not size_mb.isdigit(): return jsonify({'ok': False, 'error': 'Некорректный размер'}), 400
            server, error = get_ssh_server(name, 'ssh')
            if error: return error
            creds, error = get_ssh_credentials(server)
            if error: return error
            host, port, username, password, key_path = creds
            cmd = (f"if grep -q '/swapfile' /proc/swaps; then swapoff /swapfile || exit 1; fi && rm -f /swapfile && "
                   f"fallocate -l {size_mb}M /swapfile || dd if=/dev/zero of=/swapfile bs=1M count={size_mb} && "
                   "chmod 600 /swapfile && mkswap /swapfile && swapon /swapfile && "
                   "grep -q '/swapfile' /etc/fstab || echo '/swapfile none swap sw 0 0' >> /etc/fstab")
            logger.info(f"Изменение размера SWAP до {size_mb}MB на {name}")
            result = execute_ssh_command(host, port, username, password, cmd, timeout=180, key_path=key_path)
            if result['ok']: return jsonify({'ok': True, 'message': 'Размер SWAP изменен'})
            return jsonify({'ok': False, 'error': result['error'] or 'Не удалось изменить размер SWAP'}), 500
        except Exception as e:
            logger.error(f"Ошибка изменения размера SWAP на {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500

    @app.route('/node/servers/swap/swappiness/<name>', methods=['POST'])
    @login_required
    def node_swap_swappiness(name):
        try:
            swappiness = request.form.get('swappiness', '60')
            if not swappiness.isdigit() or not (0 <= int(swappiness) <= 100): return jsonify({'ok': False, 'error': 'Некорректное значение (0-100)'}), 400
            server, error = get_ssh_server(name, 'ssh')
            if error: return error
            creds, error = get_ssh_credentials(server)
            if error: return error
            host, port, username, password, key_path = creds
            cmd = (f"sysctl vm.swappiness={swappiness}; "
                   f"if grep -q 'vm.swappiness' /etc/sysctl.conf; then sed -i 's/^vm.swappiness.*/vm.swappiness={swappiness}/' /etc/sysctl.conf; "
                   f"else echo 'vm.swappiness={swappiness}' >> /etc/sysctl.conf; fi")
            logger.info(f"Изменение swappiness на {swappiness} на {name}")
            result = execute_ssh_command(host, port, username, password, cmd, timeout=30, key_path=key_path)
            if result['ok']: return jsonify({'ok': True, 'message': 'Параметр swappiness обновлен'})
            return jsonify({'ok': False, 'error': result['error'] or 'Не удалось обновить swappiness'}), 500
        except Exception as e:
            logger.error(f"Ошибка изменения swappiness на {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500

    @app.route('/node/servers/warp/systemd/get/<name>', methods=['GET'])
    @login_required
    def node_warp_systemd_get(name):
        try:
            server, error = get_ssh_server(name, 'ssh')
            if error: return error
            creds, error = get_ssh_credentials(server)
            if error: return error
            host, port, username, password, key_path = creds
            result = execute_ssh_command(host, port, username, password, "if [ -f /etc/systemd/system/wireproxy.service.d/override.conf ]; then cat /etc/systemd/system/wireproxy.service.d/override.conf; else echo ''; fi", timeout=15, key_path=key_path)
            if result['ok']: return jsonify({'ok': True, 'content': result['output']})
            return jsonify({'ok': False, 'error': result['error'] or 'Не удалось прочитать конфиг'}), 500
        except Exception as e:
            logger.error(f"Ошибка чтения системного конфига на {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500

    @app.route('/node/servers/warp/systemd/save/<name>', methods=['POST'])
    @login_required
    def node_warp_systemd_save(name):
        try:
            content = request.form.get('content', '')
            server, error = get_ssh_server(name, 'ssh')
            if error: return error
            creds, error = get_ssh_credentials(server)
            if error: return error
            host, port, username, password, key_path = creds
            override_dir, override_file = '/etc/systemd/system/wireproxy.service.d', '/etc/systemd/system/wireproxy.service.d/override.conf'
            safe_content = content.replace("'", "'\"'\"'")
            cmd = (f"mkdir -p {override_dir} && printf '%s' '{safe_content}' > {override_file} && "
                   "systemctl daemon-reload && systemctl restart wireproxy")
            logger.info(f"Сохранение системного конфига на {name}")
            result = execute_ssh_command(host, port, username, password, cmd, timeout=60, key_path=key_path)
            if result['ok']: return jsonify({'ok': True, 'message': 'Конфигурация сохранена и сервис перезапущен'})
            return jsonify({'ok': False, 'error': result['error'] or 'Не удалось сохранить конфиг'}), 500
        except Exception as e:
            logger.error(f"Ошибка сохранения системного конфига на {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500

    @app.route('/node/servers/warp/logs/usage/<name>', methods=['GET'])
    @login_required
    def node_warp_logs_usage(name):
        try:
            server, error = get_ssh_server(name, 'ssh')
            if error: return error
            creds, error = get_ssh_credentials(server)
            if error: return error
            host, port, username, password, key_path = creds
            result = execute_ssh_command(host, port, username, password, "journalctl --disk-usage", timeout=15, key_path=key_path)
            if result['ok']: return jsonify({'ok': True, 'usage': result['output']})
            return jsonify({'ok': False, 'error': result['error']}), 500
        except Exception as e:
            logger.error(f"Ошибка проверки использования логов на {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500

    @app.route('/node/servers/warp/logs/clean/<name>', methods=['POST'])
    @login_required
    def node_warp_logs_clean(name):
        try:
            max_size, max_age = request.form.get('max_size', '0'), request.form.get('max_age', '0')
            if not max_size.isdigit() or not max_age.isdigit(): return jsonify({'ok': False, 'error': 'Некорректные значения'}), 400
            s_int, a_int = int(max_size), int(max_age)
            if s_int == 0 and a_int == 0: return jsonify({'ok': False, 'error': 'Укажите размер или возраст'}), 400
            server, error = get_ssh_server(name, 'ssh')
            if error: return error
            creds, error = get_ssh_credentials(server)
            if error: return error
            host, port, username, password, key_path = creds
            cmd_parts = ['sudo journalctl -u wireproxy.service']
            if s_int > 0: cmd_parts.append(f'--vacuum-size={s_int}M')
            if a_int > 0: cmd_parts.append(f'--vacuum-time={a_int}d')
            logger.info(f"Очистка логов wireproxy на {name}: {' '.join(cmd_parts)}")
            result = execute_ssh_command(host, port, username, password, ' '.join(cmd_parts), timeout=60, key_path=key_path)
            if result['ok']: return jsonify({'ok': True, 'message': 'Логи wireproxy очищены', 'output': result['output']})
            return jsonify({'ok': False, 'error': result['error'] or 'Ошибка очистки логов', 'output': result['output']}), 500
        except Exception as e:
            logger.error(f"Ошибка очистки логов на {name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500

    @app.route('/node/servers/execute/<server_type>/<name>', methods=['POST'])
    @login_required
    def node_server_execute_command(server_type, name):
        try:
            import paramiko, time, re
            from flask import Response, stream_with_context
            command = request.form.get('command', '')
            server, error = get_ssh_server(name, server_type)
            if error: return error
            creds, error = get_ssh_credentials(server)
            if error: return error
            host, port, username, password, key_path = creds
            session_key = f"{server_type}:{name}"
            def generate():
                global ssh_sessions
                client, channel = None, None
                try:
                    with ssh_sessions_lock:
                        session = ssh_sessions.get(session_key)
                        if session:
                            client, channel = session.get('client'), session.get('channel')
                            try:
                                transport = client.get_transport()
                                if not (transport and transport.is_active() and not channel.closed): client, channel = None, None
                            except: client, channel = None, None
                    if not client or not channel:
                        client = paramiko.SSHClient()
                        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                        yield f"data: [INFO] Подключение к {host}:{port}...\n\n"
                        connect_kwargs = {
                            'hostname': host,
                            'port': port,
                            'username': username,
                            'timeout': 30,
                            'look_for_keys': False,
                            'allow_agent': False
                        }
                        if key_path:
                            actual_key_path = key_path
                            if not os.path.exists(actual_key_path):
                                filename = os.path.basename(key_path)
                                base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                                alt_path = os.path.join(base_dir, 'modules', 'keys', filename)
                                if os.path.exists(alt_path): actual_key_path = alt_path
                            if os.path.exists(actual_key_path): connect_kwargs['key_filename'] = actual_key_path
                            else: connect_kwargs['password'] = password
                        else:
                            connect_kwargs['password'] = password
                        client.connect(**connect_kwargs)
                        yield f"data: [INFO] Соединение установлено. Запуск оболочки...\n\n"
                        channel = client.invoke_shell(term='xterm', width=200, height=50)
                        channel.settimeout(0.1)
                        with ssh_sessions_lock: ssh_sessions[session_key] = {'client': client, 'channel': channel, 'created': time.time()}
                        time.sleep(0.5)
                        while channel.recv_ready(): channel.recv(4096)
                    channel.send(command + '\n')
                    start_time, idle_count, max_idle, timeout = time.time(), 0, 50, 30
                    while True:
                        try:
                            if channel.recv_ready():
                                data = channel.recv(4096)
                                if data:
                                    idle_count = 0
                                    try:
                                        decoded = data.decode('utf-8', errors='replace')
                                        # Detect cursor up sequences for partial replace
                                        cursor_up_re = re.compile(r'\x1b\[(\d+)A')
                                        up_matches = cursor_up_re.findall(decoded)
                                        max_up = max((int(x) for x in up_matches), default=0)
                                        # Detect clear screen / cursor home patterns (for top, htop, etc.)
                                        clear_screen_re = re.compile(r'\x1b\[2J|\x1b\[H|\x1b\[1;1H|\x1b\[0?J')
                                        has_clear_screen = bool(clear_screen_re.search(decoded))
                                        cleaned = clean_ansi(decoded)
                                        cleaned = cleaned.replace('\r\n', '\n')
                                        result_lines = []
                                        for seg in cleaned.split('\n'):
                                            if '\r' in seg:
                                                parts = seg.split('\r')
                                                last_part = parts[-1]
                                                if last_part.rstrip():
                                                    result_lines.append(last_part.rstrip())
                                            elif seg.rstrip():
                                                result_lines.append(seg.rstrip())
                                        # Send clear command if screen was cleared (full redraw like top)
                                        if has_clear_screen and result_lines:
                                            yield f"data: \x01CLEAR\x01\n\n"
                                        elif max_up > 0 and result_lines:
                                            yield f"data: \x01REPLACE:{max_up}\x01\n\n"
                                        for line in result_lines:
                                            yield f"data: {line}\n\n"
                                        yield f"data: \x01FLUSH\x01\n\n"  # Force flush marker
                                    except Exception as ex: logger.error(f"Ошибка декодирования вывода: {ex}")
                            else: idle_count += 1
                            if channel.closed:
                                yield f"data: [INFO] Сессия закрыта\n\n"
                                with ssh_sessions_lock:
                                    if session_key in ssh_sessions: del ssh_sessions[session_key]
                                break
                            if idle_count >= max_idle or (time.time() - start_time > timeout): break
                            time.sleep(0.1)
                        except Exception as loop_ex:
                            logger.error(f"Ошибка цикла SSH: {loop_ex}")
                            break
                except Exception as e:
                    logger.error(f"Ошибка выполнения команды на {server_type}/{name}: {e}")
                    yield f"data: [ERROR] {str(e)}\n\n"
                    with ssh_sessions_lock:
                        if session_key in ssh_sessions: del ssh_sessions[session_key]
                finally: yield "data: [DONE]\n\n"
            return Response(stream_with_context(generate()), mimetype='text/event-stream', headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no', 'Connection': 'keep-alive'})
        except Exception as e:
            logger.error(f"Ошибка в server_execute_command для {server_type}/{name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500

    @app.route('/node/servers/execute/close/<server_type>/<name>', methods=['POST'])
    @login_required
    def node_close_ssh_session(server_type, name):
        try:
            session_key = f"{server_type}:{name}"
            with ssh_sessions_lock:
                session = ssh_sessions.pop(session_key, None)
                if session:
                    try:
                        if session.get('channel'): session['channel'].close()
                        if session.get('client'): session['client'].close()
                    except: pass
                    return jsonify({'ok': True, 'message': 'Сессия закрыта'})
                return jsonify({'ok': True, 'message': 'Активная сессия не найдена'})
        except Exception as e:
            logger.error(f"Ошибка закрытия SSH сессии для {server_type}/{name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500

    @app.route('/node/servers/scheduler/save/<target_name>', methods=['POST'])
    @login_required
    def node_save_scheduler_config(target_name):
        try:
            value = request.form.get('value')
            unit = request.form.get('unit')
            enabled = request.form.get('enabled') == 'true'
            if not value or not value.isdigit():
                 return jsonify({'ok': False, 'error': 'Некорректное значение'}), 400
            value = int(value)
            if unit not in ['minutes', 'hours', 'days']:
                 return jsonify({'ok': False, 'error': 'Некорректная единица измерения'}), 400
            config = {
                'value': value,
                'unit': unit,
                'enabled': enabled,
                'last_run': None 
            }
            ssh_targets = rw_repo.get_all_ssh_targets()
            target = next((t for t in ssh_targets if t.get('target_name') == target_name), None)
            if not target:
                return jsonify({'ok': False, 'error': 'Цель не найдена'}), 404
            json_config = json.dumps(config)
            rw_repo.update_ssh_target_scheduler(target_name, json_config)
            return jsonify({'ok': True})
        except Exception as e:
            logger.error(f"Ошибка сохранения настроек планировщика для {target_name}: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
