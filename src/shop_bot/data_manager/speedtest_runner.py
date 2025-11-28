import asyncio
import json
import logging
import re
from urllib.parse import urlparse

import aiohttp
import paramiko

from shop_bot.data_manager import remnawave_repository as rw_repo

logger = logging.getLogger(__name__)


def _parse_host_port_from_url(url: str) -> tuple[str | None, int | None, bool]:
    try:
        u = urlparse(url)
        host = u.hostname
        port = u.port
        is_https = (u.scheme == 'https')
        if port is None:
            port = 443 if is_https else 80
        return host, port, is_https
    except Exception:
        return None, None, False


async def net_probe_for_host(host_row: dict) -> dict:
    """Lightweight network probe from panel to host_url: TCP connect + HTTP GET / (HEAD).
    Returns dict with ok, ping_ms (TCP connect time), http_ms, error (if any).
    """
    url = (host_row.get('host_url') or '').strip()
    target_host, target_port, _ = _parse_host_port_from_url(url)
    result = {
        'ok': False,
        'method': 'net',
        'ping_ms': None,
        'jitter_ms': None,
        'download_mbps': None,
        'upload_mbps': None,
        'server_name': None,
        'server_id': None,
        'http_ms': None,
        'error': None,
    }
    if not target_host or not target_port:
        result['error'] = f'Invalid host_url: {url}'
        return result


    try:
        loop = asyncio.get_event_loop()
        start = loop.time()
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(target_host, target_port), timeout=10.0
        )
        tcp_ms = (loop.time() - start) * 1000.0
        result['ping_ms'] = round(tcp_ms, 2)
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass
    except Exception as e:
        result['error'] = f'TCP connect failed: {e}'
        return result


    try:
        async with aiohttp.ClientSession() as session:
            start = asyncio.get_event_loop().time()
            async with session.head(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                _ = resp.status
            http_ms = (asyncio.get_event_loop().time() - start) * 1000.0
            result['http_ms'] = round(http_ms, 2)
        result['ok'] = True
    except Exception:

        try:
            async with aiohttp.ClientSession() as session:
                start = asyncio.get_event_loop().time()
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    _ = await resp.text()
                http_ms = (asyncio.get_event_loop().time() - start) * 1000.0
                result['http_ms'] = round(http_ms, 2)
            result['ok'] = True
        except Exception as e:
            result['error'] = f'HTTP failed: {e}'
    return result


def _ssh_exec_json(ssh: paramiko.SSHClient, commands: list[str]) -> tuple[dict | None, str | None]:
    """Try commands sequentially; expect JSON on stdout. Returns (json_obj, error)."""
    for cmd in commands:
        try:
            stdin, stdout, stderr = ssh.exec_command(cmd, timeout=120)
            out = stdout.read().decode('utf-8', errors='ignore')
            err = stderr.read().decode('utf-8', errors='ignore')
            if out:
                out = out.strip()

                m = re.search(r"\{.*\}$", out, re.S)
                if m:
                    out = m.group(0)
                try:
                    data = json.loads(out)
                    return data, None
                except Exception:
                    pass
            if err:
                logger.debug(f"SSH cmd error ({cmd}): {err}")
        except Exception as e:
            logger.debug(f"SSH exec failed for '{cmd}': {e}")
            continue
    return None, 'No JSON output from speedtest commands'


def _parse_ookla_json(data: dict) -> dict:

    try:
        ping_ms = float(data.get('ping', {}).get('latency')) if data.get('ping') else None
        jitter = float(data.get('ping', {}).get('jitter')) if data.get('ping') else None
        down_bps = float(data.get('download', {}).get('bandwidth', 0)) * 8.0
        up_bps = float(data.get('upload', {}).get('bandwidth', 0)) * 8.0
        server = data.get('server', {})
        return {
            'ping_ms': round(ping_ms, 2) if ping_ms is not None else None,
            'jitter_ms': round(jitter, 2) if jitter is not None else None,
            'download_mbps': round(down_bps / (1_000_000.0), 2) if down_bps else None,
            'upload_mbps': round(up_bps / (1_000_000.0), 2) if up_bps else None,
            'server_name': server.get('name'),
            'server_id': str(server.get('id')) if server.get('id') is not None else None,
        }
    except Exception:
        return {}


def _parse_speedtest_cli_json(data: dict) -> dict:

    try:
        ping_ms = float(data.get('ping')) if data.get('ping') is not None else None
        jitter = None
        down_bps = float(data.get('download', 0))
        up_bps = float(data.get('upload', 0))
        srv = data.get('server', {})
        return {
            'ping_ms': round(ping_ms, 2) if ping_ms is not None else None,
            'jitter_ms': jitter,
            'download_mbps': round(down_bps / 1_000_000.0, 2) if down_bps else None,
            'upload_mbps': round(up_bps / 1_000_000.0, 2) if up_bps else None,
            'server_name': srv.get('name'),
            'server_id': str(srv.get('id')) if srv.get('id') is not None else None,
        }
    except Exception:
        return {}


async def ssh_speedtest_for_host(host_row: dict) -> dict:
    """Run speedtest on remote host via SSH. Tries Ookla CLI first, then speedtest-cli.
    Returns dict with ok, metrics, error.
    """
    result = {
        'ok': False,
        'method': 'ssh',
        'ping_ms': None,
        'jitter_ms': None,
        'download_mbps': None,
        'upload_mbps': None,
        'server_name': None,
        'server_id': None,
        'error': None,
    }
    ssh_host = (host_row.get('ssh_host') or '').strip()
    ssh_port = int(host_row.get('ssh_port') or 22)
    ssh_user = (host_row.get('ssh_user') or '').strip()
    ssh_password = host_row.get('ssh_password')
    ssh_key_path = (host_row.get('ssh_key_path') or '').strip() or None

    if not ssh_host or not ssh_user:
        result['error'] = 'SSH settings are not configured for host'
        return result

    def _run_ssh() -> dict:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        if ssh_key_path:
            pkey = None
            try:
                pkey = paramiko.RSAKey.from_private_key_file(ssh_key_path)
            except Exception:
                try:
                    pkey = paramiko.Ed25519Key.from_private_key_file(ssh_key_path)
                except Exception:
                    pkey = None
            ssh.connect(ssh_host, port=ssh_port, username=ssh_user, password=ssh_password, pkey=pkey, timeout=20)
        else:
            ssh.connect(ssh_host, port=ssh_port, username=ssh_user, password=ssh_password, timeout=20)


        data, err = _ssh_exec_json(ssh, [

            'speedtest --accept-license --accept-gdpr -f json',
            'speedtest --accept-license --accept-gdpr --format=json',

            'speedtest -f json',
            'speedtest --format=json',

            'speedtest-cli --json'
        ])
        ssh.close()
        if data:
            parsed = _parse_ookla_json(data)
            if not parsed.get('download_mbps') and 'download' in data:

                parsed = _parse_speedtest_cli_json(data)
            return {'ok': True, **parsed}
        return {'ok': False, 'error': err or 'unknown'}

    try:
        loop = asyncio.get_event_loop()
        out = await loop.run_in_executor(None, _run_ssh)
        result.update(out)
    except Exception as e:
        result['error'] = str(e)
        result['ok'] = False
    return result


async def run_and_store_net_probe(host_name: str) -> dict:
    host = rw_repo.get_host(host_name)
    if not host:
        return {'ok': False, 'error': 'host not found'}
    res = await net_probe_for_host(host)
    rw_repo.insert_host_speedtest(
        host_name=host_name,
        method='net',
        ping_ms=res.get('ping_ms'),
        jitter_ms=res.get('jitter_ms'),
        download_mbps=res.get('download_mbps'),
        upload_mbps=res.get('upload_mbps'),
        server_name=res.get('server_name'),
        server_id=res.get('server_id'),
        ok=bool(res.get('ok')),
        error=res.get('error'),
    )
    return res


async def run_and_store_ssh_speedtest(host_name: str) -> dict:
    host = rw_repo.get_host(host_name)
    if not host:
        return {'ok': False, 'error': 'host not found'}
    res = await ssh_speedtest_for_host(host)
    rw_repo.insert_host_speedtest(
        host_name=host_name,
        method='ssh',
        ping_ms=res.get('ping_ms'),
        jitter_ms=res.get('jitter_ms'),
        download_mbps=res.get('download_mbps'),
        upload_mbps=res.get('upload_mbps'),
        server_name=res.get('server_name'),
        server_id=res.get('server_id'),
        ok=bool(res.get('ok')),
        error=res.get('error'),
    )
    return res


async def run_both_for_host(host_name: str) -> dict:
    ok = True
    errors: list[str] = []
    out = {'ssh': None, 'net': None}
    try:
        out['ssh'] = await run_and_store_ssh_speedtest(host_name)
        if not out['ssh'].get('ok'):
            ok = False
            if out['ssh'].get('error'):
                errors.append(f"ssh: {out['ssh'].get('error')}")
    except Exception as e:
        ok = False
        errors.append(f'ssh exception: {e}')
    try:
        out['net'] = await run_and_store_net_probe(host_name)
        if not out['net'].get('ok'):
            ok = False
            if out['net'].get('error'):
                errors.append(f"net: {out['net'].get('error')}")
    except Exception as e:
        ok = False
        errors.append(f'net exception: {e}')
    return {'ok': ok, 'details': out, 'error': '; '.join(errors) if errors else None}


def _ssh_connect(host_row: dict) -> paramiko.SSHClient:
    ssh_host = (host_row.get('ssh_host') or '').strip()
    ssh_port = int(host_row.get('ssh_port') or 22)
    ssh_user = (host_row.get('ssh_user') or '').strip()
    ssh_password = host_row.get('ssh_password')
    ssh_key_path = (host_row.get('ssh_key_path') or '').strip() or None
    if not ssh_host or not ssh_user:
        raise RuntimeError('SSH settings are not configured for host')
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    pkey = None
    if ssh_key_path:
        try:
            pkey = paramiko.RSAKey.from_private_key_file(ssh_key_path)
        except Exception:
            try:
                pkey = paramiko.Ed25519Key.from_private_key_file(ssh_key_path)
            except Exception:
                pkey = None
    ssh.connect(ssh_host, port=ssh_port, username=ssh_user, password=ssh_password, pkey=pkey, timeout=20)
    return ssh


def _ssh_exec(ssh: paramiko.SSHClient, cmd: str, timeout: int = 180) -> tuple[int, str, str]:
    stdin, stdout, stderr = ssh.exec_command(cmd, timeout=timeout)
    out = stdout.read().decode('utf-8', errors='ignore')
    err = stderr.read().decode('utf-8', errors='ignore')
    rc = stdout.channel.recv_exit_status() if hasattr(stdout, 'channel') else 0
    return rc, out, err


async def auto_install_speedtest_on_host(host_name: str) -> dict:
    """Attempt to auto-install Ookla speedtest or speedtest-cli on remote host via SSH.
    Tries package manager scripts, falls back to pip speedtest-cli. Returns {'ok', 'log'}.
    """
    host = rw_repo.get_host(host_name)
    if not host:
        return {'ok': False, 'log': 'host not found'}

    def _install() -> dict:
        log_lines: list[str] = []
        try:
            ssh = _ssh_connect(host)
        except Exception as e:
            return {'ok': False, 'log': f'SSH connect failed: {e}'}
        try:

            rc, out, err = _ssh_exec(ssh, 'command -v speedtest || command -v speedtest-cli || echo "NO"')
            if 'speedtest' in out or 'speedtest-cli' in out:
                log_lines.append('Found existing speedtest binary: ' + out.strip())

                rc2, o2, e2 = _ssh_exec(ssh, 'speedtest --accept-license --accept-gdpr --version || true')
                ver_text = (o2 + e2).strip()
                if ver_text:
                    log_lines.append(('$ speedtest --accept-license --accept-gdpr --version\n' + ver_text).strip())

                need_reinstall = True
                try:
                    if '1.2.0' in ver_text:
                        need_reinstall = False
                except Exception:
                    need_reinstall = True
                if not need_reinstall:
                    return {'ok': True, 'log': '\n'.join(log_lines)}
                else:
                    log_lines.append('Different Ookla speedtest version detected; reinstalling 1.2.0 via tarball.')


            rc, out, _ = _ssh_exec(ssh, 'cat /etc/os-release || uname -a')
            os_release = out.lower()
            log_lines.append('OS detection: ' + out.strip())



            rc, arch_out, _ = _ssh_exec(ssh, 'uname -m || echo unknown')
            arch = (arch_out or '').strip()

            if arch in ('x86_64', 'amd64'):
                arch_tag = 'linux-x86_64'
            elif arch in ('aarch64', 'arm64'):
                arch_tag = 'linux-aarch64'
            elif arch in ('armv7l',):
                arch_tag = 'linux-armhf'
            else:
                arch_tag = 'linux-x86_64'
            tar_url = f'https://install.speedtest.net/app/cli/ookla-speedtest-1.2.0-{arch_tag}.tgz'
            cmds_tar = [
                f'curl -fsSL {tar_url} -o /tmp/ookla-speedtest.tgz || wget -O /tmp/ookla-speedtest.tgz {tar_url}',
                'mkdir -p /tmp/ookla-speedtest && tar -xf /tmp/ookla-speedtest.tgz -C /tmp/ookla-speedtest',
                'install -m 0755 /tmp/ookla-speedtest/speedtest /usr/local/bin/speedtest || (cp /tmp/ookla-speedtest/speedtest /usr/local/bin/speedtest && chmod +x /usr/local/bin/speedtest)',

                'speedtest --accept-license --accept-gdpr --version || true',
                'rm -rf /tmp/ookla-speedtest /tmp/ookla-speedtest.tgz'
            ]
            for c in cmds_tar:
                rc, o, e = _ssh_exec(ssh, c)
                log_lines.append(f'$ {c}\n{o}{e}'.strip())


            rc, out, err = _ssh_exec(ssh, 'command -v speedtest || echo "NO"')
            if 'NO' not in out:
                rcv, ov, ev = _ssh_exec(ssh, 'speedtest --version 2>&1 || true')
                ver_info = (ov + ev).strip()
                if '1.2.0' in ver_info:
                    log_lines.append('Installed Ookla speedtest via tarball (1.2.0): ' + out.strip())
                    return {'ok': True, 'log': '\n'.join(log_lines)}
                else:
                    log_lines.append('Tarball install finished but version check did not return 1.2.0; continuing fallbacks.')


            cmds_deb = [
                'which sudo || true',
                'export DEBIAN_FRONTEND=noninteractive',
                'curl -fsSL https://packagecloud.io/install/repositories/ookla/speedtest-cli/script.deb.sh | bash',
                'apt-get update -y || true',
                'apt-get install -y speedtest || true'
            ]
            cmds_rpm = [
                'curl -fsSL https://packagecloud.io/install/repositories/ookla/speedtest-cli/script.rpm.sh | bash',
                'yum -y install speedtest || dnf -y install speedtest || true'
            ]
            tried_ookla = False
            if 'debian' in os_release or 'ubuntu' in os_release:
                tried_ookla = True
                for c in cmds_deb:
                    rc, o, e = _ssh_exec(ssh, c)
                    log_lines.append(f'$ {c}\n{o}{e}'.strip())
            elif any(x in os_release for x in ['centos', 'rhel', 'fedora', 'almalinux', 'rocky']):
                tried_ookla = True
                for c in cmds_rpm:
                    rc, o, e = _ssh_exec(ssh, c)
                    log_lines.append(f'$ {c}\n{o}{e}'.strip())


            rc, out, err = _ssh_exec(ssh, 'command -v speedtest || command -v speedtest-cli || echo "NO"')
            if 'speedtest' in out or 'speedtest-cli' in out:
                log_lines.append('Installed speedtest successfully: ' + out.strip())

                rc2, o2, e2 = _ssh_exec(ssh, 'speedtest --accept-license --accept-gdpr --version || true')
                if o2 or e2:
                    log_lines.append(('$ speedtest --accept-license --accept-gdpr --version\n' + (o2 + e2)).strip())
                return {'ok': True, 'log': '\n'.join(log_lines)}


            pip_try = [
                'command -v python3 || command -v python || echo NO',
                'command -v pip3 || command -v pip || (apt-get update -y && apt-get install -y python3-pip) || (yum -y install python3-pip || dnf -y install python3-pip) || true',
                'pip3 install --upgrade pip || true',
                'pip3 install speedtest-cli || pip install speedtest-cli || true',

                'command -v speedtest-cli || (which python3 && python3 -m pip show speedtest-cli && ln -sf $(python3 -c "import shutil,sys; import os; print(shutil.which(\"speedtest-cli\") or \"/usr/local/bin/speedtest-cli\")") /usr/local/bin/speedtest-cli) || true'
            ]
            for c in pip_try:
                rc, o, e = _ssh_exec(ssh, c)
                log_lines.append(f'$ {c}\n{o}{e}'.strip())


            rc, out, err = _ssh_exec(ssh, 'command -v speedtest || command -v speedtest-cli || echo "NO"')
            if 'NO' not in out:
                log_lines.append('Installed speedtest-cli via pip: ' + out.strip())
                return {'ok': True, 'log': '\n'.join(log_lines)}




            return {'ok': False, 'log': 'Failed to install speedtest using available methods.\n' + '\n'.join(log_lines)}
        finally:
            try:
                ssh.close()
            except Exception:
                pass

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _install)




def _target_to_host_row(target: dict) -> dict:
    return {
        'ssh_host': (target.get('ssh_host') or '').strip(),
        'ssh_port': int(target.get('ssh_port') or 22),
        'ssh_user': (target.get('ssh_user') or '').strip(),
        'ssh_password': target.get('ssh_password'),
        'ssh_key_path': (target.get('ssh_key_path') or '').strip() or None,
    }


async def run_and_store_ssh_speedtest_for_target(target_name: str) -> dict:
    """Выполнить SSH-спидтест для отдельной цели (speedtest_ssh_targets) и сохранить результат как host_speedtests с именем цели."""
    target = rw_repo.get_ssh_target(target_name)
    if not target:
        return {'ok': False, 'error': 'target not found'}
    host_row = _target_to_host_row(target)
    res = await ssh_speedtest_for_host(host_row)
    rw_repo.insert_host_speedtest(
        host_name=target_name,
        method='ssh',
        ping_ms=res.get('ping_ms'),
        jitter_ms=res.get('jitter_ms'),
        download_mbps=res.get('download_mbps'),
        upload_mbps=res.get('upload_mbps'),
        server_name=res.get('server_name'),
        server_id=res.get('server_id'),
        ok=bool(res.get('ok')),
        error=res.get('error'),
    )
    return res


async def auto_install_speedtest_on_target(target_name: str) -> dict:
    """Автоустановка speedtest на отдельной SSH-цели."""
    target = rw_repo.get_ssh_target(target_name)
    if not target:
        return {'ok': False, 'log': 'target not found'}

    def _install() -> dict:
        log_lines: list[str] = []
        try:
            ssh = _ssh_connect(_target_to_host_row(target))
        except Exception as e:
            return {'ok': False, 'log': f'SSH connect failed: {e}'}
        try:

            rc, out, err = _ssh_exec(ssh, 'command -v speedtest || command -v speedtest-cli || echo "NO"')
            if 'speedtest' in out or 'speedtest-cli' in out:
                log_lines.append('Found existing speedtest binary: ' + out.strip())
                rc2, o2, e2 = _ssh_exec(ssh, 'speedtest --accept-license --accept-gdpr --version || true')
                ver_text = (o2 + e2).strip()
                if ver_text:
                    log_lines.append(('$ speedtest --accept-license --accept-gdpr --version\n' + ver_text).strip())
                need_reinstall = True
                try:
                    if '1.2.0' in ver_text:
                        need_reinstall = False
                except Exception:
                    need_reinstall = True
                if not need_reinstall:
                    return {'ok': True, 'log': '\n'.join(log_lines)}
                else:
                    log_lines.append('Different Ookla speedtest version detected; reinstalling 1.2.0 via tarball.')


            rc, out, _ = _ssh_exec(ssh, 'cat /etc/os-release || uname -a')
            os_release = out.lower()
            log_lines.append('OS detection: ' + out.strip())


            rc, arch_out, _ = _ssh_exec(ssh, 'uname -m || echo unknown')
            arch = (arch_out or '').strip()
            if arch in ('x86_64', 'amd64'):
                arch_tag = 'linux-x86_64'
            elif arch in ('aarch64', 'arm64'):
                arch_tag = 'linux-aarch64'
            elif arch in ('armv7l',):
                arch_tag = 'linux-armhf'
            else:
                arch_tag = 'linux-x86_64'
            tar_url = f'https://install.speedtest.net/app/cli/ookla-speedtest-1.2.0-{arch_tag}.tgz'
            cmds_tar = [
                f'curl -fsSL {tar_url} -o /tmp/ookla-speedtest.tgz || wget -O /tmp/ookla-speedtest.tgz {tar_url}',
                'mkdir -p /tmp/ookla-speedtest && tar -xf /tmp/ookla-speedtest.tgz -C /tmp/ookla-speedtest',
                'install -m 0755 /tmp/ookla-speedtest/speedtest /usr/local/bin/speedtest || (cp /tmp/ookla-speedtest/speedtest /usr/local/bin/speedtest && chmod +x /usr/local/bin/speedtest)',
                'speedtest --accept-license --accept-gdpr --version || true',
                'rm -rf /tmp/ookla-speedtest /tmp/ookla-speedtest.tgz'
            ]
            for c in cmds_tar:
                rc, o, e = _ssh_exec(ssh, c)
                log_lines.append(f'$ {c}\n{o}{e}'.strip())


            rc, out, err = _ssh_exec(ssh, 'command -v speedtest || command -v speedtest-cli || echo "NO"')
            if 'NO' not in out:
                rcv, ov, ev = _ssh_exec(ssh, 'speedtest --version 2>&1 || true')
                ver_info = (ov + ev).strip()
                if '1.2.0' in ver_info:
                    log_lines.append('Installed Ookla speedtest via tarball (1.2.0): ' + out.strip())
                    return {'ok': True, 'log': '\n'.join(log_lines)}
                else:
                    log_lines.append('Tarball install finished but version check did not return 1.2.0; continuing fallbacks.')


            cmds_deb = [
                'which sudo || true',
                'export DEBIAN_FRONTEND=noninteractive',
                'curl -fsSL https://packagecloud.io/install/repositories/ookla/speedtest-cli/script.deb.sh | bash',
                'apt-get update -y || true',
                'apt-get install -y speedtest || true'
            ]
            cmds_rpm = [
                'curl -fsSL https://packagecloud.io/install/repositories/ookla/speedtest-cli/script.rpm.sh | bash',
                'yum -y install speedtest || dnf -y install speedtest || true'
            ]
            if 'debian' in os_release or 'ubuntu' in os_release:
                for c in cmds_deb:
                    rc, o, e = _ssh_exec(ssh, c)
                    log_lines.append(f'$ {c}\n{o}{e}'.strip())
            elif any(x in os_release for x in ['centos', 'rhel', 'fedora', 'almalinux', 'rocky']):
                for c in cmds_rpm:
                    rc, o, e = _ssh_exec(ssh, c)
                    log_lines.append(f'$ {c}\n{o}{e}'.strip())

            rc, out, err = _ssh_exec(ssh, 'command -v speedtest || command -v speedtest-cli || echo "NO"')
            if 'speedtest' in out or 'speedtest-cli' in out:
                log_lines.append('Installed speedtest successfully: ' + out.strip())
                rc2, o2, e2 = _ssh_exec(ssh, 'speedtest --accept-license --accept-gdpr --version || true')
                if o2 or e2:
                    log_lines.append(('$ speedtest --accept-license --accept-gdpr --version\n' + (o2 + e2)).strip())
                return {'ok': True, 'log': '\n'.join(log_lines)}


            pip_try = [
                'command -v python3 || command -v python || echo NO',
                'command -v pip3 || command -v pip || (apt-get update -y && apt-get install -y python3-pip) || (yum -y install python3-pip || dnf -y install python3-pip) || true',
                'pip3 install --upgrade pip || true',
                'pip3 install speedtest-cli || pip install speedtest-cli || true',
                'command -v speedtest-cli || (which python3 && python3 -m pip show speedtest-cli && ln -sf $(python3 -c "import shutil,sys; import os; print(shutil.which(\"speedtest-cli\") or \"/usr/local/bin/speedtest-cli\")") /usr/local/bin/speedtest-cli) || true'
            ]
            for c in pip_try:
                rc, o, e = _ssh_exec(ssh, c)
                log_lines.append(f'$ {c}\n{o}{e}'.strip())

            rc, out, err = _ssh_exec(ssh, 'command -v speedtest || command -v speedtest-cli || echo "NO"')
            if 'NO' not in out:
                log_lines.append('Installed speedtest-cli via pip: ' + out.strip())
                return {'ok': True, 'log': '\n'.join(log_lines)}

            return {'ok': False, 'log': 'Failed to install speedtest using available methods.\n' + '\n'.join(log_lines)}
        finally:
            try:
                ssh.close()
            except Exception:
                pass

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _install)
