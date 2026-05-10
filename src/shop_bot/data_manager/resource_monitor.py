import os
import time
import platform
import json
import logging
from typing import Any, Dict, List

try:
    import psutil
except Exception:
    psutil = None

from shop_bot.data_manager import remnawave_repository as rw_repo
from shop_bot.data_manager import speedtest_runner

logger = logging.getLogger(__name__)


def _safe_percent(numerator: float | int, denominator: float | int) -> float | None:
    try:
        d = float(denominator)
        if d <= 0:
            return None
        return round(float(numerator) * 100.0 / d, 2)
    except Exception:
        return None


def get_local_metrics() -> Dict[str, Any]:
    """Собрать базовые метрики локальной системы (панели).
    Требует psutil. Если psutil недоступен, возвращает ограниченную информацию.
    """
    data: Dict[str, Any] = {
        "ok": True,
        "hostname": platform.node(),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "uptime_sec": None,
        "cpu": {},
        "memory": {},
        "swap": {},
        "disks": [],
        "net": {},
        "error": None,
    }
    try:
        if psutil is None:
            data["ok"] = False
            data["error"] = "psutil not installed"
            logger.warning("psutil не установлен - мониторинг недоступен")

            try:
                data["uptime_sec"] = max(0, int(time.time() - psutil.boot_time())) if psutil else None
            except Exception:
                pass
            return data


        try:
            data["uptime_sec"] = max(0, int(time.time() - psutil.boot_time()))
        except Exception:
            data["uptime_sec"] = None


        try:
            data["cpu"] = {
                "count_logical": psutil.cpu_count(logical=True),
                "count_physical": psutil.cpu_count(logical=False),
                "percent": psutil.cpu_percent(interval=0.2),
                "loadavg": None,
            }
            try:
                data["cpu"]["loadavg"] = os.getloadavg()
            except Exception:
                data["cpu"]["loadavg"] = None
        except Exception:
            data["cpu"] = {}


        try:
            vm = psutil.virtual_memory()
            data["memory"] = {
                "total": vm.total,
                "available": vm.available,
                "used": vm.used,
                "percent": vm.percent,
            }
        except Exception:
            data["memory"] = {}


        try:
            sm = psutil.swap_memory()
            data["swap"] = {
                "total": sm.total,
                "used": sm.used,
                "percent": sm.percent,
            }
        except Exception:
            data["swap"] = {}


        disks: List[Dict[str, Any]] = []
        try:
            for part in psutil.disk_partitions(all=False):

                if any(x in (part.fstype or '').lower() for x in ["tmpfs", "devtmpfs", "squashfs", "overlay"]):
                    continue
                try:
                    usage = psutil.disk_usage(part.mountpoint)
                    disks.append({
                        "device": part.device,
                        "mountpoint": part.mountpoint,
                        "fstype": part.fstype,
                        "total": usage.total,
                        "used": usage.used,
                        "free": usage.free,
                        "percent": usage.percent,
                    })
                except Exception:
                    continue
        except Exception:
            pass
        data["disks"] = disks
        

        try:
            disk_percents = [d.get('percent') for d in data["disks"] or [] if d.get('percent') is not None]
            data["disk_percent"] = max(disk_percents) if disk_percents else None
        except Exception:
            data["disk_percent"] = None


        try:
            io = psutil.net_io_counters()
            data["net"] = {
                "bytes_sent": io.bytes_sent,
                "bytes_recv": io.bytes_recv,
                "packets_sent": io.packets_sent,
                "packets_recv": io.packets_recv,
                "errin": io.errin,
                "errout": io.errout,
                "dropin": io.dropin,
                "dropout": io.dropout,
            }
            logger.debug(f"Сетевые данные получены: sent={io.bytes_sent}, recv={io.bytes_recv}")
        except Exception as e:
            logger.warning(f"Ошибка получения сетевых данных: {e}")
            data["net"] = {}


        try:
            processes = []
            for proc in psutil.process_iter(['pid', 'name', 'cpu_percent', 'memory_percent', 'status']):
                try:
                    proc_info = proc.info
                    if proc_info['cpu_percent'] > 0 or proc_info['memory_percent'] > 1:
                        processes.append({
                            'pid': proc_info['pid'],
                            'name': proc_info['name'],
                            'cpu_percent': proc_info['cpu_percent'],
                            'memory_percent': proc_info['memory_percent'],
                            'status': proc_info['status']
                        })
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
            

            processes.sort(key=lambda x: x['cpu_percent'], reverse=True)
            data["top_processes"] = processes[:10]
        except Exception:
            data["top_processes"] = []


        try:
            if hasattr(psutil, 'getloadavg'):
                load_avg = psutil.getloadavg()
                data["load_avg"] = {
                    "1min": load_avg[0],
                    "5min": load_avg[1],
                    "15min": load_avg[2]
                }
        except Exception:
            data["load_avg"] = {}


        try:
            if hasattr(psutil, 'sensors_temperatures'):
                temps = psutil.sensors_temperatures()
                if temps:
                    data["temperatures"] = {}
                    for name, entries in temps.items():
                        if entries:
                            data["temperatures"][name] = {
                                'current': entries[0].current,
                                'high': entries[0].high,
                                'critical': entries[0].critical
                            }
        except Exception:
            data["temperatures"] = {}


        try:
            boot_time = psutil.boot_time()
            data["boot_time"] = boot_time
        except Exception:
            data["boot_time"] = None

    except Exception as e:
        data["ok"] = False
        data["error"] = str(e)
    return data


def _parse_free_m(text: str) -> Dict[str, Any]:




    out: Dict[str, Any] = {}
    try:
        lines = [l for l in (text or '').splitlines() if l.strip()]
        for ln in lines:
            if ln.lower().startswith("mem:"):
                parts = [p for p in ln.split() if p]

                if len(parts) >= 7:
                    total = int(parts[1])
                    used = int(parts[2])
                    free = int(parts[3])
                    avail = int(parts[6])
                    out = {
                        "total_mb": total,
                        "used_mb": used,
                        "free_mb": free,
                        "available_mb": avail,
                        "percent": _safe_percent(used, total),
                    }
                    break
    except Exception:
        pass
    return out


def _parse_loadavg(text: str) -> List[float] | None:
    try:
        parts = (text or '').split()
        return [float(parts[0]), float(parts[1]), float(parts[2])]
    except Exception:
        return None


def _parse_df_h(text: str) -> List[Dict[str, Any]]:
    disks: List[Dict[str, Any]] = []
    try:
        lines = [l for l in (text or '').splitlines() if l.strip()]

        if lines and ("Filesystem" in lines[0] or "Source" in lines[0] or "Size" in lines[0]):
            lines = lines[1:]
        for ln in lines:
            parts = [p for p in ln.split() if p]
            if len(parts) >= 6:
                src, size, used, avail, pcent, target = parts[:6]
                try:
                    p_raw = int((pcent or '').strip('%'))
                except Exception:
                    p_raw = None
                disks.append({
                    "device": src,
                    "mountpoint": target,
                    "size": size,
                    "used": used,
                    "avail": avail,
                    "percent": p_raw,
                })
    except Exception:
        pass
    return disks


def _compute_cpu_percent(loadavg: List[float] | None, cpu_count: int | None) -> float | None:
    try:
        if not loadavg or cpu_count is None or cpu_count <= 0:
            return None
        value = (float(loadavg[0]) / float(cpu_count)) * 100.0

        if value < 0:
            return 0.0
        return round(value, 2)
    except Exception:
        return None


def get_remote_metrics_for_host(host_name: str) -> Dict[str, Any]:
    """Собрать базовые метрики по SSH для хоста из xui_hosts.
    Требует настроенный SSH у хоста в БД (`ssh_host`, `ssh_user`, и т.п.).
    """
    host = rw_repo.get_host(host_name)
    if not host:
        return {"ok": False, "error": "host not found"}

    try:
        ssh = speedtest_runner._ssh_connect(host)
    except Exception as e:
        return {"ok": False, "error": f"SSH connect failed: {e}"}

    try:
        metrics: Dict[str, Any] = {"ok": True}
        cpu_count = None

        try:
            rc, out, err = speedtest_runner._ssh_exec(ssh, "uname -srmo 2>/dev/null || uname -a")
            metrics["uname"] = (out or err or '').strip()
        except Exception:
            metrics["uname"] = None

        try:
            rc, out, err = speedtest_runner._ssh_exec(ssh, "cat /proc/uptime || uptime -p")
            txt = (out or err or '').strip()
            up_sec = None
            try:

                first = float(txt.split()[0])
                up_sec = int(first)
            except Exception:
                up_sec = None
            metrics["uptime_sec"] = up_sec
        except Exception:
            metrics["uptime_sec"] = None

        try:
            rc, out, err = speedtest_runner._ssh_exec(ssh, "cat /proc/loadavg")
            metrics["loadavg"] = _parse_loadavg(out)
        except Exception:
            metrics["loadavg"] = None

        try:
            rc, out, err = speedtest_runner._ssh_exec(
                ssh,
                "nproc 2>/dev/null || getconf _NPROCESSORS_ONLN 2>/dev/null || echo 1"
            )
            first_line = (out or err or '').strip().splitlines()[0]
            cpu_count = int(first_line)
        except Exception:
            cpu_count = None
        metrics["cpu_count"] = cpu_count
        metrics["cpu_percent"] = _compute_cpu_percent(metrics.get("loadavg"), cpu_count)

        try:
            rc, out, err = speedtest_runner._ssh_exec(ssh, "free -m")
            mem = _parse_free_m(out)
            metrics["memory"] = mem
            metrics["mem_percent"] = mem.get("percent") if mem else None
        except Exception:
            metrics["memory"] = {}
            metrics["mem_percent"] = None

        try:
            rc, out, err = speedtest_runner._ssh_exec(ssh, "df -h -x tmpfs -x devtmpfs --output=source,size,used,avail,pcent,target | tail -n +2")
            metrics["disks"] = _parse_df_h(out)
        except Exception:
            metrics["disks"] = []
        try:
            disk_percents = [d.get('percent') for d in metrics["disks"] or [] if d.get('percent') is not None]
            metrics["disk_percent"] = max(disk_percents) if disk_percents else None
        except Exception:
            metrics["disk_percent"] = None
        

        if metrics.get("memory"):
            mem = metrics["memory"]
            metrics["memory_percent"] = mem.get("percent")
            metrics["memory_used_mb"] = mem.get("used_mb")
            metrics["memory_total_mb"] = mem.get("total_mb")
        
        if metrics.get("disks") and len(metrics["disks"]) > 0:
            first_disk = metrics["disks"][0]
            metrics["disk_mountpoint"] = first_disk.get("mountpoint", "/")
        

        try:
            rc, out, err = speedtest_runner._ssh_exec(ssh, "cat /proc/net/dev | grep -E 'eth0|ens|enp|wlan0' | head -1")
            if not out or err:

                rc, out, err = speedtest_runner._ssh_exec(ssh, "cat /proc/net/dev | grep -v 'lo:' | grep -v 'docker' | grep -v 'veth' | tail -n +3 | head -1")
            if out and not err:

                parts = out.strip().split()
                if len(parts) >= 10:
                    metrics["network_recv"] = int(parts[1])
                    metrics["network_sent"] = int(parts[9])
                    metrics["network_packets_recv"] = int(parts[2])
                    metrics["network_packets_sent"] = int(parts[10])
                    logger.debug(f"Сетевые данные получены через SSH: sent={parts[9]}, recv={parts[1]}")
                else:
                    metrics["network_recv"] = 0
                    metrics["network_sent"] = 0
                    metrics["network_packets_recv"] = 0
                    metrics["network_packets_sent"] = 0
            else:
                metrics["network_recv"] = 0
                metrics["network_sent"] = 0
                metrics["network_packets_recv"] = 0
                metrics["network_packets_sent"] = 0
        except Exception:
            metrics["network_recv"] = 0
            metrics["network_sent"] = 0
            metrics["network_packets_recv"] = 0
            metrics["network_packets_sent"] = 0
        
        return metrics
    finally:
        try:
            ssh.close()
        except Exception:
            pass


def get_remote_metrics_for_target(target_name: str) -> Dict[str, Any]:
    target = rw_repo.get_ssh_target(target_name)
    if not target:
        return {"ok": False, "error": "target not found"}
    host_row = speedtest_runner._target_to_host_row(target)
    try:
        ssh = speedtest_runner._ssh_connect(host_row)
    except Exception as e:
        return {"ok": False, "error": f"SSH connect failed: {e}"}

    try:
        metrics: Dict[str, Any] = {"ok": True}
        try:
            rc, out, err = speedtest_runner._ssh_exec(ssh, "uname -srmo 2>/dev/null || uname -a")
            metrics["uname"] = (out or err or '').strip()
        except Exception:
            metrics["uname"] = None
        try:
            rc, out, err = speedtest_runner._ssh_exec(ssh, "cat /proc/uptime || uptime -p")
            txt = (out or err or '').strip()
            up_sec = None
            try:
                first = float(txt.split()[0])
                up_sec = int(first)
            except Exception:
                up_sec = None
            metrics["uptime_sec"] = up_sec
        except Exception:
            metrics["uptime_sec"] = None
        try:
            rc, out, err = speedtest_runner._ssh_exec(ssh, "cat /proc/loadavg")
            metrics["loadavg"] = _parse_loadavg(out)
        except Exception:
            metrics["loadavg"] = None

        try:
            rc, out, err = speedtest_runner._ssh_exec(
                ssh,
                "nproc 2>/dev/null || getconf _NPROCESSORS_ONLN 2>/dev/null || echo 1"
            )
            first_line = (out or err or '').strip().splitlines()[0]
            cpu_count = int(first_line)
        except Exception:
            cpu_count = None
        metrics["cpu_count"] = cpu_count
        metrics["cpu_percent"] = _compute_cpu_percent(metrics.get("loadavg"), cpu_count)
        try:
            rc, out, err = speedtest_runner._ssh_exec(ssh, "free -m")
            mem = _parse_free_m(out)
            metrics["memory"] = mem
            metrics["mem_percent"] = mem.get("percent") if mem else None
        except Exception:
            metrics["memory"] = {}
            metrics["mem_percent"] = None
        try:
            rc, out, err = speedtest_runner._ssh_exec(ssh, "df -h -x tmpfs -x devtmpfs --output=source,size,used,avail,pcent,target | tail -n +2")
            metrics["disks"] = _parse_df_h(out)
        except Exception:
            metrics["disks"] = []
        try:
            disk_percents = [d.get('percent') for d in metrics["disks"] or [] if d.get('percent') is not None]
            metrics["disk_percent"] = max(disk_percents) if disk_percents else None
        except Exception:
            metrics["disk_percent"] = None
        

        if metrics.get("memory"):
            mem = metrics["memory"]
            metrics["memory_percent"] = mem.get("percent")
            metrics["memory_used_mb"] = mem.get("used_mb")
            metrics["memory_total_mb"] = mem.get("total_mb")
        
        if metrics.get("disks") and len(metrics["disks"]) > 0:
            first_disk = metrics["disks"][0]
            metrics["disk_mountpoint"] = first_disk.get("mountpoint", "/")
        

        try:
            rc, out, err = speedtest_runner._ssh_exec(ssh, "cat /proc/net/dev | grep -E 'eth0|ens|enp|wlan0' | head -1")
            if not out or err:

                rc, out, err = speedtest_runner._ssh_exec(ssh, "cat /proc/net/dev | grep -v 'lo:' | grep -v 'docker' | grep -v 'veth' | tail -n +3 | head -1")
            if out and not err:

                parts = out.strip().split()
                if len(parts) >= 10:
                    metrics["network_recv"] = int(parts[1])
                    metrics["network_sent"] = int(parts[9])
                    metrics["network_packets_recv"] = int(parts[2])
                    metrics["network_packets_sent"] = int(parts[10])
                    logger.debug(f"Сетевые данные получены через SSH: sent={parts[9]}, recv={parts[1]}")
                else:
                    metrics["network_recv"] = 0
                    metrics["network_sent"] = 0
                    metrics["network_packets_recv"] = 0
                    metrics["network_packets_sent"] = 0
            else:
                metrics["network_recv"] = 0
                metrics["network_sent"] = 0
                metrics["network_packets_recv"] = 0
                metrics["network_packets_sent"] = 0
        except Exception:
            metrics["network_recv"] = 0
            metrics["network_sent"] = 0
            metrics["network_packets_recv"] = 0
            metrics["network_packets_sent"] = 0
        
        return metrics
    finally:
        try:
            ssh.close()
        except Exception:
            pass
