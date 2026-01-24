import threading
import time
import json
import logging
import asyncio
from datetime import datetime, timedelta
from shop_bot.data_manager import remnawave_repository as rw_repo

logger = logging.getLogger(__name__)

class ServerScheduler:
    def __init__(self, ssh_executor, log_func):
        self.ssh_executor = ssh_executor
        self.log_func = log_func
        self.running = False
        self.thread = None
        self._lock = threading.Lock()

    def start(self):
        with self._lock:
            if self.running:
                return
            self.running = True
            self.thread = threading.Thread(target=self._run_loop, daemon=True)
            self.thread.start()
            logger.info("ServerScheduler started")

    def stop(self):
        with self._lock:
            self.running = False
            if self.thread:
                self.thread.join(timeout=5)
                self.thread = None
            logger.info("ServerScheduler stopped")

    def _run_loop(self):
        while self.running:
            try:
                self._check_targets()
            except Exception as e:
                logger.error(f"Error in ServerScheduler loop: {e}")
            
            for _ in range(60):
                if not self.running:
                    break
                time.sleep(1)

    def _check_targets(self):
        targets = rw_repo.get_all_ssh_targets()
        
        for target in targets:
            try:
                if not target.get('is_active'):
                    continue

                time_auto_str = target.get('time_auto')
                if not time_auto_str:
                    continue

                try:
                    config = json.loads(time_auto_str)
                except json.JSONDecodeError:
                    continue

                if not config or not config.get('enabled'):
                    continue

                interval_value = config.get('value')
                interval_unit = config.get('unit')
                last_run_timestamp = config.get('last_run')

                if not interval_value:
                    continue

                now = datetime.now()

                if not last_run_timestamp:
                    self._update_last_run(target['target_name'], config, now.timestamp())
                    continue

                last_run = datetime.fromtimestamp(last_run_timestamp)
                delta = now - last_run
                
                due = False
                if interval_unit == 'minutes':
                    if delta.total_seconds() >= interval_value * 60:
                        due = True
                elif interval_unit == 'hours':
                    if delta.total_seconds() >= interval_value * 3600:
                        due = True
                elif interval_unit == 'days':
                    if delta.days >= interval_value:
                        due = True
                
                if due:
                    self._restart_server(target, config)

            except Exception as e:
                logger.error(f"Error checking target {target.get('target_name')}: {e}")

    def _restart_server(self, target, config):
        target_name = target['target_name']
        ssh_host = target['ssh_host']
        ssh_port = target['ssh_port'] or 22
        ssh_user = target['ssh_user']
        ssh_password = target['ssh_password']
        
        result = self.ssh_executor(
            host=ssh_host,
            port=ssh_port,
            username=ssh_user,
            password=ssh_password,
            command="reboot"
        )
        
        now_ts = datetime.now().timestamp()
        
        if result['ok']:
            msg = f"✅ [Планировщик авто-перезапуска] Сервер {target_name} / {ssh_host} - Успешно перезапущен"
            self.log_func(msg)
            self._update_last_run(target_name, config, now_ts)
        else:
            error_msg = result.get('error', 'Unknown error')
            msg = f"🆘 [Планировщик авто-перезапуска] Сервер не удалось перезапустить - ({error_msg})"
            self.log_func(msg)
            self._update_last_run(target_name, config, now_ts)

    def _update_last_run(self, target_name, config, timestamp):
        config['last_run'] = timestamp
        new_time_auto = json.dumps(config)
        rw_repo.update_ssh_target_scheduler(target_name, new_time_auto)
