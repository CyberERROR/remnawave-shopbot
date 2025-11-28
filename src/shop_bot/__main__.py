import logging
import threading
import asyncio
import signal
import re
try:

    import colorama
    colorama_available = True
except Exception:
    colorama_available = False

from shop_bot.webhook_server.app import create_webhook_app
from shop_bot.data_manager.scheduler import periodic_subscription_check
from shop_bot.data_manager import remnawave_repository as rw_repo
from shop_bot.bot_controller import BotController

def main():
    if colorama_available:
        try:
            colorama.just_fix_windows_console()
        except Exception:
            pass

    class ColoredFormatter(logging.Formatter):
        COLORS = {
            'DEBUG': '\x1b[36m',
            'INFO': '\x1b[32m',
            'WARNING': '\x1b[33m',
            'ERROR': '\x1b[31m',
            'CRITICAL': '\x1b[41m',
        }
        RESET = '\x1b[0m'

        def format(self, record: logging.LogRecord) -> str:
            level = record.levelname
            color = self.COLORS.get(level, '')
            reset = self.RESET if color else ''

            fmt = f"%(asctime)s [%(levelname)s] %(message)s"

            datefmt = "%H:%M:%S"
            base = logging.Formatter(fmt=fmt, datefmt=datefmt)
            msg = base.format(record)
            if color:

                msg = msg.replace(f"[{level}]", f"{color}[{level}]{reset}")
            return msg

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    for h in list(root.handlers):
        root.removeHandler(h)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(ColoredFormatter())
    root.addHandler(ch)


    logging.getLogger('werkzeug').setLevel(logging.WARNING)

    aio_event_logger = logging.getLogger('aiogram.event')
    aio_event_logger.setLevel(logging.INFO)
    logging.getLogger('aiogram.dispatcher').setLevel(logging.WARNING)
    logging.getLogger('aiohttp').setLevel(logging.WARNING)
    logging.getLogger('paramiko').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)

    class RussianizeAiogramFilter(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:
            try:
                msg = record.getMessage()
                if 'Update id=' in msg:


                    m = re.search(r"Update id=(\d+)\s+is\s+(not handled|handled)\.\s+Duration\s+(\d+)\s+ms\s+by bot id=(\d+)", msg)
                    if m:
                        upd_id, state, dur_ms, bot_id = m.groups()
                        state_ru = 'не обработано' if state == 'not handled' else 'обработано'
                        msg = f"Обновление {upd_id} {state_ru} за {dur_ms} мс (бот {bot_id})"
                        record.msg = msg
                        record.args = ()
                    else:

                        msg = msg.replace('Update id=', 'Обновление ')
                        msg = msg.replace(' is handled.', ' обработано.')
                        msg = msg.replace(' is not handled.', ' не обработано.')
                        msg = msg.replace('Duration', 'за')
                        msg = msg.replace('by bot id=', '(бот ')
                        if msg.endswith(')') is False and 'бот ' in msg:
                            msg = msg + ')'
                        record.msg = msg
                        record.args = ()
            except Exception:
                pass
            return True


    aio_event_logger.addFilter(RussianizeAiogramFilter())
    logger = logging.getLogger(__name__)

    logger.info("Инициализация базы данных...")
    rw_repo.initialize_db()
    logger.info("Инициализация базы данных завершена.")

    bot_controller = BotController()
    flask_app = create_webhook_app(bot_controller)
    
    async def shutdown(sig: signal.Signals, loop: asyncio.AbstractEventLoop):
        logger.info(f"Получен сигнал: {sig.name}. Запускаю завершение работы...")
        if bot_controller.get_status()["is_running"]:
            bot_controller.stop()
            await asyncio.sleep(2)
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        if tasks:
            [task.cancel() for task in tasks]
            await asyncio.gather(*tasks, return_exceptions=True)
        loop.stop()

    async def start_services():
        loop = asyncio.get_running_loop()
        bot_controller.set_loop(loop)
        flask_app.config['EVENT_LOOP'] = loop
        
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda sig=sig: asyncio.create_task(shutdown(sig, loop)))
        
        flask_thread = threading.Thread(
            target=lambda: flask_app.run(host='0.0.0.0', port=1488, use_reloader=False, debug=False),
            daemon=True
        )
        flask_thread.start()
        
        logger.info("Flask-сервер запущен: http://0.0.0.0:1488")
            
        logger.info("Приложение запущено. Бота можно стартовать из веб-панели.")
        
        asyncio.create_task(periodic_subscription_check(bot_controller))


        try:
            while True:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:

            logger.info("Главная задача отменена, выполняю корректное завершение...")
            return

    try:
        asyncio.run(start_services())
    except asyncio.CancelledError:

        logger.info("Получен сигнал остановки, сервисы остановлены.")
    finally:
        logger.info("Приложение завершается.")

if __name__ == "__main__":
    main()
