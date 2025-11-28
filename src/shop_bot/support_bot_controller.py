import asyncio
import logging

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from shop_bot.data_manager import remnawave_repository as rw_repo
from shop_bot.data_manager.remnawave_repository import get_admin_ids
from shop_bot.support_bot.handlers import get_support_router

logger = logging.getLogger(__name__)

class SupportBotController:
    def __init__(self):
        self._dp: Dispatcher | None = None
        self._bot: Bot | None = None
        self._task = None
        self._is_running = False
        self._loop: asyncio.AbstractEventLoop | None = None

    def set_loop(self, loop: asyncio.AbstractEventLoop):
        self._loop = loop
        logger.info("Цикл событий установлен.")

    def get_bot_instance(self) -> Bot | None:
        return self._bot

    async def _start_polling(self):
        self._is_running = True
        logger.info("Запущен опрос Telegram (Support-бот)...")
        try:
            await self._dp.start_polling(self._bot)
        except asyncio.CancelledError:
            logger.info("Опрос остановлен (задача отменена).")
        except Exception as e:
            logger.error(f"Ошибка во время опроса: {e}", exc_info=True)
        finally:
            logger.info("Опрос корректно остановлен.")
            self._is_running = False
            self._task = None
            if self._bot:
                await self._bot.close()
            self._bot = None
            self._dp = None

    def start(self):
        if self._is_running:
            return {"status": "error", "message": "Support-бот уже запущен."}

        if not self._loop or not self._loop.is_running():
            return {"status": "error", "message": "Критическая ошибка: цикл событий не установлен."}

        token = rw_repo.get_setting("support_bot_token")
        bot_username = rw_repo.get_setting("support_bot_username")

        admin_id = rw_repo.get_setting("admin_telegram_id")
        admin_ids = get_admin_ids()

        if not all([token, bot_username]) or (not admin_id and not admin_ids):
            return {
                "status": "error",
                "message": "Невозможно запустить support-бот: заполните support_bot_token, support_bot_username и хотя бы одного администратора (admin_telegram_id или admin_telegram_ids)."
            }

        try:
            self._bot = Bot(token=token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
            self._dp = Dispatcher()
            
            router = get_support_router()
            self._dp.include_router(router)
            
            try:
                asyncio.run_coroutine_threadsafe(self._bot.delete_webhook(drop_pending_updates=True), self._loop)
            except Exception as e:
                logger.warning(f"Не удалось удалить вебхук перед запуском опроса: {e}")

            self._task = asyncio.run_coroutine_threadsafe(self._start_polling(), self._loop)
            logger.info("Команда на запуск передана в цикл событий.")
            return {"status": "success", "message": "Команда на запуск support-бота отправлена."}
        except Exception as e:
            logger.error(f"Ошибка запуска support-бота: {e}", exc_info=True)
            self._bot = None
            self._dp = None
            return {"status": "error", "message": f"Ошибка при запуске support-бота: {e}"}

    def stop(self):
        if not self._is_running:
            return {"status": "error", "message": "Support-бот не запущен."}

        if not self._loop or not self._dp:
            return {"status": "error", "message": "Критическая ошибка: компоненты бота недоступны."}

        logger.info("Отправляю сигнал на корректную остановку...")
        asyncio.run_coroutine_threadsafe(self._dp.stop_polling(), self._loop)
        return {"status": "success", "message": "Команда на остановку support-бота отправлена."}

    def get_status(self):
        return {"is_running": self._is_running}
