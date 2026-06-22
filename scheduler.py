import asyncio
import logging
from datetime import datetime
from typing import Set

from aiogram import Bot
from aiogram.exceptions import TelegramForbiddenError, TelegramBadRequest

logger = logging.getLogger(__name__)

INTERVAL_MINUTES = 15


class SignalScheduler:
    """
    Раз в 15 минут рассылает сигнал всем подписчикам.
    Подписчики хранятся в памяти (для persistence используй БД или файл).
    """

    def __init__(self, bot: Bot, engine):
        self.bot = bot
        self.engine = engine
        self.subscribers: Set[int] = set()
        self._dead: Set[int] = set()

    # ── Управление подписками ────────────────────────────────────────────────

    def subscribe(self, chat_id: int) -> bool:
        if chat_id in self.subscribers:
            return False
        self.subscribers.add(chat_id)
        self._dead.discard(chat_id)
        logger.info(f"Подписан: {chat_id} (всего: {len(self.subscribers)})")
        return True

    def unsubscribe(self, chat_id: int) -> bool:
        if chat_id not in self.subscribers:
            return False
        self.subscribers.discard(chat_id)
        logger.info(f"Отписан: {chat_id} (всего: {len(self.subscribers)})")
        return True

    # ── Рассылка ─────────────────────────────────────────────────────────────

    async def _broadcast(self, text: str):
        if not self.subscribers:
            logger.info("Нет подписчиков — рассылка пропущена.")
            return

        dead = set()
        ok = 0
        for chat_id in list(self.subscribers):
            try:
                await self.bot.send_message(chat_id, text, parse_mode="HTML")
                ok += 1
            except TelegramForbiddenError:
                logger.warning(f"Бот заблокирован пользователем {chat_id}. Удаляем.")
                dead.add(chat_id)
            except TelegramBadRequest as e:
                logger.warning(f"Bad request для {chat_id}: {e}")
            except Exception as e:
                logger.error(f"Ошибка отправки {chat_id}: {e}")

        # Удаляем мёртвые чаты
        for cid in dead:
            self.subscribers.discard(cid)

        logger.info(f"Рассылка: {ok}/{len(self.subscribers) + len(dead)} успешно.")

    # ── Главный цикл ─────────────────────────────────────────────────────────

    async def run(self):
        """
        Запускает планировщик. Ждёт до ближайшей :00 / :15 / :30 / :45,
        затем рассылает каждые 15 минут.
        """
        logger.info("Планировщик запущен.")
        await self._wait_until_next_slot()

        while True:
            now = datetime.utcnow().strftime("%H:%M UTC")
            logger.info(f"[{now}] Генерирую сигнал...")

            try:
                signal_text = await self.engine.get_signal()
                await self._broadcast(signal_text)
            except Exception as e:
                logger.error(f"Критическая ошибка в планировщике: {e}")

            await asyncio.sleep(INTERVAL_MINUTES * 60)

    @staticmethod
    async def _wait_until_next_slot():
        """Ждём до ближайшей четверти часа (xx:00, xx:15, xx:30, xx:45)."""
        import time
        now = datetime.utcnow()
        current_minutes = now.minute
        # Следующий слот
        next_slot = (current_minutes // INTERVAL_MINUTES + 1) * INTERVAL_MINUTES
        wait_seconds = (next_slot - current_minutes) * 60 - now.second
        if wait_seconds < 0:
            wait_seconds += 3600

        logger.info(
            f"Первый сигнал через {wait_seconds // 60} мин {wait_seconds % 60} сек."
        )
        await asyncio.sleep(wait_seconds)
