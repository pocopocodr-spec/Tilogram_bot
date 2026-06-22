"""
bot.py — XAU/GR Gold Signal Bot
Telegram-бот: сигналы по золоту в EUR (XAU/GR) каждые 15 минут.
Технический: EMA20, EMA50, EMA200, RSI(7), A/D, ATR
Фундаментальный: парсинг Kitco / MarketWatch / Investing.com
"""
import asyncio
import logging
import os

from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message

from signal_engine import GoldSignalEngine
from scheduler import SignalScheduler

# ── Логирование ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.FileHandler("bot.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# ── Инициализация ─────────────────────────────────────────────────────────────
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "8543608460:AAHYQxEG_zyYumkk-tCsGNzat6v861e-V60")

bot       = Bot(token=BOT_TOKEN)
dp        = Dispatcher()
engine    = GoldSignalEngine()
scheduler = SignalScheduler(bot, engine)


# ════════════════════════════════════════════════════════════════════════════════
#  КОМАНДЫ
# ════════════════════════════════════════════════════════════════════════════════

@dp.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer(
        "🥇 <b>Gold Signal Bot — XAU/GR</b>\n\n"
        "Анализирую золото в евро (XAU/GR) каждые <b>15 минут</b>.\n"
        "Мультитаймфреймовый анализ: <b>M5 · M15 · H1 · D1</b>\n"
        "Технический + фундаментальный анализ.\n\n"
        "<b>📋 Команды:</b>\n"
        "/subscribe    — подписаться на сигналы\n"
        "/unsubscribe  — отписаться\n"
        "/signal       — сигнал прямо сейчас\n"
        "/status       — статус подписки\n"
        "/help         — как работает бот\n\n"
        "⚠️ <i>Бот носит информационный характер.\n"
        "Торговля сопряжена с риском потери капитала.</i>",
        parse_mode="HTML",
    )


@dp.message(Command("subscribe"))
async def cmd_subscribe(message: Message):
    if scheduler.subscribe(message.chat.id):
        await message.answer(
            "✅ <b>Подписка активна!</b>\n\n"
            "Сигналы по <b>XAU/GR</b> будут приходить каждые <b>15 минут</b>.\n"
            "Цены в <b>USD</b>. Первый сигнал — в ближайшую четверть часа.\n\n"
            "Хотите сигнал прямо сейчас? → /signal",
            parse_mode="HTML",
        )
    else:
        await message.answer(
            "ℹ️ Вы уже подписаны.\n"
            "Хотите сигнал прямо сейчас? → /signal"
        )


@dp.message(Command("unsubscribe"))
async def cmd_unsubscribe(message: Message):
    if scheduler.unsubscribe(message.chat.id):
        await message.answer(
            "❌ <b>Подписка отменена.</b>\n"
            "Чтобы подписаться снова — /subscribe",
            parse_mode="HTML",
        )
    else:
        await message.answer("ℹ️ Вы не были подписаны. /subscribe — чтобы начать.")


@dp.message(Command("signal"))
async def cmd_signal(message: Message):
    wait_msg = await message.answer(
        "⏳ <b>Анализирую XAU/GR...</b>\n"
        "<i>Загружаю котировки и новости (~5 сек)</i>",
        parse_mode="HTML",
    )
    try:
        signal = await engine.get_signal()
        await wait_msg.edit_text(signal, parse_mode="HTML")
    except Exception as e:
        logger.error(f"Ошибка get_signal: {e}", exc_info=True)
        await wait_msg.edit_text(
            "⚠️ Не удалось получить данные. Попробуйте через минуту."
        )


@dp.message(Command("status"))
async def cmd_status(message: Message):
    cid        = message.chat.id
    subscribed = cid in scheduler.subscribers
    count      = len(scheduler.subscribers)
    icon       = "✅" if subscribed else "❌"
    status_txt = "активна" if subscribed else "не активна"

    await message.answer(
        f"📊 <b>Статус</b>\n\n"
        f"  Подписка:           {icon} {status_txt}\n"
        f"  Пара:               XAU/GR (Золото / USD)\n"
        f"  Интервал:           каждые 15 минут\n"
        f"  Всего подписчиков:  {count}\n\n"
        + ("👉 /unsubscribe — отписаться" if subscribed else "👉 /subscribe — подписаться"),
        parse_mode="HTML",
    )


@dp.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer(
        "📖 <b>Gold Signal Bot — XAU/GR (MTF)</b>\n\n"

        "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "🔢 <b>Таймфреймы (MTF-анализ)</b>\n\n"

        "📅 <b>D1</b> — дневной.  <i>Главный тренд</i> (вес ×3)\n"
        "  Определяет глобальное направление рынка.\n\n"

        "📊 <b>H1</b> — часовой.  <i>Среднесрочный тренд</i> (вес ×2)\n"
        "  Уточняет направление внутри дня/недели.\n\n"

        "🕐 <b>M15</b> — 15 минут.  <i>Точка входа</i> (вес ×1.5)\n"
        "  Основной рабочий таймфрейм для входа.\n\n"

        "⚡ <b>M5</b> — 5 минут.  <i>Триггер</i> (вес ×1)\n"
        "  Подтверждает сигнал, точный момент входа.\n\n"

        "Чем больше таймфреймов согласны → тем выше <b>% согласованности</b>.\n\n"

        "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "📐 <b>Индикаторы (на каждом TF)</b>\n\n"

        "<b>EMA 20</b> — краткосрочный импульс.\n"
        "<b>EMA 50</b> — среднесрочный тренд.\n"
        "<b>EMA 200</b> — долгосрочный тренд.\n"
        "  Golden Cross ✨ = EMA50 > EMA200 (бычий)\n"
        "  Death Cross  ☠️ = EMA50 < EMA200 (медвежий)\n\n"

        "<b>RSI(7)</b> — быстрый осциллятор.\n"
        "  < 35 → перепроданность (потенциал роста)\n"
        "  > 65 → перекупленность (потенциал снижения)\n\n"

        "<b>A/D</b> — Accumulation/Distribution.\n"
        "  ▲ Рост = накопление (покупатели)\n"
        "  ▼ Падение = распределение (продавцы)\n\n"

        "<b>ATR(14)</b> → расчёт TP и SL.\n\n"

        "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "📰 <b>Фундаментальный анализ</b>\n\n"

        "Live-парсинг заголовков:\n"
        "  · Kitco News\n"
        "  · MarketWatch\n"
        "  · Investing.com\n\n"

        "Бычий 📈: rally, rate cut, safe haven, кризис...\n"
        "Медвежий 📉: rate hike, strong dollar, risk-on...\n\n"

        "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "⚡ <b>Итоговый сигнал</b>\n\n"

        "Взвешенный score (MTF) + фундаментал = итог\n\n"
        "  🟢 BUY  — score ≥ +2.5 и TF согласны\n"
        "  🔴 SELL — score ≤ −2.5 и TF согласны\n"
        "  ⚪ WAIT — TF расходятся или score в нейтрали\n\n"

        "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "⚠️ <i>Бот не является финансовым советником.\n"
        "Торгуйте осторожно. Риски на вас.</i>",
        parse_mode="HTML",
    )


# ════════════════════════════════════════════════════════════════════════════════
#  ЗАПУСК
# ════════════════════════════════════════════════════════════════════════════════

async def main():
    logger.info("▶ Gold Signal Bot (XAU/GR) запущен")
    asyncio.create_task(scheduler.run())
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
