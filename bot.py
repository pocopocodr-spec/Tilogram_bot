"""
bot.py — GOLDgr/USD Signal Bot
Команды: /start /subscribe /unsubscribe /signal /status
Фото: отправь скриншот MT5 — получи анализ от Claude Vision
"""
import asyncio, base64, logging, os, re
import aiohttp

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, PhotoSize

from signal_engine import GoldSignalEngine
from scheduler import SignalScheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[logging.FileHandler("bot.log", encoding="utf-8"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "8543608460:AAHYQxEG_zyYumkk-tCsGNzat6v861e-V60")
CLAUDE_KEY = os.getenv("ANTHROPIC_API_KEY", "")

bot       = Bot(token=BOT_TOKEN)
dp        = Dispatcher()
engine    = GoldSignalEngine()
scheduler = SignalScheduler(bot, engine)


# ── Анализ скриншота через Claude Vision ─────────────────────────────────────

async def analyze_chart_image(image_bytes: bytes) -> str:
    gemini_key = os.getenv("GEMINI_API_KEY", "")
    if not gemini_key:
        return "⚠️ GEMINI_API_KEY не задан в Variables на Railway."

    img_b64 = base64.standard_b64encode(image_bytes).decode()

    payload = {
        "contents": [{
            "parts": [
                {
                    "inline_data": {
                        "mime_type": "image/jpeg",
                        "data": img_b64
                    }
                },
                {
                    "text": (
                        "Ты профессиональный трейдер. Проанализируй этот график GOLDgr/USD.\n\n"
                        "Дай анализ строго в таком формате:\n\n"
                        "НАПРАВЛЕНИЕ: [ПОКУПКА/ПРОДАЖА/ОЖИДАНИЕ]\n\n"
                        "АНАЛИЗ:\n"
                        "- Тренд: [опиши]\n"
                        "- Ключевые уровни: [опиши]\n"
                        "- Индикаторы: [RSI, EMA если видны]\n"
                        "- Паттерн: [если есть]\n\n"
                        "ЗОНА ВХОДА: [цена]-[цена]\n"
                        "СТОП: [цена]\n"
                        "ТЕЙК 1: [цена]\n"
                        "ТЕЙК 2: [цена]\n"
                        "R:R: 1:[число]\n"
                        "ВЕРОЯТНОСТЬ: [число]%\n\n"
                        "КОММЕНТАРИЙ: [1-2 предложения]\n\n"
                        "Отвечай только на русском."
                    )
                }
            ]
        }]
    }

    try:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={gemini_key}"
        async with aiohttp.ClientSession() as s:
            async with s.post(
                url, json=payload,
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=30)
            ) as r:
                data = await r.json()
                text = (data.get("candidates", [{}])[0]
                            .get("content", {})
                            .get("parts", [{}])[0]
                            .get("text", ""))
                return f"📊 Анализ графика:\n\n{text}" if text else "⚠️ Не удалось получить анализ."
    except Exception as e:
        logger.error(f"Gemini Vision error: {e}")
        return "⚠️ Ошибка анализа. Попробуйте ещё раз."


# ── Команды ───────────────────────────────────────────────────────────────────

@dp.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer(
        "🥇 GOLDgr/USD — Signal Bot\n\n"
        "Анализирую золото за грамм в USD каждые 15 минут.\n"
        "Таймфреймы: M5 · M15 · H1 · D1\n\n"
        "Команды:\n"
        "/subscribe — подписаться на сигналы\n"
        "/unsubscribe — отписаться\n"
        "/signal — сигнал прямо сейчас\n"
        "/status — статус\n\n"
        "📸 Отправь скриншот графика MT5 — получишь анализ!\n\n"
        "⚠️ Не финансовый совет."
    )

@dp.message(Command("subscribe"))
async def cmd_subscribe(message: Message):
    if scheduler.subscribe(message.chat.id):
        await message.answer(
            "✅ Подписка активна!\n"
            "Сигналы каждые 15 минут.\n"
            "Хотите сигнал сейчас? /signal"
        )
    else:
        await message.answer("ℹ️ Уже подписаны. /signal — получить сигнал сейчас.")

@dp.message(Command("unsubscribe"))
async def cmd_unsubscribe(message: Message):
    if scheduler.unsubscribe(message.chat.id):
        await message.answer("❌ Подписка отменена. /subscribe — подписаться снова.")
    else:
        await message.answer("ℹ️ Вы не были подписаны.")

@dp.message(Command("signal"))
async def cmd_signal(message: Message):
    wait = await message.answer("⏳ Анализирую рынок...")
    try:
        signal = await engine.get_signal()
        await wait.edit_text(signal)
    except Exception as e:
        logger.error(f"get_signal error: {e}", exc_info=True)
        await wait.edit_text("⚠️ Не удалось получить данные. Попробуйте через минуту.")

@dp.message(Command("status"))
async def cmd_status(message: Message):
    sub = message.chat.id in scheduler.subscribers
    await message.answer(
        f"Подписка: {'✅ активна' if sub else '❌ не активна'}\n"
        f"Пара: GOLDgr/USD\n"
        f"Интервал: каждые 15 минут\n"
        f"Подписчиков: {len(scheduler.subscribers)}"
    )

# ── Обработка фото (анализ графика) ──────────────────────────────────────────

@dp.message(F.photo)
async def handle_photo(message: Message):
    wait = await message.answer("📊 Анализирую график...")
    try:
        # Берём фото максимального качества
        photo: PhotoSize = message.photo[-1]
        file = await bot.get_file(photo.file_id)
        file_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file.file_path}"

        async with aiohttp.ClientSession() as s:
            async with s.get(file_url) as r:
                image_bytes = await r.read()

        result = await analyze_chart_image(image_bytes)
        await wait.edit_text(result)
    except Exception as e:
        logger.error(f"Photo handler error: {e}", exc_info=True)
        await wait.edit_text("⚠️ Ошибка анализа графика. Попробуйте ещё раз.")


# ── Запуск ────────────────────────────────────────────────────────────────────

async def main():
    logger.info("▶ GOLDgr Signal Bot запущен")
    asyncio.create_task(scheduler.run())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
