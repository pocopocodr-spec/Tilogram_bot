"""
signal_engine.py — GOLDgr/USD Signal Engine (полная переработка)
════════════════════════════════════════════════════════════════
Цена:      Реальная спот-цена через metals.live API (бесплатно, без ключа)
           Конвертация: USD/oz → USD/грамм (÷31.1034748)
Таймфреймы: M5 · M15 · H1 · D1
Индикаторы: EMA20, EMA50, EMA200, RSI(7), A/D, ATR(14)
Сигнал:    BUY/SELL при весовом MTF score ≥ ±3.5 (снижен порог)
Фунд.:     Kitco RSS + резервные источники
"""
import asyncio
import logging
import re
from datetime import datetime
from typing import Optional

import aiohttp
import pandas as pd
import numpy as np
import yfinance as yf
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

TROY_OZ_TO_GRAM = 31.1034748

TIMEFRAMES = {
    "M5":  {"interval": "5m",  "period": "5d",  "min_bars": 100, "weight": 1.0, "emoji": "⚡"},
    "M15": {"interval": "15m", "period": "30d", "min_bars": 100, "weight": 1.5, "emoji": "🕐"},
    "H1":  {"interval": "1h",  "period": "60d", "min_bars": 100, "weight": 2.0, "emoji": "📊"},
    "D1":  {"interval": "1d",  "period": "2y",  "min_bars": 100, "weight": 3.0, "emoji": "📅"},
}


# ══════════════════════════════════════════════════════════════════════════════
#  ПОЛУЧЕНИЕ РЕАЛЬНОЙ СПОТ-ЦЕНЫ
# ══════════════════════════════════════════════════════════════════════════════

async def fetch_spot_price() -> Optional[float]:
    """
    Получаем реальную спот-цену золота за унцию в USD.
    Источники (пробуем по очереди):
    1. metals.live — бесплатный API без ключа
    2. goldapi.io — публичный endpoint
    3. GC=F последняя цена как резерв
    """
    headers = {"User-Agent": "Mozilla/5.0"}

    # Источник 1: metals.live
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                "https://metals.live/api/v1/spot",
                timeout=aiohttp.ClientTimeout(total=5),
                headers=headers,
                ssl=False
            ) as r:
                data = await r.json()
                for item in data:
                    if item.get("metal") == "gold":
                        price = float(item.get("price", 0))
                        if price > 1000:
                            logger.info(f"Спот цена (metals.live): ${price}/oz")
                            return price
    except Exception as e:
        logger.warning(f"metals.live failed: {e}")

    # Источник 2: frankfurter + gold через альтернативный endpoint
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                "https://api.metals.dev/v1/latest?api_key=goldapi&base=USD&currencies=XAU",
                timeout=aiohttp.ClientTimeout(total=5),
                headers=headers,
                ssl=False
            ) as r:
                data = await r.json()
                xau = data.get("currencies", {}).get("XAU")
                if xau and xau > 0:
                    price = round(1.0 / xau, 4)
                    if price > 1000:
                        logger.info(f"Спот цена (metals.dev): ${price}/oz")
                        return price
    except Exception as e:
        logger.warning(f"metals.dev failed: {e}")

    # Источник 3: GC=F как резерв (фьючерс ≈ спот)
    try:
        import yfinance as yf
        ticker = yf.Ticker("GC=F")
        hist = ticker.history(period="1d", interval="1m")
        if not hist.empty:
            price = float(hist["Close"].iloc[-1])
            if price > 1000:
                logger.info(f"Спот цена (GC=F резерв): ${price}/oz")
                return price
    except Exception as e:
        logger.warning(f"GC=F резерв failed: {e}")

    return None


# ══════════════════════════════════════════════════════════════════════════════
#  ФУНДАМЕНТАЛЬНЫЙ АНАЛИЗАТОР (исправленный)
# ══════════════════════════════════════════════════════════════════════════════

class FundamentalAnalyzer:
    HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

    BULLISH = [
        "surge", "rally", "rises", "gains", "jumps", "soars",
        "safe haven", "rate cut", "fed cut", "dovish", "weak dollar",
        "geopolit", "crisis", "war", "conflict", "uncertainty",
        "inflows", "buying", "record",
    ]
    BEARISH = [
        "falls", "drops", "declines", "slumps", "slides", "plunges",
        "rate hike", "hawkish", "strong dollar", "risk-on",
        "outflows", "selling pressure", "profit taking",
    ]

    async def _get(self, session, url: str) -> str:
        try:
            async with session.get(
                url, timeout=aiohttp.ClientTimeout(total=8), ssl=False
            ) as r:
                return await r.text()
        except Exception as e:
            logger.warning(f"Fund fetch failed {url}: {e}")
            return ""

    async def _kitco_rss(self, s) -> list:
        """Kitco RSS — надёжнее чем HTML парсинг."""
        xml = await self._get(s, "https://www.kitco.com/rss/Latest-News.xml")
        if not xml:
            return []
        titles = re.findall(r"<title><!\[CDATA\[(.*?)\]\]></title>", xml)
        return [t for t in titles if "gold" in t.lower() or "silver" in t.lower()]

    async def _reuters_rss(self, s) -> list:
        xml = await self._get(s, "https://feeds.reuters.com/reuters/businessNews")
        if not xml:
            return []
        titles = re.findall(r"<title>(.*?)</title>", xml)
        return [t for t in titles if any(w in t.lower() for w in ["gold", "fed", "dollar", "inflation"])]

    def _score(self, headlines: list) -> tuple:
        score, top = 0, []
        for h in headlines[:20]:
            hl = h.lower()
            b = sum(1 for w in self.BULLISH if w in hl)
            m = sum(1 for w in self.BEARISH if w in hl)
            score += b - m
            if (b or m) and len(top) < 3:
                # Очищаем от HTML сущностей
                clean = re.sub(r'<[^>]+>', '', h).strip()[:80]
                top.append(clean)
        return score, top

    async def analyze(self) -> dict:
        async with aiohttp.ClientSession(headers=self.HEADERS) as session:
            results = await asyncio.gather(
                self._kitco_rss(session),
                self._reuters_rss(session),
                return_exceptions=True,
            )

        all_h = [h for r in results if isinstance(r, list) for h in r]
        if not all_h:
            return {"score": 0, "headlines": [], "sentiment": "нейтральный ➡️", "ok": False}

        score, top = self._score(all_h)

        # Более строгий порог для фона
        if score >= 3:
            sentiment = "бычий 📈"
        elif score <= -3:
            sentiment = "медвежий 📉"
        else:
            sentiment = "нейтральный ➡️"

        return {"score": score, "headlines": top, "sentiment": sentiment, "ok": True}


# ══════════════════════════════════════════════════════════════════════════════
#  ТЕХНИЧЕСКИЙ АНАЛИЗ
# ══════════════════════════════════════════════════════════════════════════════

class GoldSignalEngine:
    GOLD = "GC=F"

    def __init__(self):
        self.fundamental = FundamentalAnalyzer()

    def _fetch_tf(self, tf_key: str) -> Optional[pd.DataFrame]:
        cfg = TIMEFRAMES[tf_key]
        try:
            df = yf.download(
                self.GOLD,
                period=cfg["period"],
                interval=cfg["interval"],
                progress=False,
                auto_adjust=True,
            )
            if df.empty:
                return None
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df = df[["Open", "High", "Low", "Close", "Volume"]].copy()
            df.dropna(subset=["Close"], inplace=True)
            if len(df) < cfg["min_bars"]:
                logger.warning(f"[{tf_key}] Мало свечей: {len(df)}")
                return None
            return df
        except Exception as e:
            logger.error(f"[{tf_key}] Ошибка: {e}")
            return None

    def _fetch_all(self) -> dict:
        return {tf: self._fetch_tf(tf) for tf in TIMEFRAMES}

    # ── Индикаторы ───────────────────────────────────────────────────────────

    @staticmethod
    def _ema(s: pd.Series, n: int) -> float:
        return float(s.ewm(span=n, adjust=False).mean().iloc[-1])

    @staticmethod
    def _rsi(s: pd.Series, n: int = 7) -> float:
        """Wilder RSI — точно как в MT5."""
        delta = s.diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        # Wilder smoothing (как в MT5)
        avg_gain = gain.ewm(alpha=1/n, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1/n, adjust=False).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        return float((100 - 100 / (1 + rs)).iloc[-1])

    @staticmethod
    def _ad_trend(df: pd.DataFrame, lookback: int = 10) -> str:
        rng = (df["High"] - df["Low"]).replace(0, np.nan)
        clv = ((df["Close"] - df["Low"]) - (df["High"] - df["Close"])) / rng
        ad  = (clv * df["Volume"]).fillna(0).cumsum()
        # Сравниваем тренд A/D за последние 10 баров
        ad_now  = float(ad.iloc[-1])
        ad_prev = float(ad.iloc[-lookback])
        return "▲" if ad_now > ad_prev else "▼"

    @staticmethod
    def _atr(df: pd.DataFrame, n: int = 14) -> float:
        h, l, c = df["High"], df["Low"], df["Close"]
        tr = pd.concat([h - l, (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1).max(axis=1)
        return float(tr.ewm(span=n, adjust=False).mean().iloc[-1])

    # ── Анализ одного таймфрейма ─────────────────────────────────────────────

    def _analyze_tf(self, tf_key: str, df: pd.DataFrame, spot_oz: float) -> dict:
        close = df["Close"].squeeze()

        # Коэффициент для перевода фьючерса в спот-грамм
        futures_last = float(close.iloc[-1])
        correction   = spot_oz / futures_last if futures_last > 0 else 1.0
        to_gram      = correction / TROY_OZ_TO_GRAM

        p    = futures_last * to_gram
        e20  = self._ema(close, 20)  * to_gram
        e50  = self._ema(close, 50)  * to_gram
        e200 = self._ema(close, 200) * to_gram
        rsi  = self._rsi(close, 7)
        ad   = self._ad_trend(df)
        atr  = self._atr(df, 14) * to_gram

        score   = 0
        signals = []

        # ── RSI(7) ──────────────────────────────────────────────────────────
        if rsi < 20:
            score += 3; signals.append(f"RSI {rsi:.1f} — экстрем.перепроданность 💧💧")
        elif rsi < 35:
            score += 2; signals.append(f"RSI {rsi:.1f} — перепроданность 💧")
        elif rsi < 45:
            score += 1; signals.append(f"RSI {rsi:.1f} — слабость")
        elif rsi > 80:
            score -= 3; signals.append(f"RSI {rsi:.1f} — экстрем.перекупленность 🔥🔥")
        elif rsi > 65:
            score -= 2; signals.append(f"RSI {rsi:.1f} — перекупленность 🔥")
        elif rsi > 55:
            score -= 1; signals.append(f"RSI {rsi:.1f} — перекупленность")

        # ── EMA тренды ───────────────────────────────────────────────────────
        if p > e20:
            score += 1; signals.append("Цена выше EMA20 — бычий импульс")
        else:
            score -= 1; signals.append("Цена ниже EMA20 — медвежий импульс")

        if e20 > e50:
            score += 1
        else:
            score -= 1

        if e50 > e200:
            score += 2; signals.append("Golden Cross EMA50>EMA200")
        else:
            score -= 2; signals.append("Death Cross EMA50<EMA200")

        if p > e200:
            score += 1
        else:
            score -= 1

        # ── A/D ─────────────────────────────────────────────────────────────
        if ad == "▲":
            score += 1; signals.append("A/D растёт — накопление")
        else:
            score -= 1; signals.append("A/D падает — распределение")

        bias = "🟢 BUY" if score >= 3 else "🔴 SELL" if score <= -3 else "⚪ WAIT"

        return {
            "tf": tf_key, "price": p, "score": score, "bias": bias,
            "e20": round(e20, 3), "e50": round(e50, 3), "e200": round(e200, 3),
            "rsi": round(rsi, 1), "ad": ad, "atr": round(atr, 4),
            "signals": signals,
        }

    # ── MTF агрегация ────────────────────────────────────────────────────────

    @staticmethod
    def _mtf(tf_results: dict, spot_gram: float) -> dict:
        total_w, weighted = 0.0, 0.0
        directions = []

        for tf_key, res in tf_results.items():
            if not res:
                continue
            w = TIMEFRAMES[tf_key]["weight"]
            weighted += res["score"] * w
            total_w  += w * 9
            if res["score"] >= 3:
                directions.append("buy")
            elif res["score"] <= -3:
                directions.append("sell")

        norm = (weighted / total_w * 9) if total_w else 0

        valid = directions
        if valid:
            dominant = max(set(valid), key=valid.count)
            agreement = round(valid.count(dominant) / len(valid) * 100)
        else:
            dominant = "wait"
            agreement = 0

        # Порог снижен до 3.5 чтобы давать больше сигналов
        if norm >= 3.5 and dominant == "buy":
            direction = "BUY"
        elif norm <= -3.5 and dominant == "sell":
            direction = "SELL"
        else:
            direction = "WAIT"

        strength = "нейтральный"
        if direction != "WAIT":
            if agreement >= 75 and abs(norm) >= 6:
                strength = "очень сильный"
            elif agreement >= 50:
                strength = "умеренный"
            else:
                strength = "слабый"

        # TP/SL через ATR из M15
        atr = 0.0
        for k in ("M15", "H1", "M5", "D1"):
            if tf_results.get(k):
                atr = tf_results[k]["atr"]
                break

        p = spot_gram
        sl_d, tp_d = atr * 1.5, atr * 2.5

        if direction == "BUY":
            sl, tp1, tp2 = p - sl_d, p + tp_d * 0.6, p + tp_d
        elif direction == "SELL":
            sl, tp1, tp2 = p + sl_d, p - tp_d * 0.6, p - tp_d
        else:
            sl = tp1 = tp2 = None

        return {
            "direction": direction, "strength": strength,
            "agreement": agreement, "norm": round(norm, 2),
            "sl":  round(sl,  3) if sl  else None,
            "tp1": round(tp1, 3) if tp1 else None,
            "tp2": round(tp2, 3) if tp2 else None,
        }

    # ── Форматирование ───────────────────────────────────────────────────────

    @staticmethod
    def _fmt(price: float, mtf: dict, tf_results: dict, fund: dict) -> str:
        now  = datetime.utcnow().strftime("%d.%m.%Y %H:%M UTC")
        drct = mtf["direction"]

        ICON = {"BUY": "🟢", "SELL": "🔴", "WAIT": "⚪"}
        DRTX = {"BUY": "ПОКУПКА", "SELL": "ПРОДАЖА", "WAIT": "ОЖИДАНИЕ"}

        # Строка таймфреймов
        tf_row = " ".join(
            f"{k}:{'🟢' if (r and r['score'] >= 3) else '🔴' if (r and r['score'] <= -3) else '⚪'}"
            for k, r in tf_results.items()
        )

        # RSI из M15 или H1
        ref = tf_results.get("M15") or tf_results.get("H1")
        rsi_str = f"{ref['rsi']}" if ref else "—"
        ad_str  = ref["ad"] if ref else "—"

        lines = [
            f"{ICON[drct]} GOLDgr/USD — {DRTX[drct]}",
            f"🕐 {now}",
            f"💵 ${price:,.3f}/г",
            "",
            f"📊 {tf_row}",
            f"RSI: {rsi_str}  |  A/D: {ad_str}  |  TF: {mtf['agreement']}%",
        ]

        if drct != "WAIT":
            lines += [
                "",
                "─────────────────────",
                f"🎯 Вход:  ${price:,.3f}",
                f"✅ TP1:   ${mtf['tp1']:,.3f}",
                f"✅ TP2:   ${mtf['tp2']:,.3f}",
                f"❌ SL:    ${mtf['sl']:,.3f}",
            ]

        # Фундаментал
        fund_icon = {"бычий 📈": "📈", "медвежий 📉": "📉"}.get(fund["sentiment"], "➡️")
        lines += [
            "",
            "─────────────────────",
            f"📰 Фон: {fund_icon} {fund['sentiment']}",
            f"⚡ Сила: {mtf['strength']}  Σ{mtf['norm']:+.1f}",
            "⚠️ Не финансовый совет.",
        ]

        return "\n".join(lines)

    # ── Публичный метод ──────────────────────────────────────────────────────

    async def get_signal(self) -> str:
        loop = asyncio.get_event_loop()

        # Параллельно: все TF + спот цена + фундаментал
        tf_task   = loop.run_in_executor(None, self._fetch_all)
        spot_task = fetch_spot_price()
        fund_task = self.fundamental.analyze()

        tf_dfs, spot_oz, fund = await asyncio.gather(tf_task, spot_task, fund_task)

        # Если спот не получили — берём из фьючерса напрямую
        if not spot_oz:
            for df in tf_dfs.values():
                if df is not None:
                    spot_oz = float(df["Close"].squeeze().iloc[-1])
                    logger.warning(f"Спот не получен, используем фьючерс: ${spot_oz}/oz")
                    break

        if not spot_oz:
            return "⚠️ GOLDgr/USD — нет данных. Попробуйте позже."

        spot_gram = spot_oz / TROY_OZ_TO_GRAM

        # Анализируем каждый TF
        tf_results = {}
        for tf_key, df in tf_dfs.items():
            if df is not None:
                try:
                    tf_results[tf_key] = self._analyze_tf(tf_key, df, spot_oz)
                except Exception as e:
                    logger.error(f"[{tf_key}] Ошибка анализа: {e}")
                    tf_results[tf_key] = None
            else:
                tf_results[tf_key] = None

        if all(v is None for v in tf_results.values()):
            return "⚠️ GOLDgr/USD — не удалось загрузить данные."

        mtf = self._mtf(tf_results, spot_gram)
        return self._fmt(spot_gram, mtf, tf_results, fund)
