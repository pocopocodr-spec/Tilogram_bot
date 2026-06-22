"""
signal_engine.py — XAU/GR Multi-Timeframe Signal Engine
═══════════════════════════════════════════════════════
Пара:      XAU/GR (Gold / USD)  →  GC=F (Gold Futures, цены в USD)
Таймфреймы: M5 · M15 · H1 · D1   (все анализируются одновременно)
Индикаторы: EMA 20, EMA 50, EMA 200, RSI(7), A/D, ATR(14)
Фунд.:      Kitco · MarketWatch · Investing.com (live парсинг)

Логика MTF (мультитаймфреймового) анализа:
  D1  → главный тренд           (вес ×3)
  H1  → среднесрочный тренд     (вес ×2)
  M15 → точка входа             (вес ×1.5)
  M5  → триггер/подтверждение   (вес ×1)
"""
import asyncio
import logging
from datetime import datetime
from typing import Optional

import aiohttp
import pandas as pd
import numpy as np
import yfinance as yf
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
#  КОНФИГУРАЦИЯ ТАЙМФРЕЙМОВ
# ══════════════════════════════════════════════════════════════════════════════

TIMEFRAMES = {
    "M5": {
        "label":    "M5  (5 мин)",
        "interval": "5m",
        "period":   "5d",       # Yahoo: макс 60д для 5м
        "min_bars": 210,        # нужно для EMA200
        "weight":   1.0,
        "emoji":    "⚡",
    },
    "M15": {
        "label":    "M15 (15 мин)",
        "interval": "15m",
        "period":   "30d",
        "min_bars": 210,
        "weight":   1.5,
        "emoji":    "🕐",
    },
    "H1": {
        "label":    "H1  (1 час)",
        "interval": "1h",
        "period":   "60d",
        "min_bars": 210,
        "weight":   2.0,
        "emoji":    "📊",
    },
    "D1": {
        "label":    "D1  (дневной)",
        "interval": "1d",
        "period":   "2y",
        "min_bars": 210,
        "weight":   3.0,
        "emoji":    "📅",
    },
}


# ══════════════════════════════════════════════════════════════════════════════
#  ФУНДАМЕНТАЛЬНЫЙ АНАЛИЗАТОР
# ══════════════════════════════════════════════════════════════════════════════

class FundamentalAnalyzer:
    """Парсит Kitco, MarketWatch, Investing.com и оценивает рыночный фон."""

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    }

    BULLISH = [
        "surge", "rally", "rise", "gain", "jump", "soar", "climb",
        "bullish", "record high", "safe haven", "demand", "inflow",
        "inflation", "rate cut", "fed cut", "dovish", "weak dollar",
        "geopolit", "war", "crisis", "uncertainty", "conflict",
    ]
    BEARISH = [
        "fall", "drop", "decline", "slump", "slide", "plunge", "bearish",
        "rate hike", "hawkish", "strong dollar", "dollar strength",
        "risk-on", "equities rise", "outflow", "pressure", "selloff",
    ]

    async def _get(self, session: aiohttp.ClientSession, url: str) -> str:
        try:
            async with session.get(
                url, timeout=aiohttp.ClientTimeout(total=9), ssl=False
            ) as r:
                return await r.text()
        except Exception as e:
            logger.warning(f"Fundamental fetch failed [{url}]: {e}")
            return ""

    async def _kitco(self, s) -> list:
        html = await self._get(s, "https://www.kitco.com/news/gold/")
        soup = BeautifulSoup(html, "html.parser")
        return [
            t.get_text(strip=True)
            for t in soup.select(
                "h3.article-item__title,h2.article__title,a.title,h3.headline"
            )
            if t.get_text(strip=True)
        ]

    async def _marketwatch(self, s) -> list:
        html = await self._get(s, "https://www.marketwatch.com/search?q=gold&tab=All+News")
        soup = BeautifulSoup(html, "html.parser")
        return [
            t.get_text(strip=True)
            for t in soup.select("a.link--title,h3.article__headline")
            if t.get_text(strip=True)
        ]

    async def _investing(self, s) -> list:
        html = await self._get(s, "https://www.investing.com/commodities/gold-news")
        soup = BeautifulSoup(html, "html.parser")
        return [
            t.get_text(strip=True)
            for t in soup.select("a.title,div.textDiv a,article h2 a")
            if t.get_text(strip=True)
        ]

    def _score(self, headlines: list) -> tuple:
        score, top = 0, []
        for h in headlines[:18]:
            hl = h.lower()
            b = sum(1 for w in self.BULLISH if w in hl)
            m = sum(1 for w in self.BEARISH if w in hl)
            score += b - m
            if (b or m) and len(top) < 4:
                top.append(h.strip()[:80])
        return score, top

    async def analyze(self) -> dict:
        async with aiohttp.ClientSession(headers=self.HEADERS) as session:
            results = await asyncio.gather(
                self._kitco(session),
                self._marketwatch(session),
                self._investing(session),
                return_exceptions=True,
            )
        all_h = [h for r in results if isinstance(r, list) for h in r]
        if not all_h:
            return {"score": 0, "headlines": [], "sentiment": "нейтральный ➡️", "ok": False}

        score, top = self._score(all_h)
        sentiment = ("бычий 📈" if score >= 2 else "медвежий 📉" if score <= -2
                     else "нейтральный ➡️")
        return {"score": score, "headlines": top, "sentiment": sentiment, "ok": True}


# ══════════════════════════════════════════════════════════════════════════════
#  ДВИЖОК СИГНАЛОВ — МУЛЬТИТАЙМФРЕЙМ
# ══════════════════════════════════════════════════════════════════════════════

class GoldSignalEngine:
    GOLD = "GC=F"   # Gold Futures. Корректируем к спот-цене (-0.3%)

    def __init__(self):
        self.fundamental = FundamentalAnalyzer()

    # ── Загрузка одного таймфрейма ───────────────────────────────────────────

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

            # Нормализуем MultiIndex (возникает при одном тикере в новых версиях yfinance)
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            df = df[["Open", "High", "Low", "Close", "Volume"]].copy()
            df.dropna(subset=["Close"], inplace=True)

            if len(df) < cfg["min_bars"]:
                logger.warning(f"[{tf_key}] Мало свечей: {len(df)} < {cfg['min_bars']}")
                return None

            return df

        except Exception as e:
            logger.error(f"[{tf_key}] Ошибка загрузки: {e}")
            return None

    def _fetch_all(self) -> dict:
        """Загружаем все 4 таймфрейма (синхронно, в executor)."""
        return {tf: self._fetch_tf(tf) for tf in TIMEFRAMES}

    # ── Индикаторы ───────────────────────────────────────────────────────────

    @staticmethod
    def _ema(s: pd.Series, n: int) -> float:
        return float(s.ewm(span=n, adjust=False).mean().iloc[-1])

    @staticmethod
    def _rsi(s: pd.Series, n: int = 7) -> float:
        d = s.diff()
        g = d.clip(lower=0).rolling(n).mean()
        lo = (-d.clip(upper=0)).rolling(n).mean()
        rs = g / lo.replace(0, np.nan)
        return float((100 - 100 / (1 + rs)).iloc[-1])

    @staticmethod
    def _ad_trend(df: pd.DataFrame, lookback: int = 5) -> str:
        rng = (df["High"] - df["Low"]).replace(0, np.nan)
        clv = ((df["Close"] - df["Low"]) - (df["High"] - df["Close"])) / rng
        ad  = (clv * df["Volume"]).fillna(0).cumsum()
        return "▲ накопление" if float(ad.iloc[-1]) > float(ad.iloc[-lookback - 1]) else "▼ распределение"

    @staticmethod
    def _atr(df: pd.DataFrame, n: int = 14) -> float:
        h, l, c = df["High"], df["Low"], df["Close"]
        tr = pd.concat([h - l, (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1).max(axis=1)
        return float(tr.rolling(n).mean().iloc[-1])

    # ── Анализ одного таймфрейма ─────────────────────────────────────────────

    def _analyze_tf(self, tf_key: str, df: pd.DataFrame) -> dict:
        close = df["Close"].squeeze()
        TROY_OZ_TO_GRAM = 31.1034748
        SPOT_CORRECTION = 0.997        # фьючерс ~0.3% выше спот
        p    = float(close.iloc[-1]) / TROY_OZ_TO_GRAM * SPOT_CORRECTION
        e20  = self._ema(close, 20) / TROY_OZ_TO_GRAM * SPOT_CORRECTION
        e50  = self._ema(close, 50) / TROY_OZ_TO_GRAM * SPOT_CORRECTION
        e200 = self._ema(close, 200) / TROY_OZ_TO_GRAM * SPOT_CORRECTION
        rsi  = self._rsi(close, 7)
        ad   = self._ad_trend(df)
        atr  = self._atr(df, 14) / TROY_OZ_TO_GRAM * SPOT_CORRECTION

        score   = 0
        signals = []

        # ── RSI(7) ──────────────────────────────────────────────────────────
        if rsi < 20:
            score += 3
            signals.append(f"RSI {rsi:.1f} — экстремальная перепроданность 💧💧")
        elif rsi < 35:
            score += 2
            signals.append(f"RSI {rsi:.1f} — перепроданность 💧")
        elif rsi < 45:
            score += 1
            signals.append(f"RSI {rsi:.1f} — слабость")
        elif rsi > 80:
            score -= 3
            signals.append(f"RSI {rsi:.1f} — экстремальная перекупленность 🔥🔥")
        elif rsi > 65:
            score -= 2
            signals.append(f"RSI {rsi:.1f} — перекупленность 🔥")
        elif rsi > 55:
            score -= 1
            signals.append(f"RSI {rsi:.1f} — сила (осторожно)")

        # ── EMA20 — краткосрочный ────────────────────────────────────────────
        if p > e20:
            score += 1
            signals.append(f"Цена > EMA20 (${e20:,.0f}) ▲ импульс вверх")
        else:
            score -= 1
            signals.append(f"Цена < EMA20 (${e20:,.0f}) ▼ импульс вниз")

        # ── EMA20 vs EMA50 — среднесрочный ──────────────────────────────────
        if e20 > e50:
            score += 1
            signals.append(f"EMA20 > EMA50 — среднесрочный рост")
        else:
            score -= 1
            signals.append(f"EMA20 < EMA50 — среднесрочное снижение")

        # ── EMA50 vs EMA200 — долгосрочный ──────────────────────────────────
        if e50 > e200:
            score += 2
            signals.append("EMA50 > EMA200 — Golden Cross ✨")
        else:
            score -= 2
            signals.append("EMA50 < EMA200 — Death Cross ☠️")

        # ── Цена vs EMA200 ───────────────────────────────────────────────────
        if p > e200:
            score += 1
            signals.append(f"Цена > EMA200 (${e200:,.0f}) — бычий рынок")
        else:
            score -= 1
            signals.append(f"Цена < EMA200 (${e200:,.0f}) — медвежий рынок")

        # ── A/D ─────────────────────────────────────────────────────────────
        if "▲" in ad:
            score += 1
            signals.append("A/D ▲ — накопление (покупки доминируют)")
        else:
            score -= 1
            signals.append("A/D ▼ — распределение (продажи доминируют)")

        # Направление таймфрейма
        if score >= 3:
            bias = "🟢 BUY"
        elif score <= -3:
            bias = "🔴 SELL"
        else:
            bias = "⚪ WAIT"

        return {
            "tf":      tf_key,
            "price":   p,
            "e20":     round(e20,  2),
            "e50":     round(e50,  2),
            "e200":    round(e200, 2),
            "rsi":     round(rsi,  1),
            "ad":      ad,
            "atr":     round(atr,  2),
            "score":   score,
            "bias":    bias,
            "signals": signals,
        }

    # ── Сборка MTF-сигнала ───────────────────────────────────────────────────

    @staticmethod
    def _mtf_score(tf_results: dict) -> tuple:
        """
        Взвешенная сумма score всех таймфреймов.
        Возвращает (weighted_score, agreement_pct, dominant_price).
        """
        total_w = 0.0
        weighted = 0.0
        prices   = []

        for tf_key, res in tf_results.items():
            if res is None:
                continue
            w = TIMEFRAMES[tf_key]["weight"]
            weighted += res["score"] * w
            total_w  += w * 9          # максимум score на таймфрейм ≈ 9
            prices.append(res["price"])

        norm = (weighted / total_w * 9) if total_w else 0   # нормализуем к шкале ≈ -9…+9

        # Согласованность: доля таймфреймов, указывающих в одну сторону
        directions = [
            ("buy" if (r and r["score"] >= 3) else
             "sell" if (r and r["score"] <= -3) else "wait")
            for r in tf_results.values()
        ]
        valid = [d for d in directions if d != "wait"]
        if valid:
            dominant = max(set(valid), key=valid.count)
            agreement = round(valid.count(dominant) / len(valid) * 100)
        else:
            dominant  = "wait"
            agreement = 0

        price = prices[-1] if prices else 0   # цена последнего загруженного TF (M15/M5)
        return norm, agreement, dominant, price

    @staticmethod
    def _build_signal(tf_results: dict, norm_score: float,
                      agreement: int, dominant: str,
                      price: float, fund: dict) -> dict:

        fs    = max(-3, min(3, fund["score"]))
        total = norm_score + fs

        # Порог сигнала
        if total >= 2.5 and dominant == "buy":
            direction = "BUY"
        elif total <= -2.5 and dominant == "sell":
            direction = "SELL"
        else:
            direction = "WAIT"

        # Сила: учитываем согласованность TF
        if direction != "WAIT":
            if agreement >= 75 and abs(total) >= 5:
                strength = "💪 очень сильный"
            elif agreement >= 50:
                strength = "👍 умеренный"
            else:
                strength = "⚠️ слабый (TF расходятся)"
        else:
            strength = "🤝 нейтральный"

        # TP/SL — берём ATR из M15 (или первого доступного)
        atr = 0.0
        for tf_key in ("M15", "H1", "M5", "D1"):
            if tf_results.get(tf_key):
                atr = tf_results[tf_key]["atr"]
                break

        sl_d, tp_d = atr * 1.5, atr * 2.5

        if direction == "BUY":
            sl, tp1, tp2 = price - sl_d, price + tp_d * 0.6, price + tp_d
        elif direction == "SELL":
            sl, tp1, tp2 = price + sl_d, price - tp_d * 0.6, price - tp_d
        else:
            sl = tp1 = tp2 = None

        return {
            "price":      price,
            "direction":  direction,
            "strength":   strength,
            "agreement":  agreement,
            "norm_score": round(norm_score, 2),
            "fund_score": fs,
            "total":      round(total, 2),
            "fund_sent":  fund["sentiment"],
            "fund_heads": fund["headlines"],
            "fund_ok":    fund["ok"],
            "sl":   round(sl,  2) if sl  else None,
            "tp1":  round(tp1, 2) if tp1 else None,
            "tp2":  round(tp2, 2) if tp2 else None,
        }

    # ── Форматирование ───────────────────────────────────────────────────────

    @staticmethod
    def _fmt(sig: dict, tf_results: dict) -> str:
        now  = datetime.utcnow().strftime("%d.%m.%Y %H:%M UTC")
        p    = sig["price"]
        drct = sig["direction"]

        ICON = {"BUY": "🟢", "SELL": "🔴", "WAIT": "⚪"}
        DRTX = {"BUY": "ПОКУПКА", "SELL": "ПРОДАЖА", "WAIT": "ОЖИДАНИЕ"}

        # Компактная строка таймфреймов
        tf_row = ""
        for tf_key in ("M5", "M15", "H1", "D1"):
            res = tf_results.get(tf_key)
            if res is None:
                tf_row += f"{tf_key}:❓ "
            elif res["score"] >= 3:
                tf_row += f"{tf_key}:🟢 "
            elif res["score"] <= -3:
                tf_row += f"{tf_key}:🔴 "
            else:
                tf_row += f"{tf_key}:⚪ "

        # RSI и A/D из M15 (основной TF)
        m15 = tf_results.get("M15") or tf_results.get("H1")
        rsi_str = f"{m15['rsi']}" if m15 else "—"
        ad_str  = "▲" if m15 and "накопление" in m15["ad"] else "▼"

        lines = [
            f"{ICON[drct]} XAUg/USD — {DRTX[drct]}",
            f"🕐 {now}",
            f"💵 ${p:,.3f}/г",
            "",
            f"📊 {tf_row.strip()}",
            f"RSI: {rsi_str}  |  A/D: {ad_str}  |  TF: {sig['agreement']}%",
        ]

        if drct != "WAIT":
            lines += [
                "",
                "─────────────────────",
                f"🎯 Вход:  ${p:,.3f}",
                f"✅ TP1:   ${sig['tp1']:,.3f}",
                f"✅ TP2:   ${sig['tp2']:,.3f}",
                f"❌ SL:    ${sig['sl']:,.3f}",
            ]

        # Фундаментал — только фон
        fund_icon = "📈" if "бычий" in sig['fund_sent'] else "📉" if "медвежий" in sig['fund_sent'] else "➡️"
        lines += [
            "",
            "─────────────────────",
            f"📰 Фон: {fund_icon} {sig['fund_sent']}",
            f"⚡ Сила: {sig['strength']}  Σ{sig['total']:+.1f}",
            "⚠️ Не финансовый совет.",
        ]

        return "\n".join(lines)

    # ── Публичный метод ──────────────────────────────────────────────────────

    async def get_signal(self) -> str:
        loop = asyncio.get_event_loop()

        # Параллельно: все 4 TF + фундаментал
        tf_future   = loop.run_in_executor(None, self._fetch_all)
        fund_future = self.fundamental.analyze()

        tf_dfs, fund = await asyncio.gather(tf_future, fund_future)

        # Анализируем каждый таймфрейм
        tf_results = {}
        for tf_key, df in tf_dfs.items():
            if df is not None:
                try:
                    tf_results[tf_key] = self._analyze_tf(tf_key, df)
                except Exception as e:
                    logger.error(f"[{tf_key}] Ошибка анализа: {e}")
                    tf_results[tf_key] = None
            else:
                tf_results[tf_key] = None

        if all(v is None for v in tf_results.values()):
            return (
                "⚠️ <b>XAU/GR</b> — не удалось загрузить данные ни по одному таймфрейму.\n"
                "Проверьте соединение и попробуйте позже."
            )

        # MTF-агрегация
        norm_score, agreement, dominant, price = self._mtf_score(tf_results)

        # Если цена не получена (все None кроме D1 и т.д.) — берём первую доступную
        if price == 0:
            for tf_key in ("M5", "M15", "H1", "D1"):
                if tf_results.get(tf_key):
                    price = tf_results[tf_key]["price"]
                    break

        sig = self._build_signal(tf_results, norm_score, agreement, dominant, price, fund)
        return self._fmt(sig, tf_results)
