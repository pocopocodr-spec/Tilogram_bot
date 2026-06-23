"""
signal_engine.py — GOLDgr/USD Signal Engine v4
════════════════════════════════════════════════
Цена: GC=F фьючерс + динамическая коррекция через реальный спот
Таймфреймы: M5 · M15 · H1 · D1
Индикаторы: EMA20, EMA50, EMA200, RSI(7) Wilder, A/D, ATR
Порог сигнала: MTF score ±3.0
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
    "M5":  {"interval": "5m",  "period": "5d",  "min_bars": 60,  "weight": 1.0},
    "M15": {"interval": "15m", "period": "30d", "min_bars": 60,  "weight": 1.5},
    "H1":  {"interval": "1h",  "period": "60d", "min_bars": 60,  "weight": 2.0},
    "D1":  {"interval": "1d",  "period": "2y",  "min_bars": 60,  "weight": 3.0},
}


# ══════════════════════════════════════════════════════════════════════════════
#  РЕАЛЬНАЯ СПОТ-ЦЕНА — несколько источников
# ══════════════════════════════════════════════════════════════════════════════

async def fetch_spot_oz() -> Optional[float]:
    """Получаем спот XAU/USD за унцию."""
    headers = {"User-Agent": "Mozilla/5.0"}

    sources = [
        # 1. metals-api через rapidapi публичный endpoint
        ("https://metals-api.com/api/latest?access_key=goldapi&base=USD&symbols=XAU", "XAU"),
        # 2. Goldprice.org API
        ("https://data-asg.goldprice.org/dbXRates/USD", "items"),
    ]

    # Источник 1: goldprice.org
    try:
        async with aiohttp.ClientSession(headers=headers) as s:
            async with s.get(
                "https://data-asg.goldprice.org/dbXRates/USD",
                timeout=aiohttp.ClientTimeout(total=6),
                ssl=False
            ) as r:
                data = await r.json(content_type=None)
                items = data.get("items", [])
                if items:
                    xau_usd = items[0].get("xauPrice")
                    if xau_usd and xau_usd > 1000:
                        logger.info(f"Спот (goldprice.org): ${xau_usd}/oz")
                        return float(xau_usd)
    except Exception as e:
        logger.warning(f"goldprice.org failed: {e}")

    # Источник 2: metals.live
    try:
        async with aiohttp.ClientSession(headers=headers) as s:
            async with s.get(
                "https://metals.live/api/v1/spot",
                timeout=aiohttp.ClientTimeout(total=6),
                ssl=False
            ) as r:
                data = await r.json(content_type=None)
                for item in (data if isinstance(data, list) else []):
                    if str(item.get("metal", "")).lower() == "gold":
                        p = float(item.get("price", 0))
                        if p > 1000:
                            logger.info(f"Спот (metals.live): ${p}/oz")
                            return p
    except Exception as e:
        logger.warning(f"metals.live failed: {e}")

    # Источник 3: Kitco парсинг
    try:
        async with aiohttp.ClientSession(headers=headers) as s:
            async with s.get(
                "https://www.kitco.com/gold-price-today-usa/",
                timeout=aiohttp.ClientTimeout(total=8),
                ssl=False
            ) as r:
                html = await r.text()
                m = re.search(r'"ask"\s*:\s*"?([\d.]+)"?', html)
                if m:
                    p = float(m.group(1))
                    if p > 1000:
                        logger.info(f"Спот (kitco): ${p}/oz")
                        return p
    except Exception as e:
        logger.warning(f"kitco failed: {e}")

    logger.warning("Все источники спот-цены недоступны")
    return None


# ══════════════════════════════════════════════════════════════════════════════
#  ФУНДАМЕНТАЛЬНЫЙ АНАЛИЗ
# ══════════════════════════════════════════════════════════════════════════════

class FundamentalAnalyzer:
    HEADERS = {"User-Agent": "Mozilla/5.0"}

    BULLISH = ["surge", "rally", "rises", "gains", "safe haven",
               "rate cut", "dovish", "weak dollar", "geopolit",
               "crisis", "war", "conflict", "record high", "buying"]
    BEARISH = ["falls", "drops", "declines", "plunges", "rate hike",
               "hawkish", "strong dollar", "risk-on", "outflows",
               "selling pressure", "profit taking", "bearish"]

    async def _get(self, session, url):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=8), ssl=False) as r:
                return await r.text()
        except:
            return ""

    async def _kitco_rss(self, s):
        xml = await self._get(s, "https://www.kitco.com/rss/Latest-News.xml")
        titles = re.findall(r"<title><!\[CDATA\[(.*?)\]\]></title>", xml)
        return [t for t in titles if any(w in t.lower() for w in ["gold", "fed", "dollar", "rate"])]

    async def _reuters_rss(self, s):
        xml = await self._get(s, "https://feeds.reuters.com/reuters/businessNews")
        titles = re.findall(r"<title>(.*?)</title>", xml)
        return [t for t in titles if any(w in t.lower() for w in ["gold", "fed", "dollar", "inflation", "rate"])]

    def _score(self, headlines):
        score, top = 0, []
        for h in headlines[:20]:
            hl = h.lower()
            b = sum(1 for w in self.BULLISH if w in hl)
            m = sum(1 for w in self.BEARISH if w in hl)
            score += b - m
            if (b or m) and len(top) < 2:
                top.append(re.sub(r'<[^>]+>', '', h).strip()[:80])
        return score, top

    async def analyze(self):
        async with aiohttp.ClientSession(headers=self.HEADERS) as s:
            r1, r2 = await asyncio.gather(
                self._kitco_rss(s), self._reuters_rss(s),
                return_exceptions=True
            )
        all_h = []
        for r in [r1, r2]:
            if isinstance(r, list):
                all_h.extend(r)

        if not all_h:
            return {"score": 0, "sentiment": "нейтральный ➡️", "ok": False}

        score, top = self._score(all_h)
        if score >= 3:
            sent = "бычий 📈"
        elif score <= -3:
            sent = "медвежий 📉"
        else:
            sent = "нейтральный ➡️"
        return {"score": score, "sentiment": sent, "headlines": top, "ok": True}


# ══════════════════════════════════════════════════════════════════════════════
#  ТЕХНИЧЕСКИЙ АНАЛИЗ
# ══════════════════════════════════════════════════════════════════════════════

class GoldSignalEngine:
    GOLD = "GC=F"

    def __init__(self):
        self.fundamental = FundamentalAnalyzer()

    def _fetch_tf(self, tf_key):
        cfg = TIMEFRAMES[tf_key]
        try:
            df = yf.download(self.GOLD, period=cfg["period"],
                             interval=cfg["interval"], progress=False, auto_adjust=True)
            if df.empty:
                return None
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df = df[["Open", "High", "Low", "Close", "Volume"]].dropna(subset=["Close"])
            return df if len(df) >= cfg["min_bars"] else None
        except Exception as e:
            logger.error(f"[{tf_key}] {e}")
            return None

    def _fetch_all(self):
        return {tf: self._fetch_tf(tf) for tf in TIMEFRAMES}

    @staticmethod
    def _rsi_wilder(s: pd.Series, n=7) -> float:
        """RSI по методу Уайлдера — идентично MT5."""
        delta = s.diff()
        gain  = delta.clip(lower=0)
        loss  = (-delta.clip(upper=0))
        # Wilder EMA = alpha 1/n
        ag = gain.ewm(alpha=1/n, min_periods=n, adjust=False).mean()
        al = loss.ewm(alpha=1/n, min_periods=n, adjust=False).mean()
        rs = ag / al.replace(0, np.nan)
        return float((100 - 100/(1+rs)).iloc[-1])

    @staticmethod
    def _ema(s, n):
        return float(s.ewm(span=n, adjust=False).mean().iloc[-1])

    @staticmethod
    def _ad_trend(df, n=10):
        rng = (df["High"] - df["Low"]).replace(0, np.nan)
        clv = ((df["Close"] - df["Low"]) - (df["High"] - df["Close"])) / rng
        ad  = (clv * df["Volume"]).fillna(0).cumsum()
        return "▲" if float(ad.iloc[-1]) > float(ad.iloc[-n]) else "▼"

    @staticmethod
    def _atr(df, n=14):
        h, l, c = df["High"], df["Low"], df["Close"]
        tr = pd.concat([h-l, (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1)
        return float(tr.ewm(span=n, adjust=False).mean().iloc[-1])

    def _analyze_tf(self, tf_key, df, spot_oz):
        close = df["Close"].squeeze()
        fut   = float(close.iloc[-1])
        # Динамическая коррекция: масштабируем все цены на реальный спот
        k = (spot_oz / fut) / TROY_OZ_TO_GRAM

        p    = fut * k
        e20  = self._ema(close, 20)  * k
        e50  = self._ema(close, 50)  * k
        e200 = self._ema(close, 200) * k
        rsi  = self._rsi_wilder(close, 7)
        ad   = self._ad_trend(df)
        atr  = self._atr(df) * k

        score = 0

        # RSI
        if   rsi < 20: score += 3
        elif rsi < 35: score += 2
        elif rsi < 45: score += 1
        elif rsi > 80: score -= 3
        elif rsi > 65: score -= 2
        elif rsi > 55: score -= 1

        # EMA структура
        score += 1  if p   > e20  else -1
        score += 1  if e20 > e50  else -1
        score += 2  if e50 > e200 else -2
        score += 1  if p   > e200 else -1

        # A/D
        score += 1 if ad == "▲" else -1

        bias = "🟢" if score >= 3 else "🔴" if score <= -3 else "⚪"

        return {
            "score": score, "bias": bias, "price": p,
            "rsi": round(rsi, 1), "ad": ad, "atr": round(atr, 4),
            "e20": round(e20, 3), "e50": round(e50, 3), "e200": round(e200, 3),
        }

    def _build_signal(self, tf_results, spot_gram, fund):
        weighted, total_w = 0.0, 0.0
        dirs = []

        for tf, res in tf_results.items():
            if not res:
                continue
            w = TIMEFRAMES[tf]["weight"]
            weighted += res["score"] * w
            total_w  += w * 9
            if res["score"] >= 3:   dirs.append("buy")
            elif res["score"] <= -3: dirs.append("sell")

        norm = (weighted / total_w * 9) if total_w else 0

        dominant  = max(set(dirs), key=dirs.count) if dirs else "wait"
        agreement = round(dirs.count(dominant) / len(dirs) * 100) if dirs else 0

        # Порог 2.0 — даёт больше сигналов
        if norm >= 2.0 and dominant == "buy":
            direction = "BUY"
        elif norm <= -2.0 and dominant == "sell":
            direction = "SELL"
        else:
            direction = "WAIT"

        # Сила
        if direction == "WAIT":
            strength = "нейтральный"
        elif abs(norm) >= 6 and agreement >= 75:
            strength = "очень сильный"
        elif agreement >= 50:
            strength = "умеренный"
        else:
            strength = "слабый"

        # TP/SL
        atr = next((r["atr"] for r in tf_results.values() if r), 0)
        p   = spot_gram
        if direction == "BUY":
            sl, tp1, tp2 = p - atr*1.5, p + atr*1.5, p + atr*2.5
        elif direction == "SELL":
            sl, tp1, tp2 = p + atr*1.5, p - atr*1.5, p - atr*2.5
        else:
            sl = tp1 = tp2 = None

        return {
            "direction": direction, "strength": strength,
            "norm": round(norm, 1), "agreement": agreement,
            "sl":  round(sl,  3) if sl  else None,
            "tp1": round(tp1, 3) if tp1 else None,
            "tp2": round(tp2, 3) if tp2 else None,
        }

    @staticmethod
    def _fmt(price, sig, tf_results, fund):
        now  = datetime.utcnow().strftime("%d.%m.%Y %H:%M UTC")
        d    = sig["direction"]
        ICON = {"BUY": "🟢", "SELL": "🔴", "WAIT": "⚪"}
        DRTX = {"BUY": "ПОКУПКА", "SELL": "ПРОДАЖА", "WAIT": "ОЖИДАНИЕ"}

        tf_row = " ".join(
            f"{k}:{(r['bias'] if r else '❓')}"
            for k, r in tf_results.items()
        )
        ref = tf_results.get("M15") or tf_results.get("H1")
        rsi_val = ref["rsi"] if ref else "—"
        ad_val  = ref["ad"]  if ref else "—"

        lines = [
            f"{ICON[d]} GOLDgr/USD — {DRTX[d]}",
            f"🕐 {now}",
            f"💵 ${price:,.3f}/г",
            "",
            f"📊 {tf_row}",
            f"RSI: {rsi_val}  |  A/D: {ad_val}  |  TF: {sig['agreement']}%",
        ]

        if d != "WAIT":
            lines += [
                "",
                "─────────────────",
                f"🎯 Вход:  ${price:,.3f}",
                f"✅ TP1:   ${sig['tp1']:,.3f}",
                f"✅ TP2:   ${sig['tp2']:,.3f}",
                f"❌ SL:    ${sig['sl']:,.3f}",
            ]

        fund_icon = "📈" if "бычий" in fund["sentiment"] else "📉" if "медвежий" in fund["sentiment"] else "➡️"
        lines += [
            "",
            "─────────────────",
            f"📰 Фон: {fund_icon} {fund['sentiment']}",
            f"⚡ {sig['strength']}  Σ{sig['norm']:+.1f}",
            "⚠️ Не финансовый совет.",
        ]
        return "\n".join(lines)

    async def get_signal(self):
        loop = asyncio.get_event_loop()
        tf_task   = loop.run_in_executor(None, self._fetch_all)
        spot_task = fetch_spot_oz()
        fund_task = self.fundamental.analyze()

        tf_dfs, spot_oz, fund = await asyncio.gather(tf_task, spot_task, fund_task)

        # Резерв если спот не получен
        if not spot_oz:
            for df in tf_dfs.values():
                if df is not None and not df.empty:
                    spot_oz = float(df["Close"].squeeze().iloc[-1])
                    logger.warning(f"Спот недоступен, резерв фьючерс: ${spot_oz}/oz")
                    break

        if not spot_oz:
            return "⚠️ GOLDgr/USD — нет данных. Попробуйте позже."

        spot_gram = spot_oz / TROY_OZ_TO_GRAM

        tf_results = {}
        for tf, df in tf_dfs.items():
            if df is not None:
                try:
                    tf_results[tf] = self._analyze_tf(tf, df, spot_oz)
                except Exception as e:
                    logger.error(f"[{tf}] анализ: {e}")
                    tf_results[tf] = None
            else:
                tf_results[tf] = None

        if all(v is None for v in tf_results.values()):
            return "⚠️ GOLDgr/USD — нет данных по таймфреймам."

        sig = self._build_signal(tf_results, spot_gram, fund)
        return self._fmt(spot_gram, sig, tf_results, fund)
