"""
signal_engine.py — GOLDgr/USD v5
Цена: goldprice.org (спот) / резерв GC=F
Формат: профессиональный сигнал с зонами, Fibo, R:R
"""
import asyncio, logging, re
from datetime import datetime
from typing import Optional

import aiohttp
import pandas as pd
import numpy as np
import yfinance as yf

logger = logging.getLogger(__name__)
GRAM = 31.1034748

TIMEFRAMES = {
    "M5":  {"interval":"5m",  "period":"5d",  "min":60, "w":1.0},
    "M15": {"interval":"15m", "period":"30d", "min":60, "w":1.5},
    "H1":  {"interval":"1h",  "period":"60d", "min":60, "w":2.0},
    "D1":  {"interval":"1d",  "period":"2y",  "min":60, "w":3.0},
}

# ── Спот-цена ────────────────────────────────────────────────────────────────

async def get_spot_oz() -> Optional[float]:
    h = {"User-Agent":"Mozilla/5.0","Accept":"application/json"}
    # goldprice.org
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get("https://data-asg.goldprice.org/dbXRates/USD",
                             timeout=aiohttp.ClientTimeout(total=6), headers=h, ssl=False) as r:
                d = await r.json(content_type=None)
                p = d.get("items",[{}])[0].get("xauPrice")
                if p and float(p) > 1000:
                    return float(p)
    except: pass
    # metals.live
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get("https://metals.live/api/v1/spot",
                             timeout=aiohttp.ClientTimeout(total=6), headers=h, ssl=False) as r:
                for item in await r.json(content_type=None):
                    if str(item.get("metal","")).lower()=="gold":
                        p = float(item.get("price",0))
                        if p > 1000: return p
    except: pass
    # GC=F резерв
    try:
        df = yf.download("GC=F", period="1d", interval="1m", progress=False, auto_adjust=True)
        if not df.empty:
            p = float(df["Close"].iloc[-1]) if not isinstance(df.columns, pd.MultiIndex) else float(df["Close"].iloc[-1].iloc[0])
            if p > 1000: return p
    except: pass
    return None

# ── Фундаментал ──────────────────────────────────────────────────────────────

class FundaAnalyzer:
    H = {"User-Agent":"Mozilla/5.0"}
    BUL = ["surge","rally","rises","safe haven","rate cut","dovish","weak dollar",
           "geopolit","crisis","war","record","buying","inflows"]
    BEA = ["falls","drops","declines","plunges","rate hike","hawkish",
           "strong dollar","outflows","selling","profit taking"]

    async def _get(self, s, url):
        try:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=8), ssl=False) as r:
                return await r.text()
        except: return ""

    async def analyze(self):
        async with aiohttp.ClientSession(headers=self.H) as s:
            xml = await self._get(s, "https://www.kitco.com/rss/Latest-News.xml")
        titles = re.findall(r"<title><!\[CDATA\[(.*?)\]\]></title>", xml)
        gold_news = [t for t in titles if any(w in t.lower() for w in
                     ["gold","fed","dollar","inflation","rate","bullion"])]
        score = 0
        for h in gold_news[:15]:
            hl = h.lower()
            score += sum(1 for w in self.BUL if w in hl)
            score -= sum(1 for w in self.BEA if w in hl)
        if score >= 3:   sent = "бычий 📈"
        elif score <= -3: sent = "медвежий 📉"
        else:             sent = "нейтральный ➡️"
        return {"score": score, "sentiment": sent, "ok": bool(gold_news)}

# ── Технический анализ ───────────────────────────────────────────────────────

class GoldSignalEngine:
    GOLD = "GC=F"
    def __init__(self): self.funda = FundaAnalyzer()

    def _fetch(self, tf):
        cfg = TIMEFRAMES[tf]
        try:
            df = yf.download(self.GOLD, period=cfg["period"],
                             interval=cfg["interval"], progress=False, auto_adjust=True)
            if df.empty: return None
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df = df[["Open","High","Low","Close","Volume"]].dropna(subset=["Close"])
            return df if len(df) >= cfg["min"] else None
        except: return None

    @staticmethod
    def _rsi(s, n=7):
        d = s.diff()
        ag = d.clip(lower=0).ewm(alpha=1/n, min_periods=n, adjust=False).mean()
        al = (-d.clip(upper=0)).ewm(alpha=1/n, min_periods=n, adjust=False).mean()
        rs = ag / al.replace(0, np.nan)
        return float((100 - 100/(1+rs)).iloc[-1])

    @staticmethod
    def _ema(s, n): return float(s.ewm(span=n, adjust=False).mean().iloc[-1])

    @staticmethod
    def _atr(df, n=14):
        h,l,c = df["High"],df["Low"],df["Close"]
        tr = pd.concat([h-l,(h-c.shift()).abs(),(l-c.shift()).abs()],axis=1).max(axis=1)
        return float(tr.ewm(span=n, adjust=False).mean().iloc[-1])

    @staticmethod
    def _ad(df, n=10):
        rng = (df["High"]-df["Low"]).replace(0, np.nan)
        clv = ((df["Close"]-df["Low"])-(df["High"]-df["Close"]))/rng
        ad  = (clv*df["Volume"]).fillna(0).cumsum()
        return "▲" if float(ad.iloc[-1]) > float(ad.iloc[-n]) else "▼"

    def _tf_analyze(self, tf, df, spot_oz):
        c   = df["Close"].squeeze()
        fut = float(c.iloc[-1])
        k   = (spot_oz / fut) / GRAM  # масштаб фьючерс → спот грамм

        p    = fut * k
        e20  = self._ema(c,20)*k; e50=self._ema(c,50)*k; e200=self._ema(c,200)*k
        rsi  = self._rsi(c,7)
        ad   = self._ad(df)
        atr  = self._atr(df)*k

        sc = 0
        if   rsi<20: sc+=3
        elif rsi<35: sc+=2
        elif rsi<45: sc+=1
        elif rsi>80: sc-=3
        elif rsi>65: sc-=2
        elif rsi>55: sc-=1
        sc += 1 if p>e20  else -1
        sc += 1 if e20>e50 else -1
        sc += 2 if e50>e200 else -2
        sc += 1 if p>e200 else -1
        sc += 1 if ad=="▲" else -1

        return {"sc":sc, "rsi":round(rsi,1), "ad":ad, "atr":round(atr,5),
                "p":round(p,3), "e20":round(e20,3), "e50":round(e50,3),
                "e200":round(e200,3),
                "bias":"🟢" if sc>=3 else "🔴" if sc<=-3 else "⚪"}

    def _build(self, tfs, spot_gram, fund):
        w_sum = sum(TIMEFRAMES[k]["w"]*9 for k,v in tfs.items() if v)
        w_sc  = sum(v["sc"]*TIMEFRAMES[k]["w"] for k,v in tfs.items() if v)
        norm  = (w_sc/w_sum*9) if w_sum else 0

        dirs = []
        for k,v in tfs.items():
            if v:
                if v["sc"]>=3: dirs.append("buy")
                elif v["sc"]<=-3: dirs.append("sell")

        dom = max(set(dirs),key=dirs.count) if dirs else "wait"
        agr = round(dirs.count(dom)/len(dirs)*100) if dirs else 0

        # Добавляем фундаментал
        total = norm + max(-2, min(2, fund["score"]))

        if total >= 2.0 and dom=="buy":   direction="BUY"
        elif total <= -2.0 and dom=="sell": direction="SELL"
        else: direction="WAIT"

        if direction=="WAIT": strength="нейтральный"
        elif abs(total)>=6 and agr>=75: strength="очень сильный"
        elif agr>=50: strength="умеренный"
        else: strength="слабый"

        atr = next((v["atr"] for v in tfs.values() if v), 0.5)
        p   = spot_gram

        if direction=="BUY":
            sl=round(p-atr*1.5,3); tp1=round(p+atr*1.5,3); tp2=round(p+atr*2.5,3); tp3=round(p+atr*4.0,3)
        elif direction=="SELL":
            sl=round(p+atr*1.5,3); tp1=round(p-atr*1.5,3); tp2=round(p-atr*2.5,3); tp3=round(p-atr*4.0,3)
        else:
            sl=tp1=tp2=tp3=None

        rr = round(abs(tp2-p)/abs(sl-p),1) if sl and tp2 else 0

        return {"dir":direction, "strength":strength, "norm":round(total,1),
                "agr":agr, "sl":sl, "tp1":tp1, "tp2":tp2, "tp3":tp3, "rr":rr}

    @staticmethod
    def _fmt(price, sig, tfs, fund):
        now = datetime.utcnow().strftime("%d.%m.%Y %H:%M UTC")
        d   = sig["dir"]

        ICON = {"BUY":"🟢","SELL":"🔴","WAIT":"⚪"}
        DIR  = {"BUY":"ПОКУПКА — ЛОНГ","SELL":"ПРОДАЖА — ШОРТ","WAIT":"ОЖИДАНИЕ"}

        ref = tfs.get("M15") or tfs.get("H1")
        rsi = ref["rsi"] if ref else "—"
        ad  = ref["ad"]  if ref else "—"

        tf_row = " | ".join(
            f"{k}:{(v['bias'] if v else '❓')}"
            for k,v in tfs.items()
        )

        lines = [
            f"{ICON[d]} GOLDgr/USD — {DIR[d]}",
            f"🕐 {now}",
            f"💵 Цена: ${price:.3f}/г",
            "",
            f"📊 Таймфреймы: {tf_row}",
            f"📉 RSI(7): {rsi}  |  A/D: {ad}  |  TF согл.: {sig['agr']}%",
        ]

        if d != "WAIT":
            prob = min(95, max(45, 50 + sig["agr"]//5 + (10 if abs(sig["norm"])>4 else 0)))
            zone_lo = round(price - 0.05, 3)
            zone_hi = round(price + 0.05, 3)
            lines += [
                "",
                "─" * 22,
                f"🎯 Зона входа: ${zone_lo} — ${zone_hi}",
                f"❌ Стоп:  ${sig['sl']}",
                f"✅ Тейк 1: ${sig['tp1']}",
                f"✅ Тейк 2: ${sig['tp2']}",
                f"✅ Тейк 3: ${sig['tp3']}",
                f"⚖️  R:R: 1:{sig['rr']}",
                f"🎲 Вероятность: {prob}%",
            ]

        ficon = "📈" if "бычий" in fund["sentiment"] else "📉" if "медвежий" in fund["sentiment"] else "➡️"
        lines += [
            "",
            "─" * 22,
            f"📰 Фон: {ficon} {fund['sentiment']}",
            f"⚡ Сила: {sig['strength']}  Σ{sig['norm']:+.1f}",
            "⚠️ Не финансовый совет.",
        ]
        return "\n".join(lines)

    async def get_signal(self):
        loop = asyncio.get_event_loop()
        tf_task   = loop.run_in_executor(None, lambda: {k:self._fetch(k) for k in TIMEFRAMES})
        spot_task = get_spot_oz()
        fund_task = self.funda.analyze()

        tf_dfs, spot_oz, fund = await asyncio.gather(tf_task, spot_task, fund_task)

        if not spot_oz:
            for df in tf_dfs.values():
                if df is not None:
                    c = df["Close"].squeeze()
                    spot_oz = float(c.iloc[-1])
                    break
        if not spot_oz:
            return "⚠️ GOLDgr/USD — нет данных. Попробуйте позже."

        spot_gram = spot_oz / GRAM

        tfs = {}
        for k, df in tf_dfs.items():
            if df is not None:
                try: tfs[k] = self._tf_analyze(k, df, spot_oz)
                except Exception as e:
                    logger.error(f"[{k}] {e}")
                    tfs[k] = None
            else: tfs[k] = None

        if all(v is None for v in tfs.values()):
            return "⚠️ GOLDgr/USD — нет данных по таймфреймам."

        sig = self._build(tfs, spot_gram, fund)
        return self._fmt(spot_gram, sig, tfs, fund)
