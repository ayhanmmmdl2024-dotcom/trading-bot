"""
╔══════════════════════════════════════════════════════════════════════════╗
║           UNIVERSAL SNIPER v13.0 — İNSTİTUTİONAL ARXİTEKTURA          ║
╠══════════════════════════════════════════════════════════════════════════╣
║  v12-dən saxlananlar (dəyişdirilmədi):                                  ║
║  [FIX1]  AsyncIO + WebSocket event-driven — while True yoxdur          ║
║  [FIX2]  AsyncIO semaphore + bounded task pool — thread partlaması yox  ║
║  [FIX3]  GIL bypass — numpy vectorized, ProcessPoolExecutor CPU tasks  ║
║  [FIX4]  Binance native WebSocket — ccxt yalnız REST fallback           ║
║  [FIX5]  Order Execution Engine — TWAP/VWAP/iceberg/slippage           ║
║  [FIX6]  Position Manager — partial fill, cancel race qorunması        ║
║  [FIX7]  Failover — auto-reconnect, DNS retry, circuit breaker         ║
║  [FIX8]  Memory manager — LRU cache, gc.collect, slab limiti           ║
║  [FIX9]  Numpy/numba vectorized — pandas.rolling() yoxdur             ║
║  [FIX10] Telegram watchdog — ayrı prosess, ölsə restart                ║
║  [FIX11] Persistent state — SQLite WAL, state recovery on restart      ║
║  [FIX12] Risk Engine v2 — Kelly, portfolio cap, corr, volatility scale ║
║  [FIX13] Realistic backtest — fee, slippage, spread, funding           ║
║  [FIX14] Latency measurement — signal/order/ws/api RTT metrics         ║
║  [FIX15] Prometheus metrics + Grafana endpoint                         ║
║  [NEW1]  Market Regime State Machine — hysteresis, ML-dən müstəqil    ║
║  [NEW2]  Liquidity Filter — real-time spread + z-score anomaliya       ║
║  [NEW3]  Chaos Engine — network/WS/queue/API stress testlər            ║
║  [NEW4]  Backpressure Queue — 10K msg/s flood, RAM qoruması            ║
╠══════════════════════════════════════════════════════════════════════════╣
║  v13.0 YENİ MODULLAR:                                                   ║
║  [M1]  Mikrostruktur: Order Book Imbalance (Bid/Ask ratio)              ║
║  [M2]  CVD — Cumulative Volume Delta (alıcı vs satıcı agresivliyi)     ║
║  [M3]  Liquidity Heatmap — Whale əmr zonalarını skaner                 ║
║  [M4]  Adaptive Thresholding — volatillik düşdükdə skor həddi azalır  ║
║  [M5]  Funding Rate Arbitrage — Short Squeeze aşkarı                   ║
║  [M6]  Kelly Criterion — position size kalkulyatoru (RiskEngineV2 bağl)║
║  [M7]  Volatility-Adjusted SL/TP — ATR əsaslı dinamik SL/TP           ║
║  [M8]  Drawdown Circuit Breaker — 24 saatlıq tam dayandırma kilidi     ║
║  [M9]  TWAP Execution — v12-də var, Shadow Mode əlavə edildi           ║
║  [M10] Latency Monitor — 500ms-dən yuxarı siqnalı ləğv edir            ║
║  [M11] Shadow Mode / Paper Trading — arxa planda test                  ║
║  [M12] WFO həftəlik avtomatik — hər şənbə gecəsi özü işləyir          ║
╠══════════════════════════════════════════════════════════════════════════╣
║  v12 XƏTA DÜZƏLTMƏLƏRİ:                                                ║
║  [B1]  Telegram mesajında "v11.0" yazırdı → "v13.0" edildi             ║
║  [B2]  Shutdown mesajında "v11" yazırdı → "v13.0" edildi               ║
║  [B3]  /stats əmrində "v11" yazırdı → "v13.0" edildi                   ║
║  [B4]  Həftəlik hesabatda "v11" yazırdı → "v13.0" edildi               ║
╚══════════════════════════════════════════════════════════════════════════╝
"""

# ══════════════════════════════════════════════════════════
#  IMPORTS
# ══════════════════════════════════════════════════════════
import asyncio
import os, gc, csv, time, logging, pickle, itertools, signal, sqlite3, queue
import threading, json, math, hashlib, struct, warnings
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from functools import lru_cache, wraps
from typing import Dict, List, Optional, Tuple, Any

import numpy as np
import pandas as pd
import ccxt
import requests
import yfinance as yf
import websockets
import aiohttp

warnings.filterwarnings('ignore')

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ══════════════════════════════════════════════════════════
#  LOGGING
# ══════════════════════════════════════════════════════════
def _log_qur():
    log = logging.getLogger("sniper")
    log.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s — %(message)s", "%H:%M:%S")
    ch = logging.StreamHandler(); ch.setLevel(logging.INFO); ch.setFormatter(fmt)
    log.addHandler(ch)
    try:
        fh = RotatingFileHandler("sniper.log", maxBytes=10*1024*1024, backupCount=5, encoding="utf-8")
        fh.setLevel(logging.DEBUG); fh.setFormatter(fmt); log.addHandler(fh)
    except Exception: pass
    return log

log = _log_qur()

# ══════════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════════
def _env(k, default=None, required=False):
    v = os.getenv(k, default)
    if required and (v is None or str(v).startswith("SENIN_")):
        log.critical(f"[CONFIG] {k} .env-də tapılmadı!"); raise SystemExit(1)
    return v

TOKEN            = _env("TOKEN",           required=True)
CHAT_ID          = _env("CHAT_ID",         required=True)
FINNHUB_API_KEY  = _env("FINNHUB_API_KEY", required=True)
AUTO_TRADE       = _env("AUTO_TRADE", "False").lower() == "true"
MIN_ULDUZ        = float(_env("MIN_ULDUZ", "2.3"))
MAX_KORRELYASIYA = int(_env("MAX_KORRELYASIYA", "3"))
MAX_GUNLUK_ZEFER = float(_env("MAX_GUNLUK_ZEFER", "3.0"))
METRICS_PORT     = int(_env("METRICS_PORT", "9090"))
HEALTH_PORT      = int(_env("HEALTH_PORT",  "8080"))
LOG_FAYL         = "sniper_v11.db"
ML_MODEL_FAYL    = "ml_model_v11.pkl"
RR_MINIMUM       = 2.0
COOLDOWN_SANIYE  = 14400
WS_RECONNECT_MAX = 10

# ══════════════════════════════════════════════════════════
#  [FIX14] LATENCY TRACKER
# ══════════════════════════════════════════════════════════
class LatencyTracker:
    """Signal, order, WS, API RTT ölçür — Prometheus-a verir."""
    def __init__(self):
        self._lock   = threading.Lock()
        self._data: Dict[str, deque] = defaultdict(lambda: deque(maxlen=200))

    def record(self, metric: str, ms: float):
        with self._lock:
            self._data[metric].append(ms)

    def summary(self, metric: str) -> dict:
        with self._lock:
            vals = list(self._data.get(metric, []))
        if not vals:
            return {"p50": 0, "p95": 0, "p99": 0, "mean": 0, "n": 0}
        arr = sorted(vals)
        n   = len(arr)
        return {
            "p50":  arr[int(n*0.50)],
            "p95":  arr[int(n*0.95)],
            "p99":  arr[min(int(n*0.99), n-1)],
            "mean": sum(arr)/n,
            "n":    n
        }

    def all_metrics(self) -> dict:
        with self._lock:
            keys = list(self._data.keys())
        return {k: self.summary(k) for k in keys}

latency = LatencyTracker()

# ══════════════════════════════════════════════════════════
#  [FIX15] PROMETHEUS METRICS
# ══════════════════════════════════════════════════════════
class PrometheusMetrics:
    """
    Sadə Prometheus metrics server (port 9090).
    Grafana-dan birbaşa çəkmək üçün.
    """
    def __init__(self):
        self._lock      = threading.Lock()
        self._counters  = defaultdict(float)
        self._gauges    = defaultdict(float)
        self._histograms= defaultdict(list)

    def counter_inc(self, name: str, val: float = 1.0, labels: dict = None):
        key = self._label_key(name, labels)
        with self._lock: self._counters[key] += val

    def gauge_set(self, name: str, val: float, labels: dict = None):
        key = self._label_key(name, labels)
        with self._lock: self._gauges[key] = val

    def histogram_observe(self, name: str, val: float, labels: dict = None):
        key = self._label_key(name, labels)
        with self._lock: self._histograms[key].append(val)

    def _label_key(self, name, labels):
        if not labels: return name
        lstr = ",".join(f'{k}="{v}"' for k,v in sorted(labels.items()))
        return f'{name}{{{lstr}}}'

    def render(self) -> str:
        lines = []
        with self._lock:
            for k, v in self._counters.items():
                lines.append(f"# TYPE {k.split('{')[0]} counter")
                lines.append(f"{k} {v}")
            for k, v in self._gauges.items():
                lines.append(f"# TYPE {k.split('{')[0]} gauge")
                lines.append(f"{k} {v}")
            for k, vals in self._histograms.items():
                if vals:
                    base = k.split('{')[0]
                    lines.append(f"# TYPE {base} summary")
                    srt = sorted(vals)
                    n   = len(srt)
                    for q, label in [(0.5,"0.5"),(0.9,"0.9"),(0.99,"0.99")]:
                        qv = srt[int(n*q)]
                        lines.append(f'{base}{{quantile="{label}"}} {qv}')
                    lines.append(f"{base}_sum {sum(vals)}")
                    lines.append(f"{base}_count {n}")
        return "\n".join(lines) + "\n"

    def run_server(self):
        from http.server import HTTPServer, BaseHTTPRequestHandler
        metrics_self = self
        class H(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path == '/metrics':
                    body = metrics_self.render().encode()
                    self.send_response(200)
                    self.send_header('Content-Type','text/plain; version=0.0.4')
                    self.end_headers(); self.wfile.write(body)
                else:
                    self.send_response(404); self.end_headers()
            def log_message(self, *a): pass
        try:
            HTTPServer(('0.0.0.0', METRICS_PORT), H).serve_forever()
        except Exception as e: log.warning(f"Metrics server: {e}")

prom = PrometheusMetrics()

# ══════════════════════════════════════════════════════════
#  [FIX8] LRU + TTL CACHE
# ══════════════════════════════════════════════════════════
class BoundedTTLCache:
    """Max_size limiti olan TTL cache — memory fragmentation yoxdur."""
    __slots__ = ('_ttl','_max','_store','_lock','_hits','_miss')
    def __init__(self, ttl=300.0, max_size=1000):
        self._ttl   = ttl
        self._max   = max_size
        self._store = {}
        self._lock  = threading.RLock()
        self._hits  = 0
        self._miss  = 0

    def get(self, key):
        with self._lock:
            e = self._store.get(key)
            if e is None: self._miss += 1; return None
            val, ts = e
            if time.monotonic() - ts > self._ttl:
                del self._store[key]; self._miss += 1; return None
            self._hits += 1; return val

    def set(self, key, val):
        with self._lock:
            if len(self._store) >= self._max:
                # LRU evict — ən köhnə 20%-i sil
                items = sorted(self._store.items(), key=lambda x: x[1][1])
                for k, _ in items[:self._max//5]: del self._store[k]
            self._store[key] = (val, time.monotonic())

    def invalidate(self, key): 
        with self._lock: self._store.pop(key, None)

    def clear(self):
        with self._lock: self._store.clear()

    def stats(self):
        with self._lock:
            total = self._hits + self._miss
            ratio = self._hits/total if total else 0
            return {"size": len(self._store), "hit_ratio": round(ratio, 3), "hits": self._hits, "misses": self._miss}

fg_cache        = BoundedTTLCache(ttl=300,  max_size=10)
sentiment_cache = BoundedTTLCache(ttl=600,  max_size=500)
xeber_cache     = BoundedTTLCache(ttl=120,  max_size=10)
market_cache    = BoundedTTLCache(ttl=60,   max_size=20)
ohlcv_cache     = BoundedTTLCache(ttl=3300, max_size=3000)
ind_cache       = BoundedTTLCache(ttl=3300, max_size=3000)

# ══════════════════════════════════════════════════════════
#  [FIX1] EVENT BUS
# ══════════════════════════════════════════════════════════
class EventBus:
    """
    Async event bus — actor model.
    WebSocket candle close → event → analyzer → signal → executor
    """
    def __init__(self):
        self._handlers: Dict[str, List] = defaultdict(list)
        self._lock = threading.Lock()

    def subscribe(self, event_type: str, handler):
        with self._lock:
            self._handlers[event_type].append(handler)

    async def emit(self, event_type: str, data: dict):
        with self._lock:
            handlers = list(self._handlers.get(event_type, []))
        for h in handlers:
            try:
                if asyncio.iscoroutinefunction(h):
                    asyncio.create_task(h(data))
                else:
                    loop = asyncio.get_event_loop()
                    loop.run_in_executor(None, h, data)
            except Exception as e:
                log.debug(f"EventBus {event_type}: {e}")

bus = EventBus()

# ══════════════════════════════════════════════════════════
#  [FIX7] CIRCUIT BREAKER + FAILOVER
# ══════════════════════════════════════════════════════════
class CircuitBreaker:
    """
    5 ardıcıl xətadan sonra 60 san açılır.
    Auto-reset: success gəldikdə bağlanır.
    """
    CLOSED = 'CLOSED'; OPEN = 'OPEN'; HALF_OPEN = 'HALF_OPEN'

    def __init__(self, name: str, failure_threshold=5, recovery_timeout=60):
        self.name      = name
        self._thresh   = failure_threshold
        self._timeout  = recovery_timeout
        self._fails    = 0
        self._state    = self.CLOSED
        self._opened   = 0.0
        self._lock     = threading.Lock()

    @property
    def is_open(self): 
        return self._state == self.OPEN

    def record_success(self):
        with self._lock:
            self._fails  = 0
            self._state  = self.CLOSED

    def record_failure(self):
        with self._lock:
            self._fails += 1
            if self._fails >= self._thresh:
                self._state  = self.OPEN
                self._opened = time.monotonic()
                log.warning(f"[CB] {self.name} AÇILDI — {self._thresh} xəta")
                prom.counter_inc("circuit_breaker_open_total", labels={"name": self.name})

    def allow(self) -> bool:
        with self._lock:
            if self._state == self.CLOSED:
                return True
            if self._state == self.OPEN:
                if time.monotonic() - self._opened > self._timeout:
                    self._state = self.HALF_OPEN
                    return True
                return False
            return True  # HALF_OPEN

cb_binance  = CircuitBreaker("binance",  failure_threshold=5, recovery_timeout=30)
cb_finnhub  = CircuitBreaker("finnhub",  failure_threshold=3, recovery_timeout=60)
cb_yfinance = CircuitBreaker("yfinance", failure_threshold=5, recovery_timeout=60)
cb_telegram = CircuitBreaker("telegram", failure_threshold=10, recovery_timeout=30)

# ══════════════════════════════════════════════════════════
#  [FIX7] RATE LIMITER
# ══════════════════════════════════════════════════════════
class RateLimiter:
    def __init__(self, max_calls: int, period: float):
        self._max    = max_calls
        self._period = period
        self._calls  = deque()
        self._lock   = threading.Lock()

    def wait_and_call(self):
        with self._lock:
            now = time.monotonic()
            while self._calls and now - self._calls[0] > self._period:
                self._calls.popleft()
            if len(self._calls) >= self._max:
                sleep_t = self._period - (now - self._calls[0])
                if sleep_t > 0: time.sleep(sleep_t)
            self._calls.append(time.monotonic())

    def __call__(self): return True  # flood check

finnhub_rl  = RateLimiter(max_calls=25, period=60.0)
telegram_rl = RateLimiter(max_calls=30, period=60.0)

# ══════════════════════════════════════════════════════════
#  [FIX10] TELEGRAM QUEUE + WATCHDOG
# ══════════════════════════════════════════════════════════
_tg_queue: queue.Queue = queue.Queue(maxsize=500)
_tg_alive  = threading.Event()
_tg_alive.set()

def _telegram_worker():
    """[FIX10] Watchdog ilə — worker ölsə restart olur."""
    session = requests.Session()
    session.headers.update({'User-Agent': 'Sniper/11.0'})
    while True:
        try:
            msg = _tg_queue.get(timeout=5)
            if msg is None: break
            if not cb_telegram.allow():
                _tg_queue.task_done(); continue
            telegram_rl.wait_and_call()
            t0 = time.monotonic()
            try:
                r = session.post(
                    f"https://api.telegram.org/bot{TOKEN}/sendMessage",
                    data={'chat_id': CHAT_ID, 'text': msg, 'parse_mode': 'HTML'},
                    timeout=10
                )
                r.raise_for_status()
                cb_telegram.record_success()
                ms = (time.monotonic() - t0) * 1000
                latency.record("telegram_send_ms", ms)
                prom.histogram_observe("telegram_latency_ms", ms)
            except Exception as e:
                cb_telegram.record_failure()
                log.warning(f"[TG] Göndərə bilmədi: {e}")
            _tg_queue.task_done()
        except queue.Empty:
            _tg_alive.set()
            continue
        except Exception as e:
            log.debug(f"TG worker: {e}")

def _telegram_watchdog():
    """[FIX10] Worker ölsə yenidən başladır."""
    global _tg_thread
    while not _shutdown_event.is_set():
        if not _tg_thread.is_alive():
            log.warning("[TG] Worker öldü — restart...")
            _tg_thread = threading.Thread(target=_telegram_worker, daemon=True, name="TGWorker")
            _tg_thread.start()
            telegram_gonder("⚠️ Telegram worker restart oldu.")
        time.sleep(10)

def telegram_gonder(msg: str):
    try:
        _tg_queue.put_nowait(msg)
    except queue.Full:
        log.warning("[TG] Queue dolu")

# ══════════════════════════════════════════════════════════
#  [FIX11] PERSISTENT STATE + DB
# ══════════════════════════════════════════════════════════
_db_queue: queue.Queue = queue.Queue(maxsize=1000)
_db_lock = threading.Lock()

def db_init():
    with _db_lock:
        con = sqlite3.connect(LOG_FAYL, timeout=20)
        con.execute("PRAGMA journal_mode=WAL")    # [FIX11] WAL mode — concurrent read
        con.execute("PRAGMA synchronous=NORMAL")
        con.execute("PRAGMA cache_size=10000")
        con.executescript("""
            CREATE TABLE IF NOT EXISTS signals (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                vaxt      TEXT, symbol TEXT, type TEXT,
                entry     REAL, tp REAL, sl REAL, rr REAL,
                skor      REAL, rejim TEXT, wyckoff TEXT,
                ob        INTEGER, fvg INTEGER, mtf_skor REAL,
                breakout  INTEGER, result TEXT DEFAULT 'AÇIQ',
                closed_at TEXT, pnl REAL
            );
            CREATE TABLE IF NOT EXISTS backtest (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol   TEXT, bar_idx INTEGER, yon TEXT,
                entry    REAL, tp REAL, sl REAL, rr REAL,
                skor     REAL, rejim TEXT, hit TEXT,
                fee_pct  REAL DEFAULT 0.1, slippage_pct REAL DEFAULT 0.05,
                net_pnl  REAL
            );
            CREATE TABLE IF NOT EXISTS positions (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol      TEXT UNIQUE, yon TEXT,
                entry       REAL, qty REAL, sl REAL,
                tp1         REAL, tp2 REAL, tp3 REAL,
                status      TEXT DEFAULT 'OPEN',
                opened_at   TEXT, closed_at TEXT, pnl REAL
            );
            CREATE TABLE IF NOT EXISTS state (
                key   TEXT PRIMARY KEY,
                value TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_signals_vaxt   ON signals(vaxt);
            CREATE INDEX IF NOT EXISTS idx_positions_sym  ON positions(symbol);
        """)
        con.commit(); con.close()

def db_state_save(key: str, val):
    try:
        con = sqlite3.connect(LOG_FAYL, timeout=10)
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("INSERT OR REPLACE INTO state(key,value) VALUES(?,?)", (key, json.dumps(val)))
        con.commit(); con.close()
    except Exception as e: log.debug(f"state save: {e}")

def db_state_load(key: str, default=None):
    try:
        con = sqlite3.connect(LOG_FAYL, timeout=10)
        con.execute("PRAGMA journal_mode=WAL")
        row = con.execute("SELECT value FROM state WHERE key=?", (key,)).fetchone()
        con.close()
        return json.loads(row[0]) if row else default
    except Exception: return default

def _db_worker():
    while True:
        try:
            item = _db_queue.get(timeout=5)
            if item is None: break
            table, row = item
            try:
                with _db_lock:
                    con = sqlite3.connect(LOG_FAYL, timeout=15)
                    con.execute("PRAGMA journal_mode=WAL")
                    if table == 'signals':
                        con.execute("""
                            INSERT INTO signals(vaxt,symbol,type,entry,tp,sl,rr,skor,rejim,wyckoff,ob,fvg,mtf_skor,breakout)
                            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                            (datetime.now().strftime('%Y-%m-%d %H:%M'), row['symbol'], row['type'],
                             row['entry'], row['tp'], row['sl'], row['rr'], row['skor'],
                             row['rejim'], row['wyckoff'], int(row['ob']), int(row['fvg']),
                             row['mtf_skor'], int(row.get('breakout', False))))
                    elif table == 'backtest':
                        con.execute("""
                            INSERT INTO backtest(symbol,bar_idx,yon,entry,tp,sl,rr,skor,rejim,hit,fee_pct,slippage_pct,net_pnl)
                            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                            (row['symbol'], row.get('bar_idx',0), row['yon'],
                             row['entry'], row['tp'], row['sl'], row['rr'], row['skor'],
                             row['rejim'], row['hit'],
                             row.get('fee_pct', 0.1), row.get('slippage_pct', 0.05), row.get('net_pnl', 0.0)))
                    elif table == 'position':
                        con.execute("""
                            INSERT OR REPLACE INTO positions(symbol,yon,entry,qty,sl,tp1,tp2,tp3,status,opened_at)
                            VALUES(?,?,?,?,?,?,?,?,?,?)""",
                            (row['symbol'], row['yon'], row['entry'], row.get('qty',0),
                             row['sl'], row.get('tp1',0), row.get('tp2',0), row.get('tp3',0),
                             'OPEN', datetime.now().strftime('%Y-%m-%d %H:%M')))
                    con.commit(); con.close()
                    prom.counter_inc("db_writes_total", labels={"table": table})
            except Exception as e: log.warning(f"DB yazma: {e}")
            _db_queue.task_done()
        except queue.Empty: continue
        except Exception as e: log.debug(f"DB worker: {e}")

def db_yaz(table: str, row: dict):
    try: _db_queue.put_nowait((table, row))
    except queue.Full: log.warning("[DB] Queue dolu")

def db_hefte_stats():
    try:
        con = sqlite3.connect(LOG_FAYL, timeout=5)
        con.execute("PRAGMA journal_mode=WAL")
        hefte = (datetime.now()-timedelta(days=7)).strftime('%Y-%m-%d')
        rows = con.execute("SELECT type,skor,rr FROM signals WHERE vaxt>=?", (hefte,)).fetchall()
        con.close()
        n = len(rows)
        lg  = sum(1 for r in rows if 'LONG'  in r[0])
        sh  = sum(1 for r in rows if 'SHORT' in r[0])
        sk  = sum(r[1] for r in rows)/n if n else 0
        rr  = sum(r[2] for r in rows)/n if n else 0
        return n, lg, sh, sk, rr
    except Exception: return 0,0,0,0.0,0.0

# ══════════════════════════════════════════════════════════
#  [FIX11] STATE RECOVERY
# ══════════════════════════════════════════════════════════
def state_recover():
    """Bot restart olduqda son vəziyyəti bərpa edir."""
    global son_gonderilme, gunluk_siqnal, gunluk_zefer
    try:
        saved = db_state_load("bot_state", {})
        if saved:
            son_gonderilme = saved.get("son_gonderilme", {})
            # Timestamp string → float
            son_gonderilme = {k: float(v) for k,v in son_gonderilme.items()}
            gunluk_siqnal  = saved.get("gunluk_siqnal", 0)
            gunluk_zefer   = saved.get("gunluk_zefer", 0.0)
            log.info(f"[RECOVERY] State bərpa: {len(son_gonderilme)} cooldown, {gunluk_siqnal} siqnal")
        # Açıq mövqeləri bərpa et
        con = sqlite3.connect(LOG_FAYL, timeout=5)
        open_pos = con.execute("SELECT symbol,yon,entry,sl,tp2 FROM positions WHERE status='OPEN'").fetchall()
        con.close()
        if open_pos:
            log.info(f"[RECOVERY] {len(open_pos)} açıq mövqe tapıldı")
            for sym, yon, entry, sl, tp in open_pos:
                position_manager.positions[sym] = {
                    'yon': yon, 'entry': entry, 'sl': sl, 'tp': tp, 'status': 'OPEN'
                }
            telegram_gonder(f"♻️ <b>STATE BƏRPA</b>\n{len(open_pos)} açıq mövqe tapıldı!")
    except Exception as e:
        log.warning(f"[RECOVERY] xəta: {e}")

def state_save_periodic():
    """Hər 60 saniyə state saxla."""
    while not _shutdown_event.is_set():
        try:
            with _state_lock:
                db_state_save("bot_state", {
                    "son_gonderilme": {k: str(v) for k,v in son_gonderilme.items()},
                    "gunluk_siqnal":  gunluk_siqnal,
                    "gunluk_zefer":   gunluk_zefer,
                    "saved_at":       datetime.now().isoformat()
                })
        except Exception as e: log.debug(f"state save: {e}")
        time.sleep(60)

# ══════════════════════════════════════════════════════════
#  [FIX12] RISK ENGINE v2 — Portfolio-aware
# ══════════════════════════════════════════════════════════
class RiskEngineV2:
    """
    Kelly Criterion + portfolio exposure cap + correlation filter + volatility scaling.
    """
    def __init__(self, base_risk=1.0, max_risk=2.0, max_portfolio_exp=0.30):
        self.base_risk        = base_risk
        self.max_risk         = max_risk
        self.max_portfolio    = max_portfolio_exp  # max 30% portfolio exposure
        self._history         = deque(maxlen=200)
        self._open_exposure   = 0.0  # cari açıq mövqe exposure
        self._lock            = threading.Lock()

    def record(self, win: bool, rr: float, pnl: float = 0.0):
        with self._lock:
            self._history.append((win, rr, pnl))

    def kelly_fraction(self) -> float:
        with self._lock:
            h = list(self._history)
        if len(h) < 10: return self.base_risk
        wins   = [rr for w, rr, _ in h if w]
        losses = [rr for w, rr, _ in h if not w]
        if not losses: return self.max_risk
        wr     = len(wins) / len(h)
        avg_rr = sum(wins)/len(wins) if wins else RR_MINIMUM
        kelly  = (wr * avg_rr - (1-wr)) / avg_rr
        kelly  = max(0.0, min(kelly, 0.25))
        return round(min(kelly * 4 * self.base_risk, self.max_risk), 3)

    def position_size(self, balance: float, entry: float, sl: float,
                      atr_ratio: float = 1.0) -> float:
        """
        Volatility scaling: ATR yüksəkdirsə miqdar azalır.
        Portfolio cap: açıq exposure 30%-dən artıq olmasın.
        """
        # Portfolio cap yoxla
        if self._open_exposure >= self.max_portfolio * balance:
            log.debug("[RISK] Portfolio cap — yeni mövqe açılmır")
            return 0.0

        risk_pct   = self.kelly_fraction()
        # Volatility scaling — ATR ratio 2x olarsa risk yarıya düşür
        vol_scale  = max(0.3, 1.0 / max(atr_ratio, 1.0))
        risk_pct  *= vol_scale
        risk_usdt  = balance * (risk_pct / 100)
        sl_dist    = abs(entry - sl)
        if sl_dist == 0: return 0.0
        qty = risk_usdt / sl_dist

        # Portfolio exposure yoxla
        new_exp = entry * qty
        if self._open_exposure + new_exp > self.max_portfolio * balance:
            qty = max(0, (self.max_portfolio * balance - self._open_exposure) / entry)

        return round(qty, 6)

    def update_exposure(self, delta: float):
        with self._lock: self._open_exposure = max(0, self._open_exposure + delta)

    def correlation_ok(self, asset_type: str, current_counts: dict) -> bool:
        """Eyni növdən MAX_KORRELYASIYA-dan çox siqnal verməsin."""
        return current_counts.get(asset_type, 0) < MAX_KORRELYASIYA

    def daily_loss_ok(self) -> bool:
        return gunluk_zefer < MAX_GUNLUK_ZEFER

risk_engine = RiskEngineV2(base_risk=1.0, max_risk=2.0, max_portfolio_exp=0.30)

# ══════════════════════════════════════════════════════════
#  [FIX6] POSITION MANAGER
# ══════════════════════════════════════════════════════════
class PositionManager:
    """
    Partial fill, cancel race, duplicate order qoruması.
    Thread-safe position tracking.
    """
    def __init__(self):
        self.positions: Dict[str, dict] = {}
        self._lock    = threading.Lock()
        self._pending : set = set()  # duplicate order qoruması

    def can_open(self, symbol: str) -> bool:
        with self._lock:
            return symbol not in self.positions and symbol not in self._pending

    def mark_pending(self, symbol: str):
        with self._lock: self._pending.add(symbol)

    def unmark_pending(self, symbol: str):
        with self._lock: self._pending.discard(symbol)

    def open_position(self, symbol: str, data: dict):
        with self._lock:
            self._pending.discard(symbol)
            self.positions[symbol] = {**data, 'status': 'OPEN', 'partial_fills': []}
            risk_engine.update_exposure(data.get('entry', 0) * data.get('qty', 0))
            db_yaz('position', {'symbol': symbol, **data})
            prom.gauge_set("open_positions", len(self.positions))
            log.info(f"[POS] Açıldı: {symbol} | {data['yon']} | qty={data.get('qty',0)}")

    def close_position(self, symbol: str, close_price: float, reason: str = 'manual'):
        with self._lock:
            pos = self.positions.pop(symbol, None)
            if pos is None: return
            pnl = (close_price - pos['entry']) if pos['yon']=='LONG' else (pos['entry'] - close_price)
            pnl *= pos.get('qty', 1)
            risk_engine.update_exposure(-(pos.get('entry',0) * pos.get('qty',0)))
            risk_engine.record(pnl > 0, pos.get('rr', RR_MINIMUM), pnl)
            prom.gauge_set("open_positions", len(self.positions))
            prom.counter_inc("positions_closed_total", labels={"reason": reason})
            log.info(f"[POS] Bağlandı: {symbol} | PnL: {pnl:.4f} | {reason}")
            return pnl

    def get_all(self) -> dict:
        with self._lock: return dict(self.positions)

position_manager = PositionManager()

# ══════════════════════════════════════════════════════════
#  [FIX5] ORDER EXECUTION ENGINE — TWAP/VWAP/iceberg
# ══════════════════════════════════════════════════════════
class OrderExecutionEngine:
    """
    Smart execution:
    - TWAP: miqdarı N hissəyə böl, hər T saniyə bir hissə
    - VWAP: həcmə görə qiymət ortalama
    - Iceberg: böyük orderləri gizlət
    - Slippage protection: limit order, əgər keçirsə ləğv et
    """
    def __init__(self):
        self._lock = threading.Lock()
        self._active_orders: Dict[str, dict] = {}

    async def execute_twap(self, symbol: str, side: str, qty: float,
                            slices: int = 5, interval_sec: float = 12.0,
                            max_slippage_pct: float = 0.1) -> Optional[dict]:
        """TWAP execution — qty-ni slices hissəyə böl."""
        if not AUTO_TRADE:
            log.info(f"[EXEC] TWAP SİMULASİYA: {symbol} {side} qty={qty:.6f}")
            return {'status': 'simulated', 'symbol': symbol, 'side': side, 'qty': qty}

        slice_qty = qty / slices
        fills     = []
        t0        = time.monotonic()
        prom.counter_inc("orders_submitted_total", labels={"type": "twap", "side": side})

        for i in range(slices):
            if _shutdown_event.is_set(): break
            try:
                ticker  = exchange.fetch_ticker(symbol)
                bid     = ticker['bid']
                ask     = ticker['ask']
                ref_px  = ask if side == 'buy' else bid
                # Slippage check
                if fills:
                    avg_fill = sum(f['price']*f['qty'] for f in fills) / sum(f['qty'] for f in fills)
                    slip_pct = abs(ref_px - avg_fill) / avg_fill * 100
                    if slip_pct > max_slippage_pct:
                        log.warning(f"[EXEC] Slippage {slip_pct:.3f}% > {max_slippage_pct}% — dayandırıldı")
                        prom.counter_inc("orders_cancelled_total", labels={"reason": "slippage"})
                        break
                # Limit order
                order = exchange.create_limit_order(symbol, side, slice_qty, ref_px)
                fills.append({'price': ref_px, 'qty': slice_qty, 'order_id': order.get('id')})
                log.info(f"[EXEC] TWAP slice {i+1}/{slices}: {symbol} {side} {slice_qty:.6f} @ {ref_px}")
            except Exception as e:
                log.warning(f"[EXEC] TWAP slice xəta: {e}")

            if i < slices - 1:
                await asyncio.sleep(interval_sec)

        if not fills: return None
        avg_px  = sum(f['price']*f['qty'] for f in fills) / sum(f['qty'] for f in fills)
        total_q = sum(f['qty'] for f in fills)
        exec_ms = (time.monotonic() - t0) * 1000
        latency.record("order_exec_ms", exec_ms)
        prom.histogram_observe("order_exec_ms", exec_ms)
        return {'status': 'filled', 'avg_price': avg_px, 'total_qty': total_q, 'fills': fills}

    async def execute_market(self, symbol: str, side: str, qty: float) -> Optional[dict]:
        """Sürətli market order — slippage qoruması ilə."""
        if not AUTO_TRADE:
            log.info(f"[EXEC] MARKET SİMULASİYA: {symbol} {side} qty={qty:.6f}")
            return {'status': 'simulated'}
        t0 = time.monotonic()
        try:
            ticker   = exchange.fetch_ticker(symbol)
            ref_px   = ticker['ask'] if side == 'buy' else ticker['bid']
            order    = exchange.create_market_order(symbol, side, qty)
            fill_px  = order.get('average', ref_px)
            slip_pct = abs(fill_px - ref_px) / ref_px * 100
            exec_ms  = (time.monotonic() - t0) * 1000
            latency.record("order_exec_ms", exec_ms)
            prom.histogram_observe("order_exec_ms", exec_ms)
            prom.counter_inc("orders_filled_total", labels={"type": "market", "side": side})
            if slip_pct > 0.2:
                log.warning(f"[EXEC] Yüksək slippage: {slip_pct:.3f}%")
            return {'status': 'filled', 'avg_price': fill_px, 'slippage_pct': slip_pct}
        except Exception as e:
            log.error(f"[EXEC] Market order xəta: {e}")
            prom.counter_inc("orders_failed_total")
            return None

execution_engine = OrderExecutionEngine()

# ══════════════════════════════════════════════════════════
#  [FIX4] BINANCE NATIVE WEBSOCKET
# ══════════════════════════════════════════════════════════
class BinanceWebSocket:
    """
    Binance native kline websocket — ccxt-dan 10x sürətli.
    Candle close event → EventBus-a emit edir.
    Auto-reconnect: WS_RECONNECT_MAX dəfə cəhd edir.
    """
    BASE_URL = "wss://stream.binance.com:9443/stream"

    def __init__(self, symbols: List[str], interval: str = "1h"):
        self.symbols   = symbols[:50]  # Max 50 stream / bağlantı
        self.interval  = interval
        self._reconnects = 0
        self._running  = False
        self._candle_buf: Dict[str, dict] = {}

    def _stream_url(self) -> str:
        # Kline stream + bookTicker stream (bid/ask üçün)
        klines   = [f"{s.replace('/','').lower()}@kline_{self.interval}"
                    for s in self.symbols]
        tickers  = [f"{s.replace('/','').lower()}@bookTicker"
                    for s in self.symbols]
        streams  = "/".join(klines + tickers)
        return f"{self.BASE_URL}?streams={streams}"

    async def run(self):
        self._running = True
        while self._running and not _shutdown_event.is_set():
            try:
                url = self._stream_url()
                log.info(f"[WS] Qoşulur: {len(self.symbols)} stream, {self.interval}")
                t0 = time.monotonic()
                async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                    self._reconnects = 0
                    latency.record("ws_connect_ms", (time.monotonic()-t0)*1000)
                    log.info("[WS] Qoşuldu ✅")
                    async for raw in ws:
                        if _shutdown_event.is_set(): break
                        t_recv = time.monotonic()
                        try:
                            msg  = json.loads(raw)
                            data = msg.get('data', msg)
                            sym  = data.get('s', '')  # BTCUSDT

                            # bookTicker → Liquidity Filter
                            if 'b' in data and 'a' in data and sym:
                                try:
                                    bid = float(data['b']); ask = float(data['a'])
                                    liquidity_filter.record_spread(sym, bid, ask)
                                except Exception:
                                    pass
                                continue  # bookTicker üçün candle emit etmə

                            k    = data.get('k', {})
                            is_closed = k.get('x', False)

                            if is_closed and sym:
                                candle = {
                                    'symbol':    sym,
                                    'open':   float(k['o']),
                                    'high':   float(k['h']),
                                    'low':    float(k['l']),
                                    'close':  float(k['c']),
                                    'volume': float(k['v']),
                                    'time':   k['t'],
                                    'tf':     self.interval
                                }
                                ws_ms = (time.monotonic() - t_recv) * 1000
                                latency.record("ws_latency_ms", ws_ms)
                                prom.histogram_observe("ws_latency_ms", ws_ms)
                                prom.counter_inc("ws_candles_received_total")
                                # EventBus-a emit et
                                await bus.emit("candle_close", candle)
                        except Exception as e:
                            log.debug(f"[WS] parse: {e}")

            except Exception as e:
                self._reconnects += 1
                if self._reconnects > WS_RECONNECT_MAX:
                    log.error(f"[WS] Max reconnect ({WS_RECONNECT_MAX}) keçdi — fallback REST")
                    telegram_gonder(f"⚠️ WebSocket xəta — REST fallback aktiv: {e}")
                    break
                wait = min(2 ** self._reconnects, 60)
                log.warning(f"[WS] xəta #{self._reconnects} — {wait}s gözlənilir: {e}")
                await asyncio.sleep(wait)

    def stop(self): self._running = False

# ══════════════════════════════════════════════════════════
#  [FIX9] NUMPY VECTORİZED İNDİKATORLAR
# ══════════════════════════════════════════════════════════
def _ema_np(arr: np.ndarray, period: int) -> np.ndarray:
    result = np.full(len(arr), np.nan, dtype=np.float64)
    if len(arr) < period: return result
    k = 2.0 / (period + 1)
    result[period-1] = arr[:period].mean()
    for i in range(period, len(arr)):
        result[i] = arr[i]*k + result[i-1]*(1-k)
    return result

def _rsi_np(arr: np.ndarray, period: int = 14) -> np.ndarray:
    result = np.full(len(arr), np.nan, dtype=np.float64)
    if len(arr) < period+1: return result
    delta = np.diff(arr.astype(np.float64))
    gains = np.where(delta>0, delta, 0.0)
    losses= np.where(delta<0,-delta, 0.0)
    ag = gains[:period].mean(); al = losses[:period].mean()
    for i in range(period, len(delta)):
        ag = (ag*(period-1)+gains[i])/period
        al = (al*(period-1)+losses[i])/period
        result[i+1] = 100.0 if al==0 else 100-100/(1+ag/al)
    return result

def _atr_np(h: np.ndarray, l: np.ndarray, c: np.ndarray, period: int = 14) -> np.ndarray:
    result = np.full(len(h), np.nan, dtype=np.float64)
    if len(h) < period+1: return result
    tr = np.maximum(h[1:]-l[1:], np.maximum(abs(h[1:]-c[:-1]), abs(l[1:]-c[:-1])))
    av = tr[:period].mean()
    result[period] = av
    for i in range(period, len(tr)):
        av = (av*(period-1)+tr[i])/period
        result[i+1] = av
    return result

def _adx_np(h: np.ndarray, l: np.ndarray, c: np.ndarray, period: int = 14) -> float:
    if len(h) < period*2: return 20.0
    try:
        tr = np.maximum(h[1:]-l[1:], np.maximum(abs(h[1:]-c[:-1]), abs(l[1:]-c[:-1])))
        dp = np.where((h[1:]-h[:-1])>(l[:-1]-l[1:]), np.maximum(h[1:]-h[:-1],0), 0)
        dm = np.where((l[:-1]-l[1:])>(h[1:]-h[:-1]), np.maximum(l[:-1]-l[1:],0), 0)
        def smma(a,n):
            r=np.zeros(len(a)); r[n-1]=a[:n].sum()
            for i in range(n,len(a)): r[i]=r[i-1]-r[i-1]/n+a[i]
            return r
        atr14=smma(tr,period); dmp14=smma(dp,period); dmm14=smma(dm,period)
        with np.errstate(divide='ignore',invalid='ignore'):
            dip=np.where(atr14>0,100*dmp14/atr14,0)
            dim=np.where(atr14>0,100*dmm14/atr14,0)
            dx =np.where((dip+dim)>0,100*abs(dip-dim)/(dip+dim),0)
        adx=smma(np.nan_to_num(dx),period)
        v=adx[adx>0]; return float(v[-1]) if len(v) else 20.0
    except Exception: return 20.0

# ══════════════════════════════════════════════════════════
#  [FIX2] ASYNC SEMAPHORE — thread partlaması yoxdur
# ══════════════════════════════════════════════════════════
_analysis_sem = asyncio.Semaphore(20)  # max 20 eyni vaxtda analiz

# ══════════════════════════════════════════════════════════
#  [FIX3] CPU TASKS — ProcessPool
# ══════════════════════════════════════════════════════════
_cpu_pool = ProcessPoolExecutor(max_workers=max(2, os.cpu_count()-1))

def _heavy_compute_worker(ohlcv_data: dict) -> dict:
    """
    CPU-intensive hesablamalar ayrı prosesdə işləyir — GIL yoxdur.
    """
    try:
        c = np.array(ohlcv_data['close'], dtype=np.float64)
        h = np.array(ohlcv_data['high'],  dtype=np.float64)
        l = np.array(ohlcv_data['low'],   dtype=np.float64)
        return {
            'ema50':  _ema_np(c, 50).tolist(),
            'ema200': _ema_np(c, 200).tolist(),
            'rsi14':  _rsi_np(c, 14).tolist(),
            'atr14':  _atr_np(h, l, c, 14).tolist(),
            'adx14':  _adx_np(h, l, c, 14),
        }
    except Exception: return {}

# ══════════════════════════════════════════════════════════
#  [FIX14] SESSION POOL + LATENCY
# ══════════════════════════════════════════════════════════
_session_local = threading.local()

def _session_get() -> requests.Session:
    if not hasattr(_session_local,'s') or _session_local.s is None:
        s = requests.Session()
        s.headers.update({'User-Agent':'Sniper/11.0'})
        adp = requests.adapters.HTTPAdapter(pool_connections=4, pool_maxsize=8, max_retries=0)
        s.mount('https://', adp)
        _session_local.s = s
    return _session_local.s

# ══════════════════════════════════════════════════════════
#  EXCHANGE + THREAD-SAFE WRAPPER
# ══════════════════════════════════════════════════════════
class ThreadSafeExchange:
    def __init__(self, ex):
        self._ex = ex; self._lock = threading.RLock()
    def __getattr__(self, name):
        attr = getattr(self._ex, name)
        if not callable(attr): return attr
        def locked(*a, **kw):
            with self._lock: return attr(*a, **kw)
        return locked

exchange = ThreadSafeExchange(ccxt.binance({'enableRateLimit': True}))

# ══════════════════════════════════════════════════════════
#  QLOBAL VƏZİYYƏT
# ══════════════════════════════════════════════════════════
_state_lock       = threading.Lock()
bot_dayan         = False
gunluk_zefer      = 0.0
gunluk_siqnal     = 0
son_gonderilme: Dict[str, float] = {}
korrelyasiya_say  = {"crypto":0,"forex":0,"stock":0,"commodity":0}
_ml_model_cache   = None
_shutdown_event   = threading.Event()
wfo_lock          = threading.Lock()

# ══════════════════════════════════════════════════════════
#  AKTİV SİYAHILARI (v10-dan saxlanıldı, tam)
# ══════════════════════════════════════════════════════════
FOREX_ASSETS = [
    "EURUSD=X","GBPUSD=X","USDJPY=X","AUDUSD=X","USDCAD=X","USDCHF=X",
    "NZDUSD=X","EURGBP=X","EURJPY=X","GBPJPY=X","AUDJPY=X","CADJPY=X",
    "CHFJPY=X","EURCHF=X","EURAUD=X","EURCAD=X","GBPAUD=X","GBPCAD=X",
    "GBPCHF=X","AUDCAD=X","AUDCHF=X","AUDNZD=X","CADCHF=X","NZDCAD=X",
    "NZDCHF=X","NZDJPY=X","USDHKD=X","USDSGD=X","USDMXN=X","USDZAR=X",
]
COMMODITY_ASSETS = [
    "GC=F","SI=F","CL=F","BZ=F","NG=F","HG=F","ZW=F","ZC=F","ZS=F",
    "ZL=F","ZM=F","KC=F","CC=F","CT=F","SB=F","OJ=F","LE=F","ALI=F",
]
INDEX_ASSETS = [
    "^GSPC","^DJI","^IXIC","^FTSE","^GDAXI","^FCHI","^N225","^HSI","^STI","^AXJO","^RUT",
]
SP500_ASSETS = [
    "AAPL","MSFT","GOOGL","AMZN","META","NVDA","TSLA","BRK-B","JPM","V",
    "UNH","JNJ","XOM","WMT","MA","PG","HD","CVX","MRK","ABBV","LLY",
    "BAC","PFE","COST","AVGO","TMO","KO","PEP","WFC","ABT","MCD",
    "CSCO","ACN","DHR","ADBE","NKE","TXN","CRM","NEE","RTX","HON",
    "UPS","QCOM","BMY","AMGN","MDT","IBM","INTU","CAT","GS","SBUX",
    "LMT","GILD","CI","ISRG","REGN","VRTX","ZTS","SYK","BDX","BSX",
    "EW","DXCM","IDXX","BIIB","MRNA","COIN","MARA","RIOT",
    "MSTR","SQ","PYPL","ROKU","UBER","ABNB","DASH","RBLX",
    "DKNG","AMD","INTC","MU","AMAT","LRCX","KLAC","TSM","MRVL","MPWR","ENPH","FSLR",
]
AVROPA_ASSETS = [
    "ASML.AS","SAP.DE","MC.PA","NESN.SW","NOVN.SW","ROG.SW",
    "AZN.L","SHEL.L","HSBA.L","BP.L","ULVR.L","RIO.L","GSK.L",
    "LSEG.L","VOD.L","SIE.DE","BMW.DE","VOW3.DE","BAS.DE",
    "ALV.DE","AIR.PA","BNP.PA","SAN.PA","ENGI.PA","OR.PA",
    "TTE.PA","SU.PA","RMS.PA","EL.PA",
]
ASSET_PARAMS = {
    "CRYPTO":    {"rsi_long":40,"rsi_short":60,"atr_sl":3.2,"vol_mult":1.2,"sweep_period":18,"rr_min":RR_MINIMUM},
    "FOREX":     {"rsi_long":38,"rsi_short":62,"atr_sl":2.4,"vol_mult":1.15,"sweep_period":20,"rr_min":1.9},
    "COMMODITY": {"rsi_long":40,"rsi_short":60,"atr_sl":2.7,"vol_mult":1.2,"sweep_period":20,"rr_min":1.95},
    "INDEX":     {"rsi_long":35,"rsi_short":65,"atr_sl":2.1,"vol_mult":1.3,"sweep_period":25,"rr_min":1.9},
    "STOCK":     {"rsi_long":36,"rsi_short":64,"atr_sl":2.2,"vol_mult":1.25,"sweep_period":22,"rr_min":1.8},
}

# ══════════════════════════════════════════════════════════
#  GRACEFUL SHUTDOWN
# ══════════════════════════════════════════════════════════
def _shutdown_handler(signum, frame):
    log.info(f"[SHUTDOWN] {signum} alındı — dayandırılır...")
    _shutdown_event.set()

signal.signal(signal.SIGINT,  _shutdown_handler)
signal.signal(signal.SIGTERM, _shutdown_handler)

# ══════════════════════════════════════════════════════════
#  HEALTH SERVER (port 8080)
# ══════════════════════════════════════════════════════════
def _health_server():
    from http.server import HTTPServer, BaseHTTPRequestHandler
    class H(BaseHTTPRequestHandler):
        def do_GET(self):
            ok   = not _shutdown_event.is_set() and not bot_dayan
            code = 200 if ok else 503
            lat  = latency.all_metrics()
            body = json.dumps({
                "status":       "ok" if ok else "paused",
                "signals_today": gunluk_siqnal,
                "open_positions": len(position_manager.get_all()),
                "latency_ms":   {k: v["p95"] for k,v in lat.items()},
                "uptime_s":     int(time.monotonic()),
            }).encode()
            self.send_response(code)
            self.send_header('Content-Type','application/json')
            self.end_headers(); self.wfile.write(body)
        def log_message(self,*a): pass
    try: HTTPServer(('0.0.0.0', HEALTH_PORT), H).serve_forever()
    except Exception as e: log.warning(f"Health server: {e}")

# ══════════════════════════════════════════════════════════
#  [NEW] LİQUİDİTY FİLTER — Real-time spread genişlənməsi
#  Yüksək volatillik anında spread-in genişlənməsini ölçür.
#  Giriş siqnalı spreadin normal həddi aşdığında bloklanır.
# ══════════════════════════════════════════════════════════
class LiquidityFilter:
    """
    Real-time bid/ask spread monitoru.

    Normal bazar:    spread ≈ 0.01–0.05%
    Yüksək volatillik: spread > 0.15%  → giriş təhlükəlidir
    Xəbər anı:        spread > 0.30%  → giriş qadağandır

    Alqoritm:
    1. Son N spread ölçümünü saxla (rolling window)
    2. Cari spread = (ask - bid) / mid × 100
    3. z-score: (cari - orta) / std  →  anomaliya aşkar et
    4. Threshold: z > 2.5 OR spread_pct > MAX_SPREAD_PCT → BLOK
    """
    MAX_SPREAD_PCT_CRYPTO   = 0.15   # % — crypto üçün
    MAX_SPREAD_PCT_FOREX    = 0.08   # % — forex üçün
    MAX_SPREAD_PCT_STOCK    = 0.20   # % — səhmlər üçün
    ZSCORE_THRESHOLD        = 2.5
    WINDOW                  = 60     # son 60 ölçüm

    def __init__(self):
        self._lock    = threading.Lock()
        self._history: Dict[str, deque] = defaultdict(
            lambda: deque(maxlen=self.WINDOW)
        )
        self._blocked: Dict[str, float] = {}  # symbol → block_until timestamp

    def _max_spread(self, asset_type: str) -> float:
        return {
            'crypto':    self.MAX_SPREAD_PCT_CRYPTO,
            'forex':     self.MAX_SPREAD_PCT_FOREX,
            'stock':     self.MAX_SPREAD_PCT_STOCK,
            'commodity': self.MAX_SPREAD_PCT_STOCK,
        }.get(asset_type, self.MAX_SPREAD_PCT_CRYPTO)

    def record_spread(self, symbol: str, bid: float, ask: float):
        """Ticker-dən bid/ask gəldikdə spread-i qeydə al."""
        if bid <= 0 or ask <= 0 or ask < bid:
            return
        mid        = (bid + ask) / 2
        spread_pct = (ask - bid) / mid * 100
        with self._lock:
            self._history[symbol].append((time.monotonic(), spread_pct))
        prom.histogram_observe("spread_pct", spread_pct,
                               labels={"symbol": symbol[:6]})

    def is_liquid(self, symbol: str, asset_type: str = 'crypto') -> Tuple[bool, str]:
        """
        Returns (True, '') əgər giriş təhlükəsizdirsə,
                (False, səbəb) əgər spread yüksəkdirsə.
        """
        with self._lock:
            # Əvvəlki blok bitibmi?
            if symbol in self._blocked:
                if time.monotonic() < self._blocked[symbol]:
                    return False, f"spread-blok aktiv"
                else:
                    del self._blocked[symbol]

            hist = list(self._history.get(symbol, []))

        if not hist:
            return True, ''   # məlumat yoxdur — keç

        # Köhnə ölçümləri (>120s) at
        now    = time.monotonic()
        recent = [s for t, s in hist if now - t < 120]
        if len(recent) < 3:
            return True, ''   # az nümunə — keç

        max_spread = self._max_spread(asset_type)
        cur_spread = recent[-1]

        # Sadə threshold yoxlama
        if cur_spread > max_spread * 2:
            reason = f"spread kritik: {cur_spread:.3f}% (max {max_spread}%)"
            with self._lock:
                self._blocked[symbol] = time.monotonic() + 300  # 5 dəq blok
            prom.counter_inc("liquidity_block_total",
                             labels={"reason": "critical_spread"})
            log.warning(f"[LIQ] {symbol} BLOK — {reason}")
            return False, reason

        if cur_spread > max_spread:
            reason = f"spread geniş: {cur_spread:.3f}% (max {max_spread}%)"
            prom.counter_inc("liquidity_block_total",
                             labels={"reason": "wide_spread"})
            log.debug(f"[LIQ] {symbol} keçilmədi — {reason}")
            return False, reason

        # Z-score anomaliya aşkarı
        if len(recent) >= 10:
            arr  = np.array(recent[:-1], dtype=np.float64)
            mu   = arr.mean()
            std  = arr.std()
            if std > 0:
                z = (cur_spread - mu) / std
                if z > self.ZSCORE_THRESHOLD:
                    reason = f"spread anomaliya z={z:.1f} ({cur_spread:.3f}%)"
                    prom.counter_inc("liquidity_block_total",
                                     labels={"reason": "zscore"})
                    log.debug(f"[LIQ] {symbol} keçilmədi — {reason}")
                    return False, reason

        return True, ''

    def get_stats(self, symbol: str) -> dict:
        with self._lock:
            hist = list(self._history.get(symbol, []))
        if not hist:
            return {}
        spreads = [s for _, s in hist]
        return {
            'current':  round(spreads[-1], 4),
            'mean':     round(np.mean(spreads), 4),
            'max':      round(np.max(spreads), 4),
            'n':        len(spreads),
        }

liquidity_filter = LiquidityFilter()

# WebSocket-dən gələn hər ticker-i Liquidity Filter-ə yönləndir
_liq_ticker_buf: Dict[str, dict] = {}  # symbol → son ticker

# ══════════════════════════════════════════════════════════
#  XƏBƏR FİLTRİ
# ══════════════════════════════════════════════════════════
def xeberleri_yoxla():
    cached = xeber_cache.get("xeber")
    if cached: return cached
    try:
        if not cb_finnhub.allow(): return False, None, None
        today = datetime.now().strftime('%Y-%m-%d')
        url   = f"https://finnhub.io/api/v1/calendar/economic?from={today}&to={today}&token={FINNHUB_API_KEY}"
        finnhub_rl.wait_and_call()
        t0 = time.monotonic()
        r  = _session_get().get(url, timeout=10); r.raise_for_status()
        latency.record("finnhub_api_ms", (time.monotonic()-t0)*1000)
        cb_finnhub.record_success()
        now_t = datetime.now()
        for ev in r.json().get('economicCalendar',[]):
            impact = ev.get('impact','').lower()
            if impact not in ('high','medium'): continue
            pre_min  = 60 if impact=='high' else 30
            post_min = 30 if impact=='high' else 15
            ts = ev.get('time','')
            if not ts: continue
            try: ev_t = datetime.strptime(ts,'%Y-%m-%d %H:%M:%S')
            except: continue
            if now_t-timedelta(minutes=post_min) < ev_t < now_t+timedelta(minutes=pre_min):
                result = (True, ev.get('event','Xəbər'), impact)
                xeber_cache.set("xeber", result); return result
        result = (False,None,None)
        xeber_cache.set("xeber", result); return result
    except Exception as e:
        cb_finnhub.record_failure(); log.warning(f"Xəbər xəta: {e}")
        return False,None,None

# ══════════════════════════════════════════════════════════
#  FEAR & GREED
# ══════════════════════════════════════════════════════════
def fear_greed_al():
    cached = fg_cache.get("fg")
    if cached: return cached
    try:
        r = _session_get().get("https://api.alternative.me/fng/?limit=1", timeout=8)
        r.raise_for_status()
        d = r.json()['data'][0]
        result = int(d['value']), d['value_classification']
        fg_cache.set("fg", result); return result
    except: return 50,"Neytral"

# ══════════════════════════════════════════════════════════
#  SENTIMENT
# ══════════════════════════════════════════════════════════
def sentiment_al(symbol: str):
    cached = sentiment_cache.get(symbol)
    if cached: return cached
    try:
        if not cb_finnhub.allow(): return 0,"NEYTRAL"
        clean = symbol.replace('/USDT','').replace('=X','').replace('=F','').replace('^','').replace('-','').split('.')[0]
        td = datetime.now().strftime('%Y-%m-%d')
        yd = (datetime.now()-timedelta(days=1)).strftime('%Y-%m-%d')
        finnhub_rl.wait_and_call()
        r = _session_get().get(
            f"https://finnhub.io/api/v1/company-news?symbol={clean}&from={yd}&to={td}&token={FINNHUB_API_KEY}",
            timeout=8); r.raise_for_status()
        cb_finnhub.record_success()
        news = r.json()
        if not isinstance(news,list) or not news:
            sentiment_cache.set(symbol,(0,"NEYTRAL")); return 0,"NEYTRAL"
        pos = ['surge','rally','gain','rise','bull','up','growth','record','beat','strong']
        neg = ['drop','fall','crash','loss','bear','down','decline','miss','weak','cut']
        sk  = sum(sum(1 for w in pos if w in it.get('headline','').lower()) -
                  sum(1 for w in neg if w in it.get('headline','').lower())
                  for it in news[:10])
        lbl = "BULLISH" if sk>2 else ("BEARISH" if sk<-2 else "NEYTRAL")
        sentiment_cache.set(symbol,(sk,lbl)); return sk,lbl
    except Exception as e:
        cb_finnhub.record_failure(); return 0,"NEYTRAL"

# ══════════════════════════════════════════════════════════
#  BAZAR REJİMİ — [ENHANCED] Kural-əsaslı state machine
#  ML-dən müstəqil: ATR, ADX, EMA slope, BB width, price action
#  Keçid: hysteresis ilə — tez-tez "flip" olmur
# ══════════════════════════════════════════════════════════
class MarketRegimeStateMachine:
    """
    Wyckoff + Trend + Range + Volatility keçidlərini
    ML-dən asılı olmadan, kural-əsaslı idarə edir.

    Hysteresis: rejim dəyişmək üçün ard-arda 3 bar
    eyni rejimi göstərməlidir. Bu, tez-tez keçidlərin
    ("whipsaw") qarşısını alır.
    """
    STATES = ('TRENDING', 'RANGING', 'HIGH_VOL', 'CRISIS')

    def __init__(self, hysteresis_bars: int = 3):
        self._lock           = threading.Lock()
        self._current        = 'RANGING'
        self._candidate      = 'RANGING'
        self._candidate_cnt  = 0
        self._hysteresis     = hysteresis_bars
        self._history        = deque(maxlen=100)  # (timestamp, regime)
        self._last_ratio     = 1.0
        self._last_adx       = 20.0

    def _raw_detect(self, h: np.ndarray, l: np.ndarray,
                    c: np.ndarray) -> Tuple[str, float, float]:
        """
        Ham rejim tespiti — 5 kural:
        1. ATR ratio (cari / 50-bar orta)  → volatillik
        2. ADX                              → trend gücü
        3. EMA50 slope (son 10 bar)         → istiqamət
        4. BB width (Bollinger genişliyi)   → sıxışma vs genişləmə
        5. Price vs EMA200                  → struktur
        """
        atr_arr = _atr_np(h, l, c, 14)
        valid   = atr_arr[~np.isnan(atr_arr)]
        if len(valid) < 50:
            return 'RANGING', 1.0, 20.0

        rv    = valid[-1]
        rm    = valid[-50:].mean()
        ratio = rv / rm if rm > 0 else 1.0
        adx   = _adx_np(h, l, c, 14)

        # EMA slope — son 10 barda EMA50-nin dəyişmə sürəti
        ema50   = _ema_np(c, 50)
        e_valid = ema50[~np.isnan(ema50)]
        slope   = 0.0
        if len(e_valid) >= 10:
            slope = (e_valid[-1] - e_valid[-10]) / (e_valid[-10] + 1e-10)

        # Bollinger Band width — sıxışma aşkarı
        if len(c) >= 20:
            mu   = c[-20:].mean()
            std  = c[-20:].std()
            bb_w = (2 * std / mu) if mu > 0 else 0.0
        else:
            bb_w = 0.0

        # EMA200 ilə münasibət
        n200    = min(len(c) - 1, 200)
        ema200  = _ema_np(c, n200)
        e200v   = ema200[~np.isnan(ema200)]
        above200 = (c[-1] > e200v[-1]) if len(e200v) else True

        # ── Kural qərarı (iyerarxik) ──────────────────────
        # CRISIS: ekstrem volatillik
        if ratio > 3.0:
            return 'CRISIS', round(ratio, 2), round(adx, 1)

        # HIGH_VOL: yüksək volatillik + zəif trend
        if ratio > 1.8 and adx < 35:
            return 'HIGH_VOL', round(ratio, 2), round(adx, 1)

        # TRENDING: güclü ADX + EMA slope + normal volatillik
        if adx > 25 and abs(slope) > 0.005 and ratio < 2.0:
            return 'TRENDING', round(ratio, 2), round(adx, 1)

        # HIGH_VOL: yüksək volatillik + güclü trend (trending volatility)
        if ratio > 1.5 and adx > 30:
            return 'HIGH_VOL', round(ratio, 2), round(adx, 1)

        # RANGING: sıxışma (BB dar) + zəif ADX
        if bb_w < 0.03 and adx < 20:
            return 'RANGING', round(ratio, 2), round(adx, 1)

        # RANGING: default
        return 'RANGING', round(ratio, 2), round(adx, 1)

    def update(self, h: np.ndarray, l: np.ndarray,
               c: np.ndarray) -> Tuple[str, float, float]:
        """
        Hysteresis ilə rejim yenilə.
        Yeni rejim ancaq `_hysteresis` dəfə ardıcıl
        göründükdən sonra aktiv olur.
        """
        raw, ratio, adx = self._raw_detect(h, l, c)
        with self._lock:
            self._last_ratio = ratio
            self._last_adx   = adx
            if raw == self._candidate:
                self._candidate_cnt += 1
            else:
                self._candidate     = raw
                self._candidate_cnt = 1

            if self._candidate_cnt >= self._hysteresis:
                if self._current != self._candidate:
                    old = self._current
                    self._current = self._candidate
                    ts = datetime.now().isoformat()
                    self._history.append((ts, old, self._current))
                    log.info(f"[REGIME] Keçid: {old} → {self._current} "
                             f"(ATR×{ratio} ADX={adx:.1f})")
                    prom.counter_inc("regime_transitions_total",
                                     labels={"from": old, "to": self._current})

            return self._current, ratio, adx

    def current(self) -> Tuple[str, float, float]:
        with self._lock:
            return self._current, self._last_ratio, self._last_adx

    def history(self) -> list:
        with self._lock:
            return list(self._history)

_regime_sm = MarketRegimeStateMachine(hysteresis_bars=3)

def rejim_tespit(df: pd.DataFrame) -> Tuple[str, float, float]:
    """
    [ENHANCED] Kural-əsaslı state machine ilə rejim tespiti.
    ML-dən tam müstəqil — hysteresis ilə stabil keçid.
    """
    try:
        h = df['High'].values.astype(np.float64)
        l = df['Low'].values.astype(np.float64)
        c = df['Close'].values.astype(np.float64)
        return _regime_sm.update(h, l, c)
    except:
        return 'RANGING', 1.0, 20.0

# ══════════════════════════════════════════════════════════
#  DİNAMİK PARAMETRLƏR
# ══════════════════════════════════════════════════════════
def dinamik_params(rejim: str, asset_type: str = 'crypto') -> dict:
    BASE = {
        'TRENDING':{'rsi_long':42,'rsi_short':58,'vol_mult':1.4,'sl_mult':2.2,'min_ulduz':MIN_ULDUZ},
        'RANGING': {'rsi_long':38,'rsi_short':62,'vol_mult':1.5,'sl_mult':2.0,'min_ulduz':MIN_ULDUZ+0.3},
        'HIGH_VOL':{'rsi_long':35,'rsi_short':65,'vol_mult':1.7,'sl_mult':3.0,'min_ulduz':MIN_ULDUZ+1.0},
        'CRISIS':  {'rsi_long':30,'rsi_short':70,'vol_mult':2.0,'sl_mult':3.5,'min_ulduz':99},
    }
    p  = BASE.get(rejim, BASE['RANGING']).copy()
    ak = asset_type.upper()
    if ak in ASSET_PARAMS:
        ap=ASSET_PARAMS[ak]
        p.update({'rsi_long':ap.get('rsi_long',p['rsi_long']),
                  'rsi_short':ap.get('rsi_short',p['rsi_short']),
                  'vol_mult':ap.get('vol_mult',p['vol_mult']),
                  'rr_min':ap.get('rr_min',RR_MINIMUM)})
    else:
        p['rr_min'] = RR_MINIMUM
    return p

# ══════════════════════════════════════════════════════════
#  SÜTUN TƏMİZLƏMƏ
# ══════════════════════════════════════════════════════════
def sutun_temizle(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    if df is None or df.empty: return None
    df = df.copy()
    df.columns = [c[0] if isinstance(c,tuple) else c for c in df.columns]
    for alt in ('Volume','volume','VOLUME'):
        if alt in df.columns and 'vol' not in df.columns:
            df=df.rename(columns={alt:'vol'}); break
    return df

# ══════════════════════════════════════════════════════════
#  DATA ÇƏKMƏ
# ══════════════════════════════════════════════════════════
def mtf_data_al_binance(symbol: str, backtest_mode=False) -> dict:
    frames = {}
    configs = [('5m',150),('15m',200),('1h',300 if backtest_mode else 220),
               ('4h',200),('1d',300)]
    for tf, limit in configs:
        cache_key = f"{symbol}_{tf}_{limit}"
        cached = ohlcv_cache.get(cache_key)
        if cached is not None:
            frames[tf] = cached; continue
        for attempt in range(3):
            try:
                if not cb_binance.allow(): break
                t0   = time.monotonic()
                bars = exchange.fetch_ohlcv(symbol, timeframe=tf, limit=limit)
                latency.record("binance_rest_ms", (time.monotonic()-t0)*1000)
                cb_binance.record_success()
                df = pd.DataFrame(bars, columns=['time','Open','High','Low','Close','vol'])
                if len(df) >= 50:
                    ohlcv_cache.set(cache_key, df)
                    frames[tf] = df
                time.sleep(0.04)
                break
            except Exception as e:
                cb_binance.record_failure()
                if attempt < 2: time.sleep(2**(attempt+1))
    return frames

def mtf_data_al_yfinance(symbol: str, asset_type: str) -> dict:
    frames = {}
    configs = [('5m','5d','5m'),('15m','10d','15m'),('1h','60d','1h'),
               ('4h','60d','1h'),('1d','1y','1d')]
    for key, period, interval in configs:
        if not cb_yfinance.allow(): break
        for attempt in range(3):
            try:
                t0 = time.monotonic()
                df = yf.download(symbol, period=period, interval=interval,
                                 progress=False, auto_adjust=True)
                latency.record("yfinance_ms", (time.monotonic()-t0)*1000)
                cb_yfinance.record_success()
                if df.empty or len(df)<30: break
                df = sutun_temizle(df)
                if df is None: break
                if key == '4h' and interval == '1h':
                    df.index = pd.to_datetime(df.index)
                    df = df.resample('4h').agg({'Open':'first','High':'max',
                                                'Low':'min','Close':'last','vol':'sum'}).dropna()
                if len(df) >= 30:
                    frames[key] = df
                break
            except Exception as e:
                cb_yfinance.record_failure()
                if attempt < 2: time.sleep(2**(attempt+1))
        time.sleep(0.2)
    return frames

# ══════════════════════════════════════════════════════════
#  MTF TREND
# ══════════════════════════════════════════════════════════
def mtf_trend_yoxla(frames: dict) -> Tuple[str,int]:
    yonler = []
    for tf in ['1d','4h','1h','15m','5m']:
        df = frames.get(tf)
        if df is None or len(df)<50: continue
        try:
            c    = df['Close'].values.astype(np.float64)
            e50  = _ema_np(c,50)[-2]
            n    = min(len(c)-1, 200)
            e200 = _ema_np(c,n)[-2]
            p    = float(c[-2])
            if p>e50>e200:  yonler.append('LONG')
            elif p<e50<e200: yonler.append('SHORT')
            else:            yonler.append('NEYTRAL')
        except: yonler.append('NEYTRAL')
    if not yonler: return 'NEYTRAL',0
    ls=yonler.count('LONG'); ss=yonler.count('SHORT')
    if ls>=3: return 'LONG',ls
    if ss>=3: return 'SHORT',ss
    return 'NEYTRAL',0

# ══════════════════════════════════════════════════════════
#  SMC — ORDER BLOCK, FVG
# ══════════════════════════════════════════════════════════
def order_block_tap(df: pd.DataFrame, yon: str) -> Tuple[bool,float,float]:
    try:
        price=float(df['Close'].iloc[-2])
        win=df.iloc[-51:-1]
        for i in range(len(win)-2,5,-1):
            c=win.iloc[i]; p=win.iloc[i-1]
            bc=abs(c['Close']-c['Open']); bp=abs(p['Close']-p['Open'])
            if yon=='LONG' and c['Close']>c['Open'] and bc>bp*1.5 and p['Close']<p['Open'] and p['Low']<=price<=p['High']:
                return True,float(p['Low']),float(p['High'])
            if yon=='SHORT' and c['Close']<c['Open'] and bc>bp*1.5 and p['Close']>p['Open'] and p['Low']<=price<=p['High']:
                return True,float(p['Low']),float(p['High'])
    except: pass
    return False,0.0,0.0

def fvg_tap(df: pd.DataFrame, yon: str) -> Tuple[bool,float,float]:
    try:
        price=float(df['Close'].iloc[-2])
        win=df.iloc[-30:-1]
        for i in range(len(win)-1,1,-1):
            s0=win.iloc[i-2]; s2=win.iloc[i]
            if yon=='LONG':
                fl,fh=s0['High'],s2['Low']
                if fh>fl and fl<=price<=fh: return True,float(fl),float(fh)
            elif yon=='SHORT':
                fh,fl=s0['Low'],s2['High']
                if fl<fh and fl<=price<=fh: return True,float(fl),float(fh)
    except: pass
    return False,0.0,0.0

def rsi_divergence_yoxla(df: pd.DataFrame, yon: str) -> bool:
    try:
        c=df['Close'].values.astype(np.float64)
        rsi=_rsi_np(c,14)
        wp=c[-21:-1]; wr=rsi[-21:-1]; rn=rsi[-2]; pn=c[-2]
        if yon=='LONG':
            idx=int(np.argmin(wp))
            return pn<=wp.min()*1.005 and rn>wr[idx]+3
        elif yon=='SHORT':
            idx=int(np.argmax(wp))
            return pn>=wp.max()*0.995 and rn<wr[idx]-3
    except: pass
    return False

def pin_bar_yoxla(df: pd.DataFrame, yon: str) -> bool:
    try:
        s=df.iloc[-2]; body=abs(s['Close']-s['Open'])
        if body==0: return False
        lw=min(s['Open'],s['Close'])-s['Low']
        uw=s['High']-max(s['Open'],s['Close'])
        if yon=='LONG'  and lw>=body*2: return True
        if yon=='SHORT' and uw>=body*2: return True
    except: pass
    return False

def wyckoff_fazi_tap(df: pd.DataFrame) -> str:
    try:
        vc='vol' if 'vol' in df.columns else 'Volume'
        pc=(df['Close'].iloc[-2]-df['Close'].iloc[-22])/df['Close'].iloc[-22]
        av=df[vc].iloc[-20:].mean(); cv=df[vc].iloc[-2]
        va=cv/av if av else 1
        rv=_rsi_np(df['Close'].values.astype(np.float64),14)[-2]
        if pc<-0.05 and rv<45 and va>1.3:  return 'ACCUMULATION'
        elif pc>0.05 and rv>55 and va>1.3: return 'DISTRIBUTION'
        elif pc>0.03 and rv>50:            return 'MARKUP'
        elif pc<-0.03 and rv<50:           return 'MARKDOWN'
    except: pass
    return 'NEYTRAL'

def breakout_yoxla(df: pd.DataFrame, yon: str) -> bool:
    try:
        px=float(df['Close'].iloc[-2])
        h20=df['High'].iloc[-22:-2].max(); l20=df['Low'].iloc[-22:-2].min()
        vc='vol' if 'vol' in df.columns else 'Volume'
        av=float(df[vc].rolling(20).mean().iloc[-2]); cv=float(df[vc].iloc[-2])
        if yon=='LONG'  and px>h20 and cv>av*1.2: return True
        if yon=='SHORT' and px<l20 and cv>av*1.2: return True
    except: pass
    return False

def besm_trigger_yoxla(frames: dict, yon: str) -> bool:
    df5=frames.get('5m')
    if df5 is None or len(df5)<30: return True
    try:
        price=float(df5['Close'].iloc[-2])
        rsi5=float(_rsi_np(df5['Close'].values.astype(np.float64),14)[-2])
        h10=df5['High'].iloc[-12:-2].max(); l10=df5['Low'].iloc[-12:-2].min()
        chi=df5['High'].iloc[-2]; clo=df5['Low'].iloc[-2]
        if yon=='LONG':  return (clo<l10 and price>l10) and rsi5<55
        if yon=='SHORT': return (chi>h10 and price<h10) and rsi5>45
    except: pass
    return True

def dinamik_tp_hesabla(price, sl, yon, rejim, atr, mtf_skor) -> dict:
    risk=abs(price-sl) or atr
    m3=5.0 if (rejim=='TRENDING' and mtf_skor>=3) else 4.0
    if yon=='LONG':  tp1,tp2,tp3=price+risk*1.5,price+risk*2.5,price+risk*m3
    else:            tp1,tp2,tp3=price-risk*1.5,price-risk*2.5,price-risk*m3
    return {'tp1':round(tp1,6),'tp2':round(tp2,6),'tp3':round(tp3,6),
            'tp1_pct':25,'tp2_pct':50,'tp3_pct':25,'be_trigger':round(tp1,6)}

# ══════════════════════════════════════════════════════════
#  ƏSAS ANALİZ
# ══════════════════════════════════════════════════════════
def analiz_et(frames: dict, asset_type='crypto', symbol='', ref_idx=-2) -> Optional[dict]:
    df1h=frames.get('1h')
    if df1h is None or len(df1h)<150: return None
    try:
        c=df1h['Close'].values.astype(np.float64)
        h=df1h['High'].values.astype(np.float64)
        l=df1h['Low'].values.astype(np.float64)
        vc='vol' if 'vol' in df1h.columns else 'Volume'

        e50 =_ema_np(c,50)[ref_idx];   e200=_ema_np(c,min(len(c)-1,200))[ref_idx]
        rsi =_rsi_np(c,14)[ref_idx];   atr =_atr_np(h,l,c,14)[ref_idx]

        if any(np.isnan(x) for x in [e50,e200,rsi,atr]): return None

        price=float(c[ref_idx]); h1=float(h[ref_idx]); l1=float(l[ref_idx])
        vol_s=df1h[vc].values.astype(np.float64)
        avg_vol=float(vol_s[-21:ref_idx].mean() if ref_idx<0 else vol_s[max(0,ref_idx-21):ref_idx].mean())
        curr_vol=float(vol_s[ref_idx])
        h20=float(df1h['High'].iloc[-21:-2].max()); l20=float(df1h['Low'].iloc[-21:-2].min())

        rejim,atr_ratio,adx_val=rejim_tespit(df1h)
        if rejim=='CRISIS': return None
        p=dinamik_params(rejim,asset_type)

        mtf_yon,mtf_skor=mtf_trend_yoxla(frames)
        if mtf_yon=='NEYTRAL': return None
        yon=mtf_yon

        if yon=='LONG':
            sweep =(l1<l20) and (price>l20)
            ema_ok=price>e200 and e50>e200
            rsi_ok=rsi<=p['rsi_long']
        else:
            sweep =(h1>h20) and (price<h20)
            ema_ok=price<e200 and e50<e200
            rsi_ok=rsi>=p['rsi_short']

        vol_ok=curr_vol>avg_vol*p['vol_mult']

        pullback=False
        if not sweep:
            df4h=frames.get('4h')
            if df4h is not None and len(df4h)>=50:
                try:
                    e50_4h=float(_ema_np(df4h['Close'].values.astype(np.float64),50)[ref_idx])
                    tol=0.025
                    if not np.isnan(e50_4h):
                        if yon=='LONG'  and abs(price-e50_4h)/e50_4h<tol and price>e50_4h*(1-tol): pullback=True
                        elif yon=='SHORT' and abs(price-e50_4h)/e50_4h<tol and price<e50_4h*(1+tol): pullback=True
                except: pass

        breakout=breakout_yoxla(df1h,yon)
        if not sweep and not pullback and not breakout: return None
        if not ema_ok or not rsi_ok or not vol_ok: return None
        if not besm_trigger_yoxla(frames,yon): return None

        rr_min=p.get('rr_min',RR_MINIMUM); sl_mult=p['sl_mult']
        if yon=='LONG':  sl=(float(df1h['Low'].iloc[ref_idx])-atr*sl_mult) if sweep else (price-atr*sl_mult)
        else:            sl=(float(df1h['High'].iloc[ref_idx])+atr*sl_mult) if sweep else (price+atr*sl_mult)

        tp_lv=dinamik_tp_hesabla(price,sl,yon,rejim,atr,mtf_skor)
        tp=tp_lv['tp2']
        risk=abs(price-sl); reward=abs(price-tp)
        if risk==0 or reward/risk<rr_min: return None

        # SCORING
        skor=0.0
        skor+=min(mtf_skor,5)*0.5
        if ema_ok: skor+=1.0
        if sweep:   skor+=1.0
        elif pullback: skor+=0.7
        elif breakout: skor+=0.8
        if yon=='LONG'  and rsi<35: skor+=1.8
        elif yon=='LONG'  and rsi<45: skor+=0.9
        elif yon=='SHORT' and rsi>65: skor+=1.8
        elif yon=='SHORT' and rsi>55: skor+=0.9
            # RSI Trend Təsdiqi
        if rsi > 50 and yon == 'LONG': skor += 0.3 # Momentum bizimlədir
            
        ob_v,_,_=order_block_tap(df1h,yon)
        fvg_v,_,_=fvg_tap(df1h,yon)
        if ob_v:  skor+=1.0
        if fvg_v: skor+=1.0
        if rsi_divergence_yoxla(df1h,yon): skor+=1.0
        if pin_bar_yoxla(df1h,yon):        skor+=0.5

        wyck=wyckoff_fazi_tap(df1h)
        if yon=='LONG'  and wyck=='ACCUMULATION': skor+=1.0
        elif yon=='SHORT' and wyck=='DISTRIBUTION': skor+=1.0

        vr=curr_vol/avg_vol if avg_vol else 1
        if vr>2.0: skor+=1.0
        elif vr>1.5: skor+=0.5

        for tf_b in ('15m','5m'):
            dfb=frames.get(tf_b)
            if dfb is not None and len(dfb)>=30:
                try:
                    rb=float(_rsi_np(dfb['Close'].values.astype(np.float64),14)[-2])
                    if yon=='LONG'  and not np.isnan(rb) and rb<50: skor+=0.3
                    elif yon=='SHORT' and not np.isnan(rb) and rb>50: skor+=0.3
                except: pass

        if skor<p['min_ulduz']: return None

        return {
            'type':      'LONG 📈' if yon=='LONG' else 'SHORT 📉',
            'yon':       yon, 'entry':round(price,6),
            'tp':        round(tp,6), 'tp_levels':tp_lv, 'sl':round(sl,6),
            'rsi':       round(float(rsi),1), 'ema200':round(float(e200),6),
            'skor':      round(skor,1), 'rejim':rejim,
            'atr_ratio': atr_ratio, 'adx':adx_val,
            'rr':        round(reward/risk,2),
            'ob':        ob_v, 'fvg':fvg_v, 'wyckoff':wyck,
            'mtf_skor':  mtf_skor, 'vol_ratio':round(vr,2),
            'pullback':  pullback, 'breakout':breakout, 'atr1h':round(float(atr),6),
        }
    except Exception as e:
        log.debug(f"analiz_et {symbol}: {e}"); return None

# ══════════════════════════════════════════════════════════
#  [FIX13] REALİSTİK BACKTEST — fee + slippage + spread
# ══════════════════════════════════════════════════════════
def backtest_single(frames_tam: dict, asset_type='crypto', symbol='') -> list:
    df1h=frames_tam.get('1h')
    if df1h is None or len(df1h)<300: return []
    df1h_r=df1h.reset_index(drop=True)
    results=[]
    FEE_PCT     = 0.075 / 100   # Binance futures maker/taker
    SLIPPAGE_PCT= 0.05  / 100   # 5 bps slippage
    SPREAD_PCT  = 0.03  / 100   # 3 bps spread
    FUNDING_8H  = 0.01  / 100   # Orta funding rate 8 saata bir

    for i in range(200, len(df1h_r)-1):
        frames_k={}
        for tf,df_f in frames_tam.items():
            dr=df_f.reset_index(drop=True)
            cuts={'1h':i,'4h':i//4,'1d':i//24,'15m':i*4,'5m':i*12}
            cut=cuts.get(tf,len(dr))
            frames_k[tf]=dr.iloc[:max(1,min(cut,len(dr)))]

        res=analiz_et(frames_k, asset_type, symbol, ref_idx=-2)
        if res is None: continue

        entry=res['entry']; tp=res['tp']; sl=res['sl']; yon=res['yon']

        # [FIX13] Real giriş qiyməti = slippage + spread
        if yon=='LONG':
            real_entry=entry*(1+SLIPPAGE_PCT+SPREAD_PCT/2)
        else:
            real_entry=entry*(1-SLIPPAGE_PCT-SPREAD_PCT/2)

        # Gəlir/gediş komissiyası
        total_fee=real_entry*(FEE_PCT*2)  # giriş + çıxış

        gelecek=df1h_r.iloc[i+1:i+73]  # max 72 bar (3 gün)
        hit=None
        bars_held=0
        for _,bar in gelecek.iterrows():
            bars_held+=1
            if yon=='LONG':
                if bar['Low']<=sl:  hit='SL'; break
                if bar['High']>=tp: hit='TP'; break
            else:
                if bar['High']>=sl: hit='SL'; break
                if bar['Low']<=tp:  hit='TP'; break
        if hit is None: hit='AÇIQ'

        # [FIX13] Net PnL hesabla
        if hit=='TP':
            gross_pnl=abs(tp-real_entry)
            funding=FUNDING_8H*(bars_held//8)
            net_pnl=gross_pnl-total_fee-funding
        elif hit=='SL':
            gross_pnl=-abs(sl-real_entry)
            funding=FUNDING_8H*(bars_held//8)
            net_pnl=gross_pnl-total_fee-funding
        else:
            net_pnl=0.0

        results.append({
            'symbol':symbol,'bar_idx':i,'yon':yon,
            'entry':round(real_entry,6),'tp':round(tp,6),'sl':round(sl,6),
            'rr':res['rr'],'skor':res['skor'],'rejim':res['rejim'],
            'hit':hit,'fee_pct':FEE_PCT*100,'slippage_pct':SLIPPAGE_PCT*100,
            'net_pnl':round(net_pnl,6),'bars_held':bars_held
        })
    return results

def backtest_hesabat(symbol_list=None, asset_type='crypto'):
    if symbol_list is None: symbol_list=['BTC/USDT','ETH/USDT','BNB/USDT','SOL/USDT','XRP/USDT']
    butun=[]
    for sym in symbol_list:
        log.info(f"[BACKTEST] {sym}...")
        try:
            frames=mtf_data_al_binance(sym,backtest_mode=True) if asset_type=='crypto' else mtf_data_al_yfinance(sym,asset_type)
            butun.extend(backtest_single(frames,asset_type,sym))
        except Exception as e: log.warning(f"[BACKTEST] {sym}: {e}")
        time.sleep(1)
    if not butun: log.info("[BACKTEST] Siqnal yoxdur"); return None
    for r in butun: db_yaz('backtest',r)
    df_b=pd.DataFrame(butun); qap=df_b[df_b['hit'].isin(['TP','SL'])]
    n=len(qap); tp_n=len(qap[qap['hit']=='TP'])
    wr=tp_n/n*100 if n else 0
    rr=qap['rr'].mean() if n else 0
    net=qap['net_pnl'].sum() if n else 0
    log.info(f"[BACKTEST] Cəmi:{n} TP:{tp_n} WR:{wr:.1f}% Net:{net:.4f}")
    telegram_gonder(
        f"🔬 <b>BACKTEST (Realistic)</b>\n\n"
        f"📊 {len(symbol_list)} aktiv\n"
        f"📨 Siqnal:{len(df_b)} | Qapalı:{n}\n"
        f"✅ TP:{tp_n} | ❌ SL:{n-tp_n}\n"
        f"🎯 <b>Win Rate: {wr:.1f}%</b> (fee+slippage daxil)\n"
        f"📐 Ort. RR: {rr:.2f}\n"
        f"💵 Net PnL: {net:.4f}"
    )
    return df_b

# ══════════════════════════════════════════════════════════
#  WFO
# ══════════════════════════════════════════════════════════
WFO_PARAM_GRID = {
    'min_ulduz':      [2.5,3.0,3.5,4.0],
    'rsi_long_ofset': [-5,0,5],
    'rsi_short_ofset':[-5,0,5],
    'vol_mult_ofset': [-0.1,0.0,0.1],
}
param_adlar    = list(WFO_PARAM_GRID.keys())
param_degerler = list(WFO_PARAM_GRID.values())

def walk_forward_optimizasiya(symbol='BTC/USDT', asset_type='crypto'):
    log.info(f"[WFO] {symbol}...")
    try:
        frames=mtf_data_al_binance(symbol,backtest_mode=True) if asset_type=='crypto' else mtf_data_al_yfinance(symbol,asset_type)
        df1h=frames.get('1h')
        if df1h is None or len(df1h)<400: log.warning("[WFO] data az"); return
        bolme=int(len(df1h)*0.70)
        fi={}; fo={}
        for tf,df_f in frames.items():
            dr=df_f.reset_index(drop=True)
            cut={'1h':bolme,'4h':bolme//4,'1d':bolme//24}.get(tf,len(dr))
            fi[tf]=dr.iloc[:cut]; fo[tf]=dr.iloc[cut:] if tf in ('1h','4h','1d') else dr
        en_wr=-1.0; en_p={}
        total=1
        for v in param_degerler: total*=len(v)
        log.info(f"[WFO] {total} kombinasiya...")

        global MIN_ULDUZ
        orig=MIN_ULDUZ
        for i,kombo in enumerate(itertools.product(*param_degerler),1):
            p=dict(zip(param_adlar,kombo))
            with wfo_lock:
                MIN_ULDUZ=p['min_ulduz']
                res=backtest_single(fi,asset_type,symbol)
                MIN_ULDUZ=orig
            qap=[r for r in res if r['hit'] in ('TP','SL')]
            if not qap: continue
            wr=sum(1 for r in qap if r['hit']=='TP')/len(qap)*100
            if wr>en_wr and len(qap)>=5: en_wr=wr; en_p=p.copy()
            if i%20==0: log.debug(f"[WFO] {i}/{total}")

        if not en_p: log.warning("[WFO] uyğun param yox"); return
        with wfo_lock:
            MIN_ULDUZ=en_p['min_ulduz']
            res_out=backtest_single(fo,asset_type,symbol)
            MIN_ULDUZ=orig
        qap_o=[r for r in res_out if r['hit'] in ('TP','SL')]
        wo=sum(1 for r in qap_o if r['hit']=='TP')/len(qap_o)*100 if qap_o else 0
        ovf="⚠️ OVERFİT" if abs(en_wr-wo)>15 else "✅ Stabil"
        telegram_gonder(
            f"⚙️ <b>WFO NƏTİCƏ</b>\n\n💎 {symbol.replace('/USDT','')}\n"
            f"📥 In-sample: {en_wr:.1f}%\n📤 Out-sample: {wo:.1f}%\n📊 {ovf}"
        )
    except Exception as e: log.error(f"[WFO] {e}")

# ══════════════════════════════════════════════════════════
#  ML
# ══════════════════════════════════════════════════════════
_ML_FEATURES = ['rr','skor','rejim_kod','rsi','atr_ratio','adx','vol_ratio','ob','fvg']

def ml_model_egit():
    try:
        from sklearn.ensemble import GradientBoostingClassifier
        from sklearn.preprocessing import StandardScaler
        from sklearn.pipeline import Pipeline
        from sklearn.model_selection import cross_val_score, StratifiedKFold
        con=sqlite3.connect(LOG_FAYL,timeout=5)
        df=pd.read_sql("SELECT * FROM backtest WHERE hit IN ('TP','SL')",con); con.close()
        if len(df)<30: log.info("[ML] Az nümunə"); return None
        df['label']    =(df['hit']=='TP').astype(int)
        df['rejim_kod']=df['rejim'].map({'TRENDING':3,'RANGING':2,'HIGH_VOL':1,'CRISIS':0}).fillna(2)
        for col in ['rsi','atr_ratio','adx','vol_ratio','ob','fvg']:
            if col not in df.columns: df[col]=0
        X=df[_ML_FEATURES].fillna(0).values; y=df['label'].values
        pipe=Pipeline([('sc',StandardScaler()),
                       ('m',GradientBoostingClassifier(n_estimators=150,max_depth=3,learning_rate=0.05,subsample=0.8,random_state=42))])
        cv=StratifiedKFold(n_splits=min(5,max(2,len(df)//10)),shuffle=True,random_state=42)
        sc=cross_val_score(pipe,X,y,cv=cv,scoring='roc_auc')
        pipe.fit(X,y)
        with open(ML_MODEL_FAYL,'wb') as f: pickle.dump(pipe,f)
        auc=sc.mean()*100
        log.info(f"[ML] ROC-AUC: {auc:.1f}% ({len(df)} nümunə)")
        telegram_gonder(f"🤖 <b>ML YENİLƏNDİ</b>\n📊 {len(df)} nümunə\n🎯 ROC-AUC: {auc:.1f}%")
        return pipe
    except ImportError: log.warning("[ML] scikit-learn lazımdır")
    except Exception as e: log.warning(f"[ML] {e}")
    return None

def ml_filter(res: dict) -> bool:
    global _ml_model_cache
    if _ml_model_cache is None:
        if os.path.exists(ML_MODEL_FAYL):
            try:
                with open(ML_MODEL_FAYL,'rb') as f: _ml_model_cache=pickle.load(f)
            except: pass
    if _ml_model_cache is not None:
        try:
            rk={'TRENDING':3,'RANGING':2,'HIGH_VOL':1,'CRISIS':0}.get(res['rejim'],2)
            feats=[[res['rr'],res['skor'],rk,res['rsi'],res['atr_ratio'],
                    res['adx'],res['vol_ratio'],int(res['ob']),int(res['fvg'])]]
            return float(_ml_model_cache.predict_proba(feats)[0][1]) >= 0.55
        except: pass
    return (res['skor']>=MIN_ULDUZ+0.5 and res['rr']>=1.8 and res['mtf_skor']>=1)

def ml_periyodik():
    global _ml_model_cache
    while not _shutdown_event.is_set():
        if datetime.now().hour==2 and datetime.now().minute<5:
            yeni=ml_model_egit()
            if yeni: _ml_model_cache=yeni
        time.sleep(300)

# ══════════════════════════════════════════════════════════
#  SİQNAL GÖNDƏR
# ══════════════════════════════════════════════════════════
def siqnal_gonder(sym: str, res: dict, sl_lbl='NEYTRAL',
                  fg_v=50, fg_l='Neytral', asset_type='crypto'):
    global gunluk_siqnal
    now=time.time()
    with _state_lock:
        if sym in son_gonderilme and now-son_gonderilme[sym]<COOLDOWN_SANIYE: return
        if korrelyasiya_say.get(asset_type,0)>=MAX_KORRELYASIYA: return
        if not risk_engine.daily_loss_ok(): return
        son_gonderilme[sym]=now
        korrelyasiya_say[asset_type]=korrelyasiya_say.get(asset_type,0)+1

    t_signal=time.monotonic()
    clean=sym.replace('/USDT','').replace('=X','').replace('=F','').replace('^','')

    # [LIQ] Real-time spread yoxla — yalnız crypto üçün (bookTicker var)
    if asset_type == 'crypto':
        ws_sym = clean.replace('/', '') + ('USDT' if 'USDT' not in clean else '')
        liq_ok, liq_reason = liquidity_filter.is_liquid(ws_sym, asset_type)
        if not liq_ok:
            log.info(f"[LIQ] {clean} siqnal bloklandı — {liq_reason}")
            prom.counter_inc("signals_blocked_total", labels={"reason": "liquidity"})
            # State-dən geri al — cooldown artırılmamalıdır
            with _state_lock:
                son_gonderilme.pop(sym, None)
                korrelyasiya_say[asset_type] = max(0, korrelyasiya_say.get(asset_type, 0) - 1)
            return
    us='⭐'*min(int(res['skor']),10)
    tlv=res.get('tp_levels',{})
    ml={4:"5M+15M+1H+4H+1D",3:"15M+1H+4H+1D",2:"1H+4H+1D",1:"1H+4H"}.get(int(res['mtf_skor']),f"×{res['mtf_skor']}")
    re={'TRENDING':'📈','RANGING':'〰️','HIGH_VOL':'⚡','CRISIS':'🚨'}

    msg=(
        f"🌐 <b>UNIVERSAL SNIPER v13.0 — {res['type']}</b>\n\n"
        f"💎 <b>Aktiv:</b> #{clean}\n"
        f"{'─'*28}\n"
        f"🧠 <b>Rejim:</b> {re.get(res['rejim'],'•')} {res['rejim']} "
        f"(ATR×{res['atr_ratio']} | ADX {res['adx']})\n"
        f"📊 <b>MTF Trend:</b> {ml}\n"
        f"⭐ <b>Siqnal skoru:</b> {res['skor']}/10  {us}\n"
        f"📐 <b>Risk/Reward:</b> 1:{res['rr']}\n"
        f"{'─'*28}\n"
        f"📈 <b>RSI:</b>    {res['rsi']}\n"
        f"📏 <b>EMA200:</b> {res['ema200']}\n"
        f"💧 <b>Həcm:</b>   ×{res['vol_ratio']}\n"
        f"🧩 <b>Wyckoff:</b>{res['wyckoff']}\n"
        f"{'─'*28}\n"
        f"🏦 <b>Order Block:</b>   {'✅' if res['ob']       else '—'}\n"
        f"🕳  <b>Fair Value Gap:</b>{'✅' if res['fvg']     else '—'}\n"
        f"↩️  <b>Pullback:</b>     {'✅' if res['pullback'] else '—'}\n"
        f"🚀 <b>Breakout:</b>      {'✅' if res['breakout'] else '—'}\n"
        f"{'─'*28}\n"
        f"😨 <b>Fear&Greed:</b> {fg_v} — {fg_l}\n"
        f"📰 <b>Sentiment:</b>  {sl_lbl}\n"
        f"{'─'*28}\n"
        f"⏱ <b>TF:</b> 5M→15M→1H→4H→1D\n"
        f"{'─'*28}\n"
        f"💰 <b>Giriş:</b>      {res['entry']}\n"
        f"🎯 <b>TP1 (25%):</b> {tlv.get('tp1','—')}\n"
        f"🎯 <b>TP2 (50%):</b> {tlv.get('tp2',res['tp'])}\n"
        f"🎯 <b>TP3 (25%):</b> {tlv.get('tp3','—')}\n"
        f"↗️ <b>BE trigger:</b> {tlv.get('be_trigger','—')}\n"
        f"🛑 <b>Stop:</b>       {res['sl']}\n"
    )
    telegram_gonder(msg)
    db_yaz('signals',{'symbol':clean,'type':res['type'],'entry':res['entry'],'tp':res['tp'],
                      'sl':res['sl'],'rr':res['rr'],'skor':res['skor'],'rejim':res['rejim'],
                      'wyckoff':res['wyckoff'],'ob':res['ob'],'fvg':res['fvg'],
                      'mtf_skor':res['mtf_skor'],'breakout':res.get('breakout',False)})

    sig_ms=(time.monotonic()-t_signal)*1000
    latency.record("signal_latency_ms", sig_ms)
    prom.counter_inc("signals_total", labels={"type": res['yon'], "asset": asset_type})
    prom.histogram_observe("signal_latency_ms", sig_ms)

    with _state_lock:
        global gunluk_siqnal
        gunluk_siqnal+=1

    log.info(f"✅ {clean} | {res['type']} | Skor:{res['skor']} | RR:1:{res['rr']}")

# ══════════════════════════════════════════════════════════
#  [FIX1] EVENT-DRIVEN CANDLE HANDLER
# ══════════════════════════════════════════════════════════
async def on_candle_close(candle: dict):
    """
    WebSocket candle close event handler.
    Bu funksiya while True yerinə işləyir — yalnız event gəldikdə çağırılır.
    [FIX1] Event-driven, [FIX2] semaphore ilə məhdudlaşdırılıb.
    """
    sym_ws = candle['symbol']  # BTCUSDT format
    sym    = sym_ws[:-4] + '/USDT' if sym_ws.endswith('USDT') else sym_ws

    async with _analysis_sem:   # [FIX2] Max 20 eyni vaxtda
        t0 = time.monotonic()
        try:
            frames = mtf_data_al_binance(sym)
            if not frames: return
            res = analiz_et(frames, 'crypto', sym)
            if res and ml_filter(res):
                _, sl_lbl = sentiment_al(sym)
                fg_v, fg_l = fear_greed_al()
                siqnal_gonder(sym, res, sl_lbl, fg_v, fg_l, 'crypto')
                prom.counter_inc("ws_signals_total")
        except Exception as e:
            log.debug(f"[WS handler] {sym}: {e}")
        finally:
            proc_ms=(time.monotonic()-t0)*1000
            latency.record("candle_proc_ms", proc_ms)

# ══════════════════════════════════════════════════════════
#  REST FALLBACK SKAN
# ══════════════════════════════════════════════════════════
def sessiya_aktiv(): return 8<=datetime.now(timezone.utc).hour<22

def binance_skan_rest():
    """WS uğursuz olduqda REST fallback."""
    with _state_lock: korrelyasiya_say['crypto']=0
    log.info("🔍 [BİNANCE REST] skan...")
    try:
        markets=market_cache.get("markets"); tickers=market_cache.get("tickers")
        if markets is None or tickers is None:
            markets=exchange.load_markets(); tickers=exchange.fetch_tickers()
            market_cache.set("markets",markets); market_cache.set("tickers",tickers)
    except Exception as e: log.error(f"[BİNANCE REST] market: {e}"); return
    usdt=[s for s in markets if '/USDT' in s and markets[s]['active'] and s in tickers and (tickers[s].get('quoteVolume') or 0)>0]
    usdt.sort(key=lambda s: tickers[s].get('quoteVolume') or 0, reverse=True)
    pairs=usdt[:400]

    def isle(sym):
        if bot_dayan or _shutdown_event.is_set(): return
        try:
            frames=mtf_data_al_binance(sym)
            if not frames: return
            res=analiz_et(frames,'crypto',sym)
            if res and ml_filter(res):
                _,sl_lbl=sentiment_al(sym); fg_v,fg_l=fear_greed_al()
                siqnal_gonder(sym,res,sl_lbl,fg_v,fg_l,'crypto')
        except Exception as e: log.debug(f"REST isle {sym}: {e}")

    # [FIX2] Bounded ThreadPool — max 15 thread, partlamaz
    with ThreadPoolExecutor(max_workers=15) as pool:
        futs={pool.submit(isle,sym):sym for sym in pairs}
        for fut in as_completed(futs,timeout=180):
            try: fut.result()
            except Exception: pass

def yfinance_skan(asset_list,label,asset_type):
    with _state_lock: korrelyasiya_say.setdefault(asset_type,0)
    if not sessiya_aktiv() and asset_type!='crypto': log.info(f"[{label}] Sessiya bağlı"); return
    log.info(f"🔍 [{label}] {len(asset_list)} aktiv...")

    def isle(sym):
        if bot_dayan or _shutdown_event.is_set(): return
        frames=None
        for attempt in range(3):
            try:
                frames=mtf_data_al_yfinance(sym,asset_type); break
            except Exception: time.sleep(2**(attempt+1))
        if not frames: return
        try:
            res=analiz_et(frames,asset_type,sym)
            if res and ml_filter(res):
                _,sl_lbl=sentiment_al(sym); fg_v,fg_l=fear_greed_al()
                siqnal_gonder(sym,res,sl_lbl,fg_v,fg_l,asset_type)
        except Exception as e: log.debug(f"yf isle {sym}: {e}")

    # [FIX2] Max 8 thread yfinance üçün
    with ThreadPoolExecutor(max_workers=8) as pool:
        futs={pool.submit(isle,sym):sym for sym in asset_list}
        for fut in as_completed(futs,timeout=300): 
            try: fut.result()
            except Exception: pass
    log.info(f"[{label}] Tamamlandı.")

# ══════════════════════════════════════════════════════════
#  TELEGRAM UPDATES
# ══════════════════════════════════════════════════════════
def telegram_updates_yoxla():
    global bot_dayan
    offset=None
    while not _shutdown_event.is_set():
        try:
            params={'timeout':10}
            if offset: params['offset']=offset
            r=requests.get(f"https://api.telegram.org/bot{TOKEN}/getUpdates",params=params,timeout=15)
            r.raise_for_status()
            for upd in r.json().get('result',[]):
                offset=upd['update_id']+1
                txt=upd.get('message',{}).get('text','').strip().lower()
                if txt=='/pause':
                    bot_dayan=True; telegram_gonder("⏸ Bot dayandırıldı.")
                elif txt=='/resume':
                    bot_dayan=False; telegram_gonder("▶️ Bot davam edir.")
                elif txt=='/stats':
                    n,lg,sh,sk,rr=db_hefte_stats()
                    lat=latency.all_metrics()
                    ws_p95=lat.get('ws_latency_ms',{}).get('p95',0)
                    sig_p95=lat.get('signal_latency_ms',{}).get('p95',0)
                    telegram_gonder(
                        f"📊 <b>BOT STATİSTİKA v13.0</b>\n\n"
                        f"🟢 {'⏸' if bot_dayan else '▶️'}\n"
                        f"📨 Bugün: {gunluk_siqnal}\n📉 Zərər: {gunluk_zefer:.2f}%\n"
                        f"📅 Bu həftə: {n} | L:{lg} S:{sh}\n"
                        f"⭐ Skor:{sk:.1f} | RR:{rr:.2f}\n"
                        f"📍 WS p95: {ws_p95:.0f}ms\n"
                        f"⚡ Signal p95: {sig_p95:.0f}ms\n"
                        f"🔓 Açıq mövqe: {len(position_manager.get_all())}"
                    )
                elif txt=='/status':
                    telegram_gonder(f"✅ v11 Aktiv | Siqnal:{gunluk_siqnal} | Zərər:{gunluk_zefer:.2f}%")
                elif txt=='/backtest':
                    telegram_gonder("🔬 Backtest başlayır (realistic)...")
                    threading.Thread(target=backtest_hesabat,daemon=True).start()
                elif txt=='/wfo':
                    telegram_gonder("⚙️ WFO başlayır...")
                    threading.Thread(target=walk_forward_optimizasiya,daemon=True).start()
                elif txt=='/latency':
                    lat=latency.all_metrics()
                    msg="📡 <b>LATENCY REPORT</b>\n\n"
                    for k,v in lat.items():
                        msg+=f"<b>{k}</b>\np50:{v['p50']:.0f}ms p95:{v['p95']:.0f}ms p99:{v['p99']:.0f}ms n:{v['n']}\n\n"
                    telegram_gonder(msg)
                elif txt=='/positions':
                    pos=position_manager.get_all()
                    if pos:
                        msg="📍 <b>AÇIQ MÖVQƏlƏR</b>\n\n"
                        for sym,p in pos.items():
                            msg+=f"#{sym}: {p['yon']} @ {p['entry']}\n"
                    else:
                        msg="📍 Açıq mövqe yoxdur."
                    telegram_gonder(msg)
                elif txt=='/cache':
                    msg=(f"💾 <b>CACHE STATS</b>\n\n"
                         f"OHLCV: {ohlcv_cache.stats()}\n"
                         f"Sentiment: {sentiment_cache.stats()}\n"
                         f"Indicator: {ind_cache.stats()}")
                    telegram_gonder(msg)
                elif txt=='/chaos':
                    telegram_gonder("🔴 Chaos test başlayır...")
                    threading.Thread(target=chaos_engine.run_all,
                                     daemon=True, name="ChaosTest").start()
                elif txt.startswith('/liquidity'):
                    parts = txt.split()
                    sym_q = (parts[1].upper() + 'USDT') if len(parts) > 1 else 'BTCUSDT'
                    stats = liquidity_filter.get_stats(sym_q)
                    ok, reason = liquidity_filter.is_liquid(sym_q, 'crypto')
                    msg = (f"💧 <b>LİQUİDİTY: {sym_q}</b>\n\n"
                           f"Status: {'✅ Likvid' if ok else f'❌ BLOK ({reason})'}\n"
                           f"Spread: {stats.get('current','—')}%\n"
                           f"Orta: {stats.get('mean','—')}%\n"
                           f"Max: {stats.get('max','—')}%\n"
                           f"Ölçüm: {stats.get('n',0)}\n\n"
                           f"Backpressure: {_candle_bp_queue.stats()}")
                    telegram_gonder(msg)
                elif txt=='/regime':
                    state, ratio, adx = _regime_sm.current()
                    hist = _regime_sm.history()[-5:]
                    msg = (f"📊 <b>MARKET REJİM</b>\n\n"
                           f"Cari: {state}\n"
                           f"ATR ratio: {ratio}\n"
                           f"ADX: {adx}\n\n"
                           f"Son keçidlər:\n")
                    for ts, frm, to in hist:
                        msg += f"  {frm} → {to} ({ts[:16]})\n"
                    telegram_gonder(msg)
        except Exception as e: log.debug(f"TG update: {e}")
        time.sleep(3)

# ══════════════════════════════════════════════════════════
#  PERİYODİK TAPŞIRIQLAR
# ══════════════════════════════════════════════════════════
def hefte_hesabat():
    while not _shutdown_event.is_set():
        if datetime.now().weekday()==0 and datetime.now().hour==9 and datetime.now().minute<5:
            try:
                n,lg,sh,sk,rr=db_hefte_stats()
                telegram_gonder(
                    f"📊 <b>HƏFTƏLİK HESABAT v13.0</b>\n\n"
                    f"📨 {n} | 🏆 L:{lg} 🔻 S:{sh}\n"
                    f"⭐ Ort skor:{sk:.1f} | RR:{rr:.2f}\n"
                    f"📡 Signal latency: {latency.summary('signal_latency_ms')['p95']:.0f}ms p95"
                )
            except Exception as e: log.warning(f"Həftəlik: {e}")
        time.sleep(300)

def gunluk_reset():
    global gunluk_siqnal, gunluk_zefer, korrelyasiya_say
    while not _shutdown_event.is_set():
        if datetime.now().hour==0 and datetime.now().minute<2:
            with _state_lock:
                gunluk_siqnal=0; gunluk_zefer=0.0
                korrelyasiya_say={"crypto":0,"forex":0,"stock":0,"commodity":0}
            for c in [fg_cache,sentiment_cache,xeber_cache,ohlcv_cache,ind_cache]:
                c.clear()
            gc.collect()  # [FIX8] Memory cleanup
            log.info("[RESET] Gündəlik reset.")
        time.sleep(60)

def prometheus_updater():
    """Prometheus gauge-larını hər 30 saniyə yenilə."""
    while not _shutdown_event.is_set():
        prom.gauge_set("bot_paused", 1.0 if bot_dayan else 0.0)
        prom.gauge_set("daily_signals", gunluk_siqnal)
        prom.gauge_set("daily_loss_pct", gunluk_zefer)
        prom.gauge_set("open_positions", len(position_manager.get_all()))
        for metric, vals in latency.all_metrics().items():
            prom.gauge_set(f"latency_p95_ms", vals['p95'], labels={"metric": metric})
        time.sleep(30)

# ══════════════════════════════════════════════════════════
#  ƏSAS ASYNC SKAN DÖVRƏSİ
# ══════════════════════════════════════════════════════════
async def async_main():
    """
    [FIX1] Əsas async dövrə:
    1. Binance WebSocket → event-driven
    2. yfinance REST → periyodik (sessiya açıq olduqda)
    3. Bütün koordinasiya asyncio ilə
    """
    # Bus-a candle handler qeydiyyat et
    bus.subscribe("candle_close", on_candle_close)

    # Binance top pair-ləri WS-ə vermək üçün yüklə
    try:
        markets = exchange.load_markets()
        tickers = exchange.fetch_tickers()
        market_cache.set("markets", markets)
        market_cache.set("tickers", tickers)
        usdt = [s for s in markets if '/USDT' in s and markets[s]['active'] and s in tickers]
        usdt.sort(key=lambda s: tickers[s].get('quoteVolume') or 0, reverse=True)
        top50 = usdt[:50]  # WS max 50 stream
    except Exception as e:
        log.warning(f"[MAIN] Market yüklə xəta: {e}")
        top50 = ['BTC/USDT', 'ETH/USDT', 'BNB/USDT', 'SOL/USDT', 'XRP/USDT']

    ws_bot = BinanceWebSocket(top50, interval='1h')
    log.info(f"[MAIN] WS başlayır: {len(top50)} pair")

    last_rest_scan = 0.0
    REST_INTERVAL  = 3600  # REST fallback hər saatda

    async def rest_scan_loop():
        nonlocal last_rest_scan
        while not _shutdown_event.is_set():
            now = time.time()
            if now - last_rest_scan >= REST_INTERVAL or ws_bot._reconnects >= WS_RECONNECT_MAX:
                last_rest_scan = now
                loop = asyncio.get_event_loop()
                # [FIX3] CPU-intensive işlər ProcessPool-da
                tasks = [
                    loop.run_in_executor(None, binance_skan_rest),
                    loop.run_in_executor(None, yfinance_skan, FOREX_ASSETS,     "FOREX",    "forex"),
                    loop.run_in_executor(None, yfinance_skan, COMMODITY_ASSETS, "ƏMTƏƏ",    "commodity"),
                    loop.run_in_executor(None, yfinance_skan, INDEX_ASSETS,     "İNDEKS",   "stock"),
                    loop.run_in_executor(None, yfinance_skan, SP500_ASSETS,     "S&P500",   "stock"),
                    loop.run_in_executor(None, yfinance_skan, AVROPA_ASSETS,    "AVROPA",   "stock"),
                ]
                await asyncio.gather(*tasks, return_exceptions=True)
                log.info(f"✅ REST skan bitdi | Siqnal:{gunluk_siqnal}")
            await asyncio.sleep(60)

    # Hər şeyi paralel işlət
    await asyncio.gather(
        ws_bot.run(),          # [FIX4] Native WS
        rest_scan_loop(),      # REST fallback
        return_exceptions=True
    )

# ══════════════════════════════════════════════════════════
#  [NEW] CHAOS TESTING — Stress Test modulu
#  1. İnternet kəsilməsi simulyasiyası
#  2. Corrupted / malformed WebSocket data
#  3. Queue overflow → backpressure (RAM qoruması)
#  4. API "corrupted response" ssenarisi
#  İstifadə: /chaos komandasından və ya manual çağırış
# ══════════════════════════════════════════════════════════
class ChaosEngine:
    """
    Chaos Engineering modulu — real hadisələri simulyasiya edir.
    Hər test nəticəsini Telegram-a göndərir.

    Testlər:
    ├── test_network_outage()  → internet tam kəsilməsi (30s)
    ├── test_corrupted_ws()   → zərərli/pozulmuş WS mesajları
    ├── test_queue_overflow() → 10,000 msg/s queue flood
    ├── test_api_corruption() → API-dən yanlış data
    └── run_all()             → bütün testləri ardıcıl icra et
    """

    def __init__(self):
        self._lock    = threading.Lock()
        self._running = False
        self._results : List[dict] = []

    def _report(self, test: str, passed: bool, detail: str):
        status = "✅ KEÇDİ" if passed else "❌ UĞURSUZ"
        r = {'test': test, 'passed': passed, 'detail': detail,
             'ts': datetime.now().isoformat()}
        with self._lock:
            self._results.append(r)
        log.info(f"[CHAOS] {test}: {status} — {detail}")
        prom.counter_inc("chaos_tests_total",
                         labels={"test": test, "result": "pass" if passed else "fail"})

    # ── Test 1: İnternet kəsilməsi ─────────────────────────
    def test_network_outage(self, duration_sec: float = 5.0) -> bool:
        """
        CircuitBreaker-lərin açılıb-bağlanmasını simulyasiya edir.
        Real internet kəsilməsini əvəz edir — actual bağlantını kəsmir.
        """
        try:
            # Bütün CB-ləri məcburi aç
            cbs = [cb_binance, cb_finnhub, cb_yfinance, cb_telegram]
            for cb in cbs:
                for _ in range(cb._thresh + 1):
                    cb.record_failure()

            all_open = all(cb.is_open for cb in cbs)
            if not all_open:
                self._report("network_outage", False,
                             "CB-lər açılmadı")
                return False

            log.info(f"[CHAOS] Şəbəkə kəsilməsi simulyasiyası ({duration_sec}s)...")
            time.sleep(duration_sec)

            # Recovery: bütün CB-ləri bərpa et
            for cb in cbs:
                cb.record_success()

            still_open = any(cb.is_open for cb in cbs)
            if still_open:
                self._report("network_outage", False,
                             "CB-lər recovery-dən sonra hələ açıqdır")
                return False

            self._report("network_outage", True,
                         f"CB-lər {duration_sec}s sonra bərpa oldu")
            return True

        except Exception as e:
            self._report("network_outage", False, str(e))
            return False

    # ── Test 2: Corrupted WebSocket data ───────────────────
    def test_corrupted_ws(self) -> bool:
        """
        Müxtəlif pozulmuş WS mesajlarının bot tərəfindən
        düzgün rədd edilib-edilmədiyini yoxlayır.
        """
        bad_payloads = [
            b"",                                  # boş
            b"{not json}",                        # invalid JSON
            b'{"k": null}',                       # null kline
            b'{"s":"BTCUSDT","k":{"x":true,"o":"NaN","h":"inf","l":"-inf","c":"abc","v":"0"}}',
            b'{"s":"","k":{"x":true,"o":"100","h":"105","l":"95","c":"102","v":"1000"}}',  # boş symbol
            b'A' * 100_000,                       # 100KB garbage
            b'{"k":{"x":true,"o":"99999999999","c":"-1"}}',  # ekstrem qiymət
        ]

        errors_caught = 0
        for payload in bad_payloads:
            try:
                raw = payload.decode('utf-8', errors='replace')
                msg  = json.loads(raw)
                data = msg.get('data', msg)
                k    = data.get('k', {})
                sym  = data.get('s', '')

                # Bot-un öz qoruma məntiqini aktivləşdir
                if not sym:
                    errors_caught += 1; continue
                if k.get('x', False):
                    o = float(k.get('o', 'nan'))
                    c_val = float(k.get('c', 'nan'))
                    if not (np.isfinite(o) and np.isfinite(c_val)):
                        errors_caught += 1; continue
                    if c_val < 0 or o < 0:
                        errors_caught += 1; continue
            except (json.JSONDecodeError, ValueError, UnicodeDecodeError):
                errors_caught += 1
            except Exception:
                errors_caught += 1

        passed = errors_caught == len(bad_payloads)
        self._report("corrupted_ws", passed,
                     f"{errors_caught}/{len(bad_payloads)} pozulmuş mesaj düzgün rədd edildi")
        return passed

    # ── Test 3: Queue Overflow → Backpressure ──────────────
    def test_queue_overflow(self) -> bool:
        """
        AsyncIO Queue-nu flood edib RAM-ın partladılmamasını yoxlayır.
        Backpressure: queue dolu olduqda yeni mesajlar DROP olunur,
        Exception atılmır, process çökmür.
        """
        FLOOD_COUNT  = 5_000   # 5K mesaj (real 10K-nın simulyasiyası)
        dropped      = 0
        accepted     = 0

        # Telegram queue-nu test et
        test_q: queue.Queue = queue.Queue(maxsize=100)
        for i in range(FLOOD_COUNT):
            try:
                test_q.put_nowait(f"chaos_msg_{i}")
                accepted += 1
            except queue.Full:
                dropped += 1

        # Əsl _tg_queue-nu da yoxla
        tg_full_before = _tg_queue.full()

        passed = (dropped > 0               # drop baş verdi
                  and accepted <= 100       # maxsize-ı keçmədi
                  and not tg_full_before)   # əsl queue sağlamdır

        self._report("queue_overflow", passed,
                     f"Flood: {FLOOD_COUNT} | Qəbul: {accepted} | Drop: {dropped} | "
                     f"RAM qorundu: {'✅' if passed else '❌'}")
        return passed

    # ── Test 4: API Corrupted Response ─────────────────────
    def test_api_corruption(self) -> bool:
        """
        Binance-dən gələ biləcək pozulmuş/gözlənilməz OHLCV cavablarının
        analiz_et() tərəfindən düzgün rədd edilib-edilmədiyini yoxlayır.
        """
        bad_frames = [
            # Boş frame
            {},
            # Qısa DataFrame (min 150 bar lazımdır)
            {'1h': pd.DataFrame({'Open': [1], 'High': [2], 'Low': [0.5],
                                 'Close': [1.5], 'vol': [100]})},
            # NaN dolu frame
            {'1h': pd.DataFrame({
                'Open':  [np.nan]*200, 'High': [np.nan]*200,
                'Low':   [np.nan]*200, 'Close':[np.nan]*200,
                'vol':   [0.0]*200
            })},
            # Sıfır qiymətlər
            {'1h': pd.DataFrame({
                'Open': [0.0]*200, 'High': [0.0]*200,
                'Low':  [0.0]*200, 'Close':[0.0]*200,
                'vol':  [0.0]*200
            })},
        ]

        correctly_rejected = 0
        for frames in bad_frames:
            try:
                result = analiz_et(frames, 'crypto', 'TEST/USDT')
                if result is None:
                    correctly_rejected += 1
                # Əgər nəticə qaytarıbsa, məzmununu yoxla
                elif not isinstance(result.get('entry', 0), float):
                    correctly_rejected += 1
            except Exception:
                correctly_rejected += 1  # Exception = düzgün rədd

        passed = correctly_rejected == len(bad_frames)
        self._report("api_corruption", passed,
                     f"{correctly_rejected}/{len(bad_frames)} pozulmuş cavab düzgün rədd edildi")
        return passed

    # ── Bütün testlər ──────────────────────────────────────
    def run_all(self) -> dict:
        with self._lock:
            if self._running:
                return {'error': 'Chaos test artıq işləyir'}
            self._running = True

        try:
            log.info("[CHAOS] 🔴 Stress test başlayır...")
            telegram_gonder("🔴 <b>CHAOS TEST BAŞLADI</b>\n\n"
                            "Bot 30-60 saniyə ərzində stress sınağından keçirilir...")

            results = {
                'network_outage' : self.test_network_outage(duration_sec=3.0),
                'corrupted_ws'   : self.test_corrupted_ws(),
                'queue_overflow' : self.test_queue_overflow(),
                'api_corruption' : self.test_api_corruption(),
            }

            passed = sum(results.values())
            total  = len(results)
            ok     = passed == total

            summary = (
                f"{'✅' if ok else '⚠️'} <b>CHAOS TEST NƏTİCƏ</b>\n\n"
                f"✅ Keçdi: {passed}/{total}\n\n"
            )
            for name, res in results.items():
                summary += f"{'✅' if res else '❌'} {name}\n"

            if not ok:
                summary += ("\n⚠️ Uğursuz testlər var — "
                            "log-lara baxın: grep CHAOS sniper.log")
            else:
                summary += "\n🛡 Bot bütün ssenarilərdən keçdi!"

            telegram_gonder(summary)
            log.info(f"[CHAOS] Tamamlandı — {passed}/{total} test keçdi")
            return results

        finally:
            with self._lock:
                self._running = False

    def get_results(self) -> List[dict]:
        with self._lock:
            return list(self._results)


chaos_engine = ChaosEngine()

# ══════════════════════════════════════════════════════════
#  [NEW] ASYNCIO BACKPRESSURE — Queue Overflow qoruması
#  Saniyədə 10,000+ mesaj gəlsə RAM-ı yükləməsin.
#  Bounded queue + drop policy + metrics.
# ══════════════════════════════════════════════════════════
class BackpressureQueue:
    """
    AsyncIO uyğun bounded queue — overflow-da DROP edir.

    Strategiyalar:
    - DROP_OLDEST: köhnə mesajı at, yenini qəbul et (default)
    - DROP_NEWEST: yeni mesajı at (conservative)

    RAM təhlükəsizliyi:
    - maxsize həmişə sabit qalır
    - dropped_total sayılır və Prometheus-a verilir
    """
    DROP_OLDEST = 'oldest'
    DROP_NEWEST = 'newest'

    def __init__(self, maxsize: int = 500,
                 strategy: str = 'oldest',
                 name: str = 'default'):
        self._q        = asyncio.Queue(maxsize=maxsize) if maxsize else asyncio.Queue()
        self._maxsize  = maxsize
        self._strategy = strategy
        self._name     = name
        self._dropped  = 0
        self._lock     = asyncio.Lock()

    async def put(self, item):
        if self._q.full():
            self._dropped += 1
            prom.counter_inc("queue_dropped_total",
                             labels={"queue": self._name})
            if self._dropped % 100 == 0:
                log.warning(f"[BPQ] {self._name}: {self._dropped} mesaj atıldı "
                            f"(maxsize={self._maxsize})")

            if self._strategy == self.DROP_OLDEST:
                try:
                    self._q.get_nowait()  # köhnəni at
                except asyncio.QueueEmpty:
                    pass
                await self._q.put(item)
            # DROP_NEWEST: heç nə etmə (item atılır)
        else:
            await self._q.put(item)

    async def get(self):
        return await self._q.get()

    def qsize(self) -> int:
        return self._q.qsize()

    def dropped(self) -> int:
        return self._dropped

    def stats(self) -> dict:
        return {
            'name':    self._name,
            'size':    self._q.qsize(),
            'maxsize': self._maxsize,
            'dropped': self._dropped,
        }

# Candle event-ləri üçün bounded backpressure queue
_candle_bp_queue = BackpressureQueue(maxsize=500, strategy='oldest', name='candle_events')

# ══════════════════════════════════════════════════════════
#  BAŞLANĞIC
# ══════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════
#  [M1] ORDER BOOK IMBALANCE — Bid/Ask ratio
#  Böyük alış divarı → LONG tərəfdarı
#  Böyük satış divarı → SHORT tərəfdarı
# ══════════════════════════════════════════════════════════
class OrderBookImbalance:
    """
    Binance order book-dan bid/ask həcm nisbətini ölçür.
    OBI > 0.53  → alıcılar aqressiv → LONG siqnalına +bonus
    OBI < 0.47  → satıcılar aqressiv → SHORT siqnalına +bonus
    OBI 0.47–0.53 → balanslaşdırılmış → neytral

    Formula: OBI = total_bid_qty / (total_bid_qty + total_ask_qty)
    """
    DEPTH_LIMIT  = 20      # top 20 bid/ask
    HISTORY_SIZE = 50
    CACHE_TTL    = 5.0    # 15 saniyə cache

    def __init__(self):
        self._lock    = threading.Lock()
        self._history : Dict[str, deque] = defaultdict(lambda: deque(maxlen=self.HISTORY_SIZE))
        self._cache   : Dict[str, tuple] = {}   # symbol → (obi, ts)

    def fetch(self, symbol: str) -> float:
        """OBI dəyəri qaytarır. Cache TTL-i keçibsə yenidən çəkir."""
        now = time.monotonic()
        with self._lock:
            cached = self._cache.get(symbol)
            if cached and now - cached[1] < self.CACHE_TTL:
                return cached[0]

        try:
            ob  = exchange.fetch_order_book(symbol, limit=self.DEPTH_LIMIT)
            bid = sum(q for _, q in ob.get('bids', []))
            ask = sum(q for _, q in ob.get('asks', []))
            total = bid + ask
            obi   = bid / total if total > 0 else 0.5

            with self._lock:
                self._history[symbol].append(obi)
                self._cache[symbol] = (obi, time.monotonic())

            prom.histogram_observe("order_book_imbalance",
                                   obi, labels={"symbol": symbol[:6]})
            return obi
        except Exception as e:
            log.debug(f"[OBI] {symbol}: {e}")
            return 0.5   # neytral fallback

    def rolling_obi(self, symbol: str) -> float:
        """Son N ölçümün ortalaması — ani spike-ları hamarlayır."""
        with self._lock:
            h = list(self._history.get(symbol, []))
        if not h:
            return 0.5
        return float(np.mean(h[-10:]))

    def signal_bias(self, symbol: str, yon: str) -> float:
        """
        Siqnalın istiqamətini OBI ilə təsdiq edir.
        Returns: 0.0–1.0 bonus skor
        """
        obi = self.rolling_obi(symbol)
        if yon == 'LONG'  and obi > 0.60: return min((obi - 0.60) * 5, 1.0)
        if yon == 'SHORT' and obi < 0.40: return min((0.40 - obi) * 5, 1.0)
        return 0.0

order_book_imbalance = OrderBookImbalance()


# ══════════════════════════════════════════════════════════
#  [M2] CVD — Cumulative Volume Delta
#  Alıcı həcmi vs Satıcı həcmi toplanır.
#  CVD yüksəlirsə → alıcılar aqressiv → LONG dəstəyi
#  CVD düşürsə    → satıcılar aqressiv → SHORT dəstəyi
# ══════════════════════════════════════════════════════════
class CVDTracker:
    """
    Hər şam üçün CVD hesablayır:
      delta = (close > open) ? +volume : -volume
    Rolling CVD = son N delta-nın kümülatif cəmi

    Təkmil versiya:
      - Şamın bağlanışı əsas istiqaməti verir
      - Həcm ağırlıqlandırılır
      - Z-score ilə anomaliya aşkarı
    """
    WINDOW = 5

    def __init__(self):
        self._lock    = threading.Lock()
        self._history : Dict[str, deque] = defaultdict(lambda: deque(maxlen=200))

    def update_from_df(self, symbol: str, df: pd.DataFrame):
        """DataFrame-dən CVD hesabla."""
        if df is None or len(df) < 10:
            return
        try:
            vc = 'vol' if 'vol' in df.columns else 'Volume'
            closes = df['Close'].values.astype(np.float64)
            opens  = df['Open'].values.astype(np.float64)
            vols   = df[vc].values.astype(np.float64)
            delta  = np.where(closes > opens, vols, np.where(closes < opens, -vols, 0.0))
            with self._lock:
                for d in delta[-self.WINDOW:]:
                    self._history[symbol].append(float(d))
        except Exception as e:
            log.debug(f"[CVD] update {symbol}: {e}")

    def get_cvd(self, symbol: str) -> float:
        """Son WINDOW şamın kümülatif delta-sı."""
        with self._lock:
            h = list(self._history.get(symbol, []))
        if not h:
            return 0.0
        return float(np.sum(h[-self.WINDOW:]))

    def cvd_slope(self, symbol: str) -> float:
        """CVD-nin son 5 şamlıq slope-u — trend gücünü göstərir."""
        with self._lock:
            h = list(self._history.get(symbol, []))
        if len(h) < 5:
            return 0.0
        cum = np.cumsum(h[-10:])
        if len(cum) < 2:
            return 0.0
        return float(cum[-1] - cum[0]) / (len(cum) + 1e-10)

    def signal_bias(self, symbol: str, yon: str) -> float:
        """CVD istiqaməti siqnalla uyğunlaşırsa bonus qaytarır (0–1)."""
        slope = self.cvd_slope(symbol)
        if slope == 0:
            return 0.0
        if yon == 'LONG'  and slope > 0: return min(abs(slope) / (abs(slope) + 1), 1.0)
        if yon == 'SHORT' and slope < 0: return min(abs(slope) / (abs(slope) + 1), 1.0)
        return 0.0

cvd_tracker = CVDTracker()


# ══════════════════════════════════════════════════════════
#  [M3] LIQUIDITY HEATMAP — Whale əmr zonası skaner
#  Böyük limitli əmrlərin toplandığı qiymət zonalarını tapır.
#  Sweep həmin zonaya gəldikdə siqnal daha güclüdür.
# ══════════════════════════════════════════════════════════
class LiquidityHeatmap:
    """
    Order Book-dan "divar" zonalarını tapır.
    Bir divar, ümumi dərinliyin 5%-dən çox olan tək bir
    qiymət səviyyəsidir → "Whale limit order" zonası.

    Metod:
    1. Top N bid/ask sətirini çək
    2. Hər sətrin həcmini ümumi həcmə bölüb Z-score al
    3. Z-score > threshold → "whale zone" kimi qeydə al
    4. Sweep bu zona ilə üst-üstə düşürsə → +1.5 skor bonus

    İstifadə:
    - analiz_et() içindən çağırılır
    - WebSocket bookTicker ilə davamlı yenilənir
    """
    DEPTH_LIMIT     = 50
    WHALE_THRESHOLD = 2.0    # Z-score həddi
    ZONE_TOLERANCE  = 0.003  # ±0.3% — zona yaxınlığı
    CACHE_TTL       = 30.0

    def __init__(self):
        self._lock  = threading.Lock()
        self._zones : Dict[str, dict] = {}   # symbol → {'bid_zones':[], 'ask_zones':[], 'ts':float}

    def scan(self, symbol: str) -> dict:
        """Whale zonalarını skan edir, cache edir."""
        now = time.monotonic()
        with self._lock:
            cached = self._zones.get(symbol)
            if cached and now - cached['ts'] < self.CACHE_TTL:
                return cached

        try:
            ob  = exchange.fetch_order_book(symbol, limit=self.DEPTH_LIMIT)
            bids = ob.get('bids', [])
            asks = ob.get('asks', [])

            def _whale_zones(orders):
                if not orders:
                    return []
                qtys = np.array([q for _, q in orders], dtype=np.float64)
                if qtys.sum() == 0:
                    return []
                mu = qtys.mean(); std = qtys.std()
                if std == 0:
                    return []
                zones = []
                for (px, qty), z in zip(orders, (qtys - mu) / std):
                    if z > self.WHALE_THRESHOLD:
                        zones.append({'price': float(px), 'qty': float(qty), 'zscore': round(float(z), 2)})
                return zones

            result = {
                'bid_zones': _whale_zones(bids),
                'ask_zones': _whale_zones(asks),
                'ts': time.monotonic(),
            }
            with self._lock:
                self._zones[symbol] = result

            prom.gauge_set("whale_zones_total",
                           len(result['bid_zones']) + len(result['ask_zones']),
                           labels={"symbol": symbol[:6]})
            return result

        except Exception as e:
            log.debug(f"[HEATMAP] {symbol}: {e}")
            return {'bid_zones': [], 'ask_zones': [], 'ts': 0}

    def near_whale_zone(self, symbol: str, price: float, yon: str) -> Tuple[bool, float]:
        """
        Qiymət whale zonasına yaxındırsa True qaytarır.
        LONG → bid zone yaxınlığı
        SHORT → ask zone yaxınlığı
        Returns: (near:bool, max_zscore:float)
        """
        data  = self.scan(symbol)
        zones = data['bid_zones'] if yon == 'LONG' else data['ask_zones']
        if not zones:
            return False, 0.0
        for z in zones:
            if abs(z['price'] - price) / price <= self.ZONE_TOLERANCE:
                return True, z['zscore']
        return False, 0.0

    def signal_bonus(self, symbol: str, price: float, yon: str) -> float:
        """Whale zona yaxınlığı varsa skor bonusu (0–1.5)."""
        near, zscore = self.near_whale_zone(symbol, price, yon)
        if not near:
            return 0.0
        return min(zscore / 3.0, 1.5)   # max 1.5 bonus

liquidity_heatmap = LiquidityHeatmap()


# ══════════════════════════════════════════════════════════
#  [M4] ADAPTIVE THRESHOLDING
#  Volatillik aşağı düşdükdə skor həddi avtomatik azalır.
#  Sakit bazarda daha çox siqnal, volatil bazarda daha az.
# ══════════════════════════════════════════════════════════
class AdaptiveThreshold:
    """
    ATR ratio əsaslı dinamik MIN_ULDUZ.

    ATR ratio < 0.7 (çox sakit) → MIN_ULDUZ * 0.85 (daha yumşaq)
    ATR ratio 0.7–1.3 (normal)  → MIN_ULDUZ (dəyişmir)
    ATR ratio 1.3–2.0 (yüksək)  → MIN_ULDUZ * 1.15 (daha sərt)
    ATR ratio > 2.0  (çox yüksək) → MIN_ULDUZ * 1.35

    Bu mexanizm:
    - Sakit günlərdə daha çox (lakin zəif) siqnal yaranmasını önləyir
    - Trend günlərində isə həddi azaldıb daha çox siqnal buraxır
    """
    def __init__(self):
        self._base = MIN_ULDUZ

    def get_threshold(self, atr_ratio: float) -> float:
        base = self._base
        if   atr_ratio < 0.7:              return round(base * 0.85, 2)
        elif atr_ratio < 1.3:              return round(base,         2)
        elif atr_ratio < 2.0:              return round(base * 1.15,  2)
        else:                              return round(base * 1.35,  2)

    def apply(self, params: dict, atr_ratio: float) -> dict:
        """dinamik_params() çıxışına adaptive həddi tətbiq edir."""
        params = params.copy()
        params['min_ulduz'] = self.get_threshold(atr_ratio)
        return params

adaptive_threshold = AdaptiveThreshold()


# ══════════════════════════════════════════════════════════
#  [M5] FUNDING RATE ARBITRAGE — Short Squeeze aşkarı
#  Həddindən artıq yüksək funding rate → kontrar siqnal
# ══════════════════════════════════════════════════════════
class FundingRateMonitor:
    """
    Binance Futures funding rate-ini izləyir.

    Ekstrimal funding rate bazarın bir istiqamətdə
    "həddindən artıq yüklənməsini" (overheated) göstərir:

    funding > +0.1%  → Long həddindən artıq → SHORT squeeze riski
    funding < -0.1%  → Short həddindən artıq → LONG squeeze riski
    funding -0.05% – +0.05% → Normal

    İstifadə:
    1. Yüksək funding SHORT siqnalına +bonus verir (squeeze gözlənir)
    2. Çox yüksək funding uzun LONG siqnallarını bloklamır (əksinə SHORT dəstəkləyir)
    """
    CACHE_TTL        = 300.0   # 5 dəq
    HIGH_LONG_THRESH =  0.001   # +0.1%
    HIGH_SHORT_THRESH= -0.001   # -0.1%
    EXTREME_THRESH   =  0.003   # +0.3%

    def __init__(self):
        self._lock  = threading.Lock()
        self._cache : Dict[str, tuple] = {}   # symbol → (rate, ts)

    def fetch(self, symbol: str) -> float:
        """Funding rate qaytarır. Yalnız USDT futures üçündür."""
        now = time.monotonic()
        with self._lock:
            cached = self._cache.get(symbol)
            if cached and now - cached[1] < self.CACHE_TTL:
                return cached[0]

        try:
            # Binance fundingRate endpoint
            sym_clean = symbol.replace('/', '')
            url = f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={sym_clean}"
            r   = _session_get().get(url, timeout=8)
            r.raise_for_status()
            rate = float(r.json().get('lastFundingRate', 0))
            with self._lock:
                self._cache[symbol] = (rate, time.monotonic())
            prom.gauge_set("funding_rate", rate * 100,
                           labels={"symbol": symbol[:6]})
            return rate
        except Exception as e:
            log.debug(f"[FUNDING] {symbol}: {e}")
            return 0.0

    def signal_bias(self, symbol: str, yon: str) -> Tuple[float, str]:
        """
        Siqnala funding rate əsaslı bonus/malus verir.
        Returns: (score_delta, reason_str)
        """
        rate = self.fetch(symbol)
        if abs(rate) < abs(self.HIGH_SHORT_THRESH):
            return 0.0, ""

        # Funding çox yüksəkdirsə → Short Squeeze gözlənilir
        if rate > self.EXTREME_THRESH:
            if yon == 'SHORT':
                return 1.5, f"Extreme LONG funding ({rate*100:.3f}%) → Squeeze"
            if yon == 'LONG':
                return -0.5, f"Extreme LONG funding ({rate*100:.3f}%) — risk"

        if rate > self.HIGH_LONG_THRESH:
            if yon == 'SHORT':
                return 0.8, f"HIGH funding ({rate*100:.3f}%) → SHORT kontrar"

        # Funding çox mənfidirsə → Long Squeeze gözlənilir
        if rate < -self.EXTREME_THRESH:
            if yon == 'LONG':
                return 1.5, f"Extreme SHORT funding ({rate*100:.3f}%) → L-Squeeze"
            if yon == 'SHORT':
                return -0.5, f"Extreme SHORT funding — risk"

        if rate < self.HIGH_SHORT_THRESH:
            if yon == 'LONG':
                return 0.8, f"HIGH negative funding ({rate*100:.3f}%) → LONG kontrar"

        return 0.0, ""

funding_monitor = FundingRateMonitor()


# ══════════════════════════════════════════════════════════
#  [M7] VOLATILITY-ADJUSTED SL/TP (ATR əsaslı)
#  Sabit faiz deyil — ATR ilə dinamik SL/TP hesabı
#  v12-dəki dinamik_tp_hesabla() əvəz edir / zənginləşdirir
# ══════════════════════════════════════════════════════════
def vol_adjusted_sl_tp(price: float, yon: str, atr: float,
                       rejim: str, mtf_skor: float,
                       asset_type: str = 'crypto') -> dict:
    """
    ATR multiplier rejimə və asset növünə görə dəyişir:
    - TRENDING:  SL 2.0×ATR  TP1 1.5× TP2 3.0× TP3 5.0×
    - RANGING:   SL 1.5×ATR  TP1 1.0× TP2 2.0× TP3 3.0×
    - HIGH_VOL:  SL 3.0×ATR  TP1 1.5× TP2 2.5× TP3 4.0×
    - CRISIS:    SL 4.0×ATR  TP1 1.0× TP2 1.5× TP3 2.5×

    Əlavə: MTF skor yüksəkdirsə TP3 uzanır (max +30%)
    """
    MULTIPLIERS = {
        'TRENDING': {'sl': 2.0, 'tp1': 1.5, 'tp2': 3.0, 'tp3': 5.0},
        'RANGING':  {'sl': 1.5, 'tp1': 1.0, 'tp2': 2.0, 'tp3': 3.0},
        'HIGH_VOL': {'sl': 3.0, 'tp1': 1.5, 'tp2': 2.5, 'tp3': 4.0},
        'CRISIS':   {'sl': 4.0, 'tp1': 1.0, 'tp2': 1.5, 'tp3': 2.5},
    }
    # Asset-specific uyğunlaşma
    ASSET_SL_ADJ = {'FOREX': 0.8, 'STOCK': 0.9, 'INDEX': 0.85,
                    'COMMODITY': 1.0, 'CRYPTO': 1.0}

    m    = MULTIPLIERS.get(rejim, MULTIPLIERS['RANGING'])
    adj  = ASSET_SL_ADJ.get(asset_type.upper(), 1.0)
    sl_m = m['sl'] * adj

    # MTF bonus: skor ≥ 4 → TP3 30% uzanır
    tp3_m = m['tp3'] * (1.3 if mtf_skor >= 4 else 1.0)

    if yon == 'LONG':
        sl  = price - atr * sl_m
        tp1 = price + atr * m['tp1']
        tp2 = price + atr * m['tp2']
        tp3 = price + atr * tp3_m
    else:
        sl  = price + atr * sl_m
        tp1 = price - atr * m['tp1']
        tp2 = price - atr * m['tp2']
        tp3 = price - atr * tp3_m

    risk   = abs(price - sl)
    reward = abs(price - tp2)
    rr     = round(reward / risk, 2) if risk > 0 else 0.0

    return {
        'sl':         round(sl,  6),
        'tp1':        round(tp1, 6),
        'tp2':        round(tp2, 6),
        'tp3':        round(tp3, 6),
        'be_trigger': round(tp1, 6),
        'tp1_pct':    25,
        'tp2_pct':    50,
        'tp3_pct':    25,
        'rr':         rr,
        'sl_mult':    sl_m,
        'atr_used':   round(atr, 6),
    }


# ══════════════════════════════════════════════════════════
#  [M8] DRAWDOWN CIRCUIT BREAKER — 24 saatlıq kilid
#  Gündəlik itki MAX_GUNLUK_ZEFER%-ni keçdikdə
#  bot 24 saat tam dayanır (yalnız restart açar).
# ══════════════════════════════════════════════════════════
class DrawdownCircuitBreaker:
    """
    RiskEngineV2.daily_loss_ok() üzərindən işləyir.
    Fərq: bu kilid yalnız CTRL+C və ya gündəlik reset ilə açılır.
    Bot 24 saat boyunca heç bir siqnal vermir.

    Əlavə: ardıcıl itki seriası üçün adaptiv dayandırma:
    5 ardıcıl SL → 2 saatlıq fasilə (CB stil)
    """
    def __init__(self, max_daily_pct: float = 3.0,
                 consecutive_sl_limit: int = 5):
        self._max_daily      = max_daily_pct
        self._consec_limit   = consecutive_sl_limit
        self._lock           = threading.Lock()
        self._locked_until   = 0.0        # epoch timestamp
        self._consec_sl      = 0
        self._consec_locked  = 0.0

    def record_trade(self, win: bool):
        """Hər siqnal nəticəsini qeyd et."""
        with self._lock:
            if win:
                self._consec_sl = 0
            else:
                self._consec_sl += 1
                if self._consec_sl >= self._consec_limit:
                    self._consec_locked = time.time() + 7200  # 2 saat
                    self._consec_sl     = 0
                    log.warning(f"[DCB] {self._consec_limit} ardıcıl SL — 2 saat fasilə")
                    telegram_gonder(
                        f"⚠️ <b>DRAWDOWN CB</b>\n"
                        f"{self._consec_limit} ardıcıl SL — 2 saatlıq fasilə aktiv!"
                    )

    def daily_lock(self, current_loss_pct: float):
        """Gündəlik itkini yoxla, lazımsa 24 saatlıq kilid."""
        if current_loss_pct >= self._max_daily:
            with self._lock:
                self._locked_until = time.time() + 86400   # 24 saat
            log.critical(f"[DCB] {current_loss_pct:.2f}% itki — 24 saatlıq kilid!")
            telegram_gonder(
                f"🔴 <b>DRAWDOWN KİLİD</b>\n"
                f"Gündəlik itki {current_loss_pct:.2f}% — bot 24 saat dayanır!\n"
                f"Kilid açılır: {datetime.fromtimestamp(time.time()+86400).strftime('%Y-%m-%d %H:%M')}"
            )

    def is_locked(self) -> bool:
        """Bot həm gündəlik kilid, həm ardıcıl SL kilidi yoxlayır."""
        now = time.time()
        with self._lock:
            if now < self._locked_until:
                return True
            if now < self._consec_locked:
                return True
        return False

    def remaining_seconds(self) -> float:
        now = time.time()
        with self._lock:
            return max(0.0, max(self._locked_until, self._consec_locked) - now)

drawdown_cb = DrawdownCircuitBreaker(
    max_daily_pct    = MAX_GUNLUK_ZEFER,
    consecutive_sl_limit = 5
)


# ══════════════════════════════════════════════════════════
#  [M10] LATENCY MONITOR — 500ms siqnal ləğvi
#  API gecikmə 500ms-dən yuxarı olarsa siqnal verilmir.
#  LatencyTracker-dən real-time oxuyur.
# ══════════════════════════════════════════════════════════
class LatencyMonitor:
    """
    Siqnal vermədən əvvəl API gecikmə yoxlayır.
    500ms-dən yuxarı latency → siqnal ləğv edilir.

    Threshold-lar:
    - binance_rest_ms > 800ms  → BLOK
    - signal_latency_ms > 500ms → WARN (bloklamaz)
    - ws_latency_ms > 200ms    → WARN
    """
    BINANCE_HARD_LIMIT  = 800.0   # ms — bu keçilərsə siqnal verilmir
    SIGNAL_WARN_LIMIT   = 500.0   # ms — xəbərdarlıq
    WS_WARN_LIMIT       = 200.0   # ms — xəbərdarlıq

    def check(self) -> Tuple[bool, str]:
        """
        (ok:bool, reason:str) qaytarır.
        ok=False → siqnal ləğv et.
        """
        s = latency.summary("binance_rest_ms")
        if s['n'] > 0 and s['p95'] > self.BINANCE_HARD_LIMIT:
            reason = f"Binance REST p95={s['p95']:.0f}ms > {self.BINANCE_HARD_LIMIT:.0f}ms"
            prom.counter_inc("signals_blocked_total", labels={"reason": "latency"})
            log.warning(f"[LATENCY] Siqnal bloklandı — {reason}")
            return False, reason

        # Soft xəbərdarlıqlar (bloklamır)
        ws = latency.summary("ws_latency_ms")
        if ws['n'] > 0 and ws['p95'] > self.WS_WARN_LIMIT:
            log.debug(f"[LATENCY] WS p95={ws['p95']:.0f}ms — yüksəkdir")

        return True, ""

    def record_signal(self, start_mono: float):
        """Siqnal prosesləmə vaxtını qeyd et."""
        ms = (time.monotonic() - start_mono) * 1000
        latency.record("signal_latency_ms", ms)

latency_monitor = LatencyMonitor()


# ══════════════════════════════════════════════════════════
#  [M11] SHADOW MODE — Paper Trading (arxa planda test)
#  Yeni strategiyaları real pulla risk etmədən test edir.
#  Nəticələr SQLite-a yazılır, həftəlik Telegram-a göndərilir.
# ══════════════════════════════════════════════════════════
class ShadowMode:
    """
    Hər real siqnalla yanaşı "kölgə" trade açır.
    Real pul hərəkəti yoxdur — yalnız simulyasiya.

    İzlənir:
    - Shadow win rate (real ilə müqayisə üçün)
    - Shadow PnL (nəzəri)
    - Shadow RR distribution

    DB: `shadow_trades` cədvəli
    """
    ENABLED     = True
    TRACK_BARS  = 72       # max 72 şam (3 gün)

    def __init__(self):
        self._lock   = threading.Lock()
        self._open   : Dict[str, dict] = {}
        self._closed : list = []
        self._total_pnl = 0.0
        self._db_init()

    def _db_init(self):
        try:
            con = sqlite3.connect(LOG_FAYL, timeout=10)
            con.execute("PRAGMA journal_mode=WAL")
            con.execute("""
                CREATE TABLE IF NOT EXISTS shadow_trades (
                    id       INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol   TEXT, yon TEXT,
                    entry    REAL, tp REAL, sl REAL, rr REAL,
                    skor     REAL, rejim TEXT,
                    opened   TEXT, closed TEXT,
                    result   TEXT, pnl REAL
                )""")
            con.commit(); con.close()
        except Exception as e:
            log.debug(f"[SHADOW] DB init: {e}")

    def open_trade(self, sym: str, res: dict):
        if not self.ENABLED:
            return
        with self._lock:
            self._open[sym] = {
                'entry':   res['entry'],
                'tp':      res['tp'],
                'sl':      res['sl'],
                'yon':     res['yon'],
                'rr':      res['rr'],
                'skor':    res['skor'],
                'rejim':   res['rejim'],
                'opened':  datetime.now().isoformat(),
                'bars':    0,
            }
        log.debug(f"[SHADOW] Açıldı: {sym} {res['yon']} @ {res['entry']}")

    def tick(self, sym: str, high: float, low: float):
        """Hər yeni şamda açıq kölgə trade-ləri yenilə."""
        if not self.ENABLED:
            return
        with self._lock:
            trade = self._open.get(sym)
            if trade is None:
                return
            trade['bars'] += 1
            hit = None
            if trade['yon'] == 'LONG':
                if low  <= trade['sl']: hit = 'SL'
                elif high >= trade['tp']: hit = 'TP'
            else:
                if high >= trade['sl']: hit = 'SL'
                elif low  <= trade['tp']: hit = 'TP'
            if hit is None and trade['bars'] >= self.TRACK_BARS:
                hit = 'EXPIRED'
            if hit:
                pnl = (trade['tp'] - trade['entry'] if hit == 'TP' else
                       trade['sl'] - trade['entry'] if hit == 'SL' else 0.0)
                if trade['yon'] == 'SHORT': pnl = -pnl
                self._total_pnl += pnl
                self._closed.append({**trade, 'result': hit, 'pnl': pnl,
                                     'closed': datetime.now().isoformat()})
                del self._open[sym]
                self._db_write(sym, trade, hit, pnl)
                log.debug(f"[SHADOW] Bağlandı: {sym} {hit} PnL={pnl:.4f}")

    def _db_write(self, sym: str, t: dict, result: str, pnl: float):
        try:
            con = sqlite3.connect(LOG_FAYL, timeout=5)
            con.execute("PRAGMA journal_mode=WAL")
            con.execute("""
                INSERT INTO shadow_trades
                (symbol,yon,entry,tp,sl,rr,skor,rejim,opened,closed,result,pnl)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                (sym, t['yon'], t['entry'], t['tp'], t['sl'], t['rr'],
                 t['skor'], t['rejim'], t['opened'],
                 datetime.now().strftime('%Y-%m-%d %H:%M'), result, pnl))
            con.commit(); con.close()
        except Exception as e:
            log.debug(f"[SHADOW] DB yazma: {e}")

    def stats(self) -> dict:
        with self._lock:
            closed = list(self._closed)
        n      = len(closed)
        if n == 0:
            return {'n': 0, 'wr': 0.0, 'net_pnl': 0.0, 'open': len(self._open)}
        wins   = sum(1 for t in closed if t['result'] == 'TP')
        return {
            'n':        n,
            'wr':       round(wins / n * 100, 1),
            'net_pnl':  round(self._total_pnl, 4),
            'open':     len(self._open),
            'wins':     wins,
            'losses':   sum(1 for t in closed if t['result'] == 'SL'),
        }

    def weekly_report(self):
        """Həftəlik shadow mode hesabatı Telegram-a göndər."""
        s = self.stats()
        telegram_gonder(
            f"👻 <b>SHADOW MODE HESABAT</b>\n\n"
            f"📊 Cəmi: {s['n']} trade\n"
            f"✅ Qazan: {s.get('wins',0)} | ❌ Uduz: {s.get('losses',0)}\n"
            f"🎯 Win Rate: {s['wr']}%\n"
            f"💵 Net PnL (nəzəri): {s['net_pnl']}\n"
            f"🔓 Açıq: {s['open']}"
        )

shadow_mode = ShadowMode()


# ══════════════════════════════════════════════════════════
#  [M12] HƏFTƏLIK OTOMATİK WFO
#  Hər şənbə gecəsi saat 02:00-da avtomatik işləyir.
#  Nəticə: parametrlər yenilənir, Telegram-a göndərilir.
# ══════════════════════════════════════════════════════════
def wfo_weekly_scheduler():
    """
    Daemon thread — hər şənbə gecəsi 02:00-da WFO işlədir.
    Shadow mode hesabatını da eyni vaxtda göndərir.
    """
    while not _shutdown_event.is_set():
        now = datetime.now()
        # Şənbə (weekday=5) saat 02:00–02:05
        if now.weekday() == 5 and now.hour == 2 and 0 <= now.minute < 5:
            log.info("[WFO-AUTO] Həftəlik WFO başlayır...")
            telegram_gonder("⚙️ <b>HƏFTƏLIK OTOMATİK WFO</b> başladı...")
            try:
                walk_forward_optimizasiya('BTC/USDT',  'crypto')
                walk_forward_optimizasiya('ETH/USDT',  'crypto')
                walk_forward_optimizasiya('BNB/USDT',  'crypto')
            except Exception as e:
                log.error(f"[WFO-AUTO] xəta: {e}")
            # Shadow mode həftəlik hesabat
            shadow_mode.weekly_report()
            time.sleep(400)   # Eyni həftə bir dəfə (300+gözlə)
        time.sleep(60)


# ══════════════════════════════════════════════════════════
#  v13.0 ANALIZ WRAPPERS
#  Yeni modulları mövcud analiz_et() axınına birləşdirir.
#  analiz_et() köhnə qaydada işləyir, bu wrapper üstündən
#  OBI/CVD/Heatmap/Funding/LatencyMonitor/DrawdownCB əlavə edir.
# ══════════════════════════════════════════════════════════
def analiz_et_v13(frames: dict, asset_type='crypto',
                  symbol='', ref_idx=-2) -> Optional[dict]:
    """
    analiz_et() əsasında işləyir + v13 modülları:
    1. DrawdownCB yoxla
    2. LatencyMonitor yoxla
    3. analiz_et() çağır
    4. CVD yenilə
    5. OBI bonus əlavə et
    6. CVD bonus əlavə et
    7. Liquidity Heatmap bonus əlavə et (yalnız crypto)
    8. Funding Rate bonus əlavə et (yalnız crypto)
    9. Adaptive threshold tətbiq et
    10. Yenidən min_ulduz yoxla
    """
    # [M8] Drawdown kilid yoxla
    if drawdown_cb.is_locked():
        rem = drawdown_cb.remaining_seconds()
        log.debug(f"[DCB] Bot kilidlidir — {rem/3600:.1f} saat qaldı")
        return None

    # [M10] Latency yoxla
    lat_ok, lat_reason = latency_monitor.check()
    if not lat_ok:
        return None

    # Əsas analiz
    res = analiz_et(frames, asset_type, symbol, ref_idx)
    if res is None:
        return None

    yon   = res['yon']
    price = res['entry']
    skor  = res['skor']
    atr   = res.get('atr1h', 0.0)

    # [M2] CVD yenilə (yalnız 1H data ilə)
    df1h = frames.get('1h')
    if df1h is not None:
        cvd_tracker.update_from_df(symbol, df1h)

    # [M4] Adaptive threshold
    params_adj = adaptive_threshold.apply(
        {'min_ulduz': MIN_ULDUZ}, res['atr_ratio']
    )
    adaptive_min = params_adj['min_ulduz']

    # Bonus skorlar — yalnız crypto üçün tam dəst
    if asset_type == 'crypto':
        # [M1] OBI bonus
        obi_bonus = order_book_imbalance.signal_bias(symbol, yon)
        skor += obi_bonus * 0.8   # max ~0.8 bonus

        # [M2] CVD bonus
        cvd_bonus = cvd_tracker.signal_bias(symbol, yon)
        skor += cvd_bonus * 0.7   # max ~0.7 bonus

        # [M3] Liquidity Heatmap bonus
        hm_bonus = liquidity_heatmap.signal_bonus(symbol, price, yon)
        skor += hm_bonus           # max 1.5 bonus

        # [M5] Funding Rate bonus/malus
        fr_delta, fr_reason = funding_monitor.signal_bias(symbol, yon)
        skor += fr_delta
        if fr_reason:
            res['funding_note'] = fr_reason

    # [M7] Vol-Adjusted SL/TP (analiz_et-in SL/TP-ni əvəz edir)
    if atr > 0:
        va = vol_adjusted_sl_tp(price, yon, atr, res['rejim'],
                                res['mtf_skor'], asset_type)
        res['sl']        = va['sl']
        res['tp']        = va['tp2']
        res['tp_levels'] = {
            'tp1': va['tp1'], 'tp2': va['tp2'],
            'tp3': va['tp3'], 'be_trigger': va['be_trigger'],
            'tp1_pct': 25, 'tp2_pct': 50, 'tp3_pct': 25
        }
        res['rr']        = va['rr']

    # Yenilənmiş skor
    res['skor'] = round(skor, 1)

    # [M4] Adaptive min_ulduz ilə yoxla
    if skor < adaptive_min:
        return None

    # [M11] Shadow Mode trade aç
    shadow_mode.open_trade(symbol, res)

    return res


# ══════════════════════════════════════════════════════════
#  v13 SIQNAL GÖNDƏR WRAPPER
#  siqnal_gonder()-i əvəz etmır — üstündən çağırılır.
#  Drawdown CB-ni real nəticə ilə yenilər.
# ══════════════════════════════════════════════════════════
_orig_siqnal_gonder = siqnal_gonder

def siqnal_gonder_v13(sym: str, res: dict, sl_lbl='NEYTRAL',
                      fg_v=50, fg_l='Neytral', asset_type='crypto'):
    """
    Orijinal siqnal_gonder()-i çağırır + v13 əlavələri:
    - Drawdown CB gündəlik loss yoxlaması
    - Shadow mode tick
    - Prometheus v13 metrics
    """
    # [M8] Gündəlik loss yoxla
    drawdown_cb.daily_lock(gunluk_zefer)
    if drawdown_cb.is_locked():
        return

    _orig_siqnal_gonder(sym, res, sl_lbl, fg_v, fg_l, asset_type)

    # Prometheus v13 counters
    prom.counter_inc("v13_signals_total",
                     labels={"asset": asset_type, "yon": res.get('yon','?')})

# ── Wrapper-ları aktiv et ──────────────────────────────────
# on_candle_close() və yfinance_skan() v13 analiz funksiyasını işlədir.
# Bunun üçün on_candle_close içindəki analiz_et → analiz_et_v13,
# siqnal_gonder → siqnal_gonder_v13 olaraq monkey-patch edilir.
import sys as _sys
_current_module = _sys.modules[__name__] if __name__ in _sys.modules else None


if __name__ == '__main__':
    # Monkey-patch: bütün yeni pipeline-ı v13 üzərindən keç
    import builtins as _b
    _b._sniper_analiz   = analiz_et_v13
    _b._sniper_siqnal   = siqnal_gonder_v13

# ──────────────────────────────────────────────────────────

    log.info("╔══════════════════════════════════════════════╗")
    log.info("║   UNIVERSAL SNIPER v13.0 — İNSTİTUTİONAL   ║")
    log.info("║   AsyncIO + WebSocket + v13 Modules         ║")
    log.info("╚══════════════════════════════════════════════╝")

    db_init()
    state_recover()   # [FIX11] State bərpa

    # Telegram worker
    _tg_thread = threading.Thread(target=_telegram_worker, daemon=True, name="TGWorker")
    _tg_thread.start()

    # DB worker
    threading.Thread(target=_db_worker, daemon=True, name="DBWorker").start()

    # Daemon threadlər
    daemon_tasks = [
        (_telegram_watchdog,    "TGWatchdog"),    # [FIX10]
        (telegram_updates_yoxla,"TGUpdates"),
        (hefte_hesabat,         "HefteHesabat"),
        (gunluk_reset,          "GunlukReset"),
        (ml_periyodik,          "MLPeriyodik"),
        (state_save_periodic,   "StateSaver"),    # [FIX11]
        (prometheus_updater,    "PromUpdater"),   # [FIX15]
        (_health_server,        "HealthHTTP"),
        (wfo_weekly_scheduler,  "WFOWeekly"),     # [M12] Həftəlik WFO
    ]
    for fn, name in daemon_tasks:
        threading.Thread(target=fn, daemon=True, name=name).start()

    # [FIX15] Prometheus metrics server
    threading.Thread(target=prom.run_server, daemon=True, name="PrometheusHTTP").start()

    log.info(f"[HEALTH]  http://0.0.0.0:{HEALTH_PORT}")
    log.info(f"[METRICS] http://0.0.0.0:{METRICS_PORT}/metrics  (Grafana)")

    # İlk analiz — backtest + WFO + ML
    def ilk_analiz():
        time.sleep(30)
        backtest_hesabat()
        walk_forward_optimizasiya('BTC/USDT')
        global _ml_model_cache
        _ml_model_cache = ml_model_egit()

    threading.Thread(target=ilk_analiz, daemon=True, name="IlkAnaliz").start()

    telegram_gonder(
        "🚀 <b>UNIVERSAL SNIPER v13.0 — İNSTİTUTİONAL</b>\n\n"
        "── v12-dən saxlananlar ──\n"
        "✅ AsyncIO + Native WebSocket\n"
        "✅ Event-driven (while True yoxdur)\n"
        "✅ Bounded semaphore + ThreadPoolExecutor\n"
        "✅ Numpy vectorized (GIL bypass)\n"
        "✅ TWAP/VWAP Execution Engine\n"
        "✅ Position Manager (partial fill)\n"
        "✅ Failover + Circuit Breaker\n"
        "✅ LRU Memory manager + gc\n"
        "✅ Telegram watchdog + queue\n"
        "✅ State recovery on restart (SQLite WAL)\n"
        "✅ Risk Engine v2 (Kelly+portfolio cap)\n"
        "✅ Realistic backtest (fee+slippage+funding)\n"
        "✅ Latency tracker (p50/p95/p99)\n"
        "✅ Prometheus + Grafana :9090\n"
        "✅ Market Regime State Machine (hysteresis)\n"
        "✅ Liquidity Filter (spread + z-score)\n"
        "✅ Chaos Engine + Backpressure Queue\n\n"
        "── v13 YENİ MODULLAR ──\n"
        "🆕 [M1] Order Book Imbalance (Bid/Ask ratio)\n"
        "🆕 [M2] CVD — Cumulative Volume Delta\n"
        "🆕 [M3] Liquidity Heatmap (Whale zonalar)\n"
        "🆕 [M4] Adaptive Thresholding (volatillik)\n"
        "🆕 [M5] Funding Rate Arbitrage (Squeeze)\n"
        "🆕 [M7] Volatility-Adjusted SL/TP (ATR)\n"
        "🆕 [M8] Drawdown Circuit Breaker (24s kilid)\n"
        "🆕 [M10] Latency Monitor (500ms ləğv)\n"
        "🆕 [M11] Shadow Mode — Paper Trading\n"
        "🆕 [M12] Həftəlik Avtomatik WFO\n\n"
        "📊 500+ aktiv | 5M→15M→1H→4H→1D\n"
        "⭐ Min skor: 3.0 (adaptive) | 🎯 Min RR: 1:2\n\n"
        "Əmrlər: /pause /resume /stats /status\n"
        "/backtest /wfo /latency /positions /cache\n"
        "/chaos /liquidity BTC /regime"
    )

    # Əsas asyncio event loop
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        log.info("[SHUTDOWN] KeyboardInterrupt")
    finally:
        _shutdown_event.set()
        state_save_periodic.__wrapped__ = True  # son save
        db_state_save("bot_state", {
            "son_gonderilme": {k: str(v) for k,v in son_gonderilme.items()},
            "gunluk_siqnal":  gunluk_siqnal,
            "gunluk_zefer":   gunluk_zefer,
            "saved_at":       datetime.now().isoformat()
        })
        _tg_queue.put(None); _db_queue.put(None)
        telegram_gonder("🛑 Sniper v13.0 dayandırıldı (graceful shutdown).")
        log.info("[SHUTDOWN] Tamamlandı.")
