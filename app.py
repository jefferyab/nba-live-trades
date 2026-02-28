from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from pathlib import Path
from kalshi_python import Configuration, KalshiClient
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

_PACIFIC = ZoneInfo("America/Los_Angeles")
from collections import defaultdict, deque
import asyncio
import concurrent.futures
import websocket
import json
import time
import base64
import threading
import re
import os
import sqlite3
import unicodedata
import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend

# ============================================================
# CONFIGURATION
# ============================================================

config = Configuration(host="https://api.elections.kalshi.com/trade-api/v2")

KALSHI_API_KEY_ID = os.environ.get("KALSHI_API_KEY_ID", "b356b61d-e88c-41ed-a9fd-895025759202")
KALSHI_PRIVATE_KEY = os.environ.get("KALSHI_PRIVATE_KEY", None)

if KALSHI_PRIVATE_KEY is None:
    try:
        with open("NBA opto read and write.pem", "r") as f:
            KALSHI_PRIVATE_KEY = f.read()
    except FileNotFoundError:
        print("Warning: No Kalshi private key found")
        KALSHI_PRIVATE_KEY = ""

config.api_key_id = KALSHI_API_KEY_ID
config.private_key_pem = KALSHI_PRIVATE_KEY
client = KalshiClient(config)

ODDS_API_KEY = "565cc76e6d85a1f7315d48a2ed9396ac"

NBA_SERIES = ["KXNBAPTS", "KXNBAREB", "KXNBAAST", "KXNBA3PT"]

# ============================================================
# NAME NORMALIZATION
# ============================================================

_SUFFIX_RE = re.compile(r'\s+(jr\.?|sr\.?|iii|ii|iv)$', re.IGNORECASE)

_NAME_ALIASES = {
    'nicolas claxton': 'nic claxton',
    'cameron johnson': 'cam johnson',
    'cameron thomas': 'cam thomas',
    'herbert jones': 'herb jones',
    'kenyon martin': 'kenyon martin',
    'alexandre sarr': 'alex sarr',
    'carlton carrington': 'bub carrington',
    'nicholas richards': 'nick richards',
    'timothy hardaway': 'tim hardaway',
    'ronald holland': 'ron holland',
    'gregory jackson': 'gg jackson',
    'jakob poeltl': 'jakob poltl',
}

def normalize_name(name):
    """Normalize player name: strip accents/periods, remove suffixes, lowercase, resolve aliases."""
    nfkd = unicodedata.normalize('NFKD', name)
    ascii_name = ''.join(c for c in nfkd if not unicodedata.combining(c))
    ascii_name = ascii_name.replace('.', '')
    ascii_name = ascii_name.lower().strip()
    ascii_name = _SUFFIX_RE.sub('', ascii_name).strip()
    return _NAME_ALIASES.get(ascii_name, ascii_name)
PROP_TYPE_MAP = {
    "KXNBAPTS": "Points",
    "KXNBAREB": "Rebounds",
    "KXNBAAST": "Assists",
    "KXNBA3PT": "Threes",
}

# ============================================================
# TICKER DISCOVERY
# ============================================================

class TickerCache:
    def __init__(self):
        self.tickers = []
        self.metadata = {}  # ticker -> {player, prop_type, line, game_slug, title}
        self.games = set()
        self.lock = threading.Lock()

    def update(self, tickers, metadata, games):
        with self.lock:
            self.tickers = tickers
            self.metadata = metadata
            self.games = games


def _fetch_series_markets(series_ticker, cutoff_time):
    try:
        markets = client.get_markets(series_ticker=series_ticker, limit=500, status="open")
        return series_ticker, [m for m in markets.markets if m.open_time >= cutoff_time]
    except Exception as e:
        print(f"[TICKER] Failed to fetch {series_ticker}: {e}")
        return series_ticker, []


def discover_tickers():
    """Discover all NBA prop tickers and build metadata."""
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=48)

    all_tickers = []
    metadata = {}
    games = set()

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(_fetch_series_markets, s, cutoff): s for s in NBA_SERIES}
        for future in concurrent.futures.as_completed(futures):
            series_ticker, markets = future.result()
            prop_type = PROP_TYPE_MAP.get(series_ticker, "Unknown")

            for market in markets:
                ticker = market.ticker
                all_tickers.append(ticker)

                # Parse player/line from title: "Player Name: 27+ points"
                player = "Unknown"
                line = ""
                threshold = None
                match = re.match(r'(.+?):\s*(\d+)\+\s*(points|rebounds|assists|threes)', market.title.lower())
                if match:
                    player = normalize_name(match.group(1)).title()
                    threshold = int(match.group(2))
                    line = f"{match.group(2)}+ {match.group(3)}"

                # Game slug from ticker parts
                parts = ticker.split('-')
                game_slug = parts[1] if len(parts) >= 2 else ""
                games.add(game_slug)

                metadata[ticker] = {
                    "player": player,
                    "prop_type": prop_type,
                    "line": line,
                    "threshold": threshold,
                    "game_slug": game_slug,
                    "title": market.title,
                }

    ticker_cache.update(all_tickers, metadata, games)
    print(f"[TICKER] Discovered {len(all_tickers)} tickers across {len(games)} game slugs")
    return all_tickers


# ============================================================
# FANDUEL ODDS (for EV% calculation)
# ============================================================

_fanduel_cache = {}  # (player, stat_type, threshold, side) -> {price, is_alternate}
_fanduel_lock = threading.Lock()

PROP_TYPE_TO_STAT = {
    "Points": "points",
    "Rebounds": "rebounds",
    "Assists": "assists",
    "Threes": "threes",
}


def american_to_cents(decimal_odds):
    if decimal_odds >= 2.0:
        american = (decimal_odds - 1) * 100
        implied_prob = 100 / (american + 100)
        return round(implied_prob * 100, 2)
    else:
        american = -100 / (decimal_odds - 1)
        implied_prob = abs(american) / (abs(american) + 100)
        return round(implied_prob * 100, 2)


def _fetch_event_odds(session, event_id):
    base_url = "https://api.the-odds-api.com/v4"
    odds_url = f"{base_url}/sports/basketball_nba/events/{event_id}/odds"
    params = {
        'apiKey': ODDS_API_KEY,
        'regions': 'us',
        'markets': 'player_points,player_rebounds,player_assists,player_threes,player_points_alternate,player_rebounds_alternate,player_assists_alternate,player_threes_alternate',
        'bookmakers': 'fanduel'
    }
    try:
        resp = session.get(odds_url, params=params, timeout=10)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return None


def fetch_fanduel_props():
    """Fetch all FanDuel player props and update the cache."""
    all_props = {}
    try:
        events_url = "https://api.the-odds-api.com/v4/sports/basketball_nba/events"
        response = requests.get(events_url, params={'apiKey': ODDS_API_KEY}, timeout=30)
        if response.status_code != 200:
            print(f"[FANDUEL] Events fetch failed: {response.status_code}")
            return

        events = response.json()
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=20)
        session.mount('https://', adapter)

        stat_type_map = {
            'player_points': 'points', 'player_rebounds': 'rebounds',
            'player_assists': 'assists', 'player_threes': 'threes',
            'player_points_alternate': 'points', 'player_rebounds_alternate': 'rebounds',
            'player_assists_alternate': 'assists', 'player_threes_alternate': 'threes',
        }

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(events) or 1) as executor:
            futures = {executor.submit(_fetch_event_odds, session, e['id']): e for e in events}
            for future in concurrent.futures.as_completed(futures):
                odds_data = future.result()
                if not odds_data:
                    continue
                for bookmaker in odds_data.get('bookmakers', []):
                    if bookmaker['key'] == 'fanduel':
                        for market in bookmaker.get('markets', []):
                            market_key = market['key']
                            is_alternate = '_alternate' in market_key
                            stat_type = stat_type_map.get(market_key)
                            if not stat_type:
                                continue
                            for outcome in market.get('outcomes', []):
                                player = normalize_name(outcome.get('description', ''))
                                threshold = outcome.get('point')
                                side = outcome.get('name', '')
                                price = outcome.get('price')
                                if not player or threshold is None:
                                    continue
                                key = (player, stat_type, threshold, side)
                                if key not in all_props:
                                    all_props[key] = {'price': price, 'is_alternate': is_alternate}
                                elif all_props[key]['is_alternate'] and not is_alternate:
                                    all_props[key] = {'price': price, 'is_alternate': is_alternate}

        with _fanduel_lock:
            global _fanduel_cache
            _fanduel_cache = all_props
        print(f"[FANDUEL] Cached {len(all_props)} FanDuel lines")

    except Exception as e:
        print(f"[FANDUEL] Error: {e}")


def get_fanduel_under_price(player_name_lower, kalshi_threshold, stat_type):
    with _fanduel_lock:
        props = _fanduel_cache
    if not props:
        return None, None

    target = kalshi_threshold - 0.5

    under_key = (player_name_lower, stat_type, target, 'Under')
    if under_key in props:
        return american_to_cents(props[under_key]['price']), props[under_key].get('is_alternate')

    over_key = (player_name_lower, stat_type, target, 'Over')
    if over_key in props:
        return 100 - american_to_cents(props[over_key]['price']), props[over_key].get('is_alternate')

    for diff in [0.5, 1.0, 1.5, -0.5, -1.0, -1.5]:
        ct = target + diff
        uk = (player_name_lower, stat_type, ct, 'Under')
        if uk in props:
            return american_to_cents(props[uk]['price']), props[uk].get('is_alternate')
        ok = (player_name_lower, stat_type, ct, 'Over')
        if ok in props:
            return 100 - american_to_cents(props[ok]['price']), props[ok].get('is_alternate')

    return None, None


def get_fanduel_over_price(player_name_lower, kalshi_threshold, stat_type):
    with _fanduel_lock:
        props = _fanduel_cache
    if not props:
        return None, None

    target = kalshi_threshold - 0.5

    over_key = (player_name_lower, stat_type, target, 'Over')
    if over_key in props:
        return american_to_cents(props[over_key]['price']), props[over_key].get('is_alternate')

    under_key = (player_name_lower, stat_type, target, 'Under')
    if under_key in props:
        return 100 - american_to_cents(props[under_key]['price']), props[under_key].get('is_alternate')

    for diff in [0.5, 1.0, 1.5, -0.5, -1.0, -1.5]:
        ct = target + diff
        ok = (player_name_lower, stat_type, ct, 'Over')
        if ok in props:
            return american_to_cents(props[ok]['price']), props[ok].get('is_alternate')
        uk = (player_name_lower, stat_type, ct, 'Under')
        if uk in props:
            return 100 - american_to_cents(props[uk]['price']), props[uk].get('is_alternate')

    return None, None


def calculate_ev(kalshi_price, fanduel_price):
    if fanduel_price == 0:
        return 0
    return round(((fanduel_price - kalshi_price) / fanduel_price) * 100, 2)


# ============================================================
# TRADE WEBSOCKET (Kalshi)
# ============================================================

class TradeWebSocket:
    def __init__(self, api_key_id, private_key_pem):
        self.api_key_id = api_key_id
        self.private_key = serialization.load_pem_private_key(
            private_key_pem.encode(),
            password=None,
            backend=default_backend()
        )
        self.ws = None
        self.connected = False
        self.subscribed_tickers = set()
        self.on_trade_callback = None
        self.on_orderbook_callback = None
        self.ws_thread = None
        self.orderbooks = {}  # ticker -> {'yes': [...], 'no': [...]}
        self.ob_lock = threading.Lock()

    def _generate_auth_headers(self):
        timestamp_str = str(int(datetime.now().timestamp() * 1000))
        method = 'GET'
        path = '/trade-api/ws/v2'
        message = (timestamp_str + method + path).encode('utf-8')

        signature = self.private_key.sign(
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH
            ),
            hashes.SHA256()
        )

        signature_b64 = base64.b64encode(signature).decode('utf-8')
        return {
            'KALSHI-ACCESS-KEY': self.api_key_id,
            'KALSHI-ACCESS-SIGNATURE': signature_b64,
            'KALSHI-ACCESS-TIMESTAMP': timestamp_str
        }

    def _on_message(self, ws, message):
        data = json.loads(message)
        msg_type = data.get('type')

        if msg_type == 'trade' and self.on_trade_callback:
            try:
                self.on_trade_callback(data['msg'])
            except Exception as e:
                print(f"[TRADE WS] Callback error: {e}")

        elif msg_type == 'orderbook_snapshot':
            ticker = data['msg'].get('market_ticker')
            with self.ob_lock:
                self.orderbooks[ticker] = {
                    'yes': data['msg'].get('yes', []),
                    'no': data['msg'].get('no', []),
                }
            if self.on_orderbook_callback:
                try:
                    self.on_orderbook_callback(ticker)
                except Exception as e:
                    print(f"[TRADE WS] OB snapshot callback error: {e}")

        elif msg_type == 'orderbook_delta':
            ticker = data['msg'].get('market_ticker')
            with self.ob_lock:
                if ticker in self.orderbooks:
                    delta = data['msg']
                    if 'yes' in delta:
                        self.orderbooks[ticker]['yes'] = delta['yes']
                    if 'no' in delta:
                        self.orderbooks[ticker]['no'] = delta['no']
            if self.on_orderbook_callback:
                try:
                    self.on_orderbook_callback(ticker)
                except Exception as e:
                    print(f"[TRADE WS] OB delta callback error: {e}")

    def _on_error(self, ws, error):
        print(f'[TRADE WS] Error: {error}')
        self.connected = False

    def _on_close(self, ws, close_status_code, close_msg):
        print(f'[TRADE WS] Closed: {close_status_code}')
        self.connected = False
        old_tickers = list(self.subscribed_tickers)
        self.subscribed_tickers.clear()
        if old_tickers:
            threading.Thread(target=self._reconnect, args=(old_tickers,), daemon=True).start()

    def _reconnect(self, tickers):
        attempt = 0
        while True:
            delay = min(2 ** attempt, 60)  # cap at 60s
            time.sleep(delay)
            attempt += 1
            print(f"[TRADE WS] Reconnect attempt {attempt}...")
            try:
                if self.connect():
                    self.subscribe(tickers)
                    print(f"[TRADE WS] Reconnected, subscribed to {len(tickers)} tickers")
                    return
            except Exception as e:
                print(f"[TRADE WS] Reconnect failed: {e}")

    def _on_open(self, ws):
        print('[TRADE WS] Connected!')
        self.connected = True

    def connect(self):
        if self.connected:
            return True

        headers = self._generate_auth_headers()
        ws_url = 'wss://api.elections.kalshi.com/trade-api/ws/v2'

        self.ws = websocket.WebSocketApp(
            ws_url,
            header=headers,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close
        )

        self.ws_thread = threading.Thread(target=self.ws.run_forever)
        self.ws_thread.daemon = True
        self.ws_thread.start()

        timeout = 5
        start = time.time()
        while not self.connected and time.time() - start < timeout:
            time.sleep(0.1)

        return self.connected

    def subscribe(self, tickers):
        if not tickers:
            return
        ticker_list = list(tickers)
        # Subscribe to both trade and orderbook_delta channels
        msg = {
            'id': int(time.time()),
            'cmd': 'subscribe',
            'params': {
                'channels': ['trade', 'orderbook_delta'],
                'market_tickers': ticker_list
            }
        }
        self.ws.send(json.dumps(msg))
        self.subscribed_tickers = set(tickers)
        print(f"[TRADE WS] Subscribed to trade + orderbook_delta for {len(ticker_list)} tickers")

    def resubscribe(self, tickers):
        if self.subscribed_tickers:
            try:
                unsub = {
                    'id': int(time.time()),
                    'cmd': 'unsubscribe',
                    'params': {
                        'channels': ['trade', 'orderbook_delta'],
                        'market_tickers': list(self.subscribed_tickers)
                    }
                }
                self.ws.send(json.dumps(unsub))
                self.subscribed_tickers.clear()
                time.sleep(0.1)
            except Exception as e:
                print(f"[TRADE WS] Unsub error: {e}")
        self.subscribe(tickers)

    def close(self):
        if self.ws:
            self.ws.close()
            self.connected = False


# ============================================================
# TRADE STORE (SQLite-backed with in-memory cache)
# ============================================================

DATA_DIR = os.environ.get("DATA_DIR", os.path.dirname(os.path.abspath(__file__)))

TRADE_COLUMNS = [
    'trade_id', 'ticker', 'player', 'prop_type', 'line', 'title',
    'game_slug', 'taker_side', 'yes_price', 'no_price', 'count',
    'dollar_amount', 'ev_percent', 'fanduel_price', 'ts', 'time_str',
]


class TradeStore:
    STATS_WINDOW = 6 * 3600  # 6 hours default
    MAX_AGE = 24 * 3600      # 24 hours retention

    def __init__(self):
        db_path = os.path.join(DATA_DIR, "trades.db")
        self.db = sqlite3.connect(db_path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.lock = threading.Lock()
        self._trade_count = 0  # session counter for broadcast cadence
        self._create_table()
        # In-memory cache for fast WebSocket bootstrap
        self.recent_trades = deque(maxlen=500)
        self._load_recent()

    def _create_table(self):
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                trade_id TEXT PRIMARY KEY,
                ticker TEXT,
                player TEXT,
                prop_type TEXT,
                line TEXT,
                title TEXT,
                game_slug TEXT,
                taker_side TEXT,
                yes_price INTEGER,
                no_price INTEGER,
                count INTEGER,
                dollar_amount REAL,
                ev_percent REAL,
                fanduel_price REAL,
                ts REAL,
                time_str TEXT
            )
        """)
        self.db.execute("CREATE INDEX IF NOT EXISTS idx_trades_ts ON trades(ts)")
        self.db.commit()

    def _load_recent(self):
        cutoff = time.time() - self.MAX_AGE
        rows = self.db.execute(
            "SELECT * FROM trades WHERE ts >= ? ORDER BY ts DESC LIMIT 500",
            (cutoff,)
        ).fetchall()
        for row in rows:
            self.recent_trades.append(dict(row))
        print(f"[DB] Loaded {len(rows)} trades from disk")

    def process_trade(self, raw_trade):
        ticker = raw_trade.get('market_ticker', '')
        meta = ticker_cache.metadata.get(ticker)
        if not meta:
            return None

        taker_side = raw_trade.get('taker_side', 'yes')
        yes_price = raw_trade.get('yes_price', 0)
        no_price = raw_trade.get('no_price', 0)
        count = raw_trade.get('count', 0)

        price_paid = yes_price if taker_side == 'yes' else no_price
        dollar_amount = round(price_paid * count / 100, 2)

        ts = raw_trade.get('ts', 0)
        time_str = datetime.fromtimestamp(ts, tz=_PACIFIC).strftime('%I:%M:%S %p') if ts else ''

        # EV% calculation
        ev_percent = None
        fanduel_price = None
        threshold = meta.get('threshold')
        stat_type = PROP_TYPE_TO_STAT.get(meta['prop_type'])
        if threshold is not None and stat_type:
            player_lower = normalize_name(meta['player'])
            if taker_side == 'yes':
                fd_price, _ = get_fanduel_over_price(player_lower, threshold, stat_type)
            else:
                fd_price, _ = get_fanduel_under_price(player_lower, threshold, stat_type)
            if fd_price is not None and fd_price > 0:
                fanduel_price = round(fd_price, 2)
                ev_percent = calculate_ev(price_paid, fanduel_price)

        enriched = {
            'trade_id': raw_trade.get('trade_id', ''),
            'ticker': ticker,
            'player': meta['player'],
            'prop_type': meta['prop_type'],
            'line': meta['line'],
            'title': meta['title'],
            'game_slug': meta['game_slug'],
            'taker_side': taker_side,
            'yes_price': yes_price,
            'no_price': no_price,
            'count': count,
            'dollar_amount': dollar_amount,
            'ev_percent': ev_percent,
            'fanduel_price': fanduel_price,
            'ts': ts,
            'time_str': time_str,
        }

        with self.lock:
            try:
                self.db.execute(
                    f"INSERT OR IGNORE INTO trades ({','.join(TRADE_COLUMNS)}) VALUES ({','.join('?' * len(TRADE_COLUMNS))})",
                    tuple(enriched[c] for c in TRADE_COLUMNS)
                )
                self.db.commit()
            except Exception as e:
                print(f"[DB] Insert error: {e}")
            self.recent_trades.appendleft(enriched)
            self._trade_count += 1

        return enriched

    def get_stats(self, window_seconds=None):
        window = window_seconds or self.STATS_WINDOW
        cutoff = time.time() - window

        with self.lock:
            rows = self.db.execute(
                "SELECT player, prop_type, ticker, count, dollar_amount FROM trades WHERE ts >= ?",
                (cutoff,)
            ).fetchall()

        volume_by_prop_type = defaultdict(lambda: {"count": 0, "contracts": 0, "dollar_volume": 0.0})
        volume_by_ticker = defaultdict(lambda: {"count": 0, "contracts": 0, "dollar_volume": 0.0})
        window_trades = 0
        window_contracts = 0
        window_volume = 0.0

        for row in rows:
            prop_type = row['prop_type']
            ticker = row['ticker']
            contracts = row['count']
            dollars = row['dollar_amount']

            volume_by_prop_type[prop_type]['count'] += 1
            volume_by_prop_type[prop_type]['contracts'] += contracts
            volume_by_prop_type[prop_type]['dollar_volume'] += dollars

            volume_by_ticker[ticker]['count'] += 1
            volume_by_ticker[ticker]['contracts'] += contracts
            volume_by_ticker[ticker]['dollar_volume'] += dollars

            window_trades += 1
            window_contracts += contracts
            window_volume += dollars

        top_markets = sorted(
            volume_by_ticker.items(),
            key=lambda x: x[1]['dollar_volume'], reverse=True
        )[:10]

        top_markets_enriched = []
        for ticker, vol in top_markets:
            meta = ticker_cache.metadata.get(ticker, {})
            top_markets_enriched.append({
                'ticker': ticker,
                'title': meta.get('title', ticker),
                'player': meta.get('player', ''),
                'prop_type': meta.get('prop_type', ''),
                **vol,
            })

        # Top individual trades by dollar amount
        with self.lock:
            top_trade_rows = self.db.execute(
                "SELECT player, title, prop_type, taker_side, yes_price, no_price, count, dollar_amount, time_str FROM trades WHERE ts >= ? ORDER BY dollar_amount DESC LIMIT 10",
                (cutoff,)
            ).fetchall()
        top_trades = [dict(r) for r in top_trade_rows]

        return {
            'total_trades': window_trades,
            'total_contracts': window_contracts,
            'total_dollar_volume': round(window_volume, 2),
            'top_trades': top_trades,
            'volume_by_prop_type': {k: dict(v) for k, v in volume_by_prop_type.items()},
            'top_markets': top_markets_enriched,
        }

    def get_recent_trades(self, limit=100):
        with self.lock:
            return list(self.recent_trades)[:limit]

    def get_trades_before(self, before_ts, limit=100):
        with self.lock:
            rows = self.db.execute(
                "SELECT * FROM trades WHERE ts < ? ORDER BY ts DESC LIMIT ?",
                (before_ts, limit)
            ).fetchall()
            return [dict(row) for row in rows]

    def cleanup(self):
        cutoff = time.time() - self.MAX_AGE
        with self.lock:
            result = self.db.execute("DELETE FROM trades WHERE ts < ?", (cutoff,))
            self.db.commit()
            deleted = result.rowcount
            # Trim in-memory cache
            while self.recent_trades and self.recent_trades[-1]['ts'] < cutoff:
                self.recent_trades.pop()
        if deleted:
            print(f"[DB] Cleaned up {deleted} trades older than 24h")


# ============================================================
# GLOBAL INSTANCES
# ============================================================

ticker_cache = TickerCache()
trade_store = TradeStore()
trade_ws = TradeWebSocket(KALSHI_API_KEY_ID, KALSHI_PRIVATE_KEY)

app = FastAPI()
connected_clients = []
_event_loop = None

# ============================================================
# ROUTES
# ============================================================

@app.get("/", response_class=HTMLResponse)
async def home():
    html_path = Path(__file__).parent / "templates" / "index.html"
    return HTMLResponse(content=html_path.read_text())


@app.get("/api/stats")
async def get_stats(window: int = None):
    return trade_store.get_stats(window_seconds=window)


@app.get("/api/trades")
async def get_trades(before_ts: float = None, limit: int = Query(default=100, le=500)):
    if before_ts is None:
        return trade_store.get_recent_trades(limit)
    return trade_store.get_trades_before(before_ts, limit)


@app.get("/api/health")
async def health():
    return {
        "status": "ok",
        "ws_connected": trade_ws.connected,
        "subscribed_tickers": len(trade_ws.subscribed_tickers),
        "total_trades": trade_store._trade_count,
        "connected_clients": len(connected_clients),
    }


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.append(websocket)
    try:
        await websocket.send_text(json.dumps({
            'type': 'bootstrap',
            'trades': trade_store.get_recent_trades(100),
            'stats': trade_store.get_stats(),
            'games': sorted(list(ticker_cache.games)),
            'cheap_nos': _get_cheap_nos_list(),
        }))
        while True:
            data = await websocket.receive_text()
            msg = json.loads(data)
            if msg.get('type') == 'ping':
                await websocket.send_text(json.dumps({'type': 'pong'}))
    except WebSocketDisconnect:
        if websocket in connected_clients:
            connected_clients.remove(websocket)
    except Exception:
        if websocket in connected_clients:
            connected_clients.remove(websocket)


# ============================================================
# CHEAP NO CONTRACTS (WebSocket-driven orderbook state)
# ============================================================

_cheap_nos = {}  # ticker -> {ticker, player, prop_type, line, game_slug, no_price, quantity}
_cheap_nos_lock = threading.Lock()


def _extract_cheap_no(ticker, yes_bids):
    """Given a ticker and its YES bids, return a cheap NO entry or None."""
    total_qty = 0
    best_no_price = None
    for price, qty in yes_bids:
        no_price = 100 - price
        if no_price <= 10:
            total_qty += qty
            if best_no_price is None or no_price < best_no_price:
                best_no_price = no_price

    if total_qty > 0:
        meta = ticker_cache.metadata.get(ticker)
        if meta:
            return {
                'ticker': ticker,
                'player': meta['player'],
                'prop_type': meta['prop_type'],
                'line': meta['line'],
                'game_slug': meta['game_slug'],
                'no_price': best_no_price,
                'quantity': total_qty,
            }
    return None


def _get_cheap_nos_list():
    """Return sorted list from current cheap_nos dict."""
    with _cheap_nos_lock:
        items = list(_cheap_nos.values())
    items.sort(key=lambda x: (x['no_price'], -x['quantity']))
    return items


def _broadcast_if_changed(changed):
    """Push cheap_nos list to all connected clients if changed."""
    if changed and _event_loop and connected_clients:
        items = _get_cheap_nos_list()
        _event_loop.call_soon_threadsafe(
            asyncio.ensure_future,
            _broadcast_cheap_nos(items)
        )


def on_orderbook_update(ticker):
    """Called from TradeWebSocket thread on every orderbook snapshot/delta."""
    # Snapshot YES bids under lock so we read exactly what was just written
    with trade_ws.ob_lock:
        ob = trade_ws.orderbooks.get(ticker)
        yes_bids = list(ob['yes']) if ob and 'yes' in ob else []

    entry = _extract_cheap_no(ticker, yes_bids)
    changed = False
    with _cheap_nos_lock:
        if entry:
            prev = _cheap_nos.get(ticker)
            if prev != entry:
                _cheap_nos[ticker] = entry
                changed = True
        else:
            if ticker in _cheap_nos:
                del _cheap_nos[ticker]
                changed = True

    _broadcast_if_changed(changed)


def _full_cheap_nos_reconcile():
    """Re-scan ALL in-memory orderbooks in one pass and rebuild cheap_nos."""
    # Single lock acquisition — snapshot all orderbooks at once
    with trade_ws.ob_lock:
        all_obs = {t: list(ob.get('yes', [])) for t, ob in trade_ws.orderbooks.items()}

    new_cheap = {}
    for ticker, yes_bids in all_obs.items():
        entry = _extract_cheap_no(ticker, yes_bids)
        if entry:
            new_cheap[ticker] = entry

    changed = False
    with _cheap_nos_lock:
        if new_cheap != _cheap_nos:
            _cheap_nos.clear()
            _cheap_nos.update(new_cheap)
            changed = True

    _broadcast_if_changed(changed)


async def _broadcast_cheap_nos(items):
    msg = json.dumps({'type': 'cheap_nos_update', 'data': items})
    for client in connected_clients[:]:
        try:
            await client.send_text(msg)
        except Exception:
            pass


# ============================================================
# TRADE CALLBACK → FRONTEND PUSH
# ============================================================

def on_trade(raw_trade):
    """Called from TradeWebSocket thread for every trade."""
    enriched = trade_store.process_trade(raw_trade)
    if enriched and connected_clients and _event_loop:
        _event_loop.call_soon_threadsafe(
            asyncio.ensure_future,
            broadcast_trade(enriched)
        )


async def broadcast_trade(trade):
    msg = json.dumps({'type': 'trade', 'data': trade})
    for client in connected_clients[:]:
        try:
            await client.send_text(msg)
        except Exception:
            pass

    # Broadcast stats every 10th trade
    if trade_store._trade_count % 10 == 0:
        stats_msg = json.dumps({'type': 'stats_update', 'data': trade_store.get_stats()})
        for client in connected_clients[:]:
            try:
                await client.send_text(stats_msg)
            except Exception:
                pass


# ============================================================
# STARTUP TASKS
# ============================================================

async def bootstrap():
    try:
        tickers = await asyncio.to_thread(discover_tickers)
        if not tickers:
            print("[STARTUP] No tickers found, will retry on refresh")
            return
        # Fetch FanDuel props for EV% calculation
        await asyncio.to_thread(fetch_fanduel_props)
        trade_ws.on_trade_callback = on_trade
        trade_ws.on_orderbook_callback = on_orderbook_update
        await asyncio.to_thread(trade_ws.connect)
        await asyncio.to_thread(trade_ws.subscribe, tickers)
        print(f"[STARTUP] Live trades + orderbook feed active for {len(tickers)} NBA prop tickers")
    except Exception as e:
        print(f"[STARTUP] Bootstrap failed: {e}")


async def periodic_ticker_refresh():
    while True:
        await asyncio.sleep(300)  # 5 minutes
        try:
            new_tickers = await asyncio.to_thread(discover_tickers)
            if not new_tickers:
                continue
            # If WS is disconnected, reconnect before subscribing
            if not trade_ws.connected:
                print("[REFRESH] WS disconnected, reconnecting...")
                connected = await asyncio.to_thread(trade_ws.connect)
                if connected:
                    await asyncio.to_thread(trade_ws.subscribe, new_tickers)
                    print(f"[REFRESH] Reconnected and subscribed to {len(new_tickers)} tickers")
                else:
                    print("[REFRESH] Reconnect failed, will retry next cycle")
            elif set(new_tickers) != trade_ws.subscribed_tickers:
                await asyncio.to_thread(trade_ws.resubscribe, new_tickers)
                print(f"[REFRESH] Updated subscription to {len(new_tickers)} tickers")
        except Exception as e:
            print(f"[REFRESH] Failed: {e}")


async def periodic_fanduel_refresh():
    while True:
        await asyncio.sleep(60)  # every 60 seconds
        try:
            await asyncio.to_thread(fetch_fanduel_props)
        except Exception as e:
            print(f"[FANDUEL REFRESH] Failed: {e}")


async def periodic_cheap_nos_reconcile():
    """Re-scan all orderbooks every 2 seconds to keep cheap NOs feed fresh."""
    while True:
        await asyncio.sleep(2)
        try:
            await asyncio.to_thread(_full_cheap_nos_reconcile)
        except Exception as e:
            print(f"[CHEAP NO RECONCILE] Failed: {e}")


async def periodic_trade_cleanup():
    while True:
        await asyncio.sleep(1800)  # every 30 minutes
        try:
            await asyncio.to_thread(trade_store.cleanup)
        except Exception as e:
            print(f"[CLEANUP] Failed: {e}")


@app.on_event("startup")
async def startup_event():
    global _event_loop
    _event_loop = asyncio.get_running_loop()
    trade_store.cleanup()  # clean stale data on startup
    asyncio.create_task(bootstrap())
    asyncio.create_task(periodic_ticker_refresh())
    asyncio.create_task(periodic_fanduel_refresh())
    asyncio.create_task(periodic_cheap_nos_reconcile())
    asyncio.create_task(periodic_trade_cleanup())


# ============================================================
# RUN SERVER
# ============================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
