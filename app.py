from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from pathlib import Path
from kalshi_python import Configuration, KalshiClient
from datetime import datetime, timezone, timedelta
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

NBA_SERIES = ["KXNBAPTS", "KXNBAREB", "KXNBAAST", "KXNBA3PT"]
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
                match = re.match(r'(.+?):\s*(\d+)\+\s*(points|rebounds|assists|threes)', market.title.lower())
                if match:
                    player = match.group(1).strip().title()
                    line = f"Over {match.group(2)}.5"

                # Game slug from ticker parts
                parts = ticker.split('-')
                game_slug = parts[1] if len(parts) >= 2 else ""
                games.add(game_slug)

                metadata[ticker] = {
                    "player": player,
                    "prop_type": prop_type,
                    "line": line,
                    "game_slug": game_slug,
                    "title": market.title,
                }

    ticker_cache.update(all_tickers, metadata, games)
    print(f"[TICKER] Discovered {len(all_tickers)} tickers across {len(games)} game slugs")
    return all_tickers


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
        self.ws_thread = None

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
        if data.get('type') == 'trade' and self.on_trade_callback:
            try:
                self.on_trade_callback(data['msg'])
            except Exception as e:
                print(f"[TRADE WS] Callback error: {e}")

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

    def _reconnect(self, tickers, max_retries=5):
        for attempt in range(max_retries):
            time.sleep(2 ** attempt)
            print(f"[TRADE WS] Reconnect attempt {attempt + 1}/{max_retries}...")
            try:
                if self.connect():
                    self.subscribe(tickers)
                    print(f"[TRADE WS] Reconnected, subscribed to {len(tickers)} tickers")
                    return
            except Exception as e:
                print(f"[TRADE WS] Reconnect failed: {e}")
        print("[TRADE WS] All reconnect attempts exhausted")

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
        msg = {
            'id': int(time.time()),
            'cmd': 'subscribe',
            'params': {
                'channels': ['trade'],
                'market_tickers': list(tickers)
            }
        }
        self.ws.send(json.dumps(msg))
        self.subscribed_tickers = set(tickers)
        print(f"[TRADE WS] Subscribed to trade channel for {len(tickers)} tickers")

    def resubscribe(self, tickers):
        if self.subscribed_tickers:
            try:
                unsub = {
                    'id': int(time.time()),
                    'cmd': 'unsubscribe',
                    'params': {
                        'channels': ['trade'],
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
# TRADE STORE (in-memory)
# ============================================================

class TradeStore:
    MAX_TRADES = 500

    def __init__(self):
        self.trades = deque(maxlen=self.MAX_TRADES)
        self.lock = threading.Lock()
        self.volume_by_player = defaultdict(lambda: {"count": 0, "contracts": 0, "dollar_volume": 0.0})
        self.volume_by_prop_type = defaultdict(lambda: {"count": 0, "contracts": 0, "dollar_volume": 0.0})
        self.volume_by_ticker = defaultdict(lambda: {"count": 0, "contracts": 0, "dollar_volume": 0.0})
        self.total_trades = 0
        self.total_contracts = 0
        self.total_dollar_volume = 0.0

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
        time_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%H:%M:%S') if ts else ''

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
            'ts': ts,
            'time_str': time_str,
        }

        with self.lock:
            self.trades.appendleft(enriched)
            self._update_aggregates(enriched)

        return enriched

    def _update_aggregates(self, trade):
        player = trade['player']
        prop_type = trade['prop_type']
        ticker = trade['ticker']
        contracts = trade['count']
        dollars = trade['dollar_amount']

        for bucket, key in [
            (self.volume_by_player, player),
            (self.volume_by_prop_type, prop_type),
            (self.volume_by_ticker, ticker),
        ]:
            bucket[key]['count'] += 1
            bucket[key]['contracts'] += contracts
            bucket[key]['dollar_volume'] += dollars

        self.total_trades += 1
        self.total_contracts += contracts
        self.total_dollar_volume += dollars

    def get_stats(self):
        with self.lock:
            top_players = sorted(
                self.volume_by_player.items(),
                key=lambda x: x[1]['dollar_volume'], reverse=True
            )[:10]

            top_markets = sorted(
                self.volume_by_ticker.items(),
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

            return {
                'total_trades': self.total_trades,
                'total_contracts': self.total_contracts,
                'total_dollar_volume': round(self.total_dollar_volume, 2),
                'top_players': [{'player': p, **v} for p, v in top_players],
                'volume_by_prop_type': {k: dict(v) for k, v in self.volume_by_prop_type.items()},
                'top_markets': top_markets_enriched,
            }

    def get_recent_trades(self, limit=50):
        with self.lock:
            return list(self.trades)[:limit]


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
async def get_stats():
    return trade_store.get_stats()


@app.get("/api/health")
async def health():
    return {
        "status": "ok",
        "ws_connected": trade_ws.connected,
        "subscribed_tickers": len(trade_ws.subscribed_tickers),
        "total_trades": trade_store.total_trades,
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
    if trade_store.total_trades % 10 == 0:
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
        trade_ws.on_trade_callback = on_trade
        await asyncio.to_thread(trade_ws.connect)
        await asyncio.to_thread(trade_ws.subscribe, tickers)
        print(f"[STARTUP] Live trades feed active for {len(tickers)} NBA prop tickers")
    except Exception as e:
        print(f"[STARTUP] Bootstrap failed: {e}")


async def periodic_ticker_refresh():
    while True:
        await asyncio.sleep(300)  # 5 minutes
        try:
            new_tickers = await asyncio.to_thread(discover_tickers)
            if set(new_tickers) != trade_ws.subscribed_tickers:
                await asyncio.to_thread(trade_ws.resubscribe, new_tickers)
                print(f"[REFRESH] Updated subscription to {len(new_tickers)} tickers")
        except Exception as e:
            print(f"[REFRESH] Failed: {e}")


@app.on_event("startup")
async def startup_event():
    global _event_loop
    _event_loop = asyncio.get_running_loop()
    asyncio.create_task(bootstrap())
    asyncio.create_task(periodic_ticker_refresh())


# ============================================================
# RUN SERVER
# ============================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
