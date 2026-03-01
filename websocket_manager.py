import websocket
import json
import time
import base64
import threading
from datetime import datetime, timezone, timedelta
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend

class KalshiWebSocketManager:
    def __init__(self, api_key_id, private_key_pem):
        self.api_key_id = api_key_id
        self.private_key_pem = private_key_pem
        self.private_key = serialization.load_pem_private_key(
            private_key_pem.encode(),
            password=None,
            backend=default_backend()
        )
        
        self.ws = None
        self.connected = False
        self.orderbooks = {}
        self.subscribed_tickers = set()
        self.pending_tickers = set()
        self.lock = threading.Lock()
        self.ws_thread = None
        self.on_delta_callback = None
        
    @staticmethod
    def _merge_levels(existing, delta):
        """Merge delta price levels into existing orderbook side.
        Delta levels with qty 0 remove that price. Others update/add."""
        book = {price: qty for price, qty in existing}
        for price, qty in delta:
            if qty == 0:
                book.pop(price, None)
            else:
                book[price] = qty
        return sorted(book.items(), key=lambda x: x[0])

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
        
        if data.get('type') == 'orderbook_snapshot':
            ticker = data['msg'].get('market_ticker')
            ob_snapshot = None
            with self.lock:
                self.orderbooks[ticker] = {
                    'orderbook': {
                        'yes': data['msg'].get('yes', []),
                        'no': data['msg'].get('no', [])
                    }
                }
                self.pending_tickers.discard(ticker)
                if self.on_delta_callback:
                    ob_snapshot = {
                        'orderbook': {
                            'yes': list(data['msg'].get('yes', [])),
                            'no': list(data['msg'].get('no', []))
                        }
                    }
            if ob_snapshot and self.on_delta_callback:
                try:
                    self.on_delta_callback(ticker, ob_snapshot)
                except Exception as e:
                    print(f"Snapshot callback error: {e}")

        elif data.get('type') == 'orderbook_delta':
            ticker = data['msg'].get('market_ticker')
            ob_snapshot = None
            with self.lock:
                if ticker in self.orderbooks:
                    delta = data['msg']
                    if 'yes' in delta:
                        self.orderbooks[ticker]['orderbook']['yes'] = self._merge_levels(
                            self.orderbooks[ticker]['orderbook'].get('yes', []),
                            delta['yes']
                        )
                    if 'no' in delta:
                        self.orderbooks[ticker]['orderbook']['no'] = self._merge_levels(
                            self.orderbooks[ticker]['orderbook'].get('no', []),
                            delta['no']
                        )
                    if self.on_delta_callback:
                        ob_snapshot = {
                            'orderbook': {
                                'yes': list(self.orderbooks[ticker]['orderbook'].get('yes', [])),
                                'no': list(self.orderbooks[ticker]['orderbook'].get('no', []))
                            }
                        }
            if ob_snapshot and self.on_delta_callback:
                try:
                    self.on_delta_callback(ticker, ob_snapshot)
                except Exception as e:
                    print(f"Delta callback error: {e}")
    
    def _on_error(self, ws, error):
        print(f'WebSocket Error: {error}')
        self.connected = False
    
    def _on_close(self, ws, close_status_code, close_msg):
        print(f'WebSocket Closed: {close_status_code}')
        self.connected = False
        old_tickers = list(self.subscribed_tickers)
        self.subscribed_tickers.clear()
        if old_tickers:
            threading.Thread(target=self._reconnect, args=(old_tickers,), daemon=True).start()

    def _reconnect(self, tickers, max_retries=5):
        for attempt in range(max_retries):
            time.sleep(2 ** attempt)
            print(f"[WS] Reconnect attempt {attempt + 1}/{max_retries}...")
            try:
                if self.connect():
                    self.subscribe_to_orderbooks(tickers)
                    print(f"[WS] Reconnected and resubscribed to {len(tickers)} tickers")
                    return
            except Exception as e:
                print(f"[WS] Reconnect failed: {e}")
        print("[WS] All reconnect attempts exhausted")
    
    def _on_open(self, ws):
        print('WebSocket Connected!')
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
    
    def refresh_orderbooks(self, tickers):
        if not self.connected:
            if not self.connect():
                print('Failed to connect WebSocket')
                return False
        
        with self.lock:
            for ticker in tickers:
                self.orderbooks.pop(ticker, None)
            self.pending_tickers = set(tickers)
        
        if self.subscribed_tickers:
            try:
                unsubscribe_msg = {
                    'id': int(time.time()),
                    'cmd': 'unsubscribe',
                    'params': {
                        'channels': ['orderbook_delta'],
                        'market_tickers': list(self.subscribed_tickers)
                    }
                }
                self.ws.send(json.dumps(unsubscribe_msg))
                self.subscribed_tickers.clear()
                time.sleep(0.1)
            except Exception as e:
                print(f"Error unsubscribing: {e}")
        
        subscribe_msg = {
            'id': int(time.time()),
            'cmd': 'subscribe',
            'params': {
                'channels': ['orderbook_delta'],
                'market_tickers': tickers
            }
        }
        self.ws.send(json.dumps(subscribe_msg))
        self.subscribed_tickers = set(tickers)
        
        timeout = 10
        start = time.time()
        while time.time() - start < timeout:
            with self.lock:
                received = len(self.orderbooks)
                pending = len(self.pending_tickers)
            if received >= len(tickers) * 0.95 or pending == 0:
                break
            time.sleep(0.05)
        
        return True
    
    def subscribe_to_orderbooks(self, tickers):
        if not self.connected:
            if not self.connect():
                print('Failed to connect WebSocket')
                return False
        
        new_tickers = [t for t in tickers if t not in self.subscribed_tickers]
        
        if new_tickers:
            with self.lock:
                self.pending_tickers.update(new_tickers)
            
            subscribe_msg = {
                'id': int(time.time()),
                'cmd': 'subscribe',
                'params': {
                    'channels': ['orderbook_delta'],
                    'market_tickers': new_tickers
                }
            }
            self.ws.send(json.dumps(subscribe_msg))
            self.subscribed_tickers.update(new_tickers)
            
            timeout = 10
            start = time.time()
            while time.time() - start < timeout:
                with self.lock:
                    pending = len([t for t in new_tickers if t in self.pending_tickers])
                if pending <= len(new_tickers) * 0.05:
                    break
                time.sleep(0.05)
        
        return True
    
    def ensure_subscribed(self, tickers):
        """Subscribe to new tickers incrementally. Non-blocking — does not wait for snapshots."""
        if not self.connected:
            if not self.connect():
                print('Failed to connect WebSocket')
                return False

        new_tickers = [t for t in tickers if t not in self.subscribed_tickers]

        if new_tickers:
            with self.lock:
                self.pending_tickers.update(new_tickers)

            subscribe_msg = {
                'id': int(time.time()),
                'cmd': 'subscribe',
                'params': {
                    'channels': ['orderbook_delta'],
                    'market_tickers': new_tickers
                }
            }
            self.ws.send(json.dumps(subscribe_msg))
            self.subscribed_tickers.update(new_tickers)
            print(f"[WS] Subscribed to {len(new_tickers)} new tickers ({len(self.subscribed_tickers)} total)")

        return True

    def get_cached_orderbooks(self, tickers):
        """Return cached orderbooks without subscribing or waiting."""
        with self.lock:
            return {t: self.orderbooks[t] for t in tickers if t in self.orderbooks}

    def get_orderbooks(self, tickers):
        self.subscribe_to_orderbooks(tickers)

        with self.lock:
            return {t: self.orderbooks.get(t) for t in tickers if t in self.orderbooks}

    def get_fresh_orderbooks(self, tickers):
        self.refresh_orderbooks(tickers)

        with self.lock:
            return {t: self.orderbooks.get(t) for t in tickers if t in self.orderbooks}

    def resnapshot_tickers(self, tickers):
        """Resubscribe to specific tickers to get fresh snapshots.
        Other subscriptions are unaffected."""
        if not self.connected or not tickers:
            return False

        # Only resnapshot tickers we're actually subscribed to
        tickers = [t for t in tickers if t in self.subscribed_tickers]
        if not tickers:
            return False

        try:
            unsubscribe_msg = {
                'id': int(time.time()),
                'cmd': 'unsubscribe',
                'params': {
                    'channels': ['orderbook_delta'],
                    'market_tickers': tickers
                }
            }
            self.ws.send(json.dumps(unsubscribe_msg))
            for t in tickers:
                self.subscribed_tickers.discard(t)
            with self.lock:
                self.pending_tickers.update(tickers)
            time.sleep(0.05)

            subscribe_msg = {
                'id': int(time.time()),
                'cmd': 'subscribe',
                'params': {
                    'channels': ['orderbook_delta'],
                    'market_tickers': tickers
                }
            }
            self.ws.send(json.dumps(subscribe_msg))
            self.subscribed_tickers.update(tickers)

            # Wait for snapshots (up to 5s for targeted resnapshot)
            timeout = 5
            start = time.time()
            while time.time() - start < timeout:
                with self.lock:
                    pending = len([t for t in tickers if t in self.pending_tickers])
                if pending == 0:
                    break
                time.sleep(0.05)

            return True
        except Exception as e:
            print(f"[WS] Targeted re-snapshot failed: {e}")
            return False

    def resnapshot(self):
        """Resubscribe to all current tickers to get fresh snapshots.
        Does NOT clear the cache — stale data is served until overwritten."""
        if not self.connected or not self.subscribed_tickers:
            return False

        tickers = list(self.subscribed_tickers)
        print(f"[WS] Re-snapshotting {len(tickers)} tickers...")

        try:
            # Unsubscribe all
            unsubscribe_msg = {
                'id': int(time.time()),
                'cmd': 'unsubscribe',
                'params': {
                    'channels': ['orderbook_delta'],
                    'market_tickers': tickers
                }
            }
            self.ws.send(json.dumps(unsubscribe_msg))
            self.subscribed_tickers.clear()
            with self.lock:
                self.pending_tickers = set(tickers)
            time.sleep(0.1)

            # Resubscribe — triggers fresh snapshots
            subscribe_msg = {
                'id': int(time.time()),
                'cmd': 'subscribe',
                'params': {
                    'channels': ['orderbook_delta'],
                    'market_tickers': tickers
                }
            }
            self.ws.send(json.dumps(subscribe_msg))
            self.subscribed_tickers = set(tickers)

            # Wait for snapshots (up to 10s)
            timeout = 10
            start = time.time()
            while time.time() - start < timeout:
                with self.lock:
                    pending = len(self.pending_tickers)
                if pending <= len(tickers) * 0.05:
                    break
                time.sleep(0.05)

            with self.lock:
                received = len(tickers) - len(self.pending_tickers)
            print(f"[WS] Re-snapshot complete: {received}/{len(tickers)} refreshed")
            return True
        except Exception as e:
            print(f"[WS] Re-snapshot failed: {e}")
            return False

    def set_delta_callback(self, callback):
        """Register a callback invoked on every orderbook_delta.
        Signature: callback(ticker, orderbook_data)"""
        self.on_delta_callback = callback

    def close(self):
        if self.ws:
            self.ws.close()
            self.connected = False
