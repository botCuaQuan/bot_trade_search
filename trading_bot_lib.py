# trading_bot_lib_part1.py
import json
import hmac
import hashlib
import time
import threading
import urllib.request
import urllib.parse
import numpy as np
import websocket
import logging
import requests
import os
import math
import traceback
import random
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
import time
import ssl

# ========== RATE LIMIT CHO BINANCE ==========
_BINANCE_LAST_REQUEST_TIME = 0
_BINANCE_RATE_LOCK = threading.Lock()
_BINANCE_MIN_INTERVAL = 0.1  # 10 request/giây

# ========== CACHE DANH SÁCH COIN USDC ==========
_USDC_CACHE = {
    "pairs": [],
    "last_update": 0
}
_USDC_CACHE_TTL = 300  # 300 giây = 5 phút

# ========== LEVERAGE CACHE ==========
_LEVERAGE_CACHE = {
    "data": {},
    "last_update": 0
}
_LEVERAGE_CACHE_TTL = 300  # 5 phút

# ========== SYMBOL BLACKLIST ==========
_SYMBOL_BLACKLIST = {'BTCUSDC', 'ETHUSDC'}

# ========== BYPASS SSL VERIFICATION ==========
ssl._create_default_https_context = ssl._create_unverified_context

def _wait_for_rate_limit():
    """Đảm bảo không spam quá nhiều request/giây (toàn cục)."""
    global _BINANCE_LAST_REQUEST_TIME
    with _BINANCE_RATE_LOCK:
        now = time.time()
        delta = now - _BINANCE_LAST_REQUEST_TIME
        if delta < _BINANCE_MIN_INTERVAL:
            time.sleep(_BINANCE_MIN_INTERVAL - delta)
        _BINANCE_LAST_REQUEST_TIME = time.time()

def _last_closed_1m_quote_volume(symbol):
    data = binance_api_request(
        "https://fapi.binance.com/fapi/v1/klines",
        params={"symbol": symbol, "interval": "1m", "limit": 2}
    )
    if not data or len(data) < 2:
        return None
    k = data[-2]               # nến 1m đã đóng gần nhất
    return float(k[7])         # quoteVolume (USDC)

# ========== CẤU HÌNH LOGGING ==========
def setup_logging():
    logging.basicConfig(
        level=logging.WARNING,  # CHỈ HIỂN THỊ WARNING VÀ ERROR
        format='%(asctime)s - %(levelname)s - %(module)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('bot_errors.log')
        ]
    )
    return logging.getLogger()

logger = setup_logging()

# ========== HÀM TELEGRAM ==========
def escape_html(text):
    """Escape các ký tự đặc biệt trong HTML để tránh lỗi Telegram"""
    if not text:
        return text
    return (text.replace('&', '&amp;')
                .replace('<', '&lt;')
                .replace('>', '&gt;')
                .replace('"', '&quot;'))

def send_telegram(message, chat_id=None, reply_markup=None, bot_token=None, default_chat_id=None):
    if not bot_token:
        logger.warning("Telegram Bot Token chưa được thiết lập")
        return
    
    chat_id = chat_id or default_chat_id
    if not chat_id:
        logger.warning("Telegram Chat ID chưa được thiết lập")
        return
    
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    
    # ESCAPE MESSAGE ĐỂ TRÁNH LỖI HTML
    safe_message = escape_html(message)
    
    payload = {
        "chat_id": chat_id,
        "text": safe_message,
        "parse_mode": "HTML"
    }
    
    if reply_markup:
        payload["reply_markup"] = json.dumps(reply_markup)
    
    try:
        response = requests.post(url, json=payload, timeout=15)
        if response.status_code != 200:
            logger.error(f"Lỗi Telegram ({response.status_code}): {response.text}")
    except Exception as e:
        logger.error(f"Lỗi kết nối Telegram: {str(e)}")

# ========== MENU TELEGRAM HOÀN CHỈNH ==========
def create_cancel_keyboard():
    return {
        "keyboard": [[{"text": "❌ Hủy bỏ"}]],
        "resize_keyboard": True,
        "one_time_keyboard": True
    }

def create_strategy_keyboard():
    return {
        "keyboard": [
            [{"text": "📊 Hệ thống RSI + Khối lượng"}],
            [{"text": "❌ Hủy bỏ"}]
        ],
        "resize_keyboard": True,
        "one_time_keyboard": True
    }

def create_exit_strategy_keyboard():
    return {
        "keyboard": [
            [{"text": "🎯 Chỉ TP/SL cố định"}],
            [{"text": "❌ Hủy bỏ"}]
        ],
        "resize_keyboard": True,
        "one_time_keyboard": True
    }

def create_bot_mode_keyboard():
    return {
        "keyboard": [
            [{"text": "🤖 Bot Tĩnh - Coin cụ thể"}, {"text": "🔄 Bot Động - Tự tìm coin"}],
            [{"text": "❌ Hủy bỏ"}]
        ],
        "resize_keyboard": True,
        "one_time_keyboard": True
    }

def create_symbols_keyboard(strategy=None):
    try:
        symbols = get_all_usdc_pairs(limit=12)
        if not symbols:
            symbols = ["BNBUSDC", "ADAUSDC", "DOGEUSDC", "XRPUSDC", "DOTUSDC", "LINKUSDC", "SOLUSDC", "MATICUSDC"]
    except:
        symbols = ["BNBUSDC", "ADAUSDC", "DOGEUSDC", "XRPUSDC", "DOTUSDC", "LINKUSDC", "SOLUSDC", "MATICUSDC"]
    
    keyboard = []
    row = []
    for symbol in symbols:
        row.append({"text": symbol})
        if len(row) == 3:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([{"text": "❌ Hủy bỏ"}])
    
    return {
        "keyboard": keyboard,
        "resize_keyboard": True,
        "one_time_keyboard": True
    }

def create_main_menu():
    return {
        "keyboard": [
            [{"text": "📊 Danh sách Bot"}, {"text": "📊 Thống kê"}],
            [{"text": "➕ Thêm Bot"}, {"text": "⛔ Dừng Bot"}],
            [{"text": "⛔ Quản lý Coin"}, {"text": "📈 Vị thế"}],
            [{"text": "💰 Số dư"}, {"text": "⚙️ Cấu hình"}],
            [{"text": "🎯 Chiến lược"}]
        ],
        "resize_keyboard": True,
        "one_time_keyboard": False
    }

def create_leverage_keyboard(strategy=None):
    leverages = ["3", "5", "10", "15", "20", "25", "50", "75", "100"]
    
    keyboard = []
    row = []
    for lev in leverages:
        row.append({"text": f"{lev}x"})
        if len(row) == 3:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([{"text": "❌ Hủy bỏ"}])
    
    return {
        "keyboard": keyboard,
        "resize_keyboard": True,
        "one_time_keyboard": True
    }

def create_percent_keyboard():
    return {
        "keyboard": [
            [{"text": "1"}, {"text": "3"}, {"text": "5"}, {"text": "10"}],
            [{"text": "15"}, {"text": "20"}, {"text": "25"}, {"text": "50"}],
            [{"text": "❌ Hủy bỏ"}]
        ],
        "resize_keyboard": True,
        "one_time_keyboard": True
    }

def create_tp_keyboard():
    return {
        "keyboard": [
            [{"text": "50"}, {"text": "100"}, {"text": "200"}],
            [{"text": "300"}, {"text": "500"}, {"text": "1000"}],
            [{"text": "❌ Hủy bỏ"}]
        ],
        "resize_keyboard": True,
        "one_time_keyboard": True
    }

def create_sl_keyboard():
    return {
        "keyboard": [
            [{"text": "0"}, {"text": "50"}, {"text": "100"}],
            [{"text": "150"}, {"text": "200"}, {"text": "500"}],
            [{"text": "❌ Hủy bỏ"}]
        ],
        "resize_keyboard": True,
        "one_time_keyboard": True
    }

def create_bot_count_keyboard():
    return {
        "keyboard": [
            [{"text": "1"}, {"text": "2"}, {"text": "3"}],
            [{"text": "5"}, {"text": "10"}],
            [{"text": "❌ Hủy bỏ"}]
        ],
        "resize_keyboard": True,
        "one_time_keyboard": True
    }

def create_roi_trigger_keyboard():
    return {
        "keyboard": [
            [{"text": "30"}, {"text": "50"}, {"text": "100"}],
            [{"text": "150"}, {"text": "200"}, {"text": "300"}],
            [{"text": "❌ Tắt tính năng"}],
            [{"text": "❌ Hủy bỏ"}]
        ],
        "resize_keyboard": True,
        "one_time_keyboard": True
    }

# ========== API BINANCE - ĐÃ TỐI ƯU ==========
def sign(query, api_secret):
    try:
        return hmac.new(api_secret.encode(), query.encode(), hashlib.sha256).hexdigest()
    except Exception as e:
        logger.error(f"Lỗi tạo chữ ký: {str(e)}")
        return ""

def binance_api_request(url, method='GET', params=None, headers=None):
    """Gửi request tới Binance với rate limit + retry tối ưu"""
    max_retries = 2  # Giảm từ 3 xuống 2
    base_url = url

    for attempt in range(max_retries):
        try:
            _wait_for_rate_limit()

            url = base_url

            if headers is None:
                headers = {}

            if 'User-Agent' not in headers:
                headers['User-Agent'] = (
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                    'AppleWebKit/537.36'
                )

            if method.upper() == 'GET':
                if params:
                    query = urllib.parse.urlencode(params)
                    url = f"{url}?{query}"
                req = urllib.request.Request(url, headers=headers)
            else:
                data = urllib.parse.urlencode(params).encode() if params else None
                req = urllib.request.Request(url, data=data, headers=headers, method=method)

            with urllib.request.urlopen(req, timeout=15) as response:  # Giảm timeout
                if response.status == 200:
                    return json.loads(response.read().decode())
                else:
                    error_content = response.read().decode()
                    logger.error(f"Lỗi API ({response.status}): {error_content}")

                    if response.status == 401:
                        return None

                    if response.status == 429:
                        sleep_time = 2 ** attempt
                        logger.warning(f"⚠️ 429 Too Many Requests, ngủ {sleep_time}s rồi thử lại")
                        time.sleep(sleep_time)
                    elif response.status >= 500:
                        time.sleep(0.5)  # Giảm thời gian chờ

                    continue

        except urllib.error.HTTPError as e:
            if e.code == 451:
                logger.error("❌ Lỗi 451: Truy cập bị chặn - Có thể do hạn chế địa lý. Vui lòng kiểm tra VPN/proxy.")
                return None
            else:
                logger.error(f"Lỗi HTTP ({e.code}): {e.reason}")

            if e.code == 401:
                return None
            if e.code == 429:
                sleep_time = 2 ** attempt
                logger.warning(f"⚠️ HTTP 429 Too Many Requests, ngủ {sleep_time}s rồi thử lại")
                time.sleep(sleep_time)
            elif e.code >= 500:
                time.sleep(0.5)

            continue

        except Exception as e:
            logger.error(f"Lỗi kết nối API (lần {attempt + 1}): {str(e)}")
            time.sleep(0.5)

    logger.error(f"Không thể thực hiện yêu cầu API sau {max_retries} lần thử")
    return None

def get_all_usdc_pairs(limit=100):
    """Lấy danh sách các symbol USDC, có cache 5 phút, loại trừ blacklist"""
    global _USDC_CACHE
    try:
        now = time.time()

        if _USDC_CACHE["pairs"] and (now - _USDC_CACHE["last_update"] < _USDC_CACHE_TTL):
            pairs = _USDC_CACHE["pairs"]
        else:
            url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
            data = binance_api_request(url)
            if not data:
                logger.warning("Không lấy được dữ liệu từ Binance, trả về danh sách rỗng")
                return []

            usdc_pairs = []
            for symbol_info in data.get('symbols', []):
                symbol = symbol_info.get('symbol', '')
                if (symbol.endswith('USDC') and 
                    symbol_info.get('status') == 'TRADING' and
                    symbol not in _SYMBOL_BLACKLIST):  # Loại trừ blacklist
                    usdc_pairs.append(symbol)

            _USDC_CACHE["pairs"] = usdc_pairs
            _USDC_CACHE["last_update"] = now
            logger.info(f"✅ Lấy được {len(usdc_pairs)} coin USDC từ Binance (loại trừ BTC/ETH)")

            pairs = usdc_pairs

        return pairs[:limit]

    except Exception as e:
        logger.error(f"❌ Lỗi lấy danh sách coin từ Binance: {str(e)}")
        return []

def get_max_leverage(symbol, api_key, api_secret):
    """Lấy đòn bẩy tối đa cho một symbol với cache"""
    global _LEVERAGE_CACHE
    
    try:
        symbol = symbol.upper()
        current_time = time.time()
        
        # Kiểm tra cache
        if (symbol in _LEVERAGE_CACHE["data"] and 
            current_time - _LEVERAGE_CACHE["last_update"] < _LEVERAGE_CACHE_TTL):
            return _LEVERAGE_CACHE["data"][symbol]
        
        # Nếu không có trong cache, gọi API
        url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        data = binance_api_request(url)
        if not data:
            return 100
        
        for s in data['symbols']:
            if s['symbol'] == symbol:
                for f in s['filters']:
                    if f['filterType'] == 'LEVERAGE':
                        if 'maxLeverage' in f:
                            leverage = int(f['maxLeverage'])
                            # Cập nhật cache
                            _LEVERAGE_CACHE["data"][symbol] = leverage
                            _LEVERAGE_CACHE["last_update"] = current_time
                            return leverage
                break
        return 100
    except Exception as e:
        logger.error(f"Lỗi lấy đòn bẩy tối đa {symbol}: {str(e)}")
        return 100

def get_step_size(symbol, api_key, api_secret):
    if not symbol:
        logger.error("❌ Lỗi: Symbol là None khi lấy step size")
        return 0.001
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    try:
        data = binance_api_request(url)
        if not data:
            return 0.001
        for s in data['symbols']:
            if s['symbol'] == symbol.upper():
                for f in s['filters']:
                    if f['filterType'] == 'LOT_SIZE':
                        return float(f['stepSize'])
    except Exception as e:
        logger.error(f"Lỗi lấy step size: {str(e)}")
    return 0.001

def set_leverage(symbol, lev, api_key, api_secret):
    if not symbol:
        logger.error("❌ Lỗi: Symbol là None khi set leverage")
        return False
    try:
        ts = int(time.time() * 1000)
        params = {
            "symbol": symbol.upper(),
            "leverage": lev,
            "timestamp": ts
        }
        query = urllib.parse.urlencode(params)
        sig = sign(query, api_secret)
        url = f"https://fapi.binance.com/fapi/v1/leverage?{query}&signature={sig}"
        headers = {'X-MBX-APIKEY': api_key}
        
        response = binance_api_request(url, method='POST', headers=headers)
        if response is None:
            return False
        if response and 'leverage' in response:
            return True
        return False
    except Exception as e:
        logger.error(f"Lỗi thiết lập đòn bẩy: {str(e)}")
        return False

def get_balance(api_key, api_secret):
    """Lấy số dư KHẢ DỤNG (availableBalance) để tính toán khối lượng"""
    try:
        ts = int(time.time() * 1000)
        params = {"timestamp": ts}
        query = urllib.parse.urlencode(params)
        sig = sign(query, api_secret)
        url = f"https://fapi.binance.com/fapi/v2/account?{query}&signature={sig}"
        headers = {'X-MBX-APIKEY': api_key}
        
        data = binance_api_request(url, headers=headers)
        if not data:
            logger.error("❌ Không lấy được số dư từ Binance")
            return None
            
        for asset in data['assets']:
            if asset['asset'] == 'USDC':
                available_balance = float(asset['availableBalance'])
                total_balance = float(asset['walletBalance'])
                
                logger.info(f"💰 Số dư - Khả dụng: {available_balance:.2f} USDC, Tổng: {total_balance:.2f} USDC")
                return available_balance
        return 0
    except Exception as e:
        logger.error(f"Lỗi lấy số dư: {str(e)}")
        return None

def place_order(symbol, side, qty, api_key, api_secret):
    if not symbol:
        logger.error("❌ Không thể đặt lệnh: symbol là None")
        return None
    try:
        ts = int(time.time() * 1000)
        params = {
            "symbol": symbol.upper(),
            "side": side,
            "type": "MARKET",
            "quantity": qty,
            "timestamp": ts
        }
        query = urllib.parse.urlencode(params)
        sig = sign(query, api_secret)
        url = f"https://fapi.binance.com/fapi/v1/order?{query}&signature={sig}"
        headers = {'X-MBX-APIKEY': api_key}
        
        return binance_api_request(url, method='POST', headers=headers)
    except Exception as e:
        logger.error(f"Lỗi đặt lệnh: {str(e)}")
    return None

def cancel_all_orders(symbol, api_key, api_secret):
    if not symbol:
        logger.error("❌ Không thể hủy lệnh: symbol là None")
        return False
    try:
        ts = int(time.time() * 1000)
        params = {"symbol": symbol.upper(), "timestamp": ts}
        query = urllib.parse.urlencode(params)
        sig = sign(query, api_secret)
        url = f"https://fapi.binance.com/fapi/v1/allOpenOrders?{query}&signature={sig}"
        headers = {'X-MBX-APIKEY': api_key}
        
        binance_api_request(url, method='DELETE', headers=headers)
        return True
    except Exception as e:
        logger.error(f"Lỗi hủy lệnh: {str(e)}")
    return False

def get_current_price(symbol):
    if not symbol:
        logger.error("💰 Lỗi: Symbol là None khi lấy giá")
        return 0
    try:
        url = f"https://fapi.binance.com/fapi/v1/ticker/price?symbol={symbol.upper()}"
        data = binance_api_request(url)
        if data and 'price' in data:
            price = float(data['price'])
            if price > 0:
                return price
            else:
                logger.error(f"💰 Giá {symbol} = 0")
        return 0
    except Exception as e:
        logger.error(f"💰 Lỗi lấy giá {symbol}: {str(e)}")
    return 0

def get_positions(symbol=None, api_key=None, api_secret=None):
    try:
        ts = int(time.time() * 1000)
        params = {"timestamp": ts}
        if symbol:
            params["symbol"] = symbol.upper()
        query = urllib.parse.urlencode(params)
        sig = sign(query, api_secret)
        url = f"https://fapi.binance.com/fapi/v2/positionRisk?{query}&signature={sig}"
        headers = {'X-MBX-APIKEY': api_key}
        
        positions = binance_api_request(url, headers=headers)
        if not positions:
            return []
        if symbol:
            for pos in positions:
                if pos['symbol'] == symbol.upper():
                    return [pos]
        return positions
    except Exception as e:
        logger.error(f"Lỗi lấy vị thế: {str(e)}")
    return []

# ========== COIN MANAGER ==========
class CoinManager:
    def __init__(self):
        self.active_coins = set()
        self._lock = threading.Lock()
    
    def register_coin(self, symbol):
        if not symbol:
            return
        with self._lock:
            self.active_coins.add(symbol.upper())
    
    def unregister_coin(self, symbol):
        if not symbol:
            return
        with self._lock:
            self.active_coins.discard(symbol.upper())
    
    def is_coin_active(self, symbol):
        if not symbol:
            return False
        with self._lock:
            return symbol.upper() in self.active_coins
    
    def get_active_coins(self):
        with self._lock:
            return list(self.active_coins)

# ========== BOT EXECUTION COORDINATOR ==========
class BotExecutionCoordinator:
    """Điều phối việc tìm coin giữa các bot"""
    def __init__(self):
        self._lock = threading.Lock()
        self._bot_queue = []  # Hàng đợi bot chờ tìm coin
        self._current_finding_bot = None  # Bot đang tìm coin
        self._found_coins = set()  # Các coin đã được tìm thấy và phân phối
        
    def request_coin_search(self, bot_id):
        """Bot yêu cầu tìm coin"""
        with self._lock:
            if self._current_finding_bot is None:
                # Không có bot nào đang tìm, bot này được quyền tìm
                self._current_finding_bot = bot_id
                return True
            else:
                # Đã có bot đang tìm, thêm vào hàng đợi
                if bot_id not in self._bot_queue:
                    self._bot_queue.append(bot_id)
                return False
    
    def finish_coin_search(self, bot_id, found_symbol=None):
        """Hoàn thành việc tìm coin"""
        with self._lock:
            if self._current_finding_bot == bot_id:
                self._current_finding_bot = None
                
                if found_symbol:
                    self._found_coins.add(found_symbol)
                
                # Chuyển quyền tìm coin cho bot tiếp theo trong hàng đợi
                if self._bot_queue:
                    next_bot = self._bot_queue.pop(0)
                    self._current_finding_bot = next_bot
                    return next_bot
            
            return None
    
    def is_coin_available(self, symbol):
        """Kiểm tra coin đã được phân phối chưa"""
        with self._lock:
            return symbol not in self._found_coins
    
    def get_queue_position(self, bot_id):
        """Lấy vị trí trong hàng đợi"""
        with self._lock:
            if self._current_finding_bot == bot_id:
                return 0  # Đang tìm coin
            elif bot_id in self._bot_queue:
                return self._bot_queue.index(bot_id) + 1
            else:
                return -1  # Không trong hàng đợi

# ========== SMART COIN FINDER VỚI CACHE LEVERAGE ==========
class SmartCoinFinder:
    def __init__(self, api_key, api_secret):
        self.api_key = api_key
        self.api_secret = api_secret
        self.last_scan_time = 0
        self.scan_cooldown = 10
        # Thêm cache cho kết quả phân tích
        self.analysis_cache = {}
        self.cache_ttl = 30  # Cache 30 giây
        
    def get_symbol_leverage(self, symbol):
        """Lấy đòn bẩy từ cache - không cần API key/secret"""
        return get_max_leverage(symbol, self.api_key, self.api_secret)
    
    def calculate_rsi(self, prices, period=14):
        """Tính RSI từ danh sách giá"""
        if len(prices) < period + 1:
            return 50  # Giá trị trung bình nếu không đủ dữ liệu
            
        deltas = np.diff(prices)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        
        avg_gains = np.mean(gains[:period])
        avg_losses = np.mean(losses[:period])
        
        if avg_losses == 0:
            return 100
            
        rs = avg_gains / avg_losses
        rsi = 100 - (100 / (1 + rs))
        
        return rsi
    
    def get_rsi_signal(self, symbol, volume_threshold=20):
        """Phân tích tín hiệu RSI và khối lượng với cache"""
        try:
            current_time = time.time()
            cache_key = f"{symbol}_{volume_threshold}"
            
            # Kiểm tra cache
            if (cache_key in self.analysis_cache and 
                current_time - self.analysis_cache[cache_key]['timestamp'] < self.cache_ttl):
                return self.analysis_cache[cache_key]['signal']
            
            # Lấy dữ liệu kline 5 phút
            data = binance_api_request(
                "https://fapi.binance.com/fapi/v1/klines",
                params={"symbol": symbol, "interval": "5m", "limit": 15}
            )
            if not data or len(data) < 15:
                return None
            
            # Lấy 3 nến gần nhất để phân tích
            prev_prev_candle = data[-4]  # Nến trước đó
            prev_candle = data[-3]       # Nến trước
            current_candle = data[-2]    # Nến hiện tại (đã đóng)
            
            # Giá đóng cửa và khối lượng
            prev_prev_close = float(prev_prev_candle[4])
            prev_close = float(prev_candle[4])
            current_close = float(current_candle[4])
            
            prev_prev_volume = float(prev_prev_candle[5])
            prev_volume = float(prev_candle[5])
            current_volume = float(current_candle[5])
            
            # Tính RSI
            closes = [float(k[4]) for k in data]
            rsi_current = self.calculate_rsi(closes)
            
            # Tính toán thay đổi giá và khối lượng
            price_change_prev = prev_close - prev_prev_close
            price_change_current = current_close - prev_close
            
            volume_change_prev = (prev_volume - prev_prev_volume) / prev_prev_volume * 100
            volume_change_current = (current_volume - prev_volume) / prev_volume * 100
            
            # Xác định xu hướng giá
            price_increasing = price_change_current > 0
            price_decreasing = price_change_current < 0
            price_not_increasing = price_change_current <= 0
            price_not_decreasing = price_change_current >= 0
            
            # Xác định xu hướng khối lượng
            volume_increasing = volume_change_current > volume_threshold
            volume_decreasing = volume_change_current < -volume_threshold
            
            # 🔴 TÍCH HỢP CÁC ĐIỀU KIỆN RSI
            
            # Điều kiện 1: RSI > 80 và giá tăng, khối lượng tăng -> BÁN
            if rsi_current > 80 and price_increasing and volume_increasing:
                logger.info(f"🎯 {symbol} - Tín hiệu BÁN: RSI > 80, giá tăng, volume tăng")
                result = "SELL"
            
            # Điều kiện 2: RSI < 20 và giá giảm, khối lượng giảm -> BÁN
            elif rsi_current < 20 and price_decreasing and volume_decreasing:
                logger.info(f"🎯 {symbol} - Tín hiệu BÁN: RSI < 20, giá giảm, volume giảm")
                result = "SELL"
            
            # Điều kiện 3: RSI > 80 và giá tăng, khối lượng giảm -> MUA
            elif rsi_current > 80 and price_increasing and volume_decreasing:
                logger.info(f"🎯 {symbol} - Tín hiệu MUA: RSI > 80, giá tăng, volume giảm")
                result = "BUY"
            
            # Điều kiện 4: RSI < 20 và giá giảm, khối lượng tăng -> MUA
            elif rsi_current < 20 and price_decreasing and volume_increasing:
                logger.info(f"🎯 {symbol} - Tín hiệu MUA: RSI < 20, giá giảm, volume tăng")
                result = "BUY"
            
            # Điều kiện 5: RSI > 20 và giá không giảm, khối lượng giảm -> MUA
            elif rsi_current > 20 and price_not_decreasing and volume_decreasing:
                logger.info(f"🎯 {symbol} - Tín hiệu MUA: RSI > 20, giá không giảm, volume giảm")
                result = "BUY"
            
            # Điều kiện 6: RSI < 80 và không tăng giá, khối lượng tăng -> BÁN
            elif rsi_current < 80 and price_not_increasing and volume_increasing:
                logger.info(f"🎯 {symbol} - Tín hiệu BÁN: RSI < 80, giá không tăng, volume tăng")
                result = "SELL"
            
            else:
                result = None
            
            # Lưu kết quả vào cache
            self.analysis_cache[cache_key] = {
                'signal': result,
                'timestamp': current_time
            }
            
            return result
            
        except Exception as e:
            logger.error(f"Lỗi phân tích RSI {symbol}: {str(e)}")
            return None
    
    def find_best_coin_any_signal(self, excluded_coins=None, required_leverage=10):
        """
        Tìm coin tốt nhất với bất kỳ tín hiệu nào (BUY / SELL)
        Sử dụng cache leverage và loại trừ blacklist
        """
        try:
            now = time.time()

            if now - getattr(self, "last_scan_time", 0) < getattr(self, "scan_cooldown", 10):
                return None

            self.last_scan_time = now

            # Lấy danh sách USDC (đã loại trừ BTC/ETH)
            all_symbols = get_all_usdc_pairs(limit=20)
            if not all_symbols:
                return None

            valid_symbols = []

            for symbol in all_symbols:
                # Bị loại trừ
                if excluded_coins and symbol in excluded_coins:
                    continue

                # Đã có vị thế trên Binance
                if self.has_existing_position(symbol):
                    continue

                # Sử dụng cache leverage - nhanh hơn
                max_lev = self.get_symbol_leverage(symbol)
                if max_lev < required_leverage:
                    continue

                # Thêm delay nhỏ để không spam /klines
                time.sleep(0.05)

                # Lấy tín hiệu vào lệnh
                entry_signal = self.get_entry_signal(symbol)
                if entry_signal in ["BUY", "SELL"]:
                    valid_symbols.append((symbol, entry_signal))
                    logger.info(f"✅ Tìm thấy coin có tín hiệu: {symbol} - Tín hiệu: {entry_signal}")

            if not valid_symbols:
                return None

            # Chọn ngẫu nhiên một coin trong danh sách hợp lệ
            selected_symbol, _ = random.choice(valid_symbols)

            # Kiểm tra lại lần cuối
            if self.has_existing_position(selected_symbol):
                return None

            logger.info(f"🎯 Chọn coin để trade: {selected_symbol}")
            return selected_symbol

        except Exception as e:
            logger.error(f"❌ Lỗi find_best_coin_any_signal: {str(e)}")
            return None

    def get_entry_signal(self, symbol):
        """Tín hiệu vào lệnh - khối lượng 20%"""
        return self.get_rsi_signal(symbol, volume_threshold=20)
    
    def get_exit_signal(self, symbol):
        """Tín hiệu đóng lệnh - khối lượng 40%"""
        return self.get_rsi_signal(symbol, volume_threshold=40)
    
    def has_existing_position(self, symbol):
        """Kiểm tra xem coin đã có vị thế trên Binance chưa"""
        try:
            positions = get_positions(symbol, self.api_key, self.api_secret)
            if positions:
                for pos in positions:
                    position_amt = float(pos.get('positionAmt', 0))
                    if abs(position_amt) > 0:
                        logger.info(f"⚠️ Phát hiện vị thế trên {symbol}: {position_amt}")
                        return True
            return False
        except Exception as e:
            logger.error(f"❌ Lỗi kiểm tra vị thế {symbol}: {str(e)}")
            return True

# ========== TỐI ƯU HÓA WEBSOCKET ==========
class WebSocketManager:
    def __init__(self):
        self.connections = {}
        self.executor = ThreadPoolExecutor(max_workers=20)  # Tăng số worker
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        # Thêm cache cho price updates
        self.price_cache = {}
        self.last_price_update = {}
        
    def add_symbol(self, symbol, callback):
        if not symbol:
            return
        symbol = symbol.upper()
        with self._lock:
            if symbol not in self.connections:
                self._create_connection(symbol, callback)
                
    def _create_connection(self, symbol, callback):
        if self._stop_event.is_set():
            return
        
        # Sử dụng combined streams để giảm số lượng kết nối
        streams = [f"{symbol.lower()}@trade"]
        url = f"wss://fstream.binance.com/stream?streams={'/'.join(streams)}"
        
        def on_message(ws, message):
            try:
                data = json.loads(message)
                if 'data' in data:
                    symbol = data['data']['s']
                    price = float(data['data']['p'])
                    current_time = time.time()
                    
                    # Giảm tần suất callback với cùng một symbol
                    if (symbol in self.last_price_update and 
                        current_time - self.last_price_update[symbol] < 0.1):  # Chỉ cập nhật mỗi 100ms
                        return
                    
                    self.last_price_update[symbol] = current_time
                    self.price_cache[symbol] = price
                    self.executor.submit(callback, price)
            except Exception as e:
                logger.error(f"Lỗi xử lý tin nhắn WebSocket {symbol}: {str(e)}")
                
        def on_error(ws, error):
            logger.error(f"Lỗi WebSocket {symbol}: {str(error)}")
            if not self._stop_event.is_set():
                time.sleep(5)
                self._reconnect(symbol, callback)
            
        def on_close(ws, close_status_code, close_msg):
            logger.info(f"WebSocket đóng {symbol}: {close_status_code} - {close_msg}")
            if not self._stop_event.is_set() and symbol in self.connections:
                time.sleep(5)
                self._reconnect(symbol, callback)
                
        ws = websocket.WebSocketApp(
            url,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )
        
        thread = threading.Thread(target=ws.run_forever, daemon=True)
        thread.start()
        
        self.connections[symbol] = {
            'ws': ws,
            'thread': thread,
            'callback': callback
        }
        logger.info(f"🔗 WebSocket bắt đầu cho {symbol}")
        
    def _reconnect(self, symbol, callback):
        logger.info(f"Kết nối lại WebSocket cho {symbol}")
        self.remove_symbol(symbol)
        self._create_connection(symbol, callback)
        
    def remove_symbol(self, symbol):
        if not symbol:
            return
        symbol = symbol.upper()
        with self._lock:
            if symbol in self.connections:
                try:
                    self.connections[symbol]['ws'].close()
                except Exception as e:
                    logger.error(f"Lỗi đóng WebSocket {symbol}: {str(e)}")
                del self.connections[symbol]
                logger.info(f"WebSocket đã xóa cho {symbol}")
                
    def stop(self):
        self._stop_event.set()
        for symbol in list(self.connections.keys()):
            self.remove_symbol(symbol)

# ========== BASE BOT VỚI MULTI-THREAD ==========
class BaseBot:
    def __init__(self, symbol, lev, percent, tp, sl, roi_trigger, ws_manager, api_key, api_secret,
                 telegram_bot_token, telegram_chat_id, strategy_name, config_key=None, bot_id=None,
                 coin_manager=None, symbol_locks=None, max_coins=1, bot_coordinator=None):

        # LUÔN ĐẶT max_coins = 1 - MỖI BOT CHỈ QUẢN LÝ 1 COIN
        self.max_coins = 1
        self.active_symbols = []
        self.symbol_data = {}
        self.symbol = symbol.upper() if symbol else None
        
        self.lev = lev
        self.percent = percent
        self.tp = tp
        self.sl = sl
        self.roi_trigger = roi_trigger
        self.ws_manager = ws_manager
        self.api_key = api_key
        self.api_secret = api_secret
        self.telegram_bot_token = telegram_bot_token
        self.telegram_chat_id = telegram_chat_id
        self.strategy_name = strategy_name
        self.config_key = config_key
        self.bot_id = bot_id or f"{strategy_name}_{int(time.time())}_{random.randint(1000, 9999)}"

        self.status = "searching" if not symbol else "waiting"
        self._stop = False

        # Biến để quản lý tuần tự
        self.current_processing_symbol = None
        self.last_trade_completion_time = 0
        self.trade_cooldown = 30

        # Quản lý thời gian
        self.last_global_position_check = 0
        self.last_error_log_time = 0
        self.global_position_check_interval = 30

        # Thống kê
        self.global_long_count = 0
        self.global_short_count = 0
        self.global_long_pnl = 0
        self.global_short_pnl = 0

        self.coin_manager = coin_manager or CoinManager()
        self.symbol_locks = symbol_locks
        self.coin_finder = SmartCoinFinder(api_key, api_secret)

        self.find_new_bot_after_close = True
        self.bot_creation_time = time.time()

        # THÊM: Biến quản lý thứ tự thực thi
        self.execution_lock = threading.Lock()
        self.last_execution_time = 0
        self.execution_cooldown = 1

        # Thêm bot coordinator
        self.bot_coordinator = bot_coordinator or BotExecutionCoordinator()

        # Khởi tạo symbol đầu tiên nếu có
        if symbol and not self.coin_finder.has_existing_position(symbol):
            self._add_symbol(symbol)
        
        # Mỗi bot chạy thread riêng
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

        roi_info = f" | 🎯 ROI Trigger: {roi_trigger}%" if roi_trigger else " | 🎯 ROI Trigger: Tắt"
        self.log(f"🟢 Bot {strategy_name} khởi động | 1 coin | ĐB: {lev}x | Vốn: {percent}% | TP/SL: {tp}%/{sl}%{roi_info}")

    def _run(self):
        """Vòng lặp chính - MỖI BOT CHẠY THREAD RIÊNG"""
        while not self._stop:
            try:
                current_time = time.time()
                
                # KIỂM TRA VỊ THẾ TOÀN TÀI KHOẢN ĐỊNH KỲ
                if current_time - self.last_global_position_check > 30:
                    self.check_global_positions()
                    self.last_global_position_check = current_time
                
                # NẾU BOT KHÔNG CÓ COIN NÀO - YÊU CẦU TÌM COIN
                if not self.active_symbols:
                    search_permission = self.bot_coordinator.request_coin_search(self.bot_id)
                    
                    if search_permission:
                        # Bot này được quyền tìm coin
                        self.log(f"🔍 Đang tìm coin (vị trí: 1)...")
                        found_coin = self._find_and_add_new_coin()
                        
                        if found_coin:
                            self.bot_coordinator.finish_coin_search(self.bot_id, found_coin)
                            self.log(f"✅ Đã tìm thấy và thêm coin: {found_coin}")
                        else:
                            self.bot_coordinator.finish_coin_search(self.bot_id)
                            self.log(f"❌ Không tìm thấy coin phù hợp")
                    else:
                        # Chờ trong hàng đợi
                        queue_pos = self.bot_coordinator.get_queue_position(self.bot_id)
                        if queue_pos > 0:
                            self.log(f"⏳ Đang chờ tìm coin (vị trí: {queue_pos + 1})...")
                
                # XỬ LÝ COIN HIỆN TẠI (nếu có)
                for symbol in self.active_symbols.copy():
                    self._process_single_symbol(symbol)
                
                time.sleep(2)
                
            except Exception as e:
                if time.time() - self.last_error_log_time > 10:
                    self.log(f"❌ Lỗi hệ thống: {str(e)}")
                    self.last_error_log_time = time.time()
                time.sleep(5)

    def _process_single_symbol(self, symbol):
        """Xử lý một symbol duy nhất - CHẠY ĐỘC LẬP"""
        try:
            symbol_info = self.symbol_data[symbol]
            current_time = time.time()
            
            # Kiểm tra vị thế định kỳ
            if current_time - symbol_info.get('last_position_check', 0) > 30:
                self._check_symbol_position(symbol)
                symbol_info['last_position_check'] = current_time
            
            # Xử lý theo trạng thái
            if symbol_info['position_open']:
                # Kiểm tra đóng lệnh
                if self._check_smart_exit_condition(symbol):
                    return True
                
                # Kiểm tra TP/SL truyền thống
                self._check_symbol_tp_sl(symbol)
            else:
                # Tìm cơ hội vào lệnh
                if (current_time - symbol_info['last_trade_time'] > 30 and 
                    current_time - symbol_info['last_close_time'] > 30):
                    
                    entry_signal = self.coin_finder.get_entry_signal(symbol)
                    
                    if entry_signal:
                        target_side = self.get_next_side_based_on_comprehensive_analysis()
                        
                        if entry_signal == target_side:
                            if not self.coin_finder.has_existing_position(symbol):
                                if self._open_symbol_position(symbol, target_side):
                                    symbol_info['last_trade_time'] = current_time
                                    return True
                            
            return False
            
        except Exception as e:
            self.log(f"❌ Lỗi xử lý {symbol}: {str(e)}")
            return False

    def _check_smart_exit_condition(self, symbol):
        """Kiểm tra điều kiện đóng lệnh thông minh"""
        try:
            if not self.symbol_data[symbol]['position_open']:
                return False
            
            if not self.symbol_data[symbol]['roi_check_activated']:
                return False
            
            current_price = self.get_current_price(symbol)
            if current_price <= 0:
                return False
            
            # Tính ROI hiện tại
            if self.symbol_data[symbol]['side'] == "BUY":
                profit = (current_price - self.symbol_data[symbol]['entry']) * abs(self.symbol_data[symbol]['qty'])
            else:
                profit = (self.symbol_data[symbol]['entry'] - current_price) * abs(self.symbol_data[symbol]['qty'])
                
            invested = self.symbol_data[symbol]['entry'] * abs(self.symbol_data[symbol]['qty']) / self.lev
            if invested <= 0:
                return False
                
            current_roi = (profit / invested) * 100
            
            # Kiểm tra nếu đạt ROI trigger
            if current_roi >= self.roi_trigger:
                exit_signal = self.coin_finder.get_exit_signal(symbol)
                
                if exit_signal:
                    reason = f"🎯 Đạt ROI {self.roi_trigger}% + Tín hiệu đóng lệnh (ROI: {current_roi:.2f}%)"
                    self._close_symbol_position(symbol, reason)
                    return True
            
            return False
            
        except Exception as e:
            self.log(f"❌ Lỗi kiểm tra đóng lệnh thông minh {symbol}: {str(e)}")
            return False

    def _find_and_add_new_coin(self):
        """Tìm và thêm coin mới - SỬ DỤNG CACHE LEVERAGE"""
        try:
            active_coins = self.coin_manager.get_active_coins()
            
            new_symbol = self.coin_finder.find_best_coin_any_signal(
                excluded_coins=active_coins,
                required_leverage=self.lev
            )
            
            if new_symbol and self.bot_coordinator.is_coin_available(new_symbol):
                if self.coin_finder.has_existing_position(new_symbol):
                    return False
                    
                success = self._add_symbol(new_symbol)
                if success:
                    self.log(f"✅ Đã thêm coin: {new_symbol}")
                    
                    time.sleep(1)
                    if self.coin_finder.has_existing_position(new_symbol):
                        self.log(f"🚫 {new_symbol} - PHÁT HIỆN CÓ VỊ THẾ SAU KHI THÊM, DỪNG THEO DÕI NGAY")
                        self.stop_symbol(new_symbol)
                        return False
                        
                    return True
                
            return False
            
        except Exception as e:
            self.log(f"❌ Lỗi tìm coin mới: {str(e)}")
            return False
            
    def _add_symbol(self, symbol):
        """Thêm một symbol vào quản lý của bot"""
        if symbol in self.active_symbols:
            return False
            
        if len(self.active_symbols) >= self.max_coins:
            return False
        
        if self.coin_finder.has_existing_position(symbol):
            return False
        
        # Khởi tạo dữ liệu cho symbol
        self.symbol_data[symbol] = {
            'status': 'waiting',
            'side': '',
            'qty': 0,
            'entry': 0,
            'current_price': 0,
            'position_open': False,
            'last_trade_time': 0,
            'last_close_time': 0,
            'entry_base': 0,
            'average_down_count': 0,
            'last_average_down_time': 0,
            'high_water_mark_roi': 0,
            'roi_check_activated': False,
            'close_attempted': False,
            'last_close_attempt': 0,
            'last_position_check': 0
        }
        
        self.active_symbols.append(symbol)
        self.coin_manager.register_coin(symbol)
        self.ws_manager.add_symbol(symbol, lambda price, sym=symbol: self._handle_price_update(price, sym))
        
        self._check_symbol_position(symbol)
        
        if self.symbol_data[symbol]['position_open']:
            self.stop_symbol(symbol)
            return False
        
        return True

    def _handle_price_update(self, price, symbol):
        """Xử lý cập nhật giá cho từng symbol"""
        if symbol in self.symbol_data:
            self.symbol_data[symbol]['current_price'] = price

    def get_current_price(self, symbol):
        """Lấy giá từ cache của WebSocket"""
        if (symbol in self.ws_manager.price_cache and 
            time.time() - self.ws_manager.last_price_update.get(symbol, 0) < 5):
            return self.ws_manager.price_cache[symbol]
        
        return get_current_price(symbol)

    def _check_symbol_position(self, symbol):
        """Kiểm tra vị thế cho một symbol cụ thể"""
        try:
            positions = get_positions(symbol, self.api_key, self.api_secret)
            if not positions:
                self._reset_symbol_position(symbol)
                return
            
            position_found = False
            for pos in positions:
                if pos['symbol'] == symbol:
                    position_amt = float(pos.get('positionAmt', 0))
                    if abs(position_amt) > 0:
                        position_found = True
                        self.symbol_data[symbol]['position_open'] = True
                        self.symbol_data[symbol]['status'] = "open"
                        self.symbol_data[symbol]['side'] = "BUY" if position_amt > 0 else "SELL"
                        self.symbol_data[symbol]['qty'] = position_amt
                        self.symbol_data[symbol]['entry'] = float(pos.get('entryPrice', 0))
                        
                        current_price = self.get_current_price(symbol)
                        if current_price > 0:
                            if self.symbol_data[symbol]['side'] == "BUY":
                                profit = (current_price - self.symbol_data[symbol]['entry']) * abs(self.symbol_data[symbol]['qty'])
                            else:
                                profit = (self.symbol_data[symbol]['entry'] - current_price) * abs(self.symbol_data[symbol]['qty'])
                                
                            invested = self.symbol_data[symbol]['entry'] * abs(self.symbol_data[symbol]['qty']) / self.lev
                            if invested > 0:
                                current_roi = (profit / invested) * 100
                                if current_roi >= self.roi_trigger:
                                    self.symbol_data[symbol]['roi_check_activated'] = True
                        break
                    else:
                        position_found = True
                        self._reset_symbol_position(symbol)
                        break
            
            if not position_found:
                self._reset_symbol_position(symbol)
                
        except Exception as e:
            self.log(f"❌ Lỗi kiểm tra vị thế {symbol}: {str(e)}")

    def _reset_symbol_position(self, symbol):
        """Reset trạng thái vị thế cho một symbol"""
        if symbol in self.symbol_data:
            self.symbol_data[symbol]['position_open'] = False
            self.symbol_data[symbol]['status'] = "waiting"
            self.symbol_data[symbol]['side'] = ""
            self.symbol_data[symbol]['qty'] = 0
            self.symbol_data[symbol]['entry'] = 0
            self.symbol_data[symbol]['close_attempted'] = False
            self.symbol_data[symbol]['last_close_attempt'] = 0
            self.symbol_data[symbol]['entry_base'] = 0
            self.symbol_data[symbol]['average_down_count'] = 0
            self.symbol_data[symbol]['high_water_mark_roi'] = 0
            self.symbol_data[symbol]['roi_check_activated'] = False

    def _open_symbol_position(self, symbol, side):
        """Mở vị thế cho một symbol cụ thể"""
        try:
            if self.coin_finder.has_existing_position(symbol):
                self.log(f"⚠️ {symbol} - ĐÃ CÓ VỊ THẾ TRÊN BINANCE, BỎ QUA VÀ TÌM COIN KHÁC")
                self.stop_symbol(symbol)
                return False

            self._check_symbol_position(symbol)
            if self.symbol_data[symbol]['position_open']:
                return False

            current_leverage = self.coin_finder.get_symbol_leverage(symbol)
            if current_leverage < self.lev:
                self.log(f"❌ {symbol} - Đòn bẩy không đủ: {current_leverage}x < {self.lev}x")
                self.stop_symbol(symbol)
                return False

            if not set_leverage(symbol, self.lev, self.api_key, self.api_secret):
                self.log(f"❌ {symbol} - Không thể đặt đòn bẩy")
                self.stop_symbol(symbol)
                return False

            balance = get_balance(self.api_key, self.api_secret)
            if balance is None or balance <= 0:
                self.log(f"❌ {symbol} - Không đủ số dư")
                return False

            current_price = self.get_current_price(symbol)
            if current_price <= 0:
                self.log(f"❌ {symbol} - Lỗi lấy giá")
                self.stop_symbol(symbol)
                return False

            step_size = get_step_size(symbol, self.api_key, self.api_secret)

            usd_amount = balance * (self.percent / 100)
            qty = (usd_amount * self.lev) / current_price
            if step_size > 0:
                qty = math.floor(qty / step_size) * step_size
                qty = round(qty, 8)

            if qty <= 0 or qty < step_size:
                self.log(f"❌ {symbol} - Khối lượng không hợp lệ")
                self.stop_symbol(symbol)
                return False

            cancel_all_orders(symbol, self.api_key, self.api_secret)
            time.sleep(1)

            result = place_order(symbol, side, qty, self.api_key, self.api_secret)
            if result and 'orderId' in result:
                executed_qty = float(result.get('executedQty', 0))
                avg_price = float(result.get('avgPrice', current_price))

                if executed_qty >= 0:
                    time.sleep(1)
                    self._check_symbol_position(symbol)
                    
                    if not self.symbol_data[symbol]['position_open']:
                        self.log(f"❌ {symbol} - Lệnh đã khớp nhưng không tạo được vị thế, có thể bị hủy")
                        self.stop_symbol(symbol)
                        return False
                    
                    self.symbol_data[symbol]['entry'] = avg_price
                    self.symbol_data[symbol]['entry_base'] = avg_price
                    self.symbol_data[symbol]['average_down_count'] = 0
                    self.symbol_data[symbol]['side'] = side
                    self.symbol_data[symbol]['qty'] = executed_qty if side == "BUY" else -executed_qty
                    self.symbol_data[symbol]['position_open'] = True
                    self.symbol_data[symbol]['status'] = "open"
                    self.symbol_data[symbol]['high_water_mark_roi'] = 0
                    self.symbol_data[symbol]['roi_check_activated'] = False

                    message = (
                        f"✅ <b>ĐÃ MỞ VỊ THẾ {symbol}</b>\n"
                        f"🤖 Bot: {self.bot_id}\n"
                        f"📌 Hướng: {side}\n"
                        f"🏷️ Giá vào: {avg_price:.4f}\n"
                        f"📊 Khối lượng: {executed_qty:.4f}\n"
                        f"💰 Đòn bẩy: {self.lev}x\n"
                        f"🎯 TP: {self.tp}% | 🛡️ SL: {self.sl}%"
                    )
                    if self.roi_trigger:
                        message += f" | 🎯 ROI Trigger: {self.roi_trigger}%"
                    
                    self.log(message)
                    return True
                else:
                    self.log(f"❌ {symbol} - Lệnh không khớp")
                    self.stop_symbol(symbol)
                    return False
            else:
                error_msg = result.get('msg', 'Unknown error') if result else 'No response'
                self.log(f"❌ {symbol} - Lỗi đặt lệnh: {error_msg}")
                
                if "position" in error_msg.lower() or "exist" in error_msg.lower():
                    self.log(f"⚠️ {symbol} - Có vấn đề với vị thế, dừng theo dõi và tìm coin khác")
                    self.stop_symbol(symbol)
                else:
                    self.stop_symbol(symbol)
                    
                return False

        except Exception as e:
            self.log(f"❌ {symbol} - Lỗi mở lệnh: {str(e)}")
            self.stop_symbol(symbol)
            return False

    def _close_symbol_position(self, symbol, reason=""):
        """Đóng vị thế cho một symbol cụ thể"""
        try:
            self._check_symbol_position(symbol)
            
            if not self.symbol_data[symbol]['position_open'] or abs(self.symbol_data[symbol]['qty']) <= 0:
                return True

            current_time = time.time()
            if (self.symbol_data[symbol]['close_attempted'] and 
                current_time - self.symbol_data[symbol]['last_close_attempt'] < 30):
                return False
            
            self.symbol_data[symbol]['close_attempted'] = True
            self.symbol_data[symbol]['last_close_attempt'] = current_time

            close_side = "SELL" if self.symbol_data[symbol]['side'] == "BUY" else "BUY"
            close_qty = abs(self.symbol_data[symbol]['qty'])
            
            cancel_all_orders(symbol, self.api_key, self.api_secret)
            time.sleep(1)
            
            result = place_order(symbol, close_side, close_qty, self.api_key, self.api_secret)
            if result and 'orderId' in result:
                current_price = self.get_current_price(symbol)
                pnl = 0
                if self.symbol_data[symbol]['entry'] > 0:
                    if self.symbol_data[symbol]['side'] == "BUY":
                        pnl = (current_price - self.symbol_data[symbol]['entry']) * abs(self.symbol_data[symbol]['qty'])
                    else:
                        pnl = (self.symbol_data[symbol]['entry'] - current_price) * abs(self.symbol_data[symbol]['qty'])
                
                message = (
                    f"⛔ <b>ĐÃ ĐÓNG VỊ THẾ {symbol}</b>\n"
                    f"🤖 Bot: {self.bot_id}\n"
                    f"📌 Lý do: {reason}\n"
                    f"🏷️ Giá ra: {current_price:.4f}\n"
                    f"📊 Khối lượng: {close_qty:.4f}\n"
                    f"💰 PnL: {pnl:.2f} USDC\n"
                    f"📈 Số lần nhồi: {self.symbol_data[symbol]['average_down_count']}"
                )
                self.log(message)
                
                self.symbol_data[symbol]['last_close_time'] = time.time()
                self._reset_symbol_position(symbol)
                
                return True
            else:
                error_msg = result.get('msg', 'Unknown error') if result else 'No response'
                self.log(f"❌ {symbol} - Lỗi đóng lệnh: {error_msg}")
                self.symbol_data[symbol]['close_attempted'] = False
                return False
                
        except Exception as e:
            self.log(f"❌ {symbol} - Lỗi đóng lệnh: {str(e)}")
            self.symbol_data[symbol]['close_attempted'] = False
            return False

    def _check_symbol_tp_sl(self, symbol):
        """Kiểm tra TP/SL cho một symbol cụ thể"""
        if (not self.symbol_data[symbol]['position_open'] or 
            self.symbol_data[symbol]['entry'] <= 0 or 
            self.symbol_data[symbol]['close_attempted']):
            return

        current_price = self.get_current_price(symbol)
        if current_price <= 0:
            return

        if self.symbol_data[symbol]['side'] == "BUY":
            profit = (current_price - self.symbol_data[symbol]['entry']) * abs(self.symbol_data[symbol]['qty'])
        else:
            profit = (self.symbol_data[symbol]['entry'] - current_price) * abs(self.symbol_data[symbol]['qty'])
            
        invested = self.symbol_data[symbol]['entry'] * abs(self.symbol_data[symbol]['qty']) / self.lev
        if invested <= 0:
            return
            
        roi = (profit / invested) * 100

        if roi > self.symbol_data[symbol]['high_water_mark_roi']:
            self.symbol_data[symbol]['high_water_mark_roi'] = roi

        if (self.roi_trigger is not None and 
            self.symbol_data[symbol]['high_water_mark_roi'] >= self.roi_trigger and 
            not self.symbol_data[symbol]['roi_check_activated']):
            self.symbol_data[symbol]['roi_check_activated'] = True

        if self.tp is not None and roi >= self.tp:
            self._close_symbol_position(symbol, f"✅ Đạt TP {self.tp}% (ROI: {roi:.2f}%)")
        elif self.sl is not None and self.sl > 0 and roi <= -self.sl:
            self._close_symbol_position(symbol, f"❌ Đạt SL {self.sl}% (ROI: {roi:.2f}%)")

    def stop_symbol(self, symbol):
        """Dừng một symbol cụ thể"""
        if symbol not in self.active_symbols:
            return False
        
        self.log(f"⛔ Đang dừng coin {symbol}...")
        
        if self.current_processing_symbol == symbol:
            timeout = time.time() + 10
            while self.current_processing_symbol == symbol and time.time() < timeout:
                time.sleep(1)
        
        if self.symbol_data[symbol]['position_open']:
            self._close_symbol_position(symbol, "Dừng coin theo lệnh")
        
        self.ws_manager.remove_symbol(symbol)
        self.coin_manager.unregister_coin(symbol)
        
        if symbol in self.symbol_data:
            del self.symbol_data[symbol]
        
        if symbol in self.active_symbols:
            self.active_symbols.remove(symbol)
        
        self.log(f"✅ Đã dừng coin {symbol}")
        
        return True

    def stop_all_symbols(self):
        """Dừng tất cả coin nhưng vẫn giữ bot chạy"""
        self.log("⛔ Đang dừng tất cả coin...")
        
        symbols_to_stop = self.active_symbols.copy()
        stopped_count = 0
        
        for symbol in symbols_to_stop:
            if self.stop_symbol(symbol):
                stopped_count += 1
                time.sleep(1)
        
        self.log(f"✅ Đã dừng {stopped_count} coin, bot vẫn chạy và có thể thêm coin mới")
        return stopped_count

    def stop(self):
        """Dừng toàn bộ bot"""
        self._stop = True
        stopped_count = self.stop_all_symbols()
        self.log(f"🔴 Bot dừng - Đã dừng {stopped_count} coin")

    def check_global_positions(self):
        """Kiểm tra vị thế toàn tài khoản"""
        try:
            positions = get_positions(api_key=self.api_key, api_secret=self.api_secret)
            if not positions:
                self.global_long_count = 0
                self.global_short_count = 0
                self.global_long_pnl = 0
                self.global_short_pnl = 0
                return
            
            long_count = 0
            short_count = 0
            long_pnl_total = 0
            short_pnl_total = 0
            
            for pos in positions:
                position_amt = float(pos.get('positionAmt', 0))
                unrealized_pnl = float(pos.get('unRealizedProfit', 0))
                
                if position_amt > 0:
                    long_count += 1
                    long_pnl_total += unrealized_pnl
                elif position_amt < 0:
                    short_count += 1
                    short_pnl_total += unrealized_pnl
            
            self.global_long_count = long_count
            self.global_short_count = short_count
            self.global_long_pnl = long_pnl_total
            self.global_short_pnl = short_pnl_total
            
        except Exception as e:
            if time.time() - self.last_error_log_time > 30:
                self.log(f"❌ Lỗi kiểm tra vị thế toàn tài khoản: {str(e)}")
                self.last_error_log_time = time.time()

    def get_next_side_based_on_comprehensive_analysis(self):
        """Xác định hướng lệnh tiếp theo"""
        self.check_global_positions()
        
        long_pnl = self.global_long_pnl
        short_pnl = self.global_short_pnl
        
        if long_pnl > short_pnl:
            return "BUY"
        elif short_pnl > long_pnl:
            return "SELL"
        else:
            return random.choice(["BUY", "SELL"])

    def log(self, message):
        """Chỉ log các thông tin quan trọng"""
        important_keywords = ['❌', '✅', '⛔', '💰', '📈', '📊', '🎯', '🛡️', '🔴', '🟢', '⚠️', '🚫']
        if any(keyword in message for keyword in important_keywords):
            logger.warning(f"[{self.bot_id}] {message}")
            if self.telegram_bot_token and self.telegram_chat_id:
                send_telegram(f"<b>{self.bot_id}</b>: {message}", 
                             bot_token=self.telegram_bot_token, 
                             default_chat_id=self.telegram_chat_id)

# ========== GLOBAL MARKET BOT ==========
class GlobalMarketBot(BaseBot):
    def __init__(self, symbol, lev, percent, tp, sl, roi_trigger, ws_manager,
                 api_key, api_secret, telegram_bot_token, telegram_chat_id, bot_id=None, **kwargs):
        super().__init__(symbol, lev, percent, tp, sl, roi_trigger, ws_manager,
                         api_key, api_secret, telegram_bot_token, telegram_chat_id,
                         "Hệ-thống-RSI-Khối-lượng-MultiThread", bot_id=bot_id, **kwargs)

# ========== BOT MANAGER HOÀN CHỈNH ==========
class BotManager:
    def __init__(self, api_key=None, api_secret=None, telegram_bot_token=None, telegram_chat_id=None):
        self.ws_manager = WebSocketManager()
        self.bots = {}
        self.running = True
        self.start_time = time.time()
        self.user_states = {}

        self.api_key = api_key
        self.api_secret = api_secret
        self.telegram_bot_token = telegram_bot_token
        self.telegram_chat_id = telegram_chat_id

        # Coordinator chung cho tất cả bot
        self.bot_coordinator = BotExecutionCoordinator()
        self.coin_manager = CoinManager()
        self.symbol_locks = defaultdict(threading.Lock)

        if api_key and api_secret:
            self._verify_api_connection()
            self.log("🟢 HỆ THỐNG BOT MULTI-THREAD ĐÃ KHỞI ĐỘNG - MỖI BOT 1 THREAD RIÊNG")

            self.telegram_thread = threading.Thread(target=self._telegram_listener, daemon=True)
            self.telegram_thread.start()

            if self.telegram_chat_id:
                self.send_main_menu(self.telegram_chat_id)
        else:
            self.log("⚡ BotManager khởi động ở chế độ không config")

    def _verify_api_connection(self):
        """Kiểm tra kết nối API"""
        try:
            balance = get_balance(self.api_key, self.api_secret)
            if balance is None:
                self.log("❌ LỖI: Không thể kết nối Binance API. Kiểm tra:")
                self.log("   - API Key và Secret có đúng không?")
                self.log("   - Có thể bị chặn IP (lỗi 451), thử dùng VPN")
                self.log("   - Kiểm tra kết nối internet")
                return False
            else:
                self.log(f"✅ Kết nối Binance thành công! Số dư: {balance:.2f} USDC")
                return True
        except Exception as e:
            self.log(f"❌ Lỗi kiểm tra kết nối: {str(e)}")
            return False

    def get_position_summary(self):
        """Lấy thống kê tổng quan với thông tin multi-thread"""
        try:
            all_positions = get_positions(api_key=self.api_key, api_secret=self.api_secret)
            
            total_long_count = 0
            total_short_count = 0
            total_long_pnl = 0
            total_short_pnl = 0
            total_unrealized_pnl = 0
            
            for pos in all_positions:
                position_amt = float(pos.get('positionAmt', 0))
                if position_amt != 0:
                    unrealized_pnl = float(pos.get('unRealizedProfit', 0))
                    total_unrealized_pnl += unrealized_pnl
                    
                    if position_amt > 0:
                        total_long_count += 1
                        total_long_pnl += unrealized_pnl
                    else:
                        total_short_count += 1
                        total_short_pnl += unrealized_pnl
        
            bot_details = []
            total_bots_with_coins = 0
            trading_bots = 0
            
            for bot_id, bot in self.bots.items():
                has_coin = len(bot.active_symbols) > 0 if hasattr(bot, 'active_symbols') else False
                is_trading = False
                
                if has_coin and hasattr(bot, 'symbol_data'):
                    for symbol, data in bot.symbol_data.items():
                        if data.get('position_open', False):
                            is_trading = True
                            break
                
                if has_coin:
                    total_bots_with_coins += 1
                if is_trading:
                    trading_bots += 1
                
                bot_info = {
                    'bot_id': bot_id,
                    'has_coin': has_coin,
                    'is_trading': is_trading,
                    'symbols': bot.active_symbols if hasattr(bot, 'active_symbols') else [],
                    'symbol_data': bot.symbol_data if hasattr(bot, 'symbol_data') else {},
                    'status': bot.status,
                    'leverage': bot.lev,
                    'percent': bot.percent
                }
                bot_details.append(bot_info)
            
            summary = "📊 **THỐNG KÊ CHI TIẾT - MULTI-THREAD**\n\n"
            
            balance = get_balance(self.api_key, self.api_secret)
            if balance is not None:
                summary += f"💰 **SỐ DƯ**: {balance:.2f} USDC\n"
                summary += f"📈 **Tổng PnL**: {total_unrealized_pnl:.2f} USDC\n\n"
            else:
                summary += f"💰 **SỐ DƯ**: ❌ Lỗi kết nối\n\n"
            
            summary += f"🤖 **BOT HỆ THỐNG**: {len(self.bots)} bot | {total_bots_with_coins} bot có coin | {trading_bots} bot đang trade\n\n"
            
            summary += f"📈 **PHÂN TÍCH PnL VÀ KHỐI LƯỢNG**:\n"
            summary += f"   📊 Số lượng: LONG={total_long_count} | SHORT={total_short_count}\n"
            summary += f"   💰 PnL: LONG={total_long_pnl:.2f} USDC | SHORT={total_short_pnl:.2f} USDC\n"
            summary += f"   ⚖️ Chênh lệch: {abs(total_long_pnl - total_short_pnl):.2f} USDC\n\n"
            
            if bot_details:
                summary += "📋 **CHI TIẾT TỪNG BOT**:\n"
                for bot in bot_details:
                    status_emoji = "🟢" if bot['is_trading'] else "🟡" if bot['has_coin'] else "🔴"
                    summary += f"{status_emoji} **{bot['bot_id']}**\n"
                    summary += f"   💰 ĐB: {bot['leverage']}x | Vốn: {bot['percent']}%\n"
                    
                    if bot['symbols']:
                        for symbol in bot['symbols']:
                            symbol_info = bot['symbol_data'].get(symbol, {})
                            status = "🟢 Đang trade" if symbol_info.get('position_open') else "🟡 Chờ tín hiệu"
                            side = symbol_info.get('side', '')
                            qty = symbol_info.get('qty', 0)
                            
                            summary += f"   🔗 {symbol} | {status}"
                            if side:
                                summary += f" | {side} {abs(qty):.4f}"
                            summary += "\n"
                    else:
                        summary += f"   🔍 Đang tìm coin...\n"
                    
                    summary += "\n"
            
            summary += f"🧵 <b>THÔNG TIN MULTI-THREAD</b>\n"
            summary += f"• Tổng số bot thread: {len(self.bots)}\n"
            summary += f"• Bot đang tìm coin: {self.bot_coordinator._current_finding_bot or 'Không có'}\n"
            summary += f"• Bot trong hàng đợi: {len(self.bot_coordinator._bot_queue)}\n"
            summary += f"• Coin đã phân phối: {len(self.bot_coordinator._found_coins)}\n\n"
            
            summary += f"⚡ <b>TỐI ƯU HIỆU SUẤT</b>\n"
            summary += f"• Cache leverage 5 phút\n"
            summary += f"• WebSocket real-time\n"
            summary += f"• Loại trừ BTC/ETH tự động\n"
            summary += f"• Chỉ 1 bot tìm coin tại thời điểm\n"
            
            return summary
                    
        except Exception as e:
            return f"❌ Lỗi thống kê: {str(e)}"

    def log(self, message):
        """Chỉ log các thông tin quan trọng"""
        important_keywords = ['❌', '✅', '⛔', '💰', '📈', '📊', '🎯', '🛡️', '🔴', '🟢', '⚠️', '🚫']
        if any(keyword in message for keyword in important_keywords):
            logger.warning(f"[SYSTEM] {message}")
            if self.telegram_bot_token and self.telegram_chat_id:
                send_telegram(f"<b>SYSTEM</b>: {message}", 
                             chat_id=self.telegram_chat_id,
                             bot_token=self.telegram_bot_token, 
                             default_chat_id=self.telegram_chat_id)

    def send_main_menu(self, chat_id):
        welcome = (
            "🤖 <b>BOT GIAO DỊCH FUTURES - MULTI-THREAD</b>\n\n"
            "🎯 <b>MÔ HÌNH MỚI - MỖI BOT 1 THREAD:</b>\n"
            "• Mỗi bot chạy trong thread riêng biệt\n"
            "• Cache đòn bẩy chung - giảm API calls\n"
            "• Tự động loại trừ BTC/ETH\n"
            "• Chỉ 1 bot được tìm coin tại thời điểm\n"
            "• Bot tìm xong thì bot tiếp theo mới được tìm\n\n"
            
            "⚡ <b>TỐI ƯU HIỆU SUẤT:</b>\n"
            "• Cache leverage 5 phút\n"
            "• WebSocket real-time cho tất cả bot\n"
            "• Giảm 80% API calls với cache\n"
            "• Phân tải xử lý qua multiple threads\n\n"
            
            "🚫 <b>TỰ ĐỘNG LOẠI TRỪ:</b>\n"
            "• BTCUSDC - Quá biến động\n"
            "• ETHUSDC - Khối lượng lớn\n"
            "• Coin đã có vị thế\n"
            "• Coin không đủ đòn bẩy"
        )
        send_telegram(welcome, chat_id=chat_id, reply_markup=create_main_menu(),
                     bot_token=self.telegram_bot_token, 
                     default_chat_id=self.telegram_chat_id)

    def add_bot(self, symbol, lev, percent, tp, sl, roi_trigger, strategy_type, bot_count=1, **kwargs):
        if sl == 0:
            sl = None
            
        if not self.api_key or not self.api_secret:
            self.log("❌ Chưa thiết lập API Key trong BotManager")
            return False
        
        if not self._verify_api_connection():
            self.log("❌ KHÔNG THỂ KẾT NỐI BINANCE - KHÔNG THỂ TẠO BOT")
            return False
        
        bot_mode = kwargs.get('bot_mode', 'static')
        created_count = 0
        
        try:
            for i in range(bot_count):
                if bot_mode == 'static' and symbol:
                    bot_id = f"STATIC_{strategy_type}_{int(time.time())}_{i}"
                else:
                    bot_id = f"DYNAMIC_{strategy_type}_{int(time.time())}_{i}"
                
                if bot_id in self.bots:
                    continue
                
                bot_class = GlobalMarketBot
                
                bot = bot_class(
                    symbol, lev, percent, tp, sl, roi_trigger, self.ws_manager,
                    self.api_key, self.api_secret, self.telegram_bot_token, self.telegram_chat_id,
                    coin_manager=self.coin_manager,
                    symbol_locks=self.symbol_locks,
                    bot_coordinator=self.bot_coordinator,
                    bot_id=bot_id,
                    max_coins=1
                )
                
                bot._bot_manager = self
                self.bots[bot_id] = bot
                created_count += 1
                
        except Exception as e:
            self.log(f"❌ Lỗi tạo bot: {str(e)}")
            return False
        
        if created_count > 0:
            roi_info = f" | 🎯 ROI Trigger: {roi_trigger}%" if roi_trigger else " | 🎯 ROI Trigger: Tắt"
            
            success_msg = (
                f"✅ <b>ĐÃ TẠO {created_count} BOT MULTI-THREAD</b>\n\n"
                f"🎯 Chiến lược: {strategy_type}\n"
                f"💰 Đòn bẩy: {lev}x\n"
                f"📈 % Số dư: {percent}%\n"
                f"🎯 TP: {tp}%\n"
                f"🛡️ SL: {sl if sl is not None else 'Tắt'}%{roi_info}\n"
                f"🔧 Chế độ: {bot_mode}\n"
                f"🔢 Số bot: {created_count} (mỗi bot 1 thread)\n"
            )
            
            if bot_mode == 'static' and symbol:
                success_msg += f"🔗 Coin khởi tạo: {symbol}\n"
            else:
                success_msg += f"🔗 Coin: Tự động tìm kiếm\n"
            
            success_msg += f"\n🔄 <b>CƠ CHẾ TÌM COIN TUẦN TỰ ĐÃ KÍCH HOẠT</b>\n"
            success_msg += f"• Chỉ 1 bot được tìm coin tại thời điểm\n"
            success_msg += f"• Bot tìm xong thì bot tiếp theo được quyền tìm\n"
            success_msg += f"• Cache đòn bẩy chung cho tất cả bot\n"
            success_msg += f"• Loại trừ BTC/ETH tự động\n\n"
            success_msg += f"⚡ <b>MỖI BOT CHẠY TRONG THREAD RIÊNG</b>"
            
            self.log(success_msg)
            return True
        else:
            self.log("❌ Không thể tạo bot")
            return False

    def stop_coin(self, symbol):
        """Dừng một coin cụ thể trong tất cả bot"""
        stopped_count = 0
        symbol = symbol.upper()
        
        for bot_id, bot in self.bots.items():
            if hasattr(bot, 'stop_symbol') and symbol in bot.active_symbols:
                if bot.stop_symbol(symbol):
                    stopped_count += 1
                    
        if stopped_count > 0:
            self.log(f"✅ Đã dừng coin {symbol} trong {stopped_count} bot")
            return True
        else:
            self.log(f"❌ Không tìm thấy coin {symbol} trong bất kỳ bot nào")
            return False

    def get_coin_management_keyboard(self):
        """Tạo keyboard quản lý coin"""
        all_coins = set()
        for bot in self.bots.values():
            if hasattr(bot, 'active_symbols'):
                all_coins.update(bot.active_symbols)
        
        if not all_coins:
            return None
            
        keyboard = []
        row = []
        for coin in sorted(list(all_coins))[:12]:
            row.append({"text": f"⛔ Coin: {coin}"})
            if len(row) == 2:
                keyboard.append(row)
                row = []
        if row:
            keyboard.append(row)
        
        keyboard.append([{"text": "⛔ DỪNG TẤT CẢ COIN"}])
        keyboard.append([{"text": "❌ Hủy bỏ"}])
        
        return {
            "keyboard": keyboard,
            "resize_keyboard": True,
            "one_time_keyboard": True
        }

    def stop_bot_symbol(self, bot_id, symbol):
        """Dừng một coin cụ thể trong bot"""
        bot = self.bots.get(bot_id)
        if bot and hasattr(bot, 'stop_symbol'):
            success = bot.stop_symbol(symbol)
            if success:
                self.log(f"⛔ Đã dừng coin {symbol} trong bot {bot_id}")
            return success
        return False

    def stop_all_bot_symbols(self, bot_id):
        """Dừng tất cả coin trong một bot"""
        bot = self.bots.get(bot_id)
        if bot and hasattr(bot, 'stop_all_symbols'):
            stopped_count = bot.stop_all_symbols()
            self.log(f"⛔ Đã dừng {stopped_count} coin trong bot {bot_id}")
            return stopped_count
        return 0

    def stop_all_coins(self):
        """Dừng tất cả coin trong tất cả bot"""
        self.log("⛔ Đang dừng tất cả coin trong tất cả bot...")
        
        total_stopped = 0
        for bot_id, bot in self.bots.items():
            if hasattr(bot, 'stop_all_symbols'):
                stopped_count = bot.stop_all_symbols()
                total_stopped += stopped_count
                self.log(f"⛔ Đã dừng {stopped_count} coin trong bot {bot_id}")
        
        self.log(f"✅ Đã dừng tổng cộng {total_stopped} coin, hệ thống vẫn chạy và có thể thêm coin mới")
        return total_stopped

    def stop_bot(self, bot_id):
        """Dừng toàn bộ bot"""
        bot = self.bots.get(bot_id)
        if bot:
            bot.stop()
            del self.bots[bot_id]
            self.log(f"🔴 Đã dừng bot {bot_id}")
            return True
        return False

    def stop_all(self):
        """Dừng tất cả bot"""
        self.log("🔴 Đang dừng tất cả bot...")
        for bot_id in list(self.bots.keys()):
            self.stop_bot(bot_id)
        self.log("🔴 Đã dừng tất cả bot, hệ thống vẫn chạy và có thể thêm bot mới")

    def _telegram_listener(self):
        """Telegram listener"""
        last_update_id = 0
        
        while self.running and self.telegram_bot_token:
            try:
                url = f"https://api.telegram.org/bot{self.telegram_bot_token}/getUpdates?offset={last_update_id+1}&timeout=5"
                response = requests.get(url, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    if data.get('ok'):
                        for update in data['result']:
                            update_id = update['update_id']
                            message = update.get('message', {})
                            chat_id = str(message.get('chat', {}).get('id'))
                            text = message.get('text', '').strip()
                            
                            if chat_id != self.telegram_chat_id:
                                continue
                            
                            if update_id > last_update_id:
                                last_update_id = update_id
                                self._handle_telegram_message(chat_id, text)
                
                time.sleep(0.1)
                
            except Exception as e:
                logger.error(f"Lỗi Telegram listener: {str(e)}")
                time.sleep(1)

    def _handle_telegram_message(self, chat_id, text):
        """Xử lý tin nhắn Telegram"""
        user_state = self.user_states.get(chat_id, {})
        current_step = user_state.get('step')
        
        # Xử lý các bước tạo bot
        if current_step == 'waiting_bot_count':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id=chat_id, reply_markup=create_main_menu(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                try:
                    bot_count = int(text)
                    if bot_count <= 0 or bot_count > 10:
                        send_telegram("⚠️ Số lượng bot phải từ 1 đến 10. Vui lòng chọn lại:",
                                    chat_id=chat_id, reply_markup=create_bot_count_keyboard(),
                                    bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                        return
    
                    user_state['bot_count'] = bot_count
                    user_state['step'] = 'waiting_bot_mode'
                    
                    send_telegram(
                        f"🤖 Số lượng bot: {bot_count}\n\n"
                        f"Chọn chế độ bot:",
                        chat_id=chat_id,
                        reply_markup=create_bot_mode_keyboard(),
                        bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id
                    )
                except ValueError:
                    send_telegram("⚠️ Vui lòng nhập số hợp lệ cho số lượng bot:",
                                chat_id=chat_id, reply_markup=create_bot_count_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
    
        elif current_step == 'waiting_bot_mode':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id=chat_id, reply_markup=create_main_menu(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            elif text in ["🤖 Bot Tĩnh - Coin cụ thể", "🔄 Bot Động - Tự tìm coin"]:
                if text == "🤖 Bot Tĩnh - Coin cụ thể":
                    user_state['bot_mode'] = 'static'
                    user_state['step'] = 'waiting_symbol'
                    send_telegram(
                        "🎯 <b>ĐÃ CHỌN: BOT TĨNH</b>\n\n"
                        "🤖 Bot sẽ giao dịch coin CỐ ĐỊNH\n"
                        "📊 Bạn cần chọn coin cụ thể\n\n"
                        "Chọn coin:",
                        chat_id=chat_id,
                        reply_markup=create_symbols_keyboard(),
                        bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id
                    )
                else:
                    user_state['bot_mode'] = 'dynamic'
                    user_state['step'] = 'waiting_leverage'
                    send_telegram(
                        "🎯 <b>ĐÃ CHỌN: BOT ĐỘNG</b>\n\n"
                        f"🤖 Hệ thống sẽ tạo bot quản lý <b>{user_state.get('bot_count', 1)} coin</b>\n"
                        f"🔄 Bot sẽ xử lý từng coin một theo thứ tự\n\n"
                        "Chọn đòn bẩy:",
                        chat_id=chat_id,
                        reply_markup=create_leverage_keyboard(),
                        bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id
                    )
    
        elif current_step == 'waiting_symbol':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id=chat_id, reply_markup=create_main_menu(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                user_state['symbol'] = text
                user_state['step'] = 'waiting_leverage'
                send_telegram(
                    f"🔗 Coin: {text}\n\n"
                    f"Chọn đòn bẩy:",
                    chat_id=chat_id,
                    reply_markup=create_leverage_keyboard(),
                    bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id
                )
    
        elif current_step == 'waiting_leverage':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id=chat_id, reply_markup=create_main_menu(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                if text.endswith('x'):
                    lev_text = text[:-1]
                else:
                    lev_text = text
    
                try:
                    leverage = int(lev_text)
                    if leverage <= 0 or leverage > 100:
                        send_telegram("⚠️ Đòn bẩy phải từ 1 đến 100. Vui lòng chọn lại:",
                                    chat_id=chat_id, reply_markup=create_leverage_keyboard(),
                                    bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                        return
    
                    user_state['leverage'] = leverage
                    user_state['step'] = 'waiting_percent'
                    
                    balance = get_balance(self.api_key, self.api_secret)
                    balance_info = f"\n💰 Số dư hiện có: {balance:.2f} USDT" if balance else ""
                    
                    send_telegram(
                        f"💰 Đòn bẩy: {leverage}x{balance_info}\n\n"
                        f"Chọn % số dư cho mỗi lệnh:",
                        chat_id=chat_id,
                        reply_markup=create_percent_keyboard(),
                        bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id
                    )
                except ValueError:
                    send_telegram("⚠️ Vui lòng nhập số hợp lệ cho đòn bẩy:",
                                chat_id=chat_id, reply_markup=create_leverage_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
    
        elif current_step == 'waiting_percent':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id=chat_id, reply_markup=create_main_menu(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                try:
                    percent = float(text)
                    if percent <= 0 or percent > 100:
                        send_telegram("⚠️ % số dư phải từ 0.1 đến 100. Vui lòng chọn lại:",
                                    chat_id=chat_id, reply_markup=create_percent_keyboard(),
                                    bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                        return
    
                    user_state['percent'] = percent
                    user_state['step'] = 'waiting_tp'
                    
                    balance = get_balance(self.api_key, self.api_secret)
                    actual_amount = balance * (percent / 100) if balance else 0
                    
                    send_telegram(
                        f"📊 % Số dư: {percent}%\n"
                        f"💵 Số tiền mỗi lệnh: ~{actual_amount:.2f} USDT\n\n"
                        f"Chọn Take Profit (%):",
                        chat_id=chat_id,
                        reply_markup=create_tp_keyboard(),
                        bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id
                    )
                except ValueError:
                    send_telegram("⚠️ Vui lòng nhập số hợp lệ cho % số dư:",
                                chat_id=chat_id, reply_markup=create_percent_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
    
        elif current_step == 'waiting_tp':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id=chat_id, reply_markup=create_main_menu(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                try:
                    tp = float(text)
                    if tp <= 0:
                        send_telegram("⚠️ Take Profit phải lớn hơn 0. Vui lòng chọn lại:",
                                    chat_id=chat_id, reply_markup=create_tp_keyboard(),
                                    bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                        return
    
                    user_state['tp'] = tp
                    user_state['step'] = 'waiting_sl'
                    
                    send_telegram(
                        f"🎯 Take Profit: {tp}%\n\n"
                        f"Chọn Stop Loss (%):",
                        chat_id=chat_id,
                        reply_markup=create_sl_keyboard(),
                        bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id
                    )
                except ValueError:
                    send_telegram("⚠️ Vui lòng nhập số hợp lệ cho Take Profit:",
                                chat_id=chat_id, reply_markup=create_tp_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
    
        elif current_step == 'waiting_sl':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id=chat_id, reply_markup=create_main_menu(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                try:
                    sl = float(text)
                    if sl < 0:
                        send_telegram("⚠️ Stop Loss phải lớn hơn hoặc bằng 0. Vui lòng chọn lại:",
                                    chat_id=chat_id, reply_markup=create_sl_keyboard(),
                                    bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                        return
    
                    user_state['sl'] = sl
                    user_state['step'] = 'waiting_roi_trigger'
                    
                    send_telegram(
                        f"🛡️ Stop Loss: {sl}%\n\n"
                        f"🎯 <b>CHỌN NGƯỠNG ROI ĐỂ KÍCH HOẠT CƠ CHẾ CHỐT LỆNH THÔNG MINH</b>\n\n"
                        f"Chọn ngưỡng ROI trigger (%):",
                        chat_id=chat_id,
                        reply_markup=create_roi_trigger_keyboard(),
                        bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id
                    )
                except ValueError:
                    send_telegram("⚠️ Vui lòng nhập số hợp lệ cho Stop Loss:",
                                chat_id=chat_id, reply_markup=create_sl_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
    
        elif current_step == 'waiting_roi_trigger':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id=chat_id, reply_markup=create_main_menu(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            elif text == '❌ Tắt tính năng':
                user_state['roi_trigger'] = None
                self._finish_bot_creation(chat_id, user_state)
            else:
                try:
                    roi_trigger = float(text)
                    if roi_trigger <= 0:
                        send_telegram("⚠️ ROI Trigger phải lớn hơn 0. Vui lòng chọn lại:",
                                    chat_id=chat_id, reply_markup=create_roi_trigger_keyboard(),
                                    bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                        return
    
                    user_state['roi_trigger'] = roi_trigger
                    self._finish_bot_creation(chat_id, user_state)
                    
                except ValueError:
                    send_telegram("⚠️ Vui lòng nhập số hợp lệ cho ROI Trigger:",
                                chat_id=chat_id, reply_markup=create_roi_trigger_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
    
        # XỬ LÝ LỆNH QUẢN LÝ COIN
        elif text == "⛔ Quản lý Coin":
            keyboard = self.get_coin_management_keyboard()
            if not keyboard:
                send_telegram("📭 Không có coin nào đang được quản lý", chat_id=chat_id,
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                send_telegram(
                    "⛔ <b>QUẢN LÝ COIN</b>\n\n"
                    "Chọn coin để dừng:",
                    chat_id=chat_id, 
                    reply_markup=keyboard,
                    bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id
                )
        
        elif text.startswith("⛔ Coin: "):
            symbol = text.replace("⛔ Coin: ", "").strip()
            if self.stop_coin(symbol):
                send_telegram(f"✅ Đã dừng coin {symbol}", chat_id=chat_id,
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                send_telegram(f"❌ Không thể dừng coin {symbol}", chat_id=chat_id,
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif text == "⛔ DỪNG TẤT CẢ COIN":
            stopped_count = self.stop_all_coins()
            send_telegram(f"✅ Đã dừng {stopped_count} coin, hệ thống vẫn chạy", chat_id=chat_id,
                         bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif text.startswith("⛔ Bot: "):
            bot_id = text.replace("⛔ Bot: ", "").strip()
            if self.stop_bot(bot_id):
                send_telegram(f"✅ Đã dừng bot {bot_id}", chat_id=chat_id,
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                send_telegram(f"❌ Không tìm thấy bot {bot_id}", chat_id=chat_id,
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif text == "⛔ DỪNG TẤT CẢ BOT":
            stopped_count = len(self.bots)
            self.stop_all()
            send_telegram(f"✅ Đã dừng {stopped_count} bot, hệ thống vẫn chạy", chat_id=chat_id,
                         bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif text == "➕ Thêm Bot":
            self.user_states[chat_id] = {'step': 'waiting_bot_count'}
            balance = get_balance(self.api_key, self.api_secret)
            if balance is None:
                send_telegram("❌ <b>LỖI KẾT NỐI BINANCE</b>\nVui lòng kiểm tra API Key và kết nối mạng!", chat_id=chat_id,
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                return
            
            send_telegram(
                f"🎯 <b>CHỌN SỐ LƯỢNG BOT</b>\n\n"
                f"💰 Số dư hiện có: <b>{balance:.2f} USDT</b>\n\n"
                f"Chọn số lượng bot (mỗi bot quản lý 1 coin):",
                chat_id=chat_id,
                reply_markup=create_bot_count_keyboard(),
                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id
            )
        
        elif text == "📊 Danh sách Bot":
            summary = self.get_position_summary()
            send_telegram(summary, chat_id=chat_id,
                         bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif text == "⛔ Dừng Bot":
            if not self.bots:
                send_telegram("🤖 Không có bot nào đang chạy", chat_id=chat_id,
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                message = "⛔ <b>CHỌN BOT ĐỂ DỪNG</b>\n\n"
                
                bot_keyboard = []
                
                for bot_id, bot in self.bots.items():
                    bot_keyboard.append([{"text": f"⛔ Bot: {bot_id}"}])
                
                keyboard = []
                
                if bot_keyboard:
                    keyboard.extend(bot_keyboard)
                    keyboard.append([{"text": "⛔ DỪNG TẤT CẢ BOT"}])
                
                keyboard.append([{"text": "❌ Hủy bỏ"}])
                
                send_telegram(
                    message, 
                    chat_id=chat_id, 
                    reply_markup={"keyboard": keyboard, "resize_keyboard": True, "one_time_keyboard": True},
                    bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id
                )
        
        elif text == "📊 Thống kê":
            summary = self.get_position_summary()
            send_telegram(summary, chat_id=chat_id,
                         bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif text == "💰 Số dư":
            try:
                balance = get_balance(self.api_key, self.api_secret)
                if balance is None:
                    send_telegram("❌ <b>LỖI KẾT NỐI BINANCE</b>\nVui lòng kiểm tra API Key và kết nối mạng!", chat_id=chat_id,
                                 bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                else:
                    send_telegram(f"💰 <b>SỐ DƯ KHẢ DỤNG</b>: {balance:.2f} USDT", chat_id=chat_id,
                                 bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            except Exception as e:
                send_telegram(f"⚠️ Lỗi lấy số dư: {str(e)}", chat_id=chat_id,
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif text == "📈 Vị thế":
            try:
                positions = get_positions(api_key=self.api_key, api_secret=self.api_secret)
                if not positions:
                    send_telegram("📭 Không có vị thế nào đang mở", chat_id=chat_id,
                                 bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                    return
                
                message = "📈 <b>VỊ THẾ ĐANG MỞ</b>\n\n"
                for pos in positions:
                    position_amt = float(pos.get('positionAmt', 0))
                    if position_amt != 0:
                        symbol = pos.get('symbol', 'UNKNOWN')
                        entry = float(pos.get('entryPrice', 0))
                        side = "LONG" if position_amt > 0 else "SHORT"
                        pnl = float(pos.get('unRealizedProfit', 0))
                        
                        message += (
                            f"🔹 {symbol} | {side}\n"
                            f"📊 Khối lượng: {abs(position_amt):.4f}\n"
                            f"🏷️ Giá vào: {entry:.4f}\n"
                            f"💰 PnL: {pnl:.2f} USDT\n\n"
                        )
                
                send_telegram(message, chat_id=chat_id,
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            except Exception as e:
                send_telegram(f"⚠️ Lỗi lấy vị thế: {str(e)}", chat_id=chat_id,
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif text == "🎯 Chiến lược":
            strategy_info = (
                "🎯 <b>HỆ THỐNG RSI + KHỐI LƯỢNG NÂNG CAO</b>\n\n"
                
                "📈 <b>6 ĐIỀU KIỆN VÀO LỆNH:</b>\n"
                "1. RSI > 80 + giá tăng + volume tăng → BÁN\n"
                "2. RSI < 20 + giá giảm + volume giảm → BÁN\n"  
                "3. RSI > 80 + giá tăng + volume giảm → MUA\n"
                "4. RSI < 20 + giá giảm + volume tăng → MUA\n"
                "5. RSI > 20 + giá không giảm + volume giảm → MUA\n"
                "6. RSI < 80 + giá không tăng + volume tăng → BÁN\n\n"
                
                "🎯 <b>ĐIỀU KIỆN ĐÓNG LỆNH:</b>\n"
                "• GIỐNG HỆT điều kiện vào lệnh\n"
                "• Nhưng khối lượng thay đổi 40% (thay vì 20%)\n"
                "• VÀ phải đạt ROI trigger do người dùng thiết lập\n"
                "• Chỉ chốt lời, không vào lệnh ngược\n\n"
                
                "🔄 <b>CƠ CHẾ ĐIỀU PHỐI NỐI TIẾP THỰC SỰ:</b>\n"
                "• Hàng đợi tuần tự cố định\n"
                "• Chỉ 1 bot được thực thi tại thời điểm\n"
                "• Bot thực thi xong được chuyển xuống cuối hàng đợi\n"
                "• Chờ 1s giữa các bot\n\n"
                
                "🚫 <b>KIỂM TRA VỊ THẾ:</b>\n"
                "• Tự động phát hiện coin đã có vị thế\n"
                "• Không vào lệnh trên coin đã có vị thế\n"
                "• Tự động chuyển sang tìm coin khác"
            )
            send_telegram(strategy_info, chat_id=chat_id,
                         bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif text == "⚙️ Cấu hình":
            balance = get_balance(self.api_key, self.api_secret)
            api_status = "✅ Đã kết nối" if balance is not None else "❌ Lỗi kết nối"
            
            total_bots_with_coins = 0
            trading_bots = 0
            
            for bot in self.bots.values():
                if hasattr(bot, 'active_symbols'):
                    if len(bot.active_symbols) > 0:
                        total_bots_with_coins += 1
                    for symbol, data in bot.symbol_data.items():
                        if data.get('position_open', False):
                            trading_bots += 1
            
            config_info = (
                "⚙️ <b>CẤU HÌNH HỆ THỐNG RSI + KHỐI LƯỢNG</b>\n\n"
                f"🔑 Binance API: {api_status}\n"
                f"🤖 Tổng số bot: {len(self.bots)}\n"
                f"📊 Bot có coin: {total_bots_with_coins}\n"
                f"🟢 Bot đang trade: {trading_bots}\n"
                f"🌐 WebSocket: {len(self.ws_manager.connections)} kết nối\n"
                f"🔄 Cooldown: 1s\n"
                f"📋 Hàng đợi: {len(self.bot_coordinator._bot_queue)} bot\n\n"
                f"🔄 <b>CƠ CHẾ NỐI TIẾP THỰC SỰ ĐANG HOẠT ĐỘNG</b>\n"
                f"🎯 <b>6 ĐIỀU KIỆN RSI ĐANG HOẠT ĐỘNG</b>"
            )
            send_telegram(config_info, chat_id=chat_id,
                         bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif text:
            self.send_main_menu(chat_id)

    def _finish_bot_creation(self, chat_id, user_state):
        """Hoàn tất quá trình tạo bot"""
        try:
            bot_mode = user_state.get('bot_mode', 'static')
            leverage = user_state.get('leverage')
            percent = user_state.get('percent')
            tp = user_state.get('tp')
            sl = user_state.get('sl')
            roi_trigger = user_state.get('roi_trigger')
            symbol = user_state.get('symbol')
            bot_count = user_state.get('bot_count', 1)
            
            success = self.add_bot(
                symbol=symbol,
                lev=leverage,
                percent=percent,
                tp=tp,
                sl=sl,
                roi_trigger=roi_trigger,
                strategy_type="Hệ-thống-RSI-Khối-lượng",
                bot_mode=bot_mode,
                bot_count=bot_count
            )
            
            if success:
                roi_info = f" | 🎯 ROI Trigger: {roi_trigger}%" if roi_trigger else ""
                
                success_msg = (
                    f"✅ <b>ĐÃ TẠO BOT THÀNH CÔNG</b>\n\n"
                    f"🤖 Chiến lược: Hệ thống RSI + Khối lượng\n"
                    f"🔧 Chế độ: {bot_mode}\n"
                    f"🔢 Số bot: {bot_count} (mỗi bot 1 thread)\n"
                    f"💰 Đòn bẩy: {leverage}x\n"
                    f"📊 % Số dư: {percent}%\n"
                    f"🎯 TP: {tp}%\n"
                    f"🛡️ SL: {sl}%{roi_info}"
                )
                if bot_mode == 'static' and symbol:
                    success_msg += f"\n🔗 Coin: {symbol}"
                
                success_msg += f"\n\n🔄 <b>CƠ CHẾ TÌM COIN TUẦN TỰ ĐÃ KÍCH HOẠT</b>\n"
                success_msg += f"• Chỉ 1 bot được tìm coin tại thời điểm\n"
                success_msg += f"• Bot tìm xong thì bot tiếp theo được quyền tìm\n"
                success_msg += f"• Cache đòn bẩy chung cho tất cả bot\n"
                success_msg += f"• Loại trừ BTC/ETH tự động\n\n"
                success_msg += f"⚡ <b>MỖI BOT CHẠY TRONG THREAD RIÊNG</b>"
                
                send_telegram(success_msg, chat_id=chat_id, reply_markup=create_main_menu(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                send_telegram("❌ Có lỗi khi tạo bot. Vui lòng thử lại.",
                            chat_id=chat_id, reply_markup=create_main_menu(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            
            self.user_states[chat_id] = {}
            
        except Exception as e:
            send_telegram(f"❌ Lỗi tạo bot: {str(e)}", chat_id=chat_id, reply_markup=create_main_menu(),
                        bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            self.user_states[chat_id] = {}
