# trading_bot_lib_complete_part1.py - PHẦN 1: HỆ THỐNG RSI + KHỐI LƯỢNG VỚI CƠ CHẾ NỐI TIẾP
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

# ========== BYPASS SSL VERIFICATION ==========
ssl._create_default_https_context = ssl._create_unverified_context

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
    """Gửi tin nhắn Telegram - ĐÃ SỬA LỖI TOKEN"""
    if not bot_token:
        logger.warning("Telegram Bot Token chưa được thiết lập")
        return False
    
    chat_id = chat_id or default_chat_id
    if not chat_id:
        logger.warning("Telegram Chat ID chưa được thiết lập")
        return False
    
    # 🔴 SỬA QUAN TRỌNG: KHÔNG THÊM 'bot' VÀO ĐẦU TOKEN
    original_token = bot_token
    
    # Chuẩn hóa token - loại bỏ khoảng trắng
    bot_token = bot_token.strip()
    
    # Kiểm tra định dạng token
    if ':' not in bot_token:
        logger.error(f"❌ Bot Token không hợp lệ: {bot_token[:10]}...")
        logger.error("   📝 Token phải có định dạng: 1234567890:ABCdefGHIjklMNOpqrsTUVwxyz")
        return False
    
    # Tách token để kiểm tra
    parts = bot_token.split(':')
    if len(parts) != 2:
        logger.error(f"❌ Bot Token không đúng định dạng: {bot_token[:10]}...")
        return False
    
    bot_id, token_key = parts
    if not bot_id.isdigit() or len(token_key) < 10:
        logger.error(f"❌ Bot Token không hợp lệ: {bot_token[:10]}...")
        return False
    
    # 🔴 QUAN TRỌNG: KHÔNG THÊM 'bot' VÀO ĐẦU TOKEN
    # Telegram API yêu cầu token ở dạng gốc, không có 'bot' prefix
    
    # KIỂM TRA CHAT ID CÓ PHẢI LÀ SỐ NGUYÊN
    try:
        chat_id_int = int(chat_id)
    except (ValueError, TypeError):
        logger.error(f"❌ Chat ID không hợp lệ: {chat_id}")
        return False
    
    # 🔴 SỬA URL: Sử dụng token gốc, không thêm 'bot'
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    
    # ESCAPE MESSAGE ĐỂ TRÁNH LỖI HTML
    safe_message = escape_html(message)
    
    payload = {
        "chat_id": chat_id_int,
        "text": safe_message,
        "parse_mode": "HTML"
    }
    
    if reply_markup:
        payload["reply_markup"] = reply_markup
    
    try:
        headers = {
            'Content-Type': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        logger.info(f"🔗 Đang gửi Telegram đến {chat_id_int}...")
        
        response = requests.post(
            url, 
            json=payload, 
            headers=headers,
            timeout=15
        )
        
        if response.status_code == 200:
            logger.info("✅ Đã gửi Telegram thành công")
            return True
        else:
            error_data = response.json()
            error_description = error_data.get('description', 'Unknown error')
            
            if response.status_code == 404:
                logger.error(f"❌ Lỗi Telegram 404: {error_description}")
                logger.error(f"   Token: {bot_token[:15]}...")
                logger.error(f"   Chat ID: {chat_id_int}")
            else:
                logger.error(f"❌ Lỗi Telegram ({response.status_code}): {error_description}")
            
            return False
            
    except requests.exceptions.Timeout:
        logger.error("⏰ Timeout kết nối Telegram")
        return False
    except requests.exceptions.ConnectionError:
        logger.error("🔗 Lỗi kết nối mạng đến Telegram")
        return False
    except Exception as e:
        logger.error(f"❌ Lỗi không xác định khi gửi Telegram: {str(e)}")
        return False
def validate_telegram_config(bot_token, chat_id):
    """Kiểm tra và chuẩn hóa cấu hình Telegram"""
    if not bot_token or not chat_id:
        return None, None, "❌ Thiếu Bot Token hoặc Chat ID"
    
    # Chuẩn hóa token
    bot_token = bot_token.strip()
    
    # Kiểm tra định dạng token
    if ':' not in bot_token:
        return None, None, f"❌ Token không hợp lệ. Định dạng phải là: 1234567890:ABCdefGHIjklMNOpqrsTUVwxyz"
    
    parts = bot_token.split(':')
    if len(parts) != 2:
        return None, None, "❌ Token không đúng định dạng"
    
    bot_id, token_key = parts
    if not bot_id.isdigit() or len(token_key) < 10:
        return None, None, "❌ Token không hợp lệ: phần đầu phải là số, phần sau ít nhất 10 ký tự"
    
    # 🔴 QUAN TRỌNG: GIỮ NGUYÊN TOKEN, KHÔNG THÊM 'bot'
    
    # Chuẩn hóa chat_id
    try:
        chat_id = str(int(chat_id))
    except (ValueError, TypeError):
        return None, None, "❌ Chat ID phải là số nguyên"
    
    return bot_token, chat_id, "✅ Cấu hình Telegram hợp lệ"
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
            symbols = ["BTCUSDC", "ETHUSDC", "BNBUSDC", "ADAUSDC", "DOGEUSDC", "XRPUSDC", "DOTUSDC", "LINKUSDC"]
    except:
        symbols = ["BTCUSDC", "ETHUSDC", "BNBUSDC", "ADAUSDC", "DOGEUSDC", "XRPUSDC", "DOTUSDC", "LINKUSDC"]
    
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
            [{"text": "💰 Số dư"}, {"text": "📈 Vị thế"}],
            [{"text": "⚙️ Cấu hình"}, {"text": "🎯 Chiến lược"}]
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

def create_coin_count_keyboard():
    """Bàn phím chọn số lượng coin cho mỗi bot"""
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

# ========== API BINANCE - ĐÃ SỬA LỖI 451 ==========
def sign(query, api_secret):
    try:
        return hmac.new(api_secret.encode(), query.encode(), hashlib.sha256).hexdigest()
    except Exception as e:
        logger.error(f"Lỗi tạo chữ ký: {str(e)}")
        return ""

def binance_api_request(url, method='GET', params=None, headers=None):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # Thêm User-Agent để tránh bị chặn
            if headers is None:
                headers = {}
            
            if 'User-Agent' not in headers:
                headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            
            if method.upper() == 'GET':
                if params:
                    query = urllib.parse.urlencode(params)
                    url = f"{url}?{query}"
                req = urllib.request.Request(url, headers=headers)
            else:
                data = urllib.parse.urlencode(params).encode() if params else None
                req = urllib.request.Request(url, data=data, headers=headers, method=method)
            
            # Tăng timeout và thêm retry logic
            with urllib.request.urlopen(req, timeout=30) as response:
                if response.status == 200:
                    return json.loads(response.read().decode())
                else:
                    error_content = response.read().decode()
                    logger.error(f"Lỗi API ({response.status}): {error_content}")
                    if response.status == 401:
                        return None
                    if response.status == 429:
                        time.sleep(2 ** attempt)
                    elif response.status >= 500:
                        time.sleep(1)
                    continue
                    
        except urllib.error.HTTPError as e:
            if e.code == 451:
                logger.error(f"❌ Lỗi 451: Truy cập bị chặn - Có thể do hạn chế địa lý. Vui lòng kiểm tra VPN/proxy.")
                # Thử sử dụng endpoint thay thế
                if "fapi.binance.com" in url:
                    new_url = url.replace("fapi.binance.com", "fapi.binance.com")
                    logger.info(f"Thử URL thay thế: {new_url}")
                return None
            else:
                logger.error(f"Lỗi HTTP ({e.code}): {e.reason}")
            
            if e.code == 401:
                return None
            if e.code == 429:
                time.sleep(2 ** attempt)
            elif e.code >= 500:
                time.sleep(1)
            continue
                
        except Exception as e:
            logger.error(f"Lỗi kết nối API (lần {attempt + 1}): {str(e)}")
            time.sleep(1)
    
    logger.error(f"Không thể thực hiện yêu cầu API sau {max_retries} lần thử")
    return None

def get_all_usdc_pairs(limit=100):
    try:
        url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        data = binance_api_request(url)
        if not data:
            logger.warning("Không lấy được dữ liệu từ Binance, trả về danh sách rỗng")
            return []
        
        usdc_pairs = []
        for symbol_info in data.get('symbols', []):
            symbol = symbol_info.get('symbol', '')
            if symbol.endswith('USDC') and symbol_info.get('status') == 'TRADING':
                usdc_pairs.append(symbol)
        
        logger.info(f"✅ Lấy được {len(usdc_pairs)} coin USDC từ Binance")
        return usdc_pairs[:limit] if limit else usdc_pairs
        
    except Exception as e:
        logger.error(f"❌ Lỗi lấy danh sách coin từ Binance: {str(e)}")
        return []

def get_top_volume_symbols(limit=100):
    """Top {limit} USDC pairs theo quoteVolume của NẾN 1M đã đóng (đa luồng)."""
    try:
        universe = get_all_usdc_pairs(limit=100) or []
        if not universe:
            logger.warning("❌ Không lấy được danh sách coin USDC")
            return []

        scored, failed = [], 0
        max_workers = 8
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futmap = {ex.submit(_last_closed_1m_quote_volume, s): s for s in universe}
            for fut in as_completed(futmap):
                sym = futmap[fut]
                try:
                    qv = fut.result()
                    if qv is not None:
                        scored.append((sym, qv))
                except Exception:
                    failed += 1
                time.sleep(0.5)

        scored.sort(key=lambda x: x[1], reverse=True)
        top_syms = [s for s, _ in scored[:limit]]
        logger.info(f"✅ Top {len(top_syms)} theo 1m quoteVolume (phân tích: {len(scored)}, lỗi: {failed})")
        return top_syms

    except Exception as e:
        logger.error(f"❌ Lỗi lấy top volume 1 phút (đa luồng): {str(e)}")
        return []

def get_max_leverage(symbol, api_key, api_secret):
    """Lấy đòn bẩy tối đa cho một symbol"""
    try:
        url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        data = binance_api_request(url)
        if not data:
            return 100
        
        for s in data['symbols']:
            if s['symbol'] == symbol.upper():
                for f in s['filters']:
                    if f['filterType'] == 'LEVERAGE':
                        if 'maxLeverage' in f:
                            return int(f['maxLeverage'])
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

# ========== SMART COIN FINDER VỚI HỆ THỐNG RSI + KHỐI LƯỢNG MỚI ==========
class SmartCoinFinder:
    def __init__(self, api_key, api_secret):
        self.api_key = api_key
        self.api_secret = api_secret
        
    def get_symbol_leverage(self, symbol):
        """Lấy đòn bẩy tối đa của symbol"""
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
        """Phân tích tín hiệu RSI và khối lượng với các điều kiện mới"""
        try:
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
            
            # 🔴 TÍCH HỢP CÁC ĐIỀU KIỆN RSI MỚI
            
            # Điều kiện 1: RSI > 80 và giá tăng, khối lượng tăng -> BÁN
            if rsi_current > 80 and price_increasing and volume_increasing:
                logger.info(f"🎯 {symbol} - Tín hiệu BÁN: RSI > 80, giá tăng, volume tăng")
                return "SELL"
            
            # Điều kiện 2: RSI < 20 và giá giảm, khối lượng giảm -> BÁN
            if rsi_current < 20 and price_decreasing and volume_decreasing:
                logger.info(f"🎯 {symbol} - Tín hiệu BÁN: RSI < 20, giá giảm, volume giảm")
                return "SELL"
            
            # Điều kiện 3: RSI > 80 và giá tăng, khối lượng giảm -> MUA
            if rsi_current > 80 and price_increasing and volume_decreasing:
                logger.info(f"🎯 {symbol} - Tín hiệu MUA: RSI > 80, giá tăng, volume giảm")
                return "BUY"
            
            # Điều kiện 4: RSI < 20 và giá giảm, khối lượng tăng -> MUA
            if rsi_current < 20 and price_decreasing and volume_increasing:
                logger.info(f"🎯 {symbol} - Tín hiệu MUA: RSI < 20, giá giảm, volume tăng")
                return "BUY"
            
            # Điều kiện 5: RSI > 20 và giá không giảm, khối lượng giảm -> MUA
            if rsi_current > 20 and price_not_decreasing and volume_decreasing:
                logger.info(f"🎯 {symbol} - Tín hiệu MUA: RSI > 20, giá không giảm, volume giảm")
                return "BUY"
            
            # Điều kiện 6: RSI < 80 và không tăng giá, khối lượng tăng -> BÁN
            if rsi_current < 80 and price_not_increasing and volume_increasing:
                logger.info(f"🎯 {symbol} - Tín hiệu BÁN: RSI < 80, giá không tăng, volume tăng")
                return "SELL"
            
            # LOGIC CŨ DỰ PHÒNG
            # TH1: RSI ở vùng cực (>80 hoặc <20) và đang hồi về trung tâm
            rsi_prev = self.calculate_rsi(closes[:-1])  # RSI nến trước
            if (rsi_prev > 80 and rsi_current < rsi_prev and volume_decreasing):
                logger.info(f"🎯 {symbol} - Tín hiệu BÁN: RSI từ vùng quá mua hồi về")
                return "SELL"
            elif (rsi_prev < 20 and rsi_current > rsi_prev and volume_decreasing):
                logger.info(f"🎯 {symbol} - Tín hiệu MUA: RSI từ vùng quá bán hồi về")
                return "BUY"
            
            # TH2: RSI trong vùng 30-70 và khối lượng tăng
            elif (30 <= rsi_current <= 70 and volume_increasing):
                if rsi_current > 55:
                    logger.info(f"🎯 {symbol} - Tín hiệu MUA: RSI trong vùng 55-70, volume tăng")
                    return "BUY"
                elif rsi_current < 45:
                    logger.info(f"🎯 {symbol} - Tín hiệu BÁN: RSI trong vùng 30-45, volume tăng")
                    return "SELL"
            
            return None
            
        except Exception as e:
            logger.error(f"Lỗi phân tích RSI {symbol}: {str(e)}")
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
    
    def find_best_coin(self, target_direction, excluded_coins=None, required_leverage=10):
        """Tìm coin tốt nhất - MỖI COIN ĐỘC LẬP"""
        try:
            all_symbols = get_all_usdc_pairs(limit=50)
            if not all_symbols:
                return None
            
            valid_symbols = []
            
            for symbol in all_symbols:
                # Kiểm tra coin đã bị loại trừ
                if excluded_coins and symbol in excluded_coins:
                    continue
                
                # 🔴 QUAN TRỌNG: Kiểm tra coin đã có vị thế trên Binance
                if self.has_existing_position(symbol):
                    logger.info(f"🚫 Bỏ qua {symbol} - đã có vị thế trên Binance")
                    continue
                
                # Kiểm tra đòn bẩy
                max_lev = self.get_symbol_leverage(symbol)
                if max_lev < required_leverage:
                    continue
                
                # 🔴 SỬ DỤNG TÍN HIỆU VÀO LỆNH (20% khối lượng)
                entry_signal = self.get_entry_signal(symbol)
                if entry_signal == target_direction:
                    valid_symbols.append(symbol)
                    logger.info(f"✅ Tìm thấy coin phù hợp: {symbol} - Tín hiệu: {entry_signal}")
                else:
                    logger.info(f"🔄 Bỏ qua {symbol} - Tín hiệu: {entry_signal} (không trùng với {target_direction})")
            
            if not valid_symbols:
                logger.info(f"❌ Không tìm thấy coin nào có tín hiệu trùng với {target_direction}")
                return None
            
            # Chọn ngẫu nhiên từ danh sách hợp lệ
            selected_symbol = random.choice(valid_symbols)
            max_lev = self.get_symbol_leverage(selected_symbol)
            
            # 🔴 KIỂM TRA LẦN CUỐI: Đảm bảo coin được chọn không có vị thế
            if self.has_existing_position(selected_symbol):
                logger.info(f"🚫 {selected_symbol} - Coin được chọn đã có vị thế, bỏ qua")
                return None
            
            logger.info(f"✅ Đã chọn coin: {selected_symbol} - Tín hiệu: {target_direction} - Đòn bẩy: {max_lev}x")
            return selected_symbol
            
        except Exception as e:
            logger.error(f"❌ Lỗi tìm coin: {str(e)}")
            return None

# ========== WEBSOCKET MANAGER ==========
class WebSocketManager:
    def __init__(self):
        self.connections = {}
        self.executor = ThreadPoolExecutor(max_workers=10)
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        
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
        stream = f"{symbol.lower()}@trade"
        url = f"wss://fstream.binance.com/ws/{stream}"
        
        def on_message(ws, message):
            try:
                data = json.loads(message)
                if 'p' in data:
                    price = float(data['p'])
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

# ========== BASE BOT VỚI HỆ THỐNG RSI + KHỐI LƯỢNG MỚI ==========
class BaseBot:
    def __init__(self, symbol, lev, percent, tp, sl, roi_trigger, ws_manager, api_key, api_secret,
                 telegram_bot_token, telegram_chat_id, strategy_name, config_key=None, bot_id=None,
                 coin_manager=None, symbol_locks=None, max_coins=1):

        # Cho phép bot quản lý nhiều coin
        self.max_coins = max_coins
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
        self.trade_cooldown = 3  # Chờ 3s sau mỗi lệnh

        # Quản lý thời gian
        self.last_global_position_check = 0
        self.last_error_log_time = 0
        self.global_position_check_interval = 10

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

        # Khởi tạo symbol đầu tiên nếu có
        if symbol and not self.coin_finder.has_existing_position(symbol):
            self._add_symbol(symbol)
        
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

        roi_info = f" | 🎯 ROI Trigger: {roi_trigger}%" if roi_trigger else " | 🎯 ROI Trigger: Tắt"
        self.log(f"🟢 Bot {strategy_name} khởi động | {max_coins} coin | ĐB: {lev}x | Vốn: {percent}% | TP/SL: {tp}%/{sl}%{roi_info}")

    def _run(self):
        """Vòng lặp chính - XỬ LÝ TUẦN TỰ CÁC COIN"""
        while not self._stop:
            try:
                current_time = time.time()
                
                # KIỂM TRA VỊ THẾ TOÀN TÀI KHOẢN ĐỊNH KỲ
                if current_time - self.last_global_position_check > self.global_position_check_interval:
                    self.check_global_positions()
                    self.last_global_position_check = current_time
                
                # NẾU BOT KHÔNG CÓ COIN NÀO - TÌM COIN MỚI
                if len(self.active_symbols) < self.max_coins:
                    if self._find_and_add_new_coin():
                        self.last_trade_completion_time = current_time
                        time.sleep(3)
                    else:
                        time.sleep(5)
                    continue
                
                # XỬ LÝ TUẦN TỰ TỪNG COIN - MỖI COIN CÁCH NHAU 3s
                for symbol in self.active_symbols[:]:
                    if self._stop:
                        break
                        
                    self.current_processing_symbol = symbol
                    
                    # Xử lý coin này
                    trade_executed = self._process_single_symbol(symbol)
                    
                    # CHỜ 3s TRƯỚC KHI XỬ LÝ COIN TIẾP THEO
                    time.sleep(3)
                    
                    self.current_processing_symbol = None
                
                # Nghỉ ngơi ngắn trước khi bắt đầu vòng lặp mới
                time.sleep(1)
                
            except Exception as e:
                if time.time() - self.last_error_log_time > 10:
                    self.log(f"❌ Lỗi hệ thống: {str(e)}")
                    self.last_error_log_time = time.time()
                time.sleep(1)

    def _process_single_symbol(self, symbol):
        """Xử lý một symbol duy nhất - HỆ THỐNG RSI + KHỐI LƯỢNG MỚI"""
        try:
            symbol_info = self.symbol_data[symbol]
            current_time = time.time()
            
            # Kiểm tra vị thế định kỳ
            if current_time - symbol_info.get('last_position_check', 0) > 30:
                self._check_symbol_position(symbol)
                symbol_info['last_position_check'] = current_time
            
            # 🔴 KIỂM TRA BỔ SUNG: Đảm bảo coin không có vị thế trên Binance
            if self.coin_finder.has_existing_position(symbol) and not symbol_info['position_open']:
                self.log(f"⚠️ {symbol} - PHÁT HIỆN CÓ VỊ THẾ TRÊN BINANCE, DỪNG THEO DÕI VÀ TÌM COIN KHÁC")
                self.stop_symbol(symbol)
                return False
            
            # Xử lý theo trạng thái
            if symbol_info['position_open']:
                # 🔴 KIỂM TRA ĐÓNG LỆNH THÔNG MINH (ROI + TÍN HIỆU 40%)
                if self._check_smart_exit_condition(symbol):
                    return True
                
                # Kiểm tra TP/SL truyền thống
                self._check_symbol_tp_sl(symbol)
                
                # Kiểm tra nhồi lệnh
                self._check_symbol_averaging_down(symbol)
            else:
                # Tìm cơ hội vào lệnh - CHỈ KHI ĐỦ THỜI GIAN CHỜ
                if (current_time - symbol_info['last_trade_time'] > 60 and 
                    current_time - symbol_info['last_close_time'] > 3600):
                    
                    target_side = self.get_next_side_based_on_comprehensive_analysis()
                    
                    # 🔴 SỬ DỤNG TÍN HIỆU VÀO LỆNH MỚI (20% khối lượng)
                    entry_signal = self.coin_finder.get_entry_signal(symbol)
                    
                    if entry_signal == target_side:
                        # 🔴 KIỂM TRA CUỐI CÙNG TRƯỚC KHI VÀO LỆNH
                        if self.coin_finder.has_existing_position(symbol):
                            self.log(f"🚫 {symbol} - ĐÃ CÓ VỊ THẾ TRÊN BINANCE, BỎ QUA VÀ TÌM COIN KHÁC")
                            self.stop_symbol(symbol)
                            return False
                        
                        if self._open_symbol_position(symbol, target_side):
                            symbol_info['last_trade_time'] = current_time
                            return True
            
            return False
            
        except Exception as e:
            self.log(f"❌ Lỗi xử lý {symbol}: {str(e)}")
            return False

    def _check_smart_exit_condition(self, symbol):
        """Kiểm tra điều kiện đóng lệnh thông minh - GIỐNG HỆT ĐIỀU KIỆN VÀO LỆNH"""
        try:
            if not self.symbol_data[symbol]['position_open']:
                return False
            
            # Chỉ kiểm tra nếu đã kích hoạt ROI trigger
            if not self.symbol_data[symbol]['roi_check_activated']:
                return False
            
            current_price = get_current_price(symbol)
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
                # 🔴 SỬ DỤNG TÍN HIỆU ĐÓNG LỆNH (40% khối lượng) - GIỐNG HỆT ĐIỀU KIỆN VÀO LỆNH
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
        """Tìm và thêm coin mới vào quản lý"""
        try:
            active_coins = self.coin_manager.get_active_coins()
            target_direction = self.get_next_side_based_on_comprehensive_analysis()
            
            new_symbol = self.coin_finder.find_best_coin(
                target_direction=target_direction,
                excluded_coins=active_coins,
                required_leverage=self.lev
            )
            
            if new_symbol:
                # 🔴 KIỂM TRA BỔ SUNG: Đảm bảo coin mới không có vị thế trên Binance
                if self.coin_finder.has_existing_position(new_symbol):
                    return False
                    
                success = self._add_symbol(new_symbol)
                if success:
                    self.log(f"✅ Đã thêm coin: {new_symbol}")
                    
                    # 🔴 KIỂM TRA NGAY LẬP TỨC: Đảm bảo coin mới thêm không có vị thế
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
        """Thêm một symbol vào quản lý của bot - KIỂM TRA VỊ THẾ KHI THÊM"""
        if symbol in self.active_symbols:
            return False
            
        if len(self.active_symbols) >= self.max_coins:
            return False
        
        # 🔴 KIỂM TRA QUAN TRỌNG: Đảm bảo coin không có vị thế trên Binance trước khi thêm
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
        
        # Kiểm tra vị thế hiện tại
        self._check_symbol_position(symbol)
        
        # 🔴 KIỂM TRA LẦN CUỐI: Nếu phát hiện có vị thế, dừng ngay
        if self.symbol_data[symbol]['position_open']:
            self.stop_symbol(symbol)
            return False
        
        return True

    def _handle_price_update(self, price, symbol):
        """Xử lý cập nhật giá cho từng symbol"""
        if symbol in self.symbol_data:
            self.symbol_data[symbol]['current_price'] = price

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
                        
                        # Kích hoạt ROI check nếu đang có lợi nhuận
                        current_price = get_current_price(symbol)
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
        """Mở vị thế cho một symbol cụ thể - KIỂM TRA VỊ THẾ TRƯỚC KHI VÀO LỆNH"""
        try:
            # 🔴 KIỂM TRA QUAN TRỌNG: Đảm bảo coin không có vị thế trên Binance trước khi vào lệnh
            if self.coin_finder.has_existing_position(symbol):
                self.log(f"⚠️ {symbol} - ĐÃ CÓ VỊ THẾ TRÊN BINANCE, BỎ QUA VÀ TÌM COIN KHÁC")
                self.stop_symbol(symbol)
                return False

            # Kiểm tra lại trạng thái trong bot trước khi đặt lệnh
            self._check_symbol_position(symbol)
            if self.symbol_data[symbol]['position_open']:
                return False

            # Kiểm tra đòn bẩy
            current_leverage = self.coin_finder.get_symbol_leverage(symbol)
            if current_leverage < self.lev:
                self.log(f"❌ {symbol} - Đòn bẩy không đủ: {current_leverage}x < {self.lev}x")
                self.stop_symbol(symbol)
                return False

            if not set_leverage(symbol, self.lev, self.api_key, self.api_secret):
                self.log(f"❌ {symbol} - Không thể đặt đòn bẩy")
                self.stop_symbol(symbol)
                return False

            # Số dư
            balance = get_balance(self.api_key, self.api_secret)
            if balance is None or balance <= 0:
                self.log(f"❌ {symbol} - Không đủ số dư")
                return False

            # Giá & step size
            current_price = get_current_price(symbol)
            if current_price <= 0:
                self.log(f"❌ {symbol} - Lỗi lấy giá")
                self.stop_symbol(symbol)
                return False

            step_size = get_step_size(symbol, self.api_key, self.api_secret)

            # Tính khối lượng
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
            time.sleep(0.2)

            result = place_order(symbol, side, qty, self.api_key, self.api_secret)
            if result and 'orderId' in result:
                executed_qty = float(result.get('executedQty', 0))
                avg_price = float(result.get('avgPrice', current_price))

                if executed_qty >= 0:
                    # 🔴 KIỂM TRA LẦN CUỐI: Đảm bảo vị thế thực sự được mở
                    time.sleep(1)
                    self._check_symbol_position(symbol)
                    
                    if not self.symbol_data[symbol]['position_open']:
                        self.log(f"❌ {symbol} - Lệnh đã khớp nhưng không tạo được vị thế, có thể bị hủy")
                        self.stop_symbol(symbol)
                        return False
                    
                    # Cập nhật thông tin vị thế
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
                
                # 🔴 KIỂM TRA: Nếu lỗi do đã có vị thế, dừng theo dõi coin này
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
            time.sleep(0.5)
            
            result = place_order(symbol, close_side, close_qty, self.api_key, self.api_secret)
            if result and 'orderId' in result:
                current_price = get_current_price(symbol)
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

        current_price = get_current_price(symbol)
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

        # CẬP NHẬT ROI CAO NHẤT
        if roi > self.symbol_data[symbol]['high_water_mark_roi']:
            self.symbol_data[symbol]['high_water_mark_roi'] = roi

        # KIỂM TRA ĐIỀU KIỆN ROI TRIGGER
        if (self.roi_trigger is not None and 
            self.symbol_data[symbol]['high_water_mark_roi'] >= self.roi_trigger and 
            not self.symbol_data[symbol]['roi_check_activated']):
            self.symbol_data[symbol]['roi_check_activated'] = True

        # TP/SL TRUYỀN THỐNG
        if self.tp is not None and roi >= self.tp:
            self._close_symbol_position(symbol, f"✅ Đạt TP {self.tp}% (ROI: {roi:.2f}%)")
        elif self.sl is not None and self.sl > 0 and roi <= -self.sl:
            self._close_symbol_position(symbol, f"❌ Đạt SL {self.sl}% (ROI: {roi:.2f}%)")

    def _check_symbol_averaging_down(self, symbol):
        """Kiểm tra nhồi lệnh cho một symbol cụ thể - SỬA LỖI NHỒI LỆNH"""
        if (not self.symbol_data[symbol]['position_open'] or 
            not self.symbol_data[symbol]['entry_base'] or 
            self.symbol_data[symbol]['average_down_count'] >= 7):
            return
            
        try:
            current_time = time.time()
            if current_time - self.symbol_data[symbol]['last_average_down_time'] < 60:
                return
                
            current_price = get_current_price(symbol)
            if current_price <= 0:
                return
                
            # Tính ROI ÂM hiện tại (lỗ) - SỬA LỖI TÍNH TOÁN
            if self.symbol_data[symbol]['side'] == "BUY":
                profit = (current_price - self.symbol_data[symbol]['entry_base']) * abs(self.symbol_data[symbol]['qty'])
            else:
                profit = (self.symbol_data[symbol]['entry_base'] - current_price) * abs(self.symbol_data[symbol]['qty'])
                
            invested = self.symbol_data[symbol]['entry_base'] * abs(self.symbol_data[symbol]['qty']) / self.lev
            if invested <= 0:
                return
                
            current_roi = (profit / invested) * 100
            
            # Chỉ xét khi ROI ÂM (đang lỗ) - SỬA ĐIỀU KIỆN
            if current_roi >= -5:  # Chỉ nhồi khi lỗ từ 5% trở lên
                return
                
            # Chuyển ROI âm thành số dương để so sánh
            roi_negative = abs(current_roi)
            
            # Các mốc Fibonacci - GIẢM NGƯỠNG ĐỂ DỄ KÍCH HOẠT HƠN
            fib_levels = [200, 300, 500, 800, 1300, 2100, 3400]
            
            if self.symbol_data[symbol]['average_down_count'] < len(fib_levels):
                current_fib_level = fib_levels[self.symbol_data[symbol]['average_down_count']]
                
                # SỬA: So sánh ROI âm với ngưỡng Fibonacci
                if roi_negative >= current_fib_level:
                    if self._execute_symbol_average_down(symbol):
                        self.symbol_data[symbol]['last_average_down_time'] = current_time
                        self.symbol_data[symbol]['average_down_count'] += 1
                        self.log(f"📈 {symbol} - Đã nhồi lệnh Fibonacci ở mốc {current_fib_level}% lỗ (ROI hiện tại: {current_roi:.2f}%)")
                        
        except Exception as e:
            self.log(f"❌ {symbol} - Lỗi kiểm tra nhồi lệnh: {str(e)}")

    def _execute_symbol_average_down(self, symbol):
        """Thực hiện nhồi lệnh cho một symbol cụ thể"""
        try:
            balance = get_balance(self.api_key, self.api_secret)
            if balance is None or balance <= 0:
                return False
                
            current_price = get_current_price(symbol)
            if current_price <= 0:
                return False
                
            # Khối lượng nhồi = % số dư * (số lần nhồi + 1) * hệ số giảm
            additional_percent = self.percent * (self.symbol_data[symbol]['average_down_count'] + 1) * 0.5
            usd_amount = balance * (additional_percent / 100)
            qty = (usd_amount * self.lev) / current_price
            
            step_size = get_step_size(symbol, self.api_key, self.api_secret)
            if step_size > 0:
                qty = math.floor(qty / step_size) * step_size
                qty = round(qty, 8)
            
            if qty < step_size:
                return False
                
            # Đặt lệnh cùng hướng với vị thế hiện tại
            result = place_order(symbol, self.symbol_data[symbol]['side'], qty, self.api_key, self.api_secret)
            
            if result and 'orderId' in result:
                executed_qty = float(result.get('executedQty', 0))
                avg_price = float(result.get('avgPrice', current_price))
                
                if executed_qty >= 0:
                    # Cập nhật giá trung bình và khối lượng
                    total_qty = abs(self.symbol_data[symbol]['qty']) + executed_qty
                    new_entry = (abs(self.symbol_data[symbol]['qty']) * self.symbol_data[symbol]['entry'] + executed_qty * avg_price) / total_qty
                    self.symbol_data[symbol]['entry'] = new_entry
                    self.symbol_data[symbol]['qty'] = total_qty if self.symbol_data[symbol]['side'] == "BUY" else -total_qty
                    
                    message = (
                        f"📈 <b>ĐÃ NHỒI LỆNH {symbol}</b>\n"
                        f"🔢 Lần nhồi: {self.symbol_data[symbol]['average_down_count'] + 1}\n"
                        f"📊 Khối lượng thêm: {executed_qty:.4f}\n"
                        f"🏷️ Giá nhồi: {avg_price:.4f}\n"
                        f"📈 Giá trung bình mới: {new_entry:.4f}\n"
                        f"💰 Tổng khối lượng: {total_qty:.4f}"
                    )
                    self.log(message)
                    return True
                    
            return False
            
        except Exception as e:
            self.log(f"❌ {symbol} - Lỗi nhồi lệnh: {str(e)}")
            return False

    def stop_symbol(self, symbol):
        """Dừng một symbol cụ thể (đóng vị thế và ngừng theo dõi)"""
        if symbol not in self.active_symbols:
            return False
        
        self.log(f"⛔ Đang dừng coin {symbol}...")
        
        # Nếu đang xử lý coin này, đợi nó xong
        if self.current_processing_symbol == symbol:
            timeout = time.time() + 10
            while self.current_processing_symbol == symbol and time.time() < timeout:
                time.sleep(0.5)
        
        # Đóng vị thế nếu đang mở
        if self.symbol_data[symbol]['position_open']:
            self._close_symbol_position(symbol, "Dừng coin theo lệnh")
        
        # Dọn dẹp
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
        """Dừng toàn bộ bot (đóng tất cả vị thế)"""
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
        """Xác định hướng lệnh tiếp theo dựa trên PHÂN TÍCH PnL TOÀN TÀI KHOẢN"""
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
        """Chỉ log các thông tin quan trọng - ĐÃ SỬA LỖI TELEGRAM"""
        important_keywords = ['❌', '✅', '⛔', '💰', '📈', '📊', '🎯', '🛡️', '🔴', '🟢', '⚠️', '🚫']
        if any(keyword in message for keyword in important_keywords):
            logger.warning(f"[SYSTEM] {message}")
            
            # CHỈ GỬI TELEGRAM NẾU CONFIG ĐÚNG
            if self.telegram_bot_token and self.telegram_chat_id:
                # THÊM XỬ LÝ TRÁNH GỬI QUÁ NHIỀU
                try:
                    send_telegram(f"<b>SYSTEM</b>: {message}", 
                                 bot_token=self.telegram_bot_token, 
                                 default_chat_id=self.telegram_chat_id)
                except Exception as e:
                    logger.error(f"❌ Không thể gửi Telegram: {str(e)}")


# ========== BOT GLOBAL MARKET VỚI HỆ THỐNG RSI + KHỐI LƯỢNG ==========
class GlobalMarketBot(BaseBot):
    def __init__(self, symbol, lev, percent, tp, sl, roi_trigger, ws_manager,
                 api_key, api_secret, telegram_bot_token, telegram_chat_id, bot_id=None, **kwargs):
        super().__init__(symbol, lev, percent, tp, sl, roi_trigger, ws_manager,
                         api_key, api_secret, telegram_bot_token, telegram_chat_id,
                         "Hệ-thống-RSI-Khối-lượng", bot_id=bot_id, **kwargs)

# ========== KHỞI TẠO GLOBAL INSTANCES ==========
coin_manager = CoinManager()

# ========== BOT MANAGER HOÀN CHỈNH VỚI HỆ THỐNG RSI + KHỐI LƯỢNG ==========
class BotManager:
    def __init__(self, api_key=None, api_secret=None, telegram_bot_token=None, telegram_chat_id=None):
        self.ws_manager = WebSocketManager()
        self.bots = {}
        self.running = True
        self.start_time = time.time()
        self.user_states = {}

        self.api_key = api_key
        self.api_secret = api_secret
        
        # 🔴 SỬA QUAN TRỌNG: KIỂM TRA VÀ CHUẨN HÓA TELEGRAM CONFIG
        self.telegram_bot_token = None
        self.telegram_chat_id = None
        self.telegram_enabled = False
        
        if telegram_bot_token and telegram_chat_id:
            # 🔴 SỬA: IN RA ĐỂ DEBUG
            logger.info(f"🔧 Nhận Telegram config:")
            logger.info(f"   Token: {telegram_bot_token[:15]}...")
            logger.info(f"   Chat ID: {telegram_chat_id}")
            
            validated_token, validated_chat_id, message = validate_telegram_config(
                telegram_bot_token, telegram_chat_id
            )
            
            if validated_token and validated_chat_id:
                self.telegram_bot_token = validated_token
                self.telegram_chat_id = validated_chat_id
                self.telegram_enabled = True
                logger.info(f"🟢 {message}")
                
                # Test kết nối Telegram
                test_msg = "🤖 <b>KIỂM TRA KẾT NỐI TELEGRAM</b>\n\n" \
                          "✅ Cấu hình Telegram: Hợp lệ\n" \
                          "🔄 Đang kiểm tra kết nối..."
                
                if send_telegram(test_msg, 
                               bot_token=self.telegram_bot_token, 
                               default_chat_id=self.telegram_chat_id):
                    logger.info("🔔 Telegram: Kết nối thành công!")
                else:
                    logger.warning("⚠️ Telegram: Có vấn đề với kết nối")
            else:
                logger.error(f"🔴 {message}")
        else:
            logger.warning("🟡 Cảnh báo: Chưa cấu hình Telegram. Bot sẽ chạy không có thông báo.")

        # Các phần còn lại giữ nguyên...
        self.coin_manager = CoinManager()
        self.symbol_locks = defaultdict(threading.Lock)
        self.bot_execution_order = []
        self.bot_execution_lock = threading.Lock()
        self.last_bot_execution_time = 0
        self.bot_execution_cooldown = 3

        if api_key and api_secret:
            self._verify_api_connection()
            
            if self.telegram_enabled:
                self.telegram_thread = threading.Thread(target=self._telegram_listener, daemon=True)
                self.telegram_thread.start()
                
                # Gửi thông báo khởi động thành công
                success_msg = (
                    "🎉 <b>BOT ĐÃ KHỞI ĐỘNG THÀNH CÔNG</b>\n\n"
                    "✅ Kết nối Binance: Hoạt động\n"
                    "✅ Kết nối Telegram: Hoạt động\n"
                    "🤖 Sẵn sàng giao dịch!"
                )
                self.send_telegram_message(success_msg)
                
                # Gửi menu chính
                self.send_main_menu(self.telegram_chat_id)
            else:
                logger.info("🟢 HỆ THỐNG BOT ĐÃ KHỞI ĐỘNG (không có Telegram)")
        else:
            logger.info("⚡ BotManager khởi động ở chế độ không config")

    def send_telegram_message(self, message, chat_id=None, reply_markup=None):
        """Gửi tin nhắn Telegram an toàn"""
        if not self.telegram_enabled:
            return False
            
        return send_telegram(
            message, 
            chat_id=chat_id,
            reply_markup=reply_markup,
            bot_token=self.telegram_bot_token, 
            default_chat_id=self.telegram_chat_id
        )

    def _execute_bots_sequentially(self):
        """Điều phối các bot thực hiện TUẦN TỰ"""
        with self.bot_execution_lock:
            current_time = time.time()
            
            # Kiểm tra cooldown
            if current_time - self.last_bot_execution_time < self.bot_execution_cooldown:
                return
            
            # Cập nhật danh sách bot cần thực thi
            active_bots = [bot_id for bot_id, bot in self.bots.items() 
                          if not bot._stop and len(bot.active_symbols) < bot.max_coins]
            
            if not active_bots:
                return
            
            # Chọn bot đầu tiên trong danh sách
            bot_id_to_execute = active_bots[0]
            bot = self.bots.get(bot_id_to_execute)
            
            if bot and not bot._stop:
                # Bot này đang có ít coin hơn max_coins - cho phép tìm coin mới
                if bot._find_and_add_new_coin():
                    self.last_bot_execution_time = current_time
                    self.log(f"🔁 Đã kích hoạt bot {bot_id_to_execute} tìm coin mới")
            
            # Xoay vòng danh sách để bot tiếp theo được ưu tiên trong lần sau
            if active_bots:
                active_bots.append(active_bots.pop(0))

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
        """Lấy thống kê tổng quan"""
        try:
            all_positions = get_positions(api_key=self.api_key, api_secret=self.api_secret)
            
            total_long_count = 0
            total_short_count = 0
            total_long_pnl = 0
            total_short_pnl = 0
            total_unrealized_pnl = 0
            binance_positions = []
            
            # Tính toán toàn diện từ Binance
            for pos in all_positions:
                position_amt = float(pos.get('positionAmt', 0))
                if position_amt != 0:
                    symbol = pos.get('symbol', 'UNKNOWN')
                    entry_price = float(pos.get('entryPrice', 0))
                    unrealized_pnl = float(pos.get('unRealizedProfit', 0))
                    leverage = float(pos.get('leverage', 1))
                    position_value = abs(position_amt) * entry_price / leverage
                    
                    total_unrealized_pnl += unrealized_pnl
                    
                    if position_amt > 0:
                        total_long_count += 1
                        total_long_pnl += unrealized_pnl
                        binance_positions.append({
                            'symbol': symbol,
                            'side': 'LONG',
                            'leverage': leverage,
                            'size': abs(position_amt),
                            'entry': entry_price,
                            'value': position_value,
                            'pnl': unrealized_pnl
                        })
                    else:
                        total_short_count += 1
                        total_short_pnl += unrealized_pnl
                        binance_positions.append({
                            'symbol': symbol, 
                            'side': 'SHORT',
                            'leverage': leverage,
                            'size': abs(position_amt),
                            'entry': entry_price,
                            'value': position_value,
                            'pnl': unrealized_pnl
                        })
        
            # Thống kê bot
            bot_details = []
            total_bots_with_coins = 0
            trading_bots = 0
            total_coins = 0
            
            for bot_id, bot in self.bots.items():
                coin_count = len(bot.active_symbols) if hasattr(bot, 'active_symbols') else 0
                is_trading = False
                
                if coin_count > 0 and hasattr(bot, 'symbol_data'):
                    for symbol, data in bot.symbol_data.items():
                        if data.get('position_open', False):
                            is_trading = True
                            break
                
                if coin_count > 0:
                    total_bots_with_coins += 1
                    total_coins += coin_count
                if is_trading:
                    trading_bots += 1
                
                bot_info = {
                    'bot_id': bot_id,
                    'coin_count': coin_count,
                    'max_coins': bot.max_coins,
                    'is_trading': is_trading,
                    'symbols': bot.active_symbols if hasattr(bot, 'active_symbols') else [],
                    'symbol_data': bot.symbol_data if hasattr(bot, 'symbol_data') else {},
                    'status': bot.status,
                    'leverage': bot.lev,
                    'percent': bot.percent
                }
                bot_details.append(bot_info)
            
            # Tạo báo cáo
            summary = "📊 **THỐNG KÊ CHI TIẾT - MỖI BOT NHIỀU COIN**\n\n"
            
            # Phần 1: Số dư
            balance = get_balance(self.api_key, self.api_secret)
            if balance is not None:
                summary += f"💰 **SỐ DƯ**: {balance:.2f} USDC\n"
                summary += f"📈 **Tổng PnL**: {total_unrealized_pnl:.2f} USDC\n\n"
            else:
                summary += f"💰 **SỐ DƯ**: ❌ Lỗi kết nối\n\n"
            
            # Phần 2: Bot hệ thống
            summary += f"🤖 **BOT HỆ THỐNG**: {len(self.bots)} bot | {total_bots_with_coins} bot có coin | {trading_bots} bot đang trade | {total_coins} coin\n\n"
            
            # Phần 3: Phân tích toàn diện
            summary += f"📈 **PHÂN TÍCH PnL VÀ KHỐI LƯỢNG**:\n"
            summary += f"   📊 Số lượng: LONG={total_long_count} | SHORT={total_short_count}\n"
            summary += f"   💰 PnL: LONG={total_long_pnl:.2f} USDC | SHORT={total_short_pnl:.2f} USDC\n"
            summary += f"   ⚖️ Chênh lệch: {abs(total_long_pnl - total_short_pnl):.2f} USDC\n\n"
            
            # Phần 4: Chi tiết từng bot
            if bot_details:
                summary += "📋 **CHI TIẾT TỪNG BOT**:\n"
                for bot in bot_details:
                    status_emoji = "🟢" if bot['is_trading'] else "🟡" if bot['coin_count'] > 0 else "🔴"
                    summary += f"{status_emoji} **{bot['bot_id']}**\n"
                    summary += f"   💰 ĐB: {bot['leverage']}x | Vốn: {bot['percent']}% | Coin: {bot['coin_count']}/{bot['max_coins']}\n"
                    
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
            
            summary += "🔄 **CƠ CHẾ NỐI TIẾP**:\n"
            summary += "• Mỗi bot có thể quản lý nhiều coin\n"
            summary += "• Các coin trong bot xử lý tuần tự\n"
            summary += "• Chờ 3s giữa các coin\n"
            summary += "• Tự động điều phối thứ tự thực thi\n\n"
            
            summary += "⛔ **LỆNH DỪNG**:\n"
            summary += "• Chọn '⛔ Dừng Coin' để dừng từng coin\n"
            summary += "• '⛔ Dừng Bot' - Dừng toàn bộ bot\n"
            summary += "• 'DỪNG TẤT CẢ' - Dừng toàn bộ hệ thống\n"
            
            return summary
                    
        except Exception as e:
            return f"❌ Lỗi thống kê: {str(e)}"

    def log(self, message):
        """Chỉ log các thông tin quan trọng - ĐÃ SỬA LỖI TELEGRAM"""
        important_keywords = ['❌', '✅', '⛔', '💰', '📈', '📊', '🎯', '🛡️', '🔴', '🟢', '⚠️', '🚫']
        if any(keyword in message for keyword in important_keywords):
            logger.warning(f"[SYSTEM] {message}")
            
            # CHỈ GỬI TELEGRAM NẾU CONFIG ĐÚNG
            if self.telegram_bot_token and self.telegram_chat_id:
                # THÊM XỬ LÝ TRÁNH GỬI QUÁ NHIỀU
                try:
                    send_telegram(f"<b>SYSTEM</b>: {message}", 
                                 bot_token=self.telegram_bot_token, 
                                 default_chat_id=self.telegram_chat_id)
                except Exception as e:
                    logger.error(f"❌ Không thể gửi Telegram: {str(e)}")


    def send_main_menu(self, chat_id):
        welcome = (
            "🤖 <b>BOT GIAO DỊCH FUTURES - MỖI BOT NHIỀU COIN</b>\n\n"
            "🎯 <b>MÔ HÌNH MỚI - TUẦN TỰ HOÀN TOÀN:</b>\n"
            "• Mỗi bot có thể quản lý nhiều coin\n"
            "• Các coin trong bot xử lý tuần tự\n"
            "• Chờ 3s giữa các coin\n"
            "• Tự động điều phối thứ tự thực thi\n\n"
            
            "📈 <b>ĐIỀU KIỆN VÀO LỆNH RSI NÂNG CAO:</b>\n"
            "1. RSI > 80 + giá tăng + volume tăng → BÁN\n"
            "2. RSI < 20 + giá giảm + volume giảm → BÁN\n"  
            "3. RSI > 80 + giá tăng + volume giảm → MUA\n"
            "4. RSI < 20 + giá giảm + volume tăng → MUA\n"
            "5. RSI > 20 + giá không giảm + volume giảm → MUA\n"
            "6. RSI < 80 + giá không tăng + volume tăng → BÁN\n\n"
            
            "🎯 <b>ĐIỀU KIỆN ĐÓNG LỆNH:</b>\n"
            "• GIỐNG HỆT điều kiện vào lệnh\n"
            "• Nhưng khối lượng thay đổi 40% (thay vì 20%)\n"
            "• VÀ phải đạt ROI trigger do người dùng thiết lập\n\n"
            
            "🔄 <b>CƠ CHẾ ĐIỀU PHỐI:</b>\n"
            "• Bot Manager điều phối thứ tự thực thi\n"
            "• Chỉ 1 bot được tìm coin tại 1 thời điểm\n"
            "• Tự động cân bằng tải giữa các bot"
        )
        send_telegram(welcome, chat_id, create_main_menu(),
                     bot_token=self.telegram_bot_token, 
                     default_chat_id=self.telegram_chat_id)

    def add_bot(self, symbol, lev, percent, tp, sl, roi_trigger, strategy_type, bot_count=1, max_coins=1, **kwargs):
        if sl == 0:
            sl = None
            
        if not self.api_key or not self.api_secret:
            self.log("❌ Chưa thiết lập API Key trong BotManager")
            return False
        
        # Kiểm tra kết nối trước khi tạo bot
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
                    bot_id=bot_id,
                    max_coins=max_coins  # Số coin tối đa mỗi bot
                )
                
                bot._bot_manager = self
                self.bots[bot_id] = bot
                created_count += 1
                
                # Thêm bot vào danh sách thực thi
                with self.bot_execution_lock:
                    if bot_id not in self.bot_execution_order:
                        self.bot_execution_order.append(bot_id)
                
        except Exception as e:
            self.log(f"❌ Lỗi tạo bot: {str(e)}")
            return False
        
        if created_count > 0:
            roi_info = f" | 🎯 ROI Trigger: {roi_trigger}%" if roi_trigger else " | 🎯 ROI Trigger: Tắt"
            
            success_msg = (
                f"✅ <b>ĐÃ TẠO {created_count} BOT HỆ THỐNG RSI + KHỐI LƯỢNG</b>\n\n"
                f"🎯 Chiến lược: {strategy_type}\n"
                f"💰 Đòn bẩy: {lev}x\n"
                f"📈 % Số dư: {percent}%\n"
                f"🎯 TP: {tp}%\n"
                f"🛡️ SL: {sl if sl is not None else 'Tắt'}%{roi_info}\n"
                f"🔧 Chế độ: {bot_mode}\n"
                f"🔢 Số bot: {created_count}\n"
                f"🔗 Coin/bot: {max_coins}\n"
            )
            
            if bot_mode == 'static' and symbol:
                success_msg += f"🔗 Coin khởi tạo: {symbol}\n"
            else:
                success_msg += f"🔗 Coin: Tự động tìm kiếm\n"
            
            success_msg += f"\n🔄 <b>CƠ CHẾ NỐI TIẾP ĐÃ KÍCH HOẠT</b>\n"
            success_msg += f"• Mỗi bot quản lý tối đa {max_coins} coin\n"
            success_msg += f"• Các coin xử lý tuần tự trong bot\n"
            success_msg += f"• Chờ 3s giữa các coin\n"
            success_msg += f"• Tự động điều phối thứ tự thực thi\n\n"
            success_msg += f"🎯 <b>ĐIỀU KIỆN RSI NÂNG CAO ĐÃ KÍCH HOẠT</b>\n"
            success_msg += f"• 6 điều kiện RSI + giá + volume\n"
            success_msg += f"• Tín hiệu vào lệnh: 20% volume thay đổi\n"
            success_msg += f"• Tín hiệu đóng lệnh: 40% volume thay đổi + ROI trigger\n\n"
            success_msg += f"🚫 <b>KIỂM TRA VỊ THẾ ĐÃ KÍCH HOẠT</b>\n"
            success_msg += f"• Tự động phát hiện coin có vị thế\n"
            success_msg += f"• Không vào lệnh trên coin đã có vị thế\n"
            success_msg += f"• Tự động chuyển sang tìm coin khác"
            
            self.log(success_msg)
            return True
        else:
            self.log("❌ Không thể tạo bot")
            return False

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
        """Dừng tất cả coin trong tất cả bot nhưng vẫn giữ bot manager chạy"""
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
        """Dừng toàn bộ bot (đóng tất cả vị thế và xóa bot)"""
        bot = self.bots.get(bot_id)
        if bot:
            bot.stop()
            del self.bots[bot_id]
            self.log(f"🔴 Đã dừng bot {bot_id}")
            return True
        return False

    def stop_all(self):
        """Dừng tất cả bot (đóng tất cả vị thế và xóa tất cả bot)"""
        self.log("🔴 Đang dừng tất cả bot...")
        for bot_id in list(self.bots.keys()):
            self.stop_bot(bot_id)
        self.log("🔴 Đã dừng tất cả bot, hệ thống vẫn chạy và có thể thêm bot mới")

    def _telegram_listener(self):
        """Listener Telegram - ĐÃ SỬA LỖI KẾT NỐI"""
        last_update_id = 0
        retry_count = 0
        max_retries = 5
        
        while self.running and self.telegram_bot_token:
            try:
                # 🔴 THÊM: Điều phối bot thực thi tuần tự
                self._execute_bots_sequentially()
                
                # KIỂM TRA BOT TOKEN
                if not self.telegram_bot_token.startswith('bot'):
                    if ':' in self.telegram_bot_token:
                        self.telegram_bot_token = f"bot{self.telegram_bot_token}"
                    else:
                        logger.error("❌ Bot Token không hợp lệ, không thể kết nối Telegram")
                        time.sleep(60)
                        continue
                
                url = f"https://api.telegram.org/{self.telegram_bot_token}/getUpdates"
                params = {
                    'offset': last_update_id + 1,
                    'timeout': 10,
                    'limit': 100
                }
                
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                }
                
                response = requests.get(url, params=params, headers=headers, timeout=15)
                
                if response.status_code == 200:
                    data = response.json()
                    retry_count = 0  # Reset retry count khi thành công
                    
                    if data.get('ok'):
                        for update in data['result']:
                            update_id = update['update_id']
                            message = update.get('message', {})
                            chat_id = str(message.get('chat', {}).get('id'))
                            text = message.get('text', '').strip()
                            
                            # KIỂM TRA CHAT ID HỢP LỆ
                            if chat_id != self.telegram_chat_id:
                                logger.warning(f"⚠️ Tin nhắn từ chat ID không xác định: {chat_id}")
                                continue
                            
                            if update_id > last_update_id:
                                last_update_id = update_id
                            
                            self._handle_telegram_message(chat_id, text)
                    else:
                        error_description = data.get('description', 'Unknown error')
                        logger.error(f"❌ Telegram API error: {error_description}")
                        
                        if "invalid token" in error_description.lower():
                            logger.error("🔴 Bot Token không hợp lệ! Vui lòng kiểm tra lại.")
                            time.sleep(60)
                        elif "chat not found" in error_description.lower():
                            logger.error("🔴 Chat ID không hợp lệ! Vui lòng kiểm tra lại.")
                            time.sleep(60)
                        
                elif response.status_code == 404:
                    retry_count += 1
                    logger.error(f"❌ Lỗi 404 Telegram (lần {retry_count}/{max_retries})")
                    
                    if retry_count >= max_retries:
                        logger.error("🔴 Đã vượt quá số lần thử kết nối Telegram. Dừng listener...")
                        break
                        
                    time.sleep(10)
                else:
                    logger.error(f"❌ Lỗi HTTP {response.status_code} từ Telegram")
                    time.sleep(5)
                    
            except requests.exceptions.Timeout:
                logger.warning("⏰ Timeout kết nối Telegram, thử lại...")
                time.sleep(5)
            except requests.exceptions.ConnectionError:
                logger.warning("🔗 Mất kết nối mạng, thử lại sau 10s...")
                time.sleep(10)
            except Exception as e:
                logger.error(f"❌ Lỗi không xác định trong Telegram listener: {str(e)}")
                time.sleep(10)

    def _handle_telegram_message(self, chat_id, text):
        user_state = self.user_states.get(chat_id, {})
        current_step = user_state.get('step')
        
        # Xử lý các bước tạo bot
        if current_step == 'waiting_bot_count':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id, create_main_menu(),
                            self.telegram_bot_token, self.telegram_chat_id)
            else:
                try:
                    bot_count = int(text)
                    if bot_count <= 0 or bot_count > 10:
                        send_telegram("⚠️ Số lượng bot phải từ 1 đến 10. Vui lòng chọn lại:",
                                    chat_id, create_bot_count_keyboard(),
                                    self.telegram_bot_token, self.telegram_chat_id)
                        return
    
                    user_state['bot_count'] = bot_count
                    user_state['step'] = 'waiting_coin_count'
                    
                    send_telegram(
                        f"🤖 Số lượng bot: {bot_count}\n\n"
                        f"Chọn số coin tối đa cho mỗi bot:",
                        chat_id,
                        create_coin_count_keyboard(),
                        self.telegram_bot_token, self.telegram_chat_id
                    )
                except ValueError:
                    send_telegram("⚠️ Vui lòng nhập số hợp lệ cho số lượng bot:",
                                chat_id, create_bot_count_keyboard(),
                                self.telegram_bot_token, self.telegram_chat_id)
    
        elif current_step == 'waiting_coin_count':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id, create_main_menu(),
                            self.telegram_bot_token, self.telegram_chat_id)
            else:
                try:
                    coin_count = int(text)
                    if coin_count <= 0 or coin_count > 10:
                        send_telegram("⚠️ Số coin phải từ 1 đến 10. Vui lòng chọn lại:",
                                    chat_id, create_coin_count_keyboard(),
                                    self.telegram_bot_token, self.telegram_chat_id)
                        return
    
                    user_state['max_coins'] = coin_count
                    user_state['step'] = 'waiting_bot_mode'
                    
                    send_telegram(
                        f"🔗 Số coin/bot: {coin_count}\n\n"
                        f"Chọn chế độ bot:",
                        chat_id,
                        create_bot_mode_keyboard(),
                        self.telegram_bot_token, self.telegram_chat_id
                    )
                except ValueError:
                    send_telegram("⚠️ Vui lòng nhập số hợp lệ cho số coin:",
                                chat_id, create_coin_count_keyboard(),
                                self.telegram_bot_token, self.telegram_chat_id)
    
        elif current_step == 'waiting_bot_mode':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id, create_main_menu(),
                            self.telegram_bot_token, self.telegram_chat_id)
            elif text in ["🤖 Bot Tĩnh - Coin cụ thể", "🔄 Bot Động - Tự tìm coin"]:
                if text == "🤖 Bot Tĩnh - Coin cụ thể":
                    user_state['bot_mode'] = 'static'
                    user_state['step'] = 'waiting_symbol'
                    send_telegram(
                        "🎯 <b>ĐÃ CHỌN: BOT TĨNH</b>\n\n"
                        "🤖 Bot sẽ giao dịch coin CỐ ĐỊNH\n"
                        "📊 Bạn cần chọn coin cụ thể\n\n"
                        "Chọn coin:",
                        chat_id,
                        create_symbols_keyboard(),
                        self.telegram_bot_token, self.telegram_chat_id
                    )
                else:
                    user_state['bot_mode'] = 'dynamic'
                    user_state['step'] = 'waiting_leverage'
                    send_telegram(
                        "🎯 <b>ĐÃ CHỌN: BOT ĐỘNG</b>\n\n"
                        f"🤖 Hệ thống sẽ tạo {user_state.get('bot_count', 1)} bot\n"
                        f"🔗 Mỗi bot quản lý tối đa {user_state.get('max_coins', 1)} coin\n"
                        f"🔄 Bot sẽ xử lý từng coin một theo thứ tự\n\n"
                        "Chọn đòn bẩy:",
                        chat_id,
                        create_leverage_keyboard(),
                        self.telegram_bot_token, self.telegram_chat_id
                    )
    
        elif current_step == 'waiting_symbol':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id, create_main_menu(),
                            self.telegram_bot_token, self.telegram_chat_id)
            else:
                user_state['symbol'] = text
                user_state['step'] = 'waiting_leverage'
                send_telegram(
                    f"🔗 Coin: {text}\n\n"
                    f"Chọn đòn bẩy:",
                    chat_id,
                    create_leverage_keyboard(),
                    self.telegram_bot_token, self.telegram_chat_id
                )
    
        elif current_step == 'waiting_leverage':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id, create_main_menu(),
                            self.telegram_bot_token, self.telegram_chat_id)
            else:
                if text.endswith('x'):
                    lev_text = text[:-1]
                else:
                    lev_text = text
    
                try:
                    leverage = int(lev_text)
                    if leverage <= 0 or leverage > 100:
                        send_telegram("⚠️ Đòn bẩy phải từ 1 đến 100. Vui lòng chọn lại:",
                                    chat_id, create_leverage_keyboard(),
                                    self.telegram_bot_token, self.telegram_chat_id)
                        return
    
                    user_state['leverage'] = leverage
                    user_state['step'] = 'waiting_percent'
                    
                    balance = get_balance(self.api_key, self.api_secret)
                    balance_info = f"\n💰 Số dư hiện có: {balance:.2f} USDT" if balance else ""
                    
                    send_telegram(
                        f"💰 Đòn bẩy: {leverage}x{balance_info}\n\n"
                        f"Chọn % số dư cho mỗi lệnh:",
                        chat_id,
                        create_percent_keyboard(),
                        self.telegram_bot_token, self.telegram_chat_id
                    )
                except ValueError:
                    send_telegram("⚠️ Vui lòng nhập số hợp lệ cho đòn bẩy:",
                                chat_id, create_leverage_keyboard(),
                                self.telegram_bot_token, self.telegram_chat_id)
    
        elif current_step == 'waiting_percent':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id, create_main_menu(),
                            self.telegram_bot_token, self.telegram_chat_id)
            else:
                try:
                    percent = float(text)
                    if percent <= 0 or percent > 100:
                        send_telegram("⚠️ % số dư phải từ 0.1 đến 100. Vui lòng chọn lại:",
                                    chat_id, create_percent_keyboard(),
                                    self.telegram_bot_token, self.telegram_chat_id)
                        return
    
                    user_state['percent'] = percent
                    user_state['step'] = 'waiting_tp'
                    
                    balance = get_balance(self.api_key, self.api_secret)
                    actual_amount = balance * (percent / 100) if balance else 0
                    
                    send_telegram(
                        f"📊 % Số dư: {percent}%\n"
                        f"💵 Số tiền mỗi lệnh: ~{actual_amount:.2f} USDT\n\n"
                        f"Chọn Take Profit (%):",
                        chat_id,
                        create_tp_keyboard(),
                        self.telegram_bot_token, self.telegram_chat_id
                    )
                except ValueError:
                    send_telegram("⚠️ Vui lòng nhập số hợp lệ cho % số dư:",
                                chat_id, create_percent_keyboard(),
                                self.telegram_bot_token, self.telegram_chat_id)
    
        elif current_step == 'waiting_tp':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id, create_main_menu(),
                            self.telegram_bot_token, self.telegram_chat_id)
            else:
                try:
                    tp = float(text)
                    if tp <= 0:
                        send_telegram("⚠️ Take Profit phải lớn hơn 0. Vui lòng chọn lại:",
                                    chat_id, create_tp_keyboard(),
                                    self.telegram_bot_token, self.telegram_chat_id)
                        return
    
                    user_state['tp'] = tp
                    user_state['step'] = 'waiting_sl'
                    
                    send_telegram(
                        f"🎯 Take Profit: {tp}%\n\n"
                        f"Chọn Stop Loss (%):",
                        chat_id,
                        create_sl_keyboard(),
                        self.telegram_bot_token, self.telegram_chat_id
                    )
                except ValueError:
                    send_telegram("⚠️ Vui lòng nhập số hợp lệ cho Take Profit:",
                                chat_id, create_tp_keyboard(),
                                self.telegram_bot_token, self.telegram_chat_id)
    
        elif current_step == 'waiting_sl':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id, create_main_menu(),
                            self.telegram_bot_token, self.telegram_chat_id)
            else:
                try:
                    sl = float(text)
                    if sl < 0:
                        send_telegram("⚠️ Stop Loss phải lớn hơn hoặc bằng 0. Vui lòng chọn lại:",
                                    chat_id, create_sl_keyboard(),
                                    self.telegram_bot_token, self.telegram_chat_id)
                        return
    
                    user_state['sl'] = sl
                    user_state['step'] = 'waiting_roi_trigger'
                    
                    send_telegram(
                        f"🛡️ Stop Loss: {sl}%\n\n"
                        f"🎯 <b>CHỌN NGƯỠNG ROI ĐỂ KÍCH HOẠT CƠ CHẾ CHỐT LỆNH THÔNG MINH</b>\n\n"
                        f"Chọn ngưỡng ROI trigger (%):",
                        chat_id,
                        create_roi_trigger_keyboard(),
                        self.telegram_bot_token, self.telegram_chat_id
                    )
                except ValueError:
                    send_telegram("⚠️ Vui lòng nhập số hợp lệ cho Stop Loss:",
                                chat_id, create_sl_keyboard(),
                                self.telegram_bot_token, self.telegram_chat_id)
    
        elif current_step == 'waiting_roi_trigger':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id, create_main_menu(),
                            self.telegram_bot_token, self.telegram_chat_id)
            elif text == '❌ Tắt tính năng':
                user_state['roi_trigger'] = None
                self._finish_bot_creation(chat_id, user_state)
            else:
                try:
                    roi_trigger = float(text)
                    if roi_trigger <= 0:
                        send_telegram("⚠️ ROI Trigger phải lớn hơn 0. Vui lòng chọn lại:",
                                    chat_id, create_roi_trigger_keyboard(),
                                    self.telegram_bot_token, self.telegram_chat_id)
                        return
    
                    user_state['roi_trigger'] = roi_trigger
                    self._finish_bot_creation(chat_id, user_state)
                    
                except ValueError:
                    send_telegram("⚠️ Vui lòng nhập số hợp lệ cho ROI Trigger:",
                                chat_id, create_roi_trigger_keyboard(),
                                self.telegram_bot_token, self.telegram_chat_id)
    
        # XỬ LÝ LỆNH DỪNG TỪNG COIN
        elif text.startswith("⛔ Coin: "):
            parts = text.replace("⛔ Coin: ", "").split(" | Bot: ")
            if len(parts) == 2:
                symbol = parts[0].strip()
                bot_id = parts[1].strip()
                
                if self.stop_bot_symbol(bot_id, symbol):
                    send_telegram(f"✅ Đã dừng coin {symbol} trong bot {bot_id}", chat_id,
                                self.telegram_bot_token, self.telegram_chat_id)
                else:
                    send_telegram(f"❌ Không thể dừng coin {symbol}", chat_id,
                                self.telegram_bot_token, self.telegram_chat_id)
        
        # XỬ LÝ LỆNH DỪNG TẤT CẢ COIN
        elif text == "⛔ DỪNG TẤT CẢ COIN":
            stopped_count = self.stop_all_coins()
            send_telegram(f"✅ Đã dừng {stopped_count} coin, hệ thống vẫn chạy", chat_id,
                        self.telegram_bot_token, self.telegram_chat_id)
        
        # XỬ LÝ LỆNH DỪNG BOT
        elif text.startswith("⛔ Bot: "):
            bot_id = text.replace("⛔ Bot: ", "").strip()
            if self.stop_bot(bot_id):
                send_telegram(f"✅ Đã dừng bot {bot_id}", chat_id,
                            self.telegram_bot_token, self.telegram_chat_id)
            else:
                send_telegram(f"❌ Không tìm thấy bot {bot_id}", chat_id,
                            self.telegram_bot_token, self.telegram_chat_id)
        
        # XỬ LÝ LỆNH DỪNG TẤT CẢ BOT
        elif text == "⛔ DỪNG TẤT CẢ BOT":
            stopped_count = self.stop_all()
            send_telegram(f"✅ Đã dừng {stopped_count} bot, hệ thống vẫn chạy", chat_id,
                        self.telegram_bot_token, self.telegram_chat_id)
    
        elif text == "➕ Thêm Bot":
            self.user_states[chat_id] = {'step': 'waiting_bot_count'}
            balance = get_balance(self.api_key, self.api_secret)
            if balance is None:
                send_telegram("❌ <b>LỖI KẾT NỐI BINANCE</b>\nVui lòng kiểm tra API Key và kết nối mạng!", chat_id,
                            self.telegram_bot_token, self.telegram_chat_id)
                return
            
            send_telegram(
                f"🎯 <b>CHỌN SỐ LƯỢNG BOT</b>\n\n"
                f"💰 Số dư hiện có: <b>{balance:.2f} USDT</b>\n\n"
                f"Chọn số lượng bot:",
                chat_id,
                create_bot_count_keyboard(),
                self.telegram_bot_token, self.telegram_chat_id
            )
        
        elif text == "📊 Danh sách Bot":
            summary = self.get_position_summary()
            send_telegram(summary, chat_id,
                         self.telegram_bot_token, self.telegram_chat_id)
        
        elif text == "⛔ Dừng Bot":
            if not self.bots:
                send_telegram("🤖 Không có bot nào đang chạy", chat_id,
                            self.telegram_bot_token, self.telegram_chat_id)
            else:
                message = "⛔ <b>CHỌN BOT ĐỂ DỪNG</b>\n\n"
                
                # Hiển thị tất cả bot đang chạy
                bot_keyboard = []
                
                for bot_id, bot in self.bots.items():
                    bot_keyboard.append([{"text": f"⛔ Bot: {bot_id}"}])
                
                # Tạo keyboard
                keyboard = []
                
                if bot_keyboard:
                    keyboard.extend(bot_keyboard)
                    keyboard.append([{"text": "⛔ DỪNG TẤT CẢ BOT"}])
                
                keyboard.append([{"text": "❌ Hủy bỏ"}])
                
                send_telegram(
                    message, 
                    chat_id, 
                    {"keyboard": keyboard, "resize_keyboard": True, "one_time_keyboard": True},
                    self.telegram_bot_token, self.telegram_chat_id
                )
        
        elif text == "📊 Thống kê":
            summary = self.get_position_summary()
            send_telegram(summary, chat_id,
                         self.telegram_bot_token, self.telegram_chat_id)
        
        elif text == "💰 Số dư":
            try:
                balance = get_balance(self.api_key, self.api_secret)
                if balance is None:
                    send_telegram("❌ <b>LỖI KẾT NỐI BINANCE</b>\nVui lòng kiểm tra API Key và kết nối mạng!", chat_id,
                                self.telegram_bot_token, self.telegram_chat_id)
                else:
                    send_telegram(f"💰 <b>SỐ DƯ KHẢ DỤNG</b>: {balance:.2f} USDT", chat_id,
                                self.telegram_bot_token, self.telegram_chat_id)
            except Exception as e:
                send_telegram(f"⚠️ Lỗi lấy số dư: {str(e)}", chat_id,
                            self.telegram_bot_token, self.telegram_chat_id)
        
        elif text == "📈 Vị thế":
            try:
                positions = get_positions(api_key=self.api_key, api_secret=self.api_secret)
                if not positions:
                    send_telegram("📭 Không có vị thế nào đang mở", chat_id,
                                self.telegram_bot_token, self.telegram_chat_id)
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
                
                send_telegram(message, chat_id,
                            self.telegram_bot_token, self.telegram_chat_id)
            except Exception as e:
                send_telegram(f"⚠️ Lỗi lấy vị thế: {str(e)}", chat_id,
                            self.telegram_bot_token, self.telegram_chat_id)
        
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
                
                "🔄 <b>CƠ CHẾ ĐIỀU PHỐI:</b>\n"
                "• Mỗi bot có thể quản lý nhiều coin\n"
                "• Các coin trong bot xử lý tuần tự\n"
                "• Chờ 3s giữa các coin\n"
                "• Tự động điều phối thứ tự thực thi\n\n"
                
                "🚫 <b>KIỂM TRA VỊ THẾ:</b>\n"
                "• Tự động phát hiện coin đã có vị thế\n"
                "• Không vào lệnh trên coin đã có vị thế\n"
                "• Tự động chuyển sang tìm coin khác"
            )
            send_telegram(strategy_info, chat_id,
                        self.telegram_bot_token, self.telegram_chat_id)
        
        elif text == "⚙️ Cấu hình":
            balance = get_balance(self.api_key, self.api_secret)
            api_status = "✅ Đã kết nối" if balance is not None else "❌ Lỗi kết nối"
            
            total_bots_with_coins = 0
            trading_bots = 0
            total_coins = 0
            
            for bot in self.bots.values():
                if hasattr(bot, 'active_symbols'):
                    coin_count = len(bot.active_symbols)
                    if coin_count > 0:
                        total_bots_with_coins += 1
                        total_coins += coin_count
                    for symbol, data in bot.symbol_data.items():
                        if data.get('position_open', False):
                            trading_bots += 1
            
            config_info = (
                "⚙️ <b>CẤU HÌNH HỆ THỐNG RSI + KHỐI LƯỢNG</b>\n\n"
                f"🔑 Binance API: {api_status}\n"
                f"🤖 Tổng số bot: {len(self.bots)}\n"
                f"📊 Bot có coin: {total_bots_with_coins}\n"
                f"🔗 Tổng số coin: {total_coins}\n"
                f"🟢 Bot đang trade: {trading_bots}\n"
                f"🌐 WebSocket: {len(self.ws_manager.connections)} kết nối\n\n"
                f"🔄 <b>CƠ CHẾ NỐI TIẾP ĐANG HOẠT ĐỘNG</b>\n"
                f"🎯 <b>6 ĐIỀU KIỆN RSI ĐANG HOẠT ĐỘNG</b>"
            )
            send_telegram(config_info, chat_id,
                        self.telegram_bot_token, self.telegram_chat_id)
        
        elif text:
            self.send_main_menu(chat_id)

    def _finish_bot_creation(self, chat_id, user_state):
        """Hoàn tất quá trình tạo bot"""
        try:
            # Lấy tất cả thông tin từ user_state
            bot_mode = user_state.get('bot_mode', 'static')
            leverage = user_state.get('leverage')
            percent = user_state.get('percent')
            tp = user_state.get('tp')
            sl = user_state.get('sl')
            roi_trigger = user_state.get('roi_trigger')
            symbol = user_state.get('symbol')
            bot_count = user_state.get('bot_count', 1)
            max_coins = user_state.get('max_coins', 1)
            
            success = self.add_bot(
                symbol=symbol,
                lev=leverage,
                percent=percent,
                tp=tp,
                sl=sl,
                roi_trigger=roi_trigger,
                strategy_type="Hệ-thống-RSI-Khối-lượng",
                bot_mode=bot_mode,
                bot_count=bot_count,
                max_coins=max_coins
            )
            
            if success:
                roi_info = f" | 🎯 ROI Trigger: {roi_trigger}%" if roi_trigger else ""
                
                success_msg = (
                    f"✅ <b>ĐÃ TẠO BOT THÀNH CÔNG</b>\n\n"
                    f"🤖 Chiến lược: Hệ thống RSI + Khối lượng\n"
                    f"🔧 Chế độ: {bot_mode}\n"
                    f"🔢 Số bot: {bot_count}\n"
                    f"🔗 Coin/bot: {max_coins}\n"
                    f"💰 Đòn bẩy: {leverage}x\n"
                    f"📊 % Số dư: {percent}%\n"
                    f"🎯 TP: {tp}%\n"
                    f"🛡️ SL: {sl}%{roi_info}"
                )
                if bot_mode == 'static' and symbol:
                    success_msg += f"\n🔗 Coin: {symbol}"
                
                success_msg += f"\n\n🔄 <b>CƠ CHẾ NỐI TIẾP ĐÃ KÍCH HOẠT</b>\n"
                success_msg += f"• Mỗi bot quản lý tối đa {max_coins} coin\n"
                success_msg += f"• Các coin xử lý tuần tự trong bot\n"
                success_msg += f"• Chờ 3s giữa các coin\n"
                success_msg += f"• Tự động điều phối thứ tự thực thi\n\n"
                success_msg += f"🎯 <b>6 ĐIỀU KIỆN RSI ĐÃ KÍCH HOẠT</b>\n"
                success_msg += f"• Tín hiệu vào lệnh: 20% volume thay đổi\n"
                success_msg += f"• Tín hiệu đóng lệnh: 40% volume thay đổi + ROI trigger\n"
                success_msg += f"• Tự động kiểm tra vị thế trước khi vào lệnh"
                
                send_telegram(success_msg, chat_id, create_main_menu(),
                            self.telegram_bot_token, self.telegram_chat_id)
            else:
                send_telegram("❌ Có lỗi khi tạo bot. Vui lòng thử lại.",
                            chat_id, create_main_menu(),
                            self.telegram_bot_token, self.telegram_chat_id)
            
            self.user_states[chat_id] = {}
            
        except Exception as e:
            send_telegram(f"❌ Lỗi tạo bot: {str(e)}", chat_id, create_main_menu(),
                        self.telegram_bot_token, self.telegram_chat_id)
            self.user_states[chat_id] = {}

    def test_telegram_connection(bot_token, chat_id):
        """Kiểm tra kết nối Telegram"""
        try:
            # Chuẩn hóa token
            if not bot_token.startswith('bot'):
                if ':' in bot_token:
                    bot_token = f"bot{bot_token}"
                else:
                    return False, "Bot Token không đúng định dạng"
            
            # Chuẩn hóa chat_id
            try:
                chat_id = str(int(chat_id))
            except (ValueError, TypeError):
                return False, "Chat ID phải là số"
            
            url = f"https://api.telegram.org/{bot_token}/getMe"
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('ok'):
                    bot_info = data['result']
                    bot_name = bot_info.get('first_name', 'Unknown')
                    return True, f"✅ Kết nối Telegram thành công! Bot: {bot_name}"
                else:
                    return False, "❌ Token không hợp lệ"
            else:
                return False, f"❌ Lỗi {response.status_code}: {response.text}"
                
        except Exception as e:
            return False, f"❌ Lỗi kết nối: {str(e)}"
