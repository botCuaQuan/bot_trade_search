# trading_bot_lib_ep_huong_chung.py (ĐÃ SỬA - CHỈ CÒN 1 CHIẾN LƯỢC CÂN BẰNG)
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
import queue
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
import ssl

# ========== CẤU HÌNH & HẰNG SỐ ==========
_BINANCE_LAST_REQUEST_TIME = 0
_BINANCE_RATE_LOCK = threading.Lock()
_BINANCE_MIN_INTERVAL = 0.1

_USDC_CACHE = {"cặp": [], "cập_nhật_cuối": 0}
_USDC_CACHE_TTL = 30

# BỎ: _LEVERAGE_CACHE
_SYMBOL_BLACKLIST = {'BTCUSDC', 'ETHUSDC'}

# ========== CACHE COIN NÂNG CAO ==========
_USDC_COINS_CACHE = {
    "data": [],  # Danh sách coin với đầy đủ thông tin
    "last_volume_update": 0,  # Thời gian cập nhật volume lần cuối
    "last_price_update": 0,  # Thời gian cập nhật giá lần cuối
}
_VOLUME_CACHE_TTL = 6 * 3600  # 6 giờ
_PRICE_CACHE_TTL = 300  # 5 phút

# ========== CẤU HÌNH CÂN BẰNG LỆNH ==========
_BALANCE_CONFIG = {
    "buy_price_threshold": 1.0,  # Ngưỡng giá mua tối đa: 1 USDC
    "sell_price_threshold": 5.0,  # Ngưỡng giá bán tối thiểu: 5 USDC
    "buy_volume_sort": "asc",  # Sắp xếp khối lượng mua: tăng dần (chọn volume thấp nhất)
    "sell_volume_sort": "desc",  # Sắp xếp khối lượng bán: giảm dần (chọn volume cao nhất)
}

# ========== QUẢN LÝ HƯỚNG TOÀN CỤC ==========
class GlobalSideCoordinator:
    def __init__(self):
        self._lock = threading.Lock()
        self.last_global_check = 0
        self.global_buy_count = 0
        self.global_sell_count = 0
        self.next_global_side = None
        self.check_interval = 30
    
    def update_global_counts(self, api_key, api_secret):
        """Cập nhật số lượng vị thế toàn cục từ Binance"""
        with self._lock:
            current_time = time.time()
            if current_time - self.last_global_check < self.check_interval:
                return self.next_global_side
            
            try:
                positions = get_positions(api_key=api_key, api_secret=api_secret)
                buy_count = 0
                sell_count = 0
                
                for pos in positions:
                    position_amt = float(pos.get('positionAmt', 0))
                    if position_amt > 0:
                        buy_count += 1
                    elif position_amt < 0:
                        sell_count += 1
                
                self.global_buy_count = buy_count
                self.global_sell_count = sell_count
                
                # Quyết định hướng tiếp theo dựa trên số lượng lệnh
                if buy_count > sell_count:
                    self.next_global_side = "SELL"
                elif sell_count > buy_count:
                    self.next_global_side = "BUY"
                else:
                    # Nếu bằng nhau, chọn ngẫu nhiên
                    self.next_global_side = random.choice(["BUY", "SELL"])
                
                self.last_global_check = current_time
                logger.info(f"🌍 Số lượng vị thế toàn cục: BUY={buy_count}, SELL={sell_count} → Ưu tiên: {self.next_global_side}")
                
                return self.next_global_side
                
            except Exception as e:
                logger.error(f"❌ Lỗi cập nhật số lượng toàn cục: {str(e)}")
                self.next_global_side = random.choice(["BUY", "SELL"])
                return self.next_global_side
    
    def get_next_side(self, api_key, api_secret):
        """Lấy hướng tiếp theo dựa trên phân tích toàn cục"""
        return self.update_global_counts(api_key, api_secret)

# ========== HÀM TIỆN ÍCH ==========
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(module)s - %(message)s',
        handlers=[logging.StreamHandler(), logging.FileHandler('bot_errors.log')]
    )
    return logging.getLogger()

logger = setup_logging()

def escape_html(text):
    if not text: return text
    return (text.replace('&', '&amp;').replace('<', '&lt;')
                .replace('>', '&gt;').replace('"', '&quot;'))

def send_telegram(message, chat_id=None, reply_markup=None, bot_token=None, default_chat_id=None):
    if not bot_token or not (chat_id or default_chat_id):
        return
    
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    safe_message = escape_html(message)
    
    payload = {"chat_id": chat_id or default_chat_id, "text": safe_message, "parse_mode": "HTML"}
    if reply_markup: payload["reply_markup"] = json.dumps(reply_markup)
    
    try:
        response = requests.post(url, json=payload, timeout=15)
        if response.status_code != 200:
            logger.error(f"Lỗi Telegram ({response.status_code}): {response.text}")
    except Exception as e:
        logger.error(f"Lỗi kết nối Telegram: {str(e)}")

# ========== HÀM TẠO BÀN PHÍM ==========
def create_main_menu():
    return {
        "keyboard": [
            [{"text": "📊 Danh sách Bot"}, {"text": "📊 Thống kê"}],
            [{"text": "➕ Thêm Bot"}, {"text": "⛔ Dừng Bot"}],
            [{"text": "⛔ Quản lý Coin"}, {"text": "📈 Vị thế"}],
            [{"text": "💰 Số dư"}, {"text": "⚙️ Cấu hình"}],
            [{"text": "🎯 Chiến lược"}, {"text": "⚖️ Cân bằng lệnh"}]
        ],
        "resize_keyboard": True,
        "one_time_keyboard": False
    }

def create_cancel_keyboard():
    return {"keyboard": [[{"text": "❌ Hủy bỏ"}]], "resize_keyboard": True, "one_time_keyboard": True}

def create_bot_count_keyboard():
    return {
        "keyboard": [[{"text": "1"}, {"text": "3"}, {"text": "5"}], [{"text": "10"}, {"text": "20"}], [{"text": "❌ Hủy bỏ"}]],
        "resize_keyboard": True, "one_time_keyboard": True
    }

def create_bot_mode_keyboard():
    return {
        "keyboard": [
            [{"text": "🤖 Bot Tĩnh - Coin cụ thể"}, {"text": "🔄 Bot Động - Tự tìm coin"}],
            [{"text": "❌ Hủy bỏ"}]
        ],
        "resize_keyboard": True, "one_time_keyboard": True
    }

def create_symbols_keyboard():
    try:
        coins = get_usdc_coins_with_info()
        symbols = [coin['symbol'] for coin in coins[:12]]
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
    if row: keyboard.append(row)
    keyboard.append([{"text": "❌ Hủy bỏ"}])
    
    return {"keyboard": keyboard, "resize_keyboard": True, "one_time_keyboard": True}

def create_leverage_keyboard():
    leverages = ["3", "5", "10", "15", "20", "25", "50", "75", "100"]
    keyboard = []
    row = []
    for lev in leverages:
        row.append({"text": f"{lev}x"})
        if len(row) == 3:
            keyboard.append(row)
            row = []
    if row: keyboard.append(row)
    keyboard.append([{"text": "❌ Hủy bỏ"}])
    return {"keyboard": keyboard, "resize_keyboard": True, "one_time_keyboard": True}

def create_percent_keyboard():
    return {
        "keyboard": [
            [{"text": "1"}, {"text": "3"}, {"text": "5"}, {"text": "10"}],
            [{"text": "15"}, {"text": "20"}, {"text": "25"}, {"text": "50"}],
            [{"text": "❌ Hủy bỏ"}]
        ],
        "resize_keyboard": True, "one_time_keyboard": True
    }

def create_tp_keyboard():
    return {
        "keyboard": [
            [{"text": "50"}, {"text": "100"}, {"text": "200"}],
            [{"text": "300"}, {"text": "500"}, {"text": "1000"}],
            [{"text": "❌ Hủy bỏ"}]
        ],
        "resize_keyboard": True, "one_time_keyboard": True
    }

def create_sl_keyboard():
    return {
        "keyboard": [
            [{"text": "0"}, {"text": "50"}, {"text": "100"}],
            [{"text": "150"}, {"text": "200"}, {"text": "500"}],
            [{"text": "❌ Hủy bỏ"}]
        ],
        "resize_keyboard": True, "one_time_keyboard": True
    }

def create_roi_trigger_keyboard():
    return {
        "keyboard": [
            [{"text": "30"}, {"text": "50"}, {"text": "100"}],
            [{"text": "150"}, {"text": "200"}, {"text": "300"}],
            [{"text": "❌ Tắt tính năng"}],
            [{"text": "❌ Hủy bỏ"}]
        ],
        "resize_keyboard": True, "one_time_keyboard": True
    }

def create_pyramiding_n_keyboard():
    return {
        "keyboard": [
            [{"text": "0"}, {"text": "1"}, {"text": "2"}, {"text": "3"}],
            [{"text": "4"}, {"text": "5"}, {"text": "❌ Tắt tính năng"}],
            [{"text": "❌ Hủy bỏ"}]
        ],
        "resize_keyboard": True, "one_time_keyboard": True
    }

def create_pyramiding_x_keyboard():
    return {
        "keyboard": [
            [{"text": "100"}, {"text": "200"}, {"text": "300"}],
            [{"text": "400"}, {"text": "500"}, {"text": "1000"}],
            [{"text": "❌ Hủy bỏ"}]
        ],
        "resize_keyboard": True, "one_time_keyboard": True
    }

def create_balance_config_keyboard():
    return {
        "keyboard": [
            [{"text": "⚖️ Bật cân bằng lệnh"}, {"text": "⚖️ Tắt cân bằng lệnh"}],
            [{"text": "📊 Xem cấu hình cân bằng"}, {"text": "🔄 Làm mới cache"}],
            [{"text": "❌ Hủy bỏ"}]
        ],
        "resize_keyboard": True, "one_time_keyboard": True
    }

def create_price_threshold_keyboard():
    return {
        "keyboard": [
            [{"text": "0.5"}, {"text": "1.0"}, {"text": "2.0"}],
            [{"text": "5.0"}, {"text": "10.0"}, {"text": "20.0"}],
            [{"text": "❌ Hủy bỏ"}]
        ],
        "resize_keyboard": True, "one_time_keyboard": True
    }

def create_volume_sort_keyboard():
    return {
        "keyboard": [
            [{"text": "asc - Tăng dần"}, {"text": "desc - Giảm dần"}],
            [{"text": "❌ Hủy bỏ"}]
        ],
        "resize_keyboard": True, "one_time_keyboard": True
    }

# ========== HÀM API BINANCE ==========
def _wait_for_rate_limit():
    global _BINANCE_LAST_REQUEST_TIME
    with _BINANCE_RATE_LOCK:
        now = time.time()
        delta = now - _BINANCE_LAST_REQUEST_TIME
        if delta < _BINANCE_MIN_INTERVAL:
            time.sleep(_BINANCE_MIN_INTERVAL - delta)
        _BINANCE_LAST_REQUEST_TIME = time.time()

def sign(query, api_secret):
    try:
        return hmac.new(api_secret.encode(), query.encode(), hashlib.sha256).hexdigest()
    except Exception as e:
        logger.error(f"Lỗi ký: {str(e)}")
        return ""

def binance_api_request(url, method='GET', params=None, headers=None):
    max_retries = 2
    base_url = url

    for attempt in range(max_retries):
        try:
            _wait_for_rate_limit()
            url = base_url

            if headers is None: headers = {}
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

            with urllib.request.urlopen(req, timeout=15) as response:
                if response.status == 200:
                    return json.loads(response.read().decode())
                else:
                    error_content = response.read().decode()
                    logger.error(f"Lỗi API ({response.status}): {error_content}")
                    if response.status == 401: return None
                    if response.status == 429:
                        sleep_time = 2 ** attempt
                        logger.warning(f"⚠️ 429 Quá nhiều yêu cầu, đợi {sleep_time}s")
                        time.sleep(sleep_time)
                    elif response.status >= 500: time.sleep(0.5)
                    continue

        except urllib.error.HTTPError as e:
            if e.code == 451:
                logger.error("❌ Lỗi 451: Truy cập bị chặn - Kiểm tra VPN/proxy")
                return None
            else: logger.error(f"Lỗi HTTP ({e.code}): {e.reason}")

            if e.code == 401: return None
            if e.code == 429:
                sleep_time = 2 ** attempt
                logger.warning(f"⚠️ HTTP 429 Quá nhiều yêu cầu, đợi {sleep_time}s")
                time.sleep(sleep_time)
            elif e.code >= 500: time.sleep(0.5)
            continue

        except Exception as e:
            logger.error(f"Lỗi kết nối API (lần thử {attempt + 1}): {str(e)}")
            time.sleep(0.5)

    logger.error(f"Thất bại yêu cầu API sau {max_retries} lần thử")
    return None

def get_all_usdc_pairs(limit=50):
    global _USDC_CACHE
    try:
        now = time.time()
        if _USDC_CACHE["cặp"] and (now - _USDC_CACHE["cập_nhật_cuối"] < _USDC_CACHE_TTL):
            return _USDC_CACHE["cặp"][:limit]

        url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        data = binance_api_request(url)
        if not data: return []

        usdc_pairs = []
        for symbol_info in data.get('symbols', []):
            symbol = symbol_info.get('symbol', '')
            if (symbol.endswith('USDC') and symbol_info.get('status') == 'TRADING' 
                and symbol not in _SYMBOL_BLACKLIST):
                usdc_pairs.append(symbol)

        _USDC_CACHE["cặp"] = usdc_pairs
        _USDC_CACHE["cập_nhật_cuối"] = now
        logger.info(f"✅ Đã lấy {len(usdc_pairs)} cặp USDC (loại trừ BTC/ETH)")
        return usdc_pairs[:limit]

    except Exception as e:
        logger.error(f"❌ Lỗi lấy danh sách coin: {str(e)}")
        return []

# ========== HÀM CACHE COIN NÂNG CAO ==========
def refresh_usdc_coins_cache():
    """Lấy và cập nhật danh sách coin với thông tin đầy đủ từ Binance"""
    global _USDC_COINS_CACHE
    
    try:
        url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        data = binance_api_request(url)
        if not data:
            logger.error("❌ Không thể lấy exchangeInfo từ Binance")
            return False
        
        usdc_coins = []
        
        for symbol_info in data.get('symbols', []):
            symbol = symbol_info.get('symbol', '')
            
            if not symbol.endswith('USDC'):
                continue
            if symbol_info.get('status') != 'TRADING':
                continue
            if symbol in _SYMBOL_BLACKLIST:
                continue
            
            max_leverage = 100
            for f in symbol_info.get('filters', []):
                if f['filterType'] == 'LEVERAGE' and 'maxLeverage' in f:
                    max_leverage = int(f['maxLeverage'])
                    break
            
            step_size = 0.001
            for f in symbol_info.get('filters', []):
                if f['filterType'] == 'LOT_SIZE':
                    step_size = float(f['stepSize'])
                    break
            
            usdc_coins.append({
                'symbol': symbol,
                'max_leverage': max_leverage,
                'step_size': step_size,
                'price': 0.0,
                'volume': 0.0,
                'last_price_update': 0,
                'last_volume_update': 0
            })
        
        _USDC_COINS_CACHE["data"] = usdc_coins
        _USDC_COINS_CACHE["last_volume_update"] = time.time()
        logger.info(f"✅ Đã cập nhật danh sách {len(usdc_coins)} coin USDC với đòn bẩy")
        
        # Log một số coin để debug
        if usdc_coins:
            sample = usdc_coins[:5]
            for coin in sample:
                logger.debug(f"  Coin mẫu: {coin['symbol']} - Leverage: {coin['max_leverage']}x")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Lỗi refresh cache coin: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False

def update_coins_price():
    """Cập nhật giá cho tất cả coin trong cache"""
    global _USDC_COINS_CACHE
    
    try:
        url = "https://fapi.binance.com/fapi/v1/ticker/price"
        all_prices = binance_api_request(url)
        if not all_prices:
            return False
        
        price_dict = {item['symbol']: float(item['price']) for item in all_prices}
        
        updated_count = 0
        for coin in _USDC_COINS_CACHE["data"]:
            symbol = coin['symbol']
            if symbol in price_dict:
                coin['price'] = price_dict[symbol]
                coin['last_price_update'] = time.time()
                updated_count += 1
        
        _USDC_COINS_CACHE["last_price_update"] = time.time()
        if updated_count > 0:
            logger.info(f"✅ Đã cập nhật giá cho {updated_count} coin")
        return True
        
    except Exception as e:
        logger.error(f"❌ Lỗi cập nhật giá: {str(e)}")
        return False

def update_coins_volume():
    """Cập nhật volume cho tất cả coin trong cache"""
    global _USDC_COINS_CACHE
    
    try:
        url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
        all_tickers = binance_api_request(url)
        if not all_tickers:
            return False
        
        volume_dict = {item['symbol']: float(item['volume']) for item in all_tickers 
                      if item['symbol'].endswith('USDC')}
        
        updated_count = 0
        for coin in _USDC_COINS_CACHE["data"]:
            symbol = coin['symbol']
            if symbol in volume_dict:
                coin['volume'] = volume_dict[symbol]
                coin['last_volume_update'] = time.time()
                updated_count += 1
        
        _USDC_COINS_CACHE["last_volume_update"] = time.time()
        if updated_count > 0:
            logger.info(f"✅ Đã cập nhật volume cho {updated_count} coin")
        return True
        
    except Exception as e:
        logger.error(f"❌ Lỗi cập nhật volume: {str(e)}")
        return False

def get_usdc_coins_with_info():
    """Lấy danh sách coin với thông tin đầy đủ (đã cache)"""
    global _USDC_COINS_CACHE
    
    now = time.time()
    
    if (not _USDC_COINS_CACHE["data"] or 
        now - _USDC_COINS_CACHE["last_volume_update"] > _VOLUME_CACHE_TTL):
        logger.info("🔄 Cache đã cũ, đang làm mới danh sách coin...")
        refresh_usdc_coins_cache()
        update_coins_volume()
    
    if now - _USDC_COINS_CACHE["last_price_update"] > _PRICE_CACHE_TTL:
        update_coins_price()
    
    return _USDC_COINS_CACHE["data"]

def get_max_leverage_from_cache(symbol):
    """Lấy đòn bẩy tối đa từ cache (thay thế cho hàm get_max_leverage cũ)"""
    global _USDC_COINS_CACHE
    
    symbol = symbol.upper()
    for coin in _USDC_COINS_CACHE["data"]:
        if coin['symbol'] == symbol:
            return coin['max_leverage']
    
    # Nếu không tìm thấy trong cache, trả về giá trị mặc định an toàn
    logger.warning(f"⚠️ Không tìm thấy {symbol} trong cache, sử dụng đòn bẩy mặc định 100x")
    return 100

# ========== HÀM LỌC COIN CẢI THIỆN ==========
def filter_and_sort_coins_for_side(side, excluded_coins=None, required_leverage=10):
    """
    Lọc và sắp xếp coin theo hướng giao dịch
    - MUA: giá < 1 USDC, volume tăng dần (thấp nhất đầu tiên)
    - BÁN: giá > 5 USDC, volume giảm dần (cao nhất đầu tiên)
    """
    all_coins = get_usdc_coins_with_info()
    filtered_coins = []
    
    # Kiểm tra nếu danh sách coin trống
    if not all_coins:
        logger.warning(f"❌ Danh sách coin trống! Không thể lọc cho hướng {side}")
        return filtered_coins
    
    logger.info(f"🔍 Đang lọc coin cho hướng {side}. Tổng coin có sẵn: {len(all_coins)}")
    logger.info(f"🔧 Cấu hình: MUA < {_BALANCE_CONFIG['buy_price_threshold']}USDC, BÁN > {_BALANCE_CONFIG['sell_price_threshold']}USDC, Leverage tối thiểu: {required_leverage}x")
    
    # Biến đếm để debug
    excluded_count = 0
    leverage_fail_count = 0
    price_fail_count = 0
    blacklist_count = 0
    volume_zero_count = 0
    
    for coin in all_coins:
        symbol = coin['symbol']
        
        # Kiểm tra blacklist
        if symbol in _SYMBOL_BLACKLIST:
            blacklist_count += 1
            continue
            
        if excluded_coins and symbol in excluded_coins:
            excluded_count += 1
            continue
            
        if coin['max_leverage'] < required_leverage:
            leverage_fail_count += 1
            continue
            
        if coin['price'] <= 0:
            price_fail_count += 1
            continue
        
        # Kiểm tra ngưỡng giá
        if side == "BUY":
            if coin['price'] >= _BALANCE_CONFIG["buy_price_threshold"]:
                price_fail_count += 1
                continue
        elif side == "SELL":
            if coin['price'] <= _BALANCE_CONFIG["sell_price_threshold"]:
                price_fail_count += 1
                continue
        
        # Kiểm tra volume tối thiểu
        if coin['volume'] <= 0:
            volume_zero_count += 1
            continue
            
        filtered_coins.append(coin)
    
    # Sắp xếp theo volume
    if side == "BUY" and _BALANCE_CONFIG["buy_volume_sort"] == "asc":
        filtered_coins.sort(key=lambda x: x['volume'])  # Tăng dần: volume thấp nhất đầu tiên
    elif side == "SELL" and _BALANCE_CONFIG["sell_volume_sort"] == "desc":
        filtered_coins.sort(key=lambda x: x['volume'], reverse=True)  # Giảm dần: volume cao nhất đầu tiên
    
    # Log chi tiết về các coin bị loại
    logger.info(f"📊 Thống kê lọc coin cho {side}:")
    logger.info(f"  ✅ Coin phù hợp: {len(filtered_coins)}")
    logger.info(f"  ❌ Bị loại do:")
    logger.info(f"     - Blacklist: {blacklist_count}")
    logger.info(f"     - Đã có trong hệ thống: {excluded_count}")
    logger.info(f"     - Đòn bẩy không đủ: {leverage_fail_count}")
    logger.info(f"     - Ngưỡng giá không phù hợp: {price_fail_count}")
    logger.info(f"     - Volume bằng 0: {volume_zero_count}")
    
    if len(filtered_coins) > 0:
        logger.info(f"✅ Đã lọc được {len(filtered_coins)} coin cho hướng {side}")
        top_coins = filtered_coins[:5]
        for i, coin in enumerate(top_coins):
            logger.info(f"  {i+1}. {coin['symbol']} - Giá: {coin['price']:.4f} USDC, Volume: {coin['volume']:.2f}, Leverage: {coin['max_leverage']}x")
    else:
        logger.warning(f"⚠️ Không tìm thấy coin phù hợp cho hướng {side}")
        logger.warning(f"   Kiểm tra cấu hình:")
        logger.warning(f"   - Ngưỡng giá MUA: < {_BALANCE_CONFIG['buy_price_threshold']} USDC")
        logger.warning(f"   - Ngưỡng giá BÁN: > {_BALANCE_CONFIG['sell_price_threshold']} USDC")
        logger.warning(f"   - Đòn bẩy tối thiểu: {required_leverage}x")
        logger.warning(f"   - Tổng coin có sẵn: {len(all_coins)}")
    
    return filtered_coins

def update_balance_config(buy_price_threshold=None, sell_price_threshold=None,
                         buy_volume_sort=None, sell_volume_sort=None):
    """Cập nhật cấu hình cân bằng lệnh"""
    global _BALANCE_CONFIG
    
    if buy_price_threshold is not None:
        _BALANCE_CONFIG["buy_price_threshold"] = buy_price_threshold
    if sell_price_threshold is not None:
        _BALANCE_CONFIG["sell_price_threshold"] = sell_price_threshold
    if buy_volume_sort is not None:
        _BALANCE_CONFIG["buy_volume_sort"] = buy_volume_sort
    if sell_volume_sort is not None:
        _BALANCE_CONFIG["sell_volume_sort"] = sell_volume_sort
    
    logger.info(f"✅ Đã cập nhật cấu hình cân bằng: {_BALANCE_CONFIG}")
    return _BALANCE_CONFIG

def force_refresh_coin_cache():
    """Buộc làm mới cache coin"""
    logger.info("🔄 Buộc làm mới cache coin...")
    if refresh_usdc_coins_cache():
        update_coins_volume()
        update_coins_price()
        
        cache_info = _USDC_COINS_CACHE
        coins_count = len(cache_info.get("data", []))
        
        logger.info(f"✅ Đã làm mới cache {coins_count} coin")
        return True
    else:
        logger.error("❌ Không thể làm mới cache")
        return False

# ✅ 1️⃣ BỎ HÀM GET_MAX_LEVERAGE() CŨ - ĐÃ XÓA

def get_step_size(symbol, api_key, api_secret):
    if not symbol: return 0.001
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    try:
        data = binance_api_request(url)
        if not data: return 0.001
        for s in data['symbols']:
            if s['symbol'] == symbol.upper():
                for f in s['filters']:
                    if f['filterType'] == 'LOT_SIZE':
                        return float(f['stepSize'])
    except Exception as e:
        logger.error(f"Lỗi step size: {str(e)}")
    return 0.001

def set_leverage(symbol, lev, api_key, api_secret):
    if not symbol: return False
    try:
        ts = int(time.time() * 1000)
        params = {"symbol": symbol.upper(), "leverage": lev, "timestamp": ts}
        query = urllib.parse.urlencode(params)
        sig = sign(query, api_secret)
        url = f"https://fapi.binance.com/fapi/v1/leverage?{query}&signature={sig}"
        headers = {'X-MBX-APIKEY': api_key}
        
        response = binance_api_request(url, method='POST', headers=headers)
        return bool(response and 'leverage' in response)
    except Exception as e:
        logger.error(f"Lỗi cài đặt đòn bẩy: {str(e)}")
        return False

def get_balance(api_key, api_secret):
    try:
        ts = int(time.time() * 1000)
        params = {"timestamp": ts}
        query = urllib.parse.urlencode(params)
        sig = sign(query, api_secret)
        url = f"https://fapi.binance.com/fapi/v2/account?{query}&signature={sig}"
        headers = {'X-MBX-APIKEY': api_key}
        
        data = binance_api_request(url, headers=headers)
        if not data: return None
            
        for asset in data['assets']:
            if asset['asset'] == 'USDC':
                available_balance = float(asset['availableBalance'])
                logger.info(f"💰 Số dư - Khả dụng: {available_balance:.2f} USDC")
                return available_balance
        return 0
    except Exception as e:
        logger.error(f"Lỗi số dư: {str(e)}")
        return None

def get_total_and_available_balance(api_key, api_secret):
    try:
        ts = int(time.time() * 1000)
        params = {"timestamp": ts}
        query = urllib.parse.urlencode(params)
        sig = sign(query, api_secret)
        url = f"https://fapi.binance.com/fapi/v2/account?{query}&signature={sig}"
        headers = {"X-MBX-APIKEY": api_key}

        data = binance_api_request(url, headers=headers)
        if not data:
            logger.error("❌ Không lấy được số dư từ Binance")
            return None, None

        total_all = 0.0
        available_all = 0.0

        for asset in data["assets"]:
            if asset["asset"] in ("USDT", "USDC"):
                available_all += float(asset["availableBalance"])
                total_all += float(asset["walletBalance"])

        logger.info(
            f"💰 Tổng số dư (USDT+USDC): {total_all:.2f}, "
            f"Khả dụng: {available_all:.2f}"
        )
        return total_all, available_all
    except Exception as e:
        logger.error(f"Lỗi lấy tổng số dư: {str(e)}")
        return None, None


def get_margin_safety_info(api_key, api_secret):
    try:
        ts = int(time.time() * 1000)
        params = {"timestamp": ts}
        query = urllib.parse.urlencode(params)
        sig = sign(query, api_secret)
        url = f"https://fapi.binance.com/fapi/v2/account?{query}&signature={sig}"
        headers = {"X-MBX-APIKEY": api_key}

        data = binance_api_request(url, headers=headers)
        if not data:
            logger.error("❌ Không lấy được thông tin ký quỹ từ Binance")
            return None, None, None

        margin_balance = float(data.get("totalMarginBalance", 0.0))
        maint_margin = float(data.get("totalMaintMargin", 0.0))

        if maint_margin <= 0:
            logger.warning(
                f"⚠️ Maint margin <= 0 (margin_balance={margin_balance:.4f}, maint_margin={maint_margin:.4f})"
            )
            return margin_balance, maint_margin, None

        ratio = margin_balance / maint_margin

        logger.info(
            f"🛡️ An toàn ký quỹ: margin_balance={margin_balance:.4f}, "
            f"maint_margin={maint_margin:.4f}, tỷ lệ={ratio:.2f}x"
        )
        return margin_balance, maint_margin, ratio

    except Exception as e:
        logger.error(f"Lỗi lấy thông tin an toàn ký quỹ: {str(e)}")
        return None, None, None

def place_order(symbol, side, qty, api_key, api_secret):
    if not symbol: return None
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
        logger.error(f"Lỗi lệnh: {str(e)}")
        return None

def cancel_all_orders(symbol, api_key, api_secret):
    if not symbol: return False
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
    if not symbol: return 0
    try:
        url = f"https://fapi.binance.com/fapi/v1/ticker/price?symbol={symbol.upper()}"
        data = binance_api_request(url)
        if data and 'price' in data:
            price = float(data['price'])
            return price if price > 0 else 0
        return 0
    except Exception as e:
        logger.error(f"Lỗi giá {symbol}: {str(e)}")
        return 0

def get_positions(symbol=None, api_key=None, api_secret=None):
    try:
        ts = int(time.time() * 1000)
        params = {"timestamp": ts}
        if symbol: params["symbol"] = symbol.upper()
        query = urllib.parse.urlencode(params)
        sig = sign(query, api_secret)
        url = f"https://fapi.binance.com/fapi/v2/positionRisk?{query}&signature={sig}"
        headers = {'X-MBX-APIKEY': api_key}
        
        positions = binance_api_request(url, headers=headers)
        if not positions: return []
        if symbol:
            for pos in positions:
                if pos['symbol'] == symbol.upper():
                    return [pos]
        return positions
    except Exception as e:
        logger.error(f"Lỗi vị thế: {str(e)}")
        return []

# ========== LỚP QUẢN LÝ CỐT LÕI ==========
class CoinManager:
    def __init__(self):
        self.active_coins = set()
        self._lock = threading.Lock()
    
    def register_coin(self, symbol):
        if not symbol: return
        with self._lock: self.active_coins.add(symbol.upper())
    
    def unregister_coin(self, symbol):
        if not symbol: return
        with self._lock: self.active_coins.discard(symbol.upper())
    
    def is_coin_active(self, symbol):
        if not symbol: return False
        with self._lock: return symbol.upper() in self.active_coins
    
    def get_active_coins(self):
        with self._lock: return list(self.active_coins)

class BotExecutionCoordinator:
    def __init__(self):
        self._lock = threading.Lock()
        self._bot_queue = queue.Queue()
        self._current_finding_bot = None
        self._found_coins = set()
        self._bots_with_coins = set()
    
    def request_coin_search(self, bot_id):
        with self._lock:
            if bot_id in self._bots_with_coins:
                return False
                
            if self._current_finding_bot is None or self._current_finding_bot == bot_id:
                self._current_finding_bot = bot_id
                return True
            else:
                if bot_id not in list(self._bot_queue.queue):
                    self._bot_queue.put(bot_id)
                return False
    
    def finish_coin_search(self, bot_id, found_symbol=None, has_coin_now=False):
        with self._lock:
            if self._current_finding_bot == bot_id:
                self._current_finding_bot = None
                if found_symbol: self._found_coins.add(found_symbol)
                if has_coin_now: self._bots_with_coins.add(bot_id)
                
                if not self._bot_queue.empty():
                    next_bot = self._bot_queue.get()
                    self._current_finding_bot = next_bot
                    return next_bot
            return None
    
    def bot_has_coin(self, bot_id):
        with self._lock:
            self._bots_with_coins.add(bot_id)
            new_queue = queue.Queue()
            while not self._bot_queue.empty():
                bot_in_queue = self._bot_queue.get()
                if bot_in_queue != bot_id: new_queue.put(bot_in_queue)
            self._bot_queue = new_queue
    
    def bot_lost_coin(self, bot_id):
        with self._lock:
            if bot_id in self._bots_with_coins:
                self._bots_with_coins.remove(bot_id)
    
    def is_coin_available(self, symbol):
        with self._lock: return symbol not in self._found_coins

    def bot_processing_coin(self, bot_id):
        with self._lock:
            self._bots_with_coins.add(bot_id)
            new_queue = queue.Queue()
            while not self._bot_queue.empty():
                bot_in_queue = self._bot_queue.get()
                if bot_in_queue != bot_id:
                    new_queue.put(bot_in_queue)
            self._bot_queue = new_queue
    
    def get_queue_info(self):
        with self._lock:
            return {
                'current_finding': self._current_finding_bot,
                'queue_size': self._bot_queue.qsize(),
                'queue_bots': list(self._bot_queue.queue),
                'bots_with_coins': list(self._bots_with_coins),
                'found_coins_count': len(self._found_coins)
            }
    
    def get_queue_position(self, bot_id):
        with self._lock:
            if self._current_finding_bot == bot_id: return 0
            else:
                queue_list = list(self._bot_queue.queue)
                return queue_list.index(bot_id) + 1 if bot_id in queue_list else -1

# ========== SMART COIN FINDER CẢI THIỆN ==========
class SmartCoinFinder:
    def __init__(self, api_key, api_secret):
        self.api_key = api_key
        self.api_secret = api_secret
        self.last_scan_time = 0
        self.scan_cooldown = 10
        self.position_counts = {"BUY": 0, "SELL": 0}
        self.last_position_count_update = 0
        self._bot_manager = None
        self.last_failed_search_log = 0
        
    def set_bot_manager(self, bot_manager):
        self._bot_manager = bot_manager
        
    def update_position_counts(self):
        """Cập nhật số lượng lệnh BUY/SELL hiện tại"""
        try:
            positions = get_positions(api_key=self.api_key, api_secret=self.api_secret)
            
            buy_count = 0
            sell_count = 0
            
            for pos in positions:
                position_amt = float(pos.get('positionAmt', 0))
                if position_amt > 0:
                    buy_count += 1
                elif position_amt < 0:
                    sell_count += 1
            
            self.position_counts = {"BUY": buy_count, "SELL": sell_count}
            self.last_position_count_update = time.time()
            
            logger.info(f"📊 Cân bằng lệnh: BUY={buy_count}, SELL={sell_count}")
            
        except Exception as e:
            logger.error(f"❌ Lỗi cập nhật số lượng lệnh: {str(e)}")
    
    def get_next_side_for_balance(self):
        """Xác định hướng tiếp theo dựa trên cân bằng số lượng lệnh"""
        if time.time() - self.last_position_count_update > 30:
            self.update_position_counts()
        
        if self.position_counts["BUY"] > self.position_counts["SELL"]:
            return "SELL"
        elif self.position_counts["SELL"] > self.position_counts["BUY"]:
            return "BUY"
        else:
            return random.choice(["BUY", "SELL"])
    
    def get_symbol_leverage(self, symbol):
        """✅ 2️⃣ CHỈ dùng max_leverage từ _USDC_COINS_CACHE"""
        return get_max_leverage_from_cache(symbol)
    
    def has_existing_position(self, symbol):
        try:
            positions = get_positions(symbol, self.api_key, self.api_secret)
            if positions:
                for pos in positions:
                    if abs(float(pos.get('positionAmt', 0))) > 0:
                        return True
            return False
        except Exception as e:
            logger.error(f"Lỗi kiểm tra vị thế {symbol}: {str(e)}")
            return True

    def find_best_coin_with_balance(self, excluded_coins=None, required_leverage=10):
        """
        Tìm coin tốt nhất với cơ chế cân bằng lệnh
        - Đếm số lệnh BUY/SELL hiện có
        - Nhiều BUY hơn → tìm SELL, nhiều SELL hơn → tìm BUY
        - Lọc coin theo ngưỡng giá và volume
        """
        try:
            now = time.time()
            if now - self.last_scan_time < self.scan_cooldown:
                return None
            
            self.last_scan_time = now
            
            # Kiểm tra cache coin trước
            if not _USDC_COINS_CACHE["data"]:
                logger.warning("🔄 Cache coin trống, đang làm mới...")
                if not refresh_usdc_coins_cache():
                    logger.error("❌ Không thể làm mới cache coin")
                    return None
            
            # Xác định hướng giao dịch dựa trên số lượng lệnh
            if self._bot_manager and hasattr(self._bot_manager, 'global_side_coordinator'):
                target_side = self._bot_manager.global_side_coordinator.get_next_side(
                    self.api_key, self.api_secret
                )
            else:
                # Fallback: tự check
                target_side = self.get_next_side_for_balance()
            
            logger.info(f"🎯 Hệ thống chọn hướng: {target_side} (dựa trên số lượng lệnh)")
            
            # Lấy danh sách coin đã lọc
            filtered_coins = filter_and_sort_coins_for_side(
                target_side, excluded_coins, required_leverage
            )
            
            if not filtered_coins:
                # Chỉ log lỗi mỗi 60 giây để tránh spam
                if now - self.last_failed_search_log > 60:
                    logger.warning(f"⚠️ Không tìm thấy coin phù hợp cho hướng {target_side}")
                    logger.warning(f"   Nguyên nhân có thể do:")
                    logger.warning(f"   1. Ngưỡng giá quá khắt khe (MUA < {_BALANCE_CONFIG['buy_price_threshold']}USDC, BÁN > {_BALANCE_CONFIG['sell_price_threshold']}USDC)")
                    logger.warning(f"   2. Đòn bẩy yêu cầu {required_leverage}x quá cao")
                    logger.warning(f"   3. Tất cả coin đã có vị thế")
                    self.last_failed_search_log = now
                return None
            
            # Ưu tiên coin theo thứ tự đã sắp xếp
            for coin in filtered_coins[:20]:  # Chỉ xem xét top 20
                symbol = coin['symbol']
                
                # Kiểm tra vị thế tồn tại
                if self.has_existing_position(symbol):
                    continue
                
                # Kiểm tra xem coin có đang bị bot khác quản lý không
                if self._bot_manager:
                    if self._bot_manager.coin_manager.is_coin_active(symbol):
                        continue
                
                logger.info(f"✅ Tìm thấy coin {symbol} phù hợp ({target_side})")
                return symbol
            
            logger.warning(f"⚠️ Đã duyệt {len(filtered_coins)} coin nhưng không có coin nào chưa có vị thế")
            return None
            
        except Exception as e:
            logger.error(f"❌ Lỗi tìm coin với cân bằng: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return None

# ========== WEBSOCKET MANAGER ==========
class WebSocketManager:
    def __init__(self):
        self.connections = {}
        self.executor = ThreadPoolExecutor(max_workers=20)
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self.price_cache = {}
        self.last_price_update = {}
        
    def add_symbol(self, symbol, callback):
        if not symbol: return
        symbol = symbol.upper()
        with self._lock:
            if symbol not in self.connections:
                self._create_connection(symbol, callback)
                
    def _create_connection(self, symbol, callback):
        if self._stop_event.is_set(): return
        
        streams = [f"{symbol.lower()}@trade"]
        url = f"wss://fstream.binance.com/stream?streams={'/'.join(streams)}"
        
        def on_message(ws, message):
            try:
                data = json.loads(message)
                if 'data' in data:
                    symbol = data['data']['s']
                    price = float(data['data']['p'])
                    current_time = time.time()
                    
                    if (symbol in self.last_price_update and 
                        current_time - self.last_price_update[symbol] < 0.1):
                        return
                    
                    self.last_price_update[symbol] = current_time
                    self.price_cache[symbol] = price
                    self.executor.submit(callback, price)
            except Exception as e:
                logger.error(f"Lỗi tin nhắn WebSocket {symbol}: {str(e)}")
                
        def on_error(ws, error):
            logger.error(f"Lỗi WebSocket {symbol}: {str(error)}")
            if not self._stop_event.is_set():
                time.sleep(5)
                self._reconnect(symbol, callback)
            
        def on_close(ws, close_status_code, close_msg):
            logger.info(f"WebSocket đã đóng {symbol}: {close_status_code} - {close_msg}")
            if not self._stop_event.is_set() and symbol in self.connections:
                time.sleep(5)
                self._reconnect(symbol, callback)
                
        ws = websocket.WebSocketApp(url, on_message=on_message, on_error=on_error, on_close=on_close)
        thread = threading.Thread(target=ws.run_forever, daemon=True)
        thread.start()
        
        self.connections[symbol] = {'ws': ws, 'thread': thread, 'callback': callback}
        logger.info(f"🔗 WebSocket đã khởi động cho {symbol}")
        
    def _reconnect(self, symbol, callback):
        logger.info(f"Đang kết nối lại WebSocket cho {symbol}")
        self.remove_symbol(symbol)
        self._create_connection(symbol, callback)
        
    def remove_symbol(self, symbol):
        if not symbol: return
        symbol = symbol.upper()
        with self._lock:
            if symbol in self.connections:
                try: self.connections[symbol]['ws'].close()
                except Exception as e: logger.error(f"Lỗi đóng WebSocket {symbol}: {str(e)}")
                del self.connections[symbol]
                logger.info(f"WebSocket đã xóa cho {symbol}")
                
    def stop(self):
        self._stop_event.set()
        for symbol in list(self.connections.keys()):
            self.remove_symbol(symbol)

# ========== LỚP BOT CỐT LÕI ==========
class BaseBot:
    def __init__(self, symbol, lev, percent, tp, sl, roi_trigger, ws_manager, api_key, api_secret,
                 telegram_bot_token, telegram_chat_id, strategy_name, config_key=None, bot_id=None,
                 coin_manager=None, symbol_locks=None, max_coins=1, bot_coordinator=None,
                 pyramiding_n=0, pyramiding_x=0, **kwargs):

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

        self.pyramiding_n = int(pyramiding_n) if pyramiding_n else 0
        self.pyramiding_x = float(pyramiding_x) if pyramiding_x else 0
        self.pyramiding_enabled = self.pyramiding_n > 0 and self.pyramiding_x > 0

        self.status = "searching" if not symbol else "waiting"
        self._stop = False

        self.current_processing_symbol = None
        self.last_trade_completion_time = 0
        self.trade_cooldown = 30

        self.last_global_position_check = 0
        self.last_error_log_time = 0
        self.global_position_check_interval = 30

        self.global_long_count = 0
        self.global_short_count = 0
        self.global_long_pnl = 0
        self.global_short_pnl = 0
        self.global_long_volume = 0.0
        self.global_short_volume = 0.0
        self.next_global_side = None

        self.margin_safety_threshold = 1.05
        self.margin_safety_interval = 10
        self.last_margin_safety_check = 0

        self.coin_manager = coin_manager or CoinManager()
        self.symbol_locks = symbol_locks
        self.coin_finder = SmartCoinFinder(api_key, api_secret)

        self.find_new_bot_after_close = True
        self.bot_creation_time = time.time()

        self.execution_lock = threading.Lock()
        self.last_execution_time = 0
        self.execution_cooldown = 1

        self.bot_coordinator = bot_coordinator or BotExecutionCoordinator()

        # Cấu hình cân bằng lệnh
        self.enable_balance_orders = kwargs.get('enable_balance_orders', True)
        self.balance_config = {
            'buy_price_threshold': kwargs.get('buy_price_threshold', 1.0),
            'sell_price_threshold': kwargs.get('sell_price_threshold', 5.0),
            'buy_volume_sort': kwargs.get('buy_volume_sort', 'asc'),
            'sell_volume_sort': kwargs.get('sell_volume_sort', 'desc'),
        }
        
        global _BALANCE_CONFIG
        _BALANCE_CONFIG.update(self.balance_config)

        if symbol and not self.coin_finder.has_existing_position(symbol):
            self._add_symbol(symbol)
        
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

        self._initialize_coin_cache()
        
        roi_info = f" | 🎯 ROI Kích hoạt: {roi_trigger}%" if roi_trigger else " | 🎯 ROI Kích hoạt: Tắt"
        pyramiding_info = f" | 🔄 Nhồi lệnh: {pyramiding_n} lần tại {pyramiding_x}%" if self.pyramiding_enabled else " | 🔄 Nhồi lệnh: Tắt"
        
        balance_info = (f" | ⚖️ Cân bằng lệnh: BẬT | "
                      f"Mua <{self.balance_config['buy_price_threshold']}USDC | "
                      f"Bán >{self.balance_config['sell_price_threshold']}USDC")
        
        self.log(f"🟢 Bot {strategy_name} đã khởi động | 1 coin | Đòn bẩy: {lev}x | Vốn: {percent}% | TP/SL: {tp}%/{sl}%{roi_info}{pyramiding_info}{balance_info}")

    def _initialize_coin_cache(self):
        """Khởi tạo cache coin trước khi bot bắt đầu chạy"""
        try:
            logger.info("🔄 Đang khởi tạo cache coin...")
            
            if refresh_usdc_coins_cache():
                update_coins_volume()
                update_coins_price()
                
                cache_info = _USDC_COINS_CACHE
                coins_count = len(cache_info.get("data", []))
                
                logger.info(f"✅ Đã khởi tạo cache {coins_count} coin")
            else:
                logger.error("❌ Không thể khởi tạo cache coin")
                
        except Exception as e:
            logger.error(f"❌ Lỗi khởi tạo cache: {str(e)}")

    def _run(self):
        """Vòng lặp chính"""
        if not _USDC_COINS_CACHE["data"]:
            self._initialize_coin_cache()
        
        # Biến để tránh spam log
        last_coin_search_log = 0
        log_interval = 30  # Chỉ log 30 giây một lần
        last_no_coin_found_log = 0
        
        while not self._stop:
            try:
                current_time = time.time()
                
                if current_time - _USDC_COINS_CACHE["last_volume_update"] > 3600:
                    update_coins_volume()
                if current_time - _USDC_COINS_CACHE["last_price_update"] > 300:
                    update_coins_price()

                if current_time - self.last_margin_safety_check > self.margin_safety_interval:
                    self.last_margin_safety_check = current_time
                    if self._check_margin_safety():
                        time.sleep(5)
                        continue
                
                if current_time - self.last_global_position_check > 30:
                    self.check_global_positions()
                    self.last_global_position_check = current_time
                
                if not self.active_symbols:
                    search_permission = self.bot_coordinator.request_coin_search(self.bot_id)
                    
                    if search_permission:
                        if current_time - last_coin_search_log > log_interval:
                            queue_info = self.bot_coordinator.get_queue_info()
                            self.log(f"🔍 Đang tìm coin (vị trí: 1/{queue_info['queue_size'] + 1})...")
                            last_coin_search_log = current_time
                        
                        found_coin = None
                        if self.enable_balance_orders:
                            found_coin = self.coin_finder.find_best_coin_with_balance(
                                excluded_coins=self.coin_manager.get_active_coins(),
                                required_leverage=self.lev
                            )
                        
                        if found_coin:
                            self.bot_coordinator.bot_has_coin(self.bot_id)
                            self.log(f"✅ Đã tìm thấy coin: {found_coin}, đang chờ vào lệnh...")
                            last_coin_search_log = 0  # Reset để log lần tiếp theo
                        else:
                            self.bot_coordinator.finish_coin_search(self.bot_id)
                            # Chỉ log nếu đã qua interval
                            if current_time - last_no_coin_found_log > 60:
                                self.log(f"❌ Không tìm thấy coin phù hợp")
                                last_no_coin_found_log = current_time
                    else:
                        queue_pos = self.bot_coordinator.get_queue_position(self.bot_id)
                        if queue_pos > 0:
                            queue_info = self.bot_coordinator.get_queue_info()
                            current_finder = queue_info['current_finding']
                            if current_time - last_coin_search_log > log_interval:
                                self.log(f"⏳ Đang chờ tìm coin (vị trí: {queue_pos}/{queue_info['queue_size'] + 1}) - Bot đang tìm: {current_finder}")
                                last_coin_search_log = current_time
                        time.sleep(2)
                    
                    # Tăng delay khi không tìm thấy coin
                    time.sleep(5)
                    continue  # Quay lại đầu vòng lặp
                
                for symbol in self.active_symbols.copy():
                    position_opened = self._process_single_symbol(symbol)
                    
                    if position_opened:
                        self.log(f"🎯 Đã vào lệnh thành công {symbol}, chuyển quyền tìm coin...")
                        next_bot = self.bot_coordinator.finish_coin_search(self.bot_id)
                        if next_bot:
                            self.log(f"🔄 Đã chuyển quyền tìm coin cho bot: {next_bot}")
                        break
                
                time.sleep(1)
                
            except Exception as e:
                if time.time() - self.last_error_log_time > 10:
                    self.log(f"❌ Lỗi hệ thống: {str(e)}")
                    self.last_error_log_time = time.time()
                time.sleep(5)

    def _process_single_symbol(self, symbol):
        """Xử lý một symbol duy nhất"""
        try:
            symbol_info = self.symbol_data[symbol]
            current_time = time.time()
            
            if current_time - symbol_info.get('last_position_check', 0) > 30:
                self._check_symbol_position(symbol)
                symbol_info['last_position_check'] = current_time
            
            if symbol_info['position_open']:
                if self._check_smart_exit_condition(symbol):
                    return False
                
                self._check_symbol_tp_sl(symbol)
                
                if self.pyramiding_enabled:
                    self._check_pyramiding(symbol)
                    
                return False
            else:
                if (current_time - symbol_info['last_trade_time'] > 30 and 
                    current_time - symbol_info['last_close_time'] > 30):
                    
                    # Luôn sử dụng cơ chế cân bằng
                    target_side = self.get_next_side_based_on_comprehensive_analysis()
                    logger.info(f"🎯 Hướng giao dịch: {target_side}")
                    
                    if not self.coin_finder.has_existing_position(symbol):
                        if self._open_symbol_position(symbol, target_side):
                            symbol_info['last_trade_time'] = current_time
                            return True
                return False
                
        except Exception as e:
            self.log(f"❌ Lỗi xử lý {symbol}: {str(e)}")
            return False

    def _check_pyramiding(self, symbol):
        try:
            if not self.pyramiding_enabled:
                return False

            info = self.symbol_data.get(symbol)
            if not info or not info.get('position_open', False):
                return False

            current_count = int(info.get('pyramiding_count', 0))
            if current_count >= self.pyramiding_n:
                return False

            current_time = time.time()
            if current_time - info.get('last_pyramiding_time', 0) < 60:
                return False

            current_price = self.get_current_price(symbol)
            if current_price is None or current_price <= 0:
                return False

            entry = float(info.get('entry', 0))
            qty   = abs(float(info.get('qty', 0)))
            if entry <= 0 or qty <= 0:
                return False

            if info.get('side') == "BUY":
                profit = (current_price - entry) * qty
            else:
                profit = (entry - current_price) * qty

            invested = entry * qty / self.lev
            if invested <= 0:
                return False

            roi = (profit / invested) * 100

            if roi >= 0:
                return False

            step = float(self.pyramiding_x or 0)
            if step <= 0:
                return False

            base_roi = float(info.get('pyramiding_base_roi', 0.0))
            target_roi = base_roi - step

            if roi > target_roi:
                return False

            self.log(
                f"📉 {symbol} - ROI hiện tại {roi:.2f}% <= mốc nhồi {target_roi:.2f}% "
                f"(mốc cũ: {base_roi:.2f}%, step: {step}%) → THỬ NHỒI..."
            )

            if self._pyramid_order(symbol):
                new_count = current_count + 1
                info['pyramiding_count'] = new_count
                info['pyramiding_base_roi'] = roi
                info['last_pyramiding_time'] = current_time

                self.log(
                    f"🔄 {symbol} - ĐÃ NHỒI LẦN {new_count}/{self.pyramiding_n} "
                    f"tại ROI {roi:.2f}%. Mốc ROI mới: {roi:.2f}%"
                )
                return True

            return False

        except Exception as e:
            self.log(f"❌ Lỗi kiểm tra nhồi lệnh {symbol}: {str(e)}")
            return False

    def _pyramid_order(self, symbol):
        """Thực hiện lệnh nhồi (thêm lệnh cùng chiều)"""
        try:
            symbol_info = self.symbol_data[symbol]
            if not symbol_info['position_open']:
                return False
            
            side = symbol_info['side']
            
            total_balance, available_balance = get_total_and_available_balance(
                self.api_key, self.api_secret
            )
            if total_balance is None or total_balance <= 0:
                self.log(f"❌ {symbol} - Không đủ tổng số dư để nhồi lệnh")
                return False
    
            balance = total_balance
    
            required_usd = balance * (self.percent / 100)
    
            if available_balance is None or available_balance <= 0 or required_usd > available_balance:
                self.log(
                    f"❌ {symbol} - Không đủ số dư khả dụng để nhồi lệnh:"
                    f" cần {required_usd:.2f}, khả dụng {available_balance or 0:.2f}"
                )
                return False

            current_price = self.get_current_price(symbol)
            if current_price < 0:
                self.log(f"❌ {symbol} - Lỗi giá khi nhồi lệnh")
                return False

            step_size = get_step_size(symbol, self.api_key, self.api_secret)
            usd_amount = balance * (self.percent / 100)
            qty = (usd_amount * self.lev) / current_price
            if step_size > 0:
                qty = math.floor(qty / step_size) * step_size
                qty = round(qty, 8)

            if qty <= 0 or qty < step_size:
                self.log(f"❌ {symbol} - Khối lượng không hợp lệ khi nhồi lệnh")
                return False

            cancel_all_orders(symbol, self.api_key, self.api_secret)
            time.sleep(1)

            result = place_order(symbol, side, qty, self.api_key, self.api_secret)
            if result and 'orderId' in result:
                executed_qty = float(result.get('executedQty', 0))
                avg_price = float(result.get('avgPrice', current_price))

                if executed_qty >= 0:
                    old_qty = symbol_info['qty']
                    old_entry = symbol_info['entry']
                    
                    total_qty = abs(old_qty) + executed_qty
                    if side == "BUY":
                        new_qty = old_qty + executed_qty
                        new_entry = (old_entry * abs(old_qty) + avg_price * executed_qty) / total_qty
                    else:
                        new_qty = old_qty - executed_qty
                        new_entry = (old_entry * abs(old_qty) + avg_price * executed_qty) / total_qty
                    
                    symbol_info['qty'] = new_qty
                    symbol_info['entry'] = new_entry
                    
                    message = (f"🔄 <b>NHỒI LỆNH {symbol}</b>\n"
                              f"🤖 Bot: {self.bot_id}\n📌 Hướng: {side}\n"
                              f"🏷️ Entry: {avg_price:.4f} (Trung bình: {new_entry:.4f})\n"
                              f"📊 Khối lượng: {executed_qty:.4f} (Tổng: {abs(new_qty):.4f})\n"
                              f"💰 Đòn bẩy: {self.lev}x\n🎯 Lần nhồi: {symbol_info.get('pyramiding_count', 0) + 1}/{self.pyramiding_n}")
                    
                    self.log(message)
                    return True
                else:
                    self.log(f"❌ {symbol} - Nhồi lệnh không thành công")
                    return False
            else:
                error_msg = result.get('msg', 'Lỗi không xác định') if result else 'Không có phản hồi'
                self.log(f"❌ {symbol} - Lỗi nhồi lệnh: {error_msg}")
                return False

        except Exception as e:
            self.log(f"❌ {symbol} - Lỗi nhồi lệnh: {str(e)}")
            return False

    def _check_smart_exit_condition(self, symbol):
        try:
            if not self.symbol_data[symbol]['position_open'] or not self.symbol_data[symbol]['roi_check_activated']:
                return False
            
            current_price = self.get_current_price(symbol)
            if current_price <= 0: return False
            
            if self.symbol_data[symbol]['side'] == "BUY":
                profit = (current_price - self.symbol_data[symbol]['entry']) * abs(self.symbol_data[symbol]['qty'])
            else:
                profit = (self.symbol_data[symbol]['entry'] - current_price) * abs(self.symbol_data[symbol]['qty'])
                
            invested = self.symbol_data[symbol]['entry'] * abs(self.symbol_data[symbol]['qty']) / self.lev
            if invested <= 0: return False
                
            current_roi = (profit / invested) * 100
            
            if current_roi >= self.roi_trigger:
                # Chốt lời sớm khi đạt ROI target
                reason = f"🎯 Đạt ROI {self.roi_trigger}% (ROI hiện tại: {current_roi:.2f}%)"
                self._close_symbol_position(symbol, reason)
                return True
            return False
            
        except Exception as e:
            self.log(f"❌ Lỗi kiểm tra thoát thông minh {symbol}: {str(e)}")
            return False

    def _add_symbol(self, symbol):
        if symbol in self.active_symbols or len(self.active_symbols) >= self.max_coins:
            return False
        if self.coin_finder.has_existing_position(symbol): return False
        
        self.symbol_data[symbol] = {
            'status': 'waiting', 'side': '', 'qty': 0, 'entry': 0, 'current_price': 0,
            'position_open': False, 'last_trade_time': 0, 'last_close_time': 0,
            'entry_base': 0, 'average_down_count': 0, 'last_average_down_time': 0,
            'high_water_mark_roi': 0, 'roi_check_activated': False,
            'close_attempted': False, 'last_close_attempt': 0, 'last_position_check': 0,
            'pyramiding_count': 0,
            'next_pyramiding_roi': self.pyramiding_x if self.pyramiding_enabled else 0,
            'last_pyramiding_time': 0,
            'pyramiding_base_roi': 0.0,
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
        if symbol in self.symbol_data:
            self.symbol_data[symbol]['current_price'] = price

    def get_current_price(self, symbol):
        if (symbol in self.ws_manager.price_cache and 
            time.time() - self.ws_manager.last_price_update.get(symbol, 0) < 5):
            return self.ws_manager.price_cache[symbol]
        return get_current_price(symbol)

    def _check_symbol_position(self, symbol):
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
        if symbol in self.symbol_data:
            self.symbol_data[symbol].update({
                'position_open': False, 'status': "waiting", 'side': "", 'qty': 0, 'entry': 0,
                'close_attempted': False, 'last_close_attempt': 0, 'entry_base': 0,
                'average_down_count': 0, 'high_water_mark_roi': 0, 'roi_check_activated': False,
                'pyramiding_count': 0,
                'next_pyramiding_roi': self.pyramiding_x if self.pyramiding_enabled else 0,
                'last_pyramiding_time': 0,
                'pyramiding_base_roi': 0.0,
            })

    def _open_symbol_position(self, symbol, side):
        try:
            if self.coin_finder.has_existing_position(symbol):
                self.log(f"⚠️ {symbol} - CÓ VỊ THẾ TRÊN BINANCE, BỎ QUA")
                self.stop_symbol(symbol)
                return False

            self._check_symbol_position(symbol)
            if self.symbol_data[symbol]['position_open']: return False

            # ✅ 3️⃣ Sửa đúng 1 chỗ: Lấy đòn bẩy từ cache, không gọi API
            # Tìm coin trong cache để lấy max_leverage
            max_leverage_from_cache = None
            for coin in _USDC_COINS_CACHE["data"]:
                if coin['symbol'] == symbol:
                    max_leverage_from_cache = coin['max_leverage']
                    break
            
            if max_leverage_from_cache is None:
                self.log(f"❌ {symbol} - Không tìm thấy trong cache coin")
                self.stop_symbol(symbol)
                return False
            
            # So sánh với đòn bẩy mong muốn
            if max_leverage_from_cache < self.lev:
                self.log(f"❌ {symbol} - Đòn bẩy không đủ: {max_leverage_from_cache}x < {self.lev}x (từ cache)")
                self.stop_symbol(symbol)
                return False

            if not set_leverage(symbol, self.lev, self.api_key, self.api_secret):
                self.log(f"❌ {symbol} - Không thể cài đặt đòn bẩy (Binance từ chối)")
                self.stop_symbol(symbol)
                return False

            total_balance, available_balance = get_total_and_available_balance(
                self.api_key, self.api_secret
            )
            if total_balance is None or total_balance <= 0:
                self.log(f"❌ {symbol} - Không đủ tổng số dư")
                return False
    
            balance = total_balance
    
            required_usd = balance * (self.percent / 100)
    
            if available_balance is None or available_balance <= 0 or required_usd > available_balance:
                self.log(
                    f"❌ {symbol} - Không đủ số dư khả dụng:"
                    f" cần {required_usd:.2f}, khả dụng {available_balance or 0:.2f}"
                )
                return False

            current_price = self.get_current_price(symbol)
            if current_price <= 0:
                self.log(f"❌ {symbol} - Lỗi giá")
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
                        self.log(f"❌ {symbol} - Lệnh đã khớp nhưng không tạo vị thế")
                        self.stop_symbol(symbol)
                        return False
                    
                    pyramiding_info = {}
                    if self.pyramiding_enabled:
                        pyramiding_info = {
                            'pyramiding_count': 0,
                            'next_pyramiding_roi': self.pyramiding_x,
                            'last_pyramiding_time': 0,
                            'pyramiding_base_roi': 0.0,
                        }
                    
                    self.symbol_data[symbol].update({
                        'entry': avg_price, 'entry_base': avg_price, 'average_down_count': 0,
                        'side': side, 'qty': executed_qty if side == "BUY" else -executed_qty,
                        'position_open': True, 'status': "open", 'high_water_mark_roi': 0,
                        'roi_check_activated': False,
                        **pyramiding_info
                    })

                    self.bot_coordinator.bot_has_coin(self.bot_id)

                    message = (f"✅ <b>ĐÃ MỞ VỊ THẾ {symbol}</b>\n"
                              f"🤖 Bot: {self.bot_id}\n📌 Hướng: {side}\n"
                              f"🏷️ Entry: {avg_price:.4f}\n📊 Khối lượng: {executed_qty:.4f}\n"
                              f"💰 Đòn bẩy: {self.lev}x\n🎯 TP: {self.tp}% | 🛡️ SL: {self.sl}%")
                    if self.roi_trigger: message += f" | 🎯 ROI Kích hoạt: {self.roi_trigger}%"
                    if self.pyramiding_enabled: message += f" | 🔄 Nhồi lệnh: {self.pyramiding_n} lần tại {self.pyramiding_x}%"
                    
                    self.log(message)
                    return True
                else:
                    self.log(f"❌ {symbol} - Lệnh chưa khớp")
                    self.stop_symbol(symbol)
                    return False
            else:
                error_msg = result.get('msg', 'Lỗi không xác định') if result else 'Không có phản hồi'
                self.log(f"❌ {symbol} - Lỗi lệnh: {error_msg}")
                self.stop_symbol(symbol)
                return False

        except Exception as e:
            self.log(f"❌ {symbol} - Lỗi mở vị thế: {str(e)}")
            self.stop_symbol(symbol)
            return False

    def _close_symbol_position(self, symbol, reason=""):
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
                
                pyramiding_info = ""
                if self.pyramiding_enabled:
                    pyramiding_count = self.symbol_data[symbol].get('pyramiding_count', 0)
                    pyramiding_info = f"\n🔄 Số lần đã nhồi: {pyramiding_count}/{self.pyramiding_n}"
                
                message = (f"⛔ <b>ĐÃ ĐÓNG VỊ THẾ {symbol}</b>\n"
                          f"🤖 Bot: {self.bot_id}\n📌 Lý do: {reason}\n"
                          f"🏷️ Exit: {current_price:.4f}\n📊 Khối lượng: {close_qty:.4f}\n"
                          f"💰 PnL: {pnl:.2f} USDC\n"
                          f"📈 Lần hạ giá trung bình: {self.symbol_data[symbol]['average_down_count']}"
                          f"{pyramiding_info}")
                self.log(message)
                
                self.symbol_data[symbol]['last_close_time'] = time.time()
                self._reset_symbol_position(symbol)
                self.bot_coordinator.bot_lost_coin(self.bot_id)
                return True
            else:
                error_msg = result.get('msg', 'Lỗi không xác định') if result else 'Không có phản hồi'
                self.log(f"❌ {symbol} - Lỗi lệnh đóng: {error_msg}")
                self.symbol_data[symbol]['close_attempted'] = False
                return False
                
        except Exception as e:
            self.log(f"❌ {symbol} - Lỗi đóng vị thế: {str(e)}")
            self.symbol_data[symbol]['close_attempted'] = False
            return False

    def _check_margin_safety(self):
        try:
            margin_balance, maint_margin, ratio = get_margin_safety_info(
                self.api_key, self.api_secret
            )

            if margin_balance is None or maint_margin is None:
                return False
            
            if maint_margin <= 0:
                return False
                
            ratio = margin_balance / maint_margin

            if ratio <= self.margin_safety_threshold:
                msg = (
                    f"🛑 BẢO VỆ KÝ QUỸ ĐƯỢC KÍCH HOẠT\n"
                    f"• Margin / Maint = {ratio:.2f}x ≤ {self.margin_safety_threshold:.2f}x\n"
                    f"• Đang đóng toàn bộ vị thế của bot để tránh thanh lý."
                )
                self.log(msg)

                send_telegram(
                    msg,
                    chat_id=self.telegram_chat_id,
                    bot_token=self.telegram_bot_token,
                    default_chat_id=self.telegram_chat_id,
                )

                self.stop_all_symbols()
                return True

            return False

        except Exception as e:
            self.log(f"❌ Lỗi kiểm tra an toàn ký quỹ: {str(e)}")
            return False

    def _check_symbol_tp_sl(self, symbol):
        if (not self.symbol_data[symbol]['position_open'] or 
            self.symbol_data[symbol]['entry'] <= 0 or 
            self.symbol_data[symbol]['close_attempted']):
            return

        current_price = self.get_current_price(symbol)
        if current_price <= 0: return

        if self.symbol_data[symbol]['side'] == "BUY":
            profit = (current_price - self.symbol_data[symbol]['entry']) * abs(self.symbol_data[symbol]['qty'])
        else:
            profit = (self.symbol_data[symbol]['entry'] - current_price) * abs(self.symbol_data[symbol]['qty'])
            
        invested = self.symbol_data[symbol]['entry'] * abs(self.symbol_data[symbol]['qty']) / self.lev
        if invested <= 0: return
            
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
        if symbol not in self.active_symbols: return False
        
        self.log(f"⛔ Đang dừng coin {symbol}...")
        
        if self.current_processing_symbol == symbol:
            timeout = time.time() + 10
            while self.current_processing_symbol == symbol and time.time() < timeout:
                time.sleep(1)
        
        if self.symbol_data[symbol]['position_open']:
            self._close_symbol_position(symbol, "Dừng coin theo lệnh")
        
        self.ws_manager.remove_symbol(symbol)
        self.coin_manager.unregister_coin(symbol)
        
        if symbol in self.symbol_data: del self.symbol_data[symbol]
        if symbol in self.active_symbols: self.active_symbols.remove(symbol)
        
        self.bot_coordinator.bot_lost_coin(self.bot_id)
        self.log(f"✅ Đã dừng coin {symbol}")
        return True

    def stop_all_symbols(self):
        self.log("⛔ Đang dừng tất cả coin...")
        symbols_to_stop = self.active_symbols.copy()
        stopped_count = 0
        
        for symbol in symbols_to_stop:
            if self.stop_symbol(symbol):
                stopped_count += 1
                time.sleep(1)
        
        self.log(f"✅ Đã dừng {stopped_count} coin, bot vẫn chạy")
        return stopped_count

    def stop(self):
        self._stop = True
        stopped_count = self.stop_all_symbols()
        self.log(f"🔴 Bot đã dừng - Đã dừng {stopped_count} coin")

    def check_global_positions(self):
        try:
            positions = get_positions(api_key=self.api_key, api_secret=self.api_secret)
            if not positions:
                self.next_global_side = random.choice(["BUY", "SELL"])
                return self.next_global_side
    
            long_invested = 0.0
            short_invested = 0.0
            long_pnl = 0.0
            short_pnl = 0.0
    
            for pos in positions:
                qty = float(pos.get("positionAmt", 0))
                entry = float(pos.get("entryPrice", 0))
                unrealized = float(pos.get("unRealizedProfit", 0))
    
                if qty == 0 or entry <= 0:
                    continue
    
                invested = entry * abs(qty) / self.lev
    
                if qty > 0:
                    long_invested += invested
                    long_pnl += unrealized
                else:
                    short_invested += invested
                    short_pnl += unrealized
    
            long_roi = (long_pnl / long_invested * 100) if long_invested > 0 else 0
            short_roi = (short_pnl / short_invested * 100) if short_invested > 0 else 0
    
            if long_roi < short_roi:
                self.next_global_side = "SELL"
            elif short_roi < long_roi:
                self.next_global_side = "BUY"
            else:
                self.next_global_side = random.choice(["BUY", "SELL"])
    
            self.log(
                f"🌍 ROI TOÀN TÀI KHOẢN | "
                f"LONG: {long_roi:.2f}% | SHORT: {short_roi:.2f}% "
                f"→ Ưu tiên: {self.next_global_side}"
            )
    
            return self.next_global_side
    
        except Exception as e:
            self.log(f"❌ Lỗi phân tích ROI toàn cục: {str(e)}")
            self.next_global_side = random.choice(["BUY", "SELL"])
            return self.next_global_side

    def get_next_side_based_on_comprehensive_analysis(self):
        """Xác định hướng giao dịch dựa trên số lượng lệnh BUY/SELL hiện có"""
        self.coin_finder.update_position_counts()
        
        buy_count = self.coin_finder.position_counts["BUY"]
        sell_count = self.coin_finder.position_counts["SELL"]
        
        if buy_count > sell_count:
            self.log(f"⚖️ Cân bằng: BUY({buy_count}) > SELL({sell_count}) → Ưu tiên SELL")
            return "SELL"
        elif sell_count > buy_count:
            self.log(f"⚖️ Cân bằng: SELL({sell_count}) > BUY({buy_count}) → Ưu tiên BUY")
            return "BUY"
        else:
            self.log(f"⚖️ Cân bằng: BUY({buy_count}) = SELL({sell_count}) → Random")
            return random.choice(["BUY", "SELL"])

    def log(self, message):
        important_keywords = ['❌', '✅', '⛔', '💰', '📈', '📊', '🎯', '🛡️', '🔴', '🟢', '⚠️', '🚫', '🔄']
        if any(keyword in message for keyword in important_keywords):
            logger.warning(f"[{self.bot_id}] {message}")
            if self.telegram_bot_token and self.telegram_chat_id:
                send_telegram(f"<b>{self.bot_id}</b>: {message}", 
                             bot_token=self.telegram_bot_token, 
                             default_chat_id=self.telegram_chat_id)

class GlobalMarketBot(BaseBot):
    def __init__(self, symbol, lev, percent, tp, sl, roi_trigger, ws_manager,
                 api_key, api_secret, telegram_bot_token, telegram_chat_id, bot_id=None, **kwargs):
        pyramiding_n = kwargs.pop('pyramiding_n', 0)
        pyramiding_x = kwargs.pop('pyramiding_x', 0)
        
        super().__init__(symbol, lev, percent, tp, sl, roi_trigger, ws_manager,
                         api_key, api_secret, telegram_bot_token, telegram_chat_id,
                         "Balance-Strategy-Queue", bot_id=bot_id, 
                         pyramiding_n=pyramiding_n, pyramiding_x=pyramiding_x, **kwargs)

# ========== LỚP QUẢN LÝ BOT ==========
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

        self.bot_coordinator = BotExecutionCoordinator()
        self.coin_manager = CoinManager()
        self.symbol_locks = defaultdict(threading.Lock)
        self.global_side_coordinator = GlobalSideCoordinator()

        if api_key and api_secret:
            self._verify_api_connection()
            self.log("🟢 HỆ THỐNG BOT CÂN BẰNG LỆNH ĐÃ KHỞI ĐỘNG")

            self.telegram_thread = threading.Thread(target=self._telegram_listener, daemon=True)
            self.telegram_thread.start()

            if self.telegram_chat_id:
                self.send_main_menu(self.telegram_chat_id)
        else:
            self.log("⚡ BotManager đã khởi động ở chế độ không cấu hình")
        
        self._initialize_system_cache()

    def _initialize_system_cache(self):
        try:
            logger.info("🔄 Hệ thống đang khởi tạo cache...")
            
            if refresh_usdc_coins_cache():
                update_coins_volume()
                update_coins_price()
                
                cache_info = _USDC_COINS_CACHE
                coins_count = len(cache_info.get("data", []))
                
                logger.info(f"✅ Hệ thống đã khởi tạo cache {coins_count} coin")
                
            else:
                logger.error("❌ Hệ thống không thể khởi tạo cache")
                
        except Exception as e:
            logger.error(f"❌ Lỗi khởi tạo cache hệ thống: {str(e)}")

    def _verify_api_connection(self):
        try:
            balance = get_balance(self.api_key, self.api_secret)
            if balance is None:
                self.log("❌ LỖI: Không thể kết nối đến API Binance. Kiểm tra:")
                self.log("   - API Key và Secret")
                self.log("   - Chặn IP (lỗi 451), thử VPN")
                self.log("   - Kết nối internet")
                return False
            else:
                self.log(f"✅ Kết nối Binance thành công! Số dư: {balance:.2f} USDC")
                return True
        except Exception as e:
            self.log(f"❌ Lỗi kiểm tra kết nối: {str(e)}")
            return False

    def get_position_summary(self):
        try:
            all_positions = get_positions(api_key=self.api_key, api_secret=self.api_secret)
            
            total_long_count, total_short_count = 0, 0
            total_long_pnl, total_short_pnl, total_unrealized_pnl = 0, 0, 0
            
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
            total_bots_with_coins, trading_bots = 0, 0
            balance_bots = 0
            
            for bot_id, bot in self.bots.items():
                has_coin = len(bot.active_symbols) > 0 if hasattr(bot, 'active_symbols') else False
                is_trading = False
                
                if has_coin and hasattr(bot, 'symbol_data'):
                    for symbol, data in bot.symbol_data.items():
                        if data.get('position_open', False):
                            is_trading = True
                            break
                
                if has_coin: total_bots_with_coins += 1
                if is_trading: trading_bots += 1
                if hasattr(bot, 'enable_balance_orders') and bot.enable_balance_orders:
                    balance_bots += 1
                
                bot_details.append({
                    'bot_id': bot_id, 'has_coin': has_coin, 'is_trading': is_trading,
                    'symbols': bot.active_symbols if hasattr(bot, 'active_symbols') else [],
                    'symbol_data': bot.symbol_data if hasattr(bot, 'symbol_data') else {},
                    'status': bot.status, 'leverage': bot.lev, 'percent': bot.percent,
                    'pyramiding': f"{bot.pyramiding_n}/{bot.pyramiding_x}%" if hasattr(bot, 'pyramiding_enabled') and bot.pyramiding_enabled else "Tắt",
                    'balance_orders': "BẬT" if hasattr(bot, 'enable_balance_orders') and bot.enable_balance_orders else "TẮT"
                })
            
            summary = "📊 **THỐNG KÊ CHI TIẾT - HỆ THỐNG CÂN BẰNG**\n\n"
            
            cache_info = _USDC_COINS_CACHE
            coins_in_cache = len(cache_info.get("data", []))
            last_update = cache_info.get("last_price_update", 0)
            update_time = time.ctime(last_update) if last_update > 0 else "Chưa cập nhật"
            
            summary += f"🗂️ **CACHE HỆ THỐNG**: {coins_in_cache} coin | Cập nhật: {update_time}\n"
            summary += f"⚖️ **BOT CÂN BẰNG**: {balance_bots}/{len(self.bots)} bot\n\n"
            
            balance = get_balance(self.api_key, self.api_secret)
            if balance is not None:
                summary += f"💰 **SỐ DƯ**: {balance:.2f} USDC\n"
                summary += f"📈 **Tổng PnL**: {total_unrealized_pnl:.2f} USDC\n\n"
            else:
                summary += f"💰 **SỐ DƯ**: ❌ Lỗi kết nối\n\n"
            
            summary += f"🤖 **SỐ BOT HỆ THỐNG**: {len(self.bots)} bot | {total_bots_with_coins} bot có coin | {trading_bots} bot đang giao dịch\n\n"
            
            summary += f"📈 **PHÂN TÍCH PnL VÀ KHỐI LƯỢNG**:\n"
            summary += f"   📊 Số lượng: LONG={total_long_count} | SHORT={total_short_count}\n"
            summary += f"   💰 PnL: LONG={total_long_pnl:.2f} USDC | SHORT={total_short_pnl:.2f} USDC\n"
            summary += f"   ⚖️ Chênh lệch: {abs(total_long_pnl - total_short_pnl):.2f} USDC\n\n"
            
            queue_info = self.bot_coordinator.get_queue_info()
            summary += f"🎪 **THÔNG TIN HÀNG ĐỢI (FIFO)**\n"
            summary += f"• Bot đang tìm coin: {queue_info['current_finding'] or 'Không có'}\n"
            summary += f"• Bot trong hàng đợi: {queue_info['queue_size']}\n"
            summary += f"• Bot có coin: {len(queue_info['bots_with_coins'])}\n"
            summary += f"• Coin đã phân phối: {queue_info['found_coins_count']}\n\n"
            
            if queue_info['queue_bots']:
                summary += f"📋 **BOT TRONG HÀNG ĐỢI**:\n"
                for i, bot_id in enumerate(queue_info['queue_bots']):
                    summary += f"  {i+1}. {bot_id}\n"
                summary += "\n"
            
            if bot_details:
                summary += "📋 **CHI TIẾT BOT**:\n"
                for bot in bot_details:
                    status_emoji = "🟢" if bot['is_trading'] else "🟡" if bot['has_coin'] else "🔴"
                    balance_emoji = "⚖️" if bot['balance_orders'] == "BẬT" else ""
                    summary += f"{status_emoji} **{bot['bot_id']}** {balance_emoji}\n"
                    summary += f"   💰 Đòn bẩy: {bot['leverage']}x | Vốn: {bot['percent']}% | Nhồi lệnh: {bot['pyramiding']} | Cân bằng: {bot['balance_orders']}\n"
                    
                    if bot['symbols']:
                        for symbol in bot['symbols']:
                            symbol_info = bot['symbol_data'].get(symbol, {})
                            status = "🟢 Đang giao dịch" if symbol_info.get('position_open') else "🟡 Chờ tín hiệu"
                            side = symbol_info.get('side', '')
                            qty = symbol_info.get('qty', 0)
                            
                            summary += f"   🔗 {symbol} | {status}"
                            if side: summary += f" | {side} {abs(qty):.4f}"
                            
                            if symbol_info.get('pyramiding_count', 0) > 0:
                                summary += f" | 🔄 {symbol_info['pyramiding_count']} lần"
                                
                            summary += "\n"
                    else:
                        summary += f"   🔍 Đang tìm coin...\n"
                    summary += "\n"
            
            return summary
                    
        except Exception as e:
            return f"❌ Lỗi thống kê: {str(e)}"

    def log(self, message):
        important_keywords = ['❌', '✅', '⛔', '💰', '📈', '📊', '🎯', '🛡️', '🔴', '🟢', '⚠️', '🚫', '🔄']
        if any(keyword in message for keyword in important_keywords):
            logger.warning(f"[HỆ THỐNG] {message}")
            if self.telegram_bot_token and self.telegram_chat_id:
                send_telegram(f"<b>HỆ THỐNG</b>: {message}", 
                             chat_id=self.telegram_chat_id,
                             bot_token=self.telegram_bot_token, 
                             default_chat_id=self.telegram_chat_id)

    def send_main_menu(self, chat_id):
        welcome = (
            "🤖 <b>BOT GIAO DỊCH FUTURES - CHIẾN LƯỢC CÂN BẰNG LỆNH</b>\n\n"
            "🎯 <b>CƠ CHẾ HOẠT ĐỘNG:</b>\n"
            "• Đếm số lượng lệnh BUY/SELL hiện có trên Binance\n"
            "• Nhiều lệnh BUY hơn → tìm lệnh SELL\n"
            "• Nhiều lệnh SELL hơn → tìm lệnh BUY\n"
            "• Bằng nhau → chọn ngẫu nhiên\n\n"
            
            "📊 <b>LỰA CHỌN COIN:</b>\n"
            "• MUA: chọn coin có giá < 1 USDC, volume thấp nhất đầu tiên\n"
            "• BÁN: chọn coin có giá > 5 USDC, volume cao nhất đầu tiên\n"
            "• Loại trừ coin đã có vị thế để tránh trùng\n"
            "• Loại trừ BTCUSDC, ETHUSDC do biến động cao\n\n"
            
            "🔄 <b>NHỒI LỆNH (PYRAMIDING):</b>\n"
            "• Nhồi lệnh cùng chiều khi đạt mốc ROI\n"
            "• Số lần nhồi và mốc ROI tùy chỉnh\n"
            "• Tự động cập nhật giá trung bình\n\n"
            
            "🎯 <b>CHỐT LỜI SỚM:</b>\n"
            "• Kích hoạt khi đạt ROI target\n"
            "• Chốt lời ngay khi có tín hiệu xấu\n"
            "• Vẫn giữ cơ chế TP/SL thông thường"
        )
        send_telegram(welcome, chat_id=chat_id, reply_markup=create_main_menu(),
                     bot_token=self.telegram_bot_token, 
                     default_chat_id=self.telegram_chat_id)

    def add_bot(self, symbol, lev, percent, tp, sl, roi_trigger, strategy_type, bot_count=1, **kwargs):
        if sl == 0: sl = None
            
        if not self.api_key or not self.api_secret:
            self.log("❌ API Key chưa được cài đặt trong BotManager")
            return False
        
        if not self._verify_api_connection():
            self.log("❌ KHÔNG THỂ KẾT NỐI VỚI BINANCE - KHÔNG THỂ TẠO BOT")
            return False
        
        bot_mode = kwargs.get('bot_mode', 'static')
        pyramiding_n = kwargs.get('pyramiding_n', 0)
        pyramiding_x = kwargs.get('pyramiding_x', 0)
        
        enable_balance_orders = kwargs.get('enable_balance_orders', True)
        buy_price_threshold = kwargs.get('buy_price_threshold', 1.0)
        sell_price_threshold = kwargs.get('sell_price_threshold', 5.0)
        buy_volume_sort = kwargs.get('buy_volume_sort', 'asc')
        sell_volume_sort = kwargs.get('sell_volume_sort', 'desc')
        
        created_count = 0
        
        try:
            for i in range(bot_count):
                if bot_mode == 'static' and symbol:
                    bot_id = f"STATIC_{strategy_type}_{int(time.time())}_{i}"
                else:
                    bot_id = f"DYNAMIC_{strategy_type}_{int(time.time())}_{i}"
                
                if bot_id in self.bots: continue
                
                bot_class = GlobalMarketBot
                
                bot = bot_class(
                    symbol, lev, percent, tp, sl, roi_trigger, self.ws_manager,
                    self.api_key, self.api_secret, self.telegram_bot_token, self.telegram_chat_id,
                    coin_manager=self.coin_manager, symbol_locks=self.symbol_locks,
                    bot_coordinator=self.bot_coordinator, bot_id=bot_id, max_coins=1,
                    pyramiding_n=pyramiding_n, pyramiding_x=pyramiding_x,
                    enable_balance_orders=enable_balance_orders,
                    buy_price_threshold=buy_price_threshold,
                    sell_price_threshold=sell_price_threshold,
                    buy_volume_sort=buy_volume_sort,
                    sell_volume_sort=sell_volume_sort
                )
                
                bot._bot_manager = self
                bot.coin_finder.set_bot_manager(self)
                self.bots[bot_id] = bot
                created_count += 1
                
        except Exception as e:
            self.log(f"❌ Lỗi tạo bot: {str(e)}")
            return False
        
        if created_count > 0:
            roi_info = f" | 🎯 ROI Kích hoạt: {roi_trigger}%" if roi_trigger else " | 🎯 ROI Kích hoạt: Tắt"
            pyramiding_info = f" | 🔄 Nhồi lệnh: {pyramiding_n} lần tại {pyramiding_x}%" if pyramiding_n > 0 and pyramiding_x > 0 else " | 🔄 Nhồi lệnh: Tắt"
            
            balance_info = ""
            if enable_balance_orders:
                balance_info = (f"\n⚖️ <b>CÂN BẰNG LỆNH: BẬT</b>\n"
                              f"• Mua: giá < {buy_price_threshold} USDC | Volume: {buy_volume_sort}\n"
                              f"• Bán: giá > {sell_price_threshold} USDC | Volume: {sell_volume_sort}\n")
            
            success_msg = (f"✅ <b>ĐÃ TẠO {created_count} BOT CÂN BẰNG</b>\n\n"
                          f"🎯 Chiến lược: {strategy_type}\n💰 Đòn bẩy: {lev}x\n"
                          f"📈 % Số dư: {percent}%\n🎯 TP: {tp}%\n"
                          f"🛡️ SL: {sl if sl is not None else 'Tắt'}%{roi_info}{pyramiding_info}\n"
                          f"🔧 Chế độ: {bot_mode}\n🔢 Số bot: {created_count}\n")
            
            if bot_mode == 'static' and symbol:
                success_msg += f"🔗 Coin ban đầu: {symbol}\n"
            else:
                success_msg += f"🔗 Coin: Tự động tìm\n"
            
            success_msg += balance_info
            
            success_msg += (f"\n🔄 <b>CƠ CHẾ CÂN BẰNG ĐƯỢC KÍCH HOẠT</b>\n"
                          f"• Đếm số lượng lệnh BUY/SELL hiện có\n"
                          f"• Ưu tiên hướng ngược lại khi mất cân bằng\n"
                          f"• Lọc coin theo ngưỡng giá (MUA <1 USDC, BÁN >5 USDC)\n"
                          f"• Sắp xếp volume (MUA: thấp nhất đầu, BÁN: cao nhất đầu)\n\n")
            
            if pyramiding_n > 0:
                success_msg += (f"🔄 <b>NHỒI LỆNH ĐƯỢC KÍCH HOẠT</b>\n"
                              f"• Nhồi {pyramiding_n} lần khi đạt mỗi mốc {pyramiding_x}% ROI\n"
                              f"• Mỗi lần nhồi dùng {percent}% vốn ban đầu\n"
                              f"• Tự động cập nhật giá trung bình\n\n")
            
            success_msg += f"⚡ <b>MỖI BOT CHẠY TRONG LUỒNG RIÊNG BIỆT</b>"
            
            self.log(success_msg)
            return True
        else:
            self.log("❌ Không thể tạo bot")
            return False

    def stop_coin(self, symbol):
        stopped_count = 0
        symbol = symbol.upper()
        
        for bot_id, bot in self.bots.items():
            if hasattr(bot, 'stop_symbol') and symbol in bot.active_symbols:
                if bot.stop_symbol(symbol): stopped_count += 1
                    
        if stopped_count > 0:
            self.log(f"✅ Đã dừng coin {symbol} trong {stopped_count} bot")
            return True
        else:
            self.log(f"❌ Không tìm thấy coin {symbol} trong bot nào")
            return False

    def get_coin_management_keyboard(self):
        all_coins = set()
        for bot in self.bots.values():
            if hasattr(bot, 'active_symbols'):
                all_coins.update(bot.active_symbols)
        
        if not all_coins: return None
            
        keyboard = []
        row = []
        for coin in sorted(list(all_coins))[:12]:
            row.append({"text": f"⛔ Coin: {coin}"})
            if len(row) == 2:
                keyboard.append(row)
                row = []
        if row: keyboard.append(row)
        
        keyboard.append([{"text": "⛔ DỪNG TẤT CẢ COIN"}])
        keyboard.append([{"text": "❌ Hủy bỏ"}])
        
        return {"keyboard": keyboard, "resize_keyboard": True, "one_time_keyboard": True}

    def stop_bot_symbol(self, bot_id, symbol):
        bot = self.bots.get(bot_id)
        if bot and hasattr(bot, 'stop_symbol'):
            success = bot.stop_symbol(symbol)
            if success: self.log(f"⛔ Đã dừng coin {symbol} trong bot {bot_id}")
            return success
        return False

    def stop_all_bot_symbols(self, bot_id):
        bot = self.bots.get(bot_id)
        if bot and hasattr(bot, 'stop_all_symbols'):
            stopped_count = bot.stop_all_symbols()
            self.log(f"⛔ Đã dừng {stopped_count} coin trong bot {bot_id}")
            return stopped_count
        return 0

    def stop_all_coins(self):
        self.log("⛔ Đang dừng tất cả coin trong tất cả bot...")
        total_stopped = 0
        for bot_id, bot in self.bots.items():
            if hasattr(bot, 'stop_all_symbols'):
                stopped_count = bot.stop_all_symbols()
                total_stopped += stopped_count
                self.log(f"⛔ Đã dừng {stopped_count} coin trong bot {bot_id}")
        
        self.log(f"✅ Đã dừng tổng cộng {total_stopped} coin, hệ thống vẫn chạy")
        return total_stopped

    def stop_bot(self, bot_id):
        bot = self.bots.get(bot_id)
        if bot:
            bot.stop()
            del self.bots[bot_id]
            self.log(f"🔴 Đã dừng bot {bot_id}")
            return True
        return False

    def stop_all(self):
        self.log("🔴 Đang dừng tất cả bot...")
        for bot_id in list(self.bots.keys()):
            self.stop_bot(bot_id)
        self.log("🔴 Đã dừng tất cả bot, hệ thống vẫn chạy")

    def _telegram_listener(self):
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
                            
                            if chat_id != self.telegram_chat_id: continue
                            
                            if update_id > last_update_id:
                                last_update_id = update_id
                                self._handle_telegram_message(chat_id, text)
                
                time.sleep(0.1)
                
            except Exception as e:
                logger.error(f"Lỗi nghe Telegram: {str(e)}")
                time.sleep(1)

    def _handle_telegram_message(self, chat_id, text):
        user_state = self.user_states.get(chat_id, {})
        current_step = user_state.get('step')
        
        if text == "⚖️ Cân bằng lệnh":
            self.user_states[chat_id] = {'step': 'waiting_balance_config'}
            send_telegram("⚖️ <b>CẤU HÌNH CÂN BẰNG LỆNH</b>\n\nChọn hành động:",
                         chat_id=chat_id, reply_markup=create_balance_config_keyboard(),
                         bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif current_step == 'waiting_balance_config':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy cấu hình cân bằng", chat_id=chat_id,
                             reply_markup=create_main_menu(),
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            elif text == '⚖️ Bật cân bằng lệnh':
                user_state['enable_balance'] = True
                user_state['step'] = 'waiting_buy_threshold'
                send_telegram("✅ Đã chọn BẬT cân bằng lệnh\n\nNhập ngưỡng giá MUA (USDC):",
                             chat_id=chat_id, reply_markup=create_price_threshold_keyboard(),
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            elif text == '⚖️ Tắt cân bằng lệnh':
                updated_bots = 0
                for bot in self.bots.values():
                    if hasattr(bot, 'enable_balance_orders'):
                        bot.enable_balance_orders = False
                        updated_bots += 1
                
                self.user_states[chat_id] = {}
                send_telegram(f"✅ Đã TẮT cân bằng lệnh cho {updated_bots} bot",
                             chat_id=chat_id, reply_markup=create_main_menu(),
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            elif text == '📊 Xem cấu hình cân bằng':
                config_info = (
                    f"⚖️ <b>CẤU HÌNH CÂN BẰNG HIỆN TẠI</b>\n\n"
                    f"• Ngưỡng giá MUA: {_BALANCE_CONFIG['buy_price_threshold']} USDC\n"
                    f"• Ngưỡng giá BÁN: {_BALANCE_CONFIG['sell_price_threshold']} USDC\n"
                    f"• Sắp xếp MUA: {_BALANCE_CONFIG['buy_volume_sort']}\n"
                    f"• Sắp xếp BÁN: {_BALANCE_CONFIG['sell_volume_sort']}\n\n"
                    f"🔄 <b>CACHE HỆ THỐNG</b>\n"
                    f"• Số coin: {len(_USDC_COINS_CACHE.get('data', []))}\n"
                    f"• Cập nhật giá: {time.ctime(_USDC_COINS_CACHE.get('last_price_update', 0))}\n"
                    f"• Cập nhật volume: {time.ctime(_USDC_COINS_CACHE.get('last_volume_update', 0))}"
                )
                send_telegram(config_info, chat_id=chat_id,
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            elif text == '🔄 Làm mới cache':
                if force_refresh_coin_cache():
                    send_telegram("✅ Đã làm mới cache coin thành công",
                                 chat_id=chat_id,
                                 bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                else:
                    send_telegram("❌ Không thể làm mới cache",
                                 chat_id=chat_id,
                                 bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif current_step == 'waiting_buy_threshold':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy cấu hình cân bằng", chat_id=chat_id,
                             reply_markup=create_main_menu(),
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                try:
                    buy_threshold = float(text)
                    if buy_threshold <= 0:
                        send_telegram("⚠️ Ngưỡng giá MUA phải >0. Vui lòng chọn:",
                                    chat_id=chat_id, reply_markup=create_price_threshold_keyboard(),
                                    bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                        return
                    
                    user_state['buy_price_threshold'] = buy_threshold
                    user_state['step'] = 'waiting_sell_threshold'
                    send_telegram(f"✅ Ngưỡng MUA: < {buy_threshold} USDC\n\nNhập ngưỡng giá BÁN (USDC):",
                                chat_id=chat_id, reply_markup=create_price_threshold_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                except ValueError:
                    send_telegram("⚠️ Vui lòng nhập số hợp lệ cho ngưỡng giá MUA:",
                                chat_id=chat_id, reply_markup=create_price_threshold_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif current_step == 'waiting_sell_threshold':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy cấu hình cân bằng", chat_id=chat_id,
                             reply_markup=create_main_menu(),
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                try:
                    sell_threshold = float(text)
                    if sell_threshold <= 0:
                        send_telegram("⚠️ Ngưỡng giá BÁN phải >0. Vui lòng chọn:",
                                    chat_id=chat_id, reply_markup=create_price_threshold_keyboard(),
                                    bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                        return
                    
                    user_state['sell_price_threshold'] = sell_threshold
                    user_state['step'] = 'waiting_buy_volume_sort'
                    send_telegram(f"✅ Ngưỡng BÁN: > {sell_threshold} USDC\n\nChọn sắp xếp volume cho MUA:",
                                chat_id=chat_id, reply_markup=create_volume_sort_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                except ValueError:
                    send_telegram("⚠️ Vui lòng nhập số hợp lệ cho ngưỡng giá BÁN:",
                                chat_id=chat_id, reply_markup=create_price_threshold_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif current_step == 'waiting_buy_volume_sort':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy cấu hình cân bằng", chat_id=chat_id,
                             reply_markup=create_main_menu(),
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            elif text in ['asc - Tăng dần', 'desc - Giảm dần']:
                buy_sort = 'asc' if 'asc' in text else 'desc'
                user_state['buy_volume_sort'] = buy_sort
                user_state['step'] = 'waiting_sell_volume_sort'
                send_telegram(f"✅ Sắp xếp MUA: {buy_sort}\n\nChọn sắp xếp volume cho BÁN:",
                            chat_id=chat_id, reply_markup=create_volume_sort_keyboard(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif current_step == 'waiting_sell_volume_sort':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy cấu hình cân bằng", chat_id=chat_id,
                             reply_markup=create_main_menu(),
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            elif text in ['asc - Tăng dần', 'desc - Giảm dần']:
                sell_sort = 'asc' if 'asc' in text else 'desc'
                user_state['sell_volume_sort'] = sell_sort
                
                update_balance_config(
                    buy_price_threshold=user_state.get('buy_price_threshold'),
                    sell_price_threshold=user_state.get('sell_price_threshold'),
                    buy_volume_sort=user_state.get('buy_volume_sort'),
                    sell_volume_sort=user_state.get('sell_volume_sort')
                )
                
                updated_bots = 0
                for bot in self.bots.values():
                    if hasattr(bot, 'enable_balance_orders') and bot.enable_balance_orders:
                        bot.balance_config = {
                            'buy_price_threshold': user_state.get('buy_price_threshold', 1.0),
                            'sell_price_threshold': user_state.get('sell_price_threshold', 5.0),
                            'buy_volume_sort': user_state.get('buy_volume_sort', 'asc'),
                            'sell_volume_sort': user_state.get('sell_volume_sort', 'desc'),
                        }
                        updated_bots += 1
                
                config_summary = (
                    f"✅ <b>ĐÃ CẬP NHẬT CẤU HÌNH CÂN BẰNG</b>\n\n"
                    f"• Ngưỡng MUA: < {user_state.get('buy_price_threshold', 1.0)} USDC\n"
                    f"• Ngưỡng BÁN: > {user_state.get('sell_price_threshold', 5.0)} USDC\n"
                    f"• Sắp xếp MUA: {user_state.get('buy_volume_sort', 'asc')}\n"
                    f"• Sắp xếp BÁN: {user_state.get('sell_volume_sort', 'desc')}\n\n"
                    f"🔄 Đã cập nhật cho {updated_bots} bot có cân bằng lệnh"
                )
                
                send_telegram(config_summary, chat_id=chat_id,
                             reply_markup=create_main_menu(),
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                
                self.user_states[chat_id] = {}

        elif current_step == 'waiting_bot_mode':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id=chat_id, reply_markup=create_main_menu(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            elif text in ["🤖 Bot Tĩnh - Coin cụ thể", "🔄 Bot Động - Tự tìm coin"]:
                if text == "🤖 Bot Tĩnh - Coin cụ thể":
                    user_state['bot_mode'] = 'static'
                    user_state['step'] = 'waiting_symbol'
                    send_telegram("🎯 <b>ĐÃ CHỌN: BOT TĨNH</b>\n\nBot sẽ giao dịch COIN CỐ ĐỊNH\nChọn coin:",
                                chat_id=chat_id, reply_markup=create_symbols_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                else:
                    user_state['bot_mode'] = 'dynamic'
                    user_state['step'] = 'waiting_bot_count'
                    send_telegram("🎯 <b>ĐÃ CHỌN: BOT ĐỘNG</b>\n\nHệ thống sẽ tự động quản lý coin\nChọn số lượng bot (mỗi bot quản lý 1 coin):",
                                chat_id=chat_id, reply_markup=create_bot_count_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
    
        elif current_step == 'waiting_bot_count':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id=chat_id, reply_markup=create_main_menu(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                try:
                    bot_count = int(text)
                    if bot_count <= 0 or bot_count > 20:
                        send_telegram("⚠️ Số bot phải từ 1-20. Vui lòng chọn:",
                                    chat_id=chat_id, reply_markup=create_bot_count_keyboard(),
                                    bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                        return
    
                    user_state['bot_count'] = bot_count
                    user_state['step'] = 'waiting_leverage'
                    
                    send_telegram(f"🤖 Số bot: {bot_count}\n\nChọn đòn bẩy:",
                                chat_id=chat_id, reply_markup=create_leverage_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                except ValueError:
                    send_telegram("⚠️ Vui lòng nhập số hợp lệ cho số bot:",
                                chat_id=chat_id, reply_markup=create_bot_count_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
    
        elif current_step == 'waiting_symbol':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id=chat_id, reply_markup=create_main_menu(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                user_state['symbol'] = text
                user_state['step'] = 'waiting_leverage'
                send_telegram(f"🔗 Coin: {text}\n\nChọn đòn bẩy:",
                            chat_id=chat_id, reply_markup=create_leverage_keyboard(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
    
        elif current_step == 'waiting_leverage':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id=chat_id, reply_markup=create_main_menu(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                lev_text = text[:-1] if text.endswith('x') else text
                try:
                    leverage = int(lev_text)
                    if leverage <= 0 or leverage > 100:
                        send_telegram("⚠️ Đòn bẩy phải từ 1-100. Vui lòng chọn:",
                                    chat_id=chat_id, reply_markup=create_leverage_keyboard(),
                                    bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                        return
    
                    user_state['leverage'] = leverage
                    user_state['step'] = 'waiting_percent'
                    
                    balance = get_balance(self.api_key, self.api_secret)
                    balance_info = f"\n💰 Số dư hiện tại: {balance:.2f} USDT" if balance else ""
                    
                    send_telegram(f"💰 Đòn bẩy: {leverage}x{balance_info}\n\nChọn % số dư mỗi lệnh:",
                                chat_id=chat_id, reply_markup=create_percent_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
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
                        send_telegram("⚠️ % số dư phải từ 0.1-100. Vui lòng chọn:",
                                    chat_id=chat_id, reply_markup=create_percent_keyboard(),
                                    bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                        return
    
                    user_state['percent'] = percent
                    user_state['step'] = 'waiting_tp'
                    
                    balance = get_balance(self.api_key, self.api_secret)
                    actual_amount = balance * (percent / 100) if balance else 0
                    
                    send_telegram(f"📊 % Số dư: {percent}%\n💵 Số tiền mỗi lệnh: ~{actual_amount:.2f} USDT\n\nChọn Take Profit (%):",
                                chat_id=chat_id, reply_markup=create_tp_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
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
                        send_telegram("⚠️ Take Profit phải >0. Vui lòng chọn:",
                                    chat_id=chat_id, reply_markup=create_tp_keyboard(),
                                    bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                        return
    
                    user_state['tp'] = tp
                    user_state['step'] = 'waiting_sl'
                    
                    send_telegram(f"🎯 Take Profit: {tp}%\n\nChọn Stop Loss (%):",
                                chat_id=chat_id, reply_markup=create_sl_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
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
                        send_telegram("⚠️ Stop Loss phải >=0. Vui lòng chọn:",
                                    chat_id=chat_id, reply_markup=create_sl_keyboard(),
                                    bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                        return
    
                    user_state['sl'] = sl
                    user_state['step'] = 'waiting_pyramiding_n'
                    
                    send_telegram(f"🛡️ Stop Loss: {sl}%\n\n🔄 <b>CẤU HÌNH NHỒI LỆNH (PYRAMIDING)</b>\n\nNhập số lần nhồi lệnh (0 để tắt):",
                                chat_id=chat_id, reply_markup=create_pyramiding_n_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                except ValueError:
                    send_telegram("⚠️ Vui lòng nhập số hợp lệ cho Stop Loss:",
                                chat_id=chat_id, reply_markup=create_sl_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif current_step == 'waiting_pyramiding_n':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id=chat_id, reply_markup=create_main_menu(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            elif text == '❌ Tắt tính năng':
                user_state['pyramiding_n'] = 0
                user_state['pyramiding_x'] = 0
                user_state['step'] = 'waiting_roi_trigger'
                send_telegram(f"🔄 Nhồi lệnh: TẮT\n\n🎯 <b>CHỌN NGƯỠNG ROI CHO THOÁT THÔNG MINH</b>\n\nChọn ngưỡng kích hoạt ROI (%):",
                            chat_id=chat_id, reply_markup=create_roi_trigger_keyboard(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                try:
                    pyramiding_n = int(text)
                    if pyramiding_n < 0 or pyramiding_n > 15:
                        send_telegram("⚠️ Số lần nhồi lệnh phải từ 0-15. Vui lòng chọn:",
                                    chat_id=chat_id, reply_markup=create_pyramiding_n_keyboard(),
                                    bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                        return
    
                    user_state['pyramiding_n'] = pyramiding_n
                    
                    if pyramiding_n > 0:
                        user_state['step'] = 'waiting_pyramiding_x'
                        send_telegram(f"🔄 Số lần nhồi: {pyramiding_n}\n\nNhập mốc ROI để nhồi lệnh (%):",
                                    chat_id=chat_id, reply_markup=create_pyramiding_x_keyboard(),
                                    bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                    else:
                        user_state['pyramiding_x'] = 0
                        user_state['step'] = 'waiting_roi_trigger'
                        send_telegram(f"🔄 Nhồi lệnh: TẮT\n\n🎯 <b>CHỌN NGƯỠNG ROI CHO THOÁT THÔNG MINH</b>\n\nChọn ngưỡng kích hoạt ROI (%):",
                                    chat_id=chat_id, reply_markup=create_roi_trigger_keyboard(),
                                    bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                except ValueError:
                    send_telegram("⚠️ Vui lòng nhập số nguyên cho số lần nhồi lệnh:",
                                chat_id=chat_id, reply_markup=create_pyramiding_n_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif current_step == 'waiting_pyramiding_x':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id=chat_id, reply_markup=create_main_menu(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                try:
                    pyramiding_x = float(text)
                    if pyramiding_x <= 0:
                        send_telegram("⚠️ Mốc ROI nhồi lệnh phải >0. Vui lòng chọn:",
                                    chat_id=chat_id, reply_markup=create_pyramiding_x_keyboard(),
                                    bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                        return
    
                    user_state['pyramiding_x'] = pyramiding_x
                    user_state['step'] = 'waiting_roi_trigger'
                    
                    send_telegram(f"🔄 Nhồi lệnh: {user_state['pyramiding_n']} lần tại {pyramiding_x}% ROI\n\n🎯 <b>CHỌN NGƯỠNG ROI CHO THOÁT THÔNG MINH</b>\n\nChọn ngưỡng kích hoạt ROI (%):",
                                chat_id=chat_id, reply_markup=create_roi_trigger_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                except ValueError:
                    send_telegram("⚠️ Vui lòng nhập số cho mốc ROI nhồi lệnh:",
                                chat_id=chat_id, reply_markup=create_pyramiding_x_keyboard(),
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
                        send_telegram("⚠️ Ngưỡng ROI phải >0. Vui lòng chọn:",
                                    chat_id=chat_id, reply_markup=create_roi_trigger_keyboard(),
                                    bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                        return
    
                    user_state['roi_trigger'] = roi_trigger
                    self._finish_bot_creation(chat_id, user_state)
                    
                except ValueError:
                    send_telegram("⚠️ Vui lòng nhập số hợp lệ cho Ngưỡng ROI:",
                                chat_id=chat_id, reply_markup=create_roi_trigger_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif text == "⛔ Quản lý Coin":
            keyboard = self.get_coin_management_keyboard()
            if not keyboard:
                send_telegram("📭 Không có coin nào đang được quản lý", chat_id=chat_id,
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                send_telegram("⛔ <b>QUẢN LÝ COIN</b>\n\nChọn coin để dừng:",
                            chat_id=chat_id, reply_markup=keyboard,
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
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
            self.user_states[chat_id] = {'step': 'waiting_bot_mode'}
            balance = get_balance(self.api_key, self.api_secret)
            if balance is None:
                send_telegram("❌ <b>LỖI KẾT NỐI BINANCE</b>\nKiểm tra API Key và mạng!", chat_id=chat_id,
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                return
            
            send_telegram(f"🎯 <b>CHỌN CHẾ ĐỘ BOT</b>\n\n💰 Số dư hiện tại: <b>{balance:.2f} USDT</b>\n\nChọn chế độ bot:",
                         chat_id=chat_id, reply_markup=create_bot_mode_keyboard(),
                         bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
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
                if bot_keyboard: keyboard.extend(bot_keyboard)
                keyboard.append([{"text": "⛔ DỪNG TẤT CẢ BOT"}])
                keyboard.append([{"text": "❌ Hủy bỏ"}])
                
                send_telegram(message, chat_id=chat_id, 
                            reply_markup={"keyboard": keyboard, "resize_keyboard": True, "one_time_keyboard": True},
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif text == "📊 Thống kê":
            summary = self.get_position_summary()
            send_telegram(summary, chat_id=chat_id,
                         bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif text == "💰 Số dư":
            try:
                balance = get_balance(self.api_key, self.api_secret)
                if balance is None:
                    send_telegram("❌ <b>LỖI KẾT NỐI BINANCE</b>\nKiểm tra API Key và mạng!", chat_id=chat_id,
                                 bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                else:
                    send_telegram(f"💰 <b>SỐ DƯ KHẢ DỤNG</b>: {balance:.2f} USDT", chat_id=chat_id,
                                 bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            except Exception as e:
                send_telegram(f"⚠️ Lỗi số dư: {str(e)}", chat_id=chat_id,
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif text == "📈 Vị thế":
            try:
                positions = get_positions(api_key=self.api_key, api_secret=self.api_secret)
                if not positions:
                    send_telegram("📭 Không có vị thế mở", chat_id=chat_id,
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
                        
                        message += (f"🔹 {symbol} | {side}\n"
                                  f"📊 Khối lượng: {abs(position_amt):.4f}\n"
                                  f"🏷️ Entry: {entry:.4f}\n"
                                  f"💰 PnL: {pnl:.2f} USDT\n\n")
                
                send_telegram(message, chat_id=chat_id,
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            except Exception as e:
                send_telegram(f"⚠️ Lỗi vị thế: {str(e)}", chat_id=chat_id,
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif text == "🎯 Chiến lược":
            strategy_info = (
                "🎯 <b>CHIẾN LƯỢC CÂN BẰNG LỆNH</b>\n\n"
                "📊 <b>CƠ CHẾ CÂN BẰNG:</b>\n"
                "1. Đếm số lượng lệnh BUY/SELL hiện có trên Binance\n"
                "2. Nhiều lệnh BUY hơn → tìm lệnh SELL\n"
                "3. Nhiều lệnh SELL hơn → tìm lệnh BUY\n"
                "4. Bằng nhau → chọn ngẫu nhiên\n\n"
                
                "💰 <b>LỰA CHỌN COIN:</b>\n"
                "• MUA: chỉ chọn coin có giá < 1 USDC\n"
                "• BÁN: chỉ chọn coin có giá > 5 USDC\n"
                "• Sắp xếp MUA: volume thấp nhất đầu tiên (tăng dần)\n"
                "• Sắp xếp BÁN: volume cao nhất đầu tiên (giảm dần)\n"
                "• Loại trừ coin đã có vị thế để tránh trùng\n"
                "• Loại trừ BTCUSDC, ETHUSDC\n\n"
                
                "🎯 <b>ĐIỀU KIỆN THOÁT LỆNH:</b>\n"
                "• Chốt lời sớm khi đạt ROI target\n"
                "• Vẫn giữ cơ chế TP/SL thông thường\n"
                "• Tự động chốt lời khi có tín hiệu xấu\n\n"
                
                "🔄 <b>NHỒI LỆNH (PYRAMIDING):</b>\n"
                "• Nhồi lệnh cùng chiều khi đạt mốc ROI\n"
                "• Số lần nhồi (n) và mốc ROI (x) tùy chỉnh\n"
                "• Mỗi lần nhồi dùng % vốn ban đầu\n"
                "• Tự động cập nhật giá trung bình\n\n"
                
                "🔄 <b>CƠ CHẾ HÀNG ĐỢI (FIFO):</b>\n"
                "• Chỉ 1 bot tìm coin tại một thời điểm\n"
                "• Bot vào lệnh → bot tiếp theo tìm coin\n"
                "• Tránh trùng lặp coin giữa các bot"
            )
            send_telegram(strategy_info, chat_id=chat_id,
                         bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif text == "⚙️ Cấu hình":
            balance = get_balance(self.api_key, self.api_secret)
            api_status = "✅ Đã kết nối" if balance is not None else "❌ Lỗi kết nối"
            
            total_bots_with_coins, trading_bots = 0, 0
            pyramiding_bots = 0
            balance_bots = 0
            for bot in self.bots.values():
                if hasattr(bot, 'active_symbols'):
                    if len(bot.active_symbols) > 0: total_bots_with_coins += 1
                    for symbol, data in bot.symbol_data.items():
                        if data.get('position_open', False): trading_bots += 1
                if hasattr(bot, 'pyramiding_enabled') and bot.pyramiding_enabled:
                    pyramiding_bots += 1
                if hasattr(bot, 'enable_balance_orders') and bot.enable_balance_orders:
                    balance_bots += 1
            
            config_info = (f"⚙️ <b>CẤU HÌNH HỆ THỐNG CÂN BẰNG LỆNH</b>\n\n"
                          f"🔑 Binance API: {api_status}\n🤖 Tổng bot: {len(self.bots)}\n"
                          f"📊 Bot có coin: {total_bots_with_coins}\n"
                          f"🟢 Bot đang giao dịch: {trading_bots}\n"
                          f"🔄 Bot có nhồi lệnh: {pyramiding_bots}\n"
                          f"⚖️ Bot cân bằng lệnh: {balance_bots}\n"
                          f"🌐 WebSocket: {len(self.ws_manager.connections)} kết nối\n"
                          f"🔄 Cooldown: 1s\n📋 Hàng đợi: {self.bot_coordinator.get_queue_info()['queue_size']} bot\n\n"
                          f"⚖️ <b>CÂN BẰNG LỆNH:</b> MUA < {_BALANCE_CONFIG['buy_price_threshold']}USDC | BÁN > {_BALANCE_CONFIG['sell_price_threshold']}USDC\n"
                          f"📊 <b>SẮP XẾP:</b> MUA={_BALANCE_CONFIG['buy_volume_sort']} | BÁN={_BALANCE_CONFIG['sell_volume_sort']}\n"
                          f"🎯 <b>CHIẾN LƯỢC CÂN BẰNG ĐANG HOẠT ĐỘNG</b>")
            send_telegram(config_info, chat_id=chat_id,
                         bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif text:
            self.send_main_menu(chat_id)

    def _finish_bot_creation(self, chat_id, user_state):
        try:
            bot_mode = user_state.get('bot_mode', 'static')
            leverage = user_state.get('leverage')
            percent = user_state.get('percent')
            tp = user_state.get('tp')
            sl = user_state.get('sl')
            roi_trigger = user_state.get('roi_trigger')
            symbol = user_state.get('symbol')
            bot_count = user_state.get('bot_count', 1)
            pyramiding_n = user_state.get('pyramiding_n', 0)
            pyramiding_x = user_state.get('pyramiding_x', 0)
            enable_balance_orders = user_state.get('enable_balance_orders', True)
            
            success = self.add_bot(
                symbol=symbol, lev=leverage, percent=percent, tp=tp, sl=sl,
                roi_trigger=roi_trigger, strategy_type="Balance-Strategy",
                bot_mode=bot_mode, bot_count=bot_count,
                pyramiding_n=pyramiding_n, pyramiding_x=pyramiding_x,
                enable_balance_orders=enable_balance_orders
            )
            
            if success:
                roi_info = f" | 🎯 ROI Kích hoạt: {roi_trigger}%" if roi_trigger else ""
                pyramiding_info = f" | 🔄 Nhồi lệnh: {pyramiding_n} lần tại {pyramiding_x}%" if pyramiding_n > 0 and pyramiding_x > 0 else ""
                balance_info = " | ⚖️ Cân bằng: BẬT" if enable_balance_orders else ""
                
                success_msg = (f"✅ <b>ĐÃ TẠO BOT THÀNH CÔNG</b>\n\n"
                              f"🤖 Chiến lược: Cân bằng lệnh\n🔧 Chế độ: {bot_mode}\n"
                              f"🔢 Số bot: {bot_count}\n💰 Đòn bẩy: {leverage}x\n"
                              f"📊 % Số dư: {percent}%\n🎯 TP: {tp}%\n"
                              f"🛡️ SL: {sl}%{roi_info}{pyramiding_info}{balance_info}")
                if bot_mode == 'static' and symbol: success_msg += f"\n🔗 Coin: {symbol}"
                
                success_msg += (f"\n\n🔄 <b>CƠ CHẾ CÂN BẰNG ĐƯỢC KÍCH HOẠT</b>\n"
                              f"• Đếm số lượng lệnh BUY/SELL hiện có\n"
                              f"• Ưu tiên hướng ngược lại khi mất cân bằng\n"
                              f"• Lọc coin theo ngưỡng giá (MUA <1 USDC, BÁN >5 USDC)\n"
                              f"• Sắp xếp volume (MUA: thấp nhất đầu, BÁN: cao nhất đầu)\n\n")
                
                if pyramiding_n > 0:
                    success_msg += (f"🔄 <b>NHỒI LỆNH ĐƯỢC KÍCH HOẠT</b>\n"
                                  f"• Nhồi {pyramiding_n} lần khi đạt mỗi mốc {pyramiding_x}% ROI\n"
                                  f"• Mỗi lần nhồi dùng {percent}% vốn ban đầu\n"
                                  f"• Tự động cập nhật giá trung bình\n\n")
                
                success_msg += f"⚡ <b>MỖI BOT CHẠY TRONG LUỒNG RIÊNG BIỆT</b>"
                
                send_telegram(success_msg, chat_id=chat_id, reply_markup=create_main_menu(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                send_telegram("❌ Lỗi tạo bot. Vui lòng thử lại.",
                            chat_id=chat_id, reply_markup=create_main_menu(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            
            self.user_states[chat_id] = {}
            
        except Exception as e:
            send_telegram(f"❌ Lỗi tạo bot: {str(e)}", chat_id=chat_id, reply_markup=create_main_menu(),
                        bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            self.user_states[chat_id] = {}


ssl._create_default_https_context = ssl._create_unverified_context
