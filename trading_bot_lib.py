# trading_bot_lib_final.py (HOÀN CHỈNH - CHIẾN LƯỢC CÂN BẰNG + SẮP XẾP VOLUME)
# =============================================================================
#  TÍNH NĂNG NỔI BẬT:
#  1. Cache coin tập trung, tự động làm mới trong BotManager – mỗi bot CHỈ ĐỌC.
#  2. FIFO queue cho bot động: chỉ 1 bot được tìm coin tại 1 thời điểm.
#  3. Lọc coin CHỈ dựa trên ngưỡng giá + đòn bẩy tối thiểu (mặc định 10x).
#  4. SẮP XẾP COIN THEO KHỐI LƯỢNG GIẢM DẦN – ưu tiên thanh khoản.
#  5. Cân bằng lệnh toàn cục dựa trên số lượng vị thế LONG/SHORT.
#  6. Hỗ trợ đầy đủ USDT và USDC.
#  7. Telegram tương tác + cấu hình trực quan (đã hoàn thiện tất cả các bước).
#  8. FIX: Bot giải phóng queue ngay khi thất bại, không treo.
# =============================================================================

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

# Blacklist mở rộng cho cả USDT và USDC
_SYMBOL_BLACKLIST = {'BTCUSDT', 'ETHUSDT', 'BTCUSDC', 'ETHUSDC'}

# ========== CACHE COIN TẬP TRUNG (USDT + USDC) – CHỈ BOTMANAGER GHI ==========
_COINS_CACHE = {
    "data": [],
    "last_volume_update": 0,
    "last_price_update": 0,
}
_VOLUME_CACHE_TTL = 6 * 3600  # 6 giờ
_PRICE_CACHE_TTL = 300        # 5 phút
_CACHE_REFRESH_INTERVAL = 300 # 5 phút làm mới hoàn toàn

# ========== CẤU HÌNH CÂN BẰNG LỆNH ==========
_BALANCE_CONFIG = {
    "buy_price_threshold": 1.0,
    "sell_price_threshold": 10.0,
    "min_leverage": 10,        # Đòn bẩy tối thiểu mặc định
    "sort_by_volume": True,    # BẬT sắp xếp theo volume giảm dần
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

                if buy_count > sell_count:
                    self.next_global_side = "SELL"
                elif sell_count > buy_count:
                    self.next_global_side = "BUY"
                else:
                    self.next_global_side = random.choice(["BUY", "SELL"])

                self.last_global_check = current_time
                logger.info(f"🌍 Số lượng vị thế toàn cục: BUY={buy_count}, SELL={sell_count} → Ưu tiên: {self.next_global_side}")
                return self.next_global_side

            except Exception as e:
                logger.error(f"❌ Lỗi cập nhật số lượng toàn cục: {str(e)}")
                self.next_global_side = random.choice(["BUY", "SELL"])
                return self.next_global_side

    def get_next_side(self, api_key, api_secret):
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
    if reply_markup:
        payload["reply_markup"] = json.dumps(reply_markup)

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
        "keyboard": [
            [{"text": "1"}, {"text": "3"}, {"text": "5"}],
            [{"text": "10"}, {"text": "20"}],
            [{"text": "❌ Hủy bỏ"}]
        ],
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
        coins = get_coins_with_info()  # chỉ đọc cache
        coins_sorted = sorted(coins, key=lambda x: x['volume'], reverse=True)[:12]
        symbols = [coin['symbol'] for coin in coins_sorted if coin['volume'] > 0]
        if not symbols:
            symbols = ["BNBUSDT", "ADAUSDT", "DOGEUSDT", "XRPUSDT", "DOTUSDT", "LINKUSDT", "SOLUSDT", "MATICUSDT"]
    except:
        symbols = ["BNBUSDT", "ADAUSDT", "DOGEUSDT", "XRPUSDT", "DOTUSDT", "LINKUSDT", "SOLUSDT", "MATICUSDT"]

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
    if row:
        keyboard.append(row)
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
            else:
                logger.error(f"Lỗi HTTP ({e.code}): {e.reason}")

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

# ========== HÀM CACHE COIN – CHỈ BOTMANAGER GHI, BOT CHỈ ĐỌC ==========
def refresh_coins_cache():
    """Lấy và cập nhật danh sách coin USDT & USDC từ Binance Futures"""
    global _COINS_CACHE
    try:
        url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        data = binance_api_request(url)
        if not data:
            logger.error("❌ Không thể lấy exchangeInfo từ Binance")
            return False

        coins = []
        for symbol_info in data.get('symbols', []):
            symbol = symbol_info.get('symbol', '')
            quote = symbol_info.get('quoteAsset', '')
            if quote not in ('USDT', 'USDC'):
                continue
            if symbol_info.get('status') != 'TRADING':
                continue
            if symbol in _SYMBOL_BLACKLIST:
                continue

            max_leverage = 50
            for f in symbol_info.get('filters', []):
                if f['filterType'] == 'LEVERAGE' and 'maxLeverage' in f:
                    max_leverage = int(f['maxLeverage'])
                    break

            step_size = 0.001
            for f in symbol_info.get('filters', []):
                if f['filterType'] == 'LOT_SIZE':
                    step_size = float(f['stepSize'])
                    break

            coins.append({
                'symbol': symbol,
                'quote': quote,
                'max_leverage': max_leverage,
                'step_size': step_size,
                'price': 0.0,
                'volume': 0.0,
                'last_price_update': 0,
                'last_volume_update': 0
            })

        _COINS_CACHE["data"] = coins
        _COINS_CACHE["last_volume_update"] = time.time()
        logger.info(f"✅ Đã cập nhật cache {len(coins)} coin USDT/USDC")
        return True

    except Exception as e:
        logger.error(f"❌ Lỗi refresh cache coin: {str(e)}")
        logger.error(traceback.format_exc())
        return False

def update_coins_price():
    """Cập nhật giá cho tất cả coin trong cache"""
    global _COINS_CACHE
    try:
        url = "https://fapi.binance.com/fapi/v1/ticker/price"
        all_prices = binance_api_request(url)
        if not all_prices:
            return False

        price_dict = {item['symbol']: float(item['price']) for item in all_prices}
        updated = 0
        for coin in _COINS_CACHE["data"]:
            if coin['symbol'] in price_dict:
                coin['price'] = price_dict[coin['symbol']]
                coin['last_price_update'] = time.time()
                updated += 1

        _COINS_CACHE["last_price_update"] = time.time()
        logger.info(f"✅ Đã cập nhật giá cho {updated} coin")
        return True
    except Exception as e:
        logger.error(f"❌ Lỗi cập nhật giá: {str(e)}")
        return False

def update_coins_volume():
    """Cập nhật volume cho tất cả coin trong cache"""
    global _COINS_CACHE
    try:
        url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
        all_tickers = binance_api_request(url)
        if not all_tickers:
            return False

        volume_dict = {item['symbol']: float(item['volume']) for item in all_tickers}
        updated = 0
        for coin in _COINS_CACHE["data"]:
            if coin['symbol'] in volume_dict:
                coin['volume'] = volume_dict[coin['symbol']]
                coin['last_volume_update'] = time.time()
                updated += 1

        _COINS_CACHE["last_volume_update"] = time.time()
        logger.info(f"✅ Đã cập nhật volume cho {updated} coin")
        return True
    except Exception as e:
        logger.error(f"❌ Lỗi cập nhật volume: {str(e)}")
        return False

def get_coins_with_info():
    """Lấy danh sách coin từ cache – KHÔNG refresh, chỉ đọc"""
    global _COINS_CACHE
    return _COINS_CACHE["data"]

def get_max_leverage_from_cache(symbol):
    """Lấy đòn bẩy tối đa từ cache chung"""
    global _COINS_CACHE
    symbol = symbol.upper()
    for coin in _COINS_CACHE["data"]:
        if coin['symbol'] == symbol:
            return coin['max_leverage']
    logger.warning(f"⚠️ Không tìm thấy {symbol} trong cache, dùng mặc định 50x")
    return 50

def force_refresh_coin_cache():
    """Buộc làm mới toàn bộ cache coin (dùng cho Telegram)"""
    logger.info("🔄 Buộc làm mới cache coin...")
    if refresh_coins_cache():
        update_coins_volume()
        update_coins_price()
        return True
    return False

# ========== HÀM LỌC COIN – CÓ SẮP XẾP VOLUME GIẢM DẦN ==========
def filter_coins_for_side(side, excluded_coins=None, min_leverage=None):
    """
    Lọc coin theo chiến lược CÂN BẰNG.
    - BUY  : price < buy_price_threshold
    - SELL : price > sell_price_threshold
    - Đòn bẩy tối thiểu: min_leverage (nếu None thì lấy từ _BALANCE_CONFIG)
    - SẮP XẾP theo khối lượng giao dịch GIẢM DẦN (ưu tiên thanh khoản cao)
    - KHÔNG loại bỏ coin có volume = 0, nhưng xếp cuối.
    """
    all_coins = get_coins_with_info()
    filtered = []

    if not all_coins:
        logger.warning("❌ Cache coin trống!")
        return filtered

    # Sử dụng min_leverage từ tham số, nếu không có thì dùng config
    if min_leverage is None:
        min_leverage = _BALANCE_CONFIG["min_leverage"]

    logger.info(f"🔍 Lọc coin {side} | {len(all_coins)} coin trong cache")
    logger.info(f"⚙️ Ngưỡng: MUA < {_BALANCE_CONFIG['buy_price_threshold']} USDT/USDC, BÁN > {_BALANCE_CONFIG['sell_price_threshold']} USDT/USDC | Đòn bẩy tối thiểu: {min_leverage}x")
    logger.info(f"📊 SẮP XẾP: Theo khối lượng giảm dần (BẬT)")

    excluded_set = set(excluded_coins or [])
    blacklisted = 0
    excluded_cnt = 0
    lev_fail = 0
    price_fail = 0
    volume_zero = 0

    for coin in all_coins:
        sym = coin['symbol']
        if sym in _SYMBOL_BLACKLIST:
            blacklisted += 1
            continue
        if sym in excluded_set:
            excluded_cnt += 1
            continue
        if coin['max_leverage'] < min_leverage:
            lev_fail += 1
            continue
        if coin['price'] <= 0:
            price_fail += 1
            continue
        if coin['volume'] <= 0:
            volume_zero += 1   # vẫn giữ coin volume 0, nhưng sẽ xếp cuối

        if side == "BUY" and coin['price'] >= _BALANCE_CONFIG["buy_price_threshold"]:
            price_fail += 1
            continue
        if side == "SELL" and coin['price'] <= _BALANCE_CONFIG["sell_price_threshold"]:
            price_fail += 1
            continue

        filtered.append(coin)

    # SẮP XẾP THEO VOLUME GIẢM DẦN (coin volume 0 sẽ ở cuối)
    filtered.sort(key=lambda x: x['volume'], reverse=True)

    logger.info(f"📊 {side}: {len(filtered)} coin phù hợp (loại: blacklist={blacklisted}, excluded={excluded_cnt}, lev={lev_fail}, giá={price_fail}, volume0={volume_zero})")
    if filtered:
        # Log 5 coin đầu để tham khảo
        for i, c in enumerate(filtered[:5]):
            logger.info(f"  {i+1}. {c['symbol']} | giá: {c['price']:.4f} | volume: {c['volume']:.2f} | lev: {c['max_leverage']}x")

    return filtered

def update_balance_config(buy_price_threshold=None, sell_price_threshold=None, min_leverage=None, sort_by_volume=None):
    """Cập nhật cấu hình cân bằng lệnh"""
    global _BALANCE_CONFIG
    if buy_price_threshold is not None:
        _BALANCE_CONFIG["buy_price_threshold"] = buy_price_threshold
    if sell_price_threshold is not None:
        _BALANCE_CONFIG["sell_price_threshold"] = sell_price_threshold
    if min_leverage is not None:
        _BALANCE_CONFIG["min_leverage"] = min_leverage
    if sort_by_volume is not None:
        _BALANCE_CONFIG["sort_by_volume"] = sort_by_volume
    logger.info(f"✅ Cập nhật cấu hình cân bằng: {_BALANCE_CONFIG}")
    return _BALANCE_CONFIG

# ========== CÁC HÀM API BINANCE KHÁC ==========
def get_step_size(symbol, api_key, api_secret):
    if not symbol: return 0.001
    global _COINS_CACHE
    for coin in _COINS_CACHE["data"]:
        if coin['symbol'] == symbol.upper():
            return coin['step_size']
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
            if asset['asset'] in ('USDT', 'USDC'):
                available_balance = float(asset['availableBalance'])
                logger.info(f"💰 Số dư - Khả dụng: {available_balance:.2f} {asset['asset']}")
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

        logger.info(f"💰 Tổng số dư (USDT+USDC): {total_all:.2f}, Khả dụng: {available_all:.2f}")
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
            logger.warning(f"⚠️ Maint margin <= 0 (margin_balance={margin_balance:.4f}, maint_margin={maint_margin:.4f})")
            return margin_balance, maint_margin, None

        ratio = margin_balance / maint_margin
        logger.info(f"🛡️ An toàn ký quỹ: margin_balance={margin_balance:.4f}, maint_margin={maint_margin:.4f}, tỷ lệ={ratio:.2f}x")
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
        self.bot_leverage = 10   # sẽ được gán từ bot

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

    def find_best_coin_with_balance(self, excluded_coins=None):
        """
        Tìm coin tốt nhất với cơ chế cân bằng lệnh.
        - Hướng lấy từ GlobalSideCoordinator.
        - Lọc coin dựa trên ngưỡng giá + đòn bẩy tối thiểu (dùng bot_leverage).
        - SẮP XẾP theo khối lượng giảm dần (do filter_coins_for_side xử lý).
        """
        try:
            now = time.time()
            if now - self.last_scan_time < self.scan_cooldown:
                return None
            self.last_scan_time = now

            if not _COINS_CACHE["data"]:
                logger.warning("⚠️ Cache coin trống, không thể tìm coin.")
                return None

            if self._bot_manager and hasattr(self._bot_manager, 'global_side_coordinator'):
                target_side = self._bot_manager.global_side_coordinator.get_next_side(
                    self.api_key, self.api_secret
                )
            else:
                target_side = self.get_next_side_for_balance()

            logger.info(f"🎯 Hệ thống chọn hướng: {target_side} (đòn bẩy bot: {self.bot_leverage}x)")

            filtered_coins = filter_coins_for_side(
                target_side,
                excluded_coins,
                min_leverage=self.bot_leverage   # dùng đòn bẩy thực tế của bot
            )

            if not filtered_coins:
                if now - self.last_failed_search_log > 60:
                    logger.warning(f"⚠️ Không tìm thấy coin phù hợp cho hướng {target_side}")
                    self.last_failed_search_log = now
                return None

            # Duyệt tuần tự theo thứ tự đã sắp xếp (volume giảm dần)
            for coin in filtered_coins:
                symbol = coin['symbol']
                if self.has_existing_position(symbol):
                    continue
                if self._bot_manager and self._bot_manager.coin_manager.is_coin_active(symbol):
                    continue
                logger.info(f"✅ Tìm thấy coin {symbol} phù hợp ({target_side}) | volume: {coin['volume']:.2f}")
                return symbol

            logger.warning(f"⚠️ Đã duyệt {len(filtered_coins)} coin nhưng không có coin nào chưa có vị thế")
            return None

        except Exception as e:
            logger.error(f"❌ Lỗi tìm coin với cân bằng: {str(e)}")
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

# ========== LỚP BOT CỐT LÕI – ĐÃ SỬA LỖI QUEUE ==========
class BaseBot:
    def __init__(self, symbol, lev, percent, tp, sl, roi_trigger, ws_manager, api_key, api_secret,
                 telegram_bot_token, telegram_chat_id, strategy_name, config_key=None, bot_id=None,
                 coin_manager=None, symbol_locks=None, max_coins=1, bot_coordinator=None,
                 pyramiding_n=0, pyramiding_x=0, **kwargs):

        global _BALANCE_CONFIG

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
        self.coin_finder.bot_leverage = self.lev   # 👈 GÁN ĐÒN BẨY CỦA BOT

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
            'sell_price_threshold': kwargs.get('sell_price_threshold', 10.0),
            'min_leverage': _BALANCE_CONFIG["min_leverage"]
        }
        _BALANCE_CONFIG.update(self.balance_config)

        if symbol and not self.coin_finder.has_existing_position(symbol):
            self._add_symbol(symbol)

        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

        roi_info = f" | 🎯 ROI Kích hoạt: {roi_trigger}%" if roi_trigger else " | 🎯 ROI Kích hoạt: Tắt"
        pyramiding_info = f" | 🔄 Nhồi lệnh: {pyramiding_n} lần tại {pyramiding_x}%" if self.pyramiding_enabled else " | 🔄 Nhồi lệnh: Tắt"
        balance_info = (f" | ⚖️ Cân bằng lệnh: BẬT | "
                        f"Mua <{self.balance_config['buy_price_threshold']} USDT/USDC | "
                        f"Bán >{self.balance_config['sell_price_threshold']} USDT/USDC | "
                        f"Lev tối thiểu: {_BALANCE_CONFIG['min_leverage']}x | "
                        f"Sắp xếp: Volume giảm dần")

        self.log(f"🟢 Bot {strategy_name} đã khởi động | 1 coin | Đòn bẩy: {lev}x | Vốn: {percent}% | TP/SL: {tp}%/{sl}%{roi_info}{pyramiding_info}{balance_info}")

    def _run(self):
        """Vòng lặp chính – KHÔNG refresh cache, chỉ đọc"""
        last_coin_search_log = 0
        log_interval = 30
        last_no_coin_found_log = 0

        while not self._stop:
            try:
                current_time = time.time()

                # Kiểm tra margin safety
                if current_time - self.last_margin_safety_check > self.margin_safety_interval:
                    self.last_margin_safety_check = current_time
                    if self._check_margin_safety():
                        time.sleep(5)
                        continue

                # Cập nhật vị thế toàn cục
                if current_time - self.last_global_position_check > 30:
                    self.check_global_positions()
                    self.last_global_position_check = current_time

                # Nếu không có coin nào đang theo dõi -> tìm coin mới
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
                                excluded_coins=self.coin_manager.get_active_coins()
                            )

                        if found_coin:
                            self.bot_coordinator.bot_has_coin(self.bot_id)
                            self._add_symbol(found_coin)
                            # 👇 GIẢI PHÓNG QUYỀN TÌM COIN NGAY LẬP TỨC
                            self.bot_coordinator.finish_coin_search(self.bot_id, found_coin)
                            self.log(f"✅ Đã tìm thấy coin: {found_coin}, đang chờ vào lệnh...")
                            last_coin_search_log = 0
                        else:
                            # 👇 KHÔNG TÌM ĐƯỢC COIN, VẪN PHẢI GIẢI PHÓNG
                            self.bot_coordinator.finish_coin_search(self.bot_id)
                            if current_time - last_no_coin_found_log > 60:
                                self.log(f"❌ Không tìm thấy coin phù hợp")
                                last_no_coin_found_log = current_time
                    else:
                        queue_pos = self.bot_coordinator.get_queue_position(self.bot_id)
                        if queue_pos > 0:
                            queue_info = self.bot_coordinator.get_queue_info()
                            if current_time - last_coin_search_log > log_interval:
                                self.log(f"⏳ Đang chờ tìm coin (vị trí: {queue_pos}/{queue_info['queue_size'] + 1}) - Bot đang tìm: {queue_info['current_finding']}")
                                last_coin_search_log = current_time
                        time.sleep(2)

                    time.sleep(5)
                    continue

                # Xử lý từng symbol đang theo dõi
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
            if symbol not in self.symbol_data:
                return False
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

                    target_side = self.get_next_side_based_on_comprehensive_analysis()
                    logger.info(f"🎯 Hướng giao dịch cho {symbol}: {target_side}")

                    if not self.coin_finder.has_existing_position(symbol):
                        if self._open_symbol_position(symbol, target_side):
                            symbol_info['last_trade_time'] = current_time
                            return True
                return False
        except Exception as e:
            self.log(f"❌ Lỗi xử lý {symbol}: {str(e)}")
            return False

    # ---------- Các phương thức WebSocket và giá ----------
    def _add_symbol(self, symbol):
        """Thêm symbol vào theo dõi và WebSocket"""
        symbol = symbol.upper()
        if symbol in self.active_symbols:
            return
        self.active_symbols.append(symbol)
        self.symbol_data[symbol] = {
            'position_open': False,
            'entry': 0,
            'entry_base': 0,
            'side': None,
            'qty': 0,
            'status': 'waiting',
            'last_price': 0,
            'last_trade_time': 0,
            'last_close_time': 0,
            'last_position_check': 0,
            'pyramiding_count': 0,
            'next_pyramiding_roi': self.pyramiding_x if self.pyramiding_enabled else 0,
            'last_pyramiding_time': 0,
            'pyramiding_base_roi': 0.0,
            'high_water_mark_roi': 0,
            'roi_check_activated': False
        }
        self.ws_manager.add_symbol(symbol, lambda p, s=symbol: self._handle_price_update(s, p))
        self.coin_manager.register_coin(symbol)
        self.log(f"➕ Đã thêm {symbol} vào theo dõi")

    def _handle_price_update(self, symbol, price):
        """Xử lý cập nhật giá từ WebSocket"""
        if symbol not in self.symbol_data:
            return
        self.symbol_data[symbol]['last_price'] = price

    def get_current_price(self, symbol):
        """Lấy giá hiện tại từ cache hoặc API"""
        if symbol in self.symbol_data and self.symbol_data[symbol]['last_price'] > 0:
            return self.symbol_data[symbol]['last_price']
        return get_current_price(symbol)

    # ---------- Kiểm tra vị thế ----------
    def _check_symbol_position(self, symbol):
        """Kiểm tra xem có vị thế đang mở cho symbol này không"""
        try:
            positions = get_positions(symbol, self.api_key, self.api_secret)
            if not positions:
                return

            for pos in positions:
                if pos['symbol'] == symbol:
                    position_amt = float(pos.get('positionAmt', 0))
                    if abs(position_amt) > 0:
                        if not self.symbol_data[symbol]['position_open']:
                            entry_price = float(pos.get('entryPrice', 0))
                            self.symbol_data[symbol].update({
                                'position_open': True,
                                'entry': entry_price,
                                'entry_base': entry_price,
                                'qty': position_amt,
                                'side': 'BUY' if position_amt > 0 else 'SELL',
                                'status': 'open'
                            })
                            self.log(f"📌 Vị thế {symbol} đã mở từ Binance")
                        return
            # Nếu không có vị thế nào, đánh dấu là đã đóng
            if self.symbol_data[symbol]['position_open']:
                self._reset_symbol_position(symbol)
        except Exception as e:
            logger.error(f"Lỗi kiểm tra vị thế {symbol}: {str(e)}")

    def _reset_symbol_position(self, symbol):
        """Reset trạng thái vị thế sau khi đóng"""
        if symbol in self.symbol_data:
            self.symbol_data[symbol].update({
                'position_open': False,
                'entry': 0,
                'entry_base': 0,
                'side': None,
                'qty': 0,
                'status': 'closed',
                'pyramiding_count': 0,
                'next_pyramiding_roi': self.pyramiding_x if self.pyramiding_enabled else 0,
                'last_pyramiding_time': 0,
                'pyramiding_base_roi': 0.0,
                'high_water_mark_roi': 0,
                'roi_check_activated': False
            })
            self.symbol_data[symbol]['last_close_time'] = time.time()

    # ---------- Mở / Đóng lệnh ----------
    def _open_symbol_position(self, symbol, side):
        """Mở vị thế với side đã chọn"""
        try:
            if self.coin_finder.has_existing_position(symbol):
                self.log(f"⚠️ {symbol} - CÓ VỊ THẾ TRÊN BINANCE, BỎ QUA")
                self.stop_symbol(symbol)
                return False

            self._check_symbol_position(symbol)
            if self.symbol_data[symbol]['position_open']:
                return False

            max_leverage_from_cache = get_max_leverage_from_cache(symbol)
            if max_leverage_from_cache < self.lev:
                self.log(f"❌ {symbol} - Đòn bẩy không đủ: {max_leverage_from_cache}x < {self.lev}x (từ cache)")
                self.stop_symbol(symbol)
                # 👇 GIẢI PHÓNG NẾU KHÔNG CÒN COIN
                if not self.active_symbols:
                    self.bot_coordinator.bot_lost_coin(self.bot_id)
                    self.bot_coordinator.finish_coin_search(self.bot_id)
                return False

            if not set_leverage(symbol, self.lev, self.api_key, self.api_secret):
                self.log(f"❌ {symbol} - Không thể cài đặt đòn bẩy (Binance từ chối)")
                self.stop_symbol(symbol)
                if not self.active_symbols:
                    self.bot_coordinator.bot_lost_coin(self.bot_id)
                    self.bot_coordinator.finish_coin_search(self.bot_id)
                return False

            total_balance, available_balance = get_total_and_available_balance(
                self.api_key, self.api_secret
            )
            if total_balance is None or total_balance <= 0:
                self.log(f"❌ {symbol} - Không đủ tổng số dư")
                self.stop_symbol(symbol)
                if not self.active_symbols:
                    self.bot_coordinator.bot_lost_coin(self.bot_id)
                    self.bot_coordinator.finish_coin_search(self.bot_id)
                return False

            balance = total_balance
            required_usd = balance * (self.percent / 100)

            if available_balance is None or available_balance <= 0 or required_usd > available_balance:
                self.log(
                    f"❌ {symbol} - Không đủ số dư khả dụng:"
                    f" cần {required_usd:.2f}, khả dụng {available_balance or 0:.2f}"
                )
                self.stop_symbol(symbol)
                if not self.active_symbols:
                    self.bot_coordinator.bot_lost_coin(self.bot_id)
                    self.bot_coordinator.finish_coin_search(self.bot_id)
                return False

            current_price = self.get_current_price(symbol)
            if current_price <= 0:
                self.log(f"❌ {symbol} - Lỗi giá")
                self.stop_symbol(symbol)
                if not self.active_symbols:
                    self.bot_coordinator.bot_lost_coin(self.bot_id)
                    self.bot_coordinator.finish_coin_search(self.bot_id)
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
                if not self.active_symbols:
                    self.bot_coordinator.bot_lost_coin(self.bot_id)
                    self.bot_coordinator.finish_coin_search(self.bot_id)
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
                        if not self.active_symbols:
                            self.bot_coordinator.bot_lost_coin(self.bot_id)
                            self.bot_coordinator.finish_coin_search(self.bot_id)
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
                        'entry': avg_price,
                        'entry_base': avg_price,
                        'average_down_count': 0,
                        'side': side,
                        'qty': executed_qty if side == "BUY" else -executed_qty,
                        'position_open': True,
                        'status': "open",
                        'high_water_mark_roi': 0,
                        'roi_check_activated': False,
                        **pyramiding_info
                    })

                    self.bot_coordinator.bot_has_coin(self.bot_id)

                    message = (f"✅ <b>ĐÃ MỞ VỊ THẾ {symbol}</b>\n"
                               f"🤖 Bot: {self.bot_id}\n📌 Hướng: {side}\n"
                               f"🏷️ Entry: {avg_price:.4f}\n📊 Khối lượng: {executed_qty:.4f}\n"
                               f"💰 Đòn bẩy: {self.lev}x\n🎯 TP: {self.tp}% | 🛡️ SL: {self.sl}%")
                    if self.roi_trigger:
                        message += f" | 🎯 ROI Kích hoạt: {self.roi_trigger}%"
                    if self.pyramiding_enabled:
                        message += f" | 🔄 Nhồi lệnh: {self.pyramiding_n} lần tại {self.pyramiding_x}%"

                    self.log(message)
                    return True
                else:
                    self.log(f"❌ {symbol} - Lệnh chưa khớp")
                    self.stop_symbol(symbol)
                    if not self.active_symbols:
                        self.bot_coordinator.bot_lost_coin(self.bot_id)
                        self.bot_coordinator.finish_coin_search(self.bot_id)
                    return False
            else:
                error_msg = result.get('msg', 'Lỗi không xác định') if result else 'Không có phản hồi'
                self.log(f"❌ {symbol} - Lỗi lệnh: {error_msg}")
                self.stop_symbol(symbol)
                if not self.active_symbols:
                    self.bot_coordinator.bot_lost_coin(self.bot_id)
                    self.bot_coordinator.finish_coin_search(self.bot_id)
                return False

        except Exception as e:
            self.log(f"❌ {symbol} - Lỗi mở vị thế: {str(e)}")
            self.stop_symbol(symbol)
            if not self.active_symbols:
                self.bot_coordinator.bot_lost_coin(self.bot_id)
                self.bot_coordinator.finish_coin_search(self.bot_id)
            return False

    def _close_symbol_position(self, symbol, reason=""):
        """Đóng vị thế đang mở"""
        try:
            if symbol not in self.symbol_data:
                return False

            if not self.symbol_data[symbol]['position_open']:
                return False

            side = self.symbol_data[symbol]['side']
            qty = abs(self.symbol_data[symbol]['qty'])
            close_side = "SELL" if side == "BUY" else "BUY"

            cancel_all_orders(symbol, self.api_key, self.api_secret)
            time.sleep(1)

            result = place_order(symbol, close_side, qty, self.api_key, self.api_secret)
            if result and 'orderId' in result:
                self.log(f"🔴 Đã đóng vị thế {symbol} {reason}")
                time.sleep(1)
                self._reset_symbol_position(symbol)
                # ❌ KHÔNG gọi bot_lost_coin ở đây (vì bot vẫn còn coin để chờ tín hiệu mới)
                # self.bot_coordinator.bot_lost_coin(self.bot_id)

                if self.find_new_bot_after_close and not self.symbol:
                    self.status = "searching"
                return True
            else:
                self.log(f"❌ Đóng lệnh {symbol} thất bại")
                return False

        except Exception as e:
            self.log(f"❌ Lỗi đóng vị thế {symbol}: {str(e)}")
            return False

    # ---------- Kiểm tra an toàn ký quỹ ----------
    def _check_margin_safety(self):
        try:
            margin_balance, maint_margin, ratio = get_margin_safety_info(self.api_key, self.api_secret)
            if ratio is not None and ratio < self.margin_safety_threshold:
                self.log(f"🚫 CẢNH BÁO AN TOÀN KÝ QUỸ: tỷ lệ {ratio:.2f}x < {self.margin_safety_threshold}x")
                self.log("⛔ Đóng tất cả vị thế do margin thấp")
                for symbol in self.active_symbols.copy():
                    self._close_symbol_position(symbol, reason="(Margin safety)")
                return True
            return False
        except Exception as e:
            logger.error(f"Lỗi kiểm tra margin safety: {str(e)}")
            return False

    # ---------- Kiểm tra TP/SL ----------
    def _check_symbol_tp_sl(self, symbol):
        if symbol not in self.symbol_data:
            return
        data = self.symbol_data[symbol]
        if not data['position_open']:
            return

        entry = data['entry']
        current_price = self.get_current_price(symbol)
        if current_price <= 0:
            return

        if data['side'] == 'BUY':
            roi = (current_price - entry) / entry * 100
        else:
            roi = (entry - current_price) / entry * 100

        if roi > data['high_water_mark_roi']:
            data['high_water_mark_roi'] = roi

        if self.tp and roi >= self.tp:
            self._close_symbol_position(symbol, reason=f"(TP {self.tp}%)")
            return
        if self.sl and roi <= -self.sl:
            self._close_symbol_position(symbol, reason=f"(SL {self.sl}%)")
            return

    # ---------- Nhồi lệnh (Pyramiding) ----------
    def _check_pyramiding(self, symbol):
        if not self.pyramiding_enabled:
            return
        if symbol not in self.symbol_data:
            return
        data = self.symbol_data[symbol]
        if not data['position_open']:
            return
        if data['pyramiding_count'] >= self.pyramiding_n:
            return

        entry = data['entry_base']
        current_price = self.get_current_price(symbol)
        if current_price <= 0:
            return

        if data['side'] == 'BUY':
            roi = (current_price - entry) / entry * 100
        else:
            roi = (entry - current_price) / entry * 100

        next_roi = data['next_pyramiding_roi']
        if roi >= next_roi:
            self._pyramid_order(symbol, data['side'])
            data['pyramiding_count'] += 1
            data['next_pyramiding_roi'] = next_roi + self.pyramiding_x
            data['last_pyramiding_time'] = time.time()
            self.log(f"🔄 Nhồi lệnh {symbol} lần {data['pyramiding_count']} tại ROI {roi:.2f}%")

    def _pyramid_order(self, symbol, side):
        try:
            total_balance, available_balance = get_total_and_available_balance(self.api_key, self.api_secret)
            if total_balance is None or total_balance <= 0:
                return

            current_price = self.get_current_price(symbol)
            if current_price <= 0:
                return

            step_size = get_step_size(symbol, self.api_key, self.api_secret)
            usd_amount = total_balance * (self.percent / 100)
            qty = (usd_amount * self.lev) / current_price
            if step_size > 0:
                qty = math.floor(qty / step_size) * step_size
                qty = round(qty, 8)

            if qty <= 0 or qty < step_size:
                return

            result = place_order(symbol, side, qty, self.api_key, self.api_secret)
            if result and 'orderId' in result:
                executed_qty = float(result.get('executedQty', 0))
                avg_price = float(result.get('avgPrice', current_price))

                old_qty = self.symbol_data[symbol]['qty']
                old_entry = self.symbol_data[symbol]['entry']

                new_qty = old_qty + (executed_qty if side == "BUY" else -executed_qty)
                new_entry = (old_entry * abs(old_qty) + avg_price * executed_qty) / (abs(old_qty) + executed_qty)

                self.symbol_data[symbol].update({
                    'qty': new_qty,
                    'entry': new_entry,
                })
                self.log(f"➕ Đã nhồi thêm {executed_qty} {symbol} giá {avg_price}")
        except Exception as e:
            self.log(f"❌ Lỗi nhồi lệnh {symbol}: {str(e)}")

    # ---------- Thoát thông minh ----------
    def _check_smart_exit_condition(self, symbol):
        if not self.roi_trigger:
            return False
        if symbol not in self.symbol_data:
            return False
        data = self.symbol_data[symbol]
        if not data['position_open']:
            return False

        entry = data['entry_base']
        current_price = self.get_current_price(symbol)
        if current_price <= 0:
            return False

        if data['side'] == 'BUY':
            roi = (current_price - entry) / entry * 100
        else:
            roi = (entry - current_price) / entry * 100

        if roi >= self.roi_trigger and not data['roi_check_activated']:
            data['roi_check_activated'] = True
            self.log(f"🎯 ROI đạt {roi:.2f}% - Kích hoạt chốt lời sớm")

        if data['roi_check_activated'] and roi < data['high_water_mark_roi'] * 0.9:
            self._close_symbol_position(symbol, reason=f"(Smart exit - ROI từ {data['high_water_mark_roi']:.2f}% giảm còn {roi:.2f}%)")
            return True
        return False

    # ---------- Kiểm tra toàn cục ----------
    def check_global_positions(self):
        if hasattr(self, '_bot_manager') and self._bot_manager and hasattr(self._bot_manager, 'global_side_coordinator'):
            self.next_global_side = self._bot_manager.global_side_coordinator.get_next_side(
                self.api_key, self.api_secret
            )

    def get_next_side_based_on_comprehensive_analysis(self):
        if hasattr(self, '_bot_manager') and self._bot_manager and hasattr(self._bot_manager, 'global_side_coordinator'):
            return self._bot_manager.global_side_coordinator.get_next_side(self.api_key, self.api_secret)
        else:
            long_count = 0
            short_count = 0
            for sym, data in self.symbol_data.items():
                if data.get('position_open'):
                    if data['side'] == 'BUY':
                        long_count += 1
                    else:
                        short_count += 1
            if long_count > short_count:
                return "SELL"
            elif short_count > long_count:
                return "BUY"
            else:
                return random.choice(["BUY", "SELL"])

    # ---------- Dừng và dọn dẹp ----------
    def stop_symbol(self, symbol):
        if symbol not in self.active_symbols:
            return False
        self.log(f"⛔ Đang dừng coin {symbol}...")
        if self.symbol_data[symbol]['position_open']:
            self._close_symbol_position(symbol, reason="(Stop by user)")
        self.ws_manager.remove_symbol(symbol)
        self.active_symbols.remove(symbol)
        self.coin_manager.unregister_coin(symbol)
        self.log(f"✅ Đã dừng coin {symbol}")

        # 👇 NẾU KHÔNG CÒN COIN NÀO, GIẢI PHÓNG COORDINATOR
        if not self.active_symbols:
            self.bot_coordinator.bot_lost_coin(self.bot_id)
            self.bot_coordinator.finish_coin_search(self.bot_id)
            self.status = "searching"
        return True

    def stop_all_symbols(self):
        count = 0
        for symbol in self.active_symbols.copy():
            if self.stop_symbol(symbol):
                count += 1
        return count

    def stop(self):
        self.log("🔴 Bot đang dừng...")
        self._stop = True
        self.stop_all_symbols()
        self.log("✅ Bot đã dừng")

    def log(self, message):
        logger.info(f"[{self.bot_id}] {message}")
        if self.telegram_bot_token and self.telegram_chat_id:
            send_telegram(f"<b>{self.bot_id}</b>: {message}",
                         chat_id=self.telegram_chat_id,
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


# ========== LỚP QUẢN LÝ BOT – CHỨA CACHE UPDATER ==========
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
            self.log("🟢 HỆ THỐNG BOT CÂN BẰNG LỆNH (USDT/USDC) ĐÃ KHỞI ĐỘNG")
            self._initialize_cache()
            self._cache_thread = threading.Thread(target=self._cache_updater, daemon=True)
            self._cache_thread.start()
            self.telegram_thread = threading.Thread(target=self._telegram_listener, daemon=True)
            self.telegram_thread.start()
            if self.telegram_chat_id:
                self.send_main_menu(self.telegram_chat_id)
        else:
            self.log("⚡ BotManager đã khởi động ở chế độ không cấu hình")

    def _initialize_cache(self):
        """Khởi tạo cache lần đầu khi hệ thống start"""
        logger.info("🔄 Hệ thống đang khởi tạo cache...")
        if refresh_coins_cache():
            update_coins_volume()
            update_coins_price()
            coins_count = len(_COINS_CACHE.get("data", []))
            logger.info(f"✅ Hệ thống đã khởi tạo cache {coins_count} coin")
        else:
            logger.error("❌ Hệ thống không thể khởi tạo cache")

    def _cache_updater(self):
        """Thread tự động làm mới cache định kỳ"""
        while self.running:
            try:
                time.sleep(_CACHE_REFRESH_INTERVAL)
                logger.info("🔄 Tự động làm mới cache...")
                refresh_coins_cache()
                update_coins_volume()
                update_coins_price()
            except Exception as e:
                logger.error(f"❌ Lỗi làm mới cache tự động: {str(e)}")

    def _verify_api_connection(self):
        try:
            balance = get_balance(self.api_key, self.api_secret)
            if balance is None:
                self.log("❌ LỖI: Không thể kết nối đến API Binance. Kiểm tra API Key/Secret, VPN, internet.")
                return False
            else:
                self.log(f"✅ Kết nối Binance thành công! Số dư: {balance:.2f} USDT/USDC")
                return True
        except Exception as e:
            self.log(f"❌ Lỗi kiểm tra kết nối: {str(e)}")
            return False

    def get_position_summary(self):
        """Tạo báo cáo tổng kết hệ thống"""
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
                if has_coin:
                    total_bots_with_coins += 1
                if is_trading:
                    trading_bots += 1
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

            summary = "📊 **THỐNG KÊ CHI TIẾT - HỆ THỐNG CÂN BẰNG (USDT/USDC)**\n\n"

            cache_info = _COINS_CACHE
            coins_in_cache = len(cache_info.get("data", []))
            last_update = cache_info.get("last_price_update", 0)
            update_time = time.ctime(last_update) if last_update > 0 else "Chưa cập nhật"

            summary += f"🗂️ **CACHE HỆ THỐNG**: {coins_in_cache} coin | Cập nhật: {update_time}\n"
            summary += f"⚖️ **BOT CÂN BẰNG**: {balance_bots}/{len(self.bots)} bot\n"
            summary += f"📊 **SẮP XẾP COIN**: Theo khối lượng giảm dần (BẬT)\n\n"

            balance = get_balance(self.api_key, self.api_secret)
            if balance is not None:
                summary += f"💰 **SỐ DƯ**: {balance:.2f} USDT/USDC\n"
                summary += f"📈 **Tổng PnL**: {total_unrealized_pnl:.2f} USDT/USDC\n\n"
            else:
                summary += f"💰 **SỐ DƯ**: ❌ Lỗi kết nối\n\n"

            summary += f"🤖 **SỐ BOT HỆ THỐNG**: {len(self.bots)} bot | {total_bots_with_coins} bot có coin | {trading_bots} bot đang giao dịch\n\n"
            summary += f"📈 **PHÂN TÍCH PnL VÀ KHỐI LƯỢNG**:\n"
            summary += f"   📊 Số lượng: LONG={total_long_count} | SHORT={total_short_count}\n"
            summary += f"   💰 PnL: LONG={total_long_pnl:.2f} | SHORT={total_short_pnl:.2f}\n"
            summary += f"   ⚖️ Chênh lệch: {abs(total_long_pnl - total_short_pnl):.2f}\n\n"

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
                            if side:
                                summary += f" | {side} {abs(qty):.4f}"
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
            "🤖 <b>BOT GIAO DỊCH FUTURES - CHIẾN LƯỢC CÂN BẰNG LỆNH (USDT/USDC)</b>\n\n"
            "🎯 <b>CƠ CHẾ HOẠT ĐỘNG:</b>\n"
            "• Đếm số lượng lệnh BUY/SELL hiện có trên Binance\n"
            "• Nhiều lệnh BUY hơn → tìm lệnh SELL\n"
            "• Nhiều lệnh SELL hơn → tìm lệnh BUY\n"
            "• Bằng nhau → chọn ngẫu nhiên\n\n"
            "📊 <b>LỰA CHỌN COIN:</b>\n"
            "• MUA: chọn coin có giá < 1 USDT/USDC\n"
            "• BÁN: chọn coin có giá > 10 USDT/USDC\n"
            "• Yêu cầu đòn bẩy tối thiểu: 10x\n"
            "• SẮP XẾP theo khối lượng giao dịch GIẢM DẦN (ưu tiên thanh khoản cao)\n"
            "• Loại trừ coin đã có vị thế / đang theo dõi\n"
            "• Loại trừ BTCUSDT, ETHUSDT, BTCUSDC, ETHUSDC\n\n"
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
                     bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)

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
        sell_price_threshold = kwargs.get('sell_price_threshold', 10.0)

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
                    coin_manager=self.coin_manager, symbol_locks=self.symbol_locks,
                    bot_coordinator=self.bot_coordinator, bot_id=bot_id, max_coins=1,
                    pyramiding_n=pyramiding_n, pyramiding_x=pyramiding_x,
                    enable_balance_orders=enable_balance_orders,
                    buy_price_threshold=buy_price_threshold,
                    sell_price_threshold=sell_price_threshold
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
                                f"• Mua: giá < {buy_price_threshold} USDT/USDC\n"
                                f"• Bán: giá > {sell_price_threshold} USDT/USDC\n"
                                f"• Đòn bẩy tối thiểu: {_BALANCE_CONFIG['min_leverage']}x\n"
                                f"• SẮP XẾP: Theo khối lượng giảm dần\n")

            success_msg = (f"✅ <b>ĐÃ TẠO {created_count} BOT CÂN BẰNG</b>\n\n"
                           f"🎯 Chiến lược: {strategy_type}\n💰 Đòn bẩy: {lev}x\n"
                           f"📈 % Số dư: {percent}%\n🎯 TP: {tp}%\n"
                           f"🛡️ SL: {sl if sl is not None else 'Tắt'}%{roi_info}{pyramiding_info}\n"
                           f"🔧 Chế độ: {bot_mode}\n🔢 Số bot: {created_count}\n")
            if bot_mode == 'static' and symbol:
                success_msg += f"🔗 Coin ban đầu: {symbol}\n"
            else:
                success_msg += f"🔗 Coin: Tự động tìm (USDT/USDC) - sắp xếp theo volume\n"
            success_msg += balance_info
            success_msg += (f"\n🔄 <b>CƠ CHẾ CÂN BẰNG ĐƯỢC KÍCH HOẠT</b>\n"
                           f"• Đếm số lượng lệnh BUY/SELL hiện có\n"
                           f"• Ưu tiên hướng ngược lại khi mất cân bằng\n"
                           f"• Lọc coin theo ngưỡng giá (MUA <{buy_price_threshold}, BÁN >{sell_price_threshold})\n"
                           f"• Yêu cầu đòn bẩy tối thiểu: {_BALANCE_CONFIG['min_leverage']}x\n"
                           f"• SẮP XẾP coin theo khối lượng giảm dần\n\n")
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

    # ----- Các phương thức dừng coin, bot... -----
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

    # ----- Telegram listener (hoàn chỉnh với đầy đủ các bước tạo bot) -----
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
        """Xử lý tin nhắn Telegram – đã hoàn thiện đầy đủ các bước tạo bot."""
        user_state = self.user_states.get(chat_id, {})
        current_step = user_state.get('step')

        # ----- XỬ LÝ LỆNH HỦY -----
        if text == "❌ Hủy bỏ":
            self.user_states[chat_id] = {}
            send_telegram("❌ Đã hủy thao tác.", chat_id=chat_id, reply_markup=create_main_menu(),
                         bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            return

        # ----- MENU CHÍNH -----
        if text == "📊 Danh sách Bot":
            if not self.bots:
                send_telegram("🤖 Hiện không có bot nào đang chạy.", chat_id=chat_id,
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                bot_list = "\n".join([f"• {bid} - {'🟢' if b.status == 'running' else '🔴'}" for bid, b in self.bots.items()])
                send_telegram(f"📋 Danh sách Bot:\n{bot_list}", chat_id=chat_id,
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            return

        elif text == "📊 Thống kê":
            summary = self.get_position_summary()
            send_telegram(summary, chat_id=chat_id,
                         bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            return

        elif text == "➕ Thêm Bot":
            self.user_states[chat_id] = {'step': 'waiting_bot_mode'}
            send_telegram("🤖 Chọn chế độ bot:", chat_id=chat_id, reply_markup=create_bot_mode_keyboard(),
                         bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            return

        elif text == "⛔ Dừng Bot":
            if not self.bots:
                send_telegram("🤖 Hiện không có bot nào để dừng.", chat_id=chat_id,
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                keyboard = {"keyboard": [[{"text": bid}] for bid in self.bots.keys()] + [[{"text": "❌ Hủy bỏ"}]],
                           "resize_keyboard": True, "one_time_keyboard": True}
                self.user_states[chat_id] = {'step': 'waiting_stop_bot'}
                send_telegram("⛔ Chọn bot cần dừng:", chat_id=chat_id, reply_markup=keyboard,
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            return

        elif text == "⛔ Quản lý Coin":
            keyboard = self.get_coin_management_keyboard()
            if keyboard:
                self.user_states[chat_id] = {'step': 'waiting_stop_coin'}
                send_telegram("⛔ Chọn coin cần dừng:", chat_id=chat_id, reply_markup=keyboard,
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                send_telegram("📭 Không có coin nào đang được theo dõi.", chat_id=chat_id,
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            return

        elif text == "📈 Vị thế":
            positions = get_positions(api_key=self.api_key, api_secret=self.api_secret)
            if not positions:
                send_telegram("📭 Không có vị thế nào đang mở.", chat_id=chat_id,
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                msg = "📈 **VỊ THẾ ĐANG MỞ**\n\n"
                for pos in positions:
                    amt = float(pos.get('positionAmt', 0))
                    if amt != 0:
                        symbol = pos['symbol']
                        entry = float(pos.get('entryPrice', 0))
                        pnl = float(pos.get('unRealizedProfit', 0))
                        side = "LONG" if amt > 0 else "SHORT"
                        msg += f"{symbol} | {side} | Entry: {entry:.4f} | PnL: {pnl:.2f}\n"
                send_telegram(msg, chat_id=chat_id,
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            return

        elif text == "💰 Số dư":
            total, available = get_total_and_available_balance(self.api_key, self.api_secret)
            if total is not None:
                msg = f"💰 **SỐ DƯ**\nTổng: {total:.2f} USDT/USDC\nKhả dụng: {available:.2f} USDT/USDC"
            else:
                msg = "❌ Không thể lấy số dư"
            send_telegram(msg, chat_id=chat_id,
                         bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            return

        elif text == "⚙️ Cấu hình":
            send_telegram("⚙️ Tính năng đang phát triển.", chat_id=chat_id,
                         bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            return

        elif text == "🎯 Chiến lược":
            sort_status = "BẬT (volume giảm dần)" if _BALANCE_CONFIG.get('sort_by_volume', True) else "TẮT"
            send_telegram(f"🎯 Chiến lược hiện tại: Cân bằng lệnh (BUY <{_BALANCE_CONFIG['buy_price_threshold']}, SELL >{_BALANCE_CONFIG['sell_price_threshold']}).\n"
                         f"📊 Sắp xếp coin: {sort_status}\n"
                         f"Dùng /balance để cấu hình.",
                         chat_id=chat_id, bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            return

        elif text == "⚖️ Cân bằng lệnh":
            self.user_states[chat_id] = {'step': 'waiting_balance_config'}
            send_telegram("⚖️ <b>CẤU HÌNH CÂN BẰNG LỆNH</b>\n\nChọn hành động:",
                         chat_id=chat_id, reply_markup=create_balance_config_keyboard(),
                         bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            return

        # ----- XỬ LÝ CÁC BƯỚC TẠO BOT -----
        # 1. CHỌN CHẾ ĐỘ BOT
        if current_step == 'waiting_bot_mode':
            if text == "🤖 Bot Tĩnh - Coin cụ thể":
                user_state['bot_mode'] = 'static'
                user_state['step'] = 'waiting_symbol'
                send_telegram("🔗 Nhập tên coin (ví dụ: BTCUSDT) hoặc chọn từ danh sách:",
                             chat_id=chat_id, reply_markup=create_symbols_keyboard(),
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            elif text == "🔄 Bot Động - Tự tìm coin":
                user_state['bot_mode'] = 'dynamic'
                user_state['step'] = 'waiting_leverage'
                send_telegram("⚙️ Chọn đòn bẩy:", chat_id=chat_id, reply_markup=create_leverage_keyboard(),
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                send_telegram("⚠️ Vui lòng chọn chế độ bot hợp lệ.", chat_id=chat_id,
                             reply_markup=create_bot_mode_keyboard(),
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            return

        # 2. NHẬP SYMBOL (chỉ với bot tĩnh)
        if current_step == 'waiting_symbol':
            # Nếu là phím tắt từ keyboard thì text là tên coin, không cần xử lý thêm
            symbol = text.upper().replace(' ', '')
            # Kiểm tra sơ bộ (có thể bỏ qua nếu muốn)
            if not symbol.endswith('USDT') and not symbol.endswith('USDC'):
                send_telegram("⚠️ Coin phải kết thúc bằng USDT hoặc USDC. Vui lòng nhập lại:",
                             chat_id=chat_id, reply_markup=create_symbols_keyboard(),
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                return
            user_state['symbol'] = symbol
            user_state['step'] = 'waiting_leverage'
            send_telegram(f"✅ Đã chọn {symbol}\n⚙️ Chọn đòn bẩy:", chat_id=chat_id,
                         reply_markup=create_leverage_keyboard(),
                         bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            return

        # 3. NHẬP ĐÒN BẨY
        if current_step == 'waiting_leverage':
            try:
                lev = int(text.replace('x', ''))
                if lev < 1 or lev > 125:
                    raise ValueError
                user_state['leverage'] = lev
                user_state['step'] = 'waiting_percent'
                send_telegram(f"✅ Đòn bẩy: {lev}x\n📊 Nhập % số dư cho mỗi lệnh:",
                             chat_id=chat_id, reply_markup=create_percent_keyboard(),
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            except ValueError:
                send_telegram("⚠️ Vui lòng chọn đòn bẩy hợp lệ (1-125).", chat_id=chat_id,
                             reply_markup=create_leverage_keyboard(),
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            return

        # 4. NHẬP % SỐ DƯ
        if current_step == 'waiting_percent':
            try:
                percent = float(text)
                if percent <= 0 or percent > 100:
                    raise ValueError
                user_state['percent'] = percent
                user_state['step'] = 'waiting_tp'
                send_telegram(f"✅ Dùng {percent}% số dư mỗi lệnh.\n🎯 Nhập Take Profit (%):",
                             chat_id=chat_id, reply_markup=create_tp_keyboard(),
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            except ValueError:
                send_telegram("⚠️ Vui lòng nhập số từ 1 đến 100.", chat_id=chat_id,
                             reply_markup=create_percent_keyboard(),
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            return

        # 5. NHẬP TAKE PROFIT
        if current_step == 'waiting_tp':
            try:
                tp = float(text)
                if tp < 0:
                    raise ValueError
                user_state['tp'] = tp
                user_state['step'] = 'waiting_sl'
                send_telegram(f"✅ TP: {tp}%\n🛡️ Nhập Stop Loss % (0 để tắt):",
                             chat_id=chat_id, reply_markup=create_sl_keyboard(),
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            except ValueError:
                send_telegram("⚠️ Vui lòng nhập số >= 0.", chat_id=chat_id,
                             reply_markup=create_tp_keyboard(),
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            return

        # 6. NHẬP STOP LOSS
        if current_step == 'waiting_sl':
            try:
                sl = float(text)
                if sl < 0:
                    raise ValueError
                user_state['sl'] = sl
                user_state['step'] = 'waiting_roi_trigger'
                send_telegram(f"✅ SL: {sl}%\n🎯 Nhập ROI kích hoạt chốt lời sớm % (hoặc '❌ Tắt tính năng'):",
                             chat_id=chat_id, reply_markup=create_roi_trigger_keyboard(),
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            except ValueError:
                send_telegram("⚠️ Vui lòng nhập số >= 0.", chat_id=chat_id,
                             reply_markup=create_sl_keyboard(),
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            return

        # 7. NHẬP ROI TRIGGER
        if current_step == 'waiting_roi_trigger':
            if text == "❌ Tắt tính năng":
                roi_trigger = 0
            else:
                try:
                    roi_trigger = float(text)
                    if roi_trigger < 0:
                        raise ValueError
                except ValueError:
                    send_telegram("⚠️ Vui lòng nhập số >= 0 hoặc chọn '❌ Tắt tính năng'.", chat_id=chat_id,
                                 reply_markup=create_roi_trigger_keyboard(),
                                 bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                    return
            user_state['roi_trigger'] = roi_trigger
            user_state['step'] = 'waiting_pyramiding_n'
            send_telegram(f"✅ ROI kích hoạt: {'Tắt' if roi_trigger==0 else f'{roi_trigger}%'}\n🔄 Nhập số lần nhồi lệnh tối đa (0 để tắt):",
                         chat_id=chat_id, reply_markup=create_pyramiding_n_keyboard(),
                         bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            return

        # 8. NHẬP PYRAMIDING N
        if current_step == 'waiting_pyramiding_n':
            if text == "❌ Tắt tính năng":
                pyramiding_n = 0
            else:
                try:
                    pyramiding_n = int(text)
                    if pyramiding_n < 0:
                        raise ValueError
                except ValueError:
                    send_telegram("⚠️ Vui lòng nhập số >= 0 hoặc chọn '❌ Tắt tính năng'.", chat_id=chat_id,
                                 reply_markup=create_pyramiding_n_keyboard(),
                                 bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                    return
            user_state['pyramiding_n'] = pyramiding_n
            if pyramiding_n > 0:
                user_state['step'] = 'waiting_pyramiding_x'
                send_telegram(f"✅ Số lần nhồi: {pyramiding_n}\n🔄 Nhập mốc ROI % cho mỗi lần nhồi:",
                             chat_id=chat_id, reply_markup=create_pyramiding_x_keyboard(),
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                # Nếu tắt pyramiding, chuyển thẳng sang bước chọn số bot
                user_state['pyramiding_x'] = 0
                user_state['step'] = 'waiting_bot_count'
                send_telegram("🔢 Nhập số bot cần tạo (1,3,5,10,20):",
                             chat_id=chat_id, reply_markup=create_bot_count_keyboard(),
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            return

        # 9. NHẬP PYRAMIDING X
        if current_step == 'waiting_pyramiding_x':
            try:
                pyramiding_x = float(text)
                if pyramiding_x <= 0:
                    raise ValueError
                user_state['pyramiding_x'] = pyramiding_x
                user_state['step'] = 'waiting_bot_count'
                send_telegram(f"✅ Mốc nhồi: {pyramiding_x}%\n🔢 Nhập số bot cần tạo (1,3,5,10,20):",
                             chat_id=chat_id, reply_markup=create_bot_count_keyboard(),
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            except ValueError:
                send_telegram("⚠️ Vui lòng nhập số > 0.", chat_id=chat_id,
                             reply_markup=create_pyramiding_x_keyboard(),
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            return

        # 10. NHẬP SỐ BOT
        if current_step == 'waiting_bot_count':
            try:
                bot_count = int(text)
                if bot_count not in (1,3,5,10,20):
                    raise ValueError
                user_state['bot_count'] = bot_count
                user_state['step'] = 'waiting_buy_threshold'
                # Mặc định luôn bật cân bằng lệnh, chỉ hỏi ngưỡng
                user_state['enable_balance_orders'] = True
                send_telegram(f"✅ Số bot: {bot_count}\n⚖️ Nhập ngưỡng giá MUA (< ? USDT/USDC):",
                             chat_id=chat_id, reply_markup=create_price_threshold_keyboard(),
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            except ValueError:
                send_telegram("⚠️ Vui lòng chọn 1,3,5,10 hoặc 20.", chat_id=chat_id,
                             reply_markup=create_bot_count_keyboard(),
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            return

        # 11. NHẬP NGƯỠNG GIÁ MUA
        if current_step == 'waiting_buy_threshold':
            try:
                buy_threshold = float(text)
                if buy_threshold <= 0:
                    raise ValueError
                user_state['buy_price_threshold'] = buy_threshold
                user_state['step'] = 'waiting_sell_threshold'
                send_telegram(f"✅ Ngưỡng MUA: < {buy_threshold}\n⚖️ Nhập ngưỡng giá BÁN (> ? USDT/USDC):",
                             chat_id=chat_id, reply_markup=create_price_threshold_keyboard(),
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            except ValueError:
                send_telegram("⚠️ Vui lòng nhập số > 0.", chat_id=chat_id,
                             reply_markup=create_price_threshold_keyboard(),
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            return

        # 12. NHẬP NGƯỠNG GIÁ BÁN -> KẾT THÚC, TẠO BOT
        if current_step == 'waiting_sell_threshold':
            try:
                sell_threshold = float(text)
                if sell_threshold <= 0:
                    raise ValueError
                user_state['sell_price_threshold'] = sell_threshold
                # Cập nhật cấu hình toàn cục (tùy chọn, nhưng bot sẽ dùng giá trị riêng)
                update_balance_config(
                    buy_price_threshold=user_state['buy_price_threshold'],
                    sell_price_threshold=user_state['sell_price_threshold'],
                    sort_by_volume=True
                )
                self._finish_bot_creation(chat_id, user_state)
            except ValueError:
                send_telegram("⚠️ Vui lòng nhập số > 0.", chat_id=chat_id,
                             reply_markup=create_price_threshold_keyboard(),
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            return

        # ----- XỬ LÝ CÁC MENU PHỤ KHÁC -----
        if current_step == 'waiting_balance_config':
            if text == '⚖️ Bật cân bằng lệnh':
                updated = 0
                for bot in self.bots.values():
                    if hasattr(bot, 'enable_balance_orders'):
                        bot.enable_balance_orders = True
                        updated += 1
                send_telegram(f"✅ Đã BẬT cân bằng lệnh cho {updated} bot.", chat_id=chat_id,
                             reply_markup=create_main_menu(),
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                self.user_states[chat_id] = {}
            elif text == '⚖️ Tắt cân bằng lệnh':
                updated = 0
                for bot in self.bots.values():
                    if hasattr(bot, 'enable_balance_orders'):
                        bot.enable_balance_orders = False
                        updated += 1
                send_telegram(f"✅ Đã TẮT cân bằng lệnh cho {updated} bot.", chat_id=chat_id,
                             reply_markup=create_main_menu(),
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                self.user_states[chat_id] = {}
            elif text == '📊 Xem cấu hình cân bằng':
                sort_status = "BẬT (volume giảm dần)" if _BALANCE_CONFIG.get('sort_by_volume', True) else "TẮT"
                config_info = (
                    f"⚖️ <b>CẤU HÌNH CÂN BẰNG HIỆN TẠI</b>\n\n"
                    f"• Ngưỡng giá MUA: < {_BALANCE_CONFIG['buy_price_threshold']} USDT/USDC\n"
                    f"• Ngưỡng giá BÁN: > {_BALANCE_CONFIG['sell_price_threshold']} USDT/USDC\n"
                    f"• Đòn bẩy tối thiểu: {_BALANCE_CONFIG['min_leverage']}x\n"
                    f"• Sắp xếp coin: {sort_status}\n\n"
                    f"🔄 <b>CACHE HỆ THỐNG</b>\n"
                    f"• Số coin: {len(_COINS_CACHE.get('data', []))}\n"
                    f"• Cập nhật giá: {time.ctime(_COINS_CACHE.get('last_price_update', 0))}\n"
                    f"• Cập nhật volume: {time.ctime(_COINS_CACHE.get('last_volume_update', 0))}"
                )
                send_telegram(config_info, chat_id=chat_id,
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            elif text == '🔄 Làm mới cache':
                if force_refresh_coin_cache():
                    send_telegram("✅ Đã làm mới cache coin thành công", chat_id=chat_id,
                                 bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                else:
                    send_telegram("❌ Không thể làm mới cache", chat_id=chat_id,
                                 bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            return

        # ----- XỬ LÝ DỪNG BOT -----
        if current_step == 'waiting_stop_bot':
            if text in self.bots:
                if self.stop_bot(text):
                    send_telegram(f"✅ Đã dừng bot {text}.", chat_id=chat_id,
                                 reply_markup=create_main_menu(),
                                 bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                else:
                    send_telegram(f"❌ Không thể dừng bot {text}.", chat_id=chat_id,
                                 reply_markup=create_main_menu(),
                                 bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                self.user_states[chat_id] = {}
            else:
                send_telegram("⚠️ Không tìm thấy bot. Vui lòng chọn lại:", chat_id=chat_id,
                             reply_markup={"keyboard": [[{"text": bid}] for bid in self.bots.keys()] + [[{"text": "❌ Hủy bỏ"}]],
                                          "resize_keyboard": True, "one_time_keyboard": True},
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            return

        # ----- XỬ LÝ DỪNG COIN -----
        if current_step == 'waiting_stop_coin':
            if text == "⛔ DỪNG TẤT CẢ COIN":
                self.stop_all_coins()
                send_telegram("✅ Đã dừng tất cả coin trong tất cả bot.", chat_id=chat_id,
                             reply_markup=create_main_menu(),
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                self.user_states[chat_id] = {}
            elif text.startswith("⛔ Coin:"):
                coin = text.replace("⛔ Coin:", "").strip()
                if self.stop_coin(coin):
                    send_telegram(f"✅ Đã dừng coin {coin}.", chat_id=chat_id,
                                 reply_markup=create_main_menu(),
                                 bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                else:
                    send_telegram(f"❌ Không tìm thấy coin {coin}.", chat_id=chat_id,
                                 reply_markup=create_main_menu(),
                                 bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                self.user_states[chat_id] = {}
            else:
                # Nếu không phải lệnh hợp lệ, hiển thị lại menu quản lý coin
                keyboard = self.get_coin_management_keyboard()
                if keyboard:
                    send_telegram("⚠️ Vui lòng chọn coin từ danh sách:", chat_id=chat_id,
                                 reply_markup=keyboard,
                                 bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                else:
                    self.user_states[chat_id] = {}
                    self.send_main_menu(chat_id)
            return

        # ----- MẶC ĐỊNH: HIỂN THỊ MENU CHÍNH -----
        self.send_main_menu(chat_id)

    def _finish_bot_creation(self, chat_id, user_state):
        """Hoàn tất tạo bot với các tham số đã thu thập."""
        try:
            bot_mode = user_state.get('bot_mode', 'static')
            symbol = user_state.get('symbol') if bot_mode == 'static' else None
            leverage = user_state.get('leverage')
            percent = user_state.get('percent')
            tp = user_state.get('tp')
            sl = user_state.get('sl')
            roi_trigger = user_state.get('roi_trigger', 0)
            bot_count = user_state.get('bot_count', 1)
            pyramiding_n = user_state.get('pyramiding_n', 0)
            pyramiding_x = user_state.get('pyramiding_x', 0)
            enable_balance_orders = user_state.get('enable_balance_orders', True)
            buy_price_threshold = user_state.get('buy_price_threshold', 1.0)
            sell_price_threshold = user_state.get('sell_price_threshold', 10.0)

            success = self.add_bot(
                symbol=symbol, lev=leverage, percent=percent, tp=tp, sl=sl,
                roi_trigger=roi_trigger, strategy_type="Balance-Strategy",
                bot_mode=bot_mode, bot_count=bot_count,
                pyramiding_n=pyramiding_n, pyramiding_x=pyramiding_x,
                enable_balance_orders=enable_balance_orders,
                buy_price_threshold=buy_price_threshold,
                sell_price_threshold=sell_price_threshold
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
                if bot_mode == 'static' and symbol:
                    success_msg += f"\n🔗 Coin: {symbol}"

                success_msg += (f"\n\n🔄 <b>CƠ CHẾ CÂN BẰNG ĐƯỢC KÍCH HOẠT</b>\n"
                              f"• Đếm số lượng lệnh BUY/SELL hiện có\n"
                              f"• Ưu tiên hướng ngược lại khi mất cân bằng\n"
                              f"• Lọc coin theo ngưỡng giá (MUA <{buy_price_threshold}, BÁN >{sell_price_threshold})\n"
                              f"• Yêu cầu đòn bẩy tối thiểu: {_BALANCE_CONFIG['min_leverage']}x\n"
                              f"• SẮP XẾP coin theo khối lượng giảm dần\n\n")
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

# Bỏ qua SSL context
ssl._create_default_https_context = ssl._create_unverified_context
