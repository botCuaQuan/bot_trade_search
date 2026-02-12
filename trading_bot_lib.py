# trading_bot_lib_ep_huong_chung.py (ĐÃ SỬA - CHIẾN LƯỢC CÂN BẰNG + USDT/USDC)
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

# ========== CACHE COIN TẬP TRUNG (USDT + USDC) ==========
_COINS_CACHE = {
    "data": [],                # Danh sách coin đầy đủ
    "last_volume_update": 0,   # Thời gian cập nhật volume lần cuối
    "last_price_update": 0,    # Thời gian cập nhật giá lần cuối
}
_VOLUME_CACHE_TTL = 6 * 3600  # 6 giờ
_PRICE_CACHE_TTL = 300        # 5 phút

# ========== CẤU HÌNH CÂN BẰNG LỆNH (MỚI) ==========
_BALANCE_CONFIG = {
    "buy_price_threshold": 1.0,   # Mua < 1 USDT/USDC
    "sell_price_threshold": 10.0, # Bán > 10 USDT/USDC
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
    # Dùng cache mới lấy top coin theo volume (không gọi API mới)
    try:
        coins = get_coins_with_info()
        # Sắp xếp theo volume giảm dần, lấy top 12
        coins_sorted = sorted(coins, key=lambda x: x['volume'], reverse=True)
        symbols = [coin['symbol'] for coin in coins_sorted[:12]]
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

# ========== HÀM API BINANCE (giữ nguyên) ==========
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

# ========== HÀM CACHE COIN MỚI (USDT + USDC) ==========
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
            # Chỉ lấy cặp quote là USDT hoặc USDC, đang TRADING, không trong blacklist
            if quote not in ('USDT', 'USDC'):
                continue
            if symbol_info.get('status') != 'TRADING':
                continue
            if symbol in _SYMBOL_BLACKLIST:
                continue

            # Lấy đòn bẩy tối đa
            max_leverage = 50  # mặc định an toàn
            for f in symbol_info.get('filters', []):
                if f['filterType'] == 'LEVERAGE' and 'maxLeverage' in f:
                    max_leverage = int(f['maxLeverage'])
                    break

            # Lấy step size
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
        import traceback
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
    """Lấy danh sách coin đầy đủ từ cache (tự động refresh nếu cần)"""
    global _COINS_CACHE
    now = time.time()

    if not _COINS_CACHE["data"] or now - _COINS_CACHE["last_volume_update"] > _VOLUME_CACHE_TTL:
        logger.info("🔄 Cache cũ, làm mới danh sách coin...")
        refresh_coins_cache()
        update_coins_volume()

    if now - _COINS_CACHE["last_price_update"] > _PRICE_CACHE_TTL:
        update_coins_price()

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
    """Buộc làm mới toàn bộ cache coin"""
    logger.info("🔄 Buộc làm mới cache coin...")
    if refresh_coins_cache():
        update_coins_volume()
        update_coins_price()
        return True
    return False

# ========== HÀM LỌC COIN MỚI (USDT + USDC) ==========
def filter_and_sort_coins_for_side(side, excluded_coins=None, required_leverage=10):
    """
    Lọc coin theo chiến lược mới:
    - BUY  : price < 1 USDT/USDC, volume giảm dần
    - SELL : price > 10 USDT/USDC, volume giảm dần
    - Volume cao nhất được ưu tiên (thanh khoản tốt)
    """
    all_coins = get_coins_with_info()
    filtered = []

    if not all_coins:
        logger.warning("❌ Cache coin trống!")
        return filtered

    logger.info(f"🔍 Lọc coin {side} | {len(all_coins)} coin trong cache")
    logger.info(f"⚙️ Ngưỡng: MUA < {_BALANCE_CONFIG['buy_price_threshold']} USDT/USDC, BÁN > {_BALANCE_CONFIG['sell_price_threshold']} USDT/USDC | Volume: DESC")

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
        if coin['max_leverage'] < required_leverage:
            lev_fail += 1
            continue
        if coin['price'] <= 0:
            price_fail += 1
            continue
        if coin['volume'] <= 0:
            volume_zero += 1
            continue

        if side == "BUY" and coin['price'] >= _BALANCE_CONFIG["buy_price_threshold"]:
            price_fail += 1
            continue
        if side == "SELL" and coin['price'] <= _BALANCE_CONFIG["sell_price_threshold"]:
            price_fail += 1
            continue

        filtered.append(coin)

    # Sắp xếp volume giảm dần (thanh khoản cao nhất lên đầu)
    filtered.sort(key=lambda x: x['volume'], reverse=True)

    logger.info(f"📊 {side}: {len(filtered)} coin phù hợp (loại: blacklist={blacklisted}, excluded={excluded_cnt}, lev={lev_fail}, giá={price_fail}, volume0={volume_zero})")
    if filtered:
        for i, c in enumerate(filtered[:5]):
            logger.info(f"  {i+1}. {c['symbol']} | giá: {c['price']:.4f} | vol: {c['volume']:.1f} | lev: {c['max_leverage']}x")

    return filtered

def update_balance_config(buy_price_threshold=None, sell_price_threshold=None):
    """Cập nhật cấu hình cân bằng lệnh (bỏ volume sort)"""
    global _BALANCE_CONFIG
    if buy_price_threshold is not None:
        _BALANCE_CONFIG["buy_price_threshold"] = buy_price_threshold
    if sell_price_threshold is not None:
        _BALANCE_CONFIG["sell_price_threshold"] = sell_price_threshold
    logger.info(f"✅ Cập nhật cấu hình cân bằng: {_BALANCE_CONFIG}")
    return _BALANCE_CONFIG

# ========== CÁC HÀM API KHÁC (giữ nguyên) ==========
def get_step_size(symbol, api_key, api_secret):
    if not symbol: return 0.001
    # Có thể lấy từ cache để tránh gọi API
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

# ========== LỚP QUẢN LÝ CỐT LÕI (giữ nguyên cấu trúc) ==========
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
        """✅ Lấy đòn bẩy tối đa từ cache"""
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
        - Lọc coin theo ngưỡng giá (MUA <1, BÁN >10) và volume giảm dần
        """
        try:
            now = time.time()
            if now - self.last_scan_time < self.scan_cooldown:
                return None
            self.last_scan_time = now

            # Kiểm tra cache coin trước
            if not _COINS_CACHE["data"]:
                logger.warning("🔄 Cache coin trống, đang làm mới...")
                if not refresh_coins_cache():
                    logger.error("❌ Không thể làm mới cache coin")
                    return None

            # Xác định hướng giao dịch dựa trên số lượng lệnh
            if self._bot_manager and hasattr(self._bot_manager, 'global_side_coordinator'):
                target_side = self._bot_manager.global_side_coordinator.get_next_side(
                    self.api_key, self.api_secret
                )
            else:
                target_side = self.get_next_side_for_balance()

            logger.info(f"🎯 Hệ thống chọn hướng: {target_side} (dựa trên số lượng lệnh)")

            # Lấy danh sách coin đã lọc
            filtered_coins = filter_and_sort_coins_for_side(
                target_side, excluded_coins, required_leverage
            )

            if not filtered_coins:
                if now - self.last_failed_search_log > 60:
                    logger.warning(f"⚠️ Không tìm thấy coin phù hợp cho hướng {target_side}")
                    self.last_failed_search_log = now
                return None

            # Ưu tiên coin theo thứ tự đã sắp xếp (volume cao nhất đầu)
            for coin in filtered_coins[:20]:
                symbol = coin['symbol']
                if self.has_existing_position(symbol):
                    continue
                if self._bot_manager and self._bot_manager.coin_manager.is_coin_active(symbol):
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

# ========== WEBSOCKET MANAGER (giữ nguyên) ==========
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

# ========== LỚP BOT CỐT LÕI (sửa phần cache và lọc coin) ==========
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

        # Cấu hình cân bằng lệnh (chỉ còn ngưỡng giá)
        self.enable_balance_orders = kwargs.get('enable_balance_orders', True)
        self.balance_config = {
            'buy_price_threshold': kwargs.get('buy_price_threshold', 1.0),
            'sell_price_threshold': kwargs.get('sell_price_threshold', 10.0),
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
                        f"Mua <{self.balance_config['buy_price_threshold']} USDT/USDC | "
                        f"Bán >{self.balance_config['sell_price_threshold']} USDT/USDC")

        self.log(f"🟢 Bot {strategy_name} đã khởi động | 1 coin | Đòn bẩy: {lev}x | Vốn: {percent}% | TP/SL: {tp}%/{sl}%{roi_info}{pyramiding_info}{balance_info}")

    def _initialize_coin_cache(self):
        """Khởi tạo cache coin trước khi bot bắt đầu chạy"""
        try:
            logger.info("🔄 Đang khởi tạo cache coin...")
            if refresh_coins_cache():
                update_coins_volume()
                update_coins_price()
                coins_count = len(_COINS_CACHE.get("data", []))
                logger.info(f"✅ Đã khởi tạo cache {coins_count} coin")
            else:
                logger.error("❌ Không thể khởi tạo cache coin")
        except Exception as e:
            logger.error(f"❌ Lỗi khởi tạo cache: {str(e)}")

    def _run(self):
        """Vòng lặp chính"""
        if not _COINS_CACHE["data"]:
            self._initialize_coin_cache()

        last_coin_search_log = 0
        log_interval = 30
        last_no_coin_found_log = 0

        while not self._stop:
            try:
                current_time = time.time()

                # Tự động refresh cache nếu cũ
                if current_time - _COINS_CACHE["last_volume_update"] > _VOLUME_CACHE_TTL:
                    update_coins_volume()
                if current_time - _COINS_CACHE["last_price_update"] > _PRICE_CACHE_TTL:
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
                            last_coin_search_log = 0
                        else:
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

    # ... (các phương thức _check_pyramiding, _pyramid_order, _check_smart_exit_condition, _add_symbol,
    #      _handle_price_update, get_current_price, _check_symbol_position, _reset_symbol_position,
    #      _open_symbol_position, _close_symbol_position, _check_margin_safety, _check_symbol_tp_sl,
    #      stop_symbol, stop_all_symbols, stop, check_global_positions, get_next_side_based_on_comprehensive_analysis,
    #      log) giữ nguyên như bản gốc, chỉ sửa những chỗ dùng cache và set_leverage.

    # Dưới đây là phiên bản đã sửa của _open_symbol_position (quan trọng)
    def _open_symbol_position(self, symbol, side):
        try:
            if self.coin_finder.has_existing_position(symbol):
                self.log(f"⚠️ {symbol} - CÓ VỊ THẾ TRÊN BINANCE, BỎ QUA")
                self.stop_symbol(symbol)
                return False

            self._check_symbol_position(symbol)
            if self.symbol_data[symbol]['position_open']:
                return False

            # Lấy đòn bẩy tối đa từ cache
            max_leverage_from_cache = get_max_leverage_from_cache(symbol)
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
                    if self.roi_trigger:
                        message += f" | 🎯 ROI Kích hoạt: {self.roi_trigger}%"
                    if self.pyramiding_enabled:
                        message += f" | 🔄 Nhồi lệnh: {self.pyramiding_n} lần tại {self.pyramiding_x}%"

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

    # ... (các phương thức còn lại: _close_symbol_position, _check_margin_safety,
    #      _check_symbol_tp_sl, stop_symbol, stop_all_symbols, stop, check_global_positions,
    #      get_next_side_based_on_comprehensive_analysis, log) giữ nguyên không thay đổi,
    #      chỉ cần đảm bảo không gọi các biến cache cũ.

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
            self.log("🟢 HỆ THỐNG BOT CÂN BẰNG LỆNH (USDT/USDC) ĐÃ KHỞI ĐỘNG")
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
            if refresh_coins_cache():
                update_coins_volume()
                update_coins_price()
                coins_count = len(_COINS_CACHE.get("data", []))
                logger.info(f"✅ Hệ thống đã khởi tạo cache {coins_count} coin")
            else:
                logger.error("❌ Hệ thống không thể khởi tạo cache")
        except Exception as e:
            logger.error(f"❌ Lỗi khởi tạo cache hệ thống: {str(e)}")

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
        # (giữ nguyên, chỉ cần thay thế các biến cache cũ nếu có, nhưng trong hàm này không dùng cache)
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
            summary += f"⚖️ **BOT CÂN BẰNG**: {balance_bots}/{len(self.bots)} bot\n\n"

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
            "• MUA: chọn coin có giá < 1 USDT/USDC, volume cao nhất đầu tiên\n"
            "• BÁN: chọn coin có giá > 10 USDT/USDC, volume cao nhất đầu tiên\n"
            "• Loại trừ coin đã có vị thế để tránh trùng\n"
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
                                f"• Mua: giá < {buy_price_threshold} USDT/USDC | Volume: cao nhất đầu\n"
                                f"• Bán: giá > {sell_price_threshold} USDT/USDC | Volume: cao nhất đầu\n")

            success_msg = (f"✅ <b>ĐÃ TẠO {created_count} BOT CÂN BẰNG</b>\n\n"
                           f"🎯 Chiến lược: {strategy_type}\n💰 Đòn bẩy: {lev}x\n"
                           f"📈 % Số dư: {percent}%\n🎯 TP: {tp}%\n"
                           f"🛡️ SL: {sl if sl is not None else 'Tắt'}%{roi_info}{pyramiding_info}\n"
                           f"🔧 Chế độ: {bot_mode}\n🔢 Số bot: {created_count}\n")
            if bot_mode == 'static' and symbol:
                success_msg += f"🔗 Coin ban đầu: {symbol}\n"
            else:
                success_msg += f"🔗 Coin: Tự động tìm (USDT/USDC)\n"
            success_msg += balance_info
            success_msg += (f"\n🔄 <b>CƠ CHẾ CÂN BẰNG ĐƯỢC KÍCH HOẠT</b>\n"
                           f"• Đếm số lượng lệnh BUY/SELL hiện có\n"
                           f"• Ưu tiên hướng ngược lại khi mất cân bằng\n"
                           f"• Lọc coin theo ngưỡng giá (MUA <{buy_price_threshold}, BÁN >{sell_price_threshold})\n"
                           f"• Sắp xếp volume giảm dần cho cả hai chiều\n\n")
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
                send_telegram("✅ Đã chọn BẬT cân bằng lệnh\n\nNhập ngưỡng giá MUA (USDT/USDC):",
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
                    f"• Ngưỡng giá MUA: < {_BALANCE_CONFIG['buy_price_threshold']} USDT/USDC\n"
                    f"• Ngưỡng giá BÁN: > {_BALANCE_CONFIG['sell_price_threshold']} USDT/USDC\n"
                    f"• Sắp xếp: Volume giảm dần cho cả hai chiều\n\n"
                    f"🔄 <b>CACHE HỆ THỐNG</b>\n"
                    f"• Số coin: {len(_COINS_CACHE.get('data', []))}\n"
                    f"• Cập nhật giá: {time.ctime(_COINS_CACHE.get('last_price_update', 0))}\n"
                    f"• Cập nhật volume: {time.ctime(_COINS_CACHE.get('last_volume_update', 0))}"
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
                    send_telegram(f"✅ Ngưỡng MUA: < {buy_threshold} USDT/USDC\n\nNhập ngưỡng giá BÁN (USDT/USDC):",
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

                    # Cập nhật cấu hình ngay lập tức (không hỏi volume sort)
                    update_balance_config(
                        buy_price_threshold=user_state.get('buy_price_threshold'),
                        sell_price_threshold=user_state.get('sell_price_threshold')
                    )

                    # Cập nhật balance_config cho các bot đang chạy
                    updated_bots = 0
                    for bot in self.bots.values():
                        if hasattr(bot, 'enable_balance_orders') and bot.enable_balance_orders:
                            bot.balance_config = {
                                'buy_price_threshold': user_state.get('buy_price_threshold', 1.0),
                                'sell_price_threshold': user_state.get('sell_price_threshold', 10.0),
                            }
                            updated_bots += 1

                    config_summary = (
                        f"✅ <b>ĐÃ CẬP NHẬT CẤU HÌNH CÂN BẰNG</b>\n\n"
                        f"• Ngưỡng MUA: < {user_state.get('buy_price_threshold', 1.0)} USDT/USDC\n"
                        f"• Ngưỡng BÁN: > {user_state.get('sell_price_threshold', 10.0)} USDT/USDC\n"
                        f"• Sắp xếp: Volume giảm dần cho cả hai chiều\n\n"
                        f"🔄 Đã cập nhật cho {updated_bots} bot có cân bằng lệnh"
                    )

                    send_telegram(config_summary, chat_id=chat_id,
                                 reply_markup=create_main_menu(),
                                 bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)

                    self.user_states[chat_id] = {}
                except ValueError:
                    send_telegram("⚠️ Vui lòng nhập số hợp lệ cho ngưỡng giá BÁN:",
                                chat_id=chat_id, reply_markup=create_price_threshold_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)

        # ===== Các xử lý Telegram khác (giữ nguyên) =====
        elif current_step == 'waiting_bot_mode':
            # ... giữ nguyên code cũ ...
            pass

        # ... (các xử lý còn lại giữ nguyên, chỉ cần đảm bảo không gọi volume sort)

        # Phần xử lý mặc định
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
                enable_balance_orders=enable_balance_orders,
                buy_price_threshold=user_state.get('buy_price_threshold', 1.0),
                sell_price_threshold=user_state.get('sell_price_threshold', 10.0)
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
                              f"• Lọc coin theo ngưỡng giá (MUA <{user_state.get('buy_price_threshold',1.0)}, BÁN >{user_state.get('sell_price_threshold',10.0)})\n"
                              f"• Sắp xếp volume giảm dần cho cả hai chiều\n\n")
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
