# trading_bot_lib.py - HOÀN CHỈNH VỚI HỆ THỐNG BOT ĐỘNG ĐA LUỒNG
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

# ========== CẤU HÌNH LOGGING ==========
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(module)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('bot_errors.log')
        ]
    )
    return logging.getLogger()

logger = setup_logging()

# ========== HÀM TELEGRAM ==========
def send_telegram(message, chat_id=None, reply_markup=None, bot_token=None, default_chat_id=None):
    if not bot_token:
        logger.warning("Telegram Bot Token chưa được thiết lập")
        return
    
    chat_id = chat_id or default_chat_id
    if not chat_id:
        logger.warning("Telegram Chat ID chưa được thiết lập")
        return
    
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
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
            [{"text": "⏰ Multi-Timeframe"}],
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
        symbols = get_all_usdt_pairs(limit=12)
        if not symbols:
            symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT", "DOGEUSDT", "XRPUSDT", "DOTUSDT", "LINKUSDT"]
    except:
        symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT", "DOGEUSDT", "XRPUSDT", "DOTUSDT", "LINKUSDT"]
    
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

# ========== POSITION BALANCER ==========
class PositionBalancer:
    """CÂN BẰNG VỊ THẾ TỰ ĐỘNG DỰA TRÊN TỶ LỆ BUY/SELL"""
    
    def __init__(self, bot_manager):
        self.bot_manager = bot_manager
        self.buy_sell_history = []
        self.max_history = 50
        self.imbalance_threshold = 2
        
    def get_current_ratio(self):
        """Lấy tỷ lệ BUY/SELL hiện tại"""
        try:
            buy_count = 0
            sell_count = 0
            
            for bot_id, bot in self.bot_manager.bots.items():
                if bot.position_open:
                    if bot.side == "BUY":
                        buy_count += 1
                    elif bot.side == "SELL":
                        sell_count += 1
            
            total = buy_count + sell_count
            if total == 0:
                return 0.5, 0.5
                
            buy_ratio = buy_count / total
            sell_ratio = sell_count / total
            
            self.buy_sell_history.append((buy_count, sell_count))
            if len(self.buy_sell_history) > self.max_history:
                self.buy_sell_history.pop(0)
                
            return buy_ratio, sell_ratio
            
        except Exception as e:
            logger.error(f"❌ Lỗi tính tỷ lệ vị thế: {str(e)}")
            return 0.5, 0.5
    
    def get_recommended_direction(self):
        """Đề xuất hướng giao dịch dựa trên cân bằng vị thế"""
        try:
            buy_ratio, sell_ratio = self.get_current_ratio()
            
            if len(self.buy_sell_history) >= 10:
                recent_buys = sum([item[0] for item in self.buy_sell_history[-5:]])
                recent_sells = sum([item[1] for item in self.buy_sell_history[-5:]])
                
                if recent_buys - recent_sells >= self.imbalance_threshold:
                    recommendation = "SELL"
                    reason = f"BUY đang chiếm ưu thế ({recent_buys} BUY vs {recent_sells} SELL)"
                elif recent_sells - recent_buys >= self.imbalance_threshold:
                    recommendation = "BUY" 
                    reason = f"SELL đang chiếm ưu thế ({recent_sells} SELL vs {recent_buys} BUY)"
                else:
                    recommendation = "NEUTRAL"
                    reason = "Thị trường cân bằng"
            else:
                recommendation = "NEUTRAL"
                reason = "Chưa đủ dữ liệu lịch sử"
            
            logger.info(f"⚖️ Cân bằng vị thế: BUY {buy_ratio:.1%} / SELL {sell_ratio:.1%} → {recommendation} ({reason})")
            return recommendation
            
        except Exception as e:
            logger.error(f"❌ Lỗi đề xuất hướng: {str(e)}")
            return "NEUTRAL"

# ========== MULTI TIMEFRAME ANALYZER ==========
class MultiTimeframeAnalyzer:
    """PHÂN TÍCH ĐA KHUNG THỜI GIAN - ĐÃ SỬA LỖI TÍN HIỆU"""
    
    def __init__(self):
        self.timeframes = ['1m', '5m', '15m', '30m']
        self.lookback = 200
        
    def analyze_symbol(self, symbol):
        """Phân tích symbol trên 4 khung thời gian - ĐÃ SỬA"""
        try:
            timeframe_signals = {}
            
            for tf in self.timeframes:
                signal, stats = self.analyze_timeframe(symbol, tf)
                timeframe_signals[tf] = {
                    'signal': signal,
                    'stats': stats,
                    'bullish_ratio': stats['bullish_ratio'] if stats else 0.5
                }
            
            # Tổng hợp tín hiệu với logic MỚI - DỄ HƠN
            final_signal = self.aggregate_signals(timeframe_signals)
            return final_signal, timeframe_signals
            
        except Exception as e:
            logger.error(f"❌ Lỗi phân tích {symbol}: {str(e)}")
            return "NEUTRAL", {}
    
    def analyze_timeframe(self, symbol, timeframe):
        """Phân tích 1 khung thời gian - ĐÃ SỬA NGƯỠNG"""
        try:
            klines = self.get_klines(symbol, timeframe, self.lookback)
            if not klines or len(klines) < 50:
                return "NEUTRAL", {}
            
            bullish_count = 0
            bearish_count = 0
            price_changes = []
            
            for i in range(1, min(len(klines), self.lookback)):
                open_price = float(klines[i][1])
                close_price = float(klines[i][4])
                
                if close_price > open_price:
                    bullish_count += 1
                elif close_price < open_price:
                    bearish_count += 1
                
                price_change = ((close_price - open_price) / open_price) * 100
                price_changes.append(price_change)
            
            total_candles = bullish_count + bearish_count
            if total_candles == 0:
                return "NEUTRAL", {}
            
            bullish_ratio = bullish_count / total_candles
            bearish_ratio = bearish_count / total_candles
            
            # 🎯 SỬA QUAN TRỌNG: GIẢM NGƯỠNG XUỐNG 55%
            signal = "NEUTRAL"
            if bullish_ratio > 0.5:  # GIẢM từ 60% → 55%
                signal = "SELL"
                logger.debug(f"📈 {symbol} {timeframe}: {bullish_ratio:.1%} nến tăng → SELL")
            elif bearish_ratio > 0.5:  # GIẢM từ 60% → 55%  
                signal = "BUY"
                logger.debug(f"📉 {symbol} {timeframe}: {bearish_ratio:.1%} nến giảm → BUY")
            else:
                logger.debug(f"⚪ {symbol} {timeframe}: {bullish_ratio:.1%} nến tăng → NEUTRAL")
            
            stats = {
                'bullish_count': bullish_count,
                'bearish_count': bearish_count,
                'total_candles': total_candles,
                'bullish_ratio': bullish_ratio,
                'bearish_ratio': bearish_ratio,
                'avg_change': np.mean(price_changes) if price_changes else 0
            }
            
            return signal, stats
            
        except Exception as e:
            logger.error(f"❌ Lỗi phân tích {timeframe}: {str(e)}")
            return "NEUTRAL", {}
    
    def aggregate_signals(self, timeframe_signals):
        """Tổng hợp tín hiệu - SỬA LOGIC ĐỂ CÓ TÍN HIỆU RÕ RÀNG HƠN"""
        signals = []
        
        for tf, data in timeframe_signals.items():
            signals.append(data['signal'])
        
        # Đếm số khung thời gian đồng thuận
        buy_signals = signals.count("BUY")
        sell_signals = signals.count("SELL")
        
        logger.info(f"📊 {list(timeframe_signals.keys())[0].split('_')[0]} Tín hiệu: "
                   f"1m={signals[0]}, 5m={signals[1]}, 15m={signals[2]}, 30m={signals[3]} "
                   f"(BUY: {buy_signals}/4, SELL: {sell_signals}/4)")
        
        # 🎯 SỬA LOGIC: ƯU TIÊN TÍN HIỆU RÕ RÀNG
        # Nếu 3/4 khung đồng ý → tín hiệu mạnh
        if buy_signals >= 3:
            return "BUY"
        elif sell_signals >= 3:
            return "SELL"
        # Nếu 2/4 khung đồng ý và 2 khung còn lại là NEUTRAL → tín hiệu trung bình
        elif buy_signals >= 2 and (buy_signals + sell_signals) == 2:
            return "BUY"
        elif sell_signals >= 2 and (buy_signals + sell_signals) == 2:
            return "SELL"
        # Nếu phân hóa (2 BUY + 2 SELL) → NEUTRAL
        else:
            return "NEUTRAL"
    
    def get_klines(self, symbol, interval, limit):
        """Lấy dữ liệu nến từ Binance"""
        try:
            url = "https://fapi.binance.com/fapi/v1/klines"
            params = {
                'symbol': symbol.upper(),
                'interval': interval,
                'limit': limit
            }
            return binance_api_request(url, params=params)
        except Exception as e:
            logger.error(f"❌ Lỗi lấy nến {symbol} {interval}: {str(e)}")
            return None

# ========== SMART COIN FINDER NÂNG CẤP ==========
class SmartCoinFinder:
    """TÌM COIN THÔNG MINH DỰA TRÊN ĐA KHUNG THỜI GIAN VÀ ĐÒN BẨY"""
    
    def __init__(self, api_key, api_secret, required_leverage):
        self.api_key = api_key
        self.api_secret = api_secret
        self.required_leverage = required_leverage  # LƯU ĐÒN BẨY YÊU CẦU
        self.analyzer = MultiTimeframeAnalyzer()
        self.leverage_cache = {}  # CACHE THÔNG TIN ĐÒN BẨY
        
    def check_leverage_support(self, symbol):
        """KIỂM TRA COIN CÓ HỖ TRỢ ĐÒN BẨY YÊU CẦU KHÔNG"""
        try:
            # Sử dụng cache để tránh gọi API nhiều lần
            if symbol in self.leverage_cache:
                return self.leverage_cache[symbol]
                
            # Kiểm tra đòn bẩy thông qua leverage brackets
            max_leverage = self.get_max_leverage(symbol)
            if max_leverage and max_leverage >= self.required_leverage:
                self.leverage_cache[symbol] = True
                return True
            else:
                self.leverage_cache[symbol] = False
                return False
                
        except Exception as e:
            logger.error(f"❌ Lỗi kiểm tra đòn bẩy {symbol}: {str(e)}")
            return False
    
    def get_max_leverage(self, symbol):
        """LẤY THÔNG TIN ĐÒN BẨY TỐI ĐA CỦA COIN"""
        try:
            ts = int(time.time() * 1000)
            params = {"timestamp": ts}
            query = urllib.parse.urlencode(params)
            sig = sign(query, self.api_secret)
            url = f"https://fapi.binance.com/fapi/v1/leverageBracket?{query}&signature={sig}"
            headers = {'X-MBX-APIKEY': self.api_key}
            
            data = binance_api_request(url, headers=headers)
            if not data:
                return None
                
            for bracket in data:
                if bracket['symbol'] == symbol:
                    # Lấy đòn bẩy tối đa từ bracket
                    max_leverage = bracket['brackets'][0]['initialLeverage']
                    return max_leverage
                    
            return None
            
        except Exception as e:
            logger.error(f"❌ Lỗi lấy đòn bẩy {symbol}: {str(e)}")
            return None

    def find_coin_by_direction(self, target_direction, excluded_symbols=None):
        """TÌM COIN VỚI KIỂM TRA ĐÒN BẨY"""
        try:
            if excluded_symbols is None:
                excluded_symbols = set()
            
            logger.info(f"🔍 Bot đang tìm coin {target_direction} với đòn bẩy {self.required_leverage}x...")
            
            # Lấy danh sách coin USDT toàn bộ Binance
            all_symbols = get_all_usdt_pairs(limit=600)
            
            if not all_symbols:
                logger.error("❌ Không lấy được danh sách coin từ Binance")
                return None
            
            # Xáo trộn danh sách để random chọn coin
            random.shuffle(all_symbols)
            
            # Duyệt qua từng coin cho đến khi tìm được coin phù hợp
            for symbol in all_symbols:
                try:
                    # Skip BTC, ETH và các symbol đã bị exclude
                    if symbol in ['BTCUSDT', 'ETHUSDT', 'BNBUSDT'] or symbol in excluded_symbols:
                        continue
                    
                    # 🎯 THÊM KIỂM TRA ĐÒN BẨY
                    if not self.check_leverage_support(symbol):
                        logger.debug(f"❌ {symbol} - Không hỗ trợ đòn bẩy {self.required_leverage}x")
                        continue
                    
                    # Phân tích coin
                    result = self.analyze_symbol_for_finding(symbol, target_direction)
                    if result and result.get('qualified', False):
                        logger.info(f"✅ Bot đã tìm thấy coin: {symbol} - {target_direction} (điểm: {result['score']:.2f}, đòn bẩy: {self.required_leverage}x)")
                        return result
                        
                except Exception as e:
                    logger.debug(f"❌ Lỗi phân tích {symbol}: {str(e)}")
                    continue
            
            # Nếu không tìm thấy coin nào, sử dụng fallback
            logger.warning(f"⚠️ Không tìm thấy coin {target_direction} phù hợp với đòn bẩy {self.required_leverage}x, sử dụng fallback")
            fallback_coin = self._find_fallback_coin(target_direction, excluded_symbols)
            
            if fallback_coin:
                fallback_coin['qualified'] = True
                return fallback_coin
            else:
                return None
                
        except Exception as e:
            logger.error(f"❌ Lỗi tìm coin: {str(e)}")
            return None
    
    def analyze_symbol_for_finding(self, symbol, target_direction):
        """Phân tích chi tiết một symbol - THÊM DEBUG CHI TIẾT"""
        try:
            # Phân tích đa khung thời gian
            signal, timeframe_data = self.analyzer.analyze_symbol(symbol)
            
            logger.info(f"🔍 {symbol} - Target: {target_direction}, Actual: {signal}")
            
            if signal != target_direction:
                logger.info(f"❌ {symbol} - Signal không khớp: {signal} != {target_direction}")
                
                # Log chi tiết từng khung thời gian để debug
                for tf, data in timeframe_data.items():
                    stats = data.get('stats', {})
                    if stats:
                        logger.info(f"   {tf}: {data['signal']} | Tăng: {stats.get('bullish_ratio', 0):.1%}")
                        
                return None
            
            # Tính điểm chất lượng
            score = self.calculate_quality_score(timeframe_data, target_direction)
            
            logger.info(f"📊 {symbol} - Điểm chất lượng: {score:.2f}")
            
            # Log chi tiết điểm số
            for tf, data in timeframe_data.items():
                stats = data.get('stats', {})
                if stats:
                    bullish_ratio = stats.get('bullish_ratio', 0.5)
                    clarity_score = max(0, (bullish_ratio - 0.52)) * 3 if target_direction == "SELL" else max(0, ((1 - bullish_ratio) - 0.52)) * 3
                    logger.info(f"   {tf}: {data['signal']} | Tăng: {bullish_ratio:.1%} | Điểm rõ: {clarity_score:.2f}")
            
            # GIẢM NGƯỠNG để test
            if score >= 0.3:  # GIẢM XUỐNG 0.3 ĐỂ TEST
                logger.info(f"✅ {symbol} - ĐẠT TIÊU CHUẨN (điểm: {score:.2f} >= 0.3)")
                return {
                    'symbol': symbol,
                    'direction': target_direction,
                    'score': score,
                    'timeframe_data': timeframe_data,
                    'qualified': True
                }
            else:
                logger.info(f"❌ {symbol} - Điểm thấp: {score:.2f} < 0.3")
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Lỗi phân tích {symbol}: {str(e)}")
            return None
        
    def calculate_quality_score(self, timeframe_data, target_direction):
        """Tính điểm chất lượng - ĐÃ SỬA ĐỘ KHÓ"""
        try:
            total_score = 0
            max_score = 0
            
            for tf, data in timeframe_data.items():
                stats = data.get('stats', {})
                if not stats:
                    continue
                    
                bullish_ratio = stats.get('bullish_ratio', 0.5)
                total_candles = stats.get('total_candles', 0)
                avg_change = abs(stats.get('avg_change', 0))
                
                # Điểm cho độ rõ ràng của tín hiệu - GIẢM NGƯỠNG
                if target_direction == "SELL":
                    # SELL: bullish_ratio càng cao → điểm càng cao
                    clarity_score = max(0, (bullish_ratio - 0.52)) * 3  # GIẢM ngưỡng
                else:  # BUY
                    # BUY: bearish_ratio càng cao → điểm càng cao  
                    clarity_score = max(0, ((1 - bullish_ratio) - 0.52)) * 3  # GIẢM ngưỡng
                
                # Điểm cho số lượng nến (độ tin cậy)
                volume_score = min(total_candles / 100, 1.0)  # GIẢM yêu cầu từ 200 → 100
                
                # Điểm cho biến động giá
                volatility_score = min(avg_change / 0.3, 1.0)  # GIẢM yêu cầu từ 0.5% → 0.3%
                
                # Tổng điểm cho khung thời gian này
                tf_score = (clarity_score * 0.6 + volume_score * 0.2 + volatility_score * 0.2)
                total_score += tf_score
                max_score += 1.0
            
            final_score = total_score / max_score if max_score > 0 else 0
            
            # 🎯 GIẢM NGƯỠNG CHẤP NHẬN COIN
            return final_score
            
        except Exception as e:
            logger.error(f"❌ Lỗi tính điểm: {str(e)}")
            return 0
    
    def _find_fallback_coin(self, target_direction, excluded_symbols):
        """Phương pháp dự phòng với kiểm tra đòn bẩy"""
        logger.info(f"🔄 Sử dụng fallback cho {target_direction} với đòn bẩy {self.required_leverage}x")
        
        all_symbols = get_all_usdt_pairs(limit=600)
        if not all_symbols:
            return None
            
        random.shuffle(all_symbols)
        
        for symbol in all_symbols:
            if symbol in ['BTCUSDT', 'ETHUSDT'] or symbol in excluded_symbols:
                continue
                
            try:
                # 🎯 KIỂM TRA ĐÒN BẨY TRONG FALLBACK
                if not self.check_leverage_support(symbol):
                    continue
                    
                change_24h = get_24h_change(symbol)
                if change_24h is None:
                    continue
                
                score = 0
                if target_direction == "BUY" and change_24h < -5:
                    score = abs(change_24h) / 15
                elif target_direction == "SELL" and change_24h > 5:
                    score = abs(change_24h) / 15
                
                if score > 0.2:
                    logger.info(f"🔄 Fallback: {symbol} - {target_direction} (điểm: {score:.2f}, đòn bẩy: {self.required_leverage}x)")
                    return {
                        'symbol': symbol,
                        'direction': target_direction,
                        'score': score,
                        'fallback': True,
                        'qualified': True
                    }
                        
            except Exception as e:
                logger.debug(f"❌ Lỗi fallback {symbol}: {str(e)}")
                continue
        
        return None

# ========== QUẢN LÝ COIN CHUNG ==========
class CoinManager:
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(CoinManager, cls).__new__(cls)
                cls._instance.managed_coins = {}
                cls._instance.config_coin_count = {}
        return cls._instance
    
    def register_coin(self, symbol, bot_id, strategy, config_key=None):
        with self._lock:
            if symbol not in self.managed_coins:
                self.managed_coins[symbol] = {
                    "strategy": strategy, 
                    "bot_id": bot_id,
                    "config_key": config_key
                }
                return True
            return False
    
    def unregister_coin(self, symbol):
        with self._lock:
            if symbol in self.managed_coins:
                del self.managed_coins[symbol]
                return True
            return False
    
    def is_coin_managed(self, symbol):
        with self._lock:
            return symbol in self.managed_coins
    
    def get_managed_coins(self):
        with self._lock:
            return self.managed_coins.copy()

# ========== API BINANCE ==========
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
            if method.upper() == 'GET':
                if params:
                    query = urllib.parse.urlencode(params)
                    url = f"{url}?{query}"
                req = urllib.request.Request(url, headers=headers or {})
            else:
                data = urllib.parse.urlencode(params).encode() if params else None
                req = urllib.request.Request(url, data=data, headers=headers or {}, method=method)
            
            with urllib.request.urlopen(req, timeout=15) as response:
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
            logger.error(f"Lỗi HTTP ({e.code}): {e.reason}")
            if e.code == 401:
                return None
            if e.code == 429:
                time.sleep(2 ** attempt)
            elif e.code >= 500:
                time.sleep(1)
            continue
        except Exception as e:
            logger.error(f"Lỗi kết nối API: {str(e)}")
            time.sleep(1)
    
    logger.error(f"Không thể thực hiện yêu cầu API sau {max_retries} lần thử")
    return None

def get_all_usdt_pairs(limit=600):
    try:
        url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        data = binance_api_request(url)
        if not data:
            logger.warning("Không lấy được dữ liệu từ Binance, trả về danh sách rỗng")
            return []
        
        usdt_pairs = []
        for symbol_info in data.get('symbols', []):
            symbol = symbol_info.get('symbol', '')
            if symbol.endswith('USDT') and symbol_info.get('status') == 'TRADING':
                usdt_pairs.append(symbol)
        
        logger.info(f"✅ Lấy được {len(usdt_pairs)} coin USDT từ Binance")
        return usdt_pairs[:limit] if limit else usdt_pairs
        
    except Exception as e:
        logger.error(f"❌ Lỗi lấy danh sách coin từ Binance: {str(e)}")
        return []

def get_step_size(symbol, api_key, api_secret):
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
    try:
        ts = int(time.time() * 1000)
        params = {"timestamp": ts}
        query = urllib.parse.urlencode(params)
        sig = sign(query, api_secret)
        url = f"https://fapi.binance.com/fapi/v2/account?{query}&signature={sig}"
        headers = {'X-MBX-APIKEY': api_key}
        
        data = binance_api_request(url, headers=headers)
        if not data:
            return None
        for asset in data['assets']:
            if asset['asset'] == 'USDT':
                return float(asset['availableBalance'])
        return 0
    except Exception as e:
        logger.error(f"Lỗi lấy số dư: {str(e)}")
        return None

def place_order(symbol, side, qty, api_key, api_secret):
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
    try:
        url = f"https://fapi.binance.com/fapi/v1/ticker/price?symbol={symbol.upper()}"
        data = binance_api_request(url)
        if data and 'price' in data:
            return float(data['price'])
    except Exception as e:
        logger.error(f"Lỗi lấy giá: {str(e)}")
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

def get_24h_change(symbol):
    try:
        url = f"https://fapi.binance.com/fapi/v1/ticker/24hr?symbol={symbol.upper()}"
        data = binance_api_request(url)
        if data and 'priceChangePercent' in data:
            change = data['priceChangePercent']
            if change is None:
                return 0.0
            return float(change) if change is not None else 0.0
        return 0.0
    except Exception as e:
        logger.error(f"Lỗi lấy biến động 24h cho {symbol}: {str(e)}")
    return 0.0

# ========== WEBSOCKET MANAGER ==========
class WebSocketManager:
    def __init__(self):
        self.connections = {}
        self.executor = ThreadPoolExecutor(max_workers=10)
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        
    def add_symbol(self, symbol, callback):
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
        logger.info(f"WebSocket bắt đầu cho {symbol}")
        
    def _reconnect(self, symbol, callback):
        logger.info(f"Kết nối lại WebSocket cho {symbol}")
        self.remove_symbol(symbol)
        self._create_connection(symbol, callback)
        
    def remove_symbol(self, symbol):
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

# ========== BASE BOT NÂNG CẤP ==========
class BaseBot:
    def __init__(self, symbol, lev, percent, tp, sl, ws_manager, api_key, api_secret, 
                 telegram_bot_token, telegram_chat_id, strategy_name, config_key=None, bot_id=None):
        
        self.symbol = symbol.upper() if symbol else None
        self.lev = lev
        self.percent = percent
        self.tp = tp
        self.sl = sl
        self.ws_manager = ws_manager
        self.api_key = api_key
        self.api_secret = api_secret
        self.telegram_bot_token = telegram_bot_token
        self.telegram_chat_id = telegram_chat_id
        self.strategy_name = strategy_name
        self.config_key = config_key
        self.bot_id = bot_id or f"{strategy_name}_{int(time.time())}_{random.randint(1000, 9999)}"
        
        # === SỬA: LUÔN Ở TRẠNG THÁI TÌM KIẾM, KHÔNG CÓ TRẠNG THÁI CHỜ ===
        self.status = "searching"  # chỉ có searching và open
        self.side = ""
        self.qty = 0
        self.entry = 0
        self.prices = []
        self.position_open = False
        self._stop = False
        
        # Biến theo dõi thời gian
        self.last_trade_time = 0
        self.last_close_time = 0
        self.last_position_check = 0
        self.last_error_log_time = 0
        
        self.cooldown_period = 300
        self.position_check_interval = 30
        
        # Bảo vệ chống lặp đóng lệnh
        self._close_attempted = False
        self._last_close_attempt = 0
        
        # Cờ đánh dấu cần xóa bot
        self.should_be_removed = False
        
        # THÊM THEO DÕI CÂN BẰNG
        self.position_balance_check = 0
        self.balance_check_interval = 60
        
        self.coin_manager = CoinManager()
        
        # === SỬA: KHỞI TẠO COIN FINDER VỚI ĐÒN BẨY ===
        self.coin_finder = SmartCoinFinder(api_key, api_secret, lev)  # Thêm lev
        
        # TRẠNG THÁI TÌM COIN
        self.current_target_direction = None
        self.last_find_time = 0
        self.find_interval = 30  # Giảm thời gian tìm coin xuống 30 giây
        
        self.check_position_status()
        if self.symbol:
            self.ws_manager.add_symbol(self.symbol, self._handle_price_update)
        
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()
        
        # === SỬA: BOT LUÔN BẮT ĐẦU BẰNG TÌM KIẾM COIN ===
        if self.symbol:
            self.log(f"🟢 Bot {strategy_name} khởi động | {self.symbol} | ĐB: {lev}x | Vốn: {percent}% | TP/SL: {tp}%/{sl}%")
            # NGAY LẬP TỨC BẮT ĐẦU TÌM COIN MỚI NẾU CÓ SYMBOL
            self.status = "searching"
        else:
            self.log(f"🟢 Bot {strategy_name} khởi động | Đang tìm coin... | ĐB: {lev}x | Vốn: {percent}% | TP/SL: {tp}%/{sl}%")

    def _register_coin_with_retry(self, symbol):
        max_retries = 3
        for attempt in range(max_retries):
            success = self.coin_manager.register_coin(symbol, self.bot_id, self.strategy_name, self.config_key)
            if success:
                return True
            time.sleep(0.5)
        return False

    def log(self, message):
        bot_info = f"[Bot {self.bot_id}]" if hasattr(self, 'bot_id') else ""
        logger.info(f"{bot_info} [{self.symbol or 'NO_COIN'} - {self.strategy_name}] {message}")
        if self.telegram_bot_token and self.telegram_chat_id:
            symbol_info = f"<b>{self.symbol}</b>" if self.symbol else "<i>Đang tìm coin...</i>"
            send_telegram(f"{symbol_info} ({self.strategy_name} - Bot {self.bot_id}): {message}", 
                         bot_token=self.telegram_bot_token, 
                         default_chat_id=self.telegram_chat_id)

    def _handle_price_update(self, price):
        if self._stop or not price or price <= 0:
            return
        try:
            self.prices.append(float(price))
            if len(self.prices) > 100:
                self.prices = self.prices[-100:]
        except Exception as e:
            self.log(f"❌ Lỗi xử lý giá: {str(e)}")

    def get_signal(self):
        raise NotImplementedError("Phương thức get_signal cần được triển khai")

    def get_target_direction(self):
        """XÁC ĐỊNH HƯỚNG GIAO DỊCH - CHECK TẤT CẢ VỊ THẾ TRÊN BINANCE"""
        try:
            # 🎯 SỬA QUAN TRỌNG: Lấy tất cả vị thế từ Binance, không chỉ từ bot
            all_positions = get_positions(api_key=self.api_key, api_secret=self.api_secret)
            
            buy_count = 0
            sell_count = 0
            position_details = []
            
            # Đếm tất cả vị thế đang mở trên Binance
            for pos in all_positions:
                position_amt = float(pos.get('positionAmt', 0))
                if position_amt != 0:  # Có vị thế mở
                    symbol = pos.get('symbol', 'UNKNOWN')
                    if position_amt > 0:
                        buy_count += 1
                        position_details.append(f"{symbol}(LONG)")
                    else:
                        sell_count += 1
                        position_details.append(f"{symbol}(SHORT)")
            
            total = buy_count + sell_count
            
            self.log(f"🔍 TẤT CẢ VỊ THẾ BINANCE: {buy_count} LONG, {sell_count} SHORT")
            if position_details:
                self.log(f"🔍 Chi tiết: {', '.join(position_details)}")
            
            if total == 0:
                direction = "BUY" if random.random() > 0.5 else "SELL"
                self.log(f"⚖️ Không có vị thế nào trên Binance → RANDOM {direction}")
                return direction
            
            buy_ratio = buy_count / total
            sell_ratio = sell_count / total
            
            self.log(f"📊 TỶ LỆ VỊ THẾ: LONG {buy_ratio:.1%} vs SHORT {sell_ratio:.1%}")
            
            # 🎯 LOGIC CÂN BẰNG DỰA TRÊN TẤT CẢ VỊ THẾ
            if buy_ratio >= 0.6:  # LONG chiếm ≥60%
                self.log(f"⚖️ QUYẾT ĐỊNH: LONG chiếm ưu thế ({buy_ratio:.1%}) → TÌM SHORT")
                return "SELL"
            elif sell_ratio >= 0.6:  # SHORT chiếm ≥60%
                self.log(f"⚖️ QUYẾT ĐỊNH: SHORT chiếm ưu thế ({sell_ratio:.1%}) → TÌM LONG")
                return "BUY"
            elif buy_count > sell_count:  # LONG nhiều hơn SHORT
                self.log(f"⚖️ QUYẾT ĐỊNH: LONG nhiều hơn SHORT ({buy_count} vs {sell_count}) → TÌM SHORT")
                return "SELL"
            elif sell_count > buy_count:  # SHORT nhiều hơn LONG
                self.log(f"⚖️ QUYẾT ĐỊNH: SHORT nhiều hơn LONG ({sell_count} vs {buy_count}) → TÌM LONG")
                return "BUY"
            else:
                # Cân bằng → random
                direction = "BUY" if random.random() > 0.5 else "SELL"
                self.log(f"⚖️ QUYẾT ĐỊNH: Cân bằng ({buy_count} LONG, {sell_count} SHORT) → RANDOM {direction}")
                return direction
                
        except Exception as e:
            self.log(f"❌ Lỗi kiểm tra vị thế Binance: {str(e)}")
            return "BUY" if random.random() > 0.5 else "SELL"

    def find_and_set_coin(self):
        """TÌM VÀ SET COIN MỚI - THÊM LOGGING CÂN BẰNG"""
        try:
            current_time = time.time()
            if current_time - self.last_find_time < self.find_interval:
                return False
            
            self.last_find_time = current_time
            
            # Xác định hướng giao dịch mới
            self.current_target_direction = self.get_target_direction()
            
            # Log rõ lý do chọn hướng
            self.log(f"🎯 Đang tìm coin {self.current_target_direction} để CÂN BẰNG hệ thống")
            
            # Lấy danh sách coin đang được quản lý để tránh trùng lặp
            managed_coins = self.coin_manager.get_managed_coins()
            excluded_symbols = set(managed_coins.keys())
            
            # Log các coin đang được trade
            if excluded_symbols:
                self.log(f"🚫 Tránh các coin đang trade: {', '.join(excluded_symbols)}")
            
            # Tìm coin mới
            coin_data = self.coin_finder.find_coin_by_direction(
                self.current_target_direction, 
                excluded_symbols
            )
        
            
            # 🎯 SỬA LỖI: KIỂM TRA coin_data CÓ TỒN TẠI KHÔNG
            if coin_data is None:
                self.log(f"⚠️ Không tìm thấy coin {self.current_target_direction} phù hợp, thử lại sau")
                return False
                
            # 🎯 SỬA LỖI: KIỂM TRA qualified CÓ TỒN TẠI KHÔNG
            if not coin_data.get('qualified', False):
                self.log(f"⚠️ Coin {coin_data.get('symbol', 'UNKNOWN')} không đủ tiêu chuẩn, thử lại sau")
                return False
            
            new_symbol = coin_data['symbol']
            
            # Đăng ký coin mới
            if self._register_coin_with_retry(new_symbol):
                # Cập nhật symbol và thiết lập WebSocket
                if self.symbol:
                    self.ws_manager.remove_symbol(self.symbol)
                    self.coin_manager.unregister_coin(self.symbol)
                
                self.symbol = new_symbol
                self.ws_manager.add_symbol(self.symbol, self._handle_price_update)
                
                # Log thông tin coin mới
                analysis_info = self._format_coin_analysis(coin_data)
                self.log(f"🎯 Đã tìm thấy coin {new_symbol} - {self.current_target_direction}\n{analysis_info}")
                
                # === SỬA: KHÔNG CHUYỂN SANG WAITING, LUÔN Ở SEARCHING ===
                self.status = "searching"
                return True
            else:
                self.log(f"❌ Không thể đăng ký coin {new_symbol} - có thể đã có bot khác trade")
                return False
                
        except Exception as e:
            self.log(f"❌ Lỗi tìm coin: {str(e)}")
            return False

    def _format_coin_analysis(self, coin_data):
        """Định dạng thông tin phân tích coin"""
        info = ""
        timeframe_data = coin_data.get('timeframe_data', {})
        
        for tf, data in timeframe_data.items():
            stats = data.get('stats', {})
            if stats:
                bullish_pct = stats.get('bullish_ratio', 0) * 100
                info += f"  {tf}: {data['signal']} | Nến tăng: {bullish_pct:.1f}%\n"
        
        return info

    def get_signal_with_balance(self, original_signal):
        """ĐIỀU CHỈNH TÍN HIỆU - CÂN BẰNG VỚI TẤT CẢ VỊ THẾ BINANCE"""
        try:
            current_time = time.time()
            if current_time - self.position_balance_check < self.balance_check_interval:
                return original_signal
            
            self.position_balance_check = current_time
            
            # 🎯 SỬA: Lấy tất cả vị thế từ Binance
            all_positions = get_positions(api_key=self.api_key, api_secret=self.api_secret)
            
            buy_count = 0
            sell_count = 0
            
            for pos in all_positions:
                position_amt = float(pos.get('positionAmt', 0))
                if position_amt != 0:
                    if position_amt > 0:
                        buy_count += 1
                    else:
                        sell_count += 1
            
            total = buy_count + sell_count
            if total == 0:
                return original_signal
            
            buy_ratio = buy_count / total
            sell_ratio = sell_count / total
            
            self.log(f"📊 CÂN BẰNG TÍN HIỆU: {buy_count} LONG, {sell_count} SHORT | Tín hiệu gốc: {original_signal}")
            
            # 🎯 ĐIỀU CHỈNH TÍN HIỆU THEO TẤT CẢ VỊ THẾ
            if original_signal == "BUY" and buy_ratio > 0.6:
                self.log(f"⚖️ ĐIỀU CHỈNH: Nhiều LONG ({buy_ratio:.1%}) + tín hiệu BUY → CHUYỂN SHORT")
                return "SELL"
            elif original_signal == "SELL" and sell_ratio > 0.6:
                self.log(f"⚖️ ĐIỀU CHỈNH: Nhiều SHORT ({sell_ratio:.1%}) + tín hiệu SELL → CHUYỂN LONG")
                return "BUY"
            elif original_signal == "BUY" and buy_ratio > sell_ratio + 0.2:
                self.log(f"⚖️ ĐIỀU CHỈNH: LONG nhiều hơn SHORT → ƯU TIÊN SHORT")
                return "SELL"
            elif original_signal == "SELL" and sell_ratio > buy_ratio + 0.2:
                self.log(f"⚖️ ĐIỀU CHỈNH: SHORT nhiều hơn LONG → ƯU TIÊN LONG")
                return "BUY"
            else:
                self.log(f"⚖️ GIỮ NGUYÊN: Tín hiệu {original_signal} phù hợp với cân bằng")
                return original_signal
            
        except Exception as e:
            self.log(f"❌ Lỗi cân bằng với vị thế Binance: {str(e)}")
            return original_signal

    def check_position_status(self):
        if not self.symbol:
            return
            
        try:
            positions = get_positions(self.symbol, self.api_key, self.api_secret)
            if not positions:
                self._reset_position()
                return
            
            position_found = False
            for pos in positions:
                if pos['symbol'] == self.symbol:
                    position_amt = float(pos.get('positionAmt', 0))
                    if abs(position_amt) > 0:
                        position_found = True
                        self.position_open = True
                        self.status = "open"
                        self.side = "BUY" if position_amt > 0 else "SELL"
                        self.qty = position_amt
                        self.entry = float(pos.get('entryPrice', 0))
                        break
                    else:
                        position_found = True
                        self._reset_position()
                        break
            
            if not position_found:
                self._reset_position()
                
        except Exception as e:
            if time.time() - self.last_error_log_time > 10:
                self.log(f"❌ Lỗi kiểm tra vị thế: {str(e)}")
                self.last_error_log_time = time.time()

    def _reset_position(self):
        self.position_open = False
        # === SỬA: LUÔN CHUYỂN SANG TÌM KIẾM, KHÔNG CÓ TRẠNG THÁI CHỜ ===
        self.status = "searching"
        self.side = ""
        self.qty = 0
        self.entry = 0
        self._close_attempted = False
        self._last_close_attempt = 0

    def _run(self):
        while not self._stop:
            try:
                current_time = time.time()
                
                if current_time - self.last_position_check > self.position_check_interval:
                    self.check_position_status()
                    self.last_position_check = current_time
                
                # === SỬA: LUÔN TÌM COIN NẾU KHÔNG CÓ VỊ THẾ MỞ ===
                if not self.position_open:
                    # NẾU KHÔNG CÓ COIN HOẶC ĐANG TÌM COIN
                    if not self.symbol or self.status == "searching":
                        if self.find_and_set_coin():
                            self.log("✅ Đã tìm thấy coin mới, bắt đầu phân tích...")
                        else:
                            time.sleep(3)  # Giảm thời gian chờ xuống 3 giây
                        continue
                    
                    # === SỬA: BỎ TRẠNG THÁI CHỜ - LUÔN PHÂN TÍCH TÍN HIỆU NGAY ===
                    signal = self.get_signal()
                    
                    # 🎯 CHỈ XỬ LÝ NẾU SIGNAL KHÁC NEUTRAL
                    if signal and signal != "NEUTRAL":
                        # ÁP DỤNG CÂN BẰNG VỊ THẾ
                        balanced_signal = self.get_signal_with_balance(signal)
                        
                        if (balanced_signal and balanced_signal != "NEUTRAL" and
                            current_time - self.last_trade_time > 20 and  # Giảm thời gian chờ xuống 20 giây
                            current_time - self.last_close_time > self.cooldown_period):
                            
                            self.log(f"🎯 Nhận tín hiệu {balanced_signal}, đang mở lệnh...")
                            if self.open_position(balanced_signal):
                                self.last_trade_time = current_time
                            else:
                                time.sleep(5)  # Giảm thời gian chờ khi lỗi
                    else:
                        # Nếu signal là NEUTRAL hoặc None, tiếp tục tìm coin
                        if signal == "NEUTRAL":
                            logger.debug(f"⚪ {self.symbol} - Tín hiệu NEUTRAL, tiếp tục tìm coin")
                            # === SỬA: CHUYỂN SANG TÌM COIN MỚI KHI TÍN HIỆU NEUTRAL ===
                            self.status = "searching"
                            self.symbol = None  # Reset symbol để tìm coin mới
                        time.sleep(2)  # Giảm thời gian chờ
                
                # KIỂM TRA TP/SL
                if self.position_open and not self._close_attempted:
                    self.check_tp_sl()
                    
                time.sleep(1)
                
            except Exception as e:
                if time.time() - self.last_error_log_time > 10:
                    self.log(f"❌ Lỗi hệ thống: {str(e)}")
                    self.last_error_log_time = time.time()
                time.sleep(1)

    def stop(self):
        self._stop = True
        if self.symbol:
            self.ws_manager.remove_symbol(self.symbol)
        if self.symbol:
            self.coin_manager.unregister_coin(self.symbol)
        if self.symbol:
            cancel_all_orders(self.symbol, self.api_key, self.api_secret)
        self.log(f"🔴 Bot dừng")

    def open_position(self, side):
        # 🎯 SỬA QUAN TRỌNG: VALIDATE SIDE TRƯỚC KHI ĐẶT LỆNH
        if side not in ["BUY", "SELL"]:
            self.log(f"❌ Side không hợp lệ: {side}")
            return False
            
        try:
            self.check_position_status()
            if self.position_open:
                self.log(f"⚠️ Đã có vị thế {self.side}, bỏ qua tín hiệu {side}")
                return False
    
            if self.should_be_removed:
                self.log("⚠️ Bot đã được đánh dấu xóa, không mở lệnh mới")
                return False
    
            if not set_leverage(self.symbol, self.lev, self.api_key, self.api_secret):
                self.log(f"❌ Không thể đặt đòn bẩy {self.lev}x")
                return False
    
            balance = get_balance(self.api_key, self.api_secret)
            if balance is None or balance <= 0:
                self.log("❌ Không đủ số dư")
                return False
    
            current_price = get_current_price(self.symbol)
            if current_price <= 0:
                self.log("❌ Lỗi lấy giá")
                return False
    
            step_size = get_step_size(self.symbol, self.api_key, self.api_secret)
            usd_amount = balance * (self.percent / 100)
            qty = (usd_amount * self.lev) / current_price
            
            if step_size > 0:
                qty = math.floor(qty / step_size) * step_size
                qty = round(qty, 8)
    
            if qty < step_size:
                self.log(f"❌ Số lượng quá nhỏ: {qty}")
                return False
    
            # 🎯 THÊM LOG CHI TIẾT TRƯỚC KHI ĐẶT LỆNH
            self.log(f"📊 Đang đặt lệnh {side} - SL: {step_size}, Qty: {qty}, Giá: {current_price}")
            
            result = place_order(self.symbol, side, qty, self.api_key, self.api_secret)
            if result and 'orderId' in result:
                executed_qty = float(result.get('executedQty', 0))
                avg_price = float(result.get('avgPrice', current_price))
                
                if executed_qty >= 0:
                    self.entry = avg_price
                    self.side = side
                    self.qty = executed_qty if side == "BUY" else -executed_qty
                    self.position_open = True
                    self.status = "open"
                    
                    message = (
                        f"✅ <b>ĐÃ MỞ VỊ THẾ {self.symbol}</b>\n"
                        f"🤖 Chiến lược: {self.strategy_name}\n"
                        f"📌 Hướng: {side}\n"
                        f"🏷️ Giá vào: {self.entry:.4f}\n"
                        f"📊 Khối lượng: {executed_qty:.4f}\n"
                        f"💵 Giá trị: {executed_qty * self.entry:.2f} USDT\n"
                        f"💰 Đòn bẩy: {self.lev}x\n"
                        f"🎯 TP: {self.tp}% | 🛡️ SL: {self.sl}%"
                    )
                    self.log(message)
                    return True
                else:
                    self.log(f"❌ Lệnh không khớp - Số lượng: {qty}")
                    return False
            else:
                error_msg = result.get('msg', 'Unknown error') if result else 'No response'
                self.log(f"❌ Lỗi đặt lệnh {side}: {error_msg}")
                
                # 🎯 LOG CHI TIẾT LỖI API
                if result and 'code' in result:
                    self.log(f"📋 Mã lỗi Binance: {result['code']} - {result.get('msg', '')}")
                    
                return False
                    
        except Exception as e:
            self.log(f"❌ Lỗi mở lệnh: {str(e)}")
            return False

    def close_position(self, reason=""):
        try:
            self.check_position_status()
            
            if not self.position_open or abs(self.qty) <= 0:
                self.log(f"⚠️ Không có vị thế để đóng: {reason}")
                if self.symbol:
                    self.coin_manager.unregister_coin(self.symbol)
                # === SỬA: SAU KHI ĐÓNG LỆNH, LUÔN CHUYỂN SANG TÌM COIN MỚI ===
                self.status = "searching"
                self.symbol = None
                return False

            current_time = time.time()
            if self._close_attempted and current_time - self._last_close_attempt < 30:
                self.log(f"⚠️ Đang thử đóng lệnh lần trước, chờ...")
                return False
            
            self._close_attempted = True
            self._last_close_attempt = current_time

            close_side = "SELL" if self.side == "BUY" else "BUY"
            close_qty = abs(self.qty)
            
            cancel_all_orders(self.symbol, self.api_key, self.api_secret)
            time.sleep(0.5)
            
            result = place_order(self.symbol, close_side, close_qty, self.api_key, self.api_secret)
            if result and 'orderId' in result:
                current_price = get_current_price(self.symbol)
                pnl = 0
                if self.entry > 0:
                    if self.side == "BUY":
                        pnl = (current_price - self.entry) * abs(self.qty)
                    else:
                        pnl = (self.entry - current_price) * abs(self.qty)
                
                message = (
                    f"⛔ <b>ĐÃ ĐÓNG VỊ THẾ {self.symbol}</b>\n"
                    f"🤖 Chiến lược: {self.strategy_name}\n"
                    f"📌 Lý do: {reason}\n"
                    f"🏷️ Giá ra: {current_price:.4f}\n"
                    f"📊 Khối lượng: {close_qty:.4f}\n"
                    f"💰 PnL: {pnl:.2f} USDT"
                )
                self.log(message)
                
                # === SỬA QUAN TRỌNG: SAU KHI ĐÓNG LỆNH, LUÔN RESET VÀ TÌM COIN MỚI ===
                if self.symbol:
                    self.coin_manager.unregister_coin(self.symbol)
                    self.ws_manager.remove_symbol(self.symbol)
                
                self._reset_position()
                self.last_close_time = time.time()
                self.symbol = None  # RESET SYMBOL
                self.status = "searching"  # LUÔN CHUYỂN SANG TÌM COIN MỚI
                
                time.sleep(2)
                self.check_position_status()
                
                return True
            else:
                error_msg = result.get('msg', 'Unknown error') if result else 'No response'
                self.log(f"❌ Lỗi đóng lệnh: {error_msg}")
                self._close_attempted = False
                return False
                
        except Exception as e:
            self.log(f"❌ Lỗi đóng lệnh: {str(e)}")
            self._close_attempted = False
            return False

    def check_tp_sl(self):
        if not self.position_open or self.entry <= 0 or self._close_attempted:
            return

        current_price = get_current_price(self.symbol)
        if current_price <= 0:
            return

        if self.side == "BUY":
            profit = (current_price - self.entry) * abs(self.qty)
        else:
            profit = (self.entry - current_price) * abs(self.qty)
            
        invested = self.entry * abs(self.qty) / self.lev
        if invested <= 0:
            return
            
        roi = (profit / invested) * 100

        if self.tp is not None and roi >= self.tp:
            self.close_position(f"✅ Đạt TP {self.tp}% (ROI: {roi:.2f}%)")
        elif self.sl is not None and self.sl > 0 and roi <= -self.sl:
            self.close_position(f"❌ Đạt SL {self.sl}% (ROI: {roi:.2f}%)")

# ========== BOT MULTI-TIMEFRAME ĐỘNG ==========
class DynamicMultiTimeframeBot(BaseBot):
    """Bot động sử dụng tín hiệu đa khung thời gian - MỖI BOT LÀ 1 VÒNG LẶP ĐỘC LẬP"""
    
    def __init__(self, symbol, lev, percent, tp, sl, ws_manager, api_key, api_secret, 
                 telegram_bot_token, telegram_chat_id, config_key=None, bot_id=None):
        
        super().__init__(symbol, lev, percent, tp, sl, ws_manager, api_key, api_secret,
                        telegram_bot_token, telegram_chat_id, "Dynamic Multi-Timeframe", 
                        config_key, bot_id)
        
        self.analyzer = MultiTimeframeAnalyzer()
        self.last_analysis_time = 0
        self.analysis_interval = 300
        
    # trading_bot_lib.py - HỆ THỐNG HOÀN CHỈNH VỚI DANH SÁCH COIN GIỚI HẠN
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

# ========== CẤU HÌNH LOGGING ĐƠN GIẢN ==========
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('bot_trading.log')
        ]
    )
    return logging.getLogger()

logger = setup_logging()

# ========== HÀM TELEGRAM ==========
def send_telegram(message, chat_id=None, reply_markup=None, bot_token=None, default_chat_id=None):
    if not bot_token:
        return
    
    chat_id = chat_id or default_chat_id
    if not chat_id:
        return
    
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML"
    }
    
    if reply_markup:
        payload["reply_markup"] = json.dumps(reply_markup)
    
    try:
        response = requests.post(url, json=payload, timeout=15)
    except Exception:
        pass

# ========== MENU TELEGRAM ĐẦY ĐỦ ==========
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

def create_cancel_keyboard():
    return {
        "keyboard": [[{"text": "❌ Hủy bỏ"}]],
        "resize_keyboard": True,
        "one_time_keyboard": True
    }

def create_strategy_keyboard():
    return {
        "keyboard": [
            [{"text": "⏰ Multi-Timeframe"}],
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

def create_exit_strategy_keyboard():
    return {
        "keyboard": [
            [{"text": "🎯 Chỉ TP/SL cố định"}],
            [{"text": "❌ Hủy bỏ"}]
        ],
        "resize_keyboard": True,
        "one_time_keyboard": True
    }

def create_symbols_keyboard(strategy=None):
    try:
        symbols = get_all_usdt_pairs(limit=12)
        if not symbols:
            symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT", "DOGEUSDT", "XRPUSDT", "DOTUSDT", "LINKUSDT"]
    except:
        symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT", "DOGEUSDT", "XRPUSDT", "DOTUSDT", "LINKUSDT"]
    
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

# ========== POSITION BALANCER ==========
class PositionBalancer:
    """CÂN BẰNG VỊ THẾ TỰ ĐỘNG DỰA TRÊN TỶ LỆ BUY/SELL"""
    
    def __init__(self, bot_manager):
        self.bot_manager = bot_manager
        self.buy_sell_history = []
        self.max_history = 50
        self.imbalance_threshold = 2
        
    def get_current_ratio(self):
        """Lấy tỷ lệ BUY/SELL hiện tại"""
        try:
            buy_count = 0
            sell_count = 0
            
            for bot_id, bot in self.bot_manager.bots.items():
                if bot.position_open:
                    if bot.side == "BUY":
                        buy_count += 1
                    elif bot.side == "SELL":
                        sell_count += 1
            
            total = buy_count + sell_count
            if total == 0:
                return 0.5, 0.5
                
            buy_ratio = buy_count / total
            sell_ratio = sell_count / total
            
            self.buy_sell_history.append((buy_count, sell_count))
            if len(self.buy_sell_history) > self.max_history:
                self.buy_sell_history.pop(0)
                
            return buy_ratio, sell_ratio
            
        except Exception as e:
            return 0.5, 0.5
    
    def get_recommended_direction(self):
        """Đề xuất hướng giao dịch dựa trên cân bằng vị thế"""
        try:
            buy_ratio, sell_ratio = self.get_current_ratio()
            
            if len(self.buy_sell_history) >= 10:
                recent_buys = sum([item[0] for item in self.buy_sell_history[-5:]])
                recent_sells = sum([item[1] for item in self.buy_sell_history[-5:]])
                
                if recent_buys - recent_sells >= self.imbalance_threshold:
                    recommendation = "SELL"
                elif recent_sells - recent_buys >= self.imbalance_threshold:
                    recommendation = "BUY" 
                else:
                    recommendation = "NEUTRAL"
            else:
                recommendation = "NEUTRAL"
            
            return recommendation
            
        except Exception as e:
            return "NEUTRAL"

# ========== MULTI TIMEFRAME ANALYZER HOÀN CHỈNH ==========
class MultiTimeframeAnalyzer:
    """PHÂN TÍCH ĐA KHUNG THỜI GIAN - ĐẦY ĐỦ CHỨC NĂNG"""
    
    def __init__(self):
        self.timeframes = ['1m', '5m', '15m', '30m']
        self.lookback = 200
        
    def analyze_symbol(self, symbol):
        """Phân tích symbol trên 4 khung thời gian"""
        try:
            timeframe_signals = {}
            
            for tf in self.timeframes:
                signal, stats = self.analyze_timeframe(symbol, tf)
                timeframe_signals[tf] = {
                    'signal': signal,
                    'stats': stats,
                    'bullish_ratio': stats['bullish_ratio'] if stats else 0.5
                }
            
            # Tổng hợp tín hiệu
            final_signal = self.aggregate_signals(timeframe_signals)
            return final_signal, timeframe_signals
            
        except Exception as e:
            return "NEUTRAL", {}
    
    def analyze_timeframe(self, symbol, timeframe):
        """Phân tích 1 khung thời gian chi tiết"""
        try:
            klines = self.get_klines(symbol, timeframe, self.lookback)
            if not klines or len(klines) < 50:
                return "NEUTRAL", {}
            
            bullish_count = 0
            bearish_count = 0
            price_changes = []
            
            for i in range(1, min(len(klines), self.lookback)):
                open_price = float(klines[i][1])
                close_price = float(klines[i][4])
                
                if close_price > open_price:
                    bullish_count += 1
                elif close_price < open_price:
                    bearish_count += 1
                
                price_change = ((close_price - open_price) / open_price) * 100
                price_changes.append(price_change)
            
            total_candles = bullish_count + bearish_count
            if total_candles == 0:
                return "NEUTRAL", {}
            
            bullish_ratio = bullish_count / total_candles
            bearish_ratio = bearish_count / total_candles
            
            signal = "NEUTRAL"
            if bullish_ratio > 0.55:
                signal = "SELL"
            elif bearish_ratio > 0.55:
                signal = "BUY"
            
            stats = {
                'bullish_count': bullish_count,
                'bearish_count': bearish_count,
                'total_candles': total_candles,
                'bullish_ratio': bullish_ratio,
                'bearish_ratio': bearish_ratio,
                'avg_change': np.mean(price_changes) if price_changes else 0
            }
            
            return signal, stats
            
        except Exception as e:
            return "NEUTRAL", {}
    
    def aggregate_signals(self, timeframe_signals):
        """Tổng hợp tín hiệu từ các khung thời gian"""
        signals = []
        
        for tf, data in timeframe_signals.items():
            signals.append(data['signal'])
        
        buy_signals = signals.count("BUY")
        sell_signals = signals.count("SELL")
        
        if buy_signals >= 3:
            return "BUY"
        elif sell_signals >= 3:
            return "SELL"
        elif buy_signals >= 2 and (buy_signals + sell_signals) == 2:
            return "BUY"
        elif sell_signals >= 2 and (buy_signals + sell_signals) == 2:
            return "SELL"
        else:
            return "NEUTRAL"
    
    def get_klines(self, symbol, interval, limit):
        """Lấy dữ liệu nến từ Binance"""
        try:
            url = "https://fapi.binance.com/fapi/v1/klines"
            params = {
                'symbol': symbol.upper(),
                'interval': interval,
                'limit': limit
            }
            return binance_api_request(url, params=params)
        except Exception as e:
            return None

# ========== COIN MANAGER MỚI ==========
class CoinManager:
    """QUẢN LÝ DANH SÁCH COIN ĐANG GIAO DỊCH"""
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(CoinManager, cls).__new__(cls)
                cls._instance.trading_coins = set()  # DANH SÁCH COIN ĐANG GIAO DỊCH
                cls._instance.max_coins = 0  # SỐ LƯỢNG COIN TỐI ĐA
        return cls._instance
    
    def set_max_coins(self, max_coins):
        """Thiết lập số lượng coin tối đa"""
        with self._lock:
            self.max_coins = max_coins
    
    def can_add_coin(self):
        """Kiểm tra có thể thêm coin mới không"""
        with self._lock:
            return len(self.trading_coins) < self.max_coins
    
    def add_coin(self, symbol):
        """Thêm coin vào danh sách đang giao dịch"""
        with self._lock:
            if len(self.trading_coins) < self.max_coins:
                self.trading_coins.add(symbol)
                return True
            return False
    
    def remove_coin(self, symbol):
        """Xóa coin khỏi danh sách đang giao dịch"""
        with self._lock:
            if symbol in self.trading_coins:
                self.trading_coins.remove(symbol)
                return True
            return False
    
    def get_trading_coins(self):
        """Lấy danh sách coin đang giao dịch"""
        with self._lock:
            return self.trading_coins.copy()
    
    def get_available_slots(self):
        """Lấy số slot còn trống"""
        with self._lock:
            return max(0, self.max_coins - len(self.trading_coins))

# ========== SMART COIN FINDER NÂNG CẤP ==========
class SmartCoinFinder:
    """TÌM COIN THÔNG MINH VỚI TÍNH ĐIỂM CHẤT LƯỢNG"""
    
    def __init__(self, api_key, api_secret, required_leverage):
        self.api_key = api_key
        self.api_secret = api_secret
        self.required_leverage = required_leverage
        self.analyzer = MultiTimeframeAnalyzer()
        self.leverage_cache = {}
        
    def check_leverage_support(self, symbol):
        """KIỂM TRA COIN CÓ HỖ TRỢ ĐÒN BẨY YÊU CẦU KHÔNG"""
        try:
            if symbol in self.leverage_cache:
                return self.leverage_cache[symbol]
                
            max_leverage = self.get_max_leverage(symbol)
            if max_leverage and max_leverage >= self.required_leverage:
                self.leverage_cache[symbol] = True
                return True
            else:
                self.leverage_cache[symbol] = False
                return False
                
        except Exception as e:
            return False
    
    def get_max_leverage(self, symbol):
        """LẤY THÔNG TIN ĐÒN BẨY TỐI ĐA CỦA COIN"""
        try:
            ts = int(time.time() * 1000)
            params = {"timestamp": ts}
            query = urllib.parse.urlencode(params)
            sig = sign(query, self.api_secret)
            url = f"https://fapi.binance.com/fapi/v1/leverageBracket?{query}&signature={sig}"
            headers = {'X-MBX-APIKEY': self.api_key}
            
            data = binance_api_request(url, headers=headers)
            if not data:
                return None
                
            for bracket in data:
                if bracket['symbol'] == symbol:
                    max_leverage = bracket['brackets'][0]['initialLeverage']
                    return max_leverage
                    
            return None
            
        except Exception as e:
            return None

    def find_best_coin(self, target_direction, excluded_symbols=None):
        """TÌM COIN TỐT NHẤT VỚI KIỂM TRA ĐÒN BẨY"""
        try:
            if excluded_symbols is None:
                excluded_symbols = set()
            
            # Lấy danh sách coin USDT
            all_symbols = get_all_usdt_pairs(limit=300)
            if not all_symbols:
                return None
            
            # Xáo trộn và tìm coin tốt nhất
            random.shuffle(all_symbols)
            best_coin = None
            best_score = 0
            
            for symbol in all_symbols:
                try:
                    # Skip các symbol bị exclude
                    if symbol in ['BTCUSDT', 'ETHUSDT', 'BNBUSDT'] or symbol in excluded_symbols:
                        continue
                    
                    # Kiểm tra đòn bẩy
                    if not self.check_leverage_support(symbol):
                        continue
                    
                    # Phân tích coin
                    result = self.analyze_symbol_for_finding(symbol, target_direction)
                    if result and result.get('qualified', False):
                        score = result['score']
                        if score > best_score:
                            best_score = score
                            best_coin = result
                            
                except Exception:
                    continue
            
            return best_coin
            
        except Exception:
            return None
    
    def analyze_symbol_for_finding(self, symbol, target_direction):
        """Phân tích chi tiết một symbol"""
        try:
            # Phân tích đa khung thời gian
            signal, timeframe_data = self.analyzer.analyze_symbol(symbol)
            
            if signal != target_direction:
                return None
            
            # Tính điểm chất lượng
            score = self.calculate_quality_score(timeframe_data, target_direction)
            
            if score >= 0.3:
                return {
                    'symbol': symbol,
                    'direction': target_direction,
                    'score': score,
                    'timeframe_data': timeframe_data,
                    'qualified': True
                }
            
            return None
            
        except Exception:
            return None
        
    def calculate_quality_score(self, timeframe_data, target_direction):
        """Tính điểm chất lượng chi tiết"""
        try:
            total_score = 0
            max_score = 0
            
            for tf, data in timeframe_data.items():
                stats = data.get('stats', {})
                if not stats:
                    continue
                    
                bullish_ratio = stats.get('bullish_ratio', 0.5)
                total_candles = stats.get('total_candles', 0)
                avg_change = abs(stats.get('avg_change', 0))
                
                # Điểm cho độ rõ ràng của tín hiệu
                if target_direction == "SELL":
                    clarity_score = max(0, (bullish_ratio - 0.52)) * 3
                else:  # BUY
                    clarity_score = max(0, ((1 - bullish_ratio) - 0.52)) * 3
                
                # Điểm cho số lượng nến (độ tin cậy)
                volume_score = min(total_candles / 100, 1.0)
                
                # Điểm cho biến động giá
                volatility_score = min(avg_change / 0.3, 1.0)
                
                # Tổng điểm cho khung thời gian này
                tf_score = (clarity_score * 0.6 + volume_score * 0.2 + volatility_score * 0.2)
                total_score += tf_score
                max_score += 1.0
            
            final_score = total_score / max_score if max_score > 0 else 0
            return final_score
            
        except Exception:
            return 0

# ========== API BINANCE (GIỮ NGUYÊN) ==========
def sign(query, api_secret):
    try:
        return hmac.new(api_secret.encode(), query.encode(), hashlib.sha256).hexdigest()
    except Exception:
        return ""

def binance_api_request(url, method='GET', params=None, headers=None):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            if method.upper() == 'GET':
                if params:
                    query = urllib.parse.urlencode(params)
                    url = f"{url}?{query}"
                req = urllib.request.Request(url, headers=headers or {})
            else:
                data = urllib.parse.urlencode(params).encode() if params else None
                req = urllib.request.Request(url, data=data, headers=headers or {}, method=method)
            
            with urllib.request.urlopen(req, timeout=15) as response:
                if response.status == 200:
                    return json.loads(response.read().decode())
                else:
                    if response.status == 429:
                        time.sleep(2 ** attempt)
                    elif response.status >= 500:
                        time.sleep(1)
                    continue
        except Exception:
            time.sleep(1)
            continue
    
    return None

def get_all_usdt_pairs(limit=600):
    try:
        url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        data = binance_api_request(url)
        if not data:
            return []
        
        usdt_pairs = []
        for symbol_info in data.get('symbols', []):
            symbol = symbol_info.get('symbol', '')
            if symbol.endswith('USDT') and symbol_info.get('status') == 'TRADING':
                usdt_pairs.append(symbol)
        
        return usdt_pairs[:limit] if limit else usdt_pairs
        
    except Exception:
        return []

def get_step_size(symbol, api_key, api_secret):
    try:
        url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        data = binance_api_request(url)
        if not data:
            return 0.001
        for s in data['symbols']:
            if s['symbol'] == symbol.upper():
                for f in s['filters']:
                    if f['filterType'] == 'LOT_SIZE':
                        return float(f['stepSize'])
    except Exception:
        return 0.001

def set_leverage(symbol, lev, api_key, api_secret):
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
        return response is not None
    except Exception:
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
        if not data:
            return None
        for asset in data['assets']:
            if asset['asset'] == 'USDT':
                return float(asset['availableBalance'])
        return 0
    except Exception:
        return None

def place_order(symbol, side, qty, api_key, api_secret):
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
    except Exception:
        return None

def cancel_all_orders(symbol, api_key, api_secret):
    try:
        ts = int(time.time() * 1000)
        params = {"symbol": symbol.upper(), "timestamp": ts}
        query = urllib.parse.urlencode(params)
        sig = sign(query, api_secret)
        url = f"https://fapi.binance.com/fapi/v1/allOpenOrders?{query}&signature={sig}"
        headers = {'X-MBX-APIKEY': api_key}
        
        binance_api_request(url, method='DELETE', headers=headers)
        return True
    except Exception:
        return False

def get_current_price(symbol):
    try:
        url = f"https://fapi.binance.com/fapi/v1/ticker/price?symbol={symbol.upper()}"
        data = binance_api_request(url)
        if data and 'price' in data:
            return float(data['price'])
    except Exception:
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
    except Exception:
        return []

def get_24h_change(symbol):
    try:
        url = f"https://fapi.binance.com/fapi/v1/ticker/24hr?symbol={symbol.upper()}"
        data = binance_api_request(url)
        if data and 'priceChangePercent' in data:
            change = data['priceChangePercent']
            return float(change) if change is not None else 0.0
        return 0.0
    except Exception:
        return 0.0

# ========== WEBSOCKET MANAGER ==========
class WebSocketManager:
    def __init__(self):
        self.connections = {}
        self.executor = ThreadPoolExecutor(max_workers=10)
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        
    def add_symbol(self, symbol, callback):
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
            except Exception:
                pass
                
        def on_error(ws, error):
            if not self._stop_event.is_set():
                time.sleep(5)
                self._reconnect(symbol, callback)
            
        def on_close(ws, close_status_code, close_msg):
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
        
    def _reconnect(self, symbol, callback):
        self.remove_symbol(symbol)
        self._create_connection(symbol, callback)
        
    def remove_symbol(self, symbol):
        symbol = symbol.upper()
        with self._lock:
            if symbol in self.connections:
                try:
                    self.connections[symbol]['ws'].close()
                except Exception:
                    pass
                del self.connections[symbol]
                
    def stop(self):
        self._stop_event.set()
        for symbol in list(self.connections.keys()):
            self.remove_symbol(symbol)

# ========== BASE BOT NÂNG CẤP ==========
class BaseBot:
    def __init__(self, symbol, lev, percent, tp, sl, ws_manager, api_key, api_secret, 
                 telegram_bot_token, telegram_chat_id, strategy_name, config_key=None, bot_id=None):
        
        self.symbol = symbol.upper() if symbol else None
        self.lev = lev
        self.percent = percent
        self.tp = tp
        self.sl = sl
        self.ws_manager = ws_manager
        self.api_key = api_key
        self.api_secret = api_secret
        self.telegram_bot_token = telegram_bot_token
        self.telegram_chat_id = telegram_chat_id
        self.strategy_name = strategy_name
        self.config_key = config_key
        self.bot_id = bot_id or f"{strategy_name}_{int(time.time())}_{random.randint(1000, 9999)}"
        
        # Trạng thái bot
        self.status = "searching"
        self.side = ""
        self.qty = 0
        self.entry = 0
        self.position_open = False
        self._stop = False
        
        # Biến thời gian
        self.last_trade_time = 0
        self.last_close_time = 0
        self.last_position_check = 0
        
        self.cooldown_period = 300
        self.position_check_interval = 30
        
        # Bảo vệ chống lặp đóng lệnh
        self._close_attempted = False
        self._last_close_attempt = 0
        
        # Quản lý coin
        self.coin_manager = CoinManager()
        self.coin_finder = SmartCoinFinder(api_key, api_secret, lev)
        
        # Tìm coin
        self.current_target_direction = None
        self.last_find_time = 0
        self.find_interval = 5
        
        self.check_position_status()
        if self.symbol:
            self.ws_manager.add_symbol(self.symbol, self._handle_price_update)
        
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()
        
        self.log(f"🟢 Bot {strategy_name} khởi động | ĐB: {lev}x | Vốn: {percent}% | TP/SL: {tp}%/{sl}%")

    def log(self, message):
        """CHỈ LOG KHI ĐÓNG/MỞ VỊ THẾ"""
        logger.info(message)
        if self.telegram_bot_token and self.telegram_chat_id:
            send_telegram(message, 
                         bot_token=self.telegram_bot_token, 
                         default_chat_id=self.telegram_chat_id)

    def _handle_price_update(self, price):
        if self._stop or not price or price <= 0:
            return
        try:
            pass  # Không xử lý gì, chỉ để tránh lỗi
        except Exception:
            pass

    def get_signal(self):
        raise NotImplementedError("Phương thức get_signal cần được triển khai")

    def get_target_direction(self):
        """XÁC ĐỊNH HƯỚNG GIAO DỊCH"""
        try:
            all_positions = get_positions(api_key=self.api_key, api_secret=self.api_secret)
            
            buy_count = 0
            sell_count = 0
            
            for pos in all_positions:
                position_amt = float(pos.get('positionAmt', 0))
                if position_amt != 0:
                    if position_amt > 0:
                        buy_count += 1
                    else:
                        sell_count += 1
            
            total = buy_count + sell_count
            if total == 0:
                return "BUY" if random.random() > 0.5 else "SELL"
            
            if buy_count > sell_count:
                return "SELL"
            elif sell_count > buy_count:
                return "BUY"
            else:
                return "BUY" if random.random() > 0.5 else "SELL"
                
        except Exception:
            return "BUY" if random.random() > 0.5 else "SELL"

    def find_and_set_coin(self):
        """TÌM VÀ SET COIN MỚI - VỚI DANH SÁCH GIỚI HẠN"""
        current_time = time.time()
        if current_time - self.last_find_time < self.find_interval:
            return False
        
        self.last_find_time = current_time
        
        # 🎯 KIỂM TRA CÒN SLOT TRỐNG KHÔNG
        if not self.coin_manager.can_add_coin():
            return False
        
        # Xác định hướng giao dịch
        self.current_target_direction = self.get_target_direction()
        
        # Lấy danh sách coin đang giao dịch để tránh trùng
        trading_coins = self.coin_manager.get_trading_coins()
        
        # Tìm coin tốt nhất
        coin_data = self.coin_finder.find_best_coin(self.current_target_direction, trading_coins)
        
        if coin_data and coin_data.get('qualified', False):
            new_symbol = coin_data['symbol']
            
            # 🎯 THÊM COIN VÀO DANH SÁCH
            if self.coin_manager.add_coin(new_symbol):
                # Cập nhật symbol và thiết lập WebSocket
                if self.symbol:
                    self.ws_manager.remove_symbol(self.symbol)
                
                self.symbol = new_symbol
                self.ws_manager.add_symbol(self.symbol, self._handle_price_update)
                return True
        
        return False

    def check_position_status(self):
        if not self.symbol:
            return
            
        try:
            positions = get_positions(self.symbol, self.api_key, self.api_secret)
            if not positions:
                self._reset_position()
                return
            
            position_found = False
            for pos in positions:
                if pos['symbol'] == self.symbol:
                    position_amt = float(pos.get('positionAmt', 0))
                    if abs(position_amt) > 0:
                        position_found = True
                        self.position_open = True
                        self.status = "open"
                        self.side = "BUY" if position_amt > 0 else "SELL"
                        self.qty = position_amt
                        self.entry = float(pos.get('entryPrice', 0))
                        break
                    else:
                        position_found = True
                        self._reset_position()
                        break
            
            if not position_found:
                self._reset_position()
                
        except Exception:
            pass

    def _reset_position(self):
        """RESET TRẠNG THÁI VÀ XÓA COIN KHỎI DANH SÁCH"""
        if self.position_open and self.symbol:
            # 🎯 XÓA COIN KHỎI DANH SÁCH KHI ĐÓNG VỊ THẾ
            self.coin_manager.remove_coin(self.symbol)
            self.ws_manager.remove_symbol(self.symbol)
            
        self.position_open = False
        self.status = "searching"
        self.side = ""
        self.qty = 0
        self.entry = 0
        self._close_attempted = False
        self._last_close_attempt = 0

    def _run(self):
        while not self._stop:
            try:
                current_time = time.time()
                
                if current_time - self.last_position_check > self.position_check_interval:
                    self.check_position_status()
                    self.last_position_check = current_time
                
                if not self.position_open:
                    # TÌM COIN MỚI NẾU CHƯA CÓ VỊ THẾ
                    if not self.symbol or self.status == "searching":
                        self.find_and_set_coin()
                        time.sleep(2)
                        continue
                    
                    # PHÂN TÍCH TÍN HIỆU
                    signal = self.get_signal()
                    
                    if signal and signal != "NEUTRAL":
                        if (current_time - self.last_trade_time > 20 and
                            current_time - self.last_close_time > self.cooldown_period):
                            
                            if self.open_position(signal):
                                self.last_trade_time = current_time
                            else:
                                time.sleep(5)
                    else:
                        if signal == "NEUTRAL":
                            self.status = "searching"
                            self.symbol = None
                        time.sleep(2)
                
                # KIỂM TRA TP/SL
                if self.position_open and not self._close_attempted:
                    self.check_tp_sl()
                    
                time.sleep(1)
                
            except Exception:
                time.sleep(1)

    def stop(self):
        self._stop = True
        if self.symbol:
            self.ws_manager.remove_symbol(self.symbol)
            self.coin_manager.remove_coin(self.symbol)
        self.log(f"🔴 Bot dừng")

    def open_position(self, side):
        try:
            self.check_position_status()
            if self.position_open:
                return False
    
            if not set_leverage(self.symbol, self.lev, self.api_key, self.api_secret):
                return False
    
            balance = get_balance(self.api_key, self.api_secret)
            if balance is None or balance <= 0:
                return False
    
            current_price = get_current_price(self.symbol)
            if current_price <= 0:
                return False
    
            step_size = get_step_size(self.symbol, self.api_key, self.api_secret)
            usd_amount = balance * (self.percent / 100)
            qty = (usd_amount * self.lev) / current_price
            
            if step_size > 0:
                qty = math.floor(qty / step_size) * step_size
                qty = round(qty, 8)
    
            if qty < step_size:
                return False
    
            result = place_order(self.symbol, side, qty, self.api_key, self.api_secret)
            if result and 'orderId' in result:
                executed_qty = float(result.get('executedQty', 0))
                avg_price = float(result.get('avgPrice', current_price))
                
                if executed_qty >= 0:
                    self.entry = avg_price
                    self.side = side
                    self.qty = executed_qty if side == "BUY" else -executed_qty
                    self.position_open = True
                    self.status = "open"
                    
                    message = (
                        f"✅ <b>MỞ VỊ THẾ {self.symbol}</b>\n"
                        f"🤖 Chiến lược: {self.strategy_name}\n"
                        f"📌 Hướng: {side}\n"
                        f"🏷️ Giá vào: {self.entry:.4f}\n"
                        f"📊 Khối lượng: {executed_qty:.4f}\n"
                        f"💰 Đòn bẩy: {self.lev}x\n"
                        f"🎯 TP: {self.tp}% | 🛡️ SL: {self.sl}%"
                    )
                    self.log(message)
                    return True
                else:
                    return False
            else:
                return False
                    
        except Exception:
            return False

    def close_position(self, reason=""):
        try:
            self.check_position_status()
            
            if not self.position_open or abs(self.qty) <= 0:
                self._reset_position()
                return False

            current_time = time.time()
            if self._close_attempted and current_time - self._last_close_attempt < 30:
                return False
            
            self._close_attempted = True
            self._last_close_attempt = current_time

            close_side = "SELL" if self.side == "BUY" else "BUY"
            close_qty = abs(self.qty)
            
            cancel_all_orders(self.symbol, self.api_key, self.api_secret)
            time.sleep(0.5)
            
            result = place_order(self.symbol, close_side, close_qty, self.api_key, self.api_secret)
            if result and 'orderId' in result:
                current_price = get_current_price(self.symbol)
                pnl = 0
                if self.entry > 0:
                    if self.side == "BUY":
                        pnl = (current_price - self.entry) * abs(self.qty)
                    else:
                        pnl = (self.entry - current_price) * abs(self.qty)
                
                message = (
                    f"⛔ <b>ĐÓNG VỊ THẾ {self.symbol}</b>\n"
                    f"🤖 Chiến lược: {self.strategy_name}\n"
                    f"📌 Lý do: {reason}\n"
                    f"🏷️ Giá ra: {current_price:.4f}\n"
                    f"📊 Khối lượng: {close_qty:.4f}\n"
                    f"💰 PnL: {pnl:.2f} USDT"
                )
                self.log(message)
                
                # 🎯 XÓA COIN VÀ RESET
                self._reset_position()
                self.last_close_time = time.time()
                
                time.sleep(2)
                self.check_position_status()
                
                return True
            else:
                self._close_attempted = False
                return False
                
        except Exception:
            self._close_attempted = False
            return False

    def check_tp_sl(self):
        if not self.position_open or self.entry <= 0 or self._close_attempted:
            return

        current_price = get_current_price(self.symbol)
        if current_price <= 0:
            return

        if self.side == "BUY":
            profit = (current_price - self.entry) * abs(self.qty)
        else:
            profit = (self.entry - current_price) * abs(self.qty)
            
        invested = self.entry * abs(self.qty) / self.lev
        if invested <= 0:
            return
            
        roi = (profit / invested) * 100

        if self.tp is not None and roi >= self.tp:
            self.close_position(f"✅ Đạt TP {self.tp}% (ROI: {roi:.2f}%)")
        elif self.sl is not None and self.sl > 0 and roi <= -self.sl:
            self.close_position(f"❌ Đạt SL {self.sl}% (ROI: {roi:.2f}%)")

# ========== BOT MULTI-TIMEFRAME ĐỘNG ==========
class DynamicMultiTimeframeBot(BaseBot):
    """Bot động sử dụng tín hiệu đa khung thời gian"""
    
    def __init__(self, symbol, lev, percent, tp, sl, ws_manager, api_key, api_secret, 
                 telegram_bot_token, telegram_chat_id, config_key=None, bot_id=None):
        
        super().__init__(symbol, lev, percent, tp, sl, ws_manager, api_key, api_secret,
                        telegram_bot_token, telegram_chat_id, "Dynamic Multi-Timeframe", 
                        config_key, bot_id)
        
        self.analyzer = MultiTimeframeAnalyzer()
        self.last_analysis_time = 0
        self.analysis_interval = 300
        
    def get_signal(self):
        """Lấy tín hiệu ngẫu nhiên chỉ BUY hoặc SELL"""
        if not self.symbol:
            return None
            
        try:
            current_time = time.time()
            if current_time - self.last_analysis_time < self.analysis_interval:
                return None
            
            self.last_analysis_time = current_time
            
            # Chỉ random BUY hoặc SELL (50/50)
            signal = random.choice(["BUY", "SELL"])
            
            self.log(f"🎯 Tín hiệu NGẪU NHIÊN: {signal}")
            
            return signal
            
        except Exception as e:
            self.log(f"❌ Lỗi tạo tín hiệu ngẫu nhiên: {str(e)}")
            return None
# ========== BOT MANAGER HOÀN CHỈNH ==========
class BotManager:
    def __init__(self, api_key=None, api_secret=None, telegram_bot_token=None, telegram_chat_id=None):
        self.ws_manager = WebSocketManager()
        self.bots = {}  # 🎯 CHỈ 1 BOT DUY NHẤT
        self.running = True
        self.start_time = time.time()
        self.user_states = {}
        
        self.api_key = api_key
        self.api_secret = api_secret
        self.telegram_bot_token = telegram_bot_token
        self.telegram_chat_id = telegram_chat_id
        
        # 🎯 QUẢN LÝ COIN GIỚI HẠN
        self.coin_manager = CoinManager()
        self.position_balancer = PositionBalancer(self)
        
        if api_key and api_secret:
            self._verify_api_connection()
            self.log("🟢 HỆ THỐNG BOT ĐƠN ĐÃ KHỞI ĐỘNG - 1 BOT DUY NHẤT")
            
            self.telegram_thread = threading.Thread(target=self._telegram_listener, daemon=True)
            self.telegram_thread.start()
            
            if self.telegram_chat_id:
                self.send_main_menu(self.telegram_chat_id)
        else:
            self.log("⚡ BotManager khởi động ở chế độ không config")

    def _verify_api_connection(self):
        """KIỂM TRA KẾT NỐI API BINANCE"""
        balance = get_balance(self.api_key, self.api_secret)
        if balance is None:
            self.log("❌ LỖI: Không thể kết nối Binance API.")
        else:
            self.log(f"✅ Kết nối Binance thành công! Số dư: {balance:.2f} USDT")

    def log(self, message):
        """GHI LOG HỆ THỐNG"""
        logger.info(f"[SYSTEM] {message}")
        if self.telegram_bot_token and self.telegram_chat_id:
            send_telegram(f"<b>SYSTEM</b>: {message}", 
                         bot_token=self.telegram_bot_token, 
                         default_chat_id=self.telegram_chat_id)

    def send_main_menu(self, chat_id):
        """GỬI MENU CHÍNH TELEGRAM"""
        welcome = (
            "🤖 <b>BOT GIAO DỊCH FUTURES ĐƠN</b>\n\n"
            "🔢 <b>HỆ THỐNG 1 BOT DUY NHẤT</b>\n"
            "• 🤖 Chỉ 1 bot quét toàn bộ Binance\n"
            "• 🔢 Giới hạn số lượng coin tối đa\n"  
            "• 🔄 Tự động thay thế coin khi đóng lệnh\n"
            "• ⚖️ Cân bằng vị thế tự động"
        )
        send_telegram(welcome, chat_id, create_main_menu(),
                     bot_token=self.telegram_bot_token, 
                     default_chat_id=self.telegram_chat_id)

    def set_max_coins(self, max_coins):
        """THIẾT LẬP SỐ LƯỢNG COIN TỐI ĐA"""
        self.coin_manager.set_max_coins(max_coins)
        self.log(f"🔢 Đã thiết lập số coin tối đa: {max_coins}")

    def add_bot(self, lev, percent, tp, sl, max_coins):
        """TẠO 1 BOT DUY NHẤT VỚI GIỚI HẠN COIN"""
        if sl == 0:
            sl = None
            
        if not self.api_key or not self.api_secret:
            self.log("❌ Chưa thiết lập API Key trong BotManager")
            return False
        
        # Kiểm tra kết nối Binance
        test_balance = get_balance(self.api_key, self.api_secret)
        if test_balance is None:
            self.log("❌ LỖI: Không thể kết nối Binance")
            return False
        
        # 🎯 CHỈ CHO PHÉP 1 BOT
        if self.bots:
            self.log("⚠️ Hệ thống chỉ hỗ trợ 1 bot duy nhất")
            return False
        
        # Thiết lập số lượng coin tối đa
        self.coin_manager.set_max_coins(max_coins)
        
        try:
            # 🎯 TẠO BOT ĐỘNG DUY NHẤT
            bot_id = f"MAIN_BOT_{int(time.time())}"
            
            bot = DynamicMultiTimeframeBot(
                symbol=None,  # Bot động - tự tìm coin
                lev=lev, 
                percent=percent, 
                tp=tp, 
                sl=sl, 
                ws_manager=self.ws_manager,
                api_key=self.api_key, 
                api_secret=self.api_secret, 
                telegram_bot_token=self.telegram_bot_token,
                telegram_chat_id=self.telegram_chat_id, 
                bot_id=bot_id
            )
            
            self.bots[bot_id] = bot
            
            success_msg = (
                f"✅ <b>BOT DUY NHẤT ĐÃ ĐƯỢC TẠO</b>\n\n"
                f"💰 Đòn bẩy: {lev}x\n"
                f"📊 % Số dư: {percent}%\n"
                f"🎯 TP: {tp}%\n"
                f"🛡️ SL: {sl}%\n"
                f"🔢 Coin tối đa: {max_coins}\n\n"
                f"🔄 <b>Bot sẽ:</b>\n"
                f"• Quét TOÀN BỘ Binance\n"
                f"• Tìm coin tốt nhất với đòn bẩy {lev}x\n" 
                f"• DỪNG tìm khi đủ {max_coins} coin\n"
                f"• Tự thay thế coin khi đóng lệnh\n"
                f"• Bỏ qua coin lỗi, tìm coin khác"
            )
            
            self.log(success_msg)
            return True
            
        except Exception as e:
            self.log(f"❌ Lỗi tạo bot: {str(e)}")
            return False

    def stop_bot(self, bot_id):
        """DỪNG BOT CỤ THỂ"""
        bot = self.bots.get(bot_id)
        if bot:
            bot.stop()
            del self.bots[bot_id]
            self.log(f"⛔ Đã dừng bot {bot_id}")
            return True
        return False

    def stop_all(self):
        """DỪNG TẤT CẢ BOT VÀ HỆ THỐNG"""
        self.log("⛔ Đang dừng tất cả bot...")
        for bot_id in list(self.bots.keys()):
            self.stop_bot(bot_id)
        self.ws_manager.stop()
        self.running = False
        self.log("🔴 Hệ thống đã dừng")

    def get_status_summary(self):
        """LẤY THỐNG KÊ TỔNG QUAN HỆ THỐNG"""
        try:
            # Lấy tất cả vị thế từ Binance
            all_positions = get_positions(api_key=self.api_key, api_secret=self.api_secret)
            binance_buy_count = 0
            binance_sell_count = 0
            binance_positions = []
            
            for pos in all_positions:
                position_amt = float(pos.get('positionAmt', 0))
                if position_amt != 0:
                    symbol = pos.get('symbol', 'UNKNOWN')
                    if position_amt > 0:
                        binance_buy_count += 1
                        binance_positions.append(f"{symbol}(LONG)")
                    else:
                        binance_sell_count += 1
                        binance_positions.append(f"{symbol}(SHORT)")
            
            # Thống kê coin
            trading_coins = self.coin_manager.get_trading_coins()
            available_slots = self.coin_manager.get_available_slots()
            
            if not self.bots:
                return "🤖 <b>CHƯA CÓ BOT NÀO</b>\n\nNhấn '➕ Thêm Bot' để bắt đầu"
            
            bot = list(self.bots.values())[0]  # 🎯 CHỈ 1 BOT
            current_coin = bot.symbol if bot.symbol else "Đang tìm..."
            status = "🟢 Đang trade" if bot.position_open else "🔍 Đang tìm coin"
            
            total_binance = binance_buy_count + binance_sell_count
            
            summary = (
                f"🤖 <b>BOT DUY NHẤT</b> | {status}\n\n"
                f"💰 Đòn bẩy: {bot.lev}x\n"
                f"📊 % Số dư: {bot.percent}%\n"
                f"🎯 TP: {bot.tp}% | 🛡️ SL: {bot.sl}%\n"
                f"🔢 Coin hiện tại: {current_coin}\n\n"
                f"📈 <b>COIN GIỚI HẠN: {self.coin_manager.max_coins}</b>\n"
                f"🟢 Đang giao dịch: {len(trading_coins)} coin\n"
                f"🟡 Còn trống: {available_slots} slot\n"
            )
            
            if trading_coins:
                summary += f"\n🔗 <b>Danh sách coin:</b>\n"
                for coin in trading_coins:
                    summary += f"• {coin}\n"
            
            if total_binance > 0:
                binance_buy_ratio = binance_buy_count / total_binance
                binance_sell_ratio = binance_sell_count / total_binance
                
                summary += (
                    f"\n💰 <b>VỊ THẾ BINANCE: {total_binance}</b>\n"
                    f"🟢 LONG: {binance_buy_count} ({binance_buy_ratio:.1%})\n"
                    f"🔴 SHORT: {binance_sell_count} ({binance_sell_ratio:.1%})\n"
                )
                
                if binance_positions:
                    if len(binance_positions) > 6:
                        summary += f"📋 {', '.join(binance_positions[:6])} + {len(binance_positions) - 6} more..."
                    else:
                        summary += f"📋 {', '.join(binance_positions)}"
            
            # Đề xuất cân bằng
            if total_binance > 0:
                if binance_buy_ratio > 0.6:
                    summary += f"\n\n⚖️ <b>ĐỀ XUẤT:</b> Nhiều LONG → ƯU TIÊN TÌM SHORT"
                elif binance_sell_ratio > 0.6:
                    summary += f"\n\n⚖️ <b>ĐỀ XUẤT:</b> Nhiều SHORT → ƯU TIÊN TÌM LONG"
                else:
                    summary += f"\n\n⚖️ <b>TRẠNG THÁI:</b> Cân bằng tốt"
                    
            return summary
                    
        except Exception as e:
            return f"❌ Lỗi thống kê: {str(e)}"

    def _telegram_listener(self):
        """LẮNG NGHE TIN NHẮN TELEGRAM"""
        last_update_id = 0
        
        while self.running and self.telegram_bot_token:
            try:
                url = f"https://api.telegram.org/bot{self.telegram_bot_token}/getUpdates?offset={last_update_id+1}&timeout=30"
                response = requests.get(url, timeout=35)
                
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
                elif response.status_code == 409:
                    logger.error("Lỗi xung đột Telegram")
                    time.sleep(60)
                else:
                    time.sleep(10)
                
            except Exception as e:
                logger.error(f"Lỗi Telegram listener: {str(e)}")
                time.sleep(10)

    def _handle_telegram_message(self, chat_id, text):
        """XỬ LÝ TIN NHẮN TELEGRAM"""
        user_state = self.user_states.get(chat_id, {})
        current_step = user_state.get('step')
        
        # Xử lý các bước tạo bot
        if current_step == 'waiting_max_coins':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id, create_main_menu(),
                            self.telegram_bot_token, self.telegram_chat_id)
            else:
                try:
                    max_coins = int(text)
                    if max_coins <= 0 or max_coins > 20:
                        send_telegram("⚠️ Số coin phải từ 1 đến 20. Chọn lại:",
                                    chat_id, create_coin_count_keyboard(),
                                    self.telegram_bot_token, self.telegram_chat_id)
                        return

                    user_state['max_coins'] = max_coins
                    user_state['step'] = 'waiting_leverage'
                    
                    send_telegram(
                        f"🔢 Coin tối đa: {max_coins}\n\n"
                        f"Chọn đòn bẩy:",
                        chat_id,
                        create_leverage_keyboard(),
                        self.telegram_bot_token, self.telegram_chat_id
                    )
                except ValueError:
                    send_telegram("⚠️ Vui lòng chọn số hợp lệ:",
                                chat_id, create_coin_count_keyboard(),
                                self.telegram_bot_token, self.telegram_chat_id)

        elif current_step == 'waiting_leverage':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy", chat_id, create_main_menu(),
                            self.telegram_bot_token, self.telegram_chat_id)
            else:
                try:
                    lev_text = text[:-1] if text.endswith('x') else text
                    leverage = int(lev_text)
                    
                    if leverage <= 0 or leverage > 100:
                        send_telegram("⚠️ Đòn bẩy phải từ 1-100. Chọn lại:",
                                    chat_id, create_leverage_keyboard(),
                                    self.telegram_bot_token, self.telegram_chat_id)
                        return

                    user_state['leverage'] = leverage
                    user_state['step'] = 'waiting_percent'
                    
                    send_telegram(
                        f"💰 Đòn bẩy: {leverage}x\n\n"
                        f"Chọn % số dư cho mỗi lệnh:",
                        chat_id,
                        create_percent_keyboard(),
                        self.telegram_bot_token, self.telegram_chat_id
                    )
                except ValueError:
                    send_telegram("⚠️ Vui lòng chọn đòn bẩy hợp lệ:",
                                chat_id, create_leverage_keyboard(),
                                self.telegram_bot_token, self.telegram_chat_id)

        elif current_step == 'waiting_percent':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy", chat_id, create_main_menu(),
                            self.telegram_bot_token, self.telegram_chat_id)
            else:
                try:
                    percent = float(text)
                    if percent <= 0 or percent > 100:
                        send_telegram("⚠️ % số dư phải từ 0.1-100. Chọn lại:",
                                    chat_id, create_percent_keyboard(),
                                    self.telegram_bot_token, self.telegram_chat_id)
                        return

                    user_state['percent'] = percent
                    user_state['step'] = 'waiting_tp'
                    
                    balance = get_balance(self.api_key, self.api_secret)
                    amount_info = f"\n💵 ~{balance * (percent/100):.2f} USDT/lệnh" if balance else ""
                    
                    send_telegram(
                        f"📊 % Số dư: {percent}%{amount_info}\n\n"
                        f"Chọn Take Profit (%):",
                        chat_id,
                        create_tp_keyboard(),
                        self.telegram_bot_token, self.telegram_chat_id
                    )
                except ValueError:
                    send_telegram("⚠️ Vui lòng nhập số hợp lệ:",
                                chat_id, create_percent_keyboard(),
                                self.telegram_bot_token, self.telegram_chat_id)

        elif current_step == 'waiting_tp':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy", chat_id, create_main_menu(),
                            self.telegram_bot_token, self.telegram_chat_id)
            else:
                try:
                    tp = float(text)
                    if tp <= 0:
                        send_telegram("⚠️ Take Profit phải > 0. Chọn lại:",
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
                    send_telegram("⚠️ Vui lòng nhập số hợp lệ:",
                                chat_id, create_tp_keyboard(),
                                self.telegram_bot_token, self.telegram_chat_id)

        elif current_step == 'waiting_sl':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy", chat_id, create_main_menu(),
                            self.telegram_bot_token, self.telegram_chat_id)
            else:
                try:
                    sl = float(text)
                    if sl < 0:
                        send_telegram("⚠️ Stop Loss phải ≥ 0. Chọn lại:",
                                    chat_id, create_sl_keyboard(),
                                    self.telegram_bot_token, self.telegram_chat_id)
                        return

                    # 🎯 TẠO BOT DUY NHẤT
                    leverage = user_state.get('leverage')
                    percent = user_state.get('percent')
                    tp = user_state.get('tp')
                    max_coins = user_state.get('max_coins', 5)
                    
                    success = self.add_bot(leverage, percent, tp, sl, max_coins)
                    
                    if success:
                        success_msg = (
                            f"✅ <b>BOT DUY NHẤT ĐÃ SẴN SÀNG</b>\n\n"
                            f"💰 Đòn bẩy: {leverage}x\n"
                            f"📊 % Số dư: {percent}%\n"
                            f"🎯 TP: {tp}%\n"
                            f"🛡️ SL: {sl}%\n"
                            f"🔢 Coin tối đa: {max_coins}\n\n"
                            f"🔄 <b>Cơ chế hoạt động:</b>\n"
                            f"• Quét TOÀN BỘ Binance\n"
                            f"• Tìm coin tốt nhất với đòn bẩy {leverage}x\n"
                            f"• DỪNG khi đủ {max_coins} coin\n"
                            f"• TỰ ĐỘNG thay thế khi đóng lệnh\n"
                            f"• BỎ QUA coin lỗi, tìm coin khác"
                        )
                        send_telegram(success_msg, chat_id, create_main_menu(),
                                    self.telegram_bot_token, self.telegram_chat_id)
                    else:
                        send_telegram("❌ Lỗi tạo bot", chat_id, create_main_menu(),
                                    self.telegram_bot_token, self.telegram_chat_id)
                    
                    self.user_states[chat_id] = {}
                    
                except ValueError:
                    send_telegram("⚠️ Vui lòng nhập số hợp lệ:",
                                chat_id, create_sl_keyboard(),
                                self.telegram_bot_token, self.telegram_chat_id)

        # Xử lý các lệnh menu chính
        elif text == "➕ Thêm Bot":
            # 🎯 CHỈ CÒN 1 QUY TRÌNH ĐƠN GIẢN
            if self.bots:
                send_telegram("⚠️ Hệ thống đang chạy 1 bot. Hãy dừng bot hiện tại trước khi tạo mới.", 
                            chat_id, create_main_menu(), self.telegram_bot_token, self.telegram_chat_id)
                return
                
            self.user_states[chat_id] = {'step': 'waiting_max_coins'}
            
            balance = get_balance(self.api_key, self.api_secret)
            balance_info = f"\n💰 Số dư hiện có: <b>{balance:.2f} USDT</b>" if balance else ""
            
            send_telegram(
                "🔢 <b>THIẾT LẬP SỐ LƯỢNG COIN TỐI ĐA</b>\n\n"
                f"{balance_info}\n\n"
                "Chọn số lượng coin tối đa bot được phép giao dịch:\n"
                "<i>Bot sẽ tìm cho đến khi đủ số lượng này</i>",
                chat_id,
                create_coin_count_keyboard(),
                self.telegram_bot_token, self.telegram_chat_id
            )
        
        elif text == "📊 Danh sách Bot":
            if not self.bots:
                send_telegram("🤖 Không có bot nào đang chạy", chat_id,
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                message = "🤖 <b>THÔNG TIN BOT DUY NHẤT</b>\n\n"
                
                bot = list(self.bots.values())[0]
                symbol_info = bot.symbol if bot.symbol else "Đang tìm coin..."
                status = "🟢 Đang trade" if bot.position_open else "🔍 Đang tìm"
                
                message += f"🔹 Trạng thái: {status}\n"
                message += f"🔹 Coin hiện tại: {symbol_info}\n"
                message += f"🔹 Đòn bẩy: {bot.lev}x\n"
                message += f"🔹 % Số dư: {bot.percent}%\n"
                message += f"🔹 TP/SL: {bot.tp}%/{bot.sl}%\n"
                message += f"🔹 Coin tối đa: {self.coin_manager.max_coins}\n"
                
                trading_coins = self.coin_manager.get_trading_coins()
                if trading_coins:
                    message += f"\n🔗 <b>Coin đang giao dịch:</b>\n"
                    for coin in trading_coins:
                        message += f"• {coin}\n"
                
                send_telegram(message, chat_id,
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif text == "📊 Thống kê":
            summary = self.get_status_summary()
            send_telegram(summary, chat_id,
                         bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif text == "⛔ Dừng Bot":
            if not self.bots:
                send_telegram("🤖 Không có bot nào đang chạy", chat_id,
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                self.stop_all()
                send_telegram("⛔ Đã dừng bot và hệ thống", chat_id, create_main_menu(),
                            self.telegram_bot_token, self.telegram_chat_id)
        
        elif text == "💰 Số dư":
            try:
                balance = get_balance(self.api_key, self.api_secret)
                if balance is None:
                    send_telegram("❌ Lỗi kết nối Binance", chat_id,
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                else:
                    send_telegram(f"💰 <b>SỐ DƯ KHẢ DỤNG</b>: {balance:.2f} USDT", chat_id,
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            except Exception as e:
                send_telegram(f"⚠️ Lỗi lấy số dư: {str(e)}", chat_id,
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif text == "📈 Vị thế":
            try:
                positions = get_positions(api_key=self.api_key, api_secret=self.api_secret)
                open_positions = []
                
                for pos in positions:
                    position_amt = float(pos.get('positionAmt', 0))
                    if position_amt != 0:
                        open_positions.append(pos)
                
                if not open_positions:
                    send_telegram("📭 Không có vị thế nào đang mở", chat_id,
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                    return
                
                message = "📈 <b>VỊ THẾ ĐANG MỞ TRÊN BINANCE</b>\n\n"
                for pos in open_positions:
                    symbol = pos.get('symbol', 'UNKNOWN')
                    entry = float(pos.get('entryPrice', 0))
                    side = "LONG" if float(pos.get('positionAmt', 0)) > 0 else "SHORT"
                    pnl = float(pos.get('unRealizedProfit', 0))
                    leverage = float(pos.get('leverage', 1))
                    amount = abs(float(pos.get('positionAmt', 0)))
                    
                    message += (
                        f"🔹 {symbol} | {side}\n"
                        f"   📊 Đòn bẩy: {leverage}x\n"
                        f"   🏷️ Giá vào: {entry:.4f}\n"
                        f"   ⚖️ Khối lượng: {amount:.4f}\n"
                        f"   💰 PnL: {pnl:.2f} USDT\n\n"
                    )
                
                send_telegram(message, chat_id,
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            except Exception as e:
                send_telegram(f"⚠️ Lỗi lấy vị thế: {str(e)}", chat_id,
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif text == "🎯 Chiến lược":
            strategy_info = (
                "🎯 <b>HỆ THỐNG BOT ĐƠN DUY NHẤT</b>\n\n"
                
                "🤖 <b>1 BOT - NHIỀU COIN</b>\n"
                "• Chỉ 1 bot duy nhất quét toàn bộ Binance\n"
                "• Tự động tìm coin tốt nhất với đòn bẩy phù hợp\n"
                "• Giới hạn số lượng coin tối đa\n\n"
                
                "🔢 <b>CƠ CHẾ COIN GIỚI HẠN</b>\n"
                "• Danh sách coin ban đầu: RỖNG\n"
                "• Mở vị thế: THÊM 1 coin vào danh sách\n"  
                "• Đóng vị thế: XÓA 1 coin khỏi danh sách\n"
                "• Dừng tìm khi đủ số lượng coin\n"
                "• Tiếp tục tìm khi có slot trống\n\n"
                
                "⚖️ <b>CÂN BẰNG VỊ THẾ</b>\n"
                "• Tự động phân tích tỷ lệ LONG/SHORT\n"
                "• Ưu tiên hướng ngược lại để cân bằng\n"
                "• Tránh tập trung quá nhiều 1 hướng"
            )
            send_telegram(strategy_info, chat_id,
                        bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif text == "⚙️ Cấu hình":
            balance = get_balance(self.api_key, self.api_secret)
            api_status = "✅ Đã kết nối" if balance is not None else "❌ Lỗi kết nối"
            
            has_bot = "✅ Đang chạy" if self.bots else "❌ Chưa có"
            trading_coins = self.coin_manager.get_trading_coins()
            available_slots = self.coin_manager.get_available_slots()
            
            config_info = (
                "⚙️ <b>CẤU HÌNH HỆ THỐNG BOT ĐƠN</b>\n\n"
                f"🔑 Binance API: {api_status}\n"
                f"🤖 Bot: {has_bot}\n"
                f"🔢 Coin tối đa: {self.coin_manager.max_coins}\n"
                f"📈 Đang giao dịch: {len(trading_coins)} coin\n"
                f"🔓 Còn trống: {available_slots} slot\n"
                f"🌐 WebSocket: {len(self.ws_manager.connections)} kết nối\n"
                f"⚖️ Position Balancer: Đã sẵn sàng\n\n"
                f"🔄 <b>Hệ thống 1 bot đang hoạt động</b>\n"
                f"🎯 <b>Tự động thêm/xóa coin khi mở/đóng lệnh</b>"
            )
            send_telegram(config_info, chat_id,
                        bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif text:
            self.send_main_menu(chat_id)

def create_coin_count_keyboard():
    return {
        "keyboard": [
            [{"text": "3"}, {"text": "5"}, {"text": "8"}],
            [{"text": "10"}, {"text": "15"}],
            [{"text": "❌ Hủy bỏ"}]
        ],
        "resize_keyboard": True,
        "one_time_keyboard": True
    }
# ========== KHỞI TẠO GLOBAL INSTANCES ==========
coin_manager = CoinManager()
    
    
    def _format_analysis_info(self, timeframe_data):
        """Định dạng thông tin phân tích cho log"""
        info = ""
        for tf, data in timeframe_data.items():
            stats = data.get('stats', {})
            if stats:
                info += (f"  {tf}: {data['signal']} | "
                        f"Tăng: {stats['bullish_count']}/{stats['total_candles']} "
                        f"({stats['bullish_ratio']:.1%}) | "
                        f"TB: {stats['avg_change']:.2f}%\n")
        return info

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
        
        # === THÊM MỚI: HỆ THỐNG DUY TRÌ SỐ LƯỢNG BOT ===
        self.target_bot_count = 0
        self.bot_configs = {}  # Lưu cấu hình để tạo lại bot
        self.maintenance_thread = threading.Thread(target=self._bot_maintenance_loop, daemon=True)
        self.maintenance_thread.start()
        # === KẾT THÚC PHẦN THÊM MỚI ===
        
        # KHỞI TẠO HỆ THỐNG MỚI
        self.position_balancer = PositionBalancer(self)
        
        if api_key and api_secret:
            self._verify_api_connection()
            self.log("🟢 HỆ THỐNG BOT ĐA LUỒNG ĐÃ KHỞI ĐỘNG")
            self.log("⚖️ Mỗi bot là 1 vòng lặp độc lập - Tự tìm coin & trade")
            
            self.telegram_thread = threading.Thread(target=self._telegram_listener, daemon=True)
            self.telegram_thread.start()
            
            if self.telegram_chat_id:
                self.send_main_menu(self.telegram_chat_id)
        else:
            self.log("⚡ BotManager khởi động ở chế độ không config")

    # === THÊM MỚI: PHƯƠNG THỨC DUY TRÌ BOT ===
    def _bot_maintenance_loop(self):
        """VÒNG LẶP DUY TRÌ SỐ LƯỢNG BOT - TỰ ĐỘNG TẠO BOT MỚI KHI THIẾU"""
        while self.running:
            try:
                if self.target_bot_count > 0 and len(self.bots) < self.target_bot_count:
                    missing_count = self.target_bot_count - len(self.bots)
                    self.log(f"🔄 Đang duy trì số lượng bot: thiếu {missing_count} bot")
                    
                    # Tạo bot mới với cấu hình đã lưu
                    for config_key, config in self.bot_configs.items():
                        if missing_count <= 0:
                            break
                            
                        # Tạo tối đa missing_count bot cho mỗi cấu hình
                        bots_to_create = min(missing_count, 3)  # Tạo từng ít một
                        
                        success = self.add_bot(
                            symbol=config.get('symbol'),
                            lev=config['leverage'],
                            percent=config['percent'],
                            tp=config['tp'],
                            sl=config['sl'],
                            strategy_type=config['strategy_type'],
                            bot_mode=config.get('bot_mode', 'dynamic'),
                            bot_count=bots_to_create,
                            is_maintenance=True  # Đánh dấu là bot được tạo tự động
                        )
                        
                        if success:
                            missing_count -= bots_to_create
                            self.log(f"✅ Đã tạo {bots_to_create} bot mới để duy trì số lượng")
                
                time.sleep(10)  # Kiểm tra mỗi 10 giây
                
            except Exception as e:
                self.log(f"❌ Lỗi vòng lặp duy trì bot: {str(e)}")
                time.sleep(30)
    # === KẾT THÚC PHẦN THÊM MỚI ===

    def _verify_api_connection(self):
        balance = get_balance(self.api_key, self.api_secret)
        if balance is None:
            self.log("❌ LỖI: Không thể kết nối Binance API.")
        else:
            self.log(f"✅ Kết nối Binance thành công! Số dư: {balance:.2f} USDT")

    def get_position_summary(self):
        """Lấy thống kê tổng quan - BAO GỒM TẤT CẢ VỊ THẾ BINANCE"""
        try:
            # 🎯 Lấy tất cả vị thế từ Binance
            all_positions = get_positions(api_key=self.api_key, api_secret=self.api_secret)
            
            binance_buy_count = 0
            binance_sell_count = 0
            binance_positions = []
            
            for pos in all_positions:
                position_amt = float(pos.get('positionAmt', 0))
                if position_amt != 0:
                    symbol = pos.get('symbol', 'UNKNOWN')
                    if position_amt > 0:
                        binance_buy_count += 1
                        binance_positions.append(f"{symbol}(LONG)")
                    else:
                        binance_sell_count += 1
                        binance_positions.append(f"{symbol}(SHORT)")
            
            # Thống kê bot
            bot_buy_count = 0
            bot_sell_count = 0
            searching_bots = 0
            open_bots = 0
            bot_positions = []
            
            for bot_id, bot in self.bots.items():
                if bot.position_open:
                    if bot.side == "BUY":
                        bot_buy_count += 1
                    elif bot.side == "SELL":
                        bot_sell_count += 1
                    bot_positions.append(f"{bot.symbol}({bot.side})")
                    open_bots += 1
                else:
                    if bot.status == "searching":
                        searching_bots += 1
            
            total_binance = binance_buy_count + binance_sell_count
            total_bots = len(self.bots)
            total_bot_open = bot_buy_count + bot_sell_count
            
            summary = (
                f"📊 **THỐNG KÊ TOÀN HỆ THỐNG**\n\n"
                f"🤖 **BOT**: {total_bots} bots\n"
                f"   🔍 Đang tìm coin: {searching_bots}\n"
                f"   📈 Đang mở: {open_bots} vị thế\n"
                f"   🎯 Mục tiêu: {self.target_bot_count} bot\n\n"
            )
            
            if total_binance > 0:
                binance_buy_ratio = binance_buy_count / total_binance
                binance_sell_ratio = binance_sell_count / total_binance
                
                summary += (
                    f"💰 **TẤT CẢ VỊ THẾ BINANCE**: {total_binance}\n"
                    f"   🟢 LONG: {binance_buy_count} ({binance_buy_ratio:.1%})\n"
                    f"   🔴 SHORT: {binance_sell_count} ({binance_sell_ratio:.1%})\n"
                )
                
                if binance_positions:
                    if len(binance_positions) > 6:
                        summary += f"   🔗 {', '.join(binance_positions[:6])} + {len(binance_positions) - 6} more...\n"
                    else:
                        summary += f"   🔗 {', '.join(binance_positions)}\n"
                
                # Đề xuất cân bằng
                if binance_buy_ratio > 0.6:
                    summary += f"\n⚖️ **ĐỀ XUẤT**: Nhiều LONG → ƯU TIÊN TÌM SHORT"
                elif binance_sell_ratio > 0.6:
                    summary += f"\n⚖️ **ĐỀ XUẤT**: Nhiều SHORT → ƯU TIÊN TÌM LONG"
                else:
                    summary += f"\n⚖️ **TRẠNG THÁI**: Cân bằng tốt"
                    
            else:
                summary += f"💰 **TẤT CẢ VỊ THẾ BINANCE**: Không có vị thế nào\n"
                
            return summary
                    
        except Exception as e:
            return f"❌ Lỗi thống kê: {str(e)}"

    def log(self, message):
        logger.info(f"[SYSTEM] {message}")
        if self.telegram_bot_token and self.telegram_chat_id:
            send_telegram(f"<b>SYSTEM</b>: {message}", 
                         bot_token=self.telegram_bot_token, 
                         default_chat_id=self.telegram_chat_id)

    def send_main_menu(self, chat_id):
        welcome = "🤖 <b>BOT GIAO DỊCH FUTURES ĐA LUỒNG</b>\n\n🎯 <b>MỖI BOT LÀ 1 VÒNG LẶP ĐỘC LẬP</b>"
        send_telegram(welcome, chat_id, create_main_menu(),
                     bot_token=self.telegram_bot_token, 
                     default_chat_id=self.telegram_chat_id)

    def add_bot(self, symbol, lev, percent, tp, sl, strategy_type, bot_count=1, **kwargs):
        if sl == 0:
            sl = None
            
        if not self.api_key or not self.api_secret:
            self.log("❌ Chưa thiết lập API Key trong BotManager")
            return False
        
        test_balance = get_balance(self.api_key, self.api_secret)
        if test_balance is None:
            self.log("❌ LỖI: Không thể kết nối Binance")
            return False
        
        bot_mode = kwargs.get('bot_mode', 'static')
        created_count = 0
        
        # === THÊM MỚI: LƯU CẤU HÌNH ĐỂ DUY TRÌ SỐ LƯỢNG ===
        is_maintenance = kwargs.get('is_maintenance', False)
        if not is_maintenance:  # Chỉ lưu cấu hình khi không phải bot duy trì
            config_key = f"{strategy_type}_{bot_mode}_{lev}x_{percent}%_{int(time.time())}"
            self.bot_configs[config_key] = {
                'symbol': symbol,
                'leverage': lev,
                'percent': percent,
                'tp': tp,
                'sl': sl,
                'strategy_type': strategy_type,
                'bot_mode': bot_mode
            }
            
            # CẬP NHẬT SỐ LƯỢNG BOT MỤC TIÊU
            self.target_bot_count += bot_count
        # === KẾT THÚC PHẦN THÊM MỚI ===
        
        # TẠO NHIỀU BOT THEO SỐ LƯỢNG NGƯỜI DÙNG CHỌN
        for i in range(bot_count):
            try:
                if bot_mode == 'static' and symbol:
                    # Bot tĩnh - coin cố định
                    bot_id = f"{symbol}_{strategy_type}_{i}_{int(time.time())}"
                    
                    if bot_id in self.bots:
                        continue
                    
                    bot_class = {
                        "Multi-Timeframe": DynamicMultiTimeframeBot
                    }.get(strategy_type)
                    
                    if not bot_class:
                        continue
                    
                    bot = bot_class(symbol, lev, percent, tp, sl, self.ws_manager,
                                  self.api_key, self.api_secret, self.telegram_bot_token, 
                                  self.telegram_chat_id, bot_id=bot_id)
                    
                else:
                    # Bot động - tự tìm coin
                    bot_id = f"DYNAMIC_{strategy_type}_{i}_{int(time.time())}"
                    
                    if bot_id in self.bots:
                        continue
                    
                    bot_class = {
                        "Multi-Timeframe": DynamicMultiTimeframeBot
                    }.get(strategy_type)
                    
                    if not bot_class:
                        continue
                    
                    # Bot động bắt đầu không có symbol
                    bot = bot_class(None, lev, percent, tp, sl, self.ws_manager,
                                  self.api_key, self.api_secret, self.telegram_bot_token,
                                  self.telegram_chat_id, bot_id=bot_id)
                
                bot._bot_manager = self
                self.bots[bot_id] = bot
                created_count += 1
                
            except Exception as e:
                self.log(f"❌ Lỗi tạo bot {i}: {str(e)}")
                continue
        
        if created_count > 0:
            success_msg = (
                f"✅ <b>ĐÃ TẠO {created_count}/{bot_count} BOT {strategy_type}</b>\n\n"
                f"🎯 Chiến lược: {strategy_type}\n"
                f"💰 Đòn bẩy: {lev}x\n"
                f"📊 % Số dư: {percent}%\n"
                f"🎯 TP: {tp}%\n"
                f"🛡️ SL: {sl}%\n"
                f"🔧 Chế độ: {bot_mode}\n"
            )
            
            # === THÊM MỚI: THÔNG BÁO DUY TRÌ SỐ LƯỢNG ===
            if not is_maintenance:
                success_msg += f"🎯 <b>Hệ thống sẽ duy trì {self.target_bot_count} bot</b>\n"
                success_msg += f"🔄 <b>Tự động tạo bot mới khi thiếu</b>\n"
            # === KẾT THÚC PHẦN THÊM MỚI ===
            
            if bot_mode == 'static' and symbol:
                success_msg += f"🔗 Coin: {symbol}\n"
            else:
                success_msg += f"🔗 Coin: Tự động tìm kiếm\n"
            
            success_msg += f"\n🎯 <b>Mỗi bot là 1 vòng lặp độc lập</b>\n"
            success_msg += f"🔄 <b>Tự reset hoàn toàn sau mỗi lệnh</b>"
            
            self.log(success_msg)
            return True
        else:
            self.log("❌ Không thể tạo bot nào")
            return False

    def stop_bot(self, bot_id):
        bot = self.bots.get(bot_id)
        if bot:
            bot.stop()
            del self.bots[bot_id]
            self.log(f"⛔ Đã dừng bot {bot_id}")
            
            # === THÊM MỚI: KHÔNG GIẢM target_bot_count để hệ thống tự tạo bot mới ===
            # Hệ thống sẽ tự động phát hiện số bot thiếu và tạo mới trong maintenance loop
            # === KẾT THÚC PHẦN THÊM MỚI ===
            return True
        return False

    def stop_all(self):
        self.log("⛔ Đang dừng tất cả bot...")
        for bot_id in list(self.bots.keys()):
            self.stop_bot(bot_id)
        self.ws_manager.stop()
        self.running = False
        # === THÊM MỚI: RESET SỐ LƯỢNG MỤC TIÊU ===
        self.target_bot_count = 0
        self.bot_configs = {}
        # === KẾT THÚC PHẦN THÊM MỚI ===
        self.log("🔴 Hệ thống đã dừng")

    def check_binance_positions(self):
        """Kiểm tra nhanh vị thế Binance"""
        try:
            positions = get_positions(api_key=self.api_key, api_secret=self.api_secret)
            buy_count = 0
            sell_count = 0
            
            for pos in positions:
                position_amt = float(pos.get('positionAmt', 0))
                if position_amt != 0:
                    if position_amt > 0:
                        buy_count += 1
                    else:
                        sell_count += 1
            
            return buy_count, sell_count
        except Exception as e:
            logger.error(f"❌ Lỗi kiểm tra vị thế Binance: {e}")
            return 0, 0

    def _telegram_listener(self):
        last_update_id = 0
        
        while self.running and self.telegram_bot_token:
            try:
                url = f"https://api.telegram.org/bot{self.telegram_bot_token}/getUpdates?offset={last_update_id+1}&timeout=30"
                response = requests.get(url, timeout=35)
                
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
                elif response.status_code == 409:
                    logger.error("Lỗi xung đột Telegram")
                    time.sleep(60)
                else:
                    time.sleep(10)
                
            except Exception as e:
                logger.error(f"Lỗi Telegram listener: {str(e)}")
                time.sleep(10)

    def _handle_telegram_message(self, chat_id, text):
        user_state = self.user_states.get(chat_id, {})
        current_step = user_state.get('step')
        
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
                    user_state['step'] = 'waiting_bot_mode'
                    
                    send_telegram(
                        f"🤖 Số lượng bot: {bot_count}\n\n"
                        f"Chọn chế độ bot:",
                        chat_id,
                        create_bot_mode_keyboard(),
                        self.telegram_bot_token, self.telegram_chat_id
                    )
                except ValueError:
                    send_telegram("⚠️ Vui lòng nhập số hợp lệ cho số lượng bot:",
                                chat_id, create_bot_count_keyboard(),
                                self.telegram_bot_token, self.telegram_chat_id)

        elif current_step == 'waiting_bot_mode':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id, create_main_menu(),
                            self.telegram_bot_token, self.telegram_chat_id)
            elif text in ["🤖 Bot Tĩnh - Coin cụ thể", "🔄 Bot Động - Tự tìm coin"]:
                if text == "🤖 Bot Tĩnh - Coin cụ thể":
                    user_state['bot_mode'] = 'static'
                    user_state['step'] = 'waiting_strategy'
                    send_telegram(
                        "🎯 <b>ĐÃ CHỌN: BOT TĨNH</b>\n\n"
                        "🤖 Bot sẽ giao dịch coin CỐ ĐỊNH\n"
                        "📊 Bạn cần chọn coin cụ thể\n\n"
                        "Chọn chiến lược:",
                        chat_id,
                        create_strategy_keyboard(),
                        self.telegram_bot_token, self.telegram_chat_id
                    )
                else:
                    user_state['bot_mode'] = 'dynamic'
                    user_state['step'] = 'waiting_strategy'
                    send_telegram(
                        "🎯 <b>ĐÃ CHỌN: BOT ĐỘNG</b>\n\n"
                        f"🤖 Hệ thống sẽ tạo <b>{user_state.get('bot_count', 1)} bot độc lập</b>\n"
                        f"🔄 Mỗi bot tự tìm coin & trade độc lập\n"
                        f"🎯 Tự reset hoàn toàn sau mỗi lệnh\n"
                        f"📊 Mỗi bot là 1 vòng lặp hoàn chỉnh\n\n"
                        "Chọn chiến lược:",
                        chat_id,
                        create_strategy_keyboard(),
                        self.telegram_bot_token, self.telegram_chat_id
                    )

        elif current_step == 'waiting_strategy':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id, create_main_menu(),
                            self.telegram_bot_token, self.telegram_chat_id)
            elif text in ["⏰ Multi-Timeframe"]:
                
                strategy_map = {
                    "⏰ Multi-Timeframe": "Multi-Timeframe"
                }
                
                strategy = strategy_map[text]
                user_state['strategy'] = strategy
                user_state['step'] = 'waiting_exit_strategy'
                
                strategy_descriptions = {
                    "Multi-Timeframe": "Mỗi bot tự tìm coin & phân tích đa khung thời gian"
                }
                
                description = strategy_descriptions.get(strategy, "")
                bot_count = user_state.get('bot_count', 1)
                
                send_telegram(
                    f"🎯 <b>ĐÃ CHỌN: {strategy}</b>\n"
                    f"🤖 Số lượng: {bot_count} bot độc lập\n\n"
                    f"{description}\n\n"
                    f"Chọn chiến lược thoát lệnh:",
                    chat_id,
                    create_exit_strategy_keyboard(),
                    self.telegram_bot_token, self.telegram_chat_id
                )

        elif current_step == 'waiting_exit_strategy':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id, create_main_menu(),
                            self.telegram_bot_token, self.telegram_chat_id)
            elif text == "🎯 Chỉ TP/SL cố định":
                user_state['exit_strategy'] = 'traditional'
                self._continue_bot_creation(chat_id, user_state)

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
                    
                    strategy = user_state.get('strategy')
                    bot_mode = user_state.get('bot_mode', 'static')
                    leverage = user_state.get('leverage')
                    percent = user_state.get('percent')
                    tp = user_state.get('tp')
                    sl = user_state.get('sl')
                    symbol = user_state.get('symbol')
                    bot_count = user_state.get('bot_count', 1)
                    
                    success = self.add_bot(
                        symbol=symbol,
                        lev=leverage,
                        percent=percent,
                        tp=tp,
                        sl=sl,
                        strategy_type=strategy,
                        bot_mode=bot_mode,
                        bot_count=bot_count
                    )
                    
                    if success:
                        success_msg = (
                            f"✅ <b>ĐÃ TẠO {bot_count} BOT THÀNH CÔNG</b>\n\n"
                            f"🤖 Chiến lược: {strategy}\n"
                            f"🔧 Chế độ: {bot_mode}\n"
                            f"🔢 Số lượng: {bot_count} bot độc lập\n"
                            f"💰 Đòn bẩy: {leverage}x\n"
                            f"📊 % Số dư: {percent}%\n"
                            f"🎯 TP: {tp}%\n"
                            f"🛡️ SL: {sl}%"
                        )
                        if bot_mode == 'static' and symbol:
                            success_msg += f"\n🔗 Coin: {symbol}"
                        
                        success_msg += f"\n\n🎯 <b>Hệ thống sẽ duy trì {self.target_bot_count} bot</b>\n"
                        success_msg += f"🔄 <b>Tự động tạo bot mới khi thiếu</b>\n"
                        success_msg += f"📊 <b>Tự tìm coin & trade độc lập</b>"
                        
                        send_telegram(success_msg, chat_id, create_main_menu(),
                                    self.telegram_bot_token, self.telegram_chat_id)
                    else:
                        send_telegram("❌ Có lỗi khi tạo bot. Vui lòng thử lại.",
                                    chat_id, create_main_menu(),
                                    self.telegram_bot_token, self.telegram_chat_id)
                    
                    self.user_states[chat_id] = {}
                    
                except ValueError:
                    send_telegram("⚠️ Vui lòng nhập số hợp lệ cho Stop Loss:",
                                chat_id, create_sl_keyboard(),
                                self.telegram_bot_token, self.telegram_chat_id)

        elif text == "➕ Thêm Bot":
            self.user_states[chat_id] = {'step': 'waiting_bot_count'}
            balance = get_balance(self.api_key, self.api_secret)
            if balance is None:
                send_telegram("❌ <b>LỖI KẾT NỐI BINANCE</b>\nVui lòng kiểm tra API Key!", chat_id,
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                return
            
            send_telegram(
                f"🎯 <b>CHỌN SỐ LƯỢNG BOT ĐỘC LẬP</b>\n\n"
                f"💰 Số dư hiện có: <b>{balance:.2f} USDT</b>\n\n"
                f"Chọn số lượng bot độc lập bạn muốn tạo:\n"
                f"<i>Hệ thống sẽ duy trì số lượng bot này tự động</i>",
                chat_id,
                create_bot_count_keyboard(),
                self.telegram_bot_token, self.telegram_chat_id
            )
        
        elif text == "📊 Danh sách Bot":
            if not self.bots:
                send_telegram("🤖 Không có bot nào đang chạy", chat_id,
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                message = "🤖 <b>DANH SÁCH BOT ĐỘC LẬP ĐANG CHẠY</b>\n\n"
                
                searching_bots = 0
                open_bots = 0
                
                for bot_id, bot in self.bots.items():
                    if bot.status == "searching":
                        status = "🔍 Đang tìm coin"
                        searching_bots += 1
                    elif bot.status == "open":
                        status = "🟢 Đang trade"
                        open_bots += 1
                    else:
                        status = "⚪ Unknown"
                    
                    symbol_info = bot.symbol if bot.symbol else "Đang tìm..."
                    message += f"🔹 {bot_id}\n"
                    message += f"   📊 {symbol_info} | {status}\n"
                    message += f"   💰 ĐB: {bot.lev}x | Vốn: {bot.percent}%\n\n"
                
                message += f"📈 Tổng số: {len(self.bots)} bot\n"
                message += f"🔍 Đang tìm coin: {searching_bots} bot\n"
                message += f"📊 Đang trade: {open_bots} bot\n"
                message += f"🎯 Mục tiêu: {self.target_bot_count} bot"
                
                send_telegram(message, chat_id,
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif text == "📊 Thống kê":
            summary = self.get_position_summary()
            send_telegram(summary, chat_id,
                         bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif text == "⛔ Dừng Bot":
            if not self.bots:
                send_telegram("🤖 Không có bot nào đang chạy", chat_id,
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                message = "⛔ <b>CHỌN BOT ĐỂ DỪNG</b>\n\n"
                keyboard = []
                row = []
                
                for i, bot_id in enumerate(self.bots.keys()):
                    bot = self.bots[bot_id]
                    symbol_info = bot.symbol if bot.symbol else "No Coin"
                    message += f"🔹 {bot_id} - {symbol_info}\n"
                    row.append({"text": f"⛔ {bot_id}"})
                    if len(row) == 1 or i == len(self.bots) - 1:
                        keyboard.append(row)
                        row = []
                
                keyboard.append([{"text": "⛔ DỪNG TẤT CẢ"}])
                keyboard.append([{"text": "❌ Hủy bỏ"}])
                
                send_telegram(
                    message, 
                    chat_id, 
                    {"keyboard": keyboard, "resize_keyboard": True, "one_time_keyboard": True},
                    self.telegram_bot_token, self.telegram_chat_id
                )
        
        elif text.startswith("⛔ "):
            bot_id = text.replace("⛔ ", "").strip()
            if bot_id == "DỪNG TẤT CẢ":
                self.stop_all()
                send_telegram("⛔ Đã dừng tất cả bot", chat_id, create_main_menu(),
                            self.telegram_bot_token, self.telegram_chat_id)
            elif self.stop_bot(bot_id):
                send_telegram(f"⛔ Đã dừng bot {bot_id}", chat_id, create_main_menu(),
                            self.telegram_bot_token, self.telegram_chat_id)
            else:
                send_telegram(f"⚠️ Không tìm thấy bot {bot_id}", chat_id, create_main_menu(),
                            self.telegram_bot_token, self.telegram_chat_id)
        
        elif text == "💰 Số dư":
            try:
                balance = get_balance(self.api_key, self.api_secret)
                if balance is None:
                    send_telegram("❌ <b>LỖI KẾT NỐI BINANCE</b>\nVui lòng kiểm tra API Key!", chat_id,
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                else:
                    send_telegram(f"💰 <b>SỐ DƯ KHẢ DỤNG</b>: {balance:.2f} USDT", chat_id,
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            except Exception as e:
                send_telegram(f"⚠️ Lỗi lấy số dư: {str(e)}", chat_id,
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif text == "📈 Vị thế":
            try:
                positions = get_positions(api_key=self.api_key, api_secret=self.api_secret)
                if not positions:
                    send_telegram("📭 Không có vị thế nào đang mở", chat_id,
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
                
                send_telegram(message, chat_id,
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            except Exception as e:
                send_telegram(f"⚠️ Lỗi lấy vị thế: {str(e)}", chat_id,
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif text == "🎯 Chiến lược":
            strategy_info = (
                "🎯 <b>HỆ THỐNG BOT ĐA LUỒNG ĐỘC LẬP</b>\n\n"
                
                "🤖 <b>Mỗi Bot là 1 Vòng Lặp Hoàn Chỉnh</b>\n"
                "• 🔄 Tự tìm coin từ toàn bộ Binance\n"
                "• 📊 Phân tích đa khung thời gian\n"
                "• 🎯 Tự quyết định hướng giao dịch\n"
                "• ⚖️ Tự cân bằng với bot khác\n"
                "• 🔄 Reset hoàn toàn sau mỗi lệnh\n\n"
                
                "⏰ <b>Multi-Timeframe Strategy</b>\n"
                "• 📊 Phân tích 4 khung: 1m, 5m, 15m, 30m\n"
                "• 🎯 Tín hiệu xác nhận khi đa số đồng thuận\n"
                "• 📈 Thống kê 200 nến gần nhất\n\n"
                
                "🔄 <b>Quy Trình Tự Động Hoàn Toàn</b>\n"
                "1. 🔍 Tìm coin có tín hiệu tốt\n"
                "2. 📊 Phân tích đa khung thời gian\n"
                "3. 🎯 Mở lệnh khi có tín hiệu\n"
                "4. 💰 Theo dõi TP/SL\n"
                "5. 🔄 Reset & tìm coin mới"
            )
            send_telegram(strategy_info, chat_id,
                        bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif text == "⚙️ Cấu hình":
            balance = get_balance(self.api_key, self.api_secret)
            api_status = "✅ Đã kết nối" if balance is not None else "❌ Lỗi kết nối"
            
            searching_bots = sum(1 for bot in self.bots.values() if bot.status == "searching")
            open_bots = sum(1 for bot in self.bots.values() if bot.status == "open")
            
            config_info = (
                "⚙️ <b>CẤU HÌNH HỆ THỐNG ĐA LUỒNG</b>\n\n"
                f"🔑 Binance API: {api_status}\n"
                f"🤖 Tổng số bot: {len(self.bots)}\n"
                f"🔍 Đang tìm coin: {searching_bots} bot\n"
                f"📊 Đang trade: {open_bots} bot\n"
                f"🎯 Mục tiêu: {self.target_bot_count} bot\n"
                f"🌐 WebSocket: {len(self.ws_manager.connections)} kết nối\n"
                f"⚖️ Position Balancer: Đã sẵn sàng\n\n"
                f"🎯 <b>Hệ thống tự động duy trì số lượng bot</b>\n"
                f"🔄 <b>Mỗi bot độc lập - Tự reset hoàn toàn</b>"
            )
            send_telegram(config_info, chat_id,
                        bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif text:
            self.send_main_menu(chat_id)

    def _continue_bot_creation(self, chat_id, user_state):
        strategy = user_state.get('strategy')
        bot_mode = user_state.get('bot_mode', 'static')
        bot_count = user_state.get('bot_count', 1)
        
        if bot_mode == 'static':
            user_state['step'] = 'waiting_symbol'
            send_telegram(
                f"🎯 <b>BOT TĨNH: {strategy}</b>\n"
                f"🤖 Số lượng: {bot_count} bot độc lập\n\n"
                f"🤖 Mỗi bot sẽ trade coin CỐ ĐỊNH\n\n"
                f"Chọn cặp coin:",
                chat_id,
                create_symbols_keyboard(strategy),
                self.telegram_bot_token, self.telegram_chat_id
            )
        else:
            user_state['step'] = 'waiting_leverage'
            send_telegram(
                f"🎯 <b>BOT ĐỘNG ĐA LUỒNG</b>\n"
                f"🤖 Số lượng: {bot_count} bot độc lập\n\n"
                f"🤖 Mỗi bot sẽ tự tìm coin & trade độc lập\n"
                f"🔄 Tự reset hoàn toàn sau mỗi lệnh\n"
                f"📊 Mỗi bot là 1 vòng lặp hoàn chỉnh\n"
                f"⚖️ Tự cân bằng với các bot khác\n\n"
                f"Chọn đòn bẩy:",
                chat_id,
                create_leverage_keyboard(strategy),
                self.telegram_bot_token, self.telegram_chat_id
            )

# ========== KHỞI TẠO GLOBAL INSTANCES ==========
coin_manager = CoinManager()
