import pandas as pd
import pandas_ta as ta
import ccxt.async_support as ccxt
import asyncio
import logging
from config import config


class AdvancedSignalGenerator:
    def __init__(self):
        self.exchange = getattr(ccxt, 'bybit')({
            'enableRateLimit': True,
            'apiKey': config.BYBIT_API_KEY,
            'secret': config.BYBIT_API_SECRET,
        })
        self.exchange.set_sandbox_mode(False)  # Реальные котировки для точности
        self.symbols = []


    def update_symbols(self, new_symbols: list):
        if not new_symbols:
            return

        # Умная очистка: убираем пробелы, делаем капс, проверяем что это текст
        cleaned_symbols = list(set([
            str(s).strip().upper()
            for s in new_symbols
            if s and isinstance(s, (str, bytes))
        ]))

        # Обновляем ТОЛЬКО если есть реальные изменения
        if cleaned_symbols != self.symbols:
            self.symbols = cleaned_symbols
            logging.info(f"📋 Список пар синхронизирован: {self.symbols}")

    async def get_data_and_analyze(self, symbol: str):
        try:
            # 1. Сначала проверяем тикер на объем (чтобы не тянуть тяжелые свечи зря)
            ticker = await self.exchange.fetch_ticker(symbol)
            daily_volume = float(ticker.get('quoteVolume', 0))  # Объем в USDT (или базовой валюте)

            # Порог ликвидности: например, минимум 5,000,000 USDT объема за 24ч
            MIN_VOLUME = 5_000_000
            if daily_volume < MIN_VOLUME:
                logging.debug(f"⏭ {symbol} пропущен: низкий объем ({daily_volume:,.0f} USDT)")
                return None

            # 2. Если ликвидность есть, тянем свечи
            ohlcv = await self.exchange.fetch_ohlcv(symbol, timeframe='1h', limit=250)
            if not ohlcv or len(ohlcv) < 200:
                return None

            df = pd.DataFrame(ohlcv, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])

            # Индикаторы (ATR нужен для динамических целей)
            df['rsi'] = ta.rsi(df['close'], length=14)
            df['ema_20'] = ta.ema(df['close'], length=20)
            df['ema_50'] = ta.ema(df['close'], length=50)
            df['ema_200'] = ta.ema(df['close'], length=200)
            df['atr'] = ta.atr(df['high'], df['low'], df['close'], length=14)

            last = df.iloc[-1]
            prev = df.iloc[-2]

            # Логика тренда и пересечений (без изменений)
            is_uptrend = last['close'] > last['ema_200']
            is_downtrend = last['close'] < last['ema_200']
            local_bullish = last['ema_20'] > last['ema_50']
            local_bearish = last['ema_20'] < last['ema_50']

            rsi_ok_buy = 45 < last['rsi'] < 65
            rsi_ok_sell = 35 < last['rsi'] < 55
            cross_up = prev['close'] <= prev['ema_20'] and last['close'] > last['ema_20']
            cross_down = prev['close'] >= prev['ema_20'] and last['close'] < last['ema_20']

            direction = None
            if is_uptrend and local_bullish and rsi_ok_buy and cross_up:
                direction = "buy"
                reason = "Trend Confluence: Тренд + Импульс + Пробой"
            elif is_downtrend and local_bearish and rsi_ok_sell and cross_down:
                direction = "sell"
                reason = "Trend Confluence: Даунтренд + Импульс + Пробой"

            if direction:
                entry = float(last['close'])
                atr_val = float(last['atr'])

                # ДИНАМИЧЕСКИЙ РАСЧЕТ ЦЕЛЕЙ (уровни вместо "116$")
                if direction == "buy":
                    local_low = float(df['low'].tail(5).min())
                    # Стоп за локальный минимум, но не ближе чем 1.5 ATR
                    sl = min(local_low, entry - (atr_val * 1.5))
                    risk = entry - sl
                    tp1, tp2, tp3 = entry + risk, entry + (risk * 2), entry + (risk * 3)
                else:
                    local_high = float(df['high'].tail(5).max())
                    sl = max(local_high, entry + (atr_val * 1.5))
                    risk = sl - entry
                    tp1, tp2, tp3 = entry - risk, entry - (risk * 2), entry - (risk * 3)

                return {
                    'symbol': symbol,
                    'side': direction,
                    'entry': entry,
                    'tp1': tp1, 'tp2': tp2, 'tp3': tp3, 'sl': sl,
                    'status': 'ULTRA',
                    'confidence': 0.94,
                    'reason': reason,
                    'timeframe': '1h',
                    'volume_24h': daily_volume  # Добавили для отчета
                }

        except Exception as e:
            logging.error(f"Ошибка анализа {symbol}: {e}")
        return None

    async def run_analysis_cycle(self):
        signals = []
        for symbol in self.symbols:
            sig = await self.get_data_and_analyze(symbol)
            if sig:
                signals.append(sig)
            await asyncio.sleep(0.5)
        return signals

    async def close(self):
        await self.exchange.close()