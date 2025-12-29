import asyncio
from concurrent.futures import ProcessPoolExecutor
import pandas as pd
import pandas_ta as ta
import logging

# Настройка логгера
logger = logging.getLogger(__name__)

def cpu_bound_analysis(data_list):
    """
    Тяжелая функция анализа. Выполняется в отдельном процессе.
    Никогда не вызывай её напрямую из async функций!
    """
    try:
        # Превращаем список свечей в DataFrame
        # Предполагаем, что данные приходят в формате OHLCV
        df = pd.DataFrame(data_list, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        
        # Конвертация типов
        df['close'] = df['close'].astype(float)
        df['volume'] = df['volume'].astype(float)
        
        # --- ТЕХНИЧЕСКИЙ АНАЛИЗ (Pandas TA) ---
        # Эти операции блокируют процессор, поэтому они здесь
        
        # 1. RSI
        df.ta.rsi(length=14, append=True)
        
        # 2. Bollinger Bands
        df.ta.bbands(length=20, append=True)
        
        # 3. MACD
        df.ta.macd(append=True)
        
        # 4. ADX (Сила тренда)
        df.ta.adx(length=14, append=True)
        
        # 5. EMA 200 (Глобальный тренд)
        df.ta.ema(length=200, append=True)
        
        # Берем последнюю закрытую свечу (предпоследний ряд, если текущая еще формируется)
        # Или последнюю, если мы работаем по закрытию. Обычно берут -1.
        last = df.iloc[-1]
        
        # Формируем легкий словарь с результатами, чтобы вернуть его в основной поток
        result = {
            'close': last['close'],
            'volume': last['volume'],
            'volume_mean_20': df['volume'].rolling(20).mean().iloc[-1],
            'rsi': last['RSI_14'],
            'adx': last.get('ADX_14', 0), # Иногда pandas_ta меняет названия колонок
            'ema_200': last.get('EMA_200', 0),
            # MACD возвращает 3 колонки, нужно найти правильные имена
            'macd': last.get('MACD_12_26_9', 0),
            'macd_signal': last.get('MACDs_12_26_9', 0)
        }
        return result

    except Exception as e:
        logger.error(f"Error in CPU analysis: {e}")
        return None

class MarketMonitor:
    def __init__(self):
        # Создаем пул процессов (используем 4 ядра или меньше)
        self._executor = ProcessPoolExecutor(max_workers=4)

    async def analyze_pair(self, klines_data):
        """
        Асинхронная обертка. Вызывает тяжелую функцию в другом процессе.
        """
        loop = asyncio.get_running_loop()
        
        # Запускаем вычисления в ProcessPool
        # Это магия, которая убирает лаги
        analysis_result = await loop.run_in_executor(
            self._executor, 
            cpu_bound_analysis, 
            klines_data
        )
        
        return analysis_result

    def shutdown(self):
        """Нужно вызывать при остановке бота"""
        self._executor.shutdown()
