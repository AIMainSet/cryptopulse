"""
Мониторинг нескольких бирж для проверки цен и объемов
"""
import asyncio
import logging
import ccxt.async_support as ccxt
from typing import Dict, List
import numpy as np

logger = logging.getLogger(__name__)


class MultiExchangeMonitor:
    def __init__(self):
        self.exchanges = {
            'binance': ccxt.binance({'enableRateLimit': True}),
            'bybit': ccxt.bybit({'enableRateLimit': True}),
            'kucoin': ccxt.kucoin({'enableRateLimit': True}),
            'okx': ccxt.okx({'enableRateLimit': True}),
        }

        # Основные пары для мониторинга
        self.symbols = ['BTC/USDT', 'ETH/USDT', 'BNB/USDT']

    async def get_multi_exchange_data(self, symbol: str) -> Dict:
        """Получение данных с нескольких бирж"""
        results = {}

        for name, exchange in self.exchanges.items():
            try:
                # Получаем тикер
                ticker = await exchange.fetch_ticker(symbol)

                # Получаем стакан
                orderbook = await exchange.fetch_order_book(symbol, limit=10)

                # Получаем последние сделки
                trades = await exchange.fetch_trades(symbol, limit=10)

                results[name] = {
                    'price': ticker['last'],
                    'bid': ticker['bid'],
                    'ask': ticker['ask'],
                    'volume_24h': ticker['quoteVolume'],
                    'spread': (ticker['ask'] - ticker['bid']) / ticker['last'] * 100,
                    'orderbook_depth': self.calculate_orderbook_depth(orderbook),
                    'trade_flow': self.analyze_trade_flow(trades),
                    'timestamp': ticker['timestamp']
                }

                logger.debug(f"✅ Данные с {name}: {ticker['last']}")

            except Exception as e:
                logger.warning(f"❌ Ошибка получения данных с {name}: {e}")
                continue

        if not results:
            return None

        # Анализируем расхождения
        analysis = self.analyze_price_discrepancies(results)

        return {
            'exchange_data': results,
            'analysis': analysis,
            'consensus_price': analysis['weighted_price'],
            'reliable': analysis['max_discrepancy'] < 0.02,  # Расхождение менее 2%
            'symbol': symbol
        }

    def calculate_orderbook_depth(self, orderbook, depth_percent=0.5):
        """Расчет глубины стакана"""
        if not orderbook or 'bids' not in orderbook or 'asks' not in orderbook:
            return 0

        mid_price = (orderbook['bids'][0][0] + orderbook['asks'][0][0]) / 2

        # Объем в пределах depth_percent от цены
        bid_volume = sum(vol for price, vol in orderbook['bids']
                         if price >= mid_price * (1 - depth_percent / 100))
        ask_volume = sum(vol for price, vol in orderbook['asks']
                         if price <= mid_price * (1 + depth_percent / 100))

        return {'bid_volume': bid_volume, 'ask_volume': ask_volume}

    def analyze_trade_flow(self, trades):
        """Анализ потока сделок"""
        if not trades:
            return {'buy_volume': 0, 'sell_volume': 0, 'ratio': 0}

        buy_volume = sum(t['amount'] for t in trades if t['side'] == 'buy')
        sell_volume = sum(t['amount'] for t in trades if t['side'] == 'sell')

        total = buy_volume + sell_volume
        ratio = buy_volume / total if total > 0 else 0.5

        return {
            'buy_volume': buy_volume,
            'sell_volume': sell_volume,
            'ratio': ratio,
            'trend': 'bullish' if ratio > 0.6 else 'bearish' if ratio < 0.4 else 'neutral'
        }

    def analyze_price_discrepancies(self, exchange_data: Dict) -> Dict:
        """Анализ расхождений в ценах между биржами"""
        prices = [data['price'] for data in exchange_data.values()]
        volumes = [data['volume_24h'] for data in exchange_data.values()]

        if not prices:
            return {}

        # Средневзвешенная цена по объему
        total_volume = sum(volumes)
        if total_volume > 0:
            weighted_price = sum(p * v for p, v in zip(prices, volumes)) / total_volume
        else:
            weighted_price = np.mean(prices)

        # Максимальное расхождение
        max_price = max(prices)
        min_price = min(prices)
        max_discrepancy = (max_price - min_price) / weighted_price

        # Определяем наиболее ликвидную биржу
        most_liquid = max(exchange_data.items(), key=lambda x: x[1]['volume_24h'])[0]

        return {
            'weighted_price': weighted_price,
            'average_price': np.mean(prices),
            'median_price': np.median(prices),
            'max_discrepancy': max_discrepancy,
            'price_range': {'min': min_price, 'max': max_price},
            'most_liquid_exchange': most_liquid,
            'exchange_count': len(exchange_data),
            'volume_total': total_volume
        }

    async def validate_signal_price(self, signal_symbol: str, signal_price: float) -> Dict:
        """Валидация цены сигнала по данным с нескольких бирж"""
        market_data = await self.get_multi_exchange_data(signal_symbol)

        if not market_data:
            return {'valid': False, 'reason': 'No market data available'}

        consensus_price = market_data['analysis']['weighted_price']
        price_difference = abs(signal_price - consensus_price) / consensus_price

        is_valid = price_difference < 0.01  # Разница менее 1%

        return {
            'valid': is_valid,
            'price_difference': price_difference,
            'consensus_price': consensus_price,
            'signal_price': signal_price,
            'market_data': market_data,
            'recommendation': 'VALID' if is_valid else 'CHECK_PRICE'
        }

    async def close_all(self):
        """Закрытие всех соединений"""
        for exchange in self.exchanges.values():
            await exchange.close()

    async def monitor_all_symbols(self):
        """Мониторинг всех символов"""
        logger.info("👁️ Мониторинг всех пар на нескольких биржах...")

        results = {}
        for symbol in self.symbols:
            try:
                data = await self.get_multi_exchange_data(symbol)
                if data:
                    results[symbol] = data
                    logger.info(f"✅ {symbol}: ${data['analysis']['weighted_price']:.2f} "
                                f"(discrepancy: {data['analysis']['max_discrepancy']:.2%})")
            except Exception as e:
                logger.error(f"Ошибка мониторинга {symbol}: {e}")

        return results