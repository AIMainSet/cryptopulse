import mplfinance as mpf
import pandas as pd
import os


def create_signal_chart(df, symbol, entry, tp, sl, side):
    """Создает профессиональный график для сигнала"""
    # Подготовка данных для mplfinance
    df.index = pd.to_datetime(df['timestamp'], unit='ms')

    # Добавляем индикаторы на график
    added_plots = [
        mpf.make_addplot(df['ema_50'], color='#f39c12', width=1.0),
        mpf.make_addplot(df['ema_200'], color='#3498db', width=1.0),
    ]

    # Цветовая схема
    style = mpf.make_mpf_style(
        base_mpf_style='charles',
        gridcolor='#2c2e3a',
        facecolor='#131722',
        edgecolor='#2c2e3a',
        figcolor='#131722'
    )

    file_path = f"charts/chart_{symbol.replace('/', '_')}.png"
    os.makedirs("charts", exist_ok=True)

    # Отрисовка
    mpf.plot(
        df.tail(60),  # Показываем последние 60 свечей
        type='candle',
        style=style,
        addplot=added_plots,
        hlines=dict(hlines=[entry, tp, sl], colors=['#ffffff', '#2ecc71', '#e74c3c'], linestyle='-.', linewidths=1.5),
        savefig=file_path,
        volume=False,
        tight_layout=True
    )
    return file_path
