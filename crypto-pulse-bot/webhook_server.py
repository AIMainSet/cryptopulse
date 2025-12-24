from aiohttp import web
import asyncio
import json
from database import db


async def handle_signal(request):
    """Обработчик вебхука для получения сигналов"""
    try:
        data = await request.json()

        # Сохраняем сигнал в БД
        signal_id = await db.add_signal(
            symbol=data['symbol'],
            direction=data['direction'],
            entry=data['entry'],
            tp1=data.get('tp1'),
            tp2=data.get('tp2'),
            tp3=data.get('tp3'),
            sl=data.get('sl'),
            risk=data.get('risk', 'Medium'),
            reason=data.get('reason', '')
        )

        if signal_id:
            # Получаем сигнал
            signal = await db.get_signal_by_id(signal_id)

            # Здесь можно вызвать функцию отправки уведомлений
            # (нужно импортировать из bot.py)

            return web.json_response({
                'status': 'success',
                'signal_id': signal_id,
                'message': 'Signal received and stored'
            })
        else:
            return web.json_response({
                'status': 'error',
                'message': 'Failed to save signal'
            }, status=500)

    except Exception as e:
        return web.json_response({
            'status': 'error',
            'message': str(e)
        }, status=400)


app = web.Application()
app.router.add_post('/send_signal', handle_signal)

if __name__ == '__main__':
    web.run_app(app, port=8000)