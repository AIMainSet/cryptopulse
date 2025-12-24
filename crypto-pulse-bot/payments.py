from aiocryptopay import AioCryptoPay, Networks
from config import config  # Импортируем наш защищенный конфиг

# Используем токен из .env через объект config
# network=Networks.TEST_NET пока оставляем для тестов
crypto = AioCryptoPay(
    token=config.CRYPTOBOT_TOKEN,
    network=Networks.TEST_NET
)

async def create_invoice(amount: float, user_id: int):
    # Создаем счет. В description можно передать ID юзера для учета
    invoice = await crypto.create_invoice(
        asset='USDT',
        amount=amount,
        payload=str(user_id) # Скрытые данные, которые вернутся после оплаты
    )
    return invoice.bot_invoice_url, invoice.invoice_id

async def check_invoice_status(invoice_id: int):
    # Запрашиваем данные о счете по его ID
    invoices = await crypto.get_invoices(invoice_ids=invoice_id)
    if invoices:
        # Если статус 'paid', значит деньги пришли
        return invoices.status == 'paid'
    return False