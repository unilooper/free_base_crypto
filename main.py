import logging
import sqlite3
import csv
import os
from datetime import datetime
from typing import Optional, Dict, Any
import requests
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    ContextTypes,
    ConversationHandler,
)

# Конфигурация
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    handlers=[
        logging.FileHandler("bot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

EXCHANGE_TYPES = {"crypto": 0.02, "cleaning": 0.10}
SUPPORTED_PAIRS = {
    "BTC-USDT": "BTCUSDT",
    "ETH-USDT": "ETHUSDT",
    "BNB-USDT": "BNBUSDT",
    "XRP-USDT": "XRPUSDT",
    "TRX-USDT": "TRXUSDT",
    "cleaning": None,
}

INPUT_AMOUNT = 1
HISTORY_FILE = "transactions.csv"

class DatabaseManager:
    def __init__(self):
        self.conn = sqlite3.connect('exchanges.db', check_same_thread=False)
        self._init_db()

    def _init_db(self):
        with self.conn:
            self.conn.execute('''CREATE TABLE IF NOT EXISTS transactions
                               (id INTEGER PRIMARY KEY AUTOINCREMENT,
                                user_id INTEGER,
                                username TEXT,
                                operation_type TEXT,
                                from_amount REAL,
                                to_amount REAL,
                                commission REAL,
                                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)''')

    def save_transaction(self, user_id: int, username: str, operation: str, 
                        from_amount: float, to_amount: float, commission: float):
        with self.conn:
            self.conn.execute('''INSERT INTO transactions 
                               (user_id, username, operation_type, from_amount, to_amount, commission)
                               VALUES (?, ?, ?, ?, ?, ?)''',
                               (user_id, username, operation, from_amount, to_amount, commission))

    def get_history(self, user_id: int) -> list:
        try:
            cursor = self.conn.cursor()
            cursor.execute('''SELECT username, operation_type, from_amount, 
                            to_amount, commission, timestamp 
                            FROM transactions 
                            WHERE user_id = ? 
                            ORDER BY timestamp DESC''', (user_id,))
            return cursor.fetchall()
        except Exception as e:
            logger.error(f"Database error: {e}")
            return []

class BinanceAPI:
    @staticmethod
    def get_price(symbol: str) -> Optional[float]:
        retries = 3
        for _ in range(retries):
            try:
                response = requests.get(
                    f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}",
                    timeout=5
                )
                response.raise_for_status()
                return float(response.json()["price"])
            except requests.exceptions.RequestException as e:
                logger.warning(f"Binance API error: {e}, retrying...")
            except (KeyError, ValueError) as e:
                logger.error(f"Data parsing error: {e}")
                break
        return None

class DataExporter:
    @staticmethod
    def save_to_csv(data: Dict[str, Any]):
        file_exists = os.path.isfile(HISTORY_FILE)
        try:
            with open(HISTORY_FILE, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                if not file_exists:
                    writer.writerow([
                        'timestamp', 'user_id', 'username', 
                        'operation', 'from_amount', 
                        'to_amount', 'commission'
                    ])
                writer.writerow([
                    data['timestamp'],
                    data['user_id'],
                    data['username'],
                    data['operation'],
                    data['from_amount'],
                    data['to_amount'],
                    data['commission']
                ])
        except Exception as e:
            logger.error(f"CSV save error: {e}")

# Инициализация компонентов
db = DatabaseManager()
binance = BinanceAPI()
exporter = DataExporter()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    buttons = [
        [InlineKeyboardButton("BTC ↔ USDT", callback_data="BTC-USDT"),
         InlineKeyboardButton("USDT ↔ BTC", callback_data="USDT-BTC")],
        [InlineKeyboardButton("ETH ↔ USDT", callback_data="ETH-USDT"),
         InlineKeyboardButton("USDT ↔ ETH", callback_data="USDT-ETH")],
        [InlineKeyboardButton("🧹 Очистка (10%)", callback_data="cleaning")],
    ]
    
    await update.message.reply_text(
        "📊 Выберите тип операции:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )
    return INPUT_AMOUNT

async def handle_operation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    operation = query.data
    context.user_data.clear()
    context.user_data["operation"] = operation
    
    if operation == "cleaning":
        await query.edit_message_text("Введите сумму для очистки:")
        return INPUT_AMOUNT
    
    try:
        from_cur, to_cur = operation.split('-')
        symbol = SUPPORTED_PAIRS.get(f"{from_cur}-{to_cur}")
        price = binance.get_price(symbol)
        
        if not price:
            raise ValueError("Не удалось получить курс")
            
        context.user_data["price"] = price
        await query.edit_message_text(f"Введите количество {from_cur}:")
        return INPUT_AMOUNT
    
    except Exception as e:
        logger.error(f"Operation error: {e}")
        await query.edit_message_text("❌ Ошибка операции")
        return ConversationHandler.END

async def process_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        amount = float(update.message.text.replace(',', '.'))
        if amount <= 0:
            raise ValueError
    except:
        await update.message.reply_text("⚠️ Введите положительное число!")
        return INPUT_AMOUNT
    
    user_data = context.user_data
    operation = user_data.get("operation")
    
    if not operation:
        await update.message.reply_text("❌ Сессия устарела")
        return ConversationHandler.END
    
    user = update.message.from_user
    user_data["user"] = {
        "id": user.id,
        "username": user.username or f"user_{user.id}"
    }
    
    if operation == "cleaning":
        commission = amount * EXCHANGE_TYPES["cleaning"]
        result = amount - commission
        text = (
            f"🧹 Очистка средств\n"
            f"• Введено: {amount:.8f}\n"
            f"• Комиссия: {commission:.8f}\n"
            f"• Итого: {result:.8f}"
        )
    else:
        from_cur, to_cur = operation.split('-')
        price = user_data.get("price")
        
        if from_cur == "USDT":
            calculated = amount / price
            commission = calculated * EXCHANGE_TYPES["crypto"]
            result = calculated - commission
        else:
            calculated = amount * price
            commission = calculated * EXCHANGE_TYPES["crypto"]
            result = calculated - commission
        
        text = (
            f"🔁 Обмен {amount:.8f} {from_cur}\n"
            f"• Курс: 1 {from_cur} = {price:.8f} {to_cur}\n"
            f"• Итого: {result:.8f} {to_cur}\n"
            f"• Комиссия: {commission:.8f} {to_cur}"
        )
    
    user_data.update({
        "from_amount": amount,
        "to_amount": result,
        "commission": commission
    })
    
    keyboard = [
        [InlineKeyboardButton("✅ Подтвердить", callback_data="confirm"),
         InlineKeyboardButton("❌ Отменить", callback_data="cancel")]
    ]
    
    await update.message.reply_text(
        text + "\n\nПодтвердить операцию?",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    return ConversationHandler.END

async def handle_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    user_data = context.user_data
    choice = query.data
    
    if choice == "confirm":
        try:
            user_info = user_data["user"]
            operation = user_data["operation"]
            
            # Сохранение в базу данных
            db.save_transaction(
                user_id=user_info["id"],
                username=user_info["username"],
                operation=operation,
                from_amount=user_data["from_amount"],
                to_amount=user_data["to_amount"],
                commission=user_data["commission"]
            )
            
            # Сохранение в CSV
            exporter.save_to_csv({
                "timestamp": datetime.now().isoformat(),
                "user_id": user_info["id"],
                "username": user_info["username"],
                "operation": operation,
                "from_amount": user_data["from_amount"],
                "to_amount": user_data["to_amount"],
                "commission": user_data["commission"]
            })
            
            await query.edit_message_text("✅ Операция успешно сохранена")
        except Exception as e:
            logger.error(f"Save error: {e}")
            await query.edit_message_text("❌ Ошибка сохранения")
    else:
        await query.edit_message_text("❌ Операция отменена")
    
    context.user_data.clear()

async def show_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    transactions = db.get_history(user_id)
    
    if not transactions:
        await update.message.reply_text("📭 История операций пуста")
        return
    
    response = ["📜 История операций:\n"]
    
    for idx, tr in enumerate(transactions, 1):
        try:
            username, operation, from_amt, to_amt, commission, timestamp = tr
            date = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S").strftime("%d.%m.%Y %H:%M")
            
            if operation == "cleaning":
                response.append(
                    f"{idx}. 🧹 Очистка ({date})\n"
                    f"   Пользователь: @{username}\n"
                    f"   Сумма: {from_amt:.8f}\n"
                    f"   Итого: {to_amt:.8f}\n"
                    f"   Комиссия: {commission:.8f}\n"
                )
            else:
                from_cur, to_cur = operation.split('-')
                response.append(
                    f"{idx}. 🔄 {from_cur} → {to_cur} ({date})\n"
                    f"   Пользователь: @{username}\n"
                    f"   Отправлено: {from_amt:.8f} {from_cur}\n"
                    f"   Получено: {to_amt:.8f} {to_cur}\n"
                    f"   Комиссия: {commission:.8f} {to_cur}\n"
                )
        except Exception as e:
            logger.error(f"History format error: {e}")
    
    await update.message.reply_text("\n".join(response))

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🤖 Используйте кнопки для управления ботом")

def main():
    application = Application.builder().token("7711455065:AAHqndm9piVJTt4TfxDn7ikmb6ZDuN0netI").build()
    
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            INPUT_AMOUNT: [
                CallbackQueryHandler(handle_operation),
                MessageHandler(filters.TEXT & ~filters.COMMAND, process_amount)
            ]
        },
        fallbacks=[CommandHandler("cancel", lambda u, c: ConversationHandler.END)],
    )
    
    application.add_handler(conv_handler)
    application.add_handler(CallbackQueryHandler(handle_confirmation, pattern="^(confirm|cancel)$"))
    application.add_handler(CommandHandler("history", show_history))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    
    logger.info("Бот запущен")
    application.run_polling()

if __name__ == "__main__":
    main()
