
import sqlite3
import os

def migrate_comment_tables():
    """Создание таблиц для кампаний комментирования"""
    db_path = "telegram_sender.db"
    
    print("🔄 Запуск миграции таблиц кампаний...")
    
    if not os.path.exists(db_path):
        print("❌ База данных не найдена")
        return False
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Создаем таблицу кампаний комментирования
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS comment_campaigns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name VARCHAR NOT NULL,
                post_url VARCHAR NOT NULL,
                comments_male TEXT,
                comments_female TEXT,
                delay_seconds INTEGER DEFAULT 60,
                status VARCHAR DEFAULT 'created',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                started_at DATETIME,
                completed_at DATETIME
            )
        """)
        print("✅ Создана таблица: comment_campaigns")
        
        # Создаем таблицу логов комментариев
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS comment_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                campaign_id INTEGER,
                account_id INTEGER,
                chat_id VARCHAR,
                message_id INTEGER,
                comment TEXT,
                status VARCHAR,
                error_message TEXT,
                sent_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (campaign_id) REFERENCES comment_campaigns (id),
                FOREIGN KEY (account_id) REFERENCES accounts (id)
            )
        """)
        print("✅ Создана таблица: comment_logs")
        
        # Создаем таблицу кампаний реакций
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS reaction_campaigns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name VARCHAR NOT NULL,
                post_url VARCHAR NOT NULL,
                reaction_emoji VARCHAR DEFAULT '👍',
                delay_seconds INTEGER DEFAULT 30,
                status VARCHAR DEFAULT 'created',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print("✅ Создана таблица: reaction_campaigns")
        
        # Создаем таблицу кампаний просмотров
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS view_campaigns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name VARCHAR NOT NULL,
                post_url VARCHAR NOT NULL,
                delay_seconds INTEGER DEFAULT 15,
                status VARCHAR DEFAULT 'created',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print("✅ Создана таблица: view_campaigns")
        
        conn.commit()
        conn.close()
        
        print("🎉 Миграция завершена успешно!")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка миграции: {e}")
        return False

if __name__ == "__main__":
    migrate_comment_tables()
