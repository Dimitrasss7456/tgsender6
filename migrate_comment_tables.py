
import sqlite3
import os

def migrate_comment_tables():
    """Миграция таблиц для кампаний комментирования с обновлением существующих"""
    print("🔄 Запуск миграции таблиц кампаний...")
    
    db_path = "telegram_sender.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Создаем таблицу comment_campaigns если не существует
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
        print("✅ Создана/проверена таблица: comment_campaigns")
        
        # Проверяем существующую структуру comment_logs
        cursor.execute("PRAGMA table_info(comment_logs)")
        existing_columns = [row[1] for row in cursor.fetchall()]
        print(f"📋 Существующие столбцы в comment_logs: {existing_columns}")
        
        # Если таблица comment_logs существует, но не имеет нужных столбцов - пересоздаем её
        if existing_columns and 'chat_id' not in existing_columns:
            print("🔄 Пересоздаем таблицу comment_logs с новой структурой...")
            
            # Сохраняем старые данные если есть
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='comment_logs'")
            if cursor.fetchone():
                cursor.execute("ALTER TABLE comment_logs RENAME TO comment_logs_old")
                print("📦 Старая таблица переименована в comment_logs_old")
        
        # Создаем новую таблицу comment_logs с правильной структурой
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
        
        # Мигрируем данные из старой таблицы если она существует
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='comment_logs_old'")
        if cursor.fetchone():
            try:
                cursor.execute("""
                    INSERT INTO comment_logs (campaign_id, account_id, chat_id, message_id, comment, status, error_message, sent_at)
                    SELECT campaign_id, account_id, '', 0, comment_text, status, error_message, sent_at
                    FROM comment_logs_old
                """)
                print("📦 Данные мигрированы из старой таблицы")
                
                # Удаляем старую таблицу
                cursor.execute("DROP TABLE comment_logs_old")
                print("🗑️ Старая таблица удалена")
            except Exception as migrate_error:
                print(f"⚠️ Ошибка миграции данных: {migrate_error}")
        
        # Создаем таблицу reaction_campaigns если не существует
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
        
        # Создаем таблицу view_campaigns если не существует
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
        print("🎉 Миграция завершена успешно!")
        
    except Exception as e:
        conn.rollback()
        print(f"❌ Ошибка миграции: {e}")
        raise e
    finally:
        conn.close()

if __name__ == "__main__":
    migrate_comment_tables()
