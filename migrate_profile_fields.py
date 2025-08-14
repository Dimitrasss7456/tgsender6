
#!/usr/bin/env python3
"""
Скрипт миграции для добавления новых полей профиля в таблицу accounts
"""

import sqlite3
import os

def migrate_database():
    """Выполняет миграцию базы данных"""
    db_path = "telegram_sender.db"
    
    if not os.path.exists(db_path):
        print("❌ База данных не найдена!")
        return False
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Проверяем существующие столбцы
        cursor.execute("PRAGMA table_info(accounts)")
        existing_columns = [row[1] for row in cursor.fetchall()]
        
        # Добавляем новые столбцы если их нет
        new_columns = [
            ("first_name", "TEXT"),
            ("last_name", "TEXT"), 
            ("bio", "TEXT"),
            ("gender", "TEXT"),
            ("profile_photo_path", "TEXT")
        ]
        
        for column_name, column_type in new_columns:
            if column_name not in existing_columns:
                try:
                    cursor.execute(f"ALTER TABLE accounts ADD COLUMN {column_name} {column_type}")
                    print(f"✅ Добавлен столбец: {column_name}")
                except sqlite3.Error as e:
                    print(f"⚠️ Ошибка добавления столбца {column_name}: {e}")
        
        # Создаем новые таблицы
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS comment_campaigns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                post_url TEXT NOT NULL,
                comments_male TEXT,
                comments_female TEXT,
                delay_seconds INTEGER DEFAULT 60,
                status TEXT DEFAULT 'created',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                started_at TIMESTAMP,
                completed_at TIMESTAMP
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS comment_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                campaign_id INTEGER,
                account_id INTEGER,
                comment_text TEXT,
                status TEXT,
                error_message TEXT,
                sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (campaign_id) REFERENCES comment_campaigns (id),
                FOREIGN KEY (account_id) REFERENCES accounts (id)
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS reaction_campaigns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                post_url TEXT NOT NULL,
                reaction_emoji TEXT NOT NULL,
                delay_seconds INTEGER DEFAULT 30,
                status TEXT DEFAULT 'created',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS view_campaigns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                post_url TEXT NOT NULL,
                delay_seconds INTEGER DEFAULT 10,
                status TEXT DEFAULT 'created',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.commit()
        print("✅ Миграция завершена успешно!")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка миграции: {e}")
        return False
    finally:
        conn.close()

if __name__ == "__main__":
    print("🔄 Запуск миграции базы данных...")
    success = migrate_database()
    if success:
        print("🎉 Миграция завершена! Перезапустите приложение.")
    else:
        print("💥 Миграция не удалась!")
