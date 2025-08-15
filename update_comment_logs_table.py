
#!/usr/bin/env python3

import sqlite3
import os
from datetime import datetime

def update_comment_logs_table():
    """Обновление таблицы comment_logs"""
    print("🔄 Обновление таблицы comment_logs...")
    
    db_path = "telegram_sender.db"
    
    if not os.path.exists(db_path):
        print("❌ База данных не найдена")
        return
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Проверяем существующие столбцы
        cursor.execute("PRAGMA table_info(comment_logs)")
        existing_columns = [row[1] for row in cursor.fetchall()]
        print(f"📋 Существующие столбцы: {existing_columns}")
        
        # Добавляем новый столбец если его нет
        if 'comment_message_id' not in existing_columns:
            cursor.execute("ALTER TABLE comment_logs ADD COLUMN comment_message_id INTEGER")
            print("✅ Добавлен столбец comment_message_id")
        else:
            print("📋 Столбец comment_message_id уже существует")
        
        # Делаем campaign_id nullable если он не nullable
        cursor.execute("PRAGMA table_info(comment_logs)")
        columns_info = cursor.fetchall()
        
        campaign_id_nullable = True
        for col in columns_info:
            if col[1] == 'campaign_id' and col[3] == 1:  # not null
                campaign_id_nullable = False
                break
        
        if not campaign_id_nullable:
            print("🔄 Пересоздаем таблицу с nullable campaign_id...")
            
            # Создаем новую таблицу
            cursor.execute("""
                CREATE TABLE comment_logs_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    campaign_id INTEGER,
                    account_id INTEGER,
                    chat_id VARCHAR,
                    message_id INTEGER,
                    comment TEXT,
                    status VARCHAR,
                    error_message TEXT,
                    comment_message_id INTEGER,
                    sent_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (campaign_id) REFERENCES comment_campaigns (id),
                    FOREIGN KEY (account_id) REFERENCES accounts (id)
                )
            """)
            
            # Копируем данные
            cursor.execute("""
                INSERT INTO comment_logs_new 
                (id, campaign_id, account_id, chat_id, message_id, comment, status, error_message, sent_at)
                SELECT id, campaign_id, account_id, chat_id, message_id, comment, status, error_message, sent_at
                FROM comment_logs
            """)
            
            # Удаляем старую таблицу и переименовываем новую
            cursor.execute("DROP TABLE comment_logs")
            cursor.execute("ALTER TABLE comment_logs_new RENAME TO comment_logs")
            
            print("✅ Таблица пересоздана с правильной структурой")
        
        conn.commit()
        print("🎉 Обновление таблицы comment_logs завершено!")
        
    except Exception as e:
        print(f"❌ Ошибка обновления таблицы: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    update_comment_logs_table()
