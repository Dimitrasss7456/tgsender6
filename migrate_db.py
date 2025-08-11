
#!/usr/bin/env python3
"""
Скрипт для миграции базы данных
"""

import sqlite3
import os

def migrate_database():
    """Выполняет миграцию базы данных"""
    db_path = "telegram_sender.db"
    
    if not os.path.exists(db_path):
        print("База данных не найдена!")
        return
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Проверяем существование столбцов
        cursor.execute("PRAGMA table_info(campaigns)")
        columns = [column[1] for column in cursor.fetchall()]
        
        # Добавляем недостающие столбцы
        if 'auto_delete_accounts' not in columns:
            cursor.execute("ALTER TABLE campaigns ADD COLUMN auto_delete_accounts BOOLEAN DEFAULT FALSE")
            print("Добавлен столбец auto_delete_accounts")
        
        if 'delete_delay_minutes' not in columns:
            cursor.execute("ALTER TABLE campaigns ADD COLUMN delete_delay_minutes INTEGER DEFAULT 5")
            print("Добавлен столбец delete_delay_minutes")
        
        conn.commit()
        print("Миграция выполнена успешно!")
        
    except Exception as e:
        print(f"Ошибка миграции: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    migrate_database()
