
#!/usr/bin/env python3
"""
Скрипт для пересоздания базы данных
"""

import os
import sqlite3
from app.database import engine, Base
from app.auth import create_admin_user_if_not_exists
from app.database import get_db

def recreate_database():
    """Пересоздает базу данных"""
    db_path = "telegram_sender.db"
    
    # Удаляем старую базу данных если она существует
    if os.path.exists(db_path):
        try:
            os.remove(db_path)
            print("Старая база данных удалена")
        except Exception as e:
            print(f"Ошибка при удалении старой базы данных: {e}")
            return False
    
    try:
        # Создаем новую базу данных напрямую через SQLite
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.close()
        print("База данных создана через SQLite")
        
        # Устанавливаем права доступа
        os.chmod(db_path, 0o666)
        print("Права доступа установлены")
        
        # Создаем таблицы через SQLAlchemy
        Base.metadata.create_all(bind=engine)
        print("Таблицы созданы через SQLAlchemy")
        
        # Создаем администратора
        db = next(get_db())
        try:
            create_admin_user_if_not_exists(db)
            print("Администратор создан")
        finally:
            db.close()
        
        return True
        
    except Exception as e:
        print(f"Ошибка создания базы данных: {e}")
        return False

if __name__ == "__main__":
    if recreate_database():
        print("✅ База данных успешно пересоздана!")
    else:
        print("❌ Ошибка при создании базы данных")
