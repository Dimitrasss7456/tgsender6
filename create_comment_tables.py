
#!/usr/bin/env python3

from app.database import engine, Base, CommentLog
from sqlalchemy import inspect

def create_comment_tables():
    """Создание таблиц для истории комментариев"""
    try:
        print("🔄 Создание таблиц для истории комментариев...")
        
        # Проверяем существуют ли таблицы
        inspector = inspect(engine)
        existing_tables = inspector.get_table_names()
        
        if 'comment_logs' not in existing_tables:
            print("📝 Создание таблицы comment_logs...")
            CommentLog.__table__.create(engine, checkfirst=True)
            print("✅ Таблица comment_logs создана")
        else:
            print("📋 Таблица comment_logs уже существует")
        
        print("🎉 Все таблицы готовы!")
        
    except Exception as e:
        print(f"❌ Ошибка создания таблиц: {e}")
        raise

if __name__ == "__main__":
    create_comment_tables()
