from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean, Text, Float, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
from app.config import DATABASE_URL
import hashlib
import secrets

engine = create_engine(
    DATABASE_URL, 
    connect_args={
        "check_same_thread": False,
        "timeout": 60  # Таймаут для SQLite операций
    },
    pool_size=100,  # Максимально увеличиваем базовый размер пула
    max_overflow=200,  # Еще больше overflow для пиковых нагрузок
    pool_timeout=180,  # Увеличиваем таймаут ожидания соединения
    pool_recycle=900,  # Переиспользуем соединения в течение 15 минут
    pool_pre_ping=True,  # Проверяем соединения перед использованием
    pool_reset_on_return='commit',  # Сбрасываем транзакции при возврате соединения
    echo=False  # Отключаем SQL логирование для производительности
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True)
    password_hash = Column(String)
    is_admin = Column(Boolean, default=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    last_login = Column(DateTime, nullable=True)

    # Связь с аккаунтами Telegram
    accounts = relationship("Account", back_populates="user")

    def set_password(self, password: str):
        """Устанавливает хеш пароля"""
        self.password_hash = hashlib.sha256(password.encode()).hexdigest()

    def check_password(self, password: str) -> bool:
        """Проверяет пароль"""
        return self.password_hash == hashlib.sha256(password.encode()).hexdigest()

class UserSession(Base):
    __tablename__ = "user_sessions"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    session_token = Column(String, unique=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    expires_at = Column(DateTime)
    user_agent = Column(String, nullable=True)
    ip_address = Column(String, nullable=True)

class Account(Base):
    __tablename__ = "accounts"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True)  # Связь с пользователем
    phone = Column(String, unique=True, index=True)
    name = Column(String)
    status = Column(String, default="offline")  # online, offline, blocked, error
    session_data = Column(Text)  # зашифрованная сессия
    proxy = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    last_activity = Column(DateTime, default=datetime.utcnow)
    messages_sent_today = Column(Integer, default=0)
    messages_sent_hour = Column(Integer, default=0)
    last_message_time = Column(DateTime, nullable=True)
    is_active = Column(Boolean, default=True)
    
    # Новые поля для профиля
    first_name = Column(String, nullable=True)
    last_name = Column(String, nullable=True)
    bio = Column(Text, nullable=True)
    gender = Column(String, nullable=True)  # male, female, other
    profile_photo_path = Column(String, nullable=True)

    # Связь с пользователем
    user = relationship("User", back_populates="accounts")

class Campaign(Base):
    __tablename__ = "campaigns"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    delay_seconds = Column(Integer, default=1)
    private_message = Column(Text)
    group_message = Column(Text)
    channel_message = Column(Text)
    private_list = Column(Text)
    groups_list = Column(Text)
    channels_list = Column(Text)
    attachment_path = Column(String, nullable=True)
    status = Column(String, default="created")
    created_at = Column(DateTime, default=datetime.utcnow)
    account_id = Column(Integer, nullable=True)  # Для кампаний по контактам
    auto_delete_accounts = Column(Boolean, default=False)  # Автоудаление аккаунтов
    delete_delay_minutes = Column(Integer, default=5)  # Задержка перед удалением в минутах

class SendLog(Base):
    __tablename__ = "send_logs"

    id = Column(Integer, primary_key=True, index=True)
    campaign_id = Column(Integer)
    account_id = Column(Integer)
    recipient = Column(String)
    recipient_type = Column(String)  # channel, group, private
    status = Column(String)  # sent, failed, blocked
    message = Column(Text, nullable=True)
    error_message = Column(Text, nullable=True)
    sent_at = Column(DateTime, default=datetime.utcnow)

class CommentCampaign(Base):
    __tablename__ = "comment_campaigns"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    post_url = Column(String)  # URL поста для комментирования
    comments_male = Column(Text)  # Комментарии для мужских аккаунтов (разделенные \n)
    comments_female = Column(Text)  # Комментарии для женских аккаунтов (разделенные \n)
    delay_seconds = Column(Integer, default=60)  # Задержка между комментариями
    status = Column(String, default="created")  # created, running, completed, stopped
    created_at = Column(DateTime, default=datetime.utcnow)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)

class CommentLog(Base):
    __tablename__ = "comment_logs"

    id = Column(Integer, primary_key=True, index=True)
    campaign_id = Column(Integer, ForeignKey("comment_campaigns.id"))
    account_id = Column(Integer, ForeignKey("accounts.id"))
    comment_text = Column(Text)
    status = Column(String)  # sent, failed, blocked
    error_message = Column(Text, nullable=True)
    sent_at = Column(DateTime, default=datetime.utcnow)

class ReactionCampaign(Base):
    __tablename__ = "reaction_campaigns"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    post_url = Column(String)
    reaction_emoji = Column(String)  # Эмодзи для реакции
    delay_seconds = Column(Integer, default=30)
    status = Column(String, default="created")
    created_at = Column(DateTime, default=datetime.utcnow)

class ViewCampaign(Base):
    __tablename__ = "view_campaigns"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    post_url = Column(String)
    delay_seconds = Column(Integer, default=10)
    status = Column(String, default="created")
    created_at = Column(DateTime, default=datetime.utcnow)

# Создаем таблицы
Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_db_session():
    """Получение сессии БД для асинхронных операций"""
    return SessionLocal()

async def get_async_db_session():
    """Асинхронное получение сессии БД с автоматическим закрытием"""
    db = SessionLocal()
    try:
        return db
    except Exception as e:
        db.close()
        raise e