from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean, Text, Float, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
from app.config import DATABASE_URL
import hashlib
import secrets

engine = create_engine(
    DATABASE_URL, 
    connect_args={"check_same_thread": False},
    pool_size=50,  # Значительно увеличиваем базовый размер пула
    max_overflow=100,  # Увеличиваем overflow для больших нагрузок
    pool_timeout=120,  # Увеличиваем таймаут ожидания соединения
    pool_recycle=1800,  # Переиспользуем соединения в течение 30 минут
    pool_pre_ping=True,  # Проверяем соединения перед использованием
    pool_reset_on_return='commit'  # Сбрасываем транзакции при возврате соединения
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

# Создаем таблицы
Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()