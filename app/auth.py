
from datetime import datetime, timedelta
from typing import Optional
from fastapi import Depends, HTTPException, status, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
from app.database import User, UserSession, get_db
import secrets
import hashlib

security = HTTPBearer(auto_error=False)

def create_admin_user_if_not_exists(db: Session):
    """Создает админа по умолчанию если его нет"""
    admin = db.query(User).filter(User.username == "Dimita777").first()
    if not admin:
        admin = User(
            username="Dimita777",
            is_admin=True,
            is_active=True
        )
        admin.set_password("KnigaBratan7")  # Пароль по умолчанию
        db.add(admin)
        db.commit()
        print("Создан администратор: Dimita777 / KnigaBratan7")
        return admin
    return admin

def create_session_token(user_id: int, db: Session, user_agent: str = None, ip_address: str = None) -> str:
    """Создает новую сессию для пользователя"""
    token = secrets.token_urlsafe(32)
    
    session = UserSession(
        user_id=user_id,
        session_token=token,
        expires_at=datetime.utcnow() + timedelta(days=30),  # Сессия на 30 дней
        user_agent=user_agent,
        ip_address=ip_address
    )
    
    db.add(session)
    db.commit()
    
    return token

def get_user_from_token(token: str, db: Session) -> Optional[User]:
    """Получает пользователя по токену сессии"""
    if not token:
        return None
        
    session = db.query(UserSession).filter(
        UserSession.session_token == token,
        UserSession.expires_at > datetime.utcnow()
    ).first()
    
    if not session:
        return None
    
    user = db.query(User).filter(
        User.id == session.user_id,
        User.is_active == True
    ).first()
    
    return user

def get_current_user(
    request: Request,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db)
) -> User:
    """Получает текущего аутентифицированного пользователя"""
    
    # Создаем админа если его нет
    create_admin_user_if_not_exists(db)
    
    # Проверяем токен из заголовка Authorization
    token = None
    if credentials:
        token = credentials.credentials
    
    # Если токена в заголовке нет, проверяем cookies
    if not token:
        token = request.cookies.get("session_token")
    
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Не найден токен аутентификации",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    user = get_user_from_token(token, db)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Недействительный или истекший токен",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    return user

def get_current_admin(current_user: User = Depends(get_current_user)) -> User:
    """Проверяет что текущий пользователь - администратор"""
    if not current_user.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Недостаточно прав доступа"
        )
    return current_user

def authenticate_user(username: str, password: str, db: Session) -> Optional[User]:
    """Аутентификация пользователя"""
    user = db.query(User).filter(User.username == username).first()
    if not user or not user.check_password(password) or not user.is_active:
        return None
    return user

def invalidate_session(token: str, db: Session):
    """Инвалидирует сессию"""
    session = db.query(UserSession).filter(UserSession.session_token == token).first()
    if session:
        db.delete(session)
        db.commit()
import hashlib
import secrets
from datetime import datetime, timedelta
from typing import Optional
from sqlalchemy.orm import Session
from fastapi import Depends, HTTPException, status, Request
from app.database import User, UserSession, get_db

def hash_password(password: str) -> str:
    """Хеширование пароля"""
    return hashlib.sha256(password.encode()).hexdigest()

def verify_password(password: str, hashed_password: str) -> bool:
    """Проверка пароля"""
    return hash_password(password) == hashed_password

def create_session_token() -> str:
    """Создание токена сессии"""
    return secrets.token_urlsafe(32)

def authenticate_user(username: str, password: str, db: Session) -> Optional[User]:
    """Аутентификация пользователя"""
    user = db.query(User).filter(User.username == username).first()
    if user and verify_password(password, user.password_hash):
        return user
    return None

def create_admin_user_if_not_exists(db: Session):
    """Создание администратора если его нет"""
    admin = db.query(User).filter(User.username == "admin").first()
    if not admin:
        admin = User(
            username="admin",
            password_hash=hash_password("admin123"),
            is_admin=True
        )
        db.add(admin)
        db.commit()

def get_current_user(request: Request, db: Session = Depends(get_db)) -> Optional[User]:
    """Получение текущего пользователя"""
    token = request.cookies.get("session_token")
    if not token:
        return None
    
    session = db.query(UserSession).filter(UserSession.session_token == token).first()
    if not session or session.expires_at < datetime.utcnow():
        return None
    
    return db.query(User).filter(User.id == session.user_id).first()

def get_current_admin(request: Request, db: Session = Depends(get_db)) -> User:
    """Получение текущего администратора"""
    user = get_current_user(request, db)
    if not user or not user.is_admin:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN)
    return user

def invalidate_session(token: str, db: Session):
    """Инвалидация сессии"""
    session = db.query(UserSession).filter(UserSession.session_token == token).first()
    if session:
        db.delete(session)
        db.commit()
