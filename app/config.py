import os
from cryptography.fernet import Fernet
from dotenv import load_dotenv

# Загружаем переменные из .env файла
load_dotenv()

# Настройки приложения
DATABASE_URL = "sqlite:///./telegram_sender.db"

SECRET_KEY = os.getenv("SECRET_KEY", "your_secret_key_here")
ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY", "")

# Настройки Telegram API
API_ID = int(os.getenv("API_ID", os.getenv("TELEGRAM_API_ID", "24599932")))
API_HASH = os.getenv("API_HASH", os.getenv("TELEGRAM_API_HASH", "51bec1393e6f753d6dfcf48bb9c4119d"))

# Настройки рассылки
DEFAULT_DELAY_SECONDS = 3
MAX_MESSAGES_PER_HOUR = 30
MAX_MESSAGES_PER_DAY = 200

# Папки для хранения данных
SESSIONS_DIR = "sessions"
UPLOADS_DIR = "uploads"
LOGS_DIR = "logs"

# Создаем необходимые папки
for directory in [SESSIONS_DIR, UPLOADS_DIR, LOGS_DIR]:
    os.makedirs(directory, exist_ok=True)

# Ключ шифрования для сессий
if not ENCRYPTION_KEY:
    print("ENCRYPTION_KEY not found, generating new key")
    ENCRYPTION_KEY = Fernet.generate_key().decode()

    # Сохраняем новый ключ в .env файл
    env_file = os.path.join(os.path.dirname(__file__), '..', '.env')

    try:
        if os.path.exists(env_file):
            with open(env_file, 'r') as f:
                content = f.read()

            if 'ENCRYPTION_KEY=' in content:
                # Обновляем существующий ключ
                lines = content.split('\n')
                for i, line in enumerate(lines):
                    if line.startswith('ENCRYPTION_KEY='):
                        lines[i] = f'ENCRYPTION_KEY={ENCRYPTION_KEY}'
                        break
                content = '\n'.join(lines)
            else:
                # Добавляем новый ключ
                content += f'\nENCRYPTION_KEY={ENCRYPTION_KEY}\n'

            with open(env_file, 'w') as f:
                f.write(content)
        else:
            # Создаем .env файл
            with open(env_file, 'w') as f:
                f.write(f'ENCRYPTION_KEY={ENCRYPTION_KEY}\n')

        print(f"New encryption key saved to {env_file}")
    except Exception as e:
        print(f"Failed to save encryption key: {e}")

else:
    # Проверяем, что ключ правильной длины
    try:
        # Пробуем декодировать как base64
        key_bytes = Fernet(ENCRYPTION_KEY.encode())
        print("ENCRYPTION_KEY is valid")
    except Exception:
        print(f"Warning: Invalid ENCRYPTION_KEY, generating new key")
        # Генерируем новый правильный ключ
        ENCRYPTION_KEY = Fernet.generate_key().decode()
        print("Generated new 32-byte encryption key")
import os
from pathlib import Path

# Базовые настройки
BASE_DIR = Path(__file__).parent.parent
UPLOADS_DIR = BASE_DIR / "uploads"
STATIC_DIR = BASE_DIR / "static"
TEMPLATES_DIR = BASE_DIR / "templates"

# Создаем необходимые директории
UPLOADS_DIR.mkdir(exist_ok=True)
STATIC_DIR.mkdir(exist_ok=True)

# Настройки базы данных
DATABASE_URL = "sqlite:///./telegram_sender.db"

# Настройки безопасности
SECRET_KEY = os.getenv("SECRET_KEY", "your-secret-key-change-this")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin123")

# Telegram API настройки
TELEGRAM_API_ID = os.getenv("TELEGRAM_API_ID")
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH")
