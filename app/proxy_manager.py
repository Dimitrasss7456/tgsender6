
import os
import random
from typing import List, Optional, Dict
from sqlalchemy.orm import Session
from app.database import get_db
from app.config import UPLOADS_DIR

class ProxyManager:
    def __init__(self):
        self.proxies: List[str] = []
        self.used_proxies: Dict[str, str] = {}  # phone -> proxy mapping
        self.load_proxies()
    
    def load_proxies(self):
        """Загрузка прокси из файла"""
        proxy_file = os.path.join(UPLOADS_DIR, "proxies.txt")
        if os.path.exists(proxy_file):
            with open(proxy_file, 'r', encoding='utf-8') as f:
                self.proxies = [line.strip() for line in f if line.strip()]
            print(f"Загружено {len(self.proxies)} прокси")
        else:
            print("Файл с прокси не найден")
    
    def save_proxies(self, proxies_text: str):
        """Сохранение списка прокси в файл"""
        proxy_file = os.path.join(UPLOADS_DIR, "proxies.txt")
        with open(proxy_file, 'w', encoding='utf-8') as f:
            f.write(proxies_text)
        self.load_proxies()
    
    def get_proxy_for_phone(self, phone: str) -> Optional[str]:
        """Получение прокси для конкретного номера телефона"""
        # Если для этого номера уже есть назначенный прокси, возвращаем его
        if phone in self.used_proxies:
            return self.used_proxies[phone]
        
        # Если прокси нет, назначаем новый
        if not self.proxies:
            return None
        
        # Выбираем случайный прокси
        proxy = random.choice(self.proxies)
        self.used_proxies[phone] = proxy
        return proxy
    
    def get_available_proxies_count(self) -> int:
        """Получение количества доступных прокси"""
        return len(self.proxies)
    
    def get_used_proxies_count(self) -> int:
        """Получение количества используемых прокси"""
        return len(self.used_proxies)
    
    def clear_proxy_for_phone(self, phone: str):
        """Очистка прокси для номера телефона"""
        if phone in self.used_proxies:
            del self.used_proxies[phone]
    
    def get_all_proxies(self) -> List[str]:
        """Получение всех загруженных прокси"""
        return self.proxies.copy()

# Глобальный экземпляр менеджера прокси
proxy_manager = ProxyManager()
class ProxyManager:
    def __init__(self):
        self.proxies = {}
    
    def add_proxy(self, proxy_data: dict):
        """Добавление прокси"""
        pass
    
    def get_proxy_for_phone(self, phone: str):
        """Получение прокси для номера"""
        return None
    
    def clear_proxy_for_phone(self, phone: str):
        """Очистка прокси для номера"""
        pass

proxy_manager = ProxyManager()
