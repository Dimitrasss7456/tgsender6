
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
            try:
                with open(proxy_file, 'r', encoding='utf-8') as f:
                    self.proxies = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
                print(f"Загружено {len(self.proxies)} прокси")
            except Exception as e:
                print(f"Ошибка загрузки прокси: {e}")
                self.proxies = []
        else:
            print("Файл с прокси не найден, создаем пустой")
            self.proxies = []
    
    def save_proxies(self, proxies_text: str):
        """Сохранение списка прокси в файл"""
        try:
            # Создаем директорию если не существует
            os.makedirs(UPLOADS_DIR, exist_ok=True)
            
            proxy_file = os.path.join(UPLOADS_DIR, "proxies.txt")
            with open(proxy_file, 'w', encoding='utf-8') as f:
                f.write(proxies_text.strip())
            
            # Перезагружаем прокси после сохранения
            self.load_proxies()
            return True
        except Exception as e:
            print(f"Ошибка сохранения прокси: {e}")
            return False
    
    def get_proxy_for_phone(self, phone: str) -> Optional[str]:
        """Получение прокси для конкретного номера телефона"""
        # Если для этого номера уже есть назначенный прокси, возвращаем его
        if phone in self.used_proxies:
            assigned_proxy = self.used_proxies[phone]
            # Проверяем что прокси еще есть в списке доступных
            if assigned_proxy in self.proxies:
                return assigned_proxy
            else:
                # Прокси больше нет в списке, удаляем из назначенных
                del self.used_proxies[phone]
        
        # Если прокси нет или их список пуст, назначаем новый
        if not self.proxies:
            return None
        
        # Выбираем случайный прокси из доступных
        available_proxies = [p for p in self.proxies if p not in self.used_proxies.values()]
        
        if not available_proxies:
            # Если все прокси заняты, используем случайный (можно использовать один прокси для нескольких аккаунтов)
            proxy = random.choice(self.proxies)
        else:
            proxy = random.choice(available_proxies)
        
        self.used_proxies[phone] = proxy
        print(f"Назначен прокси {proxy} для номера {phone}")
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
            print(f"Прокси для номера {phone} очищен")
    
    def get_all_proxies(self) -> List[str]:
        """Получение всех загруженных прокси"""
        return self.proxies.copy()
    
    def add_proxy(self, proxy_data: dict):
        """Добавление одного прокси"""
        try:
            # Ожидаем что proxy_data содержит поле 'proxy' с URL прокси
            proxy_url = proxy_data.get('proxy', '').strip()
            if proxy_url and proxy_url not in self.proxies:
                self.proxies.append(proxy_url)
                # Сохраняем обновленный список в файл
                proxies_text = '\n'.join(self.proxies)
                return self.save_proxies(proxies_text)
            return False
        except Exception as e:
            print(f"Ошибка добавления прокси: {e}")
            return False
    
    def remove_proxy(self, proxy_id: int) -> bool:
        """Удаление прокси по индексу"""
        try:
            if 0 <= proxy_id < len(self.proxies):
                removed_proxy = self.proxies.pop(proxy_id)
                
                # Удаляем из назначенных прокси если он там есть
                phones_to_clear = [phone for phone, proxy in self.used_proxies.items() if proxy == removed_proxy]
                for phone in phones_to_clear:
                    del self.used_proxies[phone]
                
                # Сохраняем обновленный список
                proxies_text = '\n'.join(self.proxies)
                self.save_proxies(proxies_text)
                
                print(f"Прокси {removed_proxy} удален")
                return True
            return False
        except Exception as e:
            print(f"Ошибка удаления прокси: {e}")
            return False
    
    def validate_proxy_format(self, proxy_url: str) -> bool:
        """Проверка формата прокси"""
        try:
            # Базовая проверка что это похоже на URL прокси
            proxy_url = proxy_url.strip()
            if not proxy_url:
                return False
            
            # Проверяем что содержит протокол
            valid_protocols = ['http://', 'https://', 'socks4://', 'socks5://']
            has_protocol = any(proxy_url.startswith(protocol) for protocol in valid_protocols)
            
            if not has_protocol:
                return False
            
            # Проверяем что есть хост и порт
            parts = proxy_url.split('://')
            if len(parts) != 2:
                return False
            
            host_part = parts[1]
            if '@' in host_part:  # есть авторизация
                auth_part, host_part = host_part.split('@', 1)
            
            if ':' not in host_part:
                return False
                
            host, port = host_part.rsplit(':', 1)
            
            # Проверяем что порт - число
            try:
                port_num = int(port)
                if not (1 <= port_num <= 65535):
                    return False
            except ValueError:
                return False
                
            return True
            
        except Exception as e:
            print(f"Ошибка валидации прокси {proxy_url}: {e}")
            return False
    
    def get_proxy_stats(self) -> Dict:
        """Получение статистики по прокси"""
        return {
            "total_proxies": len(self.proxies),
            "used_proxies": len(self.used_proxies),
            "free_proxies": max(0, len(self.proxies) - len(set(self.used_proxies.values()))),
            "assignments": dict(self.used_proxies)
        }

# Глобальный экземпляр менеджера прокси
proxy_manager = ProxyManager()
