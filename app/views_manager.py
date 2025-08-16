
import asyncio
import os
import re
from typing import List, Dict, Optional
from pyrogram import Client
from pyrogram.errors import FloodWait
from app.database import Account, get_db
from app.config import API_ID, API_HASH, SESSIONS_DIR
from cryptography.fernet import Fernet

class ViewsManager:
    """Менеджер для накрутки просмотров постов"""
    
    def __init__(self):
        self.clients: Dict[int, Client] = {}
        # Получаем ключ шифрования из переменных окружения
        encryption_key = os.getenv('ENCRYPTION_KEY')
        if encryption_key:
            self.cipher = Fernet(encryption_key.encode())
        else:
            # Генерируем новый ключ если его нет
            key = Fernet.generate_key()
            self.cipher = Fernet(key)
            print(f"⚠️ Сгенерирован новый ключ шифрования для ViewsManager")
    
    def _parse_proxy(self, proxy_string: str) -> Dict:
        """Парсинг строки прокси"""
        if not proxy_string:
            return None

        parts = proxy_string.split("://")
        if len(parts) != 2:
            return None

        scheme = parts[0].lower()
        rest = parts[1]

        if "@" in rest:
            auth, address = rest.split("@", 1)
            username, password = auth.split(":", 1)
        else:
            username = password = None
            address = rest

        host, port = address.split(":", 1)

        return {
            "scheme": scheme,
            "hostname": host,
            "port": int(port),
            "username": username,
            "password": password
        }
    
    async def _get_client_for_account(self, account_id: int) -> Optional[Client]:
        """Получение клиента для аккаунта"""
        # Проверяем кеш клиентов
        if account_id in self.clients:
            client = self.clients[account_id]
            if hasattr(client, 'is_connected') and client.is_connected:
                return client
            else:
                del self.clients[account_id]

        # Получаем данные аккаунта из базы
        db = next(get_db())
        try:
            account = db.query(Account).filter(Account.id == account_id).first()
            if not account or not account.is_active:
                return None

            # Определяем путь к файлу сессии
            phone_clean = account.phone.replace('+', '').replace(' ', '').replace('(', '').replace(')', '').replace('-', '')
            session_file = os.path.join(SESSIONS_DIR, f"session_{phone_clean}")
            
            if not os.path.exists(f"{session_file}.session"):
                print(f"❌ Файл сессии не найден: {session_file}.session")
                return None

            # Создаем клиент
            client = Client(
                session_file,
                api_id=API_ID,
                api_hash=API_HASH,
                proxy=self._parse_proxy(account.proxy) if account.proxy else None,
                sleep_threshold=30,
                max_concurrent_transmissions=1,
                no_updates=True,
                workers=1
            )

            # Подключаемся
            try:
                await asyncio.wait_for(client.connect(), timeout=15)
                
                # Получаем информацию о пользователе
                try:
                    me = await asyncio.wait_for(client.get_me(), timeout=10)
                    client.me = me
                except (asyncio.TimeoutError, FloodWait):
                    # Создаем заглушку если не можем получить быстро
                    from types import SimpleNamespace
                    client.me = SimpleNamespace(
                        id=account_id,
                        first_name=account.name or "User",
                        is_premium=False,
                        is_verified=False,
                        is_bot=False
                    )

                self.clients[account_id] = client
                return client

            except Exception as e:
                print(f"❌ Ошибка подключения клиента {account_id}: {e}")
                try:
                    await client.disconnect()
                except:
                    pass
                return None

        except Exception as e:
            print(f"❌ Ошибка получения клиента {account_id}: {e}")
            return None
        finally:
            db.close()
    
    def _parse_post_url(self, post_url: str) -> Optional[Dict]:
        """Парсинг URL поста для получения chat_id и message_id"""
        try:
            # Поддерживаем различные форматы URL
            patterns = [
                r't\.me/([^/]+)/(\d+)',  # t.me/channel/123
                r'telegram\.me/([^/]+)/(\d+)',  # telegram.me/channel/123
                r'@([^/]+)/(\d+)',  # @channel/123
            ]
            
            for pattern in patterns:
                match = re.search(pattern, post_url)
                if match:
                    chat_username = match.group(1)
                    message_id = int(match.group(2))
                    
                    # Добавляем @ если его нет
                    if not chat_username.startswith('@'):
                        chat_username = f"@{chat_username}"
                    
                    return {
                        "chat_id": chat_username,
                        "message_id": message_id
                    }
            
            print(f"❌ Не удалось распарсить URL: {post_url}")
            return None
            
        except Exception as e:
            print(f"❌ Ошибка парсинга URL {post_url}: {e}")
            return None
    
    async def view_post(self, account_id: int, chat_id: str, message_id: int) -> Dict:
        """Просмотр поста (засчитывается как просмотр)"""
        try:
            print(f"👁️ Просматриваем пост {message_id} в чате {chat_id} аккаунтом {account_id}")
            
            client = await self._get_client_for_account(account_id)
            if not client:
                return {"status": "error", "message": "Не удалось получить клиент"}
            
            # Проверяем подключение
            if not client.is_connected:
                await client.connect()
            
            # Просматриваем пост - получение сообщения засчитывается как просмотр
            try:
                # Метод 1: Получаем сообщение по ID
                message = await client.get_messages(chat_id, message_id)
                
                if message:
                    print(f"✅ Пост просмотрен аккаунтом {account_id}")
                    
                    # Дополнительно отмечаем историю как прочитанную
                    try:
                        await client.read_chat_history(chat_id, max_id=message_id)
                        print(f"📖 История чата отмечена как прочитанная до сообщения {message_id}")
                    except Exception as read_error:
                        print(f"⚠️ Не удалось отметить историю как прочитанную: {read_error}")
                    
                    return {
                        "status": "success",
                        "message": f"Пост просмотрен аккаунтом {account_id}",
                        "post_id": message_id,
                        "views": getattr(message, 'views', 'N/A')
                    }
                else:
                    return {"status": "error", "message": "Сообщение не найдено"}
                    
            except Exception as view_error:
                error_msg = str(view_error)
                print(f"❌ Ошибка просмотра поста: {error_msg}")
                
                # Обрабатываем специфические ошибки
                if "CHANNEL_PRIVATE" in error_msg:
                    return {"status": "error", "message": "Канал приватный или недоступен"}
                elif "MSG_ID_INVALID" in error_msg:
                    return {"status": "error", "message": "Неверный ID сообщения"}
                elif "CHAT_ADMIN_REQUIRED" in error_msg:
                    return {"status": "error", "message": "Требуются права администратора"}
                elif "USER_BANNED_IN_CHANNEL" in error_msg:
                    return {"status": "error", "message": "Аккаунт заблокирован в канале"}
                elif "FLOOD_WAIT" in error_msg:
                    # Извлекаем время ожидания из ошибки
                    wait_time = 30  # по умолчанию
                    if "FLOOD_WAIT_" in error_msg:
                        try:
                            wait_time = int(error_msg.split("FLOOD_WAIT_")[1])
                        except:
                            pass
                    return {"status": "flood_wait", "wait_time": wait_time}
                else:
                    return {"status": "error", "message": f"Ошибка просмотра: {error_msg}"}
                    
        except Exception as e:
            print(f"❌ Общая ошибка просмотра поста: {e}")
            return {"status": "error", "message": f"Общая ошибка: {str(e)}"}
    
    async def boost_post_views(self, post_url: str, target_views: int, 
                             account_ids: List[int], delay_seconds: int = 10) -> Dict:
        """Накрутка просмотров поста"""
        try:
            print(f"🎬 Начинаем накрутку просмотров на пост {post_url}")
            print(f"🎯 Цель: {target_views} просмотров")
            print(f"👥 Используем {len(account_ids)} аккаунтов с задержкой {delay_seconds} секунд")
            
            # Парсим URL поста
            post_info = self._parse_post_url(post_url)
            if not post_info:
                return {"status": "error", "message": "Неверный формат URL поста"}
            
            chat_id = post_info["chat_id"]
            message_id = post_info["message_id"]
            
            print(f"📍 Канал: {chat_id}, ID сообщения: {message_id}")
            
            # Получаем активные аккаунты
            db = next(get_db())
            try:
                accounts = db.query(Account).filter(
                    Account.id.in_(account_ids),
                    Account.is_active == True
                ).all()
                
                if not accounts:
                    return {"status": "error", "message": "Активные аккаунты не найдены"}
                
                print(f"✅ Найдено {len(accounts)} активных аккаунтов")
                
            finally:
                db.close()
            
            # Результаты накрутки
            results = {
                "successful_views": 0,
                "failed_views": 0,
                "errors": [],
                "flood_waits": 0
            }
            
            # Выполняем накрутку
            views_completed = 0
            account_index = 0
            
            while views_completed < target_views and account_index < len(accounts) * 3:  # Максимум 3 прохода
                account = accounts[account_index % len(accounts)]
                
                print(f"👁️ Просмотр {views_completed + 1}/{target_views} от аккаунта {account.id} ({account.name})")
                
                try:
                    # Выполняем просмотр
                    result = await self.view_post(account.id, chat_id, message_id)
                    
                    if result["status"] == "success":
                        results["successful_views"] += 1
                        views_completed += 1
                        print(f"✅ Просмотр {views_completed} выполнен успешно")
                        
                    elif result["status"] == "flood_wait":
                        wait_time = result.get("wait_time", 30)
                        results["flood_waits"] += 1
                        print(f"⏰ FLOOD_WAIT {wait_time} секунд для аккаунта {account.id}")
                        
                        # Если flood wait слишком долгий, пропускаем аккаунт
                        if wait_time > 300:  # 5 минут
                            print(f"⏭️ Пропускаем аккаунт {account.id} (слишком долгое ожидание)")
                        else:
                            # Ждем и пытаемся снова
                            await asyncio.sleep(min(wait_time, 60))
                            continue
                            
                    else:
                        results["failed_views"] += 1
                        error_msg = result.get("message", "Неизвестная ошибка")
                        results["errors"].append(f"Аккаунт {account.id}: {error_msg}")
                        print(f"❌ Ошибка для аккаунта {account.id}: {error_msg}")
                    
                except Exception as e:
                    results["failed_views"] += 1
                    error_msg = f"Исключение для аккаунта {account.id}: {str(e)}"
                    results["errors"].append(error_msg)
                    print(f"❌ {error_msg}")
                
                # Переходим к следующему аккаунту
                account_index += 1
                
                # Задержка между просмотрами
                if views_completed < target_views and delay_seconds > 0:
                    await asyncio.sleep(delay_seconds)
            
            print(f"🎉 Накрутка просмотров завершена")
            print(f"📊 Успешно: {results['successful_views']}, Ошибок: {results['failed_views']}")
            
            return {
                "status": "success",
                "message": f"Накрутка завершена. Успешно: {results['successful_views']}, Ошибок: {results['failed_views']}",
                "results": results,
                "target_views": target_views,
                "completed_views": results["successful_views"]
            }
            
        except Exception as e:
            print(f"❌ Общая ошибка накрутки просмотров: {e}")
            return {"status": "error", "message": f"Общая ошибка: {str(e)}"}
    
    async def disconnect_client(self, account_id: int):
        """Отключение клиента"""
        if account_id in self.clients:
            try:
                client = self.clients[account_id]
                if hasattr(client, 'is_connected') and client.is_connected:
                    await client.disconnect()
                del self.clients[account_id]
                print(f"🔌 Клиент {account_id} отключен")
            except Exception as e:
                print(f"❌ Ошибка отключения клиента {account_id}: {e}")
    
    async def disconnect_all_clients(self):
        """Отключение всех клиентов"""
        for account_id in list(self.clients.keys()):
            await self.disconnect_client(account_id)
        print("🔌 Все клиенты отключены")

# Глобальный экземпляр менеджера просмотров
views_manager = ViewsManager()
