
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
        try:
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
                    print(f"❌ Аккаунт {account_id} не найден или неактивен")
                    return None

                # Определяем путь к файлу сессии
                phone_clean = account.phone.replace('+', '').replace(' ', '').replace('(', '').replace(')', '').replace('-', '')
                session_file = os.path.join(SESSIONS_DIR, f"session_{phone_clean}")
                
                if not os.path.exists(f"{session_file}.session"):
                    print(f"❌ Файл сессии не найден: {session_file}.session")
                    return None

                # Создаем клиент
                client = Client(
                    name=session_file,  # Используем name= вместо позиционного аргумента
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
                    print(f"✅ Клиент {account_id} подключен")
                    
                    # Получаем информацию о пользователе
                    try:
                        me = await asyncio.wait_for(client.get_me(), timeout=10)
                        client.me = me
                        print(f"✅ Получена информация о пользователе {me.first_name}")
                    except (asyncio.TimeoutError, FloodWait) as timeout_error:
                        print(f"⚠️ Таймаут получения информации о пользователе: {timeout_error}")
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

                except Exception as connect_error:
                    print(f"❌ Ошибка подключения клиента {account_id}: {connect_error}")
                    try:
                        await client.disconnect()
                    except:
                        pass
                    return None

            except Exception as db_error:
                print(f"❌ Ошибка работы с БД для аккаунта {account_id}: {db_error}")
                return None
            finally:
                db.close()
                
        except Exception as general_error:
            print(f"❌ Общая ошибка получения клиента {account_id}: {general_error}")
            return None
    
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
        """Просмотр поста с реальным увеличением счетчика просмотров"""
        try:
            print(f"👁️ Просматриваем пост {message_id} в чате {chat_id} аккаунтом {account_id}")
            
            client = await self._get_client_for_account(account_id)
            if not client:
                return {"status": "error", "message": "Не удалось получить клиент"}
            
            # Проверяем подключение
            if not hasattr(client, 'is_connected') or not client.is_connected:
                print(f"🔄 Переподключаем клиент {account_id}")
                try:
                    await client.connect()
                except Exception as reconnect_error:
                    print(f"❌ Ошибка переподключения: {reconnect_error}")
                    return {"status": "error", "message": f"Ошибка переподключения: {str(reconnect_error)}"}
            
            try:
                # Метод 1: Используем raw API для просмотра канала
                from pyrogram.raw import functions
                
                # Получаем peer для канала
                try:
                    peer = await client.resolve_peer(chat_id)
                    print(f"✅ Peer получен для {chat_id}")
                except Exception as peer_error:
                    print(f"❌ Ошибка получения peer: {peer_error}")
                    return {"status": "error", "message": f"Ошибка получения peer: {str(peer_error)}"}
                
                # Отмечаем просмотр конкретного сообщения через GetMessages
                try:
                    result = await client.invoke(
                        functions.channels.GetMessages(
                            channel=peer,
                            id=[message_id]
                        )
                    )
                except Exception as invoke_error:
                    print(f"❌ Ошибка вызова GetMessages: {invoke_error}")
                    # Пробуем fallback метод сразу
                    try:
                        message = await client.get_messages(chat_id, message_id)
                        if message:
                            await client.read_chat_history(chat_id, max_id=message_id)
                            print(f"✅ Fallback просмотр выполнен аккаунтом {account_id}")
                            return {
                                "status": "success",
                                "message": f"Пост просмотрен аккаунтом {account_id} (fallback)",
                                "post_id": message_id,
                                "views": getattr(message, 'views', 'N/A')
                            }
                        else:
                            return {"status": "error", "message": "Сообщение не найдено"}
                    except Exception as fallback_error:
                        print(f"❌ Fallback метод также не сработал: {fallback_error}")
                        return {"status": "error", "message": f"Ошибка просмотра: {str(fallback_error)}"}
                    
                    # Если дошли сюда, значит fallback не сработал
                    return {"status": "error", "message": f"Ошибка API: {str(invoke_error)}"}
                
                if result and result.messages:
                    message = result.messages[0]
                    print(f"✅ Сообщение получено через raw API аккаунтом {account_id}")
                    
                    # Дополнительно: отмечаем как прочитанное с помощью ReadHistory
                    try:
                        await client.invoke(
                            functions.messages.ReadHistory(
                                peer=peer,
                                max_id=message_id
                            )
                        )
                        print(f"📖 История отмечена как прочитанная до сообщения {message_id}")
                    except Exception as read_error:
                        print(f"⚠️ Ошибка отметки истории: {read_error}")
                    
                    # Дополнительно: используем GetHistory для имитации скроллинга
                    try:
                        await client.invoke(
                            functions.messages.GetHistory(
                                peer=peer,
                                offset_id=message_id,
                                offset_date=0,
                                add_offset=0,
                                limit=1,
                                max_id=0,
                                min_id=0,
                                hash=0
                            )
                        )
                        print(f"📜 История канала просмотрена")
                    except Exception as history_error:
                        print(f"⚠️ Ошибка получения истории: {history_error}")
                    
                    return {
                        "status": "success",
                        "message": f"Пост просмотрен аккаунтом {account_id}",
                        "post_id": message_id,
                        "views": getattr(message, 'views', 'N/A')
                    }
                else:
                    # Fallback: стандартный метод
                    print(f"🔄 Используем fallback метод для аккаунта {account_id}")
                    message = await client.get_messages(chat_id, message_id)
                    
                    if message:
                        await client.read_chat_history(chat_id, max_id=message_id)
                        print(f"✅ Fallback просмотр выполнен аккаунтом {account_id}")
                        
                        return {
                            "status": "success",
                            "message": f"Пост просмотрен аккаунтом {account_id} (fallback)",
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
    
    async def view_post_telethon(self, account_id: int, chat_id: str, message_id: int) -> Dict:
        """Альтернативный метод просмотра через Telethon"""
        try:
            print(f"📱 Telethon: Просматриваем пост {message_id} в {chat_id} аккаунтом {account_id}")
            
            # Получаем данные аккаунта
            db = next(get_db())
            try:
                from app.database import Account
                account = db.query(Account).filter(Account.id == account_id).first()
                if not account:
                    return {"status": "error", "message": "Аккаунт не найден"}
                
                # Импортируем Telethon
                try:
                    from telethon import TelegramClient
                    from telethon.tl.functions.messages import GetHistoryRequest
                    from telethon.tl.functions.channels import GetMessagesRequest
                    from telethon.tl.types import InputChannel
                except ImportError:
                    return {"status": "error", "message": "Telethon не установлен"}
                
                # Создаем временную сессию для Telethon
                import uuid
                from app.config import API_ID, API_HASH, SESSIONS_DIR
                import os
                
                phone_clean = account.phone.replace('+', '').replace(' ', '').replace('(', '').replace(')', '').replace('-', '')
                pyrogram_session_file = os.path.join(SESSIONS_DIR, f"session_{phone_clean}.session")
                
                if not os.path.exists(pyrogram_session_file):
                    return {"status": "error", "message": "Файл сессии не найден"}
                
                # Создаем уникальную сессию для Telethon
                unique_session_name = f"telethon_view_{uuid.uuid4().hex[:8]}"
                telethon_session_file = os.path.join(SESSIONS_DIR, unique_session_name)
                
                # Конвертируем сессию для Telethon
                await self._create_clean_telethon_session(pyrogram_session_file, telethon_session_file)
                
                # Создаем Telethon клиент
                telethon_client = TelegramClient(telethon_session_file, API_ID, API_HASH)
                
                try:
                    await telethon_client.start()
                    me = await telethon_client.get_me()
                    print(f"✅ Telethon: Авторизован как {me.first_name}")
                    
                    # Нормализуем chat_id
                    if chat_id.startswith('@'):
                        target_entity = chat_id
                    elif chat_id.isdigit() or (chat_id.startswith('-') and chat_id[1:].isdigit()):
                        target_entity = int(chat_id)
                    else:
                        target_entity = chat_id
                    
                    # Получаем сущность канала
                    entity = await telethon_client.get_entity(target_entity)
                    print(f"📍 Telethon: Получена сущность канала")
                    
                    # Метод 1: Получаем конкретное сообщение
                    try:
                        if hasattr(entity, 'access_hash'):  # Это канал
                            input_channel = InputChannel(entity.id, entity.access_hash)
                            result = await telethon_client(GetMessagesRequest(
                                channel=input_channel,
                                id=[message_id]
                            ))
                            
                            if result.messages:
                                print(f"✅ Telethon: Сообщение получено")
                                
                                # Дополнительно: получаем историю вокруг сообщения
                                await telethon_client(GetHistoryRequest(
                                    peer=entity,
                                    offset_id=message_id,
                                    offset_date=0,
                                    add_offset=0,
                                    limit=1,
                                    max_id=0,
                                    min_id=0,
                                    hash=0
                                ))
                                print(f"📜 Telethon: История просмотрена")
                                
                                return {
                                    "status": "success",
                                    "message": f"Telethon: Пост просмотрен аккаунтом {account_id}",
                                    "post_id": message_id
                                }
                        else:
                            # Для обычных чатов
                            message = await telethon_client.get_messages(entity, ids=message_id)
                            if message:
                                print(f"✅ Telethon: Сообщение в чате получено")
                                return {
                                    "status": "success",
                                    "message": f"Telethon: Сообщение в чате просмотрено аккаунтом {account_id}",
                                    "post_id": message_id
                                }
                            
                    except Exception as get_error:
                        print(f"❌ Telethon: Ошибка получения сообщения: {get_error}")
                        return {"status": "error", "message": f"Telethon: {str(get_error)}"}
                
                finally:
                    await telethon_client.disconnect()
                    
                    # Удаляем временную сессию
                    try:
                        session_file_path = f"{telethon_session_file}.session"
                        if os.path.exists(session_file_path):
                            os.remove(session_file_path)
                    except:
                        pass
                        
            finally:
                db.close()
                
        except Exception as e:
            print(f"❌ Telethon: Общая ошибка просмотра: {e}")
            return {"status": "error", "message": f"Telethon: {str(e)}"}
    
    async def _create_clean_telethon_session(self, pyrogram_path: str, telethon_path: str):
        """Создание чистой сессии Telethon из Pyrogram"""
        try:
            import sqlite3
            import os
            
            # Читаем auth_key из Pyrogram сессии
            conn = sqlite3.connect(pyrogram_path)
            cursor = conn.cursor()
            
            cursor.execute("SELECT dc_id, auth_key FROM sessions LIMIT 1")
            result = cursor.fetchone()
            conn.close()
            
            if not result:
                raise Exception("Не найдены данные авторизации в Pyrogram сессии")
            
            dc_id, auth_key = result
            
            # Создаем базу данных Telethon
            telethon_session_file = f"{telethon_path}.session"
            if os.path.exists(telethon_session_file):
                os.remove(telethon_session_file)
            
            conn = sqlite3.connect(telethon_session_file)
            cursor = conn.cursor()
            
            # Создаем минимальную структуру для Telethon
            cursor.execute("CREATE TABLE version (version INTEGER PRIMARY KEY)")
            cursor.execute("INSERT INTO version VALUES (1)")
            
            cursor.execute("""
                CREATE TABLE sessions (
                    dc_id INTEGER PRIMARY KEY,
                    server_address TEXT,
                    port INTEGER,
                    auth_key BLOB,
                    takeout_id INTEGER
                )
            """)
            
            # Определяем server_address по dc_id
            dc_servers = {
                1: "149.154.175.53",
                2: "149.154.167.51", 
                3: "149.154.175.100",
                4: "149.154.167.91",
                5: "91.108.56.130"
            }
            
            server_address = dc_servers.get(dc_id, "149.154.167.51")
            
            cursor.execute("""
                INSERT INTO sessions (dc_id, server_address, port, auth_key, takeout_id)
                VALUES (?, ?, ?, ?, NULL)
            """, (dc_id, server_address, 443, auth_key))
            
            cursor.execute("""
                CREATE TABLE entities (
                    id INTEGER PRIMARY KEY,
                    hash INTEGER NOT NULL,
                    username TEXT,
                    phone INTEGER,
                    name TEXT,
                    date INTEGER
                )
            """)
            
            cursor.execute("""
                CREATE TABLE sent_files (
                    md5_digest BLOB,
                    file_size INTEGER,
                    type INTEGER,
                    id INTEGER,
                    hash INTEGER,
                    PRIMARY KEY(md5_digest, file_size, type)
                )
            """)
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            print(f"❌ Ошибка создания Telethon сессии: {e}")
            raise e

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
                    # Выполняем просмотр (пробуем оба метода)
                    result = await self.view_post(account.id, chat_id, message_id)
                    
                    # Если Pyrogram не сработал, пробуем Telethon
                    if result["status"] == "error" and "не удалось" in result["message"].lower():
                        print(f"🔄 Пробуем Telethon для аккаунта {account.id}")
                        result = await self.view_post_telethon(account.id, chat_id, message_id)
                    
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
