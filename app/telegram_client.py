import asyncio
import os
import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from pyrogram import Client
from pyrogram.errors import FloodWait, SessionPasswordNeeded, PhoneCodeInvalid
from cryptography.fernet import Fernet
from sqlalchemy.orm import Session
from app.database import Account, Campaign, SendLog, get_db
from app.config import API_ID, API_HASH, SESSIONS_DIR, ENCRYPTION_KEY


class Progress:

    def __init__(self, filename, total_size):
        self.filename = filename
        self.total_size = total_size
        self.last_print = 0

    async def update(self, current, total):
        percent = (current / self.total_size) * 100
        if percent - self.last_print >= 5 or current == total:
            print(
                f"[{self.filename}] Отправлено: {current}/{self.total_size} bytes ({percent:.1f}%)"
            )
            self.last_print = percent


class TelegramManager:

    def __init__(self):
        self.clients: Dict[int, Client] = {}
        self.pending_clients: Dict[str, Client] = {}
        self.cipher = Fernet(ENCRYPTION_KEY)
        self._cleanup_temp_sessions()

    def _cleanup_temp_sessions(self):
        """Очистка временных файлов сессий"""
        try:
            if not os.path.exists(SESSIONS_DIR):
                return
            for filename in os.listdir(SESSIONS_DIR):
                if filename.startswith('temp_client_') and filename.endswith(
                        '.session'):
                    temp_path = os.path.join(SESSIONS_DIR, filename)
                    try:
                        os.remove(temp_path)
                    except:
                        pass
        except Exception as e:
            print(f"Error cleaning temp sessions: {e}")

    def encrypt_session(self, session_data: str) -> str:
        return self.cipher.encrypt(session_data.encode()).decode()

    def decrypt_session(self, encrypted_data: str) -> str:
        return self.cipher.decrypt(encrypted_data.encode()).decode()

    async def add_account(self,
                          phone: str,
                          proxy: Optional[str] = None) -> Dict:
        """Добавление нового аккаунта"""
        try:
            # Очищаем номер телефона
            clean_phone = phone.replace('+', '').replace(' ', '').replace(
                '(', '').replace(')', '').replace('-', '')
            session_name = f"session_{clean_phone}"
            session_path = os.path.join(SESSIONS_DIR, session_name)

            # Удаляем старую сессию если есть
            old_session_file = f"{session_path}.session"
            if os.path.exists(old_session_file):
                try:
                    os.remove(old_session_file)
                except:
                    pass

            client = Client(session_path,
                            api_id=API_ID,
                            api_hash=API_HASH,
                            phone_number=phone,
                            proxy=self._parse_proxy(proxy) if proxy else None,
                            sleep_threshold=30,
                            max_concurrent_transmissions=1,
                            no_updates=True)

            await client.connect()

            try:
                me = await client.get_me()
                await self._save_account(phone, session_path, me.first_name,
                                         proxy, me.id, None) # Передаем user_id
                await client.disconnect()
                return {"status": "success", "name": me.first_name}
            except:
                try:
                    # Отправляем код с задержкой
                    await asyncio.sleep(1)
                    sent_code = await client.send_code(phone)
                    self.pending_clients[session_name] = client

                    print(
                        f"Код отправлен на {phone}, hash: {sent_code.phone_code_hash}"
                    )

                    return {
                        "status": "code_required",
                        "phone_code_hash": sent_code.phone_code_hash,
                        "session_name": session_name
                    }
                except Exception as send_error:
                    await client.disconnect()
                    error_msg = str(send_error)
                    if "flood" in error_msg.lower():
                        return {
                            "status": "error",
                            "message":
                            "Слишком много попыток. Попробуйте позже"
                        }
                    return {
                        "status": "error",
                        "message": f"Ошибка отправки кода: {error_msg}"
                    }

        except Exception as e:
            return {"status": "error", "message": str(e)}

    async def verify_code(self,
                          phone: str,
                          code: str,
                          phone_code_hash: str,
                          session_name: str,
                          proxy: Optional[str] = None):
        """Подтверждение кода из SMS"""
        try:
            # Очищаем код от лишних символов и пробелов
            clean_code = ''.join(filter(str.isdigit, code.strip()))

            if len(clean_code) != 5:
                return {
                    "status": "error",
                    "message": "Код должен содержать ровно 5 цифр"
                }

            client = self.pending_clients.get(session_name)

            if not client:
                session_path = os.path.join(SESSIONS_DIR, session_name)
                client = Client(
                    session_path,
                    api_id=API_ID,
                    api_hash=API_HASH,
                    proxy=self._parse_proxy(proxy) if proxy else None,
                    no_updates=True,
                    takeout=False)
                await client.connect()

            # Дополнительная задержка перед попыткой входа
            await asyncio.sleep(1)

            try:
                await client.sign_in(phone, phone_code_hash, clean_code)
            except Exception as sign_in_error:
                # Если первая попытка не удалась, попробуем еще раз через несколько секунд
                await asyncio.sleep(3)
                await client.sign_in(phone, phone_code_hash, clean_code)

            me = await client.get_me()
            session_path = os.path.join(SESSIONS_DIR, session_name)
            await self._save_account(phone, session_path, me.first_name, proxy, me.id, None) # Передаем user_id

            await client.disconnect()

            if session_name in self.pending_clients:
                del self.pending_clients[session_name]

            return {"status": "success", "name": me.first_name}

        except Exception as e:
            error_msg = str(e).lower()
            print(f"Ошибка при верификации кода: {str(e)}")

            if "phone_code_invalid" in error_msg or "invalid code" in error_msg:
                return {
                    "status":
                    "error",
                    "message":
                    "Неверный код или код истёк. Попробуйте запросить новый код"
                }
            elif "phone_code_expired" in error_msg or "expired" in error_msg:
                return {
                    "status":
                    "error",
                    "message":
                    "Код истёк. Запросите новый код через форму добавления аккаунта"
                }
            elif "phone_code_empty" in error_msg or "empty" in error_msg:
                return {
                    "status": "error",
                    "message": "Код не может быть пустым"
                }
            elif "session_password_needed" in error_msg or "password" in error_msg or "2fa" in error_msg:
                return {
                    "status": "password_required",
                    "message": "Требуется пароль двухфакторной аутентификации",
                    "session_name": session_name
                }
            elif "flood" in error_msg:
                return {
                    "status": "error",
                    "message": "Слишком много попыток. Попробуйте позже"
                }
            else:
                return {
                    "status": "error",
                    "message":
                    f"Попробуйте запросить новый код. Детали: {str(e)}"
                }

    async def verify_password(self,
                              phone: str,
                              password: str,
                              session_name: str,
                              proxy: Optional[str] = None) -> Dict:
        """Подтверждение двухфакторной аутентификации"""
        try:
            client = self.pending_clients.get(session_name)

            if not client:
                session_path = os.path.join(SESSIONS_DIR, session_name)
                client = Client(
                    session_path,
                    api_id=API_ID,
                    api_hash=API_HASH,
                    proxy=self._parse_proxy(proxy) if proxy else None,
                    no_updates=True,
                    takeout=False)
                await client.connect()

            await client.check_password(password)
            me = await client.get_me()
            session_path = os.path.join(SESSIONS_DIR, session_name)
            await self._save_account(phone, session_path, me.first_name, proxy, me.id, None) # Передаем user_id
            await client.disconnect()

            if session_name in self.pending_clients:
                del self.pending_clients[session_name]

            return {"status": "success", "name": me.first_name}

        except Exception as e:
            if session_name in self.pending_clients:
                del self.pending_clients[session_name]
            return {"status": "error", "message": str(e)}

    async def _save_account(self, phone: str, session_path: str, name: str,
                            proxy: Optional[str], user_id: int, session_data: Optional[str]): # Добавлен user_id и session_data
        """Сохранение аккаунта в базу данных"""
        db = next(get_db())
        try:
            session_file_path = f"{session_path}.session"

            if not os.path.exists(session_file_path):
                raise Exception(f"Session file not found: {session_file_path}")

            if session_data is None: # Если session_data не переданы, читаем из файла
                with open(session_file_path, "rb") as f:
                    session_data = f.read()

            try:
                encrypted_session = self.cipher.encrypt(session_data.encode()).decode()
            except Exception:
                import base64
                encrypted_session = base64.b64encode(session_data).decode()

            existing_account = db.query(Account).filter(
                Account.phone == phone).first()
            if existing_account:
                existing_account.name = name
                existing_account.session_data = encrypted_session
                existing_account.proxy = proxy
                existing_account.status = "online"
                existing_account.is_active = True
                existing_account.user_id = user_id # Обновляем user_id
            else:
                account = Account(
                    phone=phone,
                    session_data=encrypted_session,
                    proxy=proxy,
                    status="awaiting_code",
                    user_id=user_id # Добавлен user_id
                )
                db.add(account)

            db.commit()

        except Exception as save_error:
            db.rollback()
            raise save_error
        finally:
            db.close()

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

    async def _get_client_for_account(self,
                                      account_id: int) -> Optional[Client]:
        """Получение или создание клиента для аккаунта с улучшенной диагностикой"""
        print(f"🔄 Запрос клиента для аккаунта {account_id}")

        # Проверяем существующий клиент
        if account_id in self.clients:
            client = self.clients[account_id]
            if hasattr(client, 'is_connected') and client.is_connected:
                print(
                    f"✅ Используем существующий подключенный клиент для аккаунта {account_id}"
                )
                return client
            else:
                print(
                    f"🔄 Клиент существует, но не подключен. Переподключаем...")
                try:
                    if hasattr(client, 'disconnect'):
                        await client.disconnect()
                except:
                    pass
                del self.clients[account_id]

        # Получаем данные аккаунта
        db = next(get_db())
        try:
            account = db.query(Account).filter(
                Account.id == account_id).first()
            if not account:
                print(f"❌ Аккаунт {account_id} не найден в базе данных")
                return None

            if not account.is_active:
                print(f"❌ Аккаунт {account_id} неактивен")
                return None

            print(f"✅ Найден аккаунт: {account.name} ({account.phone})")

            # Ищем файл сессии
            phone_clean = account.phone.replace('+', '').replace(
                ' ', '').replace('(', '').replace(')', '').replace('-', '')

            # Список возможных имен сессий
            possible_names = [
                f"session_{phone_clean}", f"session_{account.phone}",
                phone_clean
            ]

            session_file = None
            for name in possible_names:
                path = os.path.join(SESSIONS_DIR, f"{name}.session")
                if os.path.exists(path):
                    session_file = os.path.join(SESSIONS_DIR, name)
                    print(f"Найден файл сессии: {session_file}.session")
                    break

            if not session_file:
                print(
                    f"Файл сессии не найден для аккаунта {account_id}, проверенные пути:"
                )
                for name in possible_names:
                    print(f"  - {os.path.join(SESSIONS_DIR, name)}.session")
                return None

            # Создаем клиент
            client = Client(session_file,
                            api_id=API_ID,
                            api_hash=API_HASH,
                            proxy=self._parse_proxy(account.proxy)
                            if account.proxy else None,
                            sleep_threshold=30,
                            no_updates=True)

            # Проверяем подключение и авторизацию
            try:
                if not client.is_connected:
                    await client.connect()

                me = await client.get_me()
                print(
                    f"✓ Клиент для аккаунта {account_id} успешно подключен: {me.first_name}"
                )

                # Принудительно устанавливаем client.me для корректной работы Pyrogram
                client.me = me

                # Обновляем статус в БД
                account.status = "online"
                account.last_activity = datetime.utcnow()
                db.commit()

                self.clients[account_id] = client
                return client

            except Exception as auth_error:
                print(f"Ошибка авторизации клиента {account_id}: {auth_error}")
                try:
                    if client.is_connected:
                        await client.disconnect()
                except:
                    pass
                return None

        except Exception as e:
            print(
                f"Общая ошибка создания клиента для аккаунта {account_id}: {str(e)}"
            )
            return None
        finally:
            db.close()

    async def get_user_contacts(self, account_id: int) -> dict:
        """Безопасное получение контактов пользователя"""
        import traceback
        try:
            print(f"Получение контактов для аккаунта {account_id}")
            client = await self._get_client_for_account(account_id)
            if not client:
                return {
                    "status": "error",
                    "message": "Не удалось подключиться к аккаунту"
                }
            if not client.is_connected:
                await client.connect()
            contacts_list = []
            try:
                contacts = await client.get_contacts()
            except Exception as e:
                print(f"Ошибка получения списка контактов: {e}")
                print(traceback.format_exc())
                return {
                    "status": "error",
                    "message": f"Ошибка получения контактов: {str(e)}"
                }
            for contact in contacts:
                if contact is None:
                    continue
                try:
                    first_name = getattr(contact, "first_name", "") or ""
                    last_name = getattr(contact, "last_name", "") or ""
                    username = getattr(contact, "username", "") or ""
                    contact_id = getattr(contact, "id", None)
                    contact_data = {
                        "id":
                        contact_id,
                        "first_name":
                        first_name,
                        "last_name":
                        last_name,
                        "username":
                        username,
                        "phone":
                        getattr(contact, "phone_number", "") or "",
                        "is_bot":
                        bool(getattr(contact, "is_bot", False)),
                        "is_verified":
                        bool(getattr(contact, "is_verified", False)),
                        "is_premium":
                        bool(getattr(contact, "is_premium", False)),
                        "display_name":
                        f"{first_name} {last_name}".strip() or username
                        or f"User {contact_id}"
                    }
                    contacts_list.append(contact_data)
                except Exception as ce:
                    print(f"Ошибка обработки контакта: {ce}")
                    print(traceback.format_exc())
                    continue
            print(f"Найдено {len(contacts_list)} контактов")
            return {
                "status": "success",
                "contacts": contacts_list,
                "count": len(contacts_list)
            }
        except Exception as e:
            import traceback
            print(f"Общая ошибка при получении контактов: {e}")
            print(traceback.format_exc())
            return {
                "status": "error",
                "message": f"Не удалось получить контакты: {str(e)}"
            }

    async def get_user_dialogs(self, account_id: int) -> Dict:
        """Получение контактов из диалогов (старый метод)"""
        try:
            print(f"=== Получение диалогов для аккаунта {account_id} ===")

            client = await self._get_client_for_account(account_id)
            if not client:
                return {
                    "status": "error",
                    "message": "Не удалось подключиться к аккаунту"
                }

            contacts = []

            try:
                # Получаем информацию о себе
                me = await client.get_me()
                print(f"Получаем диалоги для: {me.first_name}")

                # Получаем диалоги с таймаутом
                dialog_count = 0
                async for dialog in client.get_dialogs(limit=50):
                    dialog_count += 1
                    chat = dialog.chat

                    # Пропускаем системные чаты и самого себя
                    if chat.id == me.id or chat.id == 777000:
                        continue

                    # Обрабатываем только приватные чаты
                    if hasattr(chat, 'type') and 'PRIVATE' in str(chat.type):
                        # Получаем данные контакта
                        first_name = getattr(chat, 'first_name', '') or ''
                        last_name = getattr(chat, 'last_name', '') or ''
                        username = getattr(chat, 'username', '') or ''

                        # Формируем имя для отображения
                        display_name = f"{first_name} {last_name}".strip()
                        if not display_name and username:
                            display_name = f"@{username}"
                        elif not display_name:
                            display_name = f"Пользователь {chat.id}"

                        contact_info = {
                            "id": chat.id,
                            "first_name": first_name,
                            "last_name": last_name,
                            "username": username,
                            "display_name": display_name
                        }

                        contacts.append(contact_info)
                        print(f"✓ Контакт: {display_name}")

                    # Ограничиваем количество для быстрой загрузки
                    if dialog_count >= 30:
                        break

                print(
                    f"✓ Найдено {len(contacts)} контактов из {dialog_count} диалогов"
                )

                # Закрываем клиент
                await client.disconnect()

                return {
                    "status": "success",
                    "contacts": contacts,
                    "total": len(contacts)
                }

            except Exception as e:
                print(f"Ошибка получения диалогов: {str(e)}")
                await client.disconnect()
                return {
                    "status": "error",
                    "message": f"Ошибка получения диалогов: {str(e)}"
                }

        except Exception as e:
            print(f"Общая ошибка получения контактов: {str(e)}")
            return {"status": "error", "message": str(e)}

    async def get_user_chats(self, account_id: int) -> Dict:
        """Получение чатов и каналов"""
        try:
            print(f"=== Получение чатов для аккаунта {account_id} ===")

            client = await self._get_client_for_account(account_id)
            if not client:
                return {
                    "status": "error",
                    "message": "Не удалось подключиться к аккаунту"
                }

            chats = {"groups": [], "channels": [], "private": []}

            try:
                dialog_count = 0
                async for dialog in client.get_dialogs(limit=30):
                    dialog_count += 1
                    chat = dialog.chat

                    if hasattr(chat, 'type'):
                        chat_type = str(chat.type)

                        # Получаем название
                        if hasattr(chat, 'title'):
                            title = chat.title
                        else:
                            first_name = getattr(chat, 'first_name', '') or ''
                            last_name = getattr(chat, 'last_name', '') or ''
                            title = f"{first_name} {last_name}".strip(
                            ) or f"Chat {chat.id}"

                        chat_data = {
                            "id": chat.id,
                            "title": title,
                            "username": getattr(chat, 'username', '') or ''
                        }

                        # Распределяем по типам
                        if 'PRIVATE' in chat_type:
                            chats["private"].append(chat_data)
                        elif 'GROUP' in chat_type:
                            chats["groups"].append(chat_data)
                        elif 'CHANNEL' in chat_type:
                            chats["channels"].append(chat_data)

                print(
                    f"✓ Найдено: {len(chats['private'])} приватных, {len(chats['groups'])} групп, {len(chats['channels'])} каналов"
                )

                # Закрываем клиент
                await client.disconnect()

                return {"status": "success", "chats": chats}

            except Exception as e:
                print(f"Ошибка получения чатов: {str(e)}")
                await client.disconnect()
                return {"status": "error", "message": str(e)}

        except Exception as e:
            print(f"Общая ошибка получения чатов: {str(e)}")
            return {"status": "error", "message": str(e)}

    async def cleanup_client(self, account_id: int):
        """Очистка клиента"""
        if account_id in self.clients:
            client = self.clients[account_id]
            try:
                await client.stop()
            except:
                pass
            del self.clients[account_id]

    async def disconnect_client(self, account_id: int) -> bool:
        """Отключение клиента"""
        try:
            if account_id in self.clients:
                client = self.clients[account_id]
                try:
                    if hasattr(client, 'is_connected') and client.is_connected:
                        await client.disconnect()
                except Exception as disconnect_error:
                    print(
                        f"Error during disconnect for client {account_id}: {disconnect_error}"
                    )
                    # Продолжаем удаление из словаря даже если disconnect не удался

                del self.clients[account_id]
                return True
        except Exception as e:
            print(f"Error disconnecting client {account_id}: {e}")
        return False

    async def send_file(
            self,
            account_id: int,
            chat_id: str,
            file_path: str,
            caption: str = "",
            chunk_size: int = 10 * 1024 * 1024  # 10MB chunks
    ) -> Dict:
        """Улучшенная отправка файлов с детальной диагностикой"""
        try:
            print(f"🔄 Начинаем отправку файла: {file_path}")
            print(f"📋 Аккаунт ID: {account_id}, Получатель: {chat_id}")

            # Проверяем существование файла
            if not os.path.exists(file_path):
                print(f"❌ Файл не найден: {file_path}")
                return {"status": "error", "message": "Файл не найден"}

            file_size = os.path.getsize(file_path)
            file_name = os.path.basename(file_path)
            print(f"📁 Файл: {file_name}, размер: {file_size} байт")

            # Получаем клиент с детальной диагностикой
            print(f"🔌 Получаем клиент для аккаунта {account_id}...")
            client = await self._get_client_for_account(account_id)

            if not client:
                print(f"❌ Клиент не найден для аккаунта {account_id}")
                return {
                    "status": "error",
                    "message": "Не удалось подключиться к аккаунту"
                }

            print(f"✅ Клиент получен успешно")

            # Проверяем подключение клиента
            if not client.is_connected:
                print("🔄 Подключаем клиент...")
                await client.connect()
                print("✅ Клиент подключен")

            # Проверяем авторизацию клиента и обновляем информацию о пользователе
            try:
                me = await client.get_me()
                if not me:
                    print("❌ Не удалось получить информацию о пользователе")
                    return {
                        "status": "error",
                        "message": "Ошибка авторизации аккаунта"
                    }

                # Принудительно устанавливаем client.me для корректной работы Pyrogram
                client.me = me
                print(f"✅ Авторизован как: {me.first_name} ({me.id})")
            except Exception as auth_error:
                print(f"❌ Ошибка авторизации: {auth_error}")
                return {
                    "status": "error",
                    "message": f"Ошибка авторизации: {str(auth_error)}"
                }

            # Нормализуем chat_id
            target_chat_id = chat_id
            if isinstance(chat_id, str):
                if chat_id.startswith('@'):
                    target_chat_id = chat_id
                elif chat_id.isdigit() or (chat_id.startswith('-')
                                           and chat_id[1:].isdigit()):
                    target_chat_id = int(chat_id)

            print(f"🎯 Целевой чат: {target_chat_id}")

            # Отправляем файл с подробным логированием
            try:
                print(f"📤 Отправляем файл...")

                # Для больших файлов используем специальную обработку
                if file_size > 2 * 1024 * 1024 * 1024:  # 2GB
                    print("📦 Большой файл, используем специальный метод")
                    return await self._send_large_file_improved(
                        client, target_chat_id, file_path, caption)

                # Стандартная отправка
                sent_msg = await client.send_document(
                    chat_id=target_chat_id,
                    document=file_path,
                    caption=caption if caption else "",
                    force_document=True,
                    disable_notification=False)

                if sent_msg and hasattr(sent_msg, 'id'):
                    print(
                        f"✅ Файл отправлен успешно! Message ID: {sent_msg.id}")
                    return {
                        "status": "success",
                        "message_id": sent_msg.id,
                        "file_name": file_name,
                        "file_size": file_size
                    }
                else:
                    print("❌ Файл отправлен, но не получен ID сообщения")
                    return {
                        "status": "error",
                        "message": "Файл отправлен, но не получен ID сообщения"
                    }

            except Exception as send_error:
                error_msg = str(send_error)
                print(f"❌ Ошибка при отправке файла: {error_msg}")

                # Обработка специфических ошибок
                if "PEER_ID_INVALID" in error_msg:
                    return {
                        "status": "error",
                        "message": f"Получатель {chat_id} не найден"
                    }
                elif "FILE_PARTS_INVALID" in error_msg:
                    return {
                        "status": "error",
                        "message": "Ошибка загрузки файла на серверы Telegram"
                    }
                elif "DOCUMENT_INVALID" in error_msg:
                    return {
                        "status": "error",
                        "message": "Недопустимый формат файла"
                    }
                elif "FLOOD_WAIT" in error_msg:
                    return {
                        "status": "error",
                        "message": "Превышен лимит отправки. Попробуйте позже"
                    }
                else:
                    return {
                        "status": "error",
                        "message": f"Ошибка отправки: {error_msg}"
                    }

        except Exception as general_error:
            error_msg = str(general_error)
            print(f"❌ Общая ошибка send_file: {error_msg}")
            return {"status": "error", "message": f"Общая ошибка: {error_msg}"}

    async def send_message(self,
                           account_id: int,
                           recipient: str,
                           message: str,
                           file_path: str = None,
                           schedule_seconds: int = 0) -> dict:
        """Отправка сообщения/файла с полным выводом ошибок Telegram"""
        import os, io, traceback, mimetypes, tempfile, shutil
        from pyrogram.errors import RPCError, AuthKeyUnregistered
        try:
            client = await self._get_client_for_account(account_id)
            if not client:
                return {"status": "error", "message": "Клиент не найден"}
            if not client.is_connected:
                await client.connect()

            # Проверяем авторизацию с обработкой ошибки AUTH_KEY_UNREGISTERED
            try:
                me = await client.get_me()
                if not me:
                    return {
                        "status": "error",
                        "message": "Ошибка авторизации аккаунта"
                    }
            except AuthKeyUnregistered:
                # Удаляем проблемную сессию и просим пользователя войти заново
                await self._handle_auth_key_unregistered(account_id)
                return {
                    "status": "error", 
                    "message": "Сессия аккаунта недействительна. Необходимо войти заново"
                }
            # Нормализация получателя
            if not recipient.startswith('@') and not recipient.startswith(
                    '+') and not recipient.isdigit(
                    ) and not recipient.startswith('-'):
                recipient = f"@{recipient}"
            target_id = recipient if not recipient.isdigit() else int(
                recipient)
            schedule_date = None
            if schedule_seconds > 0:
                from datetime import datetime, timedelta
                schedule_date = datetime.utcnow() + timedelta(
                    seconds=max(30, schedule_seconds))

            def prepare_apk_file(path):
                if not path.lower().endswith(".apk"):
                    return path, None
                tmp_dir = tempfile.mkdtemp()
                tmp_path = os.path.join(tmp_dir,
                                        os.path.basename(path) + ".zip")

                # Копируем оригинальный файл
                shutil.copy(path, tmp_path)

                # Добавляем уникальные данные для каждого контакта
                import random, time, uuid
                from datetime import datetime

                # Генерируем максимально уникальные данные
                timestamp = str(int(time.time() * 1000000))  # Микросекунды
                unique_id = str(uuid.uuid4())
                random_symbols = ['.', ',', ';', ':', '!', '?', '-', '_', '=', '+', '#', '@', '$', '%']
                random_data = ''.join(random.choices(random_symbols, k=random.randint(15, 30)))
                recipient_hash = str(hash(recipient + timestamp))

                # Добавляем максимальное разнообразие в конец файла
                with open(tmp_path, 'ab') as f:
                    # Комментарий с уникальными данными
                    unique_comment = f"\n# Unique data for {recipient}\n"
                    unique_comment += f"# Timestamp: {timestamp}\n"
                    unique_comment += f"# UUID: {unique_id}\n"
                    unique_comment += f"# Random: {random_data}\n"
                    unique_comment += f"# Hash: {recipient_hash}\n"
                    unique_comment += f"# DateTime: {datetime.now().isoformat()}\n"
                    f.write(unique_comment.encode('utf-8'))

                    # Случайные байты разной длины
                    f.write(bytes([random.randint(0, 255) for _ in range(random.randint(50, 150))]))

                    # Дополнительная строка с случайными данными
                    f.write(f"\n{random_data * random.randint(2, 5)}\n".encode('utf-8'))

                print(f"📝 Создан уникальный APK для {recipient} с ID: {unique_id[:8]}")
                return tmp_path, tmp_dir

            if file_path and os.path.exists(file_path):
                send_path, tmp_dir = prepare_apk_file(file_path)

                # Сохраняем оригинальное имя файла
                original_filename = os.path.basename(file_path)

                # Создаем BytesIO объект с правильным именем
                file_data = open(send_path, "rb").read()
                bytes_io = io.BytesIO(file_data)
                bytes_io.name = original_filename

                attempts = [
                    ("path", send_path),
                    ("bytesIO", bytes_io)
                ]

                last_error = None
                for label, doc in attempts:
                    try:
                        print(f"🔄 Попытка отправки файла через {label} ...")

                        # Параметры для отправки
                        send_params = {
                            "chat_id": target_id,
                            "document": doc,
                            "caption": message or "",
                            "force_document": True,
                            "file_name": original_filename
                        }

                        # Добавляем schedule_date только если задан
                        if schedule_date:
                            send_params["schedule_date"] = schedule_date

                        sent = await client.send_document(**send_params)
                        if tmp_dir:
                            shutil.rmtree(tmp_dir, ignore_errors=True)
                        return {
                            "status": "success",
                            "message_id": getattr(sent, "id", None)
                        }
                    except RPCError as rpc_err:
                        print(
                            f"❌ RPCError при {label}: {rpc_err} (код: {getattr(rpc_err, 'code', None)})"
                        )
                        print(traceback.format_exc())
                        last_error = f"RPCError: {rpc_err}"
                    except Exception as e:
                        print(f"❌ Ошибка при {label}: {e}")
                        print(traceback.format_exc())
                        last_error = str(e)

                if tmp_dir:
                    shutil.rmtree(tmp_dir, ignore_errors=True)
                return {
                    "status": "error",
                    "message": f"Не удалось отправить файл: {last_error}"
                }
            else:
                try:
                    sent = await client.send_message(
                        chat_id=target_id,
                        text=message or "",
                        schedule_date=schedule_date)
                    return {
                        "status": "success",
                        "message_id": getattr(sent, "id", None)
                    }
                except RPCError as rpc_err:
                    print(
                        f"❌ RPCError при отправке текста: {rpc_err} (код: {getattr(rpc_err, 'code', None)})"
                    )
                    print(traceback.format_exc())
                    return {
                        "status": "error",
                        "message": f"RPCError: {rpc_err}"
                    }
                except Exception as e4:
                    print(f"Ошибка при отправке текста: {e4}")
                    print(traceback.format_exc())
                    return {"status": "error", "message": str(e4)}
        except AuthKeyUnregistered:
            await self._handle_auth_key_unregistered(account_id)
            return {
                "status": "error", 
                "message": "Сессия аккаунта недействительна. Необходимо войти заново"
            }
        except Exception as e:
            print(f"Общая ошибка send_message: {e}")
            print(traceback.format_exc())
            return {"status": "error", "message": str(e)}

    async def _send_text_only(self,
                              client,
                              target_id,
                              text: str,
                              schedule_date=None):
        """Отправка только текстового сообщения"""
        try:
            kwargs = {"chat_id": target_id, "text": text}
            if schedule_date:
                kwargs["schedule_date"] = schedule_date

            sent_message = await client.send_message(**kwargs)
            print(f"✓ Текст отправлен: {text[:50]}...")
            return sent_message

        except Exception as e:
            print(f"❌ Ошибка отправки текста: {e}")
            raise e

    async def _send_large_file_improved(self, client, chat_id, file_path: str,
                                        caption: str):
        """Улучшенная отправка больших файлов"""
        try:
            print(f"📦 Отправляем большой файл: {file_path}")

            # Для файлов больше 2GB используем упрощенные настройки
            sent_msg = await client.send_document(
                chat_id=chat_id,
                document=file_path,
                caption=caption if caption else "",
                force_document=True,
                thumb=None,  # Отключаем превью
                disable_notification=False)

            if sent_msg and hasattr(sent_msg, 'id'):
                print(f"✅ Большой файл отправлен успешно! ID: {sent_msg.id}")
                return {
                    "status": "success",
                    "message_id": sent_msg.id,
                    "file_name": os.path.basename(file_path),
                    "file_size": os.path.getsize(file_path)
                }
            else:
                return {
                    "status": "error",
                    "message": "Файл отправлен, но не получен ID сообщения"
                }

        except Exception as e:
            error_msg = str(e)
            print(f"❌ Ошибка отправки большого файла: {error_msg}")
            return {
                "status": "error",
                "message": f"Ошибка отправки большого файла: {error_msg}"
            }

    async def _update_account_stats(self, account_id: int):
        """Обновление статистики аккаунта"""
        db = next(get_db())
        try:
            account = db.query(Account).filter(
                Account.id == account_id).first()
            if account:
                now = datetime.utcnow()
                # Ограничение по времени между сообщениями
                if account.last_message_time and (
                        now - account.last_message_time).total_seconds() < 1:
                    await asyncio.sleep(
                        1 - (now - account.last_message_time).total_seconds())

                account.messages_sent_today += 1
                account.messages_sent_hour += 1
                account.last_message_time = datetime.utcnow()
                db.commit()
        except Exception as e:
            db.rollback()
            print(f"Ошибка обновления статистики аккаунта {account_id}: {e}")
        finally:
            db.close()

    async def _handle_auth_key_unregistered(self, account_id: int):
        """Обработка ошибки AUTH_KEY_UNREGISTERED"""
        try:
            print(f"🔧 Обрабатываем недействительную сессию для аккаунта {account_id}")

            # Отключаем и удаляем клиент
            await self.disconnect_client(account_id)

            # Обновляем статус в базе данных
            db = next(get_db())
            try:
                account = db.query(Account).filter(Account.id == account_id).first()
                if account:
                    account.status = "error"
                    account.is_active = False
                    db.commit()
                    print(f"🔄 Аккаунт {account_id} деактивирован в базе данных")
            finally:
                db.close()

        except Exception as e:
            print(f"Ошибка при обработке недействительной сессии: {e}")

    async def get_client(self, account_id: int) -> Optional[Client]:
        """Вспомогательная функция для получения клиента (переименована для соответствия изменениям)"""
        return await self._get_client_for_account(account_id)


# Глобальный экземпляр менеджера
telegram_manager = TelegramManager()
import asyncio
from typing import Dict, Optional
from telethon import TelegramClient
from app.database import Account

class TelegramManager:
    def __init__(self):
        self.clients: Dict[int, TelegramClient] = {}

    async def get_client(self, account: Account) -> Optional[TelegramClient]:
        """Получение клиента для аккаунта"""
        if account.id not in self.clients:
            try:
                client = TelegramClient(
                    f"session_{account.phone}",
                    int(account.api_id),
                    account.api_hash
                )
                await client.start(phone=account.phone)
                self.clients[account.id] = client
                return client
            except Exception as e:
                print(f"Error creating client for {account.phone}: {e}")
                return None
        return self.clients[account.id]

    async def disconnect_all(self):
        """Отключение всех клиентов"""
        for client in self.clients.values():
            await client.disconnect()
        self.clients.clear()

telegram_manager = TelegramManager()