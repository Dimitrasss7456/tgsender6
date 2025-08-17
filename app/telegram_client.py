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
                          proxy: Optional[str] = None,
                          current_user_id: Optional[int] = None) -> Dict: # Добавлен current_user_id
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

            # Автоматически назначаем прокси если не указан
            if not proxy:
                from app.proxy_manager import proxy_manager
                auto_proxy = proxy_manager.get_proxy_for_phone(phone)
                if auto_proxy:
                    proxy = auto_proxy
                    print(f"🔗 Автоматически назначен прокси {proxy} для {phone}")

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
                                         proxy, me.id, None, current_user_id) # Передаем user_id и current_user_id
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
                          proxy: Optional[str] = None,
                          current_user_id: Optional[int] = None): # Добавлен current_user_id
        """Подтверждение кода из SMS"""
        try:
            # Очищаем код от лишних символов и пробелов
            clean_code = ''.join(filter(str.isdigit, code.strip()))

            if len(clean_code) != 5:
                return {
                    "status":
                    "error",
                    "message": "Код должен содержать ровно 5 цифр"
                }

            client = self.pending_clients.get(session_name)

            if not client:
                # Автоматически получаем прокси если не указан
                if not proxy:
                    from app.proxy_manager import proxy_manager
                    auto_proxy = proxy_manager.get_proxy_for_phone(phone)
                    if auto_proxy:
                        proxy = auto_proxy
                        print(f"🔗 Используем назначенный прокси {proxy} для верификации {phone}")

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
            await self._save_account(phone, session_path, me.first_name, proxy, me.id, None, current_user_id) # Передаем user_id и current_user_id

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
                return {"status": "error", "message": "Код не может быть пустым"}
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
                              proxy: Optional[str] = None,
                              current_user_id: Optional[int] = None) -> Dict: # Добавлен current_user_id
        """Подтверждение двухфакторной аутентификации"""
        try:
            client = self.pending_clients.get(session_name)

            if not client:
                # Автоматически получаем прокси если не указан
                if not proxy:
                    from app.proxy_manager import proxy_manager
                    auto_proxy = proxy_manager.get_proxy_for_phone(phone)
                    if auto_proxy:
                        proxy = auto_proxy
                        print(f"🔗 Используем назначенный прокси {proxy} для 2FA {phone}")

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
            await self._save_account(phone, session_path, me.first_name, proxy, me.id, None, current_user_id) # Передаем user_id и current_user_id
            await client.disconnect()

            if session_name in self.pending_clients:
                del self.pending_clients[session_name]

            return {"status": "success", "name": me.first_name}

        except Exception as e:
            if session_name in self.pending_clients:
                del self.pending_clients[session_name]
            return {"status": "error", "message": str(e)}

    async def add_account_from_session(self,
                                     session_file_path: str,
                                     proxy: Optional[str] = None,
                                     current_user_id: Optional[int] = None) -> Dict:
        """Добавление аккаунта из .session файла Pyrogram"""
        import shutil
        import uuid
        
        try:
            print(f"🔄 Импорт аккаунта из .session файла: {session_file_path}")
            
            # Проверяем что файл существует и это .session файл
            if not os.path.exists(session_file_path):
                return {"status": "error", "message": "Файл сессии не найден"}
            
            if not session_file_path.endswith('.session'):
                return {"status": "error", "message": "Неверный формат файла. Требуется .session файл"}
            
            # Проверяем что файл не пустой
            file_size = os.path.getsize(session_file_path)
            if file_size == 0:
                return {"status": "error", "message": "Файл сессии пустой"}
            
            print(f"📁 Размер файла сессии: {file_size} байт")
            
            # Создаем временное имя для тестирования
            temp_session_name = f"temp_session_{uuid.uuid4().hex[:8]}"
            temp_session_path = os.path.join(SESSIONS_DIR, temp_session_name)
            
            try:
                # Копируем файл во временное место
                shutil.copy2(session_file_path, f"{temp_session_path}.session")
                print(f"📋 Файл скопирован во временное место")
                
                # Автоматически получаем прокси если нужно
                if not proxy:
                    from app.proxy_manager import proxy_manager
                    auto_proxy = proxy_manager.get_proxy_for_phone("session_import")
                    if auto_proxy:
                        proxy = auto_proxy
                        print(f"🔗 Автоматически назначен прокси {proxy}")
                
                # Создаем тестовый клиент для проверки сессии
                test_client = Client(
                    temp_session_path,
                    api_id=API_ID,
                    api_hash=API_HASH,
                    proxy=self._parse_proxy(proxy) if proxy else None,
                    no_updates=True
                )
                
                print(f"🔌 Подключаемся к Telegram для проверки сессии...")
                await test_client.connect()
                
                # Получаем информацию о пользователе
                me = await test_client.get_me()
                
                if not me or not me.id:
                    await test_client.disconnect()
                    return {"status": "error", "message": "Не удалось получить информацию о пользователе из сессии"}
                
                print(f"✅ Сессия валидна: {me.first_name} ({me.phone_number})")
                
                # Создаем постоянную сессию
                phone_clean = me.phone_number.replace('+', '').replace(' ', '').replace('(', '').replace(')', '').replace('-', '')
                permanent_session_name = f"session_{phone_clean}"
                permanent_session_path = os.path.join(SESSIONS_DIR, permanent_session_name)
                
                await test_client.disconnect()
                
                # Копируем файл в постоянное место
                permanent_session_file = f"{permanent_session_path}.session"
                shutil.copy2(f"{temp_session_path}.session", permanent_session_file)
                print(f"✅ Сессия сохранена: {permanent_session_file}")
                
                # Сохраняем аккаунт в базу данных
                await self._save_account(
                    phone=me.phone_number,
                    session_path=permanent_session_path,
                    name=me.first_name or "Session User",
                    proxy=proxy,
                    user_id=me.id,
                    session_data=None,  # Будет считан из файла
                    current_user_id=current_user_id
                )
                
                return {
                    "status": "success",
                    "name": me.first_name or "Session User",
                    "phone": me.phone_number
                }
                
            except Exception as e:
                error_msg = str(e)
                print(f"❌ Ошибка обработки сессии: {error_msg}")
                
                if "AUTH_KEY_UNREGISTERED" in error_msg:
                    return {"status": "error", "message": "Сессия недействительна или устарела"}
                elif "SESSION_PASSWORD_NEEDED" in error_msg:
                    return {"status": "error", "message": "Требуется пароль двухфакторной аутентификации"}
                else:
                    return {"status": "error", "message": f"Ошибка проверки сессии: {error_msg}"}
                    
            finally:
                # Очищаем временные файлы
                try:
                    temp_file = f"{temp_session_path}.session"
                    if os.path.exists(temp_file):
                        os.remove(temp_file)
                        print(f"🧹 Временный файл удален")
                except Exception as cleanup_error:
                    print(f"⚠️ Ошибка очистки: {cleanup_error}")
                    
        except Exception as e:
            error_msg = str(e)
            print(f"❌ Общая ошибка импорта .session: {error_msg}")
            return {"status": "error", "message": f"Ошибка импорта: {error_msg}"}

    async def add_account_from_tdata(self,
                                   tdata_path: str,
                                   proxy: Optional[str] = None,
                                   current_user_id: Optional[int] = None) -> Dict:
        """Добавление аккаунта из TDATA папки с правильным парсингом файлов"""
        import shutil
        import traceback
        import struct
        import sqlite3
        import json
        from pathlib import Path

        try:
            print(f"🔄 Импорт аккаунта из TDATA: {tdata_path}")

            # Валидация входных данных
            if not tdata_path or not isinstance(tdata_path, str):
                return {"status": "error", "message": "Некорректный путь к TDATA"}

            if not os.path.exists(tdata_path):
                print(f"❌ TDATA папка не найдена: {tdata_path}")
                return {"status": "error", "message": "TDATA папка не найдена"}

            if not os.path.isdir(tdata_path):
                print(f"❌ Путь не является папкой: {tdata_path}")
                return {"status": "error", "message": "Указанный путь не является папкой"}

            # Проверяем наличие файлов
            try:
                tdata_files = os.listdir(tdata_path)
                print(f"📁 Файлы в TDATA папке: {tdata_files}")
            except Exception as list_error:
                return {"status": "error", "message": f"Ошибка чтения папки: {str(list_error)}"}

            if not tdata_files:
                return {"status": "error", "message": "TDATA папка пустая"}

            # Ищем основные файлы TDATA
            key_files = []
            map_files = []
            settings_file = None

            for file_name in tdata_files:
                if file_name.startswith("key_data"):
                    key_files.append(file_name)
                elif file_name.startswith("map"):
                    map_files.append(file_name)
                elif file_name == "settings0":
                    settings_file = file_name

            print(f"🔍 Key files: {key_files}")
            print(f"🔍 Map files: {len(map_files)} файлов")
            print(f"🔍 Settings file: {settings_file}")

            if not key_files:
                return {"status": "error", "message": "Не найден файл key_data в TDATA"}

            # Создаем временную сессию для Pyrogram
            import uuid
            temp_session_name = f"tdata_{uuid.uuid4().hex[:8]}"
            temp_session_dir = os.path.join(SESSIONS_DIR, f"temp_{temp_session_name}")

            try:
                # Создаем временную папку
                os.makedirs(temp_session_dir, exist_ok=True)

                # Копируем все TDATA файлы во временную папку
                for file_name in tdata_files:
                    src_file = os.path.join(tdata_path, file_name)
                    dst_file = os.path.join(temp_session_dir, file_name)

                    if os.path.isfile(src_file):
                        shutil.copy2(src_file, dst_file)
                        print(f"✅ Скопирован файл: {file_name}")

                # Пытаемся создать клиент, указав TDATA папку как рабочую директорию
                print(f"🔄 Создаем Pyrogram клиент с TDATA...")

                client = Client(
                    name=temp_session_name,
                    api_id=API_ID,
                    api_hash=API_HASH,
                    workdir=temp_session_dir,
                    proxy=self._parse_proxy(proxy) if proxy else None,
                    no_updates=True,
                    in_memory=False
                )

                print(f"🔄 Подключаемся к Telegram...")
                await client.connect()

                # Проверяем авторизацию
                try:
                    me = await client.get_me()

                    if me and me.id:
                        print(f"✅ Успешная авторизация: {me.first_name} ({me.phone_number})")

                        # Создаем постоянную сессию
                        phone_clean = me.phone_number.replace('+', '').replace(' ', '').replace('(', '').replace(')', '').replace('-', '')
                        permanent_session_name = f"session_{phone_clean}"
                        permanent_session_path = os.path.join(SESSIONS_DIR, permanent_session_name)

                        # Отключаемся от временного клиента
                        await client.disconnect()

                        # Ищем созданный файл сессии
                        temp_session_file = os.path.join(temp_session_dir, f"{temp_session_name}.session")

                        if os.path.exists(temp_session_file):
                            # Копируем файл сессии в постоянное место
                            permanent_session_file = f"{permanent_session_path}.session"
                            shutil.copy2(temp_session_file, permanent_session_file)
                            print(f"✅ Сессия сохранена: {permanent_session_file}")

                            # Сохраняем аккаунт в базу данных
                            await self._save_account(
                                phone=me.phone_number,
                                session_path=permanent_session_path,
                                name=me.first_name or "TDATA User",
                                proxy=proxy,
                                user_id=me.id,
                                session_data=None,  # Будет считан из файла
                                current_user_id=current_user_id
                            )

                            return {
                                "status": "success",
                                "name": me.first_name or "TDATA User",
                                "phone": me.phone_number
                            }
                        else:
                            return {"status": "error", "message": "Не удалось создать файл сессии"}
                    else:
                        await client.disconnect()
                        return {"status": "error", "message": "Не удалось получить информацию о пользователе"}

                except Exception as auth_error:
                    print(f"❌ Ошибка авторизации: {auth_error}")
                    try:
                        await client.disconnect()
                    except:
                        pass

                    # Если стандартный способ не работает, пробуем альтернативный метод
                    return await self._try_alternative_tdata_import(tdata_path, proxy, current_user_id)

            except Exception as client_error:
                print(f"❌ Ошибка создания клиента: {client_error}")
                return await self._try_alternative_tdata_import(tdata_path, proxy, current_user_id)

            finally:
                # Очищаем временную папку
                try:
                    if os.path.exists(temp_session_dir):
                        shutil.rmtree(temp_session_dir)
                        print(f"🧹 Временная папка очищена")
                except Exception as cleanup_error:
                    print(f"⚠️ Ошибка очистки: {cleanup_error}")

        except Exception as e:
            error_msg = str(e)
            error_trace = traceback.format_exc()
            print(f"❌ Общая ошибка импорта TDATA: {error_msg}")
            print(f"🔍 Стек ошибки: {error_trace}")

            return {"status": "error", "message": f"Ошибка импорта TDATA: {error_msg}"}

    async def _try_alternative_tdata_import(self, tdata_path: str, proxy: Optional[str], current_user_id: Optional[int]) -> Dict:
        """Альтернативный метод импорта TDATA используя копирование файлов"""
        import uuid
        import shutil

        try:
            print(f"🔄 Пробуем альтернативный метод импорта TDATA...")

            # Генерируем уникальное имя сессии
            temp_name = f"alt_tdata_{uuid.uuid4().hex[:8]}"
            temp_session_path = os.path.join(SESSIONS_DIR, temp_name)

            # Создаем новый клиент без TDATA
            client = Client(
                name=temp_name,
                api_id=API_ID,
                api_hash=API_HASH,
                proxy=self._parse_proxy(proxy) if proxy else None,
                no_updates=True,
                workdir=SESSIONS_DIR
            )

            # Сначала подключаемся как обычно
            await client.connect()

            # Теперь попробуем заменить файл сессии на TDATA
            await client.disconnect()

            # Ищем основные файлы TDATA
            tdata_files = os.listdir(tdata_path)
            key_data_file = None

            for file_name in tdata_files:
                if file_name.startswith("key_data"):
                    key_data_file = os.path.join(tdata_path, file_name)
                    break

            if not key_data_file or not os.path.exists(key_data_file):
                return {"status": "error", "message": "Не найден файл key_data"}

            # Копируем TDATA файлы как сессию
            session_file = f"{temp_session_path}.session"

            # Читаем key_data
            with open(key_data_file, 'rb') as f:
                key_data = f.read()

            # Создаем базовую SQLite сессию для Pyrogram
            import sqlite3

            conn = sqlite3.connect(session_file)
            cursor = conn.cursor()

            # Создаем минимальную структуру сессии Pyrogram
            cursor.execute('''
                CREATE TABLE sessions (
                    id INTEGER PRIMARY KEY,
                    dc_id INTEGER PRIMARY KEY,
                    server_address TEXT,
                    port INTEGER,
                    auth_key BLOB,
                    date INTEGER,
                    user_id INTEGER,
                    is_bot INTEGER
                )
            ''')

            cursor.execute('''
                CREATE TABLE peers (
                    id INTEGER PRIMARY KEY,
                    access_hash INTEGER,
                    type INTEGER,
                    username TEXT,
                    phone_number TEXT
                )
            ''')

            cursor.execute('''
                CREATE TABLE version (
                    number INTEGER PRIMARY KEY
                )
            ''')

            # Вставляем версию
            cursor.execute('INSERT INTO version VALUES (?)', (4,))

            # Вставляем базовые данные сессии (с дефолтными значениями)
            cursor.execute('''
                INSERT INTO sessions (dc_id, server_address, port, auth_key, date, user_id, is_bot) VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (2, 'telegram.org', 443, key_data[:256] if len(key_data) > 256 else key_data, 0, 0, 0))

            conn.commit()
            conn.close()

            print(f"✅ Создана базовая сессия из TDATA")

            # Пробуем подключиться с новой сессией
            test_client = Client(
                name=temp_name,
                api_id=API_ID,
                api_hash=API_HASH,
                proxy=self._parse_proxy(proxy) if proxy else None,
                no_updates=True,
                workdir=SESSIONS_DIR
            )

            try:
                await test_client.connect()
                me = await test_client.get_me()

                if me and me.id:
                    print(f"✅ Альтернативный метод успешен: {me.first_name}")

                    # Создаем постоянную сессию
                    phone_clean = me.phone_number.replace('+', '').replace(' ', '').replace('(', '').replace(')', '').replace('-', '')
                    final_session_name = f"session_{phone_clean}"
                    final_session_path = os.path.join(SESSIONS_DIR, final_session_name)

                    await test_client.disconnect()

                    # Копируем файл сессии
                    shutil.copy2(session_file, f"{final_session_path}.session")

                    # Удаляем временный файл
                    try:
                        os.remove(session_file)
                    except:
                        pass

                    # Сохраняем аккаунт в базу данных
                    await self._save_account(
                        phone=me.phone_number,
                        session_path=final_session_path,
                        name=me.first_name or "TDATA User",
                        proxy=proxy,
                        user_id=me.id,
                        session_data=None,
                        current_user_id=current_user_id
                    )

                    return {
                        "status": "success",
                        "name": me.first_name or "TDATA User",
                        "phone": me.phone_number
                    }
                else:
                    await test_client.disconnect()
                    return {"status": "error", "message": "Альтернативный метод: не удалось авторизоваться"}

            except Exception as test_error:
                print(f"❌ Альтернативный метод не сработал: {test_error}")
                try:
                    await test_client.disconnect()
                except:
                    pass

                # Очищаем временные файлы
                try:
                    os.remove(session_file)
                except:
                    pass

                return {"status": "error", "message": "Не удалось импортировать TDATA. Возможно, файлы повреждены или устарели"}

        except Exception as e:
            print(f"❌ Ошибка альтернативного метода: {e}")
            return {"status": "error", "message": f"Альтернативный импорт не удался: {str(e)}"}

    async def _save_account(self, phone: str, session_path: str, name: str, proxy: Optional[str], user_id: int, session_data: Optional[str], current_user_id: Optional[int]): # Добавлены user_id и current_user_id
        """Сохранение аккаунта в базу данных"""
        db = next(get_db())
        try:
            session_file_path = f"{session_path}.session"

            if not os.path.exists(session_file_path):
                raise Exception(f"Session file not found: {session_file_path}")

            if session_data is None: # Если session_data не переданы, читаем из файла
                with open(session_file_path, "rb") as f:
                    session_data = f.read()

            # Правильная обработка данных сессии
            if isinstance(session_data, bytes):
                # Если данные уже в байтах (из файла), шифруем напрямую
                encrypted_session = self.cipher.encrypt(session_data).decode()
            else:
                # Если данные в виде строки, сначала кодируем в байты
                try:
                    encrypted_session = self.cipher.encrypt(session_data.encode()).decode()
                except Exception:
                    import base64
                    encrypted_session = base64.b64encode(session_data.encode()).decode()

            existing_account = db.query(Account).filter(
                Account.phone == phone).first()
            if existing_account:
                existing_account.name = name
                existing_account.session_data = encrypted_session
                existing_account.proxy = proxy
                existing_account.status = "online"
                existing_account.is_active = True
                existing_account.user_id = current_user_id if current_user_id else user_id # Используем current_user_id как приоритетный
            else:
                account = Account(
                    phone=phone,
                    name=name,
                    session_data=encrypted_session,
                    proxy=proxy,
                    status="online",  # Устанавливаем статус "online" после успешной авторизации
                    is_active=True,
                    user_id=current_user_id if current_user_id else user_id # Используем current_user_id как приоритетный
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
        """Быстрое получение или создание клиента"""
        # Проверяем кеш клиентов
        if account_id in self.clients:
            client = self.clients[account_id]
            if hasattr(client, 'is_connected') and client.is_connected:
                return client
            else:
                del self.clients[account_id]

        # Получаем данные аккаунта
        db = next(get_db())
        try:
            account = db.query(Account).filter(
                Account.id == account_id).first()
            if not account or not account.is_active:
                return None

            # Быстрый поиск файла сессии
            phone_clean = account.phone.replace('+', '').replace(
                ' ', '').replace('(', '').replace(')', '').replace('-', '')
            
            session_file = os.path.join(SESSIONS_DIR, f"session_{phone_clean}")
            if not os.path.exists(f"{session_file}.session"):
                return None

            # Создаем клиент с быстрыми настройками
            client = Client(session_file,
                            api_id=API_ID,
                            api_hash=API_HASH,
                            proxy=self._parse_proxy(account.proxy) if account.proxy else None,
                            sleep_threshold=30,
                            max_concurrent_transmissions=2,
                            no_updates=True,
                            workers=1)

            # Быстрое подключение
            try:
                await asyncio.wait_for(client.connect(), timeout=10)
                
                # Создаем заглушку для client.me если не можем получить быстро
                try:
                    me = await asyncio.wait_for(client.get_me(), timeout=5)
                    client.me = me
                except (asyncio.TimeoutError, FloodWait):
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
                try:
                    await client.disconnect()
                except:
                    pass
                return None

        except Exception as e:
            return None
        finally:
            db.close()

    async def get_user_contacts(self, account_id: int) -> dict:
        """Безопасное получение контактов пользователя"""
        import traceback
        try:
            print(f"📱 Получение контактов для аккаунта {account_id}")

            client = await self._get_client_for_account(account_id)
            if not client:
                print(f"❌ Не удалось получить клиент для аккаунта {account_id}")
                return {"status": "error", "message": "Не удалось подключиться к аккаунту"}

            # Проверяем подключение
            if not client.is_connected:
                print(f"🔌 Подключаем клиент для аккаунта {account_id}")
                await client.connect()

            # Проверяем авторизацию
            try:
                me = await client.get_me()
                if not me:
                    return {"status": "error", "message": "Ошибка авторизации аккаунта"}
                print(f"✅ Авторизован как: {me.first_name}")
            except Exception as auth_error:
                print(f"❌ Ошибка авторизации: {auth_error}")
                return {"status": "error", "message": f"Ошибка авторизации: {str(auth_error)}"}

            contacts_list = []

            try:
                print("📋 Получаем список контактов...")
                contacts = await client.get_contacts()
                print(f"📊 Получено {len(contacts)} контактов из API")

            except Exception as e:
                error_msg = str(e)
                print(f"❌ Ошибка получения списка контактов: {error_msg}")
                print(traceback.format_exc())

                # Пробуем альтернативный метод через диалоги
                print("🔄 Пробуем получить контакты через диалоги...")
                try:
                    async for dialog in client.get_dialogs(limit=100):
                        chat = dialog.chat
                        if hasattr(chat, 'type') and 'PRIVATE' in str(chat.type):
                            if chat.id != me.id:  # Исключаем самого себя
                                contact_data = {
                                    "id": chat.id,
                                    "first_name": getattr(chat, "first_name", "") or "",
                                    "last_name": getattr(chat, "last_name", "") or "",
                                    "username": getattr(chat, "username", "") or "",
                                    "phone": "",
                                    "is_bot": bool(getattr(chat, "is_bot", False)),
                                    "is_verified": bool(getattr(chat, "is_verified", False)),
                                    "is_premium": bool(getattr(chat, "is_premium", False)),
                                    "display_name": f"{getattr(chat, 'first_name', '')} {getattr(chat, 'last_name', '')}".strip() or getattr(chat, 'username', '') or f"User {chat.id}"
                                }
                                contacts_list.append(contact_data)

                    print(f"📊 Получено {len(contacts_list)} контактов через диалоги")

                    if contacts_list:
                        return {
                            "status": "success",
                            "contacts": contacts_list,
                            "count": len(contacts_list)
                        }
                    else:
                        return {"status": "error", "message": "Контакты не найдены"}

                except Exception as dialog_error:
                    print(f"❌ Ошибка получения диалогов: {dialog_error}")
                    return {"status": "error", "message": f"Ошибка получения контактов: {error_msg}"}

            # Обрабатываем полученные контакты
            for contact in contacts:
                if contact is None:
                    continue

                try:
                    first_name = getattr(contact, "first_name", "") or ""
                    last_name = getattr(contact, "last_name", "") or ""
                    username = getattr(contact, "username", "") or ""
                    contact_id = getattr(contact, "id", None)

                    if contact_id and contact_id != me.id:  # Исключаем самого себя
                        contact_data = {
                            "id": contact_id,
                            "first_name": first_name,
                            "last_name": last_name,
                            "username": username,
                            "phone": getattr(contact, "phone_number", "") or "",
                            "is_bot": bool(getattr(contact, "is_bot", False)),
                            "is_verified": bool(getattr(contact, "is_verified", False)),
                            "is_premium": bool(getattr(contact, "is_premium", False)),
                            "display_name": f"{first_name} {last_name}".strip() or username or f"User {contact_id}"
                        }
                        contacts_list.append(contact_data)

                except Exception as ce:
                    print(f"⚠️ Ошибка обработки контакта: {ce}")
                    continue

            print(f"✅ Обработано {len(contacts_list)} контактов")

            if not contacts_list:
                return {"status": "error", "message": "У аккаунта нет контактов для рассылки"}

            return {
                "status": "success",
                "contacts": contacts_list,
                "count": len(contacts_list)
            }

        except Exception as e:
            error_msg = str(e)
            print(f"❌ Общая ошибка при получении контактов: {error_msg}")
            print(traceback.format_exc())
            return {"status": "error", "message": f"Не удалось получить контакты: {error_msg}"}

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
                elif chat_id.isdigit():
                    target_chat_id = int(chat_id)
                elif chat_id.startswith('-') and chat_id[1:].isdigit():
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

    async def send_message(self, account_id: int, recipient: str, message: str, file_path: str = None, schedule_seconds: int = 0) -> dict:
        """Быстрая отправка сообщения/файла"""
        try:
            client = await self._get_client_for_account(account_id)
            if not client:
                return {"status": "error", "message": "Клиент не найден"}

            # Быстрая проверка подключения
            if not client.is_connected:
                try:
                    await asyncio.wait_for(client.connect(), timeout=10)
                except Exception:
                    return {"status": "error", "message": "Не удалось подключиться"}

            # Нормализация получателя
            if not recipient.startswith('@') and not recipient.startswith('+') and not recipient.isdigit() and not recipient.startswith('-'):
                recipient = f"@{recipient}"
            
            target_id = recipient if not recipient.isdigit() else int(recipient)
            
            schedule_date = None
            if schedule_seconds > 0:
                from datetime import datetime, timedelta
                schedule_date = datetime.utcnow() + timedelta(seconds=schedule_seconds)

            # Быстрая отправка файла
            if file_path and os.path.exists(file_path):
                try:
                    sent = await client.send_document(
                        chat_id=target_id,
                        document=file_path,
                        caption=message or "",
                        force_document=True,
                        schedule_date=schedule_date
                    )
                    return {
                        "status": "success",
                        "message_id": getattr(sent, "id", None)
                    }
                except Exception as e:
                    return {"status": "error", "message": str(e)}
            
            # Быстрая отправка текста
            else:
                try:
                    sent = await client.send_message(
                        chat_id=target_id,
                        text=message or "",
                        schedule_date=schedule_date
                    )
                    return {
                        "status": "success",
                        "message_id": getattr(sent, "id", None)
                    }
                except Exception as e:
                    return {"status": "error", "message": str(e)}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    async def send_message_scheduled_lightning(self, account_id: int, recipient: str, message: str, file_path: str = None, schedule_seconds: int = 0) -> dict:
        """⚡ МОЛНИЕНОСНАЯ отправка через scheduled API для максимальной скорости"""
        try:
            client = await self._get_client_for_account(account_id)
            if not client:
                return {"status": "error", "message": "Клиент не найден"}

            # Супербыстрая проверка подключения
            if not client.is_connected:
                try:
                    await asyncio.wait_for(client.connect(), timeout=5)
                except Exception:
                    return {"status": "error", "message": "Не удалось подключиться"}

            # Нормализация получателя
            if not recipient.startswith('@') and not recipient.startswith('+') and not recipient.isdigit() and not recipient.startswith('-'):
                recipient = f"@{recipient}"
            
            target_id = recipient if not recipient.isdigit() else int(recipient)
            
            # ⚡ Используем scheduled API для мгновенной доставки (парадокс, но работает быстрее)
            from datetime import datetime, timedelta
            schedule_date = datetime.utcnow() + timedelta(seconds=1)  # Почти мгновенно

            # ⚡ МОЛНИЕНОСНАЯ отправка файла
            if file_path and os.path.exists(file_path):
                try:
                    sent = await client.send_document(
                        chat_id=target_id,
                        document=file_path,
                        caption=message or "",
                        force_document=True,
                        schedule_date=schedule_date,
                        disable_notification=False  # Максимальная видимость
                    )
                    return {
                        "status": "success", 
                        "message_id": getattr(sent, "id", None)
                    }
                except Exception as e:
                    # Откат на обычную отправку при ошибке scheduled
                    try:
                        sent = await client.send_document(
                            chat_id=target_id,
                            document=file_path,
                            caption=message or "",
                            force_document=True
                        )
                        return {
                            "status": "success",
                            "message_id": getattr(sent, "id", None)
                        }
                    except Exception as e2:
                        return {"status": "error", "message": str(e2)}
            
            # ⚡ МОЛНИЕНОСНАЯ отправка текста
            else:
                try:
                    sent = await client.send_message(
                        chat_id=target_id,
                        text=message or "",
                        schedule_date=schedule_date,
                        disable_notification=False  # Максимальная видимость
                    )
                    return {
                        "status": "success",
                        "message_id": getattr(sent, "id", None)
                    }
                except Exception as e:
                    # Откат на обычную отправку при ошибке scheduled
                    try:
                        sent = await client.send_message(
                            chat_id=target_id,
                            text=message or ""
                        )
                        return {
                            "status": "success",
                            "message_id": getattr(sent, "id", None)
                        }
                    except Exception as e2:
                        return {"status": "error", "message": str(e2)}
                        
        except Exception as e:
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

    async def delete_telegram_account(self, account_id: int, reason: str = "Больше не нужен") -> Dict:
        """Полное удаление аккаунта из Telegram"""
        try:
            print(f"🗑️ Начинаем удаление аккаунта {account_id} из Telegram")

            client = await self._get_client_for_account(account_id)
            if not client:
                return {"status": "error", "message": "Не удалось подключиться к аккаунту"}

            if not client.is_connected:
                await client.connect()

            # Получаем информацию о пользователе перед удалением
            try:
                me = await client.get_me()
                user_info = f"{me.first_name} ({me.phone_number})"
                print(f"📱 Удаляем аккаунт: {user_info}")
            except Exception as e:
                user_info = f"Account ID {account_id}"
                print(f"⚠️ Не удалось получить информацию о пользователе: {e}")

            # Выполняем удаление аккаунта через API Telegram
            try:
                # Отправляем запрос на удаление аккаунта
                from pyrogram.raw import functions

                await client.invoke(
                    functions.account.DeleteAccount(reason=reason)
                )

                print(f"✅ Аккаунт {user_info} успешно удален из Telegram")

                # Закрываем соединение
                await client.disconnect()

                # Удаляем клиент из памяти
                if account_id in self.clients:
                    del self.clients[account_id]

                # Удаляем файл сессии
                await self._cleanup_account_files(account_id)

                # Обновляем статус в базе данных
                await self._mark_account_as_deleted(account_id)

                return {
                    "status": "success",
                    "message": f"Аккаунт {user_info} удален из Telegram",
                    "deleted_account": user_info
                }

            except Exception as delete_error:
                error_msg = str(delete_error)
                print(f"❌ Ошибка при удалении аккаунта: {error_msg}")

                # Специальная обработка известных ошибок
                if "ACCOUNT_DELETE_DISABLED" in error_msg:
                    return {"status": "error", "message": "Удаление аккаунта отключено в настройках Telegram"}
                elif "ACCOUNT_DELETE_BLOCKED" in error_msg:
                    return {"status": "error", "message": "Удаление аккаунта заблокировано (возможно, есть активные штрафы)"}
                elif "TWO_FA_REQUIRED" in error_msg:
                    return {"status": "error", "message": "Требуется отключить двухфакторную аутентификацию перед удалением"}
                else:
                    return {"status": "error", "message": f"Ошибка удаления: {error_msg}"}

        except Exception as general_error:
            error_msg = str(general_error)
            print(f"❌ Общая ошибка удаления аккаунта {account_id}: {error_msg}")
            return {"status": "error", "message": f"Общая ошибка: {error_msg}"}

    async def _cleanup_account_files(self, account_id: int):
        """Очистка файлов аккаунта после удаления"""
        try:
            db = next(get_db())
            try:
                account = db.query(Account).filter(Account.id == account_id).first()
                if account:
                    # Определяем путь к файлу сессии
                    phone_clean = account.phone.replace('+', '').replace(' ', '').replace('(', '').replace(')', '').replace('-', '')
                    session_names = [f"session_{phone_clean}", f"session_{account.phone}", phone_clean]

                    # Удаляем все возможные файлы сессии
                    for session_name in session_names:
                        session_file = os.path.join(SESSIONS_DIR, f"{session_name}.session")
                        if os.path.exists(session_file):
                            try:
                                os.remove(session_file)
                                print(f"🗑️ Удален файл сессии: {session_file}")
                            except Exception as e:
                                print(f"⚠️ Не удалось удалить файл сессии {session_file}: {e}")
            finally:
                db.close()
        except Exception as e:
            print(f"⚠️ Ошибка очистки файлов аккаунта {account_id}: {e}")

    async def _mark_account_as_deleted(self, account_id: int):
        """Помечает аккаунт как удаленный в базе данных"""
        try:
            db = next(get_db())
            try:
                account = db.query(Account).filter(Account.id == account_id).first()
                if account:
                    account.status = "deleted"
                    account.is_active = False
                    account.session_data = None  # Удаляем данные сессии
                    db.commit()
                    print(f"📝 Аккаунт {account_id} помечен как удаленный в базе данных")
            finally:
                db.close()
        except Exception as e:
            print(f"⚠️ Ошибка обновления статуса аккаунта {account_id}: {e}")

    async def send_post_comment(self, account_id: int, chat_id: str, message_id: int, comment: str) -> Dict:
        """Отправка комментария к посту канала в секцию 'Leave a comment'"""
        try:
            print(f"📝 Отправляем комментарий к посту канала в секцию 'Leave a comment'...")

            client = await self._get_client_for_account(account_id)
            if not client:
                return {"status": "error", "message": "Не удалось получить клиент"}

            if not client.is_connected:
                await client.connect()

            # Проверяем авторизацию
            try:
                me = await client.get_me()
                client.me = me
                print(f"👤 Отправляем от имени: {me.first_name}")
            except Exception:
                print("⚠️ Не удалось получить информацию о пользователе")

            print(f"🎯 Канал: {chat_id}, пост ID: {message_id}")
            print(f"💬 Комментарий: {comment}")

            # Пробуем разные методы отправки комментария
            try:
                # Метод 1: Стандартная отправка сообщения с reply_to_message_id
                print("🔄 Пробуем стандартный метод отправки...")
                sent_message = await client.send_message(
                    chat_id=chat_id,
                    text=comment,
                    reply_to_message_id=message_id
                )

                if sent_message:
                    print(f"✅ Комментарий отправлен стандартным методом! ID: {sent_message.id}")
                    return {"status": "success", "message_id": sent_message.id}

            except Exception as e1:
                error_msg = str(e1)
                print(f"❌ Стандартный метод не сработал: {error_msg}")

                # Если ошибка связана с правами, пробуем другие методы
                if "CHAT_WRITE_FORBIDDEN" in error_msg or "CHAT_ADMIN_REQUIRED" in error_msg:
                    try:
                        # Метод 2: Попробуем использовать raw API
                        print("🔄 Пробуем raw API...")
                        from pyrogram.raw import functions, types

                        # Создаем объект InputReplyToMessage для Pyrogram v2.x
                        reply_to = types.InputReplyToMessage(
                            reply_to_msg_id=message_id
                        )

                        result = await client.invoke(
                            functions.messages.SendMessage(
                                peer=await client.resolve_peer(chat_id),
                                message=comment,
                                reply_to=reply_to,
                                random_id=client.rnd_id()
                            )
                        )

                        if result:
                            message_id = result.id if hasattr(result, 'id') else result.updates[0].id
                            print(f"✅ Комментарий отправлен через raw API! ID: {message_id}")
                            return {"status": "success", "message_id": message_id}

                    except Exception as e2:
                        print(f"❌ Raw API тоже не сработал: {str(e2)}")

                        # Метод 3: Ищем группу обсуждений канала
                        try:
                            print("🔄 Ищем группу обсуждений канала...")

                            # Получаем полную информацию о канале
                            chat = await client.get_chat(chat_id)

                            if hasattr(chat, 'linked_chat') and chat.linked_chat:
                                discussion_group_id = chat.linked_chat.id
                                print(f"📢 Найдена группа обсуждений: {discussion_group_id}")

                                # Отправляем комментарий в группу обсуждений
                                sent_message = await client.send_message(
                                    chat_id=discussion_group_id,
                                    text=comment,
                                    reply_to_message_id=message_id
                                )

                                if sent_message:
                                    print(f"✅ Комментарий отправлен в группу обсуждений! ID: {sent_message.id}")
                                    return {"status": "success", "message_id": sent_message.id}
                            else:
                                print("❌ Группа обсуждений не найдена")
                                return {"status": "error", "message": "У канала нет группы обсуждений"}

                        except Exception as e3:
                            print(f"❌ Ошибка поиска группы обсуждений: {str(e3)}")
                            return {"status": "error", "message": f"Не удалось отправить комментарий: {str(e3)}"}
                else:
                    print(f"❌ Ошибка комментария: Не удалось отправить комментарий под пост: {str(e1)}")

                    # Сохраняем ошибку в историю комментариев
                    try:
                        from app.database import CommentLog, get_db
                        db = next(get_db())

                        comment_log = CommentLog(
                            account_id=account_id,
                            chat_id=chat_id,
                            message_id=message_id,
                            comment=comment,
                            status="failed",
                            error_message=str(e1)
                        )
                        db.add(comment_log)
                        db.commit()
                        db.close()
                        print(f"📝 Комментарий с ошибкой сохранен в историю: ID {comment_log.id}")
                    except Exception as log_error:
                        print(f"❌ Ошибка сохранения в историю: {log_error}")

                    return {"status": "error", "message": f"Не удалось отправить комментарий под пост: {str(e1)}"}

        except Exception as general_error:
            error_msg = str(general_error)
            print(f"❌ Общая ошибка отправки комментария: {error_msg}")

            # Сохраняем ошибку в историю комментариев
            try:
                from app.database import CommentLog, get_db
                db = next(get_db())

                comment_log = CommentLog(
                    account_id=account_id,
                    chat_id=chat_id,
                    message_id=message_id,
                    comment=comment,
                    status="failed",
                    error_message=str(general_error)
                )
                db.add(comment_log)
                db.commit()
                db.close()
                print(f"📝 Комментарий с ошибкой сохранен в историю: ID {comment_log.id}")
            except Exception as log_error:
                print(f"❌ Ошибка сохранения в историю: {log_error}")

            return {"status": "error", "message": f"Общая ошибка: {error_msg}"}

    async def _send_comment_telethon_only(self, account_id: int, chat_id: str, message_id: int, comment: str) -> Dict:
        """Отправка комментария используя только Telethon с правильной обработкой сессий"""
        try:
            print(f"📱 Telethon: Начинаем отправку комментария...")

            # Получаем данные аккаунта
            db = next(get_db())
            try:
                account = db.query(Account).filter(Account.id == account_id).first()
                if not account:
                    return {"status": "error", "message": "Telethon: Аккаунт не найден"}

                # Импортируем telethon только когда нужно
                try:
                    from telethon import TelegramClient
                    from telethon.sessions import StringSession
                    print(f"✅ Telethon библиотека импортирована")
                except ImportError:
                    print(f"❌ Telethon не установлен")
                    return {"status": "error", "message": "Telethon не установлен. Установите: pip install telethon"}

                # Создаем уникальную сессию для Telethon
                phone_clean = account.phone.replace('+', '').replace(' ', '').replace('(', '').replace(')', '').replace('-', '')
                pyrogram_session_file = os.path.join(SESSIONS_DIR, f"session_{phone_clean}.session")

                # Создаем новую чистую сессию для каждого запроса
                import uuid
                unique_session_name = f"telethon_comment_{uuid.uuid4().hex[:8]}"
                telethon_session_file = os.path.join(SESSIONS_DIR, unique_session_name)

                print(f"🔗 Telethon: Создаем уникальную сессию: {telethon_session_file}.session")

                # Проверяем существует ли Pyrogram сессия для конвертации
                if not os.path.exists(pyrogram_session_file):
                    print(f"❌ Telethon: Файл Pyrogram сессии не найден: {pyrogram_session_file}")
                    return {"status": "error", "message": "Telethon: Pyrogram сессия не найдена"}

                # Создаем новую чистую сессию для Telethon
                try:
                    await self._create_clean_telethon_session(pyrogram_session_file, telethon_session_file)
                except Exception as convert_error:
                    print(f"❌ Telethon: Ошибка создания сессии: {convert_error}")
                    return {"status": "error", "message": f"Telethon: Не удалось создать сессию: {str(convert_error)}"}

                # Создаем Telethon клиент
                try:
                    print(f"✅ Telethon: Создаем клиент...")
                    telethon_client = TelegramClient(telethon_session_file, API_ID, API_HASH)

                except Exception as client_create_error:
                    print(f"❌ Telethon: Ошибка создания клиента: {client_create_error}")
                    return {"status": "error", "message": f"Telethon: Ошибка создания клиента: {str(client_create_error)}"}

                try:
                    print(f"🔌 Telethon: Подключаемся к Telegram...")
                    await telethon_client.start()

                    # Проверяем авторизацию
                    me = await telethon_client.get_me()
                    print(f"✅ Telethon: Авторизован как {me.first_name} ({me.phone})")

                    # Нормализуем chat_id для Telethon
                    if chat_id.startswith('@'):
                        target_entity = chat_id
                        print(f"🎯 Telethon: Цель по username: {target_entity}")
                    elif chat_id.isdigit() or (chat_id.startswith('-') and chat_id[1:].isdigit()):
                        target_entity = int(chat_id)
                        print(f"🎯 Telethon: Цель по ID: {target_entity}")
                    else:
                        target_entity = chat_id
                        print(f"🎯 Telethon: Цель как есть: {target_entity}")

                    # Получаем информацию о целевом чате/канале
                    try:
                        entity = await telethon_client.get_entity(target_entity)
                        print(f"📍 Telethon: Получена сущность - {type(entity).__name__}")

                        # Если это канал, пробуем отправить комментарий прямо под пост
                        if hasattr(entity, 'broadcast') and entity.broadcast:
                            print(f"📺 Telethon: Обнаружен канал, пробуем отправить комментарий под пост...")

                            # Метод 1: Попытка отправить комментарий напрямую под пост {message_id}
                            try:
                                print(f"🔍 Telethon: Метод 1 - Отправляем комментарий прямо под пост {message_id}...")

                                await asyncio.sleep(2)  # Имитация человеческого поведения

                                # Пробуем отправить как reply к конкретному сообщению
                                sent_message = await telethon_client.send_message(
                                    entity=entity,
                                    message=comment,
                                    reply_to=message_id
                                )

                                print(f"✅ Telethon: Комментарий отправлен под пост! ID: {sent_message.id}")
                                return {
                                    "status": "success",
                                    "message": "Комментарий отправлен под пост канала",
                                    "message_id": sent_message.id
                                }

                            except Exception as direct_error:
                                error_str = str(direct_error)
                                print(f"⚠️ Telethon: Прямая отправка не удалась: {error_str}, пробуем альтернативные методы")

                                # Если прямая отправка не работает, пробуем через reactions API
                                if "CHAT_WRITE_FORBIDDEN" in error_str or "CHAT_ADMIN_REQUIRED" in error_str:
                                    print(f"🔍 Telethon: Метод 2 - Пробуем использовать Reactions API...")

                                    try:
                                        # Используем SendReaction для отправки текстовой реакции (если поддерживается)
                                        from telethon.tl.functions.messages import SendReactionRequest
                                        from telethon.tl.types import ReactionEmoji, ReactionCustomEmoji

                                        # Пробуем отправить эмодзи реакцию с текстом (если канал поддерживает)
                                        await telethon_client(SendReactionRequest(
                                            peer=entity,
                                            msg_id=message_id,
                                            reaction=[ReactionEmoji(emoticon="💬")]  # Используем эмодзи комментария
                                        ))

                                        print(f"✅ Telethon: Реакция отправлена под пост! (комментарий как реакция)")
                                        return {
                                            "status": "success",
                                            "message": "Реакция отправлена под пост (комментарии недоступны)",
                                            "message_id": f"reaction_{message_id}"
                                        }

                                    except Exception as reaction_error:
                                        print(f"⚠️ Telethon: Реакции также недоступны: {reaction_error}")

                                        # Метод 3: Ищем тред обсуждения только если прямые методы не работают
                                        print(f"🔍 Telethon: Метод 3 - Ищем тред обсуждения как последний вариант...")

                                        try:
                                            from telethon.tl.functions.messages import GetDiscussionMessageRequest

                                            discussion_result = await telethon_client(GetDiscussionMessageRequest(
                                                peer=entity,
                                                msg_id=message_id
                                            ))

                                            if discussion_result and hasattr(discussion_result, 'messages') and len(discussion_result.messages) >= 2:
                                                discussion_head = discussion_result.messages[1]
                                                discussion_chat = discussion_result.chats[0] if discussion_result.chats else None

                                                if discussion_chat and discussion_head:
                                                    print(f"📢 Telethon: Найден тред обсуждения в чате {discussion_chat.id}")

                                                    await asyncio.sleep(2)

                                                    sent_message = await telethon_client.send_message(
                                                        entity=discussion_chat,
                                                        message=comment,
                                                        reply_to=discussion_head.id
                                                    )

                                                    print(f"⚠️ Telethon: Комментарий отправлен в тред обсуждения (не под пост)! ID: {sent_message.id}")
                                                    return {
                                                        "status": "success",
                                                        "message": "Комментарий отправлен в тред обсуждения (прямые комментарии недоступны)",
                                                        "message_id": sent_message.id
                                                    }
                                                else:
                                                    return {"status": "error", "message": "Telethon: Комментарии под этот пост недоступны"}
                                            else:
                                                return {"status": "error", "message": "Telethon: Для поста нет возможности комментирования"}

                                        except Exception as discussion_error:
                                            print(f"❌ Telethon: Все методы комментирования недоступны: {discussion_error}")
                                            return {"status": "error", "message": "Telethon: Комментарии недоступны для этого поста"}

                                # Обрабатываем другие ошибки прямой отправки
                                if "MSG_ID_INVALID" in error_str:
                                    return {"status": "error", "message": "Telethon: Неверный ID сообщения или сообщение не найдено"}
                                elif "USER_BANNED_IN_CHANNEL" in error_str:
                                    return {"status": "error", "message": "Telethon: Аккаунт заблокирован в канале"}
                                else:
                                    return {"status": "error", "message": f"Telethon: Не удалось отправить комментарий: {error_str}"}
                        else:
                            # Это обычная группа или приватный чат
                            print(f"💬 Telethon: Отправляем комментарий в обычный чат/группу...")

                            await asyncio.sleep(2)  # Имитация человеческого поведения

                            sent_message = await telethon_client.send_message(
                                entity=entity,
                                message=comment,
                                reply_to=message_id
                            )

                            print(f"✅ Telethon: Комментарий отправлен в чат! ID: {sent_message.id}")
                            return {
                                "status": "success",
                                "message": "Комментарий отправлен",
                                "message_id": sent_message.id
                            }

                    except Exception as entity_error:
                        error_str = str(entity_error)
                        print(f"❌ Telethon: Ошибка получения сущности: {error_str}")

                        # Обрабатываем специфические ошибки Telethon
                        if "USERNAME_INVALID" in error_str:
                            return {"status": "error", "message": "Telethon: Неверное имя пользователя/канала"}
                        elif "CHAT_ADMIN_REQUIRED" in error_str:
                            return {"status": "error", "message": "Telethon: Требуются права администратора"}
                        elif "MESSAGE_ID_INVALID" in error_str:
                            return {"status": "error", "message": "Telethon: Неверный ID сообщения"}
                        elif "PEER_ID_INVALID" in error_str:
                            return {"status": "error", "message": "Telethon: Чат/канал не найден или недоступен"}
                        elif "USER_BANNED_IN_CHANNEL" in error_str:
                            return {"status": "error", "message": "Telethon: Аккаунт заблокирован в канале"}
                        else:
                            return {"status": "error", "message": f"Telethon: {error_str}"}

                finally:
                    print(f"🔌 Telethon: Отключаемся от клиента...")
                    await telethon_client.disconnect()

                    # Удаляем временную сессию
                    try:
                        session_file_path = f"{telethon_session_file}.session"
                        if os.path.exists(session_file_path):
                            os.remove(session_file_path)
                            print(f"🗑️ Telethon: Временная сессия удалена")
                    except Exception as cleanup_error:
                        print(f"⚠️ Telethon: Ошибка очистки сессии: {cleanup_error}")

            finally:
                db.close()

        except Exception as e:
            print(f"❌ Telethon: Общая ошибка комментария: {e}")
            import traceback
            print(f"🔍 Telethon: Стек ошибки: {traceback.format_exc()}")
            return {"status": "error", "message": f"Telethon: {str(e)}"}

    async def _send_comment_pyrogram_enhanced(self, account_id: int, chat_id: str, message_id: int, comment: str) -> Dict:
        """Отправка комментария через Pyrogram с улучшенной логикой"""
        try:
            client = await self._get_client_for_account(account_id)
            if not client:
                return {"status": "error", "message": "Не удалось подключиться к аккаунту"}

            if not client.is_connected:
                await client.connect()

            # Проверяем что client.me установлен
            if not hasattr(client, 'me') or client.me is None:
                try:
                    me = await client.get_me()
                    client.me = me
                except Exception:
                    # Создаем заглушку если не удается получить информацию
                    from types import SimpleNamespace
                    client.me = SimpleNamespace(
                        id=account_id,
                        first_name="User",
                        is_premium=False,
                        is_verified=False,
                        is_bot=False
                    )

            print(f"🔄 Отправка комментария от аккаунта {account_id} в чат {chat_id}, к сообщению {message_id}")
            print(f"📝 Комментарий: {comment}")

            # Нормализуем chat_id
            target_chat = chat_id
            if isinstance(chat_id, str):
                if chat_id.startswith('@'):
                    target_chat = chat_id
                elif chat_id.isdigit():
                    target_chat = int(chat_id)
                elif chat_id.startswith('-') and chat_id[1:].isdigit():
                    target_chat = int(chat_id)

            # Метод 1: Попытка отправки через reply_to_message_id (имитация действий пользователя)
            try:
                print(f"🎯 Попытка отправки комментария как ответ на сообщение...")

                # Добавляем небольшую задержку для имитации человеческого поведения
                await asyncio.sleep(1)

                sent_message = await client.send_message(
                    chat_id=target_chat,
                    text=comment,
                    reply_to_message_id=message_id,
                    disable_notification=False  # Показываем что это активное действие
                )

                if sent_message and hasattr(sent_message, 'id'):
                    print(f"✅ Комментарий отправлен как ответ аккаунтом {account_id}")
                    return {
                        "status": "success",
                        "message": "Комментарий отправлен под пост",
                        "message_id": sent_message.id
                    }

            except Exception as reply_error:
                error_str = str(reply_error)
                print(f"❌ Ошибка отправки ответа: {error_str}")

                # Метод 2: Если ответ не работает, пробуем найти группу обсуждений
                if "CHAT_ADMIN_REQUIRED" in error_str or "CHAT_WRITE_FORBIDDEN" in error_str:
                    print(f"🔄 Пробуем найти группу обсуждений для канала {chat_id}")

                    try:
                        # Ищем группу обсуждений канала
                        channel = await client.get_chat(chat_id)
                        discussion_group_id = None

                        if hasattr(channel, 'linked_chat') and channel.linked_chat:
                            discussion_group_id = channel.linked_chat.id
                            print(f"📢 Найдена группа обсуждений: {discussion_group_id}")
                        else:
                            # Альтернативный способ поиска группы обсуждений
                            try:
                                from pyrogram.raw import functions
                                peer = await client.resolve_peer(target_chat)
                                full_channel = await client.invoke(
                                    functions.channels.GetFullChannel(channel=peer)
                                )

                                if hasattr(full_channel.full_chat, 'linked_chat_id') and full_channel.full_chat.linked_chat_id:
                                    discussion_group_id = -int(f"100{full_channel.full_chat.linked_chat_id}")
                                    print(f"📢 Альтернативно найдена группа обсуждений: {discussion_group_id}")
                            except Exception as alt_search_error:
                                print(f"❌ Альтернативный поиск не удался: {alt_search_error}")

                        if discussion_group_id:
                            # Отправляем в группу обсуждений
                            try:
                                await asyncio.sleep(1)  # Имитация человеческого поведения

                                sent_message = await client.send_message(
                                    chat_id=discussion_group_id,
                                    text=comment,
                                    reply_to_message_id=message_id
                                )

                                print(f"✅ Комментарий отправлен в группу обсуждений аккаунтом {account_id}")
                                return {
                                    "status": "success",
                                    "message": "Комментарий отправлен в группу обсуждений канала",
                                    "message_id": sent_message.id
                                }
                            except Exception as discussion_error:
                                print(f"❌ Ошибка отправки в группу обсуждений: {discussion_error}")

                        # Если группа обсуждений не найдена, вернем ошибку для попытки Telethon
                        return {
                            "status": "error",
                            "message": f"Pyrogram: У канала {chat_id} нет доступной группы обсуждений"
                        }

                    except Exception as channel_error:
                        print(f"❌ Ошибка поиска группы обсуждений: {channel_error}")
                        return {
                            "status": "error",
                            "message": f"Pyrogram: Для отправки комментариев в {chat_id} требуются права администратора"
                        }

                # Обрабатываем специфические ошибки Telegram
                if "USERNAME_INVALID" in error_str:
                    return {"status": "error", "message": f"Неверное имя пользователя или канала: {chat_id}"}
                elif "PEER_ID_INVALID" in error_str:
                    return {"status": "error", "message": f"Канал/чат {chat_id} не найден или недоступен"}
                elif "MESSAGE_ID_INVALID" in error_str:
                    return {"status": "error", "message": f"Сообщение с ID {message_id} не найдено или недоступно"}
                elif "USER_BANNED_IN_CHANNEL" in error_str:
                    return {"status": "error", "message": "Аккаунт заблокирован в этом канале"}
                elif "REPLY_MESSAGE_INVALID" in error_str:
                    return {"status": "error", "message": "Нельзя ответить на это сообщение"}
                elif "COMMENTS_DISABLED" in error_str:
                    return {"status": "error", "message": "Комментарии отключены для этого поста"}
                else:
                    return {"status": "error", "message": f"Pyrogram ошибка: {error_str}"}

        except Exception as e:
            print(f"❌ Ошибка Pyrogram комментария: {e}")
            return {"status": "error", "message": f"Pyrogram ошибка: {str(e)}"}

    async def _send_comment_telethon_enhanced(self, account_id: int, chat_id: str, message_id: int, comment: str) -> Dict:
        """Отправка комментария через Telethon непосредственно под пост канала"""
        try:
            print(f"📱 Telethon: Начинаем отправку комментария под пост...")

            # Получаем данные аккаунта
            db = next(get_db())
            try:
                account = db.query(Account).filter(Account.id == account_id).first()
                if not account:
                    return {"status": "error", "message": "Telethon: Аккаунт не найден"}

                # Импортируем telethon только когда нужно
                try:
                    from telethon import TelegramClient
                    from telethon.tl.functions.messages import SendMessageRequest
                    from telethon.tl.types import InputReplyToMessage
                    print(f"✅ Telethon библиотека импортирована")
                except ImportError:
                    print(f"❌ Telethon не установлен")
                    return {"status": "error", "message": "Telethon не установлен"}

                # Определяем путь к файлу сессии для Telethon
                phone_clean = account.phone.replace('+', '').replace(' ', '').replace('(', '').replace(')', '').replace('-', '')
                pyrogram_session_file = os.path.join(SESSIONS_DIR, f"session_{phone_clean}.session")
                telethon_session_file = os.path.join(SESSIONS_DIR, f"telethon_{phone_clean}")

                # Создаем/проверяем сессию для Telethon
                session_file_path = f"{telethon_session_file}.session"
                if not os.path.exists(session_file_path):
                    try:
                        print(f"🔄 Telethon: Создаем совместимую сессию...")
                        await self._convert_pyrogram_to_telethon_session(pyrogram_session_file, telethon_session_file)
                    except Exception as convert_error:
                        print(f"❌ Telethon: Ошибка конвертации: {convert_error}")
                        return {"status": "error", "message": "Telethon: Не удалось создать сессию"}

                # Создаем Telethon клиент
                try:
                    telethon_client = TelegramClient(telethon_session_file, API_ID, API_HASH)
                    await telethon_client.start()

                    me = await telethon_client.get_me()
                    print(f"✅ Telethon: Авторизован как {me.first_name} ({me.phone})")

                except Exception as client_error:
                    print(f"❌ Telethon: Ошибка создания клиента: {client_error}")
                    return {"status": "error", "message": "Telethon: Ошибка подключения"}

                try:
                    # Нормализуем chat_id для Telethon
                    if chat_id.startswith('@'):
                        target_entity = chat_id
                    elif chat_id.isdigit() or (chat_id.startswith('-') and chat_id[1:].isdigit()):
                        target_entity = int(chat_id)
                    else:
                        target_entity = chat_id

                    print(f"📍 Telethon: Работаем с каналом {target_entity}")

                    # Получаем информацию о целевом канале
                    entity = await telethon_client.get_entity(target_entity)
                    print(f"📍 Telethon: Получена сущность - {type(entity).__name__}")

                    # Метод 1: Отправляем комментарий прямо под пост используя InputReplyToMessage
                    try:
                        print(f"🎯 Telethon: Отправляем комментарий прямо под пост {message_id}...")

                        # Создаем reply для комментария под постом
                        reply_to = InputReplyToMessage(
                            reply_to_msg_id=message_id,
                            top_msg_id=message_id  # Указываем что это комментарий к топ-сообщению
                        )

                        # Отправляем сообщение с reply
                        result = await telethon_client(SendMessageRequest(
                            peer=entity,
                            message=comment,
                            reply_to=reply_to,
                            random_id=telethon_client._get_random_id()
                        ))

                        if result and hasattr(result, 'updates') and result.updates:
                            # Находим отправленное сообщение в результатах
                            for update in result.updates:
                                if hasattr(update, 'message') and hasattr(update.message, 'id'):
                                    print(f"✅ Telethon: Комментарий отправлен под пост! ID: {update.message.id}")
                                    return {
                                        "status": "success",
                                        "message": "Комментарий отправлен под пост канала",
                                        "message_id": update.message.id
                                    }

                        print(f"✅ Telethon: Комментарий отправлен под пост (без ID)")
                        return {
                            "status": "success",
                            "message": "Комментарий отправлен под пост канала"
                        }

                    except Exception as direct_error:
                        error_str = str(direct_error)
                        print(f"❌ Telethon: Прямая отправка не удалась: {error_str}")

                        # Метод 2: Стандартный reply если прямой метод не работает
                        try:
                            print(f"🔄 Telethon: Пробуем стандартный reply метод...")

                            await asyncio.sleep(2)  # Имитация человеческого поведения

                            sent_message = await telethon_client.send_message(
                                entity=entity,
                                message=comment,
                                reply_to=message_id
                            )

                            print(f"✅ Telethon: Стандартный метод сработал! ID: {sent_message.id}")
                            return {
                                "status": "success",
                                "message": "Комментарий отправлен под пост (стандартный метод)",
                                "message_id": sent_message.id
                            }

                        except Exception as standard_error:
                            error_str = str(standard_error)
                            print(f"❌ Telethon: Стандартный метод тоже не сработал: {error_str}")

                            if "CHAT_ADMIN_REQUIRED" in error_str:
                                return {"status": "error", "message": "Telethon: Требуются права администратора для комментариев"}
                            elif "MSG_ID_INVALID" in error_str:
                                return {"status": "error", "message": "Telethon: Неверный ID сообщения"}
                            elif "USER_BANNED_IN_CHANNEL" in error_str:
                                return {"status": "error", "message": "Telethon: Аккаунт заблокирован в канале"}
                            else:
                                return {"status": "error", "message": f"Telethon: Не удалось отправить комментарий: {error_str}"}

                except Exception as send_error:
                    error_str = str(send_error)
                    print(f"❌ Telethon: Ошибка отправки: {error_str}")

                    if "USERNAME_INVALID" in error_str:
                        return {"status": "error", "message": "Telethon: Неверное имя канала"}
                    elif "PEER_ID_INVALID" in error_str:
                        return {"status": "error", "message": "Telethon: Канал не найден"}
                    elif "USER_BANNED_IN_CHANNEL" in error_str:
                        return {"status": "error", "message": "Telethon: Аккаунт заблокирован"}
                    else:
                        return {"status": "error", "message": f"Telethon: {error_str}"}

                finally:
                    print(f"🔌 Telethon: Отключаемся от клиента...")
                    await telethon_client.disconnect()

                    # Удаляем временную сессию
                    try:
                        session_file_path = f"{telethon_session_file}.session"
                        if os.path.exists(session_file_path):
                            os.remove(session_file_path)
                            print(f"🗑️ Telethon: Временная сессия удалена")
                    except Exception as cleanup_error:
                        print(f"⚠️ Telethon: Ошибка очистки сессии: {cleanup_error}")

            finally:
                db.close()

        except Exception as e:
            print(f"❌ Telethon: Общая ошибка: {e}")
            import traceback
            print(f"🔍 Telethon: Стек ошибки: {traceback.format_exc()}")
            return {"status": "error", "message": f"Telethon: {str(e)}"}

    async def send_comment(self, account_id: int, chat_id: str, message_id: int, comment: str) -> Dict:
        """Отправка настоящего комментария под пост канала в секцию 'Leave a comment'"""
        try:
            print(f"💬 Отправляем комментарий в секцию 'Leave a comment' под постом канала...")

            client = await self._get_client_for_account(account_id)
            if not client:
                return {"status": "error", "message": "Не удалось подключиться к аккаунту"}

            if not client.is_connected:
                await client.connect()

            # Проверяем что client.me установлен
            if not hasattr(client, 'me') or client.me is None:
                try:
                    me = await client.get_me()
                    client.me = me
                except Exception:
                    from types import SimpleNamespace
                    client.me = SimpleNamespace(
                        id=account_id,
                        first_name="User",
                        is_premium=False,
                        is_verified=False,
                        is_bot=False
                    )

            print(f"📺 Работаем с каналом: {chat_id}, пост ID: {message_id}")

            try:
                # Используем специальный API для комментариев к постам канала
                from pyrogram.raw import functions, types

                # Получаем peer канала
                peer = await client.resolve_peer(chat_id)

                print(f"🎯 Отправляем комментарий через discussions API...")

                # Используем SendMessage для комментариев с правильными параметрами
                result = await client.invoke(
                    functions.messages.SendMessage(
                        peer=peer,
                        message=comment,
                        reply_to=types.InputReplyToMessage(
                            reply_to_msg_id=message_id,
                            top_msg_id=None,  # Не указываем top_msg_id для комментариев
                            reply_to_peer_id=peer  # Указываем peer для комментариев
                        ),
                        random_id=client.rnd_id(),
                        silent=False  # Комментарии обычно не тихие
                    )
                )

                if result and hasattr(result, 'updates') and result.updates:
                    for update in result.updates:
                        if hasattr(update, 'message') and hasattr(update.message, 'id'):
                            print(f"✅ Комментарий успешно добавлен в секцию 'Leave a comment'! ID: {update.message.id}")
                            return {
                                "status": "success",
                                "message": "Комментарий добавлен в секцию 'Leave a comment'",
                                "message_id": update.message.id
                            }

                # Если не получили ID, но ошибки нет - считаем успешным
                print(f"✅ Комментарий отправлен в секцию 'Leave a comment'")
                return {
                    "status": "success",
                    "message": "Комментарий добавлен в секцию 'Leave a comment'"
                }

            except Exception as api_error:
                error_str = str(api_error)
                print(f"❌ Ошибка API комментариев: {error_str}")

                # Если основной метод не работает, пробуем альтернативный через группу обсуждений
                if "CHAT_ADMIN_REQUIRED" in error_str or "PEER_ID_INVALID" in error_str:
                    print(f"🔄 Пробуем найти группу обсуждений для отправки комментария...")

                    try:
                        # Получаем информацию о канале
                        channel = await client.get_chat(chat_id)

                        # Ищем связанную группу обсуждений
                        discussion_group_id = None
                        if hasattr(channel, 'linked_chat') and channel.linked_chat:
                            discussion_group_id = channel.linked_chat.id
                            print(f"📢 Найдена группа обсуждений: {discussion_group_id}")
                        else:
                            # Альтернативный поиск через GetFullChannel
                            try:
                                from pyrogram.raw import functions
                                peer = await client.resolve_peer(chat_id)
                                full_channel = await client.invoke(
                                    functions.channels.GetFullChannel(channel=peer)
                                )

                                if hasattr(full_channel.full_chat, 'linked_chat_id') and full_channel.full_chat.linked_chat_id:
                                    discussion_group_id = -int(f"100{full_channel.full_chat.linked_chat_id}")
                                    print(f"📢 Найдена группа обсуждений (альтернативно): {discussion_group_id}")
                            except Exception as search_error:
                                print(f"❌ Поиск группы обсуждений не удался: {search_error}")

                        if discussion_group_id:
                            # Отправляем в группу обсуждений как настоящий комментарий
                            try:
                                await asyncio.sleep(1)

                                sent_message = await client.send_message(
                                    chat_id=discussion_group_id,
                                    text=comment,
                                    reply_to_message_id=message_id
                                )

                                print(f"✅ Комментарий отправлен в группу обсуждений (комментарии канала)")
                                return {
                                    "status": "success",
                                    "message": "Комментарий отправлен в обсуждения канала",
                                    "message_id": sent_message.id
                                }
                            except Exception as discussion_error:
                                print(f"❌ Ошибка отправки в группу обсуждений: {discussion_error}")

                        else:
                            return {"status": "error", "message": "У канала нет доступной секции комментариев"}

                    except Exception as channel_error:
                        print(f"❌ Ошибка получения информации о канале: {channel_error}")
                        return {"status": "error", "message": f"Ошибка доступа к каналу: {str(channel_error)}"}

                # Обрабатываем специфические ошибки
                if "USERNAME_INVALID" in error_str:
                    return {"status": "error", "message": f"Неверное имя канала: {chat_id}"}
                elif "MESSAGE_ID_INVALID" in error_str:
                    return {"status": "error", "message": f"Сообщение с ID {message_id} не найдено или недоступно"}
                elif "USER_BANNED_IN_CHANNEL" in error_str:
                    return {"status": "error", "message": "Аккаунт заблокирован в канале"}
                elif "COMMENTS_DISABLED" in error_str:
                    return {"status": "error", "message": "Комментарии отключены для этого поста"}
                else:
                    return {"status": "error", "message": f"Ошибка отправки комментария: {error_str}"}

        except Exception as e:
            print(f"❌ Общая ошибка отправки комментария: {e}")
            return {"status": "error", "message": f"Не удалось отправить комментарий: {str(e)}"}

    async def _send_comment_telethon_enhanced(self, account_id: int, chat_id: str, message_id: int, comment: str) -> Dict:
        """Отправка комментария через Telethon непосредственно под пост канала"""
        try:
            print(f"📱 Telethon: Начинаем отправку комментария под пост...")

            # Получаем данные аккаунта
            db = next(get_db())
            try:
                account = db.query(Account).filter(Account.id == account_id).first()
                if not account:
                    return {"status": "error", "message": "Telethon: Аккаунт не найден"}

                # Импортируем telethon только когда нужно
                try:
                    from telethon import TelegramClient
                    from telethon.tl.functions.messages import SendMessageRequest
                    from telethon.tl.types import InputReplyToMessage
                    print(f"✅ Telethon библиотека импортирована")
                except ImportError:
                    print(f"❌ Telethon не установлен")
                    return {"status": "error", "message": "Telethon не установлен"}

                # Определяем путь к файлу сессии для Telethon
                phone_clean = account.phone.replace('+', '').replace(' ', '').replace('(', '').replace(')', '').replace('-', '')
                pyrogram_session_file = os.path.join(SESSIONS_DIR, f"session_{phone_clean}.session")
                telethon_session_file = os.path.join(SESSIONS_DIR, f"telethon_{phone_clean}")

                # Создаем/проверяем сессию для Telethon
                session_file_path = f"{telethon_session_file}.session"
                if not os.path.exists(session_file_path):
                    try:
                        print(f"🔄 Telethon: Создаем совместимую сессию...")
                        await self._convert_pyrogram_to_telethon_session(pyrogram_session_file, telethon_session_file)
                    except Exception as convert_error:
                        print(f"❌ Telethon: Ошибка конвертации: {convert_error}")
                        return {"status": "error", "message": "Telethon: Не удалось создать сессию"}

                # Создаем Telethon клиент
                try:
                    telethon_client = TelegramClient(telethon_session_file, API_ID, API_HASH)
                    await telethon_client.start()

                    me = await telethon_client.get_me()
                    print(f"✅ Telethon: Авторизован как {me.first_name} ({me.phone})")

                except Exception as client_error:
                    print(f"❌ Telethon: Ошибка создания клиента: {client_error}")
                    return {"status": "error", "message": "Telethon: Ошибка подключения"}

                try:
                    # Нормализуем chat_id для Telethon
                    if chat_id.startswith('@'):
                        target_entity = chat_id
                    elif chat_id.isdigit() or (chat_id.startswith('-') and chat_id[1:].isdigit()):
                        target_entity = int(chat_id)
                    else:
                        target_entity = chat_id

                    print(f"📍 Telethon: Работаем с каналом {target_entity}")

                    # Получаем информацию о целевом канале
                    entity = await telethon_client.get_entity(target_entity)
                    print(f"📍 Telethon: Получена сущность - {type(entity).__name__}")

                    # Метод 1: Отправляем комментарий прямо под пост используя InputReplyToMessage
                    try:
                        print(f"🎯 Telethon: Отправляем комментарий прямо под пост {message_id}...")

                        # Создаем reply для комментария под постом
                        reply_to = InputReplyToMessage(
                            reply_to_msg_id=message_id,
                            top_msg_id=message_id  # Указываем что это комментарий к топ-сообщению
                        )

                        # Отправляем сообщение с reply
                        result = await telethon_client(SendMessageRequest(
                            peer=entity,
                            message=comment,
                            reply_to=reply_to,
                            random_id=telethon_client._get_random_id()
                        ))

                        if result and hasattr(result, 'updates') and result.updates:
                            # Находим отправленное сообщение в результатах
                            for update in result.updates:
                                if hasattr(update, 'message') and hasattr(update.message, 'id'):
                                    print(f"✅ Telethon: Комментарий отправлен под пост! ID: {update.message.id}")
                                    return {
                                        "status": "success",
                                        "message": "Комментарий отправлен под пост канала",
                                        "message_id": update.message.id
                                    }

                        print(f"✅ Telethon: Комментарий отправлен под пост (без ID)")
                        return {
                            "status": "success",
                            "message": "Комментарий отправлен под пост канала"
                        }

                    except Exception as direct_error:
                        error_str = str(direct_error)
                        print(f"❌ Telethon: Прямая отправка не удалась: {error_str}")

                        # Метод 2: Стандартный reply если прямой метод не работает
                        try:
                            print(f"🔄 Telethon: Пробуем стандартный reply метод...")

                            await asyncio.sleep(2)  # Имитация человеческого поведения

                            sent_message = await telethon_client.send_message(
                                entity=entity,
                                message=comment,
                                reply_to=message_id
                            )

                            print(f"✅ Telethon: Стандартный метод сработал! ID: {sent_message.id}")
                            return {
                                "status": "success",
                                "message": "Комментарий отправлен под пост (стандартный метод)",
                                "message_id": sent_message.id
                            }

                        except Exception as standard_error:
                            error_str = str(standard_error)
                            print(f"❌ Telethon: Стандартный метод тоже не сработал: {error_str}")

                            if "CHAT_ADMIN_REQUIRED" in error_str:
                                return {"status": "error", "message": "Telethon: Требуются права администратора для комментариев"}
                            elif "MSG_ID_INVALID" in error_str:
                                return {"status": "error", "message": "Telethon: Неверный ID сообщения"}
                            elif "USER_BANNED_IN_CHANNEL" in error_str:
                                return {"status": "error", "message": "Telethon: Аккаунт заблокирован в канале"}
                            else:
                                return {"status": "error", "message": f"Telethon: Не удалось отправить комментарий: {error_str}"}

                except Exception as send_error:
                    error_str = str(send_error)
                    print(f"❌ Telethon: Ошибка отправки: {error_str}")

                    if "USERNAME_INVALID" in error_str:
                        return {"status": "error", "message": "Telethon: Неверное имя канала"}
                    elif "PEER_ID_INVALID" in error_str:
                        return {"status": "error", "message": "Telethon: Канал не найден"}
                    elif "USER_BANNED_IN_CHANNEL" in error_str:
                        return {"status": "error", "message": "Telethon: Аккаунт заблокирован"}
                    else:
                        return {"status": "error", "message": f"Telethon: {error_str}"}

                finally:
                    print(f"🔌 Telethon: Отключаемся от клиента...")
                    await telethon_client.disconnect()

                    # Удаляем временную сессию
                    try:
                        session_file_path = f"{telethon_session_file}.session"
                        if os.path.exists(session_file_path):
                            os.remove(session_file_path)
                            print(f"🗑️ Telethon: Временная сессия удалена")
                    except Exception as cleanup_error:
                        print(f"⚠️ Telethon: Ошибка очистки сессии: {cleanup_error}")

            finally:
                db.close()

        except Exception as e:
            print(f"❌ Telethon: Общая ошибка: {e}")
            import traceback
            print(f"🔍 Telethon: Стек ошибки: {traceback.format_exc()}")
            return {"status": "error", "message": f"Telethon: {str(e)}"}

    async def _send_comment_pyrogram_enhanced(self, account_id: int, chat_id: str, message_id: int, comment: str) -> Dict:
        """Отправка комментария через Pyrogram с улучшенной логикой"""
        try:
            client = await self._get_client_for_account(account_id)
            if not client:
                return {"status": "error", "message": "Не удалось подключиться к аккаунту"}

            if not client.is_connected:
                await client.connect()

            # Проверяем что client.me установлен
            if not hasattr(client, 'me') or client.me is None:
                try:
                    me = await client.get_me()
                    client.me = me
                except Exception:
                    # Создаем заглушку если не удается получить информацию
                    from types import SimpleNamespace
                    client.me = SimpleNamespace(
                        id=account_id,
                        first_name="User",
                        is_premium=False,
                        is_verified=False,
                        is_bot=False
                    )

            print(f"🔄 Отправка комментария от аккаунта {account_id} в чат {chat_id}, к сообщению {message_id}")
            print(f"📝 Комментарий: {comment}")

            # Нормализуем chat_id
            target_chat = chat_id
            if isinstance(chat_id, str):
                if chat_id.startswith('@'):
                    target_chat = chat_id
                elif chat_id.isdigit():
                    target_chat = int(chat_id)
                elif chat_id.startswith('-') and chat_id[1:].isdigit():
                    target_chat = int(chat_id)

            # Метод 1: Попытка отправки через reply_to_message_id (имитация действий пользователя)
            try:
                print(f"🎯 Попытка отправки комментария как ответ на сообщение...")

                # Добавляем небольшую задержку для имитации человеческого поведения
                await asyncio.sleep(1)

                sent_message = await client.send_message(
                    chat_id=target_chat,
                    text=comment,
                    reply_to_message_id=message_id,
                    disable_notification=False  # Показываем что это активное действие
                )

                if sent_message and hasattr(sent_message, 'id'):
                    print(f"✅ Комментарий отправлен как ответ аккаунтом {account_id}")
                    return {
                        "status": "success",
                        "message": "Комментарий отправлен под пост",
                        "message_id": sent_message.id
                    }

            except Exception as reply_error:
                error_str = str(reply_error)
                print(f"❌ Ошибка отправки ответа: {error_str}")

                # Метод 2: Если ответ не работает, пробуем найти группу обсуждений
                if "CHAT_ADMIN_REQUIRED" in error_str or "CHAT_WRITE_FORBIDDEN" in error_str:
                    print(f"🔄 Пробуем найти группу обсуждений для канала {chat_id}")

                    try:
                        # Ищем группу обсуждений канала
                        channel = await client.get_chat(chat_id)
                        discussion_group_id = None

                        if hasattr(channel, 'linked_chat') and channel.linked_chat:
                            discussion_group_id = channel.linked_chat.id
                            print(f"📢 Найдена группа обсуждений: {discussion_group_id}")
                        else:
                            # Альтернативный способ поиска группы обсуждений
                            try:
                                from pyrogram.raw import functions
                                peer = await client.resolve_peer(target_chat)
                                full_channel = await client.invoke(
                                    functions.channels.GetFullChannel(channel=peer)
                                )

                                if hasattr(full_channel.full_chat, 'linked_chat_id') and full_channel.full_chat.linked_chat_id:
                                    discussion_group_id = -int(f"100{full_channel.full_chat.linked_chat_id}")
                                    print(f"📢 Альтернативно найдена группа обсуждений: {discussion_group_id}")
                            except Exception as alt_search_error:
                                print(f"❌ Альтернативный поиск не удался: {alt_search_error}")

                        if discussion_group_id:
                            # Отправляем в группу обсуждений
                            try:
                                await asyncio.sleep(1)  # Имитация человеческого поведения

                                sent_message = await client.send_message(
                                    chat_id=discussion_group_id,
                                    text=comment,
                                    reply_to_message_id=message_id
                                )

                                print(f"✅ Комментарий отправлен в группу обсуждений аккаунтом {account_id}")
                                return {
                                    "status": "success",
                                    "message": "Комментарий отправлен в группу обсуждений канала",
                                    "message_id": sent_message.id
                                }
                            except Exception as discussion_error:
                                print(f"❌ Ошибка отправки в группу обсуждений: {discussion_error}")

                        # Если группа обсуждений не найдена, вернем ошибку для попытки Telethon
                        return {
                            "status": "error",
                            "message": f"Pyrogram: У канала {chat_id} нет доступной группы обсуждений"
                        }

                    except Exception as channel_error:
                        print(f"❌ Ошибка поиска группы обсуждений: {channel_error}")
                        return {
                            "status": "error",
                            "message": f"Pyrogram: Для отправки комментариев в {chat_id} требуются права администратора"
                        }

                # Обрабатываем специфические ошибки Telegram
                if "USERNAME_INVALID" in error_str:
                    return {"status": "error", "message": f"Неверное имя пользователя или канала: {chat_id}"}
                elif "PEER_ID_INVALID" in error_str:
                    return {"status": "error", "message": f"Канал/чат {chat_id} не найден или недоступен"}
                elif "MESSAGE_ID_INVALID" in error_str:
                    return {"status": "error", "message": f"Сообщение с ID {message_id} не найдено или недоступно"}
                elif "USER_BANNED_IN_CHANNEL" in error_str:
                    return {"status": "error", "message": "Аккаунт заблокирован в этом канале"}
                elif "REPLY_MESSAGE_INVALID" in error_str:
                    return {"status": "error", "message": "Нельзя ответить на это сообщение"}
                elif "COMMENTS_DISABLED" in error_str:
                    return {"status": "error", "message": "Комментарии отключены для этого поста"}
                else:
                    return {"status": "error", "message": f"Pyrogram ошибка: {error_str}"}

        except Exception as e:
            print(f"❌ Ошибка Pyrogram комментария: {e}")
            return {"status": "error", "message": f"Pyrogram ошибка: {str(e)}"}

    async def _convert_pyrogram_to_telethon_session(self, pyrogram_path: str, telethon_path: str):
        """Конвертация сессии Pyrogram в формат Telethon с полной совместимостью"""
        try:
            import sqlite3
            import shutil

            print(f"🔄 Создаем полностью новую Telethon сессию")

            # Создаем новую базу данных для Telethon с нуля
            telethon_session_file = f"{telethon_path}.session"

            # Удаляем старый файл если существует
            if os.path.exists(telethon_session_file):
                os.remove(telethon_session_file)

            # Читаем данные из Pyrogram сессии для получения auth_key
            pyrogram_conn = sqlite3.connect(pyrogram_path)
            pyrogram_cursor = pyrogram_conn.cursor()

            try:
                # Сначала проверяем структуру таблицы sessions в Pyrogram
                pyrogram_cursor.execute("PRAGMA table_info(sessions)")
                columns_info = pyrogram_cursor.fetchall()
                column_names = [col[1] for col in columns_info]
                print(f"📋 Структура таблицы Pyrogram sessions: {column_names}")

                # Получаем данные сессии из Pyrogram с правильными полями
                if 'server_address' in column_names:
                    query = "SELECT dc_id, server_address, port, auth_key, user_id FROM sessions LIMIT 1"
                    pyrogram_cursor.execute(query)
                    session_data = pyrogram_cursor.fetchone()
                    if session_data:
                        dc_id, server_address, port, auth_key, user_id = session_data
                    else:
                        raise Exception("Не найдены данные сессии в Pyrogram файле")
                else:
                    # Альтернативный способ чтения с базовыми полями
                    query = "SELECT dc_id, auth_key FROM sessions LIMIT 1"
                    pyrogram_cursor.execute(query)
                    session_data = pyrogram_cursor.fetchone()
                    if session_data:
                        dc_id, auth_key = session_data
                        # Используем стандартные значения для отсутствующих полей
                        server_address = "149.154.167.51" if dc_id == 2 else "149.154.175.53"
                        port = 443
                        user_id = 0
                    else:
                        raise Exception("Не найдены данные сессии в Pyrogram файле")

                print(f"📋 Получены данные сессии: DC{dc_id}, Server: {server_address}:{port}")

            finally:
                pyrogram_conn.close()

            # Создаем новую базу данных для Telethon
            conn = sqlite3.connect(telethon_session_file)
            cursor = conn.cursor()

            try:
                # Создаем правильную структуру для Telethon с проверкой существования таблиц
                print("🔨 Создаем структуру базы данных Telethon...")

                # Проверяем какие таблицы уже существуют
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                existing_tables = [row[0] for row in cursor.fetchall()]
                print(f"📋 Существующие таблицы: {existing_tables}")

                # Проверяем какие таблицы нужно создать
                print(f"📋 Проверяем существующие таблицы: {existing_tables}")

                # Таблица version (обязательная для Telethon)
                cursor.execute("CREATE TABLE IF NOT EXISTS version (version INTEGER)")
                cursor.execute("INSERT INTO version VALUES (1)")
                print("✅ Создана таблица version")

                # Таблица sessions (основная таблица с данными авторизации)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS sessions (
                        dc_id INTEGER PRIMARY KEY,
                        server_address TEXT,
                        port INTEGER,
                        auth_key BLOB,
                        takeout_id INTEGER
                    )
                """)
                print("✅ Создана таблица sessions")

                # Вставляем данные сессии
                cursor.execute("""
                    INSERT OR REPLACE INTO sessions (dc_id, server_address, port, auth_key, takeout_id)
                    VALUES (?, ?, ?, ?, NULL)
                """, (dc_id, server_address, port, auth_key))
                print("✅ Данные авторизации добавлены в таблицу sessions")

                # Таблица entities (для кеша пользователей/чатов)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS entities (
                        id INTEGER PRIMARY KEY,
                        hash INTEGER NOT NULL,
                        username TEXT,
                        phone INTEGER,
                        name TEXT,
                        date INTEGER
                    )
                """)
                print("✅ Создана таблица entities")

                # Таблица sent_files (для кеша отправленных файлов)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS sent_files (
                        md5_digest BLOB,
                        file_size INTEGER,
                        type INTEGER,
                        id INTEGER,
                        hash INTEGER,
                        PRIMARY KEY(md5_digest, file_size, type)
                    )
                """)
                print("✅ Создана таблица sent_files")

                # НЕ создаем update_state - Telethon создает её сам при необходимости
                print("⚠️ Таблица update_state не создается - Telethon управляет ею сам")

                conn.commit()
                print("✅ Сессия успешно создана для Telethon с полной совместимостью")

            finally:
                conn.close()

        except Exception as e:
            print(f"❌ Ошибка создания Telethon сессии: {e}")
            # Если не удалось создать сессию, удаляем поврежденный файл
            try:
                if os.path.exists(f"{telethon_path}.session"):
                    os.remove(f"{telethon_path}.session")
            except:
                pass
            raise e

    async def auto_delete_after_campaign(self, campaign_id: int, delay_seconds: int = 5) -> Dict:
        """Автоматическое удаление аккаунтов после завершения кампании"""
        try:
            print(f"⏰ Запланировано автоудаление аккаунтов через {delay_seconds} секунд после кампании {campaign_id}")

            # Ждем указанное время
            await asyncio.sleep(delay_seconds)

            # Получаем аккаунты, участвовавшие в кампании
            db = next(get_db())
            try:
                # Находим все логи отправки для этой кампании
                send_logs = db.query(SendLog).filter(SendLog.campaign_id == campaign_id).all()
                account_ids = list(set(log.account_id for log in send_logs))

                if not account_ids:
                    print(f"⚠️ Не найдено аккаунтов для удаления в кампании {campaign_id}")
                    return {"status": "error", "message": "Не найдено аккаунтов для удаления"}

                print(f"🗑️ Начинаем автоудаление {len(account_ids)} аккаунтов")

                deleted_accounts = []
                failed_deletions = []

                for account_id in account_ids:
                    print(f"🔄 Удаляем аккаунт {account_id}...")

                    result = await self.delete_telegram_account(
                        account_id,
                        reason="Автоматическое удаление после рассылки"
                    )

                    if result["status"] == "success":
                        deleted_accounts.append(result.get("deleted_account", f"Account {account_id}"))
                        print(f"✅ Аккаунт {account_id} удален")
                    else:
                        failed_deletions.append(f"Account {account_id}: {result['message']}")
                        print(f"❌ Не удалось удалить аккаунт {account_id}: {result['message']}")

                    # Небольшая задержка между удалениями
                    await asyncio.sleep(2)

                return {
                    "status": "success",
                    "message": f"Автоудаление завершено. Удалено: {len(deleted_accounts)}, ошибок: {len(failed_deletions)}",
                    "deleted_accounts": deleted_accounts,
                    "failed_deletions": failed_deletions
                }

            finally:
                db.close()

        except Exception as e:
            print(f"❌ Ошибка автоудаления после кампании {campaign_id}: {e}")
            return {"status": "error", "message": f"Ошибка автоудаления: {str(e)}"}

    async def get_client(self, account_id: int) -> Optional[Client]:
        """Вспомогательная функция для получения клиента (переименована для соответствия изменениям)"""
        return await self._get_client_for_account(account_id)

    async def update_profile(self, account_id: int, first_name: str = None, last_name: str = None, bio: str = None, profile_photo_path: str = None) -> Dict:
        """Обновление профиля аккаунта в Telegram"""
        try:
            print(f"🔄 Обновление профиля аккаунта {account_id}")
            print(f"📝 Данные: имя={first_name}, фамилия={last_name}, био={bio}")

            client = await self._get_client_for_account(account_id)
            if not client:
                return {"status": "error", "message": "Не удалось подключиться к аккаунту"}

            if not client.is_connected:
                await client.connect()

            # Получаем текущую информацию о пользователе
            try:
                me = await client.get_me()
                print(f"👤 Текущий профиль: {me.first_name} {me.last_name or ''}")
            except Exception as me_error:
                print(f"⚠️ Не удалось получить текущую информацию: {me_error}")
                me = None

            # Обновляем текстовые данные профиля
            update_success = False
            try:
                # Убираем пустые значения и используем разумные ограничения
                first_name_clean = (first_name or "").strip()[:64] if first_name else ""
                last_name_clean = (last_name or "").strip()[:64] if last_name else ""
                bio_clean = (bio or "").strip()[:70] if bio else ""  # Telegram ограничивает био до 70 символов

                if not first_name_clean:
                    first_name_clean = "User"  # Telegram требует непустое имя

                print(f"🔄 Отправляем обновление профиля...")
                print(f"   Имя: '{first_name_clean}'")
                print(f"   Фамилия: '{last_name_clean}'")
                print(f"   Био: '{bio_clean}'")

                await client.update_profile(
                    first_name=first_name_clean,
                    last_name=last_name_clean,
                    bio=bio_clean
                )

                print(f"✅ Профиль успешно обновлен в Telegram")
                update_success = True

            except Exception as profile_error:
                error_str = str(profile_error).lower()
                print(f"❌ Ошибка обновления профиля: {profile_error}")

                # Обработка специфических ошибок Telegram
                if "firstname_invalid" in error_str:
                    return {"status": "error", "message": "Неверный формат имени. Используйте только буквы и пробелы"}
                elif "about_too_long" in error_str:
                    return {"status": "error", "message": "Описание слишком длинное (максимум 70 символов)"}
                elif "flood" in error_str:
                    return {"status": "error", "message": "Слишком частые изменения профиля. Попробуйте позже"}
                else:
                    return {"status": "error", "message": f"Ошибка обновления профиля: {str(profile_error)}"}

            # Обновляем фото профиля если предоставлено
            photo_success = True
            if profile_photo_path and os.path.exists(profile_photo_path):
                try:
                    print(f"🖼️ Обновляем фото профиля: {profile_photo_path}")
                    await client.set_profile_photo(photo=profile_photo_path)
                    print(f"✅ Фото профиля обновлено")
                except Exception as photo_error:
                    print(f"❌ Ошибка обновления фото профиля: {photo_error}")
                    photo_success = False

            # Проверяем результат обновления
            try:
                await asyncio.sleep(1)  # Даем время на синхронизацию
                updated_me = await client.get_me()
                print(f"🔍 Проверка обновления: {updated_me.first_name} {updated_me.last_name or ''}")

                if update_success:
                    if profile_photo_path and not photo_success:
                        return {"status": "success", "message": "Профиль обновлен, но не удалось установить фото"}
                    else:
                        return {"status": "success", "message": "Профиль успешно обновлен в Telegram"}
                else:
                    return {"status": "error", "message": "Не удалось обновить профиль"}

            except Exception as check_error:
                print(f"⚠️ Не удалось проверить обновление: {check_error}")
                if update_success:
                    return {"status": "success", "message": "Профиль вероятно обновлен (не удалось проверить)"}
                else:
                    return {"status": "error", "message": "Ошибка при проверке обновления профиля"}

        except Exception as e:
            print(f"❌ Общая ошибка обновления профиля: {e}")
            return {"status": "error", "message": f"Общая ошибка: {str(e)}"}

    async def send_reaction(self, account_id: int, chat_id: str, message_id: int, emoji: str) -> Dict:
        """Отправка реакции на сообщение"""
        try:
            client = await self._get_client_for_account(account_id)
            if not client:
                return {"status": "error", "message": "Не удалось подключиться к аккаунту"}

            if not client.is_connected:
                await client.connect()

            # Отправляем реакцию
            from pyrogram.raw import functions
            from pyrogram.raw.types import ReactionEmoji

            await client.invoke(
                functions.messages.SendReaction(
                    peer=await client.resolve_peer(chat_id),
                    msg_id=message_id,
                    reaction=[ReactionEmoji(emoticon=emoji)]
                )
            )

            return {"status": "success", "message": "Реакция отправлена"}

        except Exception as e:
            return {"status": "error", "message": f"Ошибка отправки реакции: {str(e)}"}

    async def view_message(self, account_id: int, chat_id: str, message_id: int) -> Dict:
        """Просмотр сообщения"""
        try:
            client = await self._get_client_for_account(account_id)
            if not client:
                return {"status": "error", "message": "Не удалось подключиться к аккаунту"}

            if not client.is_connected:
                await client.connect()

            # Читаем историю чата до указанного сообщения
            await client.read_chat_history(chat_id=chat_id, max_id=message_id)

            return {"status": "success", "message": "Сообщение просмотрено"}

        except Exception as e:
            return {"status": "error", "message": f"Ошибка просмотра сообщения: {str(e)}"}

    async def send_comment(self, account_id: int, chat_id: str, message_id: int, comment: str) -> Dict:
        """Отправка настоящего комментария под пост канала в секцию 'Leave a comment'"""
        try:
            print(f"💬 Отправляем комментарий в секцию 'Leave a comment' под постом канала...")

            client = await self._get_client_for_account(account_id)
            if not client:
                return {"status": "error", "message": "Не удалось подключиться к аккаунту"}

            if not client.is_connected:
                await client.connect()

            # Проверяем что client.me установлен
            if not hasattr(client, 'me') or client.me is None:
                try:
                    me = await client.get_me()
                    client.me = me
                except Exception:
                    from types import SimpleNamespace
                    client.me = SimpleNamespace(
                        id=account_id,
                        first_name="User",
                        is_premium=False,
                        is_verified=False,
                        is_bot=False
                    )

            print(f"📺 Работаем с каналом: {chat_id}, пост ID: {message_id}")

            try:
                # Используем специальный API для комментариев к постам канала
                from pyrogram.raw import functions, types

                # Получаем peer канала
                peer = await client.resolve_peer(chat_id)

                print(f"🎯 Отправляем комментарий через discussions API...")

                # Используем SendMessage для комментариев с правильными параметрами
                result = await client.invoke(
                    functions.messages.SendMessage(
                        peer=peer,
                        message=comment,
                        reply_to=types.InputReplyToMessage(
                            reply_to_msg_id=message_id,
                            top_msg_id=None,  # Не указываем top_msg_id для комментариев
                            reply_to_peer_id=peer  # Указываем peer для комментариев
                        ),
                        random_id=client.rnd_id(),
                        silent=False  # Комментарии обычно не тихие
                    )
                )

                if result and hasattr(result, 'updates') and result.updates:
                    for update in result.updates:
                        if hasattr(update, 'message') and hasattr(update.message, 'id'):
                            print(f"✅ Комментарий успешно добавлен в секцию 'Leave a comment'! ID: {update.message.id}")
                            return {
                                "status": "success",
                                "message": "Комментарий добавлен в секцию 'Leave a comment'",
                                "message_id": update.message.id
                            }

                # Если не получили ID, но ошибки нет - считаем успешным
                print(f"✅ Комментарий отправлен в секцию 'Leave a comment'")
                return {
                    "status": "success",
                    "message": "Комментарий добавлен в секцию 'Leave a comment'"
                }

            except Exception as api_error:
                error_str = str(api_error)
                print(f"❌ Ошибка API комментариев: {error_str}")

                # Если основной метод не работает, пробуем альтернативный через группу обсуждений
                if "CHAT_ADMIN_REQUIRED" in error_str or "PEER_ID_INVALID" in error_str:
                    print(f"🔄 Пробуем найти группу обсуждений для отправки комментария...")

                    try:
                        # Получаем информацию о канале
                        channel = await client.get_chat(chat_id)

                        # Ищем связанную группу обсуждений
                        discussion_group_id = None
                        if hasattr(channel, 'linked_chat') and channel.linked_chat:
                            discussion_group_id = channel.linked_chat.id
                            print(f"📢 Найдена группа обсуждений: {discussion_group_id}")
                        else:
                            # Альтернативный поиск через GetFullChannel
                            try:
                                from pyrogram.raw import functions
                                peer = await client.resolve_peer(chat_id)
                                full_channel = await client.invoke(
                                    functions.channels.GetFullChannel(channel=peer)
                                )

                                if hasattr(full_channel.full_chat, 'linked_chat_id') and full_channel.full_chat.linked_chat_id:
                                    discussion_group_id = -int(f"100{full_channel.full_chat.linked_chat_id}")
                                    print(f"📢 Найдена группа обсуждений (альтернативно): {discussion_group_id}")
                            except Exception as search_error:
                                print(f"❌ Поиск группы обсуждений не удался: {search_error}")

                        if discussion_group_id:
                            # Отправляем в группу обсуждений как настоящий комментарий
                            try:
                                await asyncio.sleep(1)

                                sent_message = await client.send_message(
                                    chat_id=discussion_group_id,
                                    text=comment,
                                    reply_to_message_id=message_id
                                )

                                print(f"✅ Комментарий отправлен в группу обсуждений (комментарии канала)")
                                return {
                                    "status": "success",
                                    "message": "Комментарий отправлен в обсуждения канала",
                                    "message_id": sent_message.id
                                }
                            except Exception as discussion_error:
                                print(f"❌ Ошибка отправки в группу обсуждений: {discussion_error}")

                        else:
                            return {"status": "error", "message": "У канала нет доступной секции комментариев"}

                    except Exception as channel_error:
                        print(f"❌ Ошибка получения информации о канале: {channel_error}")
                        return {"status": "error", "message": f"Ошибка доступа к каналу: {str(channel_error)}"}

                # Обрабатываем специфические ошибки
                if "USERNAME_INVALID" in error_str:
                    return {"status": "error", "message": f"Неверное имя канала: {chat_id}"}
                elif "MESSAGE_ID_INVALID" in error_str:
                    return {"status": "error", "message": f"Сообщение с ID {message_id} не найдено или недоступно"}
                elif "USER_BANNED_IN_CHANNEL" in error_str:
                    return {"status": "error", "message": "Аккаунт заблокирован в канале"}
                elif "COMMENTS_DISABLED" in error_str:
                    return {"status": "error", "message": "Комментарии отключены для этого поста"}
                else:
                    return {"status": "error", "message": f"Ошибка отправки комментария: {error_str}"}

        except Exception as e:
            print(f"❌ Общая ошибка отправки комментария: {e}")
            return {"status": "error", "message": f"Не удалось отправить комментарий: {str(e)}"}

    async def _send_comment_telethon_enhanced(self, account_id: int, chat_id: str, message_id: int, comment: str) -> Dict:
        """Отправка комментария через Telethon непосредственно под пост канала"""
        try:
            print(f"📱 Telethon: Начинаем отправку комментария под пост...")

            # Получаем данные аккаунта
            db = next(get_db())
            try:
                account = db.query(Account).filter(Account.id == account_id).first()
                if not account:
                    return {"status": "error", "message": "Telethon: Аккаунт не найден"}

                # Импортируем telethon только когда нужно
                try:
                    from telethon import TelegramClient
                    from telethon.tl.functions.messages import SendMessageRequest
                    from telethon.tl.types import InputReplyToMessage
                    print(f"✅ Telethon библиотека импортирована")
                except ImportError:
                    print(f"❌ Telethon не установлен")
                    return {"status": "error", "message": "Telethon не установлен"}

                # Определяем путь к файлу сессии для Telethon
                phone_clean = account.phone.replace('+', '').replace(' ', '').replace('(', '').replace(')', '').replace('-', '')
                pyrogram_session_file = os.path.join(SESSIONS_DIR, f"session_{phone_clean}.session")
                telethon_session_file = os.path.join(SESSIONS_DIR, f"telethon_{phone_clean}")

                # Создаем/проверяем сессию для Telethon
                session_file_path = f"{telethon_session_file}.session"
                if not os.path.exists(session_file_path):
                    try:
                        print(f"🔄 Telethon: Создаем совместимую сессию...")
                        await self._convert_pyrogram_to_telethon_session(pyrogram_session_file, telethon_session_file)
                    except Exception as convert_error:
                        print(f"❌ Telethon: Ошибка конвертации: {convert_error}")
                        return {"status": "error", "message": "Telethon: Не удалось создать сессию"}

                # Создаем Telethon клиент
                try:
                    telethon_client = TelegramClient(telethon_session_file, API_ID, API_HASH)
                    await telethon_client.start()

                    me = await telethon_client.get_me()
                    print(f"✅ Telethon: Авторизован как {me.first_name} ({me.phone})")

                except Exception as client_error:
                    print(f"❌ Telethon: Ошибка создания клиента: {client_error}")
                    return {"status": "error", "message": "Telethon: Ошибка подключения"}

                try:
                    # Нормализуем chat_id для Telethon
                    if chat_id.startswith('@'):
                        target_entity = chat_id
                    elif chat_id.isdigit() or (chat_id.startswith('-') and chat_id[1:].isdigit()):
                        target_entity = int(chat_id)
                    else:
                        target_entity = chat_id

                    print(f"📍 Telethon: Работаем с каналом {target_entity}")

                    # Получаем информацию о целевом канале
                    entity = await telethon_client.get_entity(target_entity)
                    print(f"📍 Telethon: Получена сущность - {type(entity).__name__}")

                    # Метод 1: Отправляем комментарий прямо под пост используя InputReplyToMessage
                    try:
                        print(f"🎯 Telethon: Отправляем комментарий прямо под пост {message_id}...")

                        # Создаем reply для комментария под постом
                        reply_to = InputReplyToMessage(
                            reply_to_msg_id=message_id,
                            top_msg_id=message_id  # Указываем что это комментарий к топ-сообщению
                        )

                        # Отправляем сообщение с reply
                        result = await telethon_client(SendMessageRequest(
                            peer=entity,
                            message=comment,
                            reply_to=reply_to,
                            random_id=telethon_client._get_random_id()
                        ))

                        if result and hasattr(result, 'updates') and result.updates:
                            # Находим отправленное сообщение в результатах
                            for update in result.updates:
                                if hasattr(update, 'message') and hasattr(update.message, 'id'):
                                    print(f"✅ Telethon: Комментарий отправлен под пост! ID: {update.message.id}")
                                    return {
                                        "status": "success",
                                        "message": "Комментарий отправлен под пост канала",
                                        "message_id": update.message.id
                                    }

                        print(f"✅ Telethon: Комментарий отправлен под пост (без ID)")
                        return {
                            "status": "success",
                            "message": "Комментарий отправлен под пост канала"
                        }

                    except Exception as direct_error:
                        error_str = str(direct_error)
                        print(f"❌ Telethon: Прямая отправка не удалась: {error_str}")

                        # Метод 2: Стандартный reply если прямой метод не работает
                        try:
                            print(f"🔄 Telethon: Пробуем стандартный reply метод...")

                            await asyncio.sleep(2)  # Имитация человеческого поведения

                            sent_message = await telethon_client.send_message(
                                entity=entity,
                                message=comment,
                                reply_to=message_id
                            )

                            print(f"✅ Telethon: Стандартный метод сработал! ID: {sent_message.id}")
                            return {
                                "status": "success",
                                "message": "Комментарий отправлен под пост (стандартный метод)",
                                "message_id": sent_message.id
                            }

                        except Exception as standard_error:
                            error_str = str(standard_error)
                            print(f"❌ Telethon: Стандартный метод тоже не сработал: {error_str}")

                            if "CHAT_ADMIN_REQUIRED" in error_str:
                                return {"status": "error", "message": "Telethon: Требуются права администратора для комментариев"}
                            elif "MSG_ID_INVALID" in error_str:
                                return {"status": "error", "message": "Telethon: Неверный ID сообщения"}
                            elif "USER_BANNED_IN_CHANNEL" in error_str:
                                return {"status": "error", "message": "Telethon: Аккаунт заблокирован в канале"}
                            else:
                                return {"status": "error", "message": f"Telethon: Не удалось отправить комментарий: {error_str}"}

                except Exception as send_error:
                    error_str = str(send_error)
                    print(f"❌ Telethon: Ошибка отправки: {error_str}")

                    if "USERNAME_INVALID" in error_str:
                        return {"status": "error", "message": "Telethon: Неверное имя канала"}
                    elif "PEER_ID_INVALID" in error_str:
                        return {"status": "error", "message": "Telethon: Канал не найден"}
                    elif "USER_BANNED_IN_CHANNEL" in error_str:
                        return {"status": "error", "message": "Telethon: Аккаунт заблокирован"}
                    else:
                        return {"status": "error", "message": f"Telethon: {error_str}"}

                finally:
                    print(f"🔌 Telethon: Отключаемся от клиента...")
                    await telethon_client.disconnect()

                    # Удаляем временную сессию
                    try:
                        session_file_path = f"{telethon_session_file}.session"
                        if os.path.exists(session_file_path):
                            os.remove(session_file_path)
                            print(f"🗑️ Telethon: Временная сессия удалена")
                    except Exception as cleanup_error:
                        print(f"⚠️ Telethon: Ошибка очистки сессии: {cleanup_error}")

            finally:
                db.close()

        except Exception as e:
            print(f"❌ Telethon: Общая ошибка: {e}")
            import traceback
            print(f"🔍 Telethon: Стек ошибки: {traceback.format_exc()}")
            return {"status": "error", "message": f"Telethon: {str(e)}"}

    async def _send_comment_pyrogram_enhanced(self, account_id: int, chat_id: str, message_id: int, comment: str) -> Dict:
        """Отправка комментария через Pyrogram с улучшенной логикой"""
        try:
            client = await self._get_client_for_account(account_id)
            if not client:
                return {"status": "error", "message": "Не удалось подключиться к аккаунту"}

            if not client.is_connected:
                await client.connect()

            # Проверяем что client.me установлен
            if not hasattr(client, 'me') or client.me is None:
                try:
                    me = await client.get_me()
                    client.me = me
                except Exception:
                    # Создаем заглушку если не удается получить информацию
                    from types import SimpleNamespace
                    client.me = SimpleNamespace(
                        id=account_id,
                        first_name="User",
                        is_premium=False,
                        is_verified=False,
                        is_bot=False
                    )

            print(f"🔄 Отправка комментария от аккаунта {account_id} в чат {chat_id}, к сообщению {message_id}")
            print(f"📝 Комментарий: {comment}")

            # Нормализуем chat_id
            target_chat = chat_id
            if isinstance(chat_id, str):
                if chat_id.startswith('@'):
                    target_chat = chat_id
                elif chat_id.isdigit():
                    target_chat = int(chat_id)
                elif chat_id.startswith('-') and chat_id[1:].isdigit():
                    target_chat = int(chat_id)

            # Метод 1: Попытка отправки через reply_to_message_id (имитация действий пользователя)
            try:
                print(f"🎯 Попытка отправки комментария как ответ на сообщение...")

                # Добавляем небольшую задержку для имитации человеческого поведения
                await asyncio.sleep(1)

                sent_message = await client.send_message(
                    chat_id=target_chat,
                    text=comment,
                    reply_to_message_id=message_id,
                    disable_notification=False  # Показываем что это активное действие
                )

                if sent_message and hasattr(sent_message, 'id'):
                    print(f"✅ Комментарий отправлен как ответ аккаунтом {account_id}")
                    return {
                        "status": "success",
                        "message": "Комментарий отправлен под пост",
                        "message_id": sent_message.id
                    }

            except Exception as reply_error:
                error_str = str(reply_error)
                print(f"❌ Ошибка отправки ответа: {error_str}")

                # Метод 2: Если ответ не работает, пробуем найти группу обсуждений
                if "CHAT_ADMIN_REQUIRED" in error_str or "CHAT_WRITE_FORBIDDEN" in error_str:
                    print(f"🔄 Пробуем найти группу обсуждений для канала {chat_id}")

                    try:
                        # Ищем группу обсуждений канала
                        channel = await client.get_chat(chat_id)
                        discussion_group_id = None

                        if hasattr(channel, 'linked_chat') and channel.linked_chat:
                            discussion_group_id = channel.linked_chat.id
                            print(f"📢 Найдена группа обсуждений: {discussion_group_id}")
                        else:
                            # Альтернативный способ поиска группы обсуждений
                            try:
                                from pyrogram.raw import functions
                                peer = await client.resolve_peer(target_chat)
                                full_channel = await client.invoke(
                                    functions.channels.GetFullChannel(channel=peer)
                                )

                                if hasattr(full_channel.full_chat, 'linked_chat_id') and full_channel.full_chat.linked_chat_id:
                                    discussion_group_id = -int(f"100{full_channel.full_chat.linked_chat_id}")
                                    print(f"📢 Альтернативно найдена группа обсуждений: {discussion_group_id}")
                            except Exception as alt_search_error:
                                print(f"❌ Альтернативный поиск не удался: {alt_search_error}")

                        if discussion_group_id:
                            # Отправляем в группу обсуждений
                            try:
                                await asyncio.sleep(1)  # Имитация человеческого поведения

                                sent_message = await client.send_message(
                                    chat_id=discussion_group_id,
                                    text=comment,
                                    reply_to_message_id=message_id
                                )

                                print(f"✅ Комментарий отправлен в группу обсуждений аккаунтом {account_id}")
                                return {
                                    "status": "success",
                                    "message": "Комментарий отправлен в группу обсуждений канала",
                                    "message_id": sent_message.id
                                }
                            except Exception as discussion_error:
                                print(f"❌ Ошибка отправки в группу обсуждений: {discussion_error}")

                        # Если группа обсуждений не найдена, вернем ошибку для попытки Telethon
                        return {
                            "status": "error",
                            "message": f"Pyrogram: У канала {chat_id} нет доступной группы обсуждений"
                        }

                    except Exception as channel_error:
                        print(f"❌ Ошибка поиска группы обсуждений: {channel_error}")
                        return {
                            "status": "error",
                            "message": f"Pyrogram: Для отправки комментариев в {chat_id} требуются права администратора"
                        }

                # Обрабатываем специфические ошибки Telegram
                if "USERNAME_INVALID" in error_str:
                    return {"status": "error", "message": f"Неверное имя пользователя или канала: {chat_id}"}
                elif "PEER_ID_INVALID" in error_str:
                    return {"status": "error", "message": f"Канал/чат {chat_id} не найден или недоступен"}
                elif "MESSAGE_ID_INVALID" in error_str:
                    return {"status": "error", "message": f"Сообщение с ID {message_id} не найдено или недоступно"}
                elif "USER_BANNED_IN_CHANNEL" in error_str:
                    return {"status": "error", "message": "Аккаунт заблокирован в этом канале"}
                elif "REPLY_MESSAGE_INVALID" in error_str:
                    return {"status": "error", "message": "Нельзя ответить на это сообщение"}
                elif "COMMENTS_DISABLED" in error_str:
                    return {"status": "error", "message": "Комментарии отключены для этого поста"}
                else:
                    return {"status": "error", "message": f"Pyrogram ошибка: {error_str}"}

        except Exception as e:
            print(f"❌ Ошибка Pyrogram комментария: {e}")
            return {"status": "error", "message": f"Pyrogram ошибка: {str(e)}"}

    async def _convert_pyrogram_to_telethon_session(self, pyrogram_path: str, telethon_path: str):
        """Конвертация сессии Pyrogram в формат Telethon с полной совместимостью"""
        try:
            import sqlite3
            import shutil

            print(f"🔄 Создаем полностью новую Telethon сессию")

            # Создаем новую базу данных для Telethon с нуля
            telethon_session_file = f"{telethon_path}.session"

            # Удаляем старый файл если существует
            if os.path.exists(telethon_session_file):
                os.remove(telethon_session_file)

            # Читаем данные из Pyrogram сессии для получения auth_key
            pyrogram_conn = sqlite3.connect(pyrogram_path)
            pyrogram_cursor = pyrogram_conn.cursor()

            try:
                # Сначала проверяем структуру таблицы sessions в Pyrogram
                pyrogram_cursor.execute("PRAGMA table_info(sessions)")
                columns_info = pyrogram_cursor.fetchall()
                column_names = [col[1] for col in columns_info]
                print(f"📋 Структура таблицы Pyrogram sessions: {column_names}")

                # Получаем данные сессии из Pyrogram с правильными полями
                if 'server_address' in column_names:
                    query = "SELECT dc_id, server_address, port, auth_key, user_id FROM sessions LIMIT 1"
                    pyrogram_cursor.execute(query)
                    session_data = pyrogram_cursor.fetchone()
                    if session_data:
                        dc_id, server_address, port, auth_key, user_id = session_data
                    else:
                        raise Exception("Не найдены данные сессии в Pyrogram файле")
                else:
                    # Альтернативный способ чтения с базовыми полями
                    query = "SELECT dc_id, auth_key FROM sessions LIMIT 1"
                    pyrogram_cursor.execute(query)
                    session_data = pyrogram_cursor.fetchone()
                    if session_data:
                        dc_id, auth_key = session_data
                        # Используем стандартные значения для отсутствующих полей
                        server_address = "149.154.167.51" if dc_id == 2 else "149.154.175.53"
                        port = 443
                        user_id = 0
                    else:
                        raise Exception("Не найдены данные сессии в Pyrogram файле")

                print(f"📋 Получены данные сессии: DC{dc_id}, Server: {server_address}:{port}")

            finally:
                pyrogram_conn.close()

            # Создаем новую базу данных для Telethon
            conn = sqlite3.connect(telethon_session_file)
            cursor = conn.cursor()

            try:
                # Создаем правильную структуру для Telethon с проверкой существования таблиц
                print("🔨 Создаем структуру базы данных Telethon...")

                # Проверяем какие таблицы уже существуют
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                existing_tables = [row[0] for row in cursor.fetchall()]
                print(f"📋 Существующие таблицы: {existing_tables}")

                # Проверяем какие таблицы нужно создать
                print(f"📋 Проверяем существующие таблицы: {existing_tables}")

                # Таблица version (обязательная для Telethon)
                cursor.execute("CREATE TABLE IF NOT EXISTS version (version INTEGER)")
                cursor.execute("INSERT INTO version VALUES (1)")
                print("✅ Создана таблица version")

                # Таблица sessions (основная таблица с данными авторизации)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS sessions (
                        dc_id INTEGER PRIMARY KEY,
                        server_address TEXT,
                        port INTEGER,
                        auth_key BLOB,
                        takeout_id INTEGER
                    )
                """)
                print("✅ Создана таблица sessions")

                # Вставляем данные сессии
                cursor.execute("""
                    INSERT OR REPLACE INTO sessions (dc_id, server_address, port, auth_key, takeout_id)
                    VALUES (?, ?, ?, ?, NULL)
                """, (dc_id, server_address, port, auth_key))
                print("✅ Данные авторизации добавлены в таблицу sessions")

                # Таблица entities (для кеша пользователей/чатов)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS entities (
                        id INTEGER PRIMARY KEY,
                        hash INTEGER NOT NULL,
                        username TEXT,
                        phone INTEGER,
                        name TEXT,
                        date INTEGER
                    )
                """)
                print("✅ Создана таблица entities")

                # Таблица sent_files (для кеша отправленных файлов)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS sent_files (
                        md5_digest BLOB,
                        file_size INTEGER,
                        type INTEGER,
                        id INTEGER,
                        hash INTEGER,
                        PRIMARY KEY(md5_digest, file_size, type)
                    )
                """)
                print("✅ Создана таблица sent_files")

                # НЕ создаем update_state - Telethon создает её сам при необходимости
                print("⚠️ Таблица update_state не создается - Telethon управляет ею сам")

                conn.commit()
                print("✅ Сессия успешно создана для Telethon с полной совместимостью")

            finally:
                conn.close()

        except Exception as e:
            print(f"❌ Ошибка создания Telethon сессии: {e}")
            # Если не удалось создать сессию, удаляем поврежденный файл
            try:
                if os.path.exists(f"{telethon_path}.session"):
                    os.remove(f"{telethon_path}.session")
            except:
                pass
            raise e

    async def auto_delete_after_campaign(self, campaign_id: int, delay_seconds: int = 5) -> Dict:
        """Автоматическое удаление аккаунтов после завершения кампании"""
        try:
            print(f"⏰ Запланировано автоудаление аккаунтов через {delay_seconds} секунд после кампании {campaign_id}")

            # Ждем указанное время
            await asyncio.sleep(delay_seconds)

            # Получаем аккаунты, участвовавшие в кампании
            db = next(get_db())
            try:
                # Находим все логи отправки для этой кампании
                send_logs = db.query(SendLog).filter(SendLog.campaign_id == campaign_id).all()
                account_ids = list(set(log.account_id for log in send_logs))

                if not account_ids:
                    print(f"⚠️ Не найдено аккаунтов для удаления в кампании {campaign_id}")
                    return {"status": "error", "message": "Не найдено аккаунтов для удаления"}

                print(f"🗑️ Начинаем автоудаление {len(account_ids)} аккаунтов")

                deleted_accounts = []
                failed_deletions = []

                for account_id in account_ids:
                    print(f"🔄 Удаляем аккаунт {account_id}...")

                    result = await self.delete_telegram_account(
                        account_id,
                        reason="Автоматическое удаление после рассылки"
                    )

                    if result["status"] == "success":
                        deleted_accounts.append(result.get("deleted_account", f"Account {account_id}"))
                        print(f"✅ Аккаунт {account_id} удален")
                    else:
                        failed_deletions.append(f"Account {account_id}: {result['message']}")
                        print(f"❌ Не удалось удалить аккаунт {account_id}: {result['message']}")

                    # Небольшая задержка между удалениями
                    await asyncio.sleep(2)

                return {
                    "status": "success",
                    "message": f"Автоудаление завершено. Удалено: {len(deleted_accounts)}, ошибок: {len(failed_deletions)}",
                    "deleted_accounts": deleted_accounts,
                    "failed_deletions": failed_deletions
                }

            finally:
                db.close()

        except Exception as e:
            print(f"❌ Ошибка автоудаления после кампании {campaign_id}: {e}")
            return {"status": "error", "message": f"Ошибка автоудаления: {str(e)}"}

    async def get_client(self, account_id: int) -> Optional[Client]:
        """Вспомогательная функция для получения клиента (переименована для соответствия изменениям)"""
        return await self._get_client_for_account(account_id)

    async def update_profile(self, account_id: int, first_name: str = None, last_name: str = None, bio: str = None, profile_photo_path: str = None) -> Dict:
        """Обновление профиля аккаунта в Telegram"""
        try:
            print(f"🔄 Обновление профиля аккаунта {account_id}")
            print(f"📝 Данные: имя={first_name}, фамилия={last_name}, био={bio}")

            client = await self._get_client_for_account(account_id)
            if not client:
                return {"status": "error", "message": "Не удалось подключиться к аккаунту"}

            if not client.is_connected:
                await client.connect()

            # Получаем текущую информацию о пользователе
            try:
                me = await client.get_me()
                print(f"👤 Текущий профиль: {me.first_name} {me.last_name or ''}")
            except Exception as me_error:
                print(f"⚠️ Не удалось получить текущую информацию: {me_error}")
                me = None

            # Обновляем текстовые данные профиля
            update_success = False
            try:
                # Убираем пустые значения и используем разумные ограничения
                first_name_clean = (first_name or "").strip()[:64] if first_name else ""
                last_name_clean = (last_name or "").strip()[:64] if last_name else ""
                bio_clean = (bio or "").strip()[:70] if bio else ""  # Telegram ограничивает био до 70 символов

                if not first_name_clean:
                    first_name_clean = "User"  # Telegram требует непустое имя

                print(f"🔄 Отправляем обновление профиля...")
                print(f"   Имя: '{first_name_clean}'")
                print(f"   Фамилия: '{last_name_clean}'")
                print(f"   Био: '{bio_clean}'")

                await client.update_profile(
                    first_name=first_name_clean,
                    last_name=last_name_clean,
                    bio=bio_clean
                )

                print(f"✅ Профиль успешно обновлен в Telegram")
                update_success = True

            except Exception as profile_error:
                error_str = str(profile_error).lower()
                print(f"❌ Ошибка обновления профиля: {profile_error}")

                # Обработка специфических ошибок Telegram
                if "firstname_invalid" in error_str:
                    return {"status": "error", "message": "Неверный формат имени. Используйте только буквы и пробелы"}
                elif "about_too_long" in error_str:
                    return {"status": "error", "message": "Описание слишком длинное (максимум 70 символов)"}
                elif "flood" in error_str:
                    return {"status": "error", "message": "Слишком частые изменения профиля. Попробуйте позже"}
                else:
                    return {"status": "error", "message": f"Ошибка обновления профиля: {str(profile_error)}"}

            # Обновляем фото профиля если предоставлено
            photo_success = True
            if profile_photo_path and os.path.exists(profile_photo_path):
                try:
                    print(f"🖼️ Обновляем фото профиля: {profile_photo_path}")
                    await client.set_profile_photo(photo=profile_photo_path)
                    print(f"✅ Фото профиля обновлено")
                except Exception as photo_error:
                    print(f"❌ Ошибка обновления фото профиля: {photo_error}")
                    photo_success = False

            # Проверяем результат обновления
            try:
                await asyncio.sleep(1)  # Даем время на синхронизацию
                updated_me = await client.get_me()
                print(f"🔍 Проверка обновления: {updated_me.first_name} {updated_me.last_name or ''}")

                if update_success:
                    if profile_photo_path and not photo_success:
                        return {"status": "success", "message": "Профиль обновлен, но не удалось установить фото"}
                    else:
                        return {"status": "success", "message": "Профиль успешно обновлен в Telegram"}
                else:
                    return {"status": "error", "message": "Не удалось обновить профиль"}

            except Exception as check_error:
                print(f"⚠️ Не удалось проверить обновление: {check_error}")
                if update_success:
                    return {"status": "success", "message": "Профиль вероятно обновлен (не удалось проверить)"}
                else:
                    return {"status": "error", "message": "Ошибка при проверке обновления профиля"}

        except Exception as e:
            print(f"❌ Общая ошибка обновления профиля: {e}")
            return {"status": "error", "message": f"Общая ошибка: {str(e)}"}

    async def send_reaction(self, account_id: int, chat_id: str, message_id: int, emoji: str) -> Dict:
        """Отправка реакции на сообщение"""
        try:
            client = await self._get_client_for_account(account_id)
            if not client:
                return {"status": "error", "message": "Не удалось подключиться к аккаунту"}

            if not client.is_connected:
                await client.connect()

            # Отправляем реакцию
            from pyrogram.raw import functions
            from pyrogram.raw.types import ReactionEmoji

            await client.invoke(
                functions.messages.SendReaction(
                    peer=await client.resolve_peer(chat_id),
                    msg_id=message_id,
                    reaction=[ReactionEmoji(emoticon=emoji)]
                )
            )

            return {"status": "success", "message": "Реакция отправлена"}

        except Exception as e:
            return {"status": "error", "message": f"Ошибка отправки реакции: {str(e)}"}

    async def view_message(self, account_id: int, chat_id: str, message_id: int) -> Dict:
        """Просмотр сообщения"""
        try:
            client = await self._get_client_for_account(account_id)
            if not client:
                return {"status": "error", "message": "Не удалось подключиться к аккаунту"}

            if not client.is_connected:
                await client.connect()

            # Читаем историю чата до указанного сообщения
            await client.read_chat_history(chat_id=chat_id, max_id=message_id)

            return {"status": "success", "message": "Сообщение просмотрено"}

        except Exception as e:
            return {"status": "error", "message": f"Ошибка просмотра сообщения: {str(e)}"}

    async def send_comment(self, account_id: int, chat_id: str, message_id: int, comment: str) -> Dict:
        """Отправка настоящего комментария под пост канала в секцию 'Leave a comment'"""
        try:
            print(f"💬 Отправляем комментарий в секцию 'Leave a comment' под постом канала...")

            client = await self._get_client_for_account(account_id)
            if not client:
                return {"status": "error", "message": "Не удалось подключиться к аккаунту"}

            if not client.is_connected:
                await client.connect()

            # Проверяем что client.me установлен
            if not hasattr(client, 'me') or client.me is None:
                try:
                    me = await client.get_me()
                    client.me = me
                except Exception:
                    from types import SimpleNamespace
                    client.me = SimpleNamespace(
                        id=account_id,
                        first_name="User",
                        is_premium=False,
                        is_verified=False,
                        is_bot=False
                    )

            print(f"📺 Работаем с каналом: {chat_id}, пост ID: {message_id}")

            try:
                # Используем специальный API для комментариев к постам канала
                from pyrogram.raw import functions, types

                # Получаем peer канала
                peer = await client.resolve_peer(chat_id)

                print(f"🎯 Отправляем комментарий через discussions API...")

                # Используем SendMessage для комментариев с правильными параметрами
                result = await client.invoke(
                    functions.messages.SendMessage(
                        peer=peer,
                        message=comment,
                        reply_to=types.InputReplyToMessage(
                            reply_to_msg_id=message_id,
                            top_msg_id=None,  # Не указываем top_msg_id для комментариев
                            reply_to_peer_id=peer  # Указываем peer для комментариев
                        ),
                        random_id=client.rnd_id(),
                        silent=False  # Комментарии обычно не тихие
                    )
                )

                if result and hasattr(result, 'updates') and result.updates:
                    for update in result.updates:
                        if hasattr(update, 'message') and hasattr(update.message, 'id'):
                            print(f"✅ Комментарий успешно добавлен в секцию 'Leave a comment'! ID: {update.message.id}")
                            return {
                                "status": "success",
                                "message": "Комментарий добавлен в секцию 'Leave a comment'",
                                "message_id": update.message.id
                            }

                # Если не получили ID, но ошибки нет - считаем успешным
                print(f"✅ Комментарий отправлен в секцию 'Leave a comment'")
                return {
                    "status": "success",
                    "message": "Комментарий добавлен в секцию 'Leave a comment'"
                }

            except Exception as api_error:
                error_str = str(api_error)
                print(f"❌ Ошибка API комментариев: {error_str}")

                # Если основной метод не работает, пробуем альтернативный через группу обсуждений
                if "CHAT_ADMIN_REQUIRED" in error_str or "PEER_ID_INVALID" in error_str:
                    print(f"🔄 Пробуем найти группу обсуждений для отправки комментария...")

                    try:
                        # Получаем информацию о канале
                        channel = await client.get_chat(chat_id)

                        # Ищем связанную группу обсуждений
                        discussion_group_id = None
                        if hasattr(channel, 'linked_chat') and channel.linked_chat:
                            discussion_group_id = channel.linked_chat.id
                            print(f"📢 Найдена группа обсуждений: {discussion_group_id}")
                        else:
                            # Альтернативный поиск через GetFullChannel
                            try:
                                from pyrogram.raw import functions
                                peer = await client.resolve_peer(chat_id)
                                full_channel = await client.invoke(
                                    functions.channels.GetFullChannel(channel=peer)
                                )

                                if hasattr(full_channel.full_chat, 'linked_chat_id') and full_channel.full_chat.linked_chat_id:
                                    discussion_group_id = -int(f"100{full_channel.full_chat.linked_chat_id}")
                                    print(f"📢 Найдена группа обсуждений (альтернативно): {discussion_group_id}")
                            except Exception as search_error:
                                print(f"❌ Поиск группы обсуждений не удался: {search_error}")

                        if discussion_group_id:
                            # Отправляем в группу обсуждений как настоящий комментарий
                            try:
                                await asyncio.sleep(1)

                                sent_message = await client.send_message(
                                    chat_id=discussion_group_id,
                                    text=comment,
                                    reply_to_message_id=message_id
                                )

                                print(f"✅ Комментарий отправлен в группу обсуждений (комментарии канала)")
                                return {
                                    "status": "success",
                                    "message": "Комментарий отправлен в обсуждения канала",
                                    "message_id": sent_message.id
                                }
                            except Exception as discussion_error:
                                print(f"❌ Ошибка отправки в группу обсуждений: {discussion_error}")

                        else:
                            return {"status": "error", "message": "У канала нет доступной секции комментариев"}

                    except Exception as channel_error:
                        print(f"❌ Ошибка получения информации о канале: {channel_error}")
                        return {"status": "error", "message": f"Ошибка доступа к каналу: {str(channel_error)}"}

                # Обрабатываем специфические ошибки
                if "USERNAME_INVALID" in error_str:
                    return {"status": "error", "message": f"Неверное имя канала: {chat_id}"}
                elif "MESSAGE_ID_INVALID" in error_str:
                    return {"status": "error", "message": f"Сообщение с ID {message_id} не найдено или недоступно"}
                elif "USER_BANNED_IN_CHANNEL" in error_str:
                    return {"status": "error", "message": "Аккаунт заблокирован в канале"}
                elif "COMMENTS_DISABLED" in error_str:
                    return {"status": "error", "message": "Комментарии отключены для этого поста"}
                else:
                    return {"status": "error", "message": f"Ошибка отправки комментария: {error_str}"}

        except Exception as e:
            print(f"❌ Общая ошибка отправки комментария: {e}")
            return {"status": "error", "message": f"Не удалось отправить комментарий: {str(e)}"}

    async def _send_comment_telethon_enhanced(self, account_id: int, chat_id: str, message_id: int, comment: str) -> Dict:
        """Отправка комментария через Telethon непосредственно под пост канала"""
        try:
            print(f"📱 Telethon: Начинаем отправку комментария под пост...")

            # Получаем данные аккаунта
            db = next(get_db())
            try:
                account = db.query(Account).filter(Account.id == account_id).first()
                if not account:
                    return {"status": "error", "message": "Telethon: Аккаунт не найден"}

                # Импортируем telethon только когда нужно
                try:
                    from telethon import TelegramClient
                    from telethon.tl.functions.messages import SendMessageRequest
                    from telethon.tl.types import InputReplyToMessage
                    print(f"✅ Telethon библиотека импортирована")
                except ImportError:
                    print(f"❌ Telethon не установлен")
                    return {"status": "error", "message": "Telethon не установлен"}

                # Определяем путь к файлу сессии для Telethon
                phone_clean = account.phone.replace('+', '').replace(' ', '').replace('(', '').replace(')', '').replace('-', '')
                pyrogram_session_file = os.path.join(SESSIONS_DIR, f"session_{phone_clean}.session")
                telethon_session_file = os.path.join(SESSIONS_DIR, f"telethon_{phone_clean}")

                # Создаем/проверяем сессию для Telethon
                session_file_path = f"{telethon_session_file}.session"
                if not os.path.exists(session_file_path):
                    try:
                        print(f"🔄 Telethon: Создаем совместимую сессию...")
                        await self._convert_pyrogram_to_telethon_session(pyrogram_session_file, telethon_session_file)
                    except Exception as convert_error:
                        print(f"❌ Telethon: Ошибка конвертации: {convert_error}")
                        return {"status": "error", "message": "Telethon: Не удалось создать сессию"}

                # Создаем Telethon клиент
                try:
                    telethon_client = TelegramClient(telethon_session_file, API_ID, API_HASH)
                    await telethon_client.start()

                    me = await telethon_client.get_me()
                    print(f"✅ Telethon: Авторизован как {me.first_name} ({me.phone})")

                except Exception as client_error:
                    print(f"❌ Telethon: Ошибка создания клиента: {client_error}")
                    return {"status": "error", "message": "Telethon: Ошибка подключения"}

                try:
                    # Нормализуем chat_id для Telethon
                    if chat_id.startswith('@'):
                        target_entity = chat_id
                    elif chat_id.isdigit() or (chat_id.startswith('-') and chat_id[1:].isdigit()):
                        target_entity = int(chat_id)
                    else:
                        target_entity = chat_id

                    print(f"📍 Telethon: Работаем с каналом {target_entity}")

                    # Получаем информацию о целевом канале
                    entity = await telethon_client.get_entity(target_entity)
                    print(f"📍 Telethon: Получена сущность - {type(entity).__name__}")

                    # Метод 1: Отправляем комментарий прямо под пост используя InputReplyToMessage
                    try:
                        print(f"🎯 Telethon: Отправляем комментарий прямо под пост {message_id}...")

                        # Создаем reply для комментария под постом
                        reply_to = InputReplyToMessage(
                            reply_to_msg_id=message_id,
                            top_msg_id=message_id  # Указываем что это комментарий к топ-сообщению
                        )

                        # Отправляем сообщение с reply
                        result = await telethon_client(SendMessageRequest(
                            peer=entity,
                            message=comment,
                            reply_to=reply_to,
                            random_id=telethon_client._get_random_id()
                        ))

                        if result and hasattr(result, 'updates') and result.updates:
                            # Находим отправленное сообщение в результатах
                            for update in result.updates:
                                if hasattr(update, 'message') and hasattr(update.message, 'id'):
                                    print(f"✅ Telethon: Комментарий отправлен под пост! ID: {update.message.id}")
                                    return {
                                        "status": "success",
                                        "message": "Комментарий отправлен под пост канала",
                                        "message_id": update.message.id
                                    }

                        print(f"✅ Telethon: Комментарий отправлен под пост (без ID)")
                        return {
                            "status": "success",
                            "message": "Комментарий отправлен под пост канала"
                        }

                    except Exception as direct_error:
                        error_str = str(direct_error)
                        print(f"❌ Telethon: Прямая отправка не удалась: {error_str}")

                        # Метод 2: Стандартный reply если прямой метод не работает
                        try:
                            print(f"🔄 Telethon: Пробуем стандартный reply метод...")

                            await asyncio.sleep(2)  # Имитация человеческого поведения

                            sent_message = await telethon_client.send_message(
                                entity=entity,
                                message=comment,
                                reply_to=message_id
                            )

                            print(f"✅ Telethon: Стандартный метод сработал! ID: {sent_message.id}")
                            return {
                                "status": "success",
                                "message": "Комментарий отправлен под пост (стандартный метод)",
                                "message_id": sent_message.id
                            }

                        except Exception as standard_error:
                            error_str = str(standard_error)
                            print(f"❌ Telethon: Стандартный метод тоже не сработал: {error_str}")

                            if "CHAT_ADMIN_REQUIRED" in error_str:
                                return {"status": "error", "message": "Telethon: Требуются права администратора для комментариев"}
                            elif "MSG_ID_INVALID" in error_str:
                                return {"status": "error", "message": "Telethon: Неверный ID сообщения"}
                            elif "USER_BANNED_IN_CHANNEL" in error_str:
                                return {"status": "error", "message": "Telethon: Аккаунт заблокирован в канале"}
                            else:
                                return {"status": "error", "message": f"Telethon: Не удалось отправить комментарий: {error_str}"}

                except Exception as send_error:
                    error_str = str(send_error)
                    print(f"❌ Telethon: Ошибка отправки: {error_str}")

                    if "USERNAME_INVALID" in error_str:
                        return {"status": "error", "message": "Telethon: Неверное имя канала"}
                    elif "PEER_ID_INVALID" in error_str:
                        return {"status": "error", "message": "Telethon: Канал не найден"}
                    elif "USER_BANNED_IN_CHANNEL" in error_str:
                        return {"status": "error", "message": "Telethon: Аккаунт заблокирован"}
                    else:
                        return {"status": "error", "message": f"Telethon: {error_str}"}

                finally:
                    print(f"🔌 Telethon: Отключаемся от клиента...")
                    await telethon_client.disconnect()

                    # Удаляем временную сессию
                    try:
                        session_file_path = f"{telethon_session_file}.session"
                        if os.path.exists(session_file_path):
                            os.remove(session_file_path)
                            print(f"🗑️ Telethon: Временная сессия удалена")
                    except Exception as cleanup_error:
                        print(f"⚠️ Telethon: Ошибка очистки сессии: {cleanup_error}")

            finally:
                db.close()

        except Exception as e:
            print(f"❌ Telethon: Общая ошибка: {e}")
            import traceback
            print(f"🔍 Telethon: Стек ошибки: {traceback.format_exc()}")
            return {"status": "error", "message": f"Telethon: {str(e)}"}

    async def _send_comment_pyrogram_enhanced(self, account_id: int, chat_id: str, message_id: int, comment: str) -> Dict:
        """Отправка комментария через Pyrogram с улучшенной логикой"""
        try:
            client = await self._get_client_for_account(account_id)
            if not client:
                return {"status": "error", "message": "Не удалось подключиться к аккаунту"}

            if not client.is_connected:
                await client.connect()

            # Проверяем что client.me установлен
            if not hasattr(client, 'me') or client.me is None:
                try:
                    me = await client.get_me()
                    client.me = me
                except Exception:
                    # Создаем заглушку если не удается получить информацию
                    from types import SimpleNamespace
                    client.me = SimpleNamespace(
                        id=account_id,
                        first_name="User",
                        is_premium=False,
                        is_verified=False,
                        is_bot=False
                    )

            print(f"🔄 Отправка комментария от аккаунта {account_id} в чат {chat_id}, к сообщению {message_id}")
            print(f"📝 Комментарий: {comment}")

            # Нормализуем chat_id
            target_chat = chat_id
            if isinstance(chat_id, str):
                if chat_id.startswith('@'):
                    target_chat = chat_id
                elif chat_id.isdigit():
                    target_chat = int(chat_id)
                elif chat_id.startswith('-') and chat_id[1:].isdigit():
                    target_chat = int(chat_id)

            # Метод 1: Попытка отправки через reply_to_message_id (имитация действий пользователя)
            try:
                print(f"🎯 Попытка отправки комментария как ответ на сообщение...")

                # Добавляем небольшую задержка для имитации человеческого поведения
                await asyncio.sleep(1)

                sent_message = await client.send_message(
                    chat_id=target_chat,
                    text=comment,
                    reply_to_message_id=message_id,
                    disable_notification=False  # Показываем что это активное действие
                )

                if sent_message and hasattr(sent_message, 'id'):
                    print(f"✅ Комментарий отправлен как ответ аккаунтом {account_id}")
                    return {
                        "status": "success",
                        "message": "Комментарий отправлен под пост",
                        "message_id": sent_message.id
                    }

            except Exception as reply_error:
                error_str = str(reply_error)
                print(f"❌ Ошибка отправки ответа: {error_str}")

                # Метод 2: Если ответ не работает, пробуем найти группу обсуждений
                if "CHAT_ADMIN_REQUIRED" in error_str or "CHAT_WRITE_FORBIDDEN" in error_str:
                    print(f"🔄 Пробуем найти группу обсуждений для канала {chat_id}")

                    try:
                        # Ищем группу обсуждений канала
                        channel = await client.get_chat(chat_id)
                        discussion_group_id = None

                        if hasattr(channel, 'linked_chat') and channel.linked_chat:
                            discussion_group_id = channel.linked_chat.id
                            print(f"📢 Найдена группа обсуждений: {discussion_group_id}")
                        else:
                            # Альтернативный способ поиска группы обсуждений
                            try:
                                from pyrogram.raw import functions
                                peer = await client.resolve_peer(target_chat)
                                full_channel = await client.invoke(
                                    functions.channels.GetFullChannel(channel=peer)
                                )

                                if hasattr(full_channel.full_chat, 'linked_chat_id') and full_channel.full_chat.linked_chat_id:
                                    discussion_group_id = -int(f"100{full_channel.full_chat.linked_chat_id}")
                                    print(f"📢 Альтернативно найдена группа обсуждений: {discussion_group_id}")
                            except Exception as alt_search_error:
                                print(f"❌ Альтернативный поиск не удался: {alt_search_error}")

                        if discussion_group_id:
                            # Отправляем в группу обсуждений
                            try:
                                await asyncio.sleep(1)  # Имитация человеческого поведения

                                sent_message = await client.send_message(
                                    chat_id=discussion_group_id,
                                    text=comment,
                                    reply_to_message_id=message_id
                                )

                                print(f"✅ Комментарий отправлен в группу обсуждений аккаунтом {account_id}")
                                return {
                                    "status": "success",
                                    "message": "Комментарий отправлен в группу обсуждений канала",
                                    "message_id": sent_message.id
                                }
                            except Exception as discussion_error:
                                print(f"❌ Ошибка отправки в группу обсуждений: {discussion_error}")

                        # Если группа обсуждений не найдена, вернем ошибку для попытки Telethon
                        return {
                            "status": "error",
                            "message": f"Pyrogram: У канала {chat_id} нет доступной группы обсуждений"
                        }

                    except Exception as channel_error:
                        print(f"❌ Ошибка поиска группы обсуждений: {channel_error}")
                        return {
                            "status": "error",
                            "message": f"Pyrogram: Для отправки комментариев в {chat_id} требуются права администратора"
                        }

                # Обрабатываем специфические ошибки Telegram
                if "USERNAME_INVALID" in error_str:
                    return {"status": "error", "message": f"Неверное имя пользователя или канала: {chat_id}"}
                elif "PEER_ID_INVALID" in error_str:
                    return {"status": "error", "message": f"Канал/чат {chat_id} не найден или недоступен"}
                elif "MESSAGE_ID_INVALID" in error_str:
                    return {"status": "error", "message": f"Сообщение с ID {message_id} не найдено или недоступно"}
                elif "USER_BANNED_IN_CHANNEL" in error_str:
                    return {"status": "error", "message": "Аккаунт заблокирован в этом канале"}
                elif "REPLY_MESSAGE_INVALID" in error_str:
                    return {"status": "error", "message": "Нельзя ответить на это сообщение"}
                elif "COMMENTS_DISABLED" in error_str:
                    return {"status": "error", "message": "Комментарии отключены для этого поста"}
                else:
                    return {"status": "error", "message": f"Pyrogram ошибка: {error_str}"}

        except Exception as e:
            print(f"❌ Ошибка Pyrogram комментария: {e}")
            return {"status": "error", "message": f"Pyrogram ошибка: {str(e)}"}

    async def _convert_pyrogram_to_telethon_session(self, pyrogram_path: str, telethon_path: str):
        """Конвертация сессии Pyrogram в формат Telethon с полной совместимостью"""
        try:
            import sqlite3
            import shutil

            print(f"🔄 Создаем полностью новую Telethon сессию")

            # Создаем новую базу данных для Telethon с нуля
            telethon_session_file = f"{telethon_path}.session"

            # Удаляем старый файл если существует
            if os.path.exists(telethon_session_file):
                os.remove(telethon_session_file)

            # Читаем данные из Pyrogram сессии для получения auth_key
            pyrogram_conn = sqlite3.connect(pyrogram_path)
            pyrogram_cursor = pyrogram_conn.cursor()

            try:
                # Сначала проверяем структуру таблицы sessions в Pyrogram
                pyrogram_cursor.execute("PRAGMA table_info(sessions)")
                columns_info = pyrogram_cursor.fetchall()
                column_names = [col[1] for col in columns_info]
                print(f"📋 Структура таблицы Pyrogram sessions: {column_names}")

                # Получаем данные сессии из Pyrogram с правильными полями
                if 'server_address' in column_names:
                    query = "SELECT dc_id, server_address, port, auth_key, user_id FROM sessions LIMIT 1"
                    pyrogram_cursor.execute(query)
                    session_data = pyrogram_cursor.fetchone()
                    if session_data:
                        dc_id, server_address, port, auth_key, user_id = session_data
                    else:
                        raise Exception("Не найдены данные сессии в Pyrogram файле")
                else:
                    # Альтернативный способ чтения с базовыми полями
                    query = "SELECT dc_id, auth_key FROM sessions LIMIT 1"
                    pyrogram_cursor.execute(query)
                    session_data = pyrogram_cursor.fetchone()
                    if session_data:
                        dc_id, auth_key = session_data
                        # Используем стандартные значения для отсутствующих полей
                        server_address = "149.154.167.51" if dc_id == 2 else "149.154.175.53"
                        port = 443
                        user_id = 0
                    else:
                        raise Exception("Не найдены данные сессии в Pyrogram файле")

                print(f"📋 Получены данные сессии: DC{dc_id}, Server: {server_address}:{port}")

            finally:
                pyrogram_conn.close()

            # Создаем новую базу данных для Telethon
            conn = sqlite3.connect(telethon_session_file)
            cursor = conn.cursor()

            try:
                # Создаем правильную структуру для Telethon с проверкой существования таблиц
                print("🔨 Создаем структуру базы данных Telethon...")

                # Проверяем какие таблицы уже существуют
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                existing_tables = [row[0] for row in cursor.fetchall()]
                print(f"📋 Существующие таблицы: {existing_tables}")

                # Проверяем какие таблицы нужно создать
                print(f"📋 Проверяем существующие таблицы: {existing_tables}")

                # Таблица version (обязательная для Telethon)
                cursor.execute("CREATE TABLE IF NOT EXISTS version (version INTEGER)")
                cursor.execute("INSERT INTO version VALUES (1)")
                print("✅ Создана таблица version")

                # Таблица sessions (основная таблица с данными авторизации)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS sessions (
                        dc_id INTEGER PRIMARY KEY,
                        server_address TEXT,
                        port INTEGER,
                        auth_key BLOB,
                        takeout_id INTEGER
                    )
                """)
                print("✅ Создана таблица sessions")

                # Вставляем данные сессии
                cursor.execute("""
                    INSERT OR REPLACE INTO sessions (dc_id, server_address, port, auth_key, takeout_id)
                    VALUES (?, ?, ?, ?, NULL)
                """, (dc_id, server_address, port, auth_key))
                print("✅ Данные авторизации добавлены в таблицу sessions")

                # Таблица entities (для кеша пользователей/чатов)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS entities (
                        id INTEGER PRIMARY KEY,
                        hash INTEGER NOT NULL,
                        username TEXT,
                        phone INTEGER,
                        name TEXT,
                        date INTEGER
                    )
                """)
                print("✅ Создана таблица entities")

                # Таблица sent_files (для кеша отправленных файлов)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS sent_files (
                        md5_digest BLOB,
                        file_size INTEGER,
                        type INTEGER,
                        id INTEGER,
                        hash INTEGER,
                        PRIMARY KEY(md5_digest, file_size, type)
                    )
                """)
                print("✅ Создана таблица sent_files")

                # НЕ создаем update_state - Telethon создает её сам при необходимости
                print("⚠️ Таблица update_state не создается - Telethon управляет ею сам")

                conn.commit()
                print("✅ Сессия успешно создана для Telethon с полной совместимостью")

            finally:
                conn.close()

        except Exception as e:
            print(f"❌ Ошибка создания Telethon сессии: {e}")
            # Если не удалось создать сессию, удаляем поврежденный файл
            try:
                if os.path.exists(f"{telethon_path}.session"):
                    os.remove(f"{telethon_path}.session")
            except:
                pass
            raise e

    async def auto_delete_after_campaign(self, campaign_id: int, delay_seconds: int = 5) -> Dict:
        """Автоматическое удаление аккаунтов после завершения кампании"""
        try:
            print(f"⏰ Запланировано автоудаление аккаунтов через {delay_seconds} секунд после кампании {campaign_id}")

            # Ждем указанное время
            await asyncio.sleep(delay_seconds)

            # Получаем аккаунты, участвовавшие в кампании
            db = next(get_db())
            try:
                # Находим все логи отправки для этой кампании
                send_logs = db.query(SendLog).filter(SendLog.campaign_id == campaign_id).all()
                account_ids = list(set(log.account_id for log in send_logs))

                if not account_ids:
                    print(f"⚠️ Не найдено аккаунтов для удаления в кампании {campaign_id}")
                    return {"status": "error", "message": "Не найдено аккаунтов для удаления"}

                print(f"🗑️ Начинаем автоудаление {len(account_ids)} аккаунтов")

                deleted_accounts = []
                failed_deletions = []

                for account_id in account_ids:
                    print(f"🔄 Удаляем аккаунт {account_id}...")

                    result = await self.delete_telegram_account(
                        account_id,
                        reason="Автоматическое удаление после рассылки"
                    )

                    if result["status"] == "success":
                        deleted_accounts.append(result.get("deleted_account", f"Account {account_id}"))
                        print(f"✅ Аккаунт {account_id} удален")
                    else:
                        failed_deletions.append(f"Account {account_id}: {result['message']}")
                        print(f"❌ Не удалось удалить аккаунт {account_id}: {result['message']}")

                    # Небольшая задержка между удалениями
                    await asyncio.sleep(2)

                return {
                    "status": "success",
                    "message": f"Автоудаление завершено. Удалено: {len(deleted_accounts)}, ошибок: {len(failed_deletions)}",
                    "deleted_accounts": deleted_accounts,
                    "failed_deletions": failed_deletions
                }

            finally:
                db.close()

        except Exception as e:
            print(f"❌ Ошибка автоудаления после кампании {campaign_id}: {e}")
            return {"status": "error", "message": f"Ошибка автоудаления: {str(e)}"}

    async def get_client(self, account_id: int) -> Optional[Client]:
        """Вспомогательная функция для получения клиента (переименована для соответствия изменениям)"""
        return await self._get_client_for_account(account_id)

    async def update_profile(self, account_id: int, first_name: str = None, last_name: str = None, bio: str = None, profile_photo_path: str = None) -> Dict:
        """Обновление профиля аккаунта в Telegram"""
        try:
            print(f"🔄 Обновление профиля аккаунта {account_id}")
            print(f"📝 Данные: имя={first_name}, фамилия={last_name}, био={bio}")

            client = await self._get_client_for_account(account_id)
            if not client:
                return {"status": "error", "message": "Не удалось подключиться к аккаунту"}

            if not client.is_connected:
                await client.connect()

            # Получаем текущую информацию о пользователе
            try:
                me = await client.get_me()
                print(f"👤 Текущий профиль: {me.first_name} {me.last_name or ''}")
            except Exception as me_error:
                print(f"⚠️ Не удалось получить текущую информацию: {me_error}")
                me = None

            # Обновляем текстовые данные профиля
            update_success = False
            try:
                # Убираем пустые значения и используем разумные ограничения
                first_name_clean = (first_name or "").strip()[:64] if first_name else ""
                last_name_clean = (last_name or "").strip()[:64] if last_name else ""
                bio_clean = (bio or "").strip()[:70] if bio else ""  # Telegram ограничивает био до 70 символов

                if not first_name_clean:
                    first_name_clean = "User"  # Telegram требует непустое имя

                print(f"🔄 Отправляем обновление профиля...")
                print(f"   Имя: '{first_name_clean}'")
                print(f"   Фамилия: '{last_name_clean}'")
                print(f"   Био: '{bio_clean}'")

                await client.update_profile(
                    first_name=first_name_clean,
                    last_name=last_name_clean,
                    bio=bio_clean
                )

                print(f"✅ Профиль успешно обновлен в Telegram")
                update_success = True

            except Exception as profile_error:
                error_str = str(profile_error).lower()
                print(f"❌ Ошибка обновления профиля: {profile_error}")

                # Обработка специфических ошибок Telegram
                if "firstname_invalid" in error_str:
                    return {"status": "error", "message": "Неверный формат имени. Используйте только буквы и пробелы"}
                elif "about_too_long" in error_str:
                    return {"status": "error", "message": "Описание слишком длинное (максимум 70 символов)"}
                elif "flood" in error_str:
                    return {"status": "error", "message": "Слишком частые изменения профиля. Попробуйте позже"}
                else:
                    return {"status": "error", "message": f"Ошибка обновления профиля: {str(profile_error)}"}

            # Обновляем фото профиля если предоставлено
            photo_success = True
            if profile_photo_path and os.path.exists(profile_photo_path):
                try:
                    print(f"🖼️ Обновляем фото профиля: {profile_photo_path}")
                    await client.set_profile_photo(photo=profile_photo_path)
                    print(f"✅ Фото профиля обновлено")
                except Exception as photo_error:
                    print(f"❌ Ошибка обновления фото профиля: {photo_error}")
                    photo_success = False

            # Проверяем результат обновления
            try:
                await asyncio.sleep(1)  # Даем время на синхронизацию
                updated_me = await client.get_me()
                print(f"🔍 Проверка обновления: {updated_me.first_name} {updated_me.last_name or ''}")

                if update_success:
                    if profile_photo_path and not photo_success:
                        return {"status": "success", "message": "Профиль обновлен, но не удалось установить фото"}
                    else:
                        return {"status": "success", "message": "Профиль успешно обновлен в Telegram"}
                else:
                    return {"status": "error", "message": "Не удалось обновить профиль"}

            except Exception as check_error:
                print(f"⚠️ Не удалось проверить обновление: {check_error}")
                if update_success:
                    return {"status": "success", "message": "Профиль вероятно обновлен (не удалось проверить)"}
                else:
                    return {"status": "error", "message": "Ошибка при проверке обновления профиля"}

        except Exception as e:
            print(f"❌ Общая ошибка обновления профиля: {e}")
            return {"status": "error", "message": f"Общая ошибка: {str(e)}"}

    async def send_reaction(self, account_id: int, chat_id: str, message_id: int, emoji: str) -> Dict:
        """Отправка реакции на сообщение"""
        try:
            client = await self._get_client_for_account(account_id)
            if not client:
                return {"status": "error", "message": "Не удалось подключиться к аккаунту"}

            if not client.is_connected:
                await client.connect()

            # Отправляем реакцию
            from pyrogram.raw import functions
            from pyrogram.raw.types import ReactionEmoji

            await client.invoke(
                functions.messages.SendReaction(
                    peer=await client.resolve_peer(chat_id),
                    msg_id=message_id,
                    reaction=[ReactionEmoji(emoticon=emoji)]
                )
            )

            return {"status": "success", "message": "Реакция отправлена"}

        except Exception as e:
            return {"status": "error", "message": f"Ошибка отправки реакции: {str(e)}"}

    async def view_message(self, account_id: int, chat_id: str, message_id: int) -> Dict:
        """Просмотр сообщения"""
        try:
            client = await self._get_client_for_account(account_id)
            if not client:
                return {"status": "error", "message": "Не удалось подключиться к аккаунту"}

            if not client.is_connected:
                await client.connect()

            # Читаем историю чата до указанного сообщения
            await client.read_chat_history(chat_id=chat_id, max_id=message_id)

            return {"status": "success", "message": "Сообщение просмотрено"}

        except Exception as e:
            return {"status": "error", "message": f"Ошибка просмотра сообщения: {str(e)}"}

    async def send_comment(self, account_id: int, chat_id: str, message_id: int, comment: str) -> Dict:
        """Отправка настоящего комментария под пост канала в секцию 'Leave a comment'"""
        try:
            print(f"💬 Отправляем комментарий в секцию 'Leave a comment' под постом канала...")

            client = await self._get_client_for_account(account_id)
            if not client:
                return {"status": "error", "message": "Не удалось подключиться к аккаунту"}

            if not client.is_connected:
                await client.connect()

            # Проверяем что client.me установлен
            if not hasattr(client, 'me') or client.me is None:
                try:
                    me = await client.get_me()
                    client.me = me
                except Exception:
                    from types import SimpleNamespace
                    client.me = SimpleNamespace(
                        id=account_id,
                        first_name="User",
                        is_premium=False,
                        is_verified=False,
                        is_bot=False
                    )

            print(f"📺 Работаем с каналом: {chat_id}, пост ID: {message_id}")

            try:
                # Используем специальный API для комментариев к постам канала
                from pyrogram.raw import functions, types

                # Получаем peer канала
                peer = await client.resolve_peer(chat_id)

                print(f"🎯 Отправляем комментарий через discussions API...")

                # Используем SendMessage для комментариев с правильными параметрами
                result = await client.invoke(
                    functions.messages.SendMessage(
                        peer=peer,
                        message=comment,
                        reply_to=types.InputReplyToMessage(
                            reply_to_msg_id=message_id,
                            top_msg_id=None,  # Не указываем top_msg_id для комментариев
                            reply_to_peer_id=peer  # Указываем peer для комментариев
                        ),
                        random_id=client.rnd_id(),
                        silent=False  # Комментарии обычно не тихие
                    )
                )

                if result and hasattr(result, 'updates') and result.updates:
                    for update in result.updates:
                        if hasattr(update, 'message') and hasattr(update.message, 'id'):
                            print(f"✅ Комментарий успешно добавлен в секцию 'Leave a comment'! ID: {update.message.id}")
                            return {
                                "status": "success",
                                "message": "Комментарий добавлен в секцию 'Leave a comment'",
                                "message_id": update.message.id
                            }

                # Если не получили ID, но ошибки нет - считаем успешным
                print(f"✅ Комментарий отправлен в секцию 'Leave a comment'")
                return {
                    "status": "success",
                    "message": "Комментарий добавлен в секцию 'Leave a comment'"
                }

            except Exception as api_error:
                error_str = str(api_error)
                print(f"❌ Ошибка API комментариев: {error_str}")

                # Если основной метод не работает, пробуем альтернативный через группу обсуждений
                if "CHAT_ADMIN_REQUIRED" in error_str or "PEER_ID_INVALID" in error_str:
                    print(f"🔄 Пробуем найти группу обсуждений для отправки комментария...")

                    try:
                        # Получаем информацию о канале
                        channel = await client.get_chat(chat_id)

                        # Ищем связанную группу обсуждений
                        discussion_group_id = None
                        if hasattr(channel, 'linked_chat') and channel.linked_chat:
                            discussion_group_id = channel.linked_chat.id
                            print(f"📢 Найдена группа обсуждений: {discussion_group_id}")
                        else:
                            # Альтернативный поиск через GetFullChannel
                            try:
                                from pyrogram.raw import functions
                                peer = await client.resolve_peer(chat_id)
                                full_channel = await client.invoke(
                                    functions.channels.GetFullChannel(channel=peer)
                                )

                                if hasattr(full_channel.full_chat, 'linked_chat_id') and full_channel.full_chat.linked_chat_id:
                                    discussion_group_id = -int(f"100{full_channel.full_chat.linked_chat_id}")
                                    print(f"📢 Найдена группа обсуждений (альтернативно): {discussion_group_id}")
                            except Exception as search_error:
                                print(f"❌ Поиск группы обсуждений не удался: {search_error}")

                        if discussion_group_id:
                            # Отправляем в группу обсуждений как настоящий комментарий
                            try:
                                await asyncio.sleep(1)

                                sent_message = await client.send_message(
                                    chat_id=discussion_group_id,
                                    text=comment,
                                    reply_to_message_id=message_id
                                )

                                print(f"✅ Комментарий отправлен в группу обсуждений (комментарии канала)")
                                return {
                                    "status": "success",
                                    "message": "Комментарий отправлен в обсуждения канала",
                                    "message_id": sent_message.id
                                }
                            except Exception as discussion_error:
                                print(f"❌ Ошибка отправки в группу обсуждений: {discussion_error}")

                        else:
                            return {"status": "error", "message": "У канала нет доступной секции комментариев"}

                    except Exception as channel_error:
                        print(f"❌ Ошибка получения информации о канале: {channel_error}")
                        return {"status": "error", "message": f"Ошибка доступа к каналу: {str(channel_error)}"}

                # Обрабатываем специфические ошибки
                if "USERNAME_INVALID" in error_str:
                    return {"status": "error", "message": f"Неверное имя канала: {chat_id}"}
                elif "MESSAGE_ID_INVALID" in error_str:
                    return {"status": "error", "message": f"Сообщение с ID {message_id} не найдено или недоступно"}
                elif "USER_BANNED_IN_CHANNEL" in error_str:
                    return {"status": "error", "message": "Аккаунт заблокирован в канале"}
                elif "COMMENTS_DISABLED" in error_str:
                    return {"status": "error", "message": "Комментарии отключены для этого поста"}
                else:
                    return {"status": "error", "message": f"Ошибка отправки комментария: {error_str}"}

        except Exception as e:
            print(f"❌ Общая ошибка отправки комментария: {e}")
            return {"status": "error", "message": f"Не удалось отправить комментарий: {str(e)}"}

    async def _send_comment_telethon_enhanced(self, account_id: int, chat_id: str, message_id: int, comment: str) -> Dict:
        """Отправка комментария через Telethon непосредственно под пост канала"""
        try:
            print(f"📱 Telethon: Начинаем отправку комментария под пост...")

            # Получаем данные аккаунта
            db = next(get_db())
            try:
                account = db.query(Account).filter(Account.id == account_id).first()
                if not account:
                    return {"status": "error", "message": "Telethon: Аккаунт не найден"}

                # Импортируем telethon только когда нужно
                try:
                    from telethon import TelegramClient
                    from telethon.tl.functions.messages import SendMessageRequest
                    from telethon.tl.types import InputReplyToMessage
                    print(f"✅ Telethon библиотека импортирована")
                except ImportError:
                    print(f"❌ Telethon не установлен")
                    return {"status": "error", "message": "Telethon не установлен"}

                # Определяем путь к файлу сессии для Telethon
                phone_clean = account.phone.replace('+', '').replace(' ', '').replace('(', '').replace(')', '').replace('-', '')
                pyrogram_session_file = os.path.join(SESSIONS_DIR, f"session_{phone_clean}.session")
                telethon_session_file = os.path.join(SESSIONS_DIR, f"telethon_{phone_clean}")

                # Создаем/проверяем сессию для Telethon
                session_file_path = f"{telethon_session_file}.session"
                if not os.path.exists(session_file_path):
                    try:
                        print(f"🔄 Telethon: Создаем совместимую сессию...")
                        await self._convert_pyrogram_to_telethon_session(pyrogram_session_file, telethon_session_file)
                    except Exception as convert_error:
                        print(f"❌ Telethon: Ошибка конвертации: {convert_error}")
                        return {"status": "error", "message": "Telethon: Не удалось создать сессию"}

                # Создаем Telethon клиент
                try:
                    telethon_client = TelegramClient(telethon_session_file, API_ID, API_HASH)
                    await telethon_client.start()

                    me = await telethon_client.get_me()
                    print(f"✅ Telethon: Авторизован как {me.first_name} ({me.phone})")

                except Exception as client_error:
                    print(f"❌ Telethon: Ошибка создания клиента: {client_error}")
                    return {"status": "error", "message": "Telethon: Ошибка подключения"}

                try:
                    # Нормализуем chat_id для Telethon
                    if chat_id.startswith('@'):
                        target_entity = chat_id
                    elif chat_id.isdigit() or (chat_id.startswith('-') and chat_id[1:].isdigit()):
                        target_entity = int(chat_id)
                    else:
                        target_entity = chat_id

                    print(f"📍 Telethon: Работаем с каналом {target_entity}")

                    # Получаем информацию о целевом канале
                    entity = await telethon_client.get_entity(target_entity)
                    print(f"📍 Telethon: Получена сущность - {type(entity).__name__}")

                    # Метод 1: Отправляем комментарий прямо под пост используя InputReplyToMessage
                    try:
                        print(f"🎯 Telethon: Отправляем комментарий прямо под пост {message_id}...")

                        # Создаем reply для комментария под постом
                        reply_to = InputReplyToMessage(
                            reply_to_msg_id=message_id,
                            top_msg_id=message_id  # Указываем что это комментарий к топ-сообщению
                        )

                        # Отправляем сообщение с reply
                        result = await telethon_client(SendMessageRequest(
                            peer=entity,
                            message=comment,
                            reply_to=reply_to,
                            random_id=telethon_client._get_random_id()
                        ))

                        if result and hasattr(result, 'updates') and result.updates:
                            # Находим отправленное сообщение в результатах
                            for update in result.updates:
                                if hasattr(update, 'message') and hasattr(update.message, 'id'):
                                    print(f"✅ Telethon: Комментарий отправлен под пост! ID: {update.message.id}")
                                    return {
                                        "status": "success",
                                        "message": "Комментарий отправлен под пост канала",
                                        "message_id": update.message.id
                                    }

                        print(f"✅ Telethon: Комментарий отправлен под пост (без ID)")
                        return {
                            "status": "success",
                            "message": "Комментарий отправлен под пост канала"
                        }

                    except Exception as direct_error:
                        error_str = str(direct_error)
                        print(f"❌ Telethon: Прямая отправка не удалась: {error_str}")

                        # Метод 2: Стандартный reply если прямой метод не работает
                        try:
                            print(f"🔄 Telethon: Пробуем стандартный reply метод...")

                            await asyncio.sleep(2)  # Имитация человеческого поведения

                            sent_message = await telethon_client.send_message(
                                entity=entity,
                                message=comment,
                                reply_to=message_id
                            )

                            print(f"✅ Telethon: Стандартный метод сработал! ID: {sent_message.id}")
                            return {
                                "status": "success",
                                "message": "Комментарий отправлен под пост (стандартный метод)",
                                "message_id": sent_message.id
                            }

                        except Exception as standard_error:
                            error_str = str(standard_error)
                            print(f"❌ Telethon: Стандартный метод тоже не сработал: {error_str}")

                            if "CHAT_ADMIN_REQUIRED" in error_str:
                                return {"status": "error", "message": "Telethon: Требуются права администратора для комментариев"}
                            elif "MSG_ID_INVALID" in error_str:
                                return {"status": "error", "message": "Telethon: Неверный ID сообщения"}
                            elif "USER_BANNED_IN_CHANNEL" in error_str:
                                return {"status": "error", "message": "Telethon: Аккаунт заблокирован в канале"}
                            else:
                                return {"status": "error", "message": f"Telethon: Не удалось отправить комментарий: {error_str}"}

                except Exception as send_error:
                    error_str = str(send_error)
                    print(f"❌ Telethon: Ошибка отправки: {error_str}")

                    if "USERNAME_INVALID" in error_str:
                        return {"status": "error", "message": "Telethon: Неверное имя канала"}
                    elif "PEER_ID_INVALID" in error_str:
                        return {"status": "error", "message": "Telethon: Канал не найден"}
                    elif "USER_BANNED_IN_CHANNEL" in error_str:
                        return {"status": "error", "message": "Telethon: Аккаунт заблокирован"}
                    else:
                        return {"status": "error", "message": f"Telethon: {error_str}"}

                finally:
                    print(f"🔌 Telethon: Отключаемся от клиента...")
                    await telethon_client.disconnect()

                    # Удаляем временную сессию
                    try:
                        session_file_path = f"{telethon_session_file}.session"
                        if os.path.exists(session_file_path):
                            os.remove(session_file_path)
                            print(f"🗑️ Telethon: Временная сессия удалена")
                    except Exception as cleanup_error:
                        print(f"⚠️ Telethon: Ошибка очистки сессии: {cleanup_error}")

            finally:
                db.close()

        except Exception as e:
            print(f"❌ Telethon: Общая ошибка: {e}")
            import traceback
            print(f"🔍 Telethon: Стек ошибки: {traceback.format_exc()}")
            return {"status": "error", "message": f"Telethon: {str(e)}"}

# Глобальный экземпляр менеджера
telegram_manager = TelegramManager()