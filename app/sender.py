import asyncio
import json
import csv
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from sqlalchemy.orm import Session
from app.database import Account, Campaign, SendLog, get_db
from app.telegram_client import telegram_manager

class MessageSender:
    def __init__(self):
        self.active_campaigns: Dict[int, bool] = {}
        self.scheduled_campaigns: Dict[int, asyncio.Task] = {}

    async def start_campaign(self, campaign_id: int) -> Dict:
        """Запуск кампании рассылки"""
        if campaign_id in self.active_campaigns:
            return {"status": "error", "message": "Кампания уже запущена"}

        db = next(get_db())
        try:
            campaign = db.query(Campaign).filter(Campaign.id == campaign_id).first()
            if not campaign:
                return {"status": "error", "message": "Кампания не найдена"}

            campaign.status = "running"
            db.commit()

            self.active_campaigns[campaign_id] = True

            # Запускаем отправку в фоне
            asyncio.create_task(self._run_campaign(campaign_id))

            return {"status": "success", "message": "Кампания запущена"}
        finally:
            db.close()

    async def create_auto_campaign(self, account_id: int, message: str, delay_seconds: int = 5, target_types: List[str] = None) -> Dict:
        """Создание автоматической кампании для всех контактов пользователя"""
        if target_types is None:
            target_types = ["private"]  # По умолчанию только приватные сообщения

        try:
            from app.telegram_client import telegram_manager

            # Получаем все чаты пользователя
            chats_result = await telegram_manager.get_user_chats(account_id)
            if chats_result["status"] != "success":
                return {"status": "error", "message": "Не удалось получить список чатов"}

            chats = chats_result["chats"]
            recipients = {"private": [], "groups": [], "channels": []}

            # Формируем списки получателей
            for chat_type in target_types:
                if chat_type in chats:
                    for chat in chats[chat_type]:
                        if chat["username"]:
                            recipients[chat_type].append(f"@{chat['username']}")
                        else:
                            recipients[chat_type].append(str(chat["id"]))

            # Создаем кампанию в базе данных
            db = next(get_db())
            try:
                campaign = Campaign(
                    name=f"Автоматическая рассылка {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                    delay_seconds=delay_seconds,
                    private_message=message if "private" in target_types else None,
                    group_message=message if "groups" in target_types else None,
                    channel_message=message if "channels" in target_types else None,
                    private_list="\n".join(recipients["private"]) if recipients["private"] else None,
                    groups_list="\n".join(recipients["groups"]) if recipients["groups"] else None,
                    channels_list="\n".join(recipients["channels"]) if recipients["channels"] else None,
                    status="created"
                )

                db.add(campaign)
                db.commit()
                db.refresh(campaign)

                return {
                    "status": "success",
                    "campaign_id": campaign.id,
                    "recipients_count": sum(len(recipients[t]) for t in recipients),
                    "message": f"Создана автоматическая кампания с {sum(len(recipients[t]) for t in recipients)} получателями"
                }

            finally:
                db.close()

        except Exception as e:
            print(f"Error creating auto campaign: {str(e)}")
            return {"status": "error", "message": str(e)}

    async def start_auto_campaign(self, account_id: int, message: str, delay_seconds: int = 5, target_types: List[str] = None) -> Dict:
        """Создание и запуск автоматической кампании"""
        # Создаем кампанию
        result = await self.create_auto_campaign(account_id, message, delay_seconds, target_types)
        if result["status"] != "success":
            return result

        # Запускаем кампанию
        campaign_id = result["campaign_id"]
        start_result = await self.start_campaign(campaign_id)

        if start_result["status"] == "success":
            return {
                "status": "success",
                "campaign_id": campaign_id,
                "recipients_count": result["recipients_count"],
                "message": f"Автоматическая рассылка запущена для {result['recipients_count']} получателей"
            }
        else:
            return start_result

    async def stop_campaign(self, campaign_id: int) -> Dict:
        """Остановка кампании"""
        if campaign_id in self.active_campaigns:
            self.active_campaigns[campaign_id] = False

            db = next(get_db())
            campaign = db.query(Campaign).filter(Campaign.id == campaign_id).first()
            if campaign:
                campaign.status = "paused"
                db.commit()
            db.close()

            return {"status": "success", "message": "Кампания остановлена"}

        return {"status": "error", "message": "Кампания не активна"}

    async def _run_campaign(self, campaign_id: int):
        """Выполнение кампании рассылки с параллельной отправкой"""
        db = next(get_db())
        try:
            campaign = db.query(Campaign).filter(Campaign.id == campaign_id).first()
            if not campaign:
                print(f"Campaign {campaign_id} not found")
                return

            print(f"Starting campaign {campaign_id} execution")

            # Получаем все активные аккаунты для параллельной отправки
            if hasattr(campaign, 'account_id') and campaign.account_id:
                # Для кампаний по контактам получаем все аккаунты из списка
                # Ищем другие аккаунты, которые могли быть заданы в названии кампании или другим способом
                accounts = db.query(Account).filter(Account.is_active == True).all()
                if not accounts:
                    print("No active accounts found")
                    campaign.status = "completed"
                    db.commit()
                    return

                # Сбрасываем счетчики для всех аккаунтов
                for acc in accounts:
                    acc.messages_sent_today = 0
                    acc.messages_sent_hour = 0
                db.commit()
                print(f"Reset message counters for {len(accounts)} accounts")
            else:
                # Для обычных кампаний используем все активные аккаунты
                accounts = db.query(Account).filter(Account.is_active == True).all()
                if not accounts:
                    print("No active accounts found")
                    campaign.status = "completed"
                    db.commit()
                    return

            # Парсим списки получателей
            recipients = self._parse_recipients(campaign)
            print(f"Recipients parsed: {recipients}")

            if not recipients or not any(recipients.values()):
                print("No recipients found")
                campaign.status = "completed"
                db.commit()
                return

            # Собираем все задачи отправки для параллельного выполнения
            send_tasks = []

            for recipient_type, recipient_list in recipients.items():
                if not self.active_campaigns.get(campaign_id, False):
                    print(f"Campaign {campaign_id} stopped by user")
                    break

                if not recipient_list:
                    print(f"No recipients for type {recipient_type}")
                    continue

                message = self._get_message_for_type(campaign, recipient_type)
                if not message:
                    print(f"No message for recipient type {recipient_type}")
                    continue

                print(f"Preparing {len(recipient_list)} recipients of type {recipient_type} for parallel sending")

                # Создаем задачи для каждого получателя
                for i, recipient in enumerate(recipient_list):
                    if not self.active_campaigns.get(campaign_id, False):
                        break

                    # Распределяем получателей по аккаунтам равномерно
                    account = accounts[i % len(accounts)]

                    # Создаем задачу отправки
                    task = asyncio.create_task(
                        self._send_message_task(
                            campaign_id, account, recipient, message,
                            recipient_type, getattr(campaign, 'attachment_path', None)
                        )
                    )
                    send_tasks.append(task)

            print(f"🔄 Запускаем {len(send_tasks)} задач с ограничением concurrency")

            # Выполняем задачи с ограничением количества одновременных операций
            results = await self._execute_tasks_with_concurrency_limit(send_tasks, max_concurrent=3)


            # Подсчитываем успешные отправки
            total_sent = 0
            for result in results:
                if isinstance(result, dict) and result.get("status") == "success":
                    total_sent += 1
                elif isinstance(result, Exception):
                    print(f"Task exception: {result}")

            print(f"Campaign {campaign_id} completed. Total sent: {total_sent}")
            # Завершаем кампанию
            campaign.status = "completed"
            db.commit()

            # Проверяем, нужно ли автоматически удалять аккаунты
            if hasattr(campaign, 'auto_delete_accounts') and campaign.auto_delete_accounts:
                delete_delay = getattr(campaign, 'delete_delay_minutes', 5)
                print(f"🗑️ Запланировано автоудаление аккаунтов через {delete_delay} секунд")

                # Запускаем автоудаление в фоне
                asyncio.create_task(
                    telegram_manager.auto_delete_after_campaign(campaign_id, delete_delay)
                )

            if campaign_id in self.active_campaigns:
                del self.active_campaigns[campaign_id]

        except Exception as e:
            print(f"Error in campaign {campaign_id}: {str(e)}")
            # Обновляем статус кампании при ошибке
            try:
                campaign = db.query(Campaign).filter(Campaign.id == campaign_id).first()
                if campaign:
                    campaign.status = "error"
                    db.commit()
            except:
                pass
        finally:
            db.close()

    async def _send_message_task(self, campaign_id: int, account: Account, recipient: str, message: str, recipient_type: str, attachment_path: str = None) -> Dict:
        """Задача отправки одного сообщения"""
        try:
            # Проверяем, что кампания все еще активна
            if not self.active_campaigns.get(campaign_id, False):
                return {"status": "error", "message": "Campaign stopped"}

            print(f"🚀 Sending to {recipient} via account {account.id} ({account.name})")

            # Отправляем сообщение мгновенно (без задержки)
            result = await telegram_manager.send_message(
                account.id,
                recipient,
                message,
                attachment_path,
                schedule_seconds=0  # Мгновенная отправка
            )

            # Проверяем результат
            if hasattr(result, 'id'):  # Это объект Message
                result = {"status": "success", "message_id": result.id}
            elif not isinstance(result, dict):
                result = {"status": "error", "message": f"Неизвестный тип результата: {type(result)}"}

            # Обрабатываем FLOOD_WAIT
            if result.get("status") == "flood_wait":
                wait_time = result.get("wait_time", 30)
                print(f"⏰ FLOOD_WAIT для {recipient}: ожидание {wait_time} секунд")
                # Не логируем как ошибку, просто пропускаем
                return {"status": "skipped", "message": f"FLOOD_WAIT: {wait_time} секунд"}

            # Логируем результат
            self._log_send_result(campaign_id, account.id, recipient, recipient_type, result)

            if result.get("status") == "success":
                print(f"✅ Message sent instantly to {recipient} via account {account.id}")
            else:
                print(f"❌ Failed to send message to {recipient}: {result.get('message', 'Unknown error')}")

            return result

        except Exception as send_error:
            print(f"❌ Exception while sending to {recipient}: {str(send_error)}")
            error_result = {"status": "error", "message": str(send_error)}

            # Логируем ошибку
            try:
                self._log_send_result(campaign_id, account.id, recipient, recipient_type, error_result)
            except Exception as log_error:
                print(f"Failed to log error: {log_error}")

            return error_result

    def _parse_recipients(self, campaign: Campaign) -> Dict[str, List[str]]:
        """Парсинг списков получателей"""
        recipients = {}

        if campaign.channels_list:
            try:
                recipients["channel"] = json.loads(campaign.channels_list)
            except:
                recipients["channel"] = [line.strip() for line in campaign.channels_list.split("\n") if line.strip()]

        if campaign.groups_list:
            try:
                recipients["group"] = json.loads(campaign.groups_list)
            except:
                recipients["group"] = [line.strip() for line in campaign.groups_list.split("\n") if line.strip()]

        if campaign.private_list:
            try:
                recipients["private"] = json.loads(campaign.private_list)
            except:
                recipients["private"] = [line.strip() for line in campaign.private_list.split("\n") if line.strip()]

        # Убираем пустые строки и очищаем от лишних символов
        for key in recipients:
            cleaned_recipients = []
            for r in recipients[key]:
                if r.strip():
                    clean_r = r.strip()

                    # Обрабатываем ссылки Telegram
                    if 't.me/' in clean_r:
                        if 't.me/joinchat/' in clean_r:
                            # Старый формат приватных ссылок
                            clean_r = clean_r.split('t.me/joinchat/')[1]
                            clean_r = f"+{clean_r}"
                        elif 't.me/+' in clean_r:
                            # Новый формат приватных ссылок
                            clean_r = clean_r.split('t.me/')[1]
                        else:
                            # Это обычный username
                            clean_r = clean_r.split('t.me/')[1].split('?')[0]  # убираем параметры
                            # Для обычных username не убираем @, оставляем как есть
                            if not clean_r.startswith('@') and not clean_r.startswith('+'):
                                clean_r = f"@{clean_r}"
                    else:
                        # Если это просто username или ID
                        if clean_r.startswith('@'):
                            # Оставляем @ для групп и каналов
                            pass
                        elif clean_r.startswith('+'):
                            # Приватная ссылка без t.me
                            pass
                        elif clean_r.isdigit() or clean_r.startswith('-'):
                            # Это ID чата
                            pass
                        else:
                            # Обычный username без @ - добавляем @
                            clean_r = f"@{clean_r}"

                    if clean_r:
                        cleaned_recipients.append(clean_r)
            recipients[key] = cleaned_recipients

        print(f"Parsed recipients: {recipients}")
        return recipients

    def _get_message_for_type(self, campaign: Campaign, recipient_type: str) -> str:
        """Получение сообщения для типа получателя"""
        if recipient_type == "channel":
            return campaign.channel_message
        elif recipient_type == "group":
            return campaign.group_message
        elif recipient_type == "private":
            return campaign.private_message
        return None

    def _check_account_limits(self, account: Account) -> bool:
        """Проверка лимитов аккаунта"""
        from app.config import MAX_MESSAGES_PER_HOUR, MAX_MESSAGES_PER_DAY

        # Временно отключаем лимиты для тестирования
        return True

        # Раскомментируйте строки ниже если нужно включить лимиты обратно
        # if account.messages_sent_today >= MAX_MESSAGES_PER_DAY:
        #     print(f"Account {account.id} reached daily limit: {account.messages_sent_today}/{MAX_MESSAGES_PER_DAY}")
        #     return False

        # if account.messages_sent_hour >= MAX_MESSAGES_PER_HOUR:
        #     print(f"Account {account.id} reached hourly limit: {account.messages_sent_hour}/{MAX_MESSAGES_PER_HOUR}")
        #     return False

        # return True

    def _log_send_result(self, campaign_id: int, account_id: int,
                        recipient: str, recipient_type: str, result: Dict):
        """Логирование результата отправки"""
        db = next(get_db())
        try:
            # Определяем статус для логирования
            if result["status"] == "success":
                log_status = "sent"
                error_message = None
            elif result["status"] == "skipped":
                log_status = "skipped"
                error_message = result.get("message", "Unknown reason")
            else:
                log_status = "failed"
                error_message = result.get("message", "Unknown error")

            log = SendLog(
                campaign_id=campaign_id,
                account_id=account_id,
                recipient=recipient,
                recipient_type=recipient_type,
                status=log_status,
                error_message=error_message
            )
            db.add(log)
            db.commit()
            print(f"Logged result for {recipient}: {log_status}")
        except Exception as e:
            print(f"Error logging send result: {str(e)}")
        finally:
            db.close()

    async def create_and_start_auto_campaign(self, account_id: int, message: str,
                                          delay_seconds: int, unique_targets: bool = True) -> Dict:
        """Создание и запуск автоматической кампании"""
        try:
            # Получаем контакты пользователя
            contacts_result = await telegram_manager.get_user_contacts(account_id)
            if contacts_result["status"] != "success":
                return {"status": "error", "message": f"Не удалось получить контакты: {contacts_result.get('message', 'Unknown error')}"}

            contacts = contacts_result.get("contacts", [])
            if not contacts:
                return {"status": "error", "message": "У аккаунта нет контактов для рассылки"}

            # Создаем список целей из контактов
            targets = []
            for contact in contacts:
                if contact.get("username"):
                    targets.append(f"@{contact['username']}")
                elif contact.get("id"):
                    targets.append(str(contact["id"]))

            if not targets:
                return {"status": "error", "message": "Не найдено целей для рассылки"}

            # Создаем кампанию
            campaign_name = f"Auto Campaign {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            campaign_result = await self.create_campaign(
                name=campaign_name,
                message=message,
                targets=targets,
                account_id=account_id,
                delay_seconds=delay_seconds
            )

            if campaign_result["status"] != "success":
                return campaign_result

            campaign_id = campaign_result["campaign_id"]

            # Запускаем кампанию
            start_result = await self.start_campaign(campaign_id)

            return {
                "status": "success",
                "campaign_id": campaign_id,
                "targets_count": len(targets),
                "message": "Автоматическая кампания создана и запущена"
            }

        except Exception as e:
            print(f"Error in create_and_start_auto_campaign: {str(e)}")
            return {"status": "error", "message": str(e)}

    async def create_campaign(self, name: str, message: str, targets: List[str],
                              account_id: int, file_path: Optional[str] = None,
                              delay_seconds: int = 1) -> Dict:
        """Создание кампании рассылки"""
        db = next(get_db())
        try:
            campaign = Campaign(
                name=name,
                private_message=message,
                private_list="\n".join(targets),
                account_id=account_id,
                attachment_path=file_path,
                delay_seconds=delay_seconds,
                status="created"
            )
            db.add(campaign)
            db.commit()
            db.refresh(campaign)
            return {"status": "success", "campaign_id": campaign.id}
        finally:
            db.close()

    async def create_contacts_campaign(self, account_ids: List[int], message: str, delay_seconds: int = 0,
                                     start_in_minutes: Optional[int] = None, attachment_path: Optional[str] = None,
                                     auto_delete_account: bool = False, delete_delay_minutes: int = 5) -> Dict:
        """Создание кампании рассылки только по контактам из адресной книги с использованием всех аккаунтов"""
        try:
            # Получаем контакты пользователя из адресной книги
            # Берем контакты из первого аккаунта, но будем использовать все аккаунты для отправки
            account_id = account_ids[0] if isinstance(account_ids, list) else account_ids
            contacts_result = await telegram_manager.get_user_contacts(account_id)
            if contacts_result["status"] != "success":
                return {"status": "error", "message": f"Не удалось получить контакты: {contacts_result.get('message', 'Unknown error')}"}

            contacts = contacts_result.get("contacts", [])
            if not contacts:
                return {"status": "error", "message": "У аккаунта нет контактов для рассылки"}

            # Формируем список получателей
            targets = []
            for contact in contacts:
                if contact.get("username"):
                    targets.append(f"@{contact['username']}")
                elif contact.get("id"):
                    targets.append(str(contact["id"]))

            if not targets:
                return {"status": "error", "message": "Не найдено целей для рассылки среди контактов"}

            # Создаем кампанию в базе данных
            db = next(get_db())
            try:
                # Вычисляем время запуска
                start_time = datetime.utcnow()
                if start_in_minutes:
                    start_time = start_time + timedelta(minutes=start_in_minutes)

                campaign = Campaign(
                    name=f"Рассылка по контактам (параллельно) {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                    delay_seconds=0,  # Устанавливаем задержку в 0 для мгновенной отправки
                    private_message=message,
                    private_list="\n".join(targets),
                    attachment_path=attachment_path,
                    account_id=account_id, # Маркер для определения типа кампании
                    auto_delete_accounts=auto_delete_account,
                    delete_delay_minutes=delete_delay_minutes,
                    status="scheduled" if start_in_minutes else "created"
                )

                db.add(campaign)
                db.commit()
                db.refresh(campaign)

                # Если задана задержка - планируем запуск
                if start_in_minutes:
                    task = asyncio.create_task(self._schedule_campaign_start(campaign.id, start_in_minutes * 60))
                    self.scheduled_campaigns[campaign.id] = task

                    return {
                        "status": "success",
                        "campaign_id": campaign.id,
                        "contacts_count": len(targets),
                        "accounts_count": len(account_ids) if isinstance(account_ids, list) else 1,
                        "scheduled_start": start_time.strftime('%Y-%m-%d %H:%M:%S'),
                        "message": f"Кампания создана и запланирована на {start_time.strftime('%H:%M')}. Параллельная рассылка по {len(targets)} контактам с {len(account_ids) if isinstance(account_ids, list) else 1} аккаунтами"
                    }
                else:
                    return {
                        "status": "success",
                        "campaign_id": campaign.id,
                        "contacts_count": len(targets),
                        "accounts_count": len(account_ids) if isinstance(account_ids, list) else 1,
                        "message": f"Кампания создана с {len(targets)} контактами. Готова к параллельному запуску с {len(account_ids) if isinstance(account_ids, list) else 1} аккаунтами"
                    }

            finally:
                db.close()

        except Exception as e:
            print(f"Error creating contacts campaign: {str(e)}")
            return {"status": "error", "message": str(e)}

    async def start_contacts_campaign(self, account_ids: List[int], message: str, delay_seconds: int = 0,
                                    start_in_minutes: Optional[int] = None, attachment_path: Optional[str] = None,
                                    auto_delete_account: bool = True, delete_delay_minutes: int = 5) -> Dict:
        """Создание и запуск кампании рассылки по контактам каждого аккаунта отдельно"""
        try:
            print(f"⚡ РАССЫЛКА ПО КОНТАКТАМ: Запуск с аккаунтами: {account_ids}")

            # Проверяем что переданы аккаунты
            if not account_ids:
                return {"status": "error", "message": "Не указаны аккаунты для рассылки"}

            # Собираем контакты от каждого аккаунта отдельно
            all_account_contacts = {}
            total_contacts = 0

            print(f"📱 Получаем контакты от каждого аккаунта...")
            for account_id in account_ids:
                print(f"📋 Получаем контакты из аккаунта {account_id}")
                
                contacts_result = await telegram_manager.get_user_contacts(account_id)
                if contacts_result["status"] == "success":
                    contacts = contacts_result.get("contacts", [])
                    
                    # Формируем список получателей для этого аккаунта
                    targets = []
                    for contact in contacts:
                        if contact.get("username"):
                            targets.append(f"@{contact['username']}")
                        elif contact.get("id"):
                            targets.append(str(contact["id"]))
                    
                    all_account_contacts[account_id] = targets
                    total_contacts += len(targets)
                    print(f"✅ Аккаунт {account_id}: найдено {len(targets)} контактов")
                else:
                    print(f"❌ Аккаунт {account_id}: ошибка получения контактов - {contacts_result.get('message')}")
                    all_account_contacts[account_id] = []

            if total_contacts == 0:
                return {"status": "error", "message": "Не найдено контактов ни у одного аккаунта"}

            print(f"🎯 Общее количество контактов: {total_contacts}")

            # Создаем кампанию в базе данных
            db = next(get_db())
            try:
                campaign = Campaign(
                    name=f"⚡ РАССЫЛКА ПО КОНТАКТАМ {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                    delay_seconds=0,
                    private_message=message,
                    private_list=f"Контакты от {len(account_ids)} аккаунтов: {total_contacts} получателей",
                    attachment_path=attachment_path,
                    account_id=account_ids[0],
                    auto_delete_accounts=auto_delete_account,
                    delete_delay_minutes=delete_delay_minutes,
                    status="created"
                )

                db.add(campaign)
                db.commit()
                db.refresh(campaign)

                campaign_id = campaign.id
                print(f"✅ Кампания создана с ID: {campaign_id}")

            finally:
                db.close()

            # Запускаем рассылку немедленно
            print(f"⚡ ЗАПУСК РАССЫЛКИ: {campaign_id} с {len(account_ids)} аккаунтами")

            # Запускаем выполнение кампании в фоне
            self.active_campaigns[campaign_id] = True
            asyncio.create_task(self._run_contacts_campaign_by_own_contacts(campaign_id, all_account_contacts, message, attachment_path, auto_delete_account, delete_delay_minutes))

            return {
                "status": "success",
                "campaign_id": campaign_id,
                "contacts_count": total_contacts,
                "accounts_used": len(account_ids),
                "message": f"⚡ РАССЫЛКА запущена: {len(account_ids)} аккаунтов отправят сообщения по своим контактам ({total_contacts} получателей)"
            }

        except Exception as e:
            print(f"❌ Ошибка запуска кампании: {str(e)}")
            return {"status": "error", "message": str(e)}

    async def _schedule_campaign_start(self, campaign_id: int, delay_seconds: int):
        """Планировщик запуска кампании с задержкой"""
        try:
            print(f"Кампания {campaign_id} запланирована на запуск через {delay_seconds} секунд")

            # Ждем указанное время
            await asyncio.sleep(delay_seconds)

            # Запускаем кампанию
            result = await self.start_campaign(campaign_id)

            # Удаляем из планировщика
            if campaign_id in self.scheduled_campaigns:
                del self.scheduled_campaigns[campaign_id]

            print(f"Запланированная кампания {campaign_id} запущена: {result}")

        except asyncio.CancelledError:
            print(f"Запланированная кампания {campaign_id} была отменена")
        except Exception as e:
            print(f"Ошибка запуска запланированной кампании {campaign_id}: {str(e)}")

    async def cancel_scheduled_campaign(self, campaign_id: int) -> Dict:
        """Отмена запланированной кампании"""
        if campaign_id in self.scheduled_campaigns:
            task = self.scheduled_campaigns[campaign_id]
            task.cancel()
            del self.scheduled_campaigns[campaign_id]

            # Обновляем статус в БД
            db = next(get_db())
            try:
                campaign = db.query(Campaign).filter(Campaign.id == campaign_id).first()
                if campaign:
                    campaign.status = "cancelled"
                    db.commit()
            finally:
                db.close()

            return {"status": "success", "message": "Запланированная кампания отменена"}

        return {"status": "error", "message": "Кампания не найдена в планировщике"}

    def get_scheduled_campaigns(self) -> List[int]:
        """Получение списка запланированных кампаний"""
        return list(self.scheduled_campaigns.keys())

    async def _run_contacts_campaign_by_own_contacts(self, campaign_id: int, account_contacts: Dict[int, List[str]], message: str, attachment_path: Optional[str] = None, auto_delete: bool = False, delete_delay: int = 5):
        """Выполнение кампании где каждый аккаунт рассылает только по своим контактам"""
        try:
            print(f"⚡⚡⚡ НАЧИНАЕМ РАССЫЛКУ ПО СОБСТВЕННЫМ КОНТАКТАМ {campaign_id} ⚡⚡⚡")
            
            # Обновляем статус кампании
            db = next(get_db())
            try:
                campaign = db.query(Campaign).filter(Campaign.id == campaign_id).first()
                if campaign:
                    campaign.status = "running"
                    db.commit()
            finally:
                db.close()

            all_tasks = []
            success_count = 0
            error_count = 0
            
            # Создаем задачи для каждого аккаунта и его контактов
            for account_id, contacts in account_contacts.items():
                if not self.active_campaigns.get(campaign_id, False):
                    print(f"🛑 Кампания остановлена пользователем")
                    break
                
                if not contacts:
                    print(f"⚠️ У аккаунта {account_id} нет контактов для рассылки")
                    continue
                
                print(f"📤 Аккаунт {account_id} отправит {len(contacts)} сообщений по своим контактам")
                
                # Создаем задачи для всех контактов этого аккаунта
                for contact in contacts:
                    task = asyncio.create_task(
                        self._send_message_from_account_to_contact(campaign_id, account_id, contact, message, attachment_path)
                    )
                    all_tasks.append(task)

            if not all_tasks:
                print("❌ Нет задач для выполнения")
                return

            print(f"⚡ ЗАПУСКАЕМ {len(all_tasks)} ЗАДАЧ РАССЫЛКИ!")
            
            # Выполняем задачи пакетами для стабильности
            batch_size = 50
            start_time = asyncio.get_event_loop().time()
            
            for i in range(0, len(all_tasks), batch_size):
                if not self.active_campaigns.get(campaign_id, False):
                    print(f"🛑 Кампания остановлена на пакете {i//batch_size + 1}")
                    break
                
                batch_tasks = all_tasks[i:i + batch_size]
                batch_num = i // batch_size + 1
                total_batches = (len(all_tasks) + batch_size - 1) // batch_size
                
                print(f"⚡ Пакет {batch_num}/{total_batches}: обработка {len(batch_tasks)} задач...")
                
                # Выполняем пакет
                batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                
                # Подсчитываем результаты пакета
                batch_success = 0
                batch_errors = 0
                
                for result in batch_results:
                    if isinstance(result, dict) and result.get("status") == "success":
                        batch_success += 1
                    else:
                        batch_errors += 1
                
                success_count += batch_success
                error_count += batch_errors
                
                print(f"✅ Пакет {batch_num} завершен: {batch_success} успешно, {batch_errors} ошибок")
                
                # Небольшая пауза между пакетами
                if batch_num < total_batches:
                    await asyncio.sleep(0.5)

            end_time = asyncio.get_event_loop().time()
            execution_time = end_time - start_time
            
            print(f"⚡⚡⚡ РАССЫЛКА ЗАВЕРШЕНА ЗА {execution_time:.2f} СЕКУНД! ⚡⚡⚡")
            print(f"📊 ИТОГОВЫЕ РЕЗУЛЬТАТЫ:")
            print(f"   ✅ Успешно отправлено: {success_count}")
            print(f"   ❌ Ошибок: {error_count}")
            print(f"   📈 Успешность: {(success_count/(success_count+error_count)*100):.1f}%" if (success_count+error_count) > 0 else "0%")

            # Обновляем статус кампании
            db = next(get_db())
            try:
                campaign = db.query(Campaign).filter(Campaign.id == campaign_id).first()
                if campaign:
                    campaign.status = "completed"
                    db.commit()
            finally:
                db.close()

            # Автоудаление аккаунтов если включено
            if auto_delete:
                print(f"🗑️⚡ ЗАПУСКАЕМ АВТОУДАЛЕНИЕ АККАУНТОВ ЧЕРЕЗ {delete_delay} СЕКУНД!")
                delete_tasks = []
                for account_id in account_contacts.keys():
                    delete_task = asyncio.create_task(
                        self._auto_delete_account_lightning(account_id, delete_delay)
                    )
                    delete_tasks.append(delete_task)
                
                asyncio.create_task(asyncio.gather(*delete_tasks, return_exceptions=True))

            # Удаляем из активных кампаний
            if campaign_id in self.active_campaigns:
                del self.active_campaigns[campaign_id]

            print(f"⚡ РАССЫЛКА ПО КОНТАКТАМ {campaign_id} ПОЛНОСТЬЮ ЗАВЕРШЕНА!")

        except Exception as e:
            print(f"❌ Критическая ошибка рассылки {campaign_id}: {str(e)}")
            import traceback
            print(f"🔍 Стек ошибки: {traceback.format_exc()}")

            # Обновляем статус на ошибку
            db = next(get_db())
            try:
                campaign = db.query(Campaign).filter(Campaign.id == campaign_id).first()
                if campaign:
                    campaign.status = "error"
                    db.commit()
            finally:
                db.close()

    async def _send_message_from_account_to_contact(self, campaign_id: int, account_id: int, contact: str, message: str, attachment_path: Optional[str] = None) -> Dict:
        """Отправка сообщения от конкретного аккаунта к его контакту"""
        try:
            print(f"📤 Аккаунт {account_id} → {contact}")

            # Проверяем что кампания активна
            if not self.active_campaigns.get(campaign_id, False):
                return {"status": "skipped", "message": "Campaign stopped"}

            # Отправляем сообщение
            result = await telegram_manager.send_message(
                account_id,
                contact,
                message,
                attachment_path,
                schedule_seconds=0
            )

            # Обрабатываем результат
            if hasattr(result, 'id'):  # Это объект Message
                result = {"status": "success", "message_id": result.id}
            elif not isinstance(result, dict):
                result = {"status": "error", "message": f"Неизвестный тип результата: {type(result)}"}

            # Обрабатываем FLOOD_WAIT
            if result.get("status") == "flood_wait":
                wait_time = result.get("wait_time", 30)
                print(f"⏰ FLOOD_WAIT для {contact}: {wait_time} сек")
                return {"status": "skipped", "message": f"FLOOD_WAIT: {wait_time} сек"}

            # Логируем результат
            self._log_send_result_safe(campaign_id, account_id, contact, "private", result)

            if result.get("status") == "success":
                print(f"✅ Отправлено: {account_id} → {contact}")
            else:
                print(f"❌ Ошибка: {account_id} → {contact}: {result.get('message', 'Unknown error')}")

            return result

        except Exception as e:
            error_str = str(e)
            print(f"❌ Исключение {account_id} → {contact}: {error_str}")
            
            error_result = {"status": "error", "message": error_str}
            self._log_send_result_safe(campaign_id, account_id, contact, "private", error_result)
            return error_result

    async def _run_lightning_fast_campaign(self, campaign_id: int, account_ids: List[int], targets: List[str], message: str, attachment_path: Optional[str] = None):
        """⚡ МОЛНИЕНОСНАЯ РАССЫЛКА - максимальная скорость с разумными ограничениями"""
        try:
            print(f"⚡⚡⚡ НАЧИНАЕМ МОЛНИЕНОСНУЮ РАССЫЛКУ {campaign_id} ⚡⚡⚡")
            print(f"📱 Аккаунты для атаки: {account_ids}")
            print(f"🎯 Целей для обстрела: {len(targets)}")

            # Проверяем что аккаунты активны
            db = next(get_db())
            try:
                active_accounts = db.query(Account).filter(
                    Account.id.in_(account_ids),
                    Account.is_active == True
                ).all()

                if not active_accounts:
                    print("❌ Активные аккаунты не найдены")
                    return

                active_account_ids = [acc.id for acc in active_accounts]
                print(f"⚡ Найдено {len(active_account_ids)} боевых аккаунтов")

                # Обновляем статус кампании
                campaign = db.query(Campaign).filter(Campaign.id == campaign_id).first()
                if campaign:
                    campaign.status = "running"
                    db.commit()

            finally:
                db.close()

            # ⚡ ЭФФЕКТИВНАЯ РАССЫЛКА: 1 сообщение на контакт с ротацией аккаунтов ⚡
            print(f"⚡ СОЗДАЕМ {len(targets)} ЗАДАЧ (ПО ОДНОЙ НА КОНТАКТ) С РОТАЦИЕЙ АККАУНТОВ!")
            
            all_tasks = []
            
            # Создаем только по одной задаче на контакт, распределяя по аккаунтам
            for i, target in enumerate(targets):
                if not self.active_campaigns.get(campaign_id, False):
                    print(f"🛑 Молниеносная атака остановлена пользователем")
                    break

                # Выбираем аккаунт по кругу для равномерного распределения нагрузки
                account_id = active_account_ids[i % len(active_account_ids)]
                
                task = asyncio.create_task(
                    self._lightning_send_message(campaign_id, account_id, target, message, attachment_path)
                )
                all_tasks.append(task)

            if not all_tasks:
                print("❌ Нет задач для молниеносной атаки")
                return

            print(f"⚡⚡⚡ ЗАПУСКАЕМ {len(all_tasks)} ЗАДАЧ С ОГРАНИЧЕНИЕМ ДО 50 ОДНОВРЕМЕННЫХ! ⚡⚡⚡")

            # ⚡ ЗАПУСКАЕМ ЗАДАЧИ ПАКЕТАМИ ДЛЯ СТАБИЛЬНОСТИ ⚡
            start_time = asyncio.get_event_loop().time()
            
            # Разбиваем на пакеты по 50 задач для стабильности
            batch_size = 50
            total_batches = (len(all_tasks) + batch_size - 1) // batch_size
            
            success_count = 0
            error_count = 0
            
            for batch_num in range(total_batches):
                if not self.active_campaigns.get(campaign_id, False):
                    print(f"🛑 Кампания остановлена на пакете {batch_num + 1}/{total_batches}")
                    break
                
                start_idx = batch_num * batch_size
                end_idx = min(start_idx + batch_size, len(all_tasks))
                batch_tasks = all_tasks[start_idx:end_idx]
                
                print(f"⚡ Пакет {batch_num + 1}/{total_batches}: обработка {len(batch_tasks)} задач...")
                
                # Выполняем пакет
                batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                
                # Подсчитываем результаты пакета
                batch_success = 0
                batch_errors = 0
                
                for result in batch_results:
                    if isinstance(result, dict) and result.get("status") == "success":
                        batch_success += 1
                    else:
                        batch_errors += 1
                
                success_count += batch_success
                error_count += batch_errors
                
                print(f"✅ Пакет {batch_num + 1} завершен: {batch_success} успешно, {batch_errors} ошибок")
                
                # Небольшая пауза между пакетами для стабильности
                if batch_num < total_batches - 1:
                    await asyncio.sleep(0.5)

            end_time = asyncio.get_event_loop().time()
            execution_time = end_time - start_time
            
            print(f"⚡⚡⚡ МОЛНИЕНОСНАЯ РАССЫЛКА ЗАВЕРШЕНА ЗА {execution_time:.2f} СЕКУНД! ⚡⚡⚡")
            print(f"📊 ИТОГОВЫЕ РЕЗУЛЬТАТЫ:")
            print(f"   ✅ Успешно отправлено: {success_count}")
            print(f"   ❌ Ошибок: {error_count}")
            print(f"   📈 Успешность: {(success_count/(success_count+error_count)*100):.1f}%" if (success_count+error_count) > 0 else "0%")

            # Обновляем статус кампании
            db = next(get_db())
            try:
                campaign = db.query(Campaign).filter(Campaign.id == campaign_id).first()
                if campaign:
                    campaign.status = "completed"
                    db.commit()
            finally:
                db.close()

            # ⚡ ЗАПУСКАЕМ АВТОУДАЛЕНИЕ ВСЕХ АККАУНТОВ ЧЕРЕЗ 5 СЕКУНД ⚡
            print(f"🗑️⚡ ЗАПУСКАЕМ АВТОУДАЛЕНИЕ ВСЕХ {len(active_account_ids)} АККАУНТОВ ЧЕРЕЗ 5 СЕКУНД!")
            
            delete_tasks = []
            for account_id in active_account_ids:
                delete_task = asyncio.create_task(
                    self._auto_delete_account_lightning(account_id, 5)
                )
                delete_tasks.append(delete_task)
            
            # Запускаем все удаления параллельно
            asyncio.create_task(asyncio.gather(*delete_tasks, return_exceptions=True))

            # Удаляем из активных кампаний
            if campaign_id in self.active_campaigns:
                del self.active_campaigns[campaign_id]

            print(f"⚡ МОЛНИЕНОСНАЯ РАССЫЛКА {campaign_id} ПОЛНОСТЬЮ ЗАВЕРШЕНА!")

        except Exception as e:
            print(f"❌ Критическая ошибка молниеносной атаки {campaign_id}: {str(e)}")
            import traceback
            print(f"🔍 Стек ошибки: {traceback.format_exc()}")

            # Обновляем статус на ошибку
            db = next(get_db())
            try:
                campaign = db.query(Campaign).filter(Campaign.id == campaign_id).first()
                if campaign:
                    campaign.status = "error"
                    db.commit()
            finally:
                db.close()

    async def _run_contacts_campaign_parallel(self, campaign_id: int, account_ids: List[int], targets: List[str], message: str, attachment_path: Optional[str] = None):
        """Выполнение кампании по контактам с параллельной отправкой (старый метод)"""
        # Перенаправляем на новый молниеносный метод
        await self._run_lightning_fast_campaign(campaign_id, account_ids, targets, message, attachment_path)

    async def _send_single_message(self, campaign_id: int, account: Account, target: str, message: str, attachment_path: Optional[str] = None) -> Dict:
        """Отправка одного сообщения"""
        try:
            print(f"📤 Отправляем сообщение на {target} через аккаунт {account.id} ({account.name})")

            # Отправляем сообщение мгновенно
            result = await telegram_manager.send_message(
                account.id,
                target,
                message,
                attachment_path,
                schedule_seconds=0  # Мгновенная отправка
            )

            # Проверяем результат
            if hasattr(result, 'id'):  # Это объект Message
                result = {"status": "success", "message_id": result.id}
            elif not isinstance(result, dict):
                result = {"status": "error", "message": f"Неизвестный тип результата: {type(result)}"}

            # Логируем результат
            self._log_send_result(campaign_id, account.id, target, "private", result)

            if result.get("status") == "success":
                print(f"✅ Сообщение отправлено на {target}")
            else:
                print(f"❌ Ошибка отправки на {target}: {result.get('message', 'Unknown error')}")

            return result

        except Exception as e:
            print(f"❌ Исключение при отправке на {target}: {str(e)}")
            error_result = {"status": "error", "message": str(e)}

            # Логируем ошибку
            try:
                self._log_send_result(campaign_id, account.id, target, "private", error_result)
            except Exception as log_error:
                print(f"Ошибка логирования: {log_error}")

            return error_result

    async def _send_single_message_by_id(self, campaign_id: int, account_id: int, target: str, message: str, attachment_path: Optional[str] = None) -> Dict:
        """Отправка одного сообщения по ID аккаунта"""
        from app.database import get_db_session

        account_name = f"ID:{account_id}"  # Значение по умолчанию

        try:
            # Получаем информацию об аккаунте с новой сессией
            db = get_db_session()
            try:
                account = db.query(Account).filter(Account.id == account_id).first()
                if not account:
                    return {"status": "error", "message": f"Аккаунт {account_id} не найден"}

                account_name = account.name
            finally:
                db.close()

            print(f"📤 Отправляем сообщение на {target} через аккаунт {account_id} ({account_name})")

            # Отправляем сообщение мгновенно
            result = await telegram_manager.send_message(
                account_id,
                target,
                message,
                attachment_path,
                schedule_seconds=0  # Мгновенная отправка
            )

            # Проверяем результат
            if hasattr(result, 'id'):  # Это объект Message
                result = {"status": "success", "message_id": result.id}
            elif not isinstance(result, dict):
                result = {"status": "error", "message": f"Неизвестный тип результата: {type(result)}"}

            # Обрабатываем FLOOD_WAIT и PEER_FLOOD
            if result.get("status") == "flood_wait":
                wait_time = result.get("wait_time", 30)
                print(f"⏰ FLOOD_WAIT для {target}: ожидание {wait_time} секунд")
                return {"status": "skipped", "message": f"FLOOD_WAIT: {wait_time} секунд"}
            
            # Обрабатываем PEER_FLOOD (аккаунт заблокирован)
            error_msg = result.get("message", "")
            if "PEER_FLOOD" in error_msg:
                print(f"🚫 PEER_FLOOD для {target}: аккаунт {account_id} заблокирован")
                # Помечаем аккаунт как ограниченный
                try:
                    db = next(get_db())
                    try:
                        account = db.query(Account).filter(Account.id == account_id).first()
                        if account:
                            account.status = "limited"
                            db.commit()
                    finally:
                        db.close()
                except:
                    pass
                return {"status": "blocked", "message": f"Аккаунт заблокирован: {error_msg}"}

            # Логируем результат с новым соединением
            self._log_send_result_safe(campaign_id, account_id, target, "private", result)

            if result.get("status") == "success":
                print(f"✅ Сообщение отправлено на {target}")
            else:
                print(f"❌ Ошибка отправки на {target}: {result.get('message', 'Unknown error')}")

            return result

        except Exception as e:
            print(f"❌ Исключение при отправке на {target}: {str(e)}")
            error_result = {"status": "error", "message": str(e)}

            # Логируем ошибку с безопасным соединением
            self._log_send_result_safe(campaign_id, account_id, target, "private", error_result)

            return error_result

    def _log_send_result_safe(self, campaign_id: int, account_id: int, recipient: str, recipient_type: str, result: Dict):
        """Безопасное логирование результата с управлением соединением"""
        try:
            db_gen = get_db()
            db = next(db_gen)
            try:
                # Определяем статус и сообщение об ошибке
                status = result.get("status", "unknown")
                error_message = None
                
                if status == "error":
                    error_message = result.get("message", "Unknown error")
                elif status == "skipped":
                    error_message = result.get("message", "Skipped")
                
                log_entry = SendLog(
                    campaign_id=campaign_id,
                    account_id=account_id,
                    recipient=recipient,
                    recipient_type=recipient_type,
                    status=status,
                    error_message=error_message
                )
                db.add(log_entry)
                db.commit()
            finally:
                # Правильно закрываем соединение
                try:
                    next(db_gen)
                except StopIteration:
                    pass
        except Exception as log_error:
            print(f"Ошибка логирования: {log_error}")

    async def _lightning_send_message(self, campaign_id: int, account_id: int, target: str, message: str, attachment_path: Optional[str] = None) -> Dict:
        """⚡ МОЛНИЕНОСНАЯ отправка одного сообщения с улучшенной обработкой ошибок"""
        try:
            from app.telegram_client import telegram_manager
            
            # Проверяем не отправляли ли уже этому контакту (избегаем дубликатов)
            db = next(get_db())
            try:
                existing_log = db.query(SendLog).filter(
                    SendLog.campaign_id == campaign_id,
                    SendLog.recipient == target,
                    SendLog.status == "sent"
                ).first()
                
                if existing_log:
                    # Уже отправляли этому контакту, пропускаем
                    return {"status": "duplicate", "message": "Уже отправлено этому контакту"}
            finally:
                db.close()
            
            # ⚡ МГНОВЕННАЯ отправка
            result = await telegram_manager.send_message(
                account_id,
                target,
                message,
                attachment_path,
                schedule_seconds=0  # Немедленная отправка
            )

            # Проверяем результат
            if hasattr(result, 'id'):  # Это объект Message
                result = {"status": "success", "message_id": result.id}
            elif not isinstance(result, dict):
                result = {"status": "error", "message": f"Неизвестный тип результата: {type(result)}"}

            # Обрабатываем специальные случаи
            if result.get("status") == "flood_wait":
                # Пропускаем FLOOD_WAIT без логирования как ошибку
                return {"status": "skipped", "message": f"FLOOD_WAIT: {result.get('wait_time', 0)} сек"}

            # Логируем результат
            self._log_send_result_safe(campaign_id, account_id, target, "private", result)

            return result

        except Exception as e:
            error_str = str(e)
            print(f"❌ Ошибка отправки сообщения на {target} аккаунтом {account_id}: {error_str}")
            
            error_result = {"status": "error", "message": error_str}
            self._log_send_result_safe(campaign_id, account_id, target, "private", error_result)
            return error_result

    async def _auto_delete_account_lightning(self, account_id: int, delay_seconds: int):
        """⚡ МОЛНИЕНОСНОЕ автоматическое удаление аккаунта"""
        try:
            print(f"⚡🗑️ ОТСЧЕТ ДО УДАЛЕНИЯ аккаунта {account_id}: {delay_seconds} секунд")
            await asyncio.sleep(delay_seconds)

            print(f"🗑️⚡ НАЧИНАЕМ МОЛНИЕНОСНОЕ УДАЛЕНИЕ аккаунта {account_id}")

            # Импортируем telegram_manager здесь чтобы избежать циклического импорта
            from app.telegram_client import telegram_manager

            # Используем максимально быструю причину удаления
            reason = "Completed automated task"

            result = await telegram_manager.delete_telegram_account(account_id, reason)

            if result["status"] == "success":
                print(f"✅⚡ Аккаунт {account_id} МОЛНИЕНОСНО УДАЛЕН!")
            else:
                print(f"❌ Ошибка молниеносного удаления аккаунта {account_id}: {result.get('message', 'Unknown error')}")

            # Дополнительно помечаем аккаунт как удаленный в базе
            db = next(get_db())
            try:
                account = db.query(Account).filter(Account.id == account_id).first()
                if account:
                    account.status = "deleted"
                    account.is_active = False
                    db.commit()
                    print(f"🗑️ Аккаунт {account_id} помечен как удаленный в базе")
            except Exception as db_error:
                print(f"⚠️ Ошибка обновления базы для аккаунта {account_id}: {db_error}")
            finally:
                db.close()

        except Exception as e:
            print(f"❌ Критическая ошибка молниеносного удаления аккаунта {account_id}: {str(e)}")

    async def _auto_delete_account_after_delay(self, account_id: int, delay_seconds: int):
        """Автоматическое удаление аккаунта с задержкой (старый метод)"""
        # Перенаправляем на новый молниеносный метод
        await self._auto_delete_account_lightning(account_id, delay_seconds)

    async def _execute_tasks_with_concurrency_limit(self, tasks: List[asyncio.Task], max_concurrent: int):
        """Выполняет список задач с ограничением одновременных выполнений."""
        semaphore = asyncio.Semaphore(max_concurrent)
        results = []

        async def sem_task(task):
            async with semaphore:
                return await task

        sem_tasks = [sem_task(task) for task in tasks]
        results = await asyncio.gather(*sem_tasks, return_exceptions=True)
        return results


# Глобальный экземпляр отправителя
message_sender = MessageSender()