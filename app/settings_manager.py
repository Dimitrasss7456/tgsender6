
import json
import os
from typing import Dict, Any
from dataclasses import dataclass, asdict
from app.config import UPLOADS_DIR

@dataclass
class DelaySettings:
    min_delay: int = 3
    max_delay: int = 7
    series_pause: int = 10
    messages_per_series: int = 50
    pm_delay: int = 10
    group_delay: int = 5
    channel_delay: int = 3

@dataclass
class AccountSettings:
    enable_rotation: bool = True
    switch_after_messages: int = 100
    account_pause: int = 5
    enable_parallel: bool = False
    max_parallel_accounts: int = 3

@dataclass
class ContentSettings:
    enable_synonyms: bool = False
    shuffle_sentences: bool = False
    random_emoji: bool = False
    invisible_chars: bool = False
    modify_images: bool = False
    resize_images: bool = False
    resize_percent: int = 2

@dataclass
class LimitSettings:
    allow_pm: bool = True
    pm_per_hour: int = 10
    pm_per_day: int = 50
    allow_groups: bool = True
    groups_per_hour: int = 30
    groups_per_day: int = 200
    allow_channels: bool = True
    channels_per_hour: int = 20
    channels_per_day: int = 100

@dataclass
class BehaviorSettings:
    enable_warmup: bool = False
    auto_subscribe: bool = False
    auto_like: bool = False
    chat_with_bots: bool = False
    warmup_actions_per_day: int = 20
    typing_effect: bool = True
    typing_duration: int = 3
    random_interactions: bool = False
    soft_mode: bool = True
    reply_mode: bool = False
    old_chats_only: bool = False

@dataclass
class ErrorSettings:
    auto_flood_handling: bool = True
    auto_account_switch: bool = True
    flood_wait_multiplier: float = 1.2

@dataclass
class AntiSpamSettings:
    delays: DelaySettings
    accounts: AccountSettings
    content: ContentSettings
    limits: LimitSettings
    behavior: BehaviorSettings
    errors: ErrorSettings

class SettingsManager:
    def __init__(self):
        self.settings_file = os.path.join(UPLOADS_DIR, "antispam_settings.json")
        self.settings = self.load_settings()

    def load_settings(self) -> AntiSpamSettings:
        """Загружает настройки из файла или создает стандартные"""
        try:
            if os.path.exists(self.settings_file):
                with open(self.settings_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                return self._dict_to_settings(data)
            else:
                return self._create_default_settings()
        except Exception as e:
            print(f"Ошибка загрузки настроек: {e}")
            return self._create_default_settings()

    def _create_default_settings(self) -> AntiSpamSettings:
        """Создает стандартные настройки"""
        return AntiSpamSettings(
            delays=DelaySettings(),
            accounts=AccountSettings(),
            content=ContentSettings(),
            limits=LimitSettings(),
            behavior=BehaviorSettings(),
            errors=ErrorSettings()
        )

    def _dict_to_settings(self, data: Dict[str, Any]) -> AntiSpamSettings:
        """Преобразует словарь в объект настроек"""
        try:
            return AntiSpamSettings(
                delays=DelaySettings(**data.get('delays', {})),
                accounts=AccountSettings(**data.get('accounts', {})),
                content=ContentSettings(**data.get('content', {})),
                limits=LimitSettings(**data.get('limits', {})),
                behavior=BehaviorSettings(**data.get('behavior', {})),
                errors=ErrorSettings(**data.get('errors', {}))
            )
        except Exception:
            return self._create_default_settings()

    def save_settings(self) -> bool:
        """Сохраняет настройки в файл"""
        try:
            os.makedirs(UPLOADS_DIR, exist_ok=True)
            with open(self.settings_file, 'w', encoding='utf-8') as f:
                json.dump(asdict(self.settings), f, ensure_ascii=False, indent=2)
            return True
        except Exception as e:
            print(f"Ошибка сохранения настроек: {e}")
            return False

    def update_section(self, section: str, data: Dict[str, Any]) -> bool:
        """Обновляет определенную секцию настроек"""
        try:
            if hasattr(self.settings, section):
                section_obj = getattr(self.settings, section)
                for key, value in data.items():
                    if hasattr(section_obj, key):
                        setattr(section_obj, key, value)
                return self.save_settings()
            return False
        except Exception as e:
            print(f"Ошибка обновления секции {section}: {e}")
            return False

    def update_all_settings(self, data: Dict[str, Any]) -> bool:
        """Обновляет все настройки"""
        try:
            self.settings = self._dict_to_settings(data)
            return self.save_settings()
        except Exception as e:
            print(f"Ошибка обновления всех настроек: {e}")
            return False

    def reset_to_defaults(self) -> bool:
        """Сбрасывает настройки к стандартным значениям"""
        try:
            self.settings = self._create_default_settings()
            return self.save_settings()
        except Exception as e:
            print(f"Ошибка сброса настроек: {e}")
            return False

    def get_settings_dict(self) -> Dict[str, Any]:
        """Возвращает настройки в виде словаря"""
        return asdict(self.settings)

    def get_delay_for_chat_type(self, chat_type: str) -> int:
        """Возвращает задержку для определенного типа чата"""
        if chat_type == 'pm':
            return self.settings.delays.pm_delay
        elif chat_type == 'group':
            return self.settings.delays.group_delay
        elif chat_type == 'channel':
            return self.settings.delays.channel_delay
        else:
            return self.settings.delays.min_delay

    def is_chat_type_allowed(self, chat_type: str) -> bool:
        """Проверяет, разрешена ли отправка в определенный тип чата"""
        if chat_type == 'pm':
            return self.settings.limits.allow_pm
        elif chat_type == 'group':
            return self.settings.limits.allow_groups
        elif chat_type == 'channel':
            return self.settings.limits.allow_channels
        else:
            return True

    def get_limit_for_chat_type(self, chat_type: str, period: str = 'hour') -> int:
        """Возвращает лимит для определенного типа чата и периода"""
        limits = self.settings.limits
        
        if chat_type == 'pm':
            return limits.pm_per_hour if period == 'hour' else limits.pm_per_day
        elif chat_type == 'group':
            return limits.groups_per_hour if period == 'hour' else limits.groups_per_day
        elif chat_type == 'channel':
            return limits.channels_per_hour if period == 'hour' else limits.channels_per_day
        else:
            return 100 if period == 'hour' else 500

# Глобальный экземпляр менеджера настроек
settings_manager = SettingsManager()
