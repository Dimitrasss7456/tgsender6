import os
import json
from typing import List, Optional
from datetime import datetime, timedelta
from fastapi import FastAPI, Request, Form, File, UploadFile, Depends, HTTPException, status
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from app.database import Account, Campaign, SendLog, User, UserSession, get_db
from app.telegram_client import telegram_manager
from app.sender import message_sender
from app.proxy_manager import proxy_manager
from app.settings_manager import settings_manager
from app.config import UPLOADS_DIR
from app.auth import (
    get_current_user, get_current_admin, authenticate_user,
    create_session_token, invalidate_session, create_admin_user_if_not_exists
)
import asyncio

# Создаем приложение FastAPI в самом начале
app = FastAPI(title="Telegram Mass Sender")

# Создаем папки для статики и шаблонов
os.makedirs("static", exist_ok=True)
os.makedirs("templates", exist_ok=True)

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# Страницы аутентификации

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    """Страница входа"""
    return templates.TemplateResponse("login.html", {"request": request})

@app.post("/login")
async def login(request: Request, username: str = Form(...), password: str = Form(...), db: Session = Depends(get_db)):
    """Обработка входа"""
    # Создаем админа если его нет
    create_admin_user_if_not_exists(db)

    user = authenticate_user(username, password, db)
    if not user:
        return templates.TemplateResponse("login.html", {
            "request": request,
            "error": "Неверный логин или пароль"
        })

    # Обновляем время последнего входа
    user.last_login = datetime.utcnow()
    db.commit()

    # Создаем сессию
    user_agent = request.headers.get("user-agent", "")
    client_ip = request.client.host if request.client else ""
    token = create_session_token(user.id, db, user_agent, client_ip)

    # Перенаправляем на главную страницу с установкой cookie
    response = RedirectResponse(url="/", status_code=303)
    response.set_cookie(
        key="session_token",
        value=token,
        max_age=30*24*60*60,  # 30 дней
        httponly=True,
        secure=False  # Для development
    )
    return response

@app.get("/logout")
async def logout(request: Request, db: Session = Depends(get_db)):
    """Выход из системы"""
    token = request.cookies.get("session_token")
    if token:
        invalidate_session(token, db)

    response = RedirectResponse(url="/login", status_code=303)
    response.delete_cookie("session_token")
    return response

# Панель администратора

@app.get("/admin", response_class=HTMLResponse)
async def admin_panel(request: Request, db: Session = Depends(get_db), admin_user: User = Depends(get_current_admin)):
    """Панель администратора"""
    users = db.query(User).all()
    return templates.TemplateResponse("admin.html", {
        "request": request,
        "users": users,
        "current_user": admin_user
    })

@app.post("/admin/users")
async def create_user(
    username: str = Form(...),
    password: str = Form(...),
    is_admin: bool = Form(False),
    db: Session = Depends(get_db),
    admin_user: User = Depends(get_current_admin)
):
    """Создание нового пользователя"""
    # Проверяем что пользователь с таким именем не существует
    existing_user = db.query(User).filter(User.username == username).first()
    if existing_user:
        return JSONResponse({"status": "error", "message": "Пользователь с таким именем уже существует"})

    user = User(
        username=username,
        is_admin=bool(is_admin),
        is_active=True
    )
    user.set_password(password)

    db.add(user)
    db.commit()

    return JSONResponse({"status": "success", "message": "Пользователь создан"})

@app.delete("/admin/users/{user_id}")
async def delete_user(
    user_id: int,
    db: Session = Depends(get_db),
    admin_user: User = Depends(get_current_admin)
):
    """Удаление пользователя"""
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        return JSONResponse({"status": "error", "message": "Пользователь не найден"})

    if user.id == admin_user.id:
        return JSONResponse({"status": "error", "message": "Нельзя удалить самого себя"})

    # Удаляем все связанные данные
    db.query(UserSession).filter(UserSession.user_id == user_id).delete()
    db.query(Account).filter(Account.user_id == user_id).delete()

    db.delete(user)
    db.commit()

    return JSONResponse({"status": "success", "message": "Пользователь удален"})

@app.post("/admin/users/{user_id}/toggle")
async def toggle_user(
    user_id: int,
    db: Session = Depends(get_db),
    admin_user: User = Depends(get_current_admin)
):
    """Активация/деактивация пользователя"""
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        return JSONResponse({"status": "error", "message": "Пользователь не найден"})

    if user.id == admin_user.id:
        return JSONResponse({"status": "error", "message": "Нельзя деактивировать самого себя"})

    user.is_active = not user.is_active
    db.commit()

    return JSONResponse({"status": "success", "message": f"Пользователь {'активирован' if user.is_active else 'деактивирован'}"})


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request, db: Session = Depends(get_db)):
    """Главная страница dashboard с проверкой авторизации"""
    # Проверяем авторизацию пользователя
    try:
        current_user = get_current_user(request, None, db)
    except HTTPException:
        # Если пользователь не авторизован, перенаправляем на страницу входа
        return RedirectResponse(url="/login", status_code=303)

    # Фильтруем аккаунты по текущему пользователю (админ видит все)
    if current_user.is_admin:
        accounts = db.query(Account).all()
        campaigns = db.query(Campaign).order_by(Campaign.created_at.desc()).limit(10).all()
    else:
        accounts = db.query(Account).filter(Account.user_id == current_user.id).all()
        campaigns = db.query(Campaign).filter(Campaign.account_id.in_([a.id for a in accounts])).order_by(Campaign.created_at.desc()).limit(10).all()

    # Базовая статистика
    total_accounts = len(accounts)
    active_accounts = len([a for a in accounts if a.is_active and a.status == "online"])
    total_campaigns = len(campaigns)
    messages_sent_today = db.query(SendLog).filter(
        SendLog.sent_at >= datetime.utcnow().date(),
        SendLog.account_id.in_([a.id for a in accounts]) if accounts else False
    ).count()

    # Для админа показываем демо-статистику
    if current_user.is_admin:
        import random
        seed = datetime.utcnow().day
        random.seed(seed)

        demo_stats = {
            "total_accounts": max(total_accounts, 47),
            "active_accounts": max(active_accounts, 43),
            "total_campaigns": max(total_campaigns, 28),
            "messages_sent_today": max(messages_sent_today, 12847 + random.randint(100, 500))
        }
    else:
        demo_stats = {
            "total_accounts": total_accounts,
            "active_accounts": active_accounts,
            "total_campaigns": total_campaigns,
            "messages_sent_today": messages_sent_today
        }

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "accounts": accounts,
        "campaigns": campaigns,
        "current_user": current_user,
        "stats": demo_stats
    })

@app.get("/accounts", response_class=HTMLResponse)
async def accounts_page(request: Request, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    """Страница управления аккаунтами"""
    # Фильтруем аккаунты по пользователю (админ видит все)
    if current_user.is_admin:
        accounts = db.query(Account).all()
    else:
        accounts = db.query(Account).filter(Account.user_id == current_user.id).all()

    return templates.TemplateResponse("accounts.html", {
        "request": request,
        "accounts": accounts,
        "current_user": current_user
    })

@app.post("/accounts/add")
async def add_account(
    phone: str = Form(...),
    use_auto_proxy: bool = Form(False),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Добавление нового аккаунта"""
    try:
        proxy = None
        if use_auto_proxy:
            proxy = proxy_manager.get_proxy_for_phone(phone)
            if not proxy:
                return JSONResponse({"status": "error", "message": "Нет доступных прокси. Загрузите список прокси."})

        result = await telegram_manager.add_account(phone, proxy, current_user.id)
        return JSONResponse(result)
    except Exception as e:
        print(f"Error in add_account: {str(e)}")
        return JSONResponse({"status": "error", "message": f"Ошибка при добавлении аккаунта: {str(e)}"})

@app.post("/accounts/verify_code")
async def verify_code(
    phone: str = Form(...),
    code: str = Form(...),
    phone_code_hash: str = Form(...),
    session_name: str = Form(...),
    proxy: Optional[str] = Form(None),
    current_user: User = Depends(get_current_user)
):
    """Подтверждение кода"""
    try:
        # Очищаем код от лишних символов
        clean_code = ''.join(filter(str.isdigit, code.strip()))

        # Валидация входных данных
        if not clean_code:
            return JSONResponse({"status": "error", "message": "Код не может быть пустым"})

        if len(clean_code) != 5:
            return JSONResponse({"status": "error", "message": f"Код должен содержать ровно 5 цифр, получено: {len(clean_code)}"})

        print(f"Проверяем код: '{clean_code}' для номера {phone}")

        result = await telegram_manager.verify_code(phone, clean_code, phone_code_hash, session_name, proxy, current_user.id)

        # Проверяем, что result не None
        if result is None:
            result = {"status": "error", "message": "Внутренняя ошибка сервера"}

        print(f"Результат проверки кода: {result}")
        return JSONResponse(result)

    except Exception as e:
        error_msg = str(e)
        print(f"Веб-ошибка при верификации: {error_msg}")

        # Логируем ошибку
        with open("unknown_errors.txt", "a", encoding="utf-8") as f:
            f.write(f"Web verify code error: {error_msg}\n")
            f.write(f"Phone: {phone}\n")
            f.write(f"Code: {code}\n")
            f.write(f"Clean code: {clean_code if 'clean_code' in locals() else 'N/A'}\n")
            f.write(f"Exception type: {type(e).__name__}\n")
            f.write("---\n")

        return JSONResponse({"status": "error", "message": f"Ошибка сервера: {error_msg}"})

@app.post("/accounts/verify_password")
async def verify_password(
    phone: str = Form(...),
    password: str = Form(...),
    session_name: str = Form(...),
    proxy: str = Form(default=""),
    current_user: User = Depends(get_current_user)
):
    """Подтверждение пароля 2FA"""
    result = await telegram_manager.verify_password(phone, password, session_name, proxy, current_user.id)
    return JSONResponse(result)

@app.post("/accounts/add_tdata")
async def add_account_from_tdata(
    tdata_files: List[UploadFile] = File(...),
    use_auto_proxy: bool = Form(False),
    current_user: User = Depends(get_current_user)
):
    """Добавление аккаунта из TDATA файлов"""
    import tempfile
    import shutil
    import traceback

    tdata_temp_dir = None

    try:
        print(f"🔄 Начинаем импорт TDATA для пользователя {current_user.username}")

        if not tdata_files or len(tdata_files) == 0:
            print("❌ Файлы TDATA не загружены")
            return JSONResponse({
                "status": "error",
                "message": "Выберите файлы TDATA для импорта"
            })

        # Создаем временную папку для TDATA
        tdata_temp_dir = tempfile.mkdtemp(prefix="tdata_import_")
        print(f"📁 Создана временная папка: {tdata_temp_dir}")

        # Валидация и сохранение файлов
        saved_files = []
        required_files = []

        for file in tdata_files:
            if not file.filename:
                continue

            try:
                file_path = os.path.join(tdata_temp_dir, file.filename)
                content = await file.read()

                if len(content) == 0:
                    print(f"⚠️ Файл {file.filename} пустой, пропускаем")
                    continue

                # Проверяем размер файла (ограничение 100MB на файл)
                if len(content) > 100 * 1024 * 1024:
                    print(f"⚠️ Файл {file.filename} слишком большой ({len(content)} байт)")
                    continue

                with open(file_path, "wb") as buffer:
                    buffer.write(content)

                saved_files.append(file.filename)

                # Отмечаем важные файлы
                if file.filename.startswith("key_data") or file.filename.startswith("map") or file.filename == "settings0":
                    required_files.append(file.filename)

                print(f"✅ Сохранен файл: {file.filename} ({len(content)} байт)")

            except Exception as file_error:
                print(f"❌ Ошибка сохранения файла {file.filename}: {str(file_error)}")
                continue

        if not saved_files:
            return JSONResponse({
                "status": "error",
                "message": "Не удалось сохранить файлы. Проверьте формат загружаемых файлов"
            })

        # Проверяем наличие ключевых файлов
        has_key_data = any(f.startswith("key_data") for f in saved_files)
        if not has_key_data:
            return JSONResponse({
                "status": "error",
                "message": "В загруженных файлах не найден key_data. Убедитесь что загружаете правильные файлы из папки tdata"
            })

        print(f"📁 Сохранено файлов: {len(saved_files)}, ключевых: {len(required_files)}")

        # Получаем прокси если нужно
        proxy = None
        if use_auto_proxy:
            try:
                proxy = proxy_manager.get_proxy_for_phone("tdata_import")
                if proxy:
                    print(f"🔗 Используем прокси: {proxy}")
                else:
                    print("⚠️ Прокси не назначен, продолжаем без прокси")
            except Exception as proxy_error:
                print(f"❌ Ошибка получения прокси: {str(proxy_error)}")

        # Импортируем аккаунт
        print("🔄 Начинаем импорт аккаунта...")
        result = await telegram_manager.add_account_from_tdata(
            tdata_temp_dir,
            proxy,
            current_user.id
        )

        print(f"✅ Результат импорта: {result}")

        # Валидация результата
        if not isinstance(result, dict):
            result = {"status": "error", "message": "Внутренняя ошибка сервера"}

        if 'status' not in result:
            result['status'] = 'error'

        if result.get('status') == 'error' and 'message' not in result:
            result['message'] = 'Неизвестная ошибка импорта'

        # Добавляем дополнительную информацию для успешного импорта
        if result.get('status') == 'success':
            result['files_processed'] = len(saved_files)
            result['message'] = f"Аккаунт успешно импортирован. Обработано файлов: {len(saved_files)}"

        return JSONResponse(result)

    except Exception as e:
        error_msg = str(e)
        error_trace = traceback.format_exc()

        print(f"❌ Критическая ошибка обработки TDATA: {error_msg}")
        print(f"🔍 Стек ошибки: {error_trace}")

        # Логируем в файл для отладки
        try:
            with open("tdata_import_errors.log", "a", encoding="utf-8") as log_file:
                log_file.write(f"\n=== TDATA Import Error {datetime.utcnow()} ===\n")
                log_file.write(f"User: {current_user.username if current_user else 'Unknown'}\n")
                log_file.write(f"Error: {error_msg}\n")
                log_file.write(f"Traceback: {error_trace}\n")
                log_file.write("=" * 50 + "\n")
        except:
            pass

        return JSONResponse({
            "status": "error",
            "message": f"Критическая ошибка импорта: {error_msg}"
        })

    finally:
        # Очистка временной папки
        if tdata_temp_dir and os.path.exists(tdata_temp_dir):
            try:
                shutil.rmtree(tdata_temp_dir)
                print(f"🧹 Временная папка очищена: {tdata_temp_dir}")
            except Exception as cleanup_error:
                print(f"⚠️ Ошибка очистки временной папки: {str(cleanup_error)}")


@app.post("/accounts/{account_id}/toggle")
async def toggle_account(account_id: int, db: Session = Depends(get_db)):
    """Включение/отключение аккаунта"""
    account = db.query(Account).filter(Account.id == account_id).first()
    if account:
        account.is_active = not account.is_active
        db.commit()
        return JSONResponse({"status": "success"})
    return JSONResponse({"status": "error", "message": "Аккаунт не найден"})

@app.delete("/accounts/{account_id}")
async def delete_account(account_id: int, db: Session = Depends(get_db)):
    """Удаление аккаунта"""
    account = db.query(Account).filter(Account.id == account_id).first()
    if account:
        db.delete(account)
        db.commit()
        return JSONResponse({"status": "success"})
    return JSONResponse({"status": "error", "message": "Аккаунт не найден"})

@app.get("/campaigns", response_class=HTMLResponse)
async def campaigns_page(request: Request, db: Session = Depends(get_db)):
    """Страница кампаний"""
    campaigns = db.query(Campaign).order_by(Campaign.created_at.desc()).all()
    return templates.TemplateResponse("campaigns.html", {
        "request": request,
        "campaigns": campaigns
    })

@app.get("/campaigns/new", response_class=HTMLResponse)
async def new_campaign_page(request: Request):
    """Страница создания новой кампании"""
    return templates.TemplateResponse("campaign_form.html", {
        "request": request,
        "campaign": None
    })

@app.post("/campaigns")
@app.post("/campaigns/new")
async def create_campaign(
    name: str = Form(...),
    channel_message: str = Form(""),
    group_message: str = Form(""),
    private_message: str = Form(""),
    channels_list: str = Form(""),
    groups_list: str = Form(""),
    private_list: str = Form(""),
    delay_seconds: int = Form(3),
    attachment: Optional[UploadFile] = File(None),
    db: Session = Depends(get_db)
):
    """Создание новой кампании"""

    attachment_path = None
    if attachment and attachment.filename:
        file_path = os.path.join(UPLOADS_DIR, attachment.filename)
        with open(file_path, "wb") as f:
            content = await attachment.read()
            f.write(content)
        attachment_path = file_path

    campaign = Campaign(
        name=name,
        channel_message=channel_message,
        group_message=group_message,
        private_message=private_message,
        channels_list=channels_list,
        groups_list=groups_list,
        private_list=private_list,
        delay_seconds=delay_seconds,
        attachment_path=attachment_path
    )

    db.add(campaign)
    db.commit()

    return RedirectResponse(url="/campaigns", status_code=303)

@app.post("/campaigns/{campaign_id}/start")
async def start_campaign(campaign_id: int):
    """Запуск кампании"""
    result = await message_sender.start_campaign(campaign_id)
    return JSONResponse(result)

@app.post("/campaigns/{campaign_id}/stop")
async def stop_campaign(campaign_id: int):
    """Остановка кампании"""
    result = await message_sender.stop_campaign(campaign_id)
    return JSONResponse(result)

@app.post("/accounts/{account_id}/delete_telegram")
async def delete_telegram_account(
    account_id: int,
    reason: str = Form("Больше не нужен"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Удаление аккаунта из Telegram"""
    try:
        # Проверяем права доступа
        if not current_user.is_admin:
            account = db.query(Account).filter(
                Account.id == account_id,
                Account.user_id == current_user.id
            ).first()
            if not account:
                return JSONResponse({"status": "error", "message": "Аккаунт не найден или нет прав доступа"})

        result = await telegram_manager.delete_telegram_account(account_id, reason)
        return JSONResponse(result)
    except Exception as e:
        return JSONResponse({"status": "error", "message": f"Ошибка: {str(e)}"})

@app.post("/campaigns/{campaign_id}/auto_delete_accounts")
async def auto_delete_campaign_accounts(
    campaign_id: int,
    delay_seconds: int = Form(5),
    current_user: User = Depends(get_current_user)
):
    """Автоматическое удаление всех аккаунтов после кампании"""
    try:
        result = await telegram_manager.auto_delete_after_campaign(campaign_id, delay_seconds)
        return JSONResponse(result)
    except Exception as e:
        return JSONResponse({"status": "error", "message": f"Ошибка: {str(e)}"})

@app.get("/logs")
async def logs_page(request: Request, db: Session = Depends(get_db)):
    """Страница логов"""
    logs = db.query(SendLog).order_by(SendLog.sent_at.desc()).limit(100).all()
    return templates.TemplateResponse("logs.html", {
        "request": request,
        "logs": logs
    })

@app.get("/settings")
async def settings_page(request: Request):
    """Страница настроек антиспам-системы"""
    return templates.TemplateResponse("settings.html", {"request": request})

@app.get("/contacts-campaign")
async def contacts_campaign_page(request: Request):
    """Страница рассылки по контактам"""
    return templates.TemplateResponse("contacts_campaign.html", {"request": request})

# API endpoints

@app.get("/proxies", response_class=HTMLResponse)
async def proxies_page(request: Request):
    """Страница управления прокси"""
    return templates.TemplateResponse("proxies.html", {
        "request": request,
        "proxies_count": getattr(proxy_manager, 'get_available_proxies_count', lambda: 0)(),
        "used_count": getattr(proxy_manager, 'get_used_proxies_count', lambda: 0)(),
        "proxies": getattr(proxy_manager, 'get_all_proxies', lambda: [])()
    })

@app.post("/proxies/upload")
async def upload_proxies(proxies_text: str = Form(...)):
    """Загрузка списка прокси"""
    try:
        proxy_manager.save_proxies(proxies_text)
        return JSONResponse({
            "status": "success",
            "message": f"Загружено {getattr(proxy_manager, 'get_available_proxies_count', lambda: 0)()} прокси"
        })
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)})

@app.post("/api/proxy/delete/{proxy_id}")
async def delete_proxy(proxy_id: int):
    """Удаление прокси"""
    success = proxy_manager.remove_proxy(proxy_id)
    return {"success": success}

@app.get("/api/settings")
async def get_settings():
    """Получение всех настроек"""
    return {"success": True, "settings": settings_manager.get_settings_dict()}

@app.post("/api/settings")
async def save_all_settings(request: Request):
    """Сохранение всех настроек"""
    try:
        data = await request.json()
        success = settings_manager.update_all_settings(data)
        return {"success": success, "message": "Настройки сохранены" if success else "Ошибка сохранения"}
    except Exception as e:
        return {"success": False, "message": str(e)}

@app.post("/api/settings/{section}")
async def save_settings_section(section: str, request: Request):
    """Сохранение конкретной секции настроек"""
    try:
        data = await request.json()
        success = settings_manager.update_section(section, data)
        return {"success": success, "message": f"Настройки {section} сохранены" if success else "Ошибка сохранения"}
    except Exception as e:
        return {"success": False, "message": str(e)}

@app.post("/api/settings/reset")
async def reset_settings():
    """Сброс настроек к умолчаниям"""
    try:
        success = settings_manager.reset_to_defaults()
        return {"success": success, "message": "Настройки сброшены" if success else "Ошибка сброса"}
    except Exception as e:
        return {"success": False, "message": str(e)}

@app.delete("/accounts/{account_id}")
async def delete_account(account_id: int, db: Session = Depends(get_db)):
    """Удаление аккаунта"""
    account = db.query(Account).filter(Account.id == account_id).first()
    if account:
        # Освобождаем прокси для этого номера
        proxy_manager.clear_proxy_for_phone(account.phone)
        db.delete(account)
        db.commit()
        return JSONResponse({"status": "success"})
    return JSONResponse({"status": "error", "message": "Аккаунт не найден"})

@app.get("/api/accounts/{account_id}")
async def get_account_api(account_id: int, db: Session = Depends(get_db)):
    """API для получения данных конкретного аккаунта"""
    account = db.query(Account).filter(Account.id == account_id).first()
    if not account:
        return JSONResponse({"error": "Аккаунт не найден"}, status_code=404)

    return JSONResponse({
        "id": account.id,
        "name": account.name,
        "phone": account.phone,
        "first_name": account.first_name,
        "last_name": account.last_name,
        "bio": account.bio,
        "gender": account.gender,
        "is_active": account.is_active,
        "status": account.status
    })

@app.get("/api/accounts")
async def get_accounts_api(db: Session = Depends(get_db)):
    """API для получения списка аккаунтов"""
    accounts = db.query(Account).all()
    accounts_data = []
    for account in accounts:
        accounts_data.append({
            "id": account.id,
            "name": account.name,
            "phone": account.phone,
            "is_active": account.is_active,
            "status": account.status
        })
    return JSONResponse(accounts_data)

@app.get("/api/contacts/{account_id}")
async def get_contacts(account_id: int, db: Session = Depends(get_db)):
    """API для получения контактов аккаунта"""
    try:
        print(f"API запрос контактов для аккаунта {account_id}")
        result = await telegram_manager.get_user_contacts(account_id)
        print(f"Результат получения контактов: {result}")
        return JSONResponse(result)
    except Exception as e:
        print(f"Error in get_contacts API: {str(e)}")
        return JSONResponse(
            {"status": "error", "message": f"Ошибка получения контактов: {str(e)}"},
            status_code=500
        )

@app.get("/api/chats/{account_id}")
async def get_chats(account_id: int, db: Session = Depends(get_db)):
    """API для получения чатов аккаунта"""
    try:
        print(f"API запрос чатов для аккаунта {account_id}")
        result = await telegram_manager.get_user_chats(account_id)
        print(f"Результат получения чатов: {result}")
        return JSONResponse(result)
    except Exception as e:
        print(f"Error in get_chats API: {str(e)}")
        return JSONResponse(
            {"status": "error", "message": f"Ошибка получения чатов: {str(e)}"},
            status_code=500
        )

@app.get("/campaign-stats", response_class=HTMLResponse)
async def campaign_stats_page(request: Request, current_user: User = Depends(get_current_user)):
    """Страница статистики кампаний"""
    return templates.TemplateResponse("campaign_stats.html", {
        "request": request,
        "current_user": current_user
    })

@app.get("/profile_manager", response_class=HTMLResponse)
async def profile_manager_page(request: Request, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    """Страница управления профилями"""
    # Фильтруем аккаунты по пользователю (админ видит все)
    if current_user.is_admin:
        accounts = db.query(Account).all()
    else:
        accounts = db.query(Account).filter(Account.user_id == current_user.id).all()

    return templates.TemplateResponse("profile_manager.html", {
        "request": request,
        "accounts": accounts,
        "current_user": current_user
    })

@app.get("/api/campaign-stats")
async def get_campaign_stats(db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    """API для получения статистики кампаний"""
    try:
        # Фильтруем кампании по пользователю
        if current_user.is_admin:
            campaigns = db.query(Campaign).all()
        else:
            user_accounts = db.query(Account).filter(Account.user_id == current_user.id).all()
            account_ids = [a.id for a in user_accounts]
            campaigns = db.query(Campaign).filter(Campaign.account_id.in_(account_ids)).all() if account_ids else []

        campaign_stats = []
        total_sent = 0
        total_failed = 0

        for campaign in campaigns:
            # Получаем статистику отправки для каждой кампании
            sent_logs = db.query(SendLog).filter(
                SendLog.campaign_id == campaign.id,
                SendLog.status == "sent"
            ).count()

            failed_logs = db.query(SendLog).filter(
                SendLog.campaign_id == campaign.id,
                SendLog.status == "failed"
            ).count()

            # Подсчитываем общее количество целей
            total_targets = 0
            if campaign.private_list:
                try:
                    import json
                    targets = json.loads(campaign.private_list)
                    total_targets += len(targets)
                except:
                    targets = campaign.private_list.split('\n')
                    total_targets += len([t for t in targets if t.strip()])

            if campaign.groups_list:
                try:
                    import json
                    targets = json.loads(campaign.groups_list)
                    total_targets += len(targets)
                except:
                    targets = campaign.groups_list.split('\n')
                    total_targets += len([t for t in targets if t.strip()])

            if campaign.channels_list:
                try:
                    import json
                    targets = json.loads(campaign.channels_list)
                    total_targets += len(targets)
                except:
                    targets = campaign.channels_list.split('\n')
                    total_targets += len([t for t in targets if t.strip()])

            # Подсчитываем количество использованных аккаунтов
            accounts_used = db.query(SendLog.account_id).filter(
                SendLog.campaign_id == campaign.id
            ).distinct().count()

            campaign_stat = {
                "id": campaign.id,
                "name": campaign.name,
                "status": campaign.status,
                "created_at": campaign.created_at.isoformat(),
                "sent_count": sent_logs,
                "failed_count": failed_logs,
                "total_targets": total_targets,
                "accounts_used": accounts_used,
                "delay_seconds": campaign.delay_seconds
            }

            campaign_stats.append(campaign_stat)
            total_sent += sent_logs
            total_failed += failed_logs

        # Общая статистика
        overall_stats = {
            "total_campaigns": len(campaigns),
            "total_sent": total_sent,
            "total_failed": total_failed,
            "success_rate": round((total_sent / (total_sent + total_failed)) * 100, 1) if (total_sent + total_failed) > 0 else 0
        }

        return JSONResponse({
            "status": "success",
            "overall": overall_stats,
            "campaigns": campaign_stats
        })

    except Exception as e:
        return JSONResponse({
            "status": "error",
            "message": f"Ошибка получения статистики: {str(e)}"
        })

@app.get("/api/campaign-details/{campaign_id}")
async def get_campaign_details(campaign_id: int, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    """API для получения детальной информации о кампании"""
    try:
        # Проверяем права доступа
        campaign = db.query(Campaign).filter(Campaign.id == campaign_id).first()
        if not campaign:
            return JSONResponse({"status": "error", "message": "Кампания не найдена"})

        if not current_user.is_admin and campaign.account_id:
            account = db.query(Account).filter(Account.id == campaign.account_id).first()
            if not account or account.user_id != current_user.id:
                return JSONResponse({"status": "error", "message": "Нет доступа к этой кампании"})

        # Получаем детальную статистику
        sent_count = db.query(SendLog).filter(
            SendLog.campaign_id == campaign_id,
            SendLog.status == "sent"
        ).count()

        failed_count = db.query(SendLog).filter(
            SendLog.campaign_id == campaign_id,
            SendLog.status == "failed"
        ).count()

        # Подсчитываем общее количество целей
        total_targets = 0
        if campaign.private_list:
            try:
                import json
                targets = json.loads(campaign.private_list)
                total_targets += len(targets)
            except:
                targets = campaign.private_list.split('\n')
                total_targets += len([t for t in targets if t.strip()])

        if campaign.groups_list:
            try:
                import json
                targets = json.loads(campaign.groups_list)
                total_targets += len(targets)
            except:
                targets = campaign.groups_list.split('\n')
                total_targets += len([t for t in targets if t.strip()])

        if campaign.channels_list:
            try:
                import json
                targets = json.loads(campaign.channels_list)
                total_targets += len(targets)
            except:
                targets = campaign.channels_list.split('\n')
                total_targets += len([t for t in targets if t.strip()])

        # Получаем последние логи отправки
        logs = db.query(SendLog).filter(
            SendLog.campaign_id == campaign_id
        ).order_by(SendLog.sent_at.desc()).limit(50).all()

        logs_data = []
        for log in logs:
            logs_data.append({
                "recipient": log.recipient,
                "status": log.status,
                "sent_at": log.sent_at.isoformat(),
                "error_message": log.error_message
            })

        campaign_data = {
            "id": campaign.id,
            "name": campaign.name,
            "status": campaign.status,
            "created_at": campaign.created_at.isoformat(),
            "delay_seconds": campaign.delay_seconds,
            "private_message": campaign.private_message,
            "group_message": campaign.group_message,
            "channel_message": campaign.channel_message,
            "sent_count": sent_count,
            "failed_count": failed_count,
            "total_targets": total_targets
        }

        return JSONResponse({
            "status": "success",
            "campaign": campaign_data,
            "logs": logs_data
        })

    except Exception as e:
        return JSONResponse({
            "status": "error",
            "message": f"Ошибка получения деталей кампании: {str(e)}"
        })

@app.get("/api/stats")
async def get_stats(request: Request, db: Session = Depends(get_db)):
    """API для получения статистики"""
    # Проверяем, авторизован ли пользователь как админ
    is_admin = False
    try:
        current_user = get_current_user(request, None, db)
        is_admin = current_user.is_admin if current_user else False
    except:
        pass

    accounts = db.query(Account).all()
    campaigns = db.query(Campaign).all()

    # Базовая статистика
    base_stats = {
        "accounts": {
            "total": len(accounts),
            "active": len([a for a in accounts if a.is_active]),
            "online": len([a for a in accounts if a.status == "online"])
        },
        "campaigns": {
            "total": len(campaigns),
            "running": len([c for c in campaigns if c.status == "running"])
        },
        "messages_today": db.query(SendLog).filter(
            SendLog.sent_at >= datetime.utcnow().date()
        ).count(),
        "proxies": {
            "total": getattr(proxy_manager, 'get_available_proxies_count', lambda: 0)(),
            "used": getattr(proxy_manager, 'get_used_proxies_count', lambda: 0)()
        }
    }

    # Для админа показываем демо-статистику
    if is_admin:
        import random
        # Генерируем стабильную "случайную" статистику на основе текущего дня
        seed = datetime.utcnow().day
        random.seed(seed)

        demo_stats = {
            "accounts": {
                "total": max(base_stats["accounts"]["total"], 47),
                "active": max(base_stats["accounts"]["active"], 43),
                "online": max(base_stats["accounts"]["online"], 39)
            },
            "campaigns": {
                "total": max(base_stats["campaigns"]["total"], 28),
                "running": max(base_stats["campaigns"]["running"], 3)
            },
            "messages_today": max(base_stats["messages_today"], 12847 + random.randint(100, 500)),
            "proxies": {
                "total": max(base_stats["proxies"]["total"], 156),
                "used": max(base_stats["proxies"]["used"], 89)
            },
            "performance": {
                "success_rate": 98.7,
                "avg_speed": "847 сообщений/час",
                "uptime": "99.2%"
            }
        }
        return JSONResponse(demo_stats)

    return JSONResponse(base_stats)

@app.post("/api/contacts-campaign")
async def create_contacts_campaign(request: Request, db: Session = Depends(get_db)):
    """Создание кампании рассылки по контактам"""
    try:
        data = await request.json()

        account_id = data.get('account_id')
        message = data.get('message')
        delay_seconds = data.get('delay_seconds', 5)
        start_in_minutes = data.get('start_in_minutes')

        if not account_id or not message:
            return JSONResponse({"status": "error", "message": "Не указан аккаунт или сообщение"})

        # Проверяем активность аккаунта
        account = db.query(Account).filter(Account.id == account_id).first()
        if not account or not account.is_active:
            return JSONResponse({"status": "error", "message": "Аккаунт неактивен или не найден"})

        result = await message_sender.create_contacts_campaign(
            account_id, message, delay_seconds, start_in_minutes
        )

        return JSONResponse(result)

    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)})

@app.post("/api/contacts-campaign/start")
@app.post("/api/start-contacts-campaign")
async def start_contacts_campaign_api(
    request: Request,
    message: str = Form(...),
    delay_seconds: int = Form(0),
    start_in_minutes: Optional[int] = Form(None),
    attachment: Optional[UploadFile] = File(None),
    auto_delete_account: bool = Form(False),
    delete_delay_minutes: int = Form(5),
    selected_accounts: str = Form(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """API для запуска кампании рассылки по контактам с упрощенной обработкой аккаунтов"""
    try:
        print(f"🚀 Получен запрос на запуск кампании от пользователя {current_user.username}")
        print(f"📋 Полученные аккаунты: '{selected_accounts}'")
        print(f"📝 Сообщение: '{message[:50]}{'...' if len(message) > 50 else ''}'")

        # Простая и надежная обработка выбранных аккаунтов
        account_ids = []
        
        if selected_accounts and selected_accounts.strip():
            try:
                # Разбиваем строку по запятым и преобразуем в числа
                raw_ids = selected_accounts.strip().split(',')
                for raw_id in raw_ids:
                    clean_id = raw_id.strip()
                    if clean_id and clean_id.isdigit():
                        account_ids.append(int(clean_id))
                
                # Удаляем дубликаты
                account_ids = list(set(account_ids))
                print(f"✅ Обработанные ID аккаунтов: {account_ids}")
                
            except Exception as parse_error:
                print(f"❌ Ошибка парсинга аккаунтов: {parse_error}")
                return JSONResponse({
                    "status": "error",
                    "message": f"Ошибка обработки списка аккаунтов: {str(parse_error)}"
                })
        
        # Проверяем что аккаунты выбраны
        if not account_ids:
            print("❌ Не выбраны аккаунты")
            return JSONResponse({
                "status": "error",
                "message": "Не выбраны аккаунты для рассылки. Выберите хотя бы один активный аккаунт."
            })

        # Проверяем сообщение
        if not message or not message.strip():
            print("❌ Пустое сообщение")
            return JSONResponse({
                "status": "error",
                "message": "Введите текст сообщения для рассылки"
            })

        # Проверяем что аккаунты существуют и активны
        active_accounts = db.query(Account).filter(
            Account.id.in_(account_ids),
            Account.is_active == True,
            Account.status == 'online'
        ).all()

        if not active_accounts:
            print(f"❌ Активные аккаунты не найдены среди {account_ids}")
            return JSONResponse({
                "status": "error",
                "message": "Среди выбранных аккаунтов нет активных онлайн аккаунтов"
            })

        active_account_ids = [acc.id for acc in active_accounts]
        print(f"✅ Найдено {len(active_accounts)} активных аккаунтов: {active_account_ids}")

        # Обработка файла вложения
        attachment_path = None
        if attachment and attachment.filename:
            try:
                print(f"📎 Обрабатываем файл: {attachment.filename}")
                file_content = await attachment.read()
                filename = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{attachment.filename}"
                attachment_path = os.path.join(UPLOADS_DIR, filename)

                with open(attachment_path, "wb") as f:
                    f.write(file_content)

                print(f"✅ Файл сохранен: {attachment_path}")
            except Exception as file_error:
                print(f"❌ Ошибка сохранения файла: {file_error}")
                return JSONResponse({
                    "status": "error",
                    "message": f"Ошибка сохранения файла: {str(file_error)}"
                })

        # Запускаем кампанию
        print(f"🚀 Запускаем кампанию с {len(active_account_ids)} аккаунтами")
        print(f"⚙️ Параметры: delay={delay_seconds}, auto_delete={auto_delete_account}")

        result = await message_sender.start_contacts_campaign(
            account_ids=active_account_ids,
            message=message,
            delay_seconds=delay_seconds,
            start_in_minutes=start_in_minutes,
            attachment_path=attachment_path,
            auto_delete_account=auto_delete_account,
            delete_delay_minutes=delete_delay_minutes
        )

        print(f"📊 Результат кампании: {result}")

        # Добавляем дополнительную информацию в ответ
        if result.get("status") == "success":
            result["accounts_used"] = len(active_account_ids)
            if "message" not in result:
                result["message"] = f"Рассылка запущена с {len(active_account_ids)} аккаунтами"

        return JSONResponse(result)

    except Exception as e:
        import traceback
        error_msg = str(e)
        error_trace = traceback.format_exc()
        print(f"❌ Ошибка API кампании по контактам: {error_msg}")
        print(f"🔍 Трассировка: {error_trace}")

        return JSONResponse({
            "status": "error",
            "message": f"Ошибка запуска кампании: {error_msg}"
        })

@app.post("/api/campaigns/{campaign_id}/cancel")
async def cancel_scheduled_campaign(campaign_id: int):
    """Отмена запланированной кампании"""
    result = await message_sender.cancel_scheduled_campaign(campaign_id)
    return JSONResponse(result)

@app.get("/api/scheduled-campaigns")
async def get_scheduled_campaigns():
    """Получение списка запланированных кампаний"""
    scheduled = message_sender.get_scheduled_campaigns()
    return JSONResponse({"scheduled_campaigns": scheduled})

@app.get("/api/dialogs/{account_id}")
async def get_dialogs(account_id: int, db: Session = Depends(get_db)):
    """API для получения диалогов аккаунта (старый метод)"""
    try:
        print(f"API запрос диалогов для аккаунта {account_id}")
        result = await telegram_manager.get_user_dialogs(account_id)
        print(f"Результат получения диалогов: {result}")
        return JSONResponse(result)
    except Exception as e:
        print(f"Error in get_dialogs API: {str(e)}")
        return JSONResponse(
            {"status": "error", "message": f"Ошибка получения диалогов: {str(e)}"},
            status_code=500
        )

@app.post("/api/upload-file")
async def upload_file(file: UploadFile = File(...)):
    """Загрузка файла для рассылки"""
    try:
        # Проверяем, что файл был загружен
        if not file.filename:
            return JSONResponse(
                status_code=400,
                content={"status": "error", "message": "Файл не выбран"}
            )

        # Читаем содержимое файла
        content = await file.read()

        # Проверяем размер файла
        if len(content) == 0:
            return JSONResponse(
                status_code=400,
                content={"status": "error", "message": "Файл пустой"}
            )

        if len(content) > 50 * 1024 * 1024:  # 50MB
            return JSONResponse(
                status_code=400,
                content={"status": "error", "message": "Файл слишком большой. Максимальный размер: 50 МБ"}
            )

        # Создаем папку uploads если её нет
        os.makedirs(UPLOADS_DIR, exist_ok=True)

        # Генерируем уникальное имя файла
        import uuid
        file_extension = os.path.splitext(file.filename)[1]
        unique_filename = f"{uuid.uuid4()}{file_extension}"
        file_path = os.path.join(UPLOADS_DIR, unique_filename)

        # Сохраняем файл
        with open(file_path, "wb") as f:
            f.write(content)

        # Проверяем, что файл действительно сохранился с правильным размером
        if os.path.exists(file_path):
            saved_size = os.path.getsize(file_path)
            if saved_size != len(content):
                print(f"⚠️ Размер сохранённого файла ({saved_size}) не совпадает с исходным ({len(content)})")
                os.remove(file_path)  # Удаляем повреждённый файл
                return JSONResponse(
                    status_code=500,
                    content={"status": "error", "message": "Ошибка сохранения файла"}
                )

            print(f"✓ Файл {unique_filename} успешно сохранён ({saved_size} байт)")

            return JSONResponse(content={
                "status": "success",
                "filename": unique_filename,
                "path": file_path,
                "size": saved_size,
                "original_name": file.filename
            })
        else:
            return JSONResponse(
                status_code=500,
                content={"status": "error", "message": "Файл не был сохранён"}
            )

    except Exception as e:
        print(f"Ошибка загрузки файла: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": f"Ошибка загрузки файла: {str(e)}"}
        )

# API для управления профилями
@app.post("/api/accounts/{account_id}/update_field")
async def update_account_field(account_id: int, request: Request, db: Session = Depends(get_db)):
    """Обновление поля аккаунта"""
    try:
        data = await request.json()
        field = data.get('field')
        value = data.get('value')

        account = db.query(Account).filter(Account.id == account_id).first()
        if not account:
            return {"success": False, "message": "Аккаунт не найден"}

        if hasattr(account, field):
            setattr(account, field, value)
            db.commit()
            return {"success": True}
        else:
            return {"success": False, "message": "Неизвестное поле"}
    except Exception as e:
        return {"success": False, "message": str(e)}

@app.post("/api/accounts/auto_fill_profiles")
async def auto_fill_profiles(request: Request, db: Session = Depends(get_db)):
    """Автозаполнение профилей по гендеру"""
    try:
        data = await request.json()
        gender = data.get('gender', 'male')

        # Читаем файлы с именами
        if gender == 'male':
            with open('firstnames_male.txt', 'r', encoding='utf-8') as f:
                first_names = [line.strip() for line in f if line.strip()]
            with open('lastnames_male.txt', 'r', encoding='utf-8') as f:
                last_names = [line.strip() for line in f if line.strip()]
        else:
            with open('firstnames_female.txt', 'r', encoding='utf-8') as f:
                first_names = [line.strip() for line in f if line.strip()]
            with open('lastnames_female.txt', 'r', encoding='utf-8') as f:
                last_names = [line.strip() for line in f if line.strip()]

        # Получаем аккаунты без гендера или с нужным гендером
        accounts = db.query(Account).filter(
            (Account.gender == None) | (Account.gender == gender),
            Account.is_active == True
        ).all()

        import random
        updated_count = 0

        for account in accounts:
            account.gender = gender
            account.first_name = random.choice(first_names)
            account.last_name = random.choice(last_names)

            # Генерируем простое био
            bios = [
                "Люблю жизнь и путешествия",
                "Работаю и учусь",
                "Интересуюсь спортом",
                "Фотограф-любитель",
                "Читаю книги",
                "Слушаю музыку",
                "Занимаюсь спортом"
            ]
            account.bio = random.choice(bios)
            updated_count += 1

        db.commit()
        return {"success": True, "message": f"Обновлено {updated_count} аккаунтов"}
    except Exception as e:
        return {"success": False, "message": str(e)}

@app.post("/api/accounts/{account_id}/update")
async def update_account_full(
    account_id: int,
    first_name: str = Form(...),
    last_name: str = Form(...),
    gender: str = Form(...),
    bio: str = Form(...),
    photo: Optional[UploadFile] = File(None),
    db: Session = Depends(get_db)
):
    """Полное обновление аккаунта"""
    try:
        account = db.query(Account).filter(Account.id == account_id).first()
        if not account:
            return {"success": False, "message": "Аккаунт не найден"}

        account.first_name = first_name
        account.last_name = last_name
        account.gender = gender
        account.bio = bio

        if photo and photo.filename:
            # Сохраняем фото
            import uuid
            file_extension = os.path.splitext(photo.filename)[1]
            unique_filename = f"profile_{account_id}_{uuid.uuid4().hex[:8]}{file_extension}"

            # Определяем папку по гендеру
            folder = f"profile_photos/{gender}" if gender in ['male', 'female'] else "profile_photos"
            os.makedirs(folder, exist_ok=True)

            photo_path = os.path.join(folder, unique_filename)

            with open(photo_path, "wb") as f:
                content = await photo.read()
                f.write(content)

            account.profile_photo_path = photo_path

        db.commit()
        return {"success": True}
    except Exception as e:
        return {"success": False, "message": str(e)}

@app.post("/api/sequential_comments")
async def sequential_comments(
    request: Request,
    current_user: User = Depends(get_current_user)
):
    """Последовательное комментирование с одного аккаунта в секцию 'Leave a comment'"""
    try:
        data = await request.json()
        account_id = data.get("account_id")
        targets = data.get("targets", [])  # [{"chat_id": "@channel", "message_id": 123, "comment": "text"}]
        delay_seconds = data.get("delay_seconds", 3)

        if not account_id:
            return {"success": False, "message": "Не указан аккаунт"}

        if not targets:
            return {"success": False, "message": "Не указаны цели для комментирования"}

        print(f"🚀 Запуск последовательного комментирования в 'Leave a comment': {len(targets)} комментариев")

        results = []

        for i, target in enumerate(targets, 1):
            chat_id = target.get("chat_id")
            message_id = target.get("message_id")
            comment = target.get("comment")

            if not all([chat_id, message_id, comment]):
                result = {"status": "error", "message": "Неполные данные цели"}
                results.append(result)
                continue

            print(f"💬 Комментарий {i}/{len(targets)} от аккаунта {account_id}")

            # Используем специальный метод для комментариев к постам
            result = await telegram_manager.send_post_comment(
                account_id=account_id,
                chat_id=chat_id,
                message_id=int(message_id),
                comment=comment
            )

            results.append(result)

            if result["status"] == "success":
                print(f"✅ Комментарий добавлен в 'Leave a comment': {comment[:50]}...")
            else:
                print(f"❌ Ошибка комментария: {result.get('message', 'неизвестная ошибка')}")

            # Задержка между комментариями (кроме последнего)
            if i < len(targets):
                await asyncio.sleep(delay_seconds)

        print(f"🎉 Последовательное комментирование завершено")

        success_count = len([r for r in results if r["status"] == "success"])
        return {
            "success": True,
            "message": f"Комментирование в 'Leave a comment' завершено: {success_count}/{len(targets)} успешно",
            "results": results
        }

    except Exception as e:
        print(f"❌ Ошибка последовательного комментирования: {e}")
        return {"success": False, "message": str(e)}

@app.post("/api/multiple_reactions")
async def start_multiple_reactions(request: Request, db: Session = Depends(get_db)):
    """Запуск множественных реакций"""
    try:
        data = await request.json()
        post_url = data.get('post_url')
        reactions = data.get('reactions', [])
        total_count = data.get('total_count', 9)
        selected_accounts = data.get('selected_accounts', [])
        delay_seconds = data.get('delay_seconds', 20)

        # Парсим URL поста
        import re
        url_match = re.search(r't\.me/([^/]+)/(\d+)', post_url)
        if not url_match:
            return {"success": False, "message": "Неверный формат URL"}

        chat_id = f"@{url_match.group(1)}"
        message_id = int(url_match.group(2))

        # Получаем аккаунты
        accounts = db.query(Account).filter(
            Account.id.in_(selected_accounts),
            Account.is_active == True
        ).all()

        if not accounts:
            return {"success": False, "message": "Нет активных аккаунтов"}

        # Запускаем задачу в фоне
        asyncio.create_task(run_multiple_reactions(
            chat_id, message_id, accounts, reactions, total_count, delay_seconds
        ))

        return {"success": True}
    except Exception as e:
        return {"success": False, "message": str(e)}

@app.post("/api/post_views")
async def start_post_views(request: Request, db: Session = Depends(get_db)):
    """Запуск просмотров постов"""
    try:
        data = await request.json()
        post_url = data.get('post_url')
        view_count = data.get('view_count', 10)
        selected_accounts = data.get('selected_accounts', [])
        delay_seconds = data.get('delay_seconds', 10)

        # Парсим URL поста
        import re
        url_match = re.search(r't\.me/([^/]+)/(\d+)', post_url)
        if not url_match:
            return {"success": False, "message": "Неверный формат URL"}

        chat_id = f"@{url_match.group(1)}"
        message_id = int(url_match.group(2))

        # Получаем аккаунты
        accounts = db.query(Account).filter(
            Account.id.in_(selected_accounts),
            Account.is_active == True
        ).limit(view_count).all()

        if not accounts:
            return {"success": False, "message": "Нет активных аккаунтов"}

        # Запускаем задачу в фоне
        asyncio.create_task(run_post_views(
            chat_id, message_id, accounts, delay_seconds
        ))

        return {"success": True}
    except Exception as e:
        return {"success": False, "message": str(e)}

@app.post("/api/accounts/update_all_telegram_profiles")
async def update_all_telegram_profiles(request: Request, db: Session = Depends(get_db)):
    """Массовое обновление профилей в Telegram"""
    try:
        data = await request.json()
        account_ids = data.get('account_ids', [])

        accounts = db.query(Account).filter(
            Account.id.in_([int(a_id) for a_id in account_ids]),  # Ensure IDs are integers
            Account.is_active == True
        ).all()

        updated_count = 0
        for account in accounts:
            try:
                result = await telegram_manager.update_profile(
                    account_id=account.id,
                    first_name=account.first_name or "",
                    last_name=account.last_name or "",
                    bio=account.bio or "",
                    profile_photo_path=account.profile_photo_path
                )
                if result["status"] == "success":
                    updated_count += 1
                    await asyncio.sleep(2)  # Защита от спама
            except Exception as e:
                print(f"Ошибка обновления аккаунта {account.id}: {e}")
                continue

        return {"success": True, "updated_count": updated_count}
    except Exception as e:
        return {"success": False, "message": str(e)}

@app.post("/api/accounts/auto_assign_genders")
async def auto_assign_genders(db: Session = Depends(get_db)):
    """Автоматическое назначение гендеров"""
    try:
        import random

        # Примерные списки имен (можно расширить)
        male_first_names = [
            "Александр", "Дмитрий", "Сергей", "Андрей", "Алексей", "Павел", "Николай", "Михаил",
            "Иван", "Владимир", "Константин", "Олег", "Роман", "Антон", "Денис", "Максим"
        ]
        female_first_names = [
            "Анна", "Елена", "Мария", "Наталья", "Ольга", "Екатерина", "Татьяна", "Ирина",
            "Юлия", "Светлана", "Людмила", "Галина", "Валентина", "Дарья", "Алёна", "Ксения"
        ]

        male_last_names = [
            "Иванов", "Петров", "Сидоров", "Козлов", "Новиков", "Морозов", "Петров", "Волков",
            "Соколов", "Зайцев", "Попов", "Васильев", "Кузнецов", "Смирнов", "Федоров", "Михайлов"
        ]
        female_last_names = [
            "Иванова", "Петрова", "Сидорова", "Козлова", "Новикова", "Морозова", "Петрова", "Волкова",
            "Соколова", "Зайцева", "Попова", "Васильева", "Кузнецова", "Смирнова", "Федорова", "Михайлова"
        ]

        accounts = db.query(Account).filter(Account.is_active == True).all()
        updated_count = 0

        for account in accounts:
            # Случайно назначаем гендер если не задан
            if not account.gender:
                account.gender = random.choice(['male', 'female'])

            # Назначаем имя и фамилию по гендеру
            if account.gender == 'male':
                account.first_name = random.choice(male_first_names)
                account.last_name = random.choice(male_last_names)
            elif account.gender == 'female':
                account.first_name = random.choice(female_first_names)
                account.last_name = random.choice(female_last_names)

            updated_count += 1

        db.commit()
        return {"success": True, "message": f"Обновлено {updated_count} аккаунтов"}
    except Exception as e:
        return {"success": False, "message": str(e)}

@app.post("/api/accounts/{account_id}/upload_photo")
async def upload_profile_photo(account_id: int, photo: UploadFile = File(...), db: Session = Depends(get_db)):
    """Загрузка фото профиля"""
    try:
        account = db.query(Account).filter(Account.id == account_id).first()
        if not account:
            return {"success": False, "message": "Аккаунт не найден"}

        # Сохраняем фото
        import uuid
        file_extension = os.path.splitext(photo.filename)[1]
        unique_filename = f"profile_{account_id}_{uuid.uuid4().hex[:8]}{file_extension}"
        photo_path = os.path.join(UPLOADS_DIR, unique_filename)

        with open(photo_path, "wb") as f:
            content = await photo.read()
            f.write(content)

        # Обновляем путь в БД
        account.profile_photo_path = photo_path
        db.commit()

        return {"success": True, "message": "Фото загружено успешно"}

    except Exception as e:
        return {"success": False, "message": str(e)}

@app.post("/api/accounts/{account_id}/update_telegram_profile")
async def update_telegram_profile(account_id: int, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    """Обновление профиля в Telegram"""
    try:
        # Проверяем права доступа
        if not current_user.is_admin:
            account = db.query(Account).filter(
                Account.id == account_id,
                Account.user_id == current_user.id
            ).first()
        else:
            account = db.query(Account).filter(Account.id == account_id).first()

        if not account:
            return {"success": False, "message": "Аккаунт не найден или нет прав доступа"}

        if not account.is_active:
            return {"success": False, "message": "Аккаунт неактивен"}

        # Проверяем что есть данные для обновления
        if not account.first_name and not account.last_name:
            return {"success": False, "message": "Укажите имя и/или фамилию перед обновлением"}

        print(f"🔄 API: Обновление профиля аккаунта {account_id}")
        print(f"📝 Данные из БД: {account.first_name}, {account.last_name}, {account.bio}")

        # Обновляем профиль в Telegram
        result = await telegram_manager.update_profile(
            account_id=account_id,
            first_name=account.first_name or "",
            last_name=account.last_name or "",
            bio=account.bio or "",
            profile_photo_path=account.profile_photo_path
        )

        print(f"📊 Результат обновления: {result}")

        if result["status"] == "success":
            # Обновляем статус аккаунта после успешного изменения
            account.last_activity = datetime.utcnow()
            db.commit()
            return {"success": True, "message": result["message"]}
        else:
            return {"success": False, "message": result["message"]}

    except Exception as e:
        print(f"❌ Ошибка API обновления профиля: {e}")
        return {"success": False, "message": f"Ошибка сервера: {str(e)}"}

@app.get("/api/comments/history/{account_id}")
async def get_comment_history(account_id: int, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    """Получение истории комментариев аккаунта"""
    try:
        # Проверяем права доступа
        if not current_user.is_admin:
            account = db.query(Account).filter(
                Account.id == account_id,
                Account.user_id == current_user.id
            ).first()
        else:
            account = db.query(Account).filter(Account.id == account_id).first()

        if not account:
            return {"success": False, "message": "Аккаунт не найден или нет прав доступа"}

        # Получаем комментарии из таблицы comment_logs
        try:
            from app.database import CommentLog
            comments = db.query(CommentLog).filter(
                CommentLog.account_id == account_id
            ).order_by(CommentLog.sent_at.desc()).limit(100).all()

            comments_data = []
            for comment in comments:
                comments_data.append({
                    "id": comment.id,
                    "chat_id": comment.chat_id,
                    "message_id": comment.message_id,
                    "comment": comment.comment,
                    "status": comment.status,
                    "error_message": comment.error_message,
                    "sent_at": comment.sent_at.isoformat() if comment.sent_at else None
                })

            return {
                "success": True,
                "comments": comments_data,
                "total": len(comments_data)
            }

        except Exception as query_error:
            print(f"❌ Ошибка запроса комментариев: {query_error}")
            return {
                "success": True,
                "comments": [],
                "total": 0,
                "message": "История комментариев пуста"
            }

    except Exception as e:
        print(f"❌ Ошибка получения истории комментариев: {e}")
        return {"success": False, "message": f"Ошибка сервера: {str(e)}"}

@app.delete("/api/comments/{comment_log_id}")
async def delete_comment_from_telegram(comment_log_id: int, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    """Удаление комментария из Telegram и базы данных"""
    try:
        # Получаем запись комментария
        try:
            from app.database import CommentLog
            comment_log = db.query(CommentLog).filter(CommentLog.id == comment_log_id).first()
        except:
            return {"success": False, "message": "Таблица комментариев не найдена"}

        if not comment_log:
            return {"success": False, "message": "Комментарий не найден"}

        # Проверяем права доступа
        if not current_user.is_admin:
            account = db.query(Account).filter(
                Account.id == comment_log.account_id,
                Account.user_id == current_user.id
            ).first()
            if not account:
                return {"success": False, "message": "Нет прав доступа к этому комментарию"}

        print(f"🗑️ Удаляем комментарий {comment_log_id} из чата {comment_log.chat_id}")

        # Пытаемся удалить комментарий из Telegram
        delete_result = await telegram_manager.delete_message(
            account_id=comment_log.account_id,
            chat_id=comment_log.chat_id,
            message_id=comment_log.message_id
        )

        # Удаляем запись из базы данных в любом случае
        db.delete(comment_log)
        db.commit()

        if delete_result.get("status") == "success":
            return {
                "success": True,
                "message": "Комментарий удален из Telegram и базы данных"
            }
        else:
            return {
                "success": True,
                "message": f"Комментарий удален из базы данных. Из Telegram: {delete_result.get('message', 'не удалось удалить')}"
            }

    except Exception as e:
        print(f"❌ Ошибка удаления комментария: {e}")
        return {"success": False, "message": f"Ошибка сервера: {str(e)}"}

@app.delete("/api/comments/clear/{account_id}")
async def clear_comment_history(account_id: int, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    """Очистка всей истории комментариев аккаунта"""
    try:
        # Проверяем права доступа
        if not current_user.is_admin:
            account = db.query(Account).filter(
                Account.id == account_id,
                Account.user_id == current_user.id
            ).first()
        else:
            account = db.query(Account).filter(Account.id == account_id).first()

        if not account:
            return {"success": False, "message": "Аккаунт не найден или нет прав доступа"}

        # Удаляем все комментарии аккаунта из базы данных
        try:
            from app.database import CommentLog
            deleted_count = db.query(CommentLog).filter(
                CommentLog.account_id == account_id
            ).delete()
            db.commit()

            return {
                "success": True,
                "message": f"Удалено {deleted_count} записей из истории комментариев"
            }

        except Exception as clear_error:
            print(f"❌ Ошибка очистки истории: {clear_error}")
            return {
                "success": True,
                "message": "История комментариев уже пуста"
            }

    except Exception as e:
        print(f"❌ Ошибка очистки истории комментариев: {e}")
        return {"success": False, "message": f"Ошибка сервера: {str(e)}"}

# API для кампаний комментирования
@app.post("/api/comment_campaigns")
async def create_comment_campaign(request: Request, db: Session = Depends(get_db)):
    """Создание кампании комментирования"""
    try:
        from app.database import CommentCampaign

        data = await request.json()

        campaign = CommentCampaign(
            name=data['name'],
            post_url=data['post_url'],
            comments_male=data['male_comments'],
            comments_female=data['female_comments'],
            delay_seconds=data['delay_seconds']
        )

        db.add(campaign)
        db.commit()

        return {"success": True, "campaign_id": campaign.id}
    except Exception as e:
        return {"success": False, "message": str(e)}

@app.get("/api/comment_campaigns")
async def get_comment_campaigns(db: Session = Depends(get_db)):
    """Получение списка кампаний комментирования"""
    try:
        from app.database import CommentCampaign

        campaigns = db.query(CommentCampaign).order_by(CommentCampaign.created_at.desc()).all()
        campaigns_data = []

        for campaign in campaigns:
            campaigns_data.append({
                "id": campaign.id,
                "name": campaign.name,
                "post_url": campaign.post_url,
                "status": campaign.status,
                "created_at": campaign.created_at.isoformat()
            })

        return {"campaigns": campaigns_data}
    except Exception as e:
        return {"success": False, "message": str(e)}

@app.post("/api/comment_campaigns/{campaign_id}/start")
async def start_comment_campaign(campaign_id: int, db: Session = Depends(get_db)):
    """Запуск кампании комментирования"""
    try:
        from app.database import CommentCampaign

        campaign = db.query(CommentCampaign).filter(CommentCampaign.id == campaign_id).first()
        if not campaign:
            return {"success": False, "message": "Кампания не найдена"}

        # Запускаем кампанию в фоне
        asyncio.create_task(run_comment_campaign(campaign_id))

        campaign.status = "running"
        campaign.started_at = datetime.utcnow()
        db.commit()

        return {"success": True}
    except Exception as e:
        return {"success": False, "message": str(e)}

async def run_comment_campaign(campaign_id: int):
    """Выполнение кампании комментирования"""
    from app.database import CommentCampaign, CommentLog, Account
    import re
    import random

    db = next(get_db())
    try:
        campaign = db.query(CommentCampaign).filter(CommentCampaign.id == campaign_id).first()
        if not campaign:
            print(f"❌ Кампания комментирования {campaign_id} не найдена")
            return

        print(f"🔄 Запуск кампании комментирования {campaign_id}: {campaign.name}")

        # Парсим URL поста
        url_match = re.search(r't\.me/([^/]+)/(\d+)', campaign.post_url)
        if not url_match:
            print(f"❌ Неверный формат URL: {campaign.post_url}")
            campaign.status = "failed"
            db.commit()
            return

        chat_id = f"@{url_match.group(1)}"
        message_id = int(url_match.group(2))

        print(f"📍 Цель: {chat_id}, сообщение: {message_id}")

        # Получаем активные аккаунты
        accounts = db.query(Account).filter(Account.is_active == True).all()
        if not accounts:
            print("❌ Нет активных аккаунтов для комментирования")
            campaign.status = "failed"
            db.commit()
            return

        print(f"👥 Найдено {len(accounts)} активных аккаунтов")

        # Парсим комментарии
        male_comments = [c.strip() for c in (campaign.comments_male or "").split('\n') if c.strip()]
        female_comments = [c.strip() for c in (campaign.comments_female or "").split('\n') if c.strip()]

        if not male_comments and not female_comments:
            print("❌ Нет комментариев для отправки")
            campaign.status = "failed"
            db.commit()
            return

        print(f"💬 Мужских комментариев: {len(male_comments)}, женских: {len(female_comments)}")

        success_count = 0
        for account in accounts:
            try:
                # Выбираем комментарий по гендеру аккаунта
                if account.gender == 'male' and male_comments:
                    comment = random.choice(male_comments)
                elif account.gender == 'female' and female_comments:
                    comment = random.choice(female_comments)
                elif male_comments:
                    comment = random.choice(male_comments)
                elif female_comments:
                    comment = random.choice(female_comments)
                else:
                    continue

                # Отправляем комментарий
                result = await telegram_manager.send_post_comment(
                    account_id=account.id,
                    chat_id=chat_id,
                    message_id=message_id,
                    comment=comment
                )

                # Логируем результат
                try:
                    log_entry = CommentLog(
                        campaign_id=campaign_id,
                        account_id=account.id,
                        chat_id=chat_id,
                        message_id=message_id,
                        comment=comment,
                        status=result["status"],
                        error_message=result.get("message") if result["status"] == "error" else None
                    )
                    db.add(log_entry)
                    db.commit()

                    if result["status"] == "success":
                        success_count += 1
                        print(f"✅ Комментарий отправлен от аккаунта {account.id}")
                    else:
                        print(f"❌ Ошибка от аккаунта {account.id}: {result.get('message', 'Неизвестная ошибка')}")

                except Exception as log_error:
                    print(f"❌ Исключение при отправке комментария: {log_error}")
                    db.rollback()

                # Задержка между комментариями
                if campaign.delay_seconds > 0:
                    await asyncio.sleep(campaign.delay_seconds)

            except Exception as account_error:
                print(f"❌ Ошибка с аккаунтом {account.id}: {account_error}")
                continue

        # Обновляем статус кампании
        campaign.status = "completed"
        campaign.completed_at = datetime.utcnow()
        db.commit()

        print(f"🎉 Кампания комментирования завершена. Успешно: {success_count}/{len(accounts)}")

    except Exception as e:
        print(f"❌ Ошибка в кампании комментирования {campaign_id}: {e}")
        try:
            campaign.status = "failed"
            db.commit()
        except:
            pass
    finally:
        db.close()

async def run_reaction_campaign(campaign_id: int):
    """Выполнение кампании реакций"""
    try:
        from app.database import ReactionCampaign, get_db_session

        db = get_db_session()
        try:
            campaign = db.query(ReactionCampaign).filter(ReactionCampaign.id == campaign_id).first()
            if not campaign:
                return

            # Получаем активные аккаунты
            accounts = db.query(Account).filter(Account.is_active == True).all()

            # Извлекаем данные из URL
            chat_id, message_id = parse_telegram_url(campaign.post_url)
            if not chat_id or not message_id:
                print(f"❌ Невозможно извлечь данные из URL: {campaign.post_url}")
                return

            campaign.status = "running"
            db.commit()

            for account in accounts:
                if campaign.status != "running":
                    break

                await send_reaction_to_post(account.id, chat_id, message_id, campaign.reaction_emoji)
                await asyncio.sleep(campaign.delay_seconds)

            campaign.status = "completed"
            db.commit()

        finally:
            db.close()

    except Exception as e:
        print(f"❌ Ошибка в кампании реакций {campaign_id}: {e}")

async def send_reaction_to_post(account_id: int, chat_id: str, message_id: int, emoji: str):
    """Отправка реакции на пост"""
    try:
        client = await telegram_manager.get_client(account_id)
        if not client:
            return

        # Отправляем реакцию
        await client.send_reaction(
            chat_id=chat_id,
            message_id=message_id,
            emoji=emoji
        )

        print(f"✅ Реакция {emoji} отправлена аккаунтом {account_id}")

    except Exception as e:
        print(f"❌ Ошибка отправки реакции аккаунтом {account_id}: {e}")

async def run_view_campaign(campaign_id: int):
    """Выполнение кампании просмотров"""
    try:
        from app.database import ViewCampaign, get_db_session

        db = get_db_session()
        try:
            campaign = db.query(ViewCampaign).filter(ViewCampaign.id == campaign_id).first()
            if not campaign:
                return

            # Получаем активные аккаунты
            accounts = db.query(Account).filter(Account.is_active == True).all()

            # Извлекаем данные из URL
            chat_id, message_id = parse_telegram_url(campaign.post_url)
            if not chat_id or not message_id:
                print(f"❌ Невозможно извлечь данные из URL: {campaign.post_url}")
                return

            campaign.status = "running"
            db.commit()

            for account in accounts:
                if campaign.status != "running":
                    break

                await view_post(account.id, chat_id, message_id)
                await asyncio.sleep(campaign.delay_seconds)

            campaign.status = "completed"
            db.commit()

        finally:
            db.close()

    except Exception as e:
        print(f"❌ Ошибка в кампании просмотров {campaign_id}: {e}")

async def view_post(account_id: int, chat_id: str, message_id: int):
    """Просмотр поста"""
    try:
        client = await telegram_manager.get_client(account_id)
        if not client:
            return

        # Читаем сообщение (это засчитывается как просмотр)
        await client.read_chat_history(chat_id=chat_id, max_id=message_id)

        print(f"✅ Пост просмотрен аккаунтом {account_id}")

    except Exception as e:
        print(f"❌ Ошибка просмотра поста аккаунтом {account_id}: {e}")

def parse_telegram_url(url: str):
    """Извлечение chat_id и message_id из URL Telegram"""
    try:
        import re

        # Паттерны для разных форматов URL
        patterns = [
            r'https://t\.me/([^/]+)/(\d+)',  # https://t.me/channel/123
            r'https://telegram\.me/([^/]+)/(\d+)',  # https://telegram.me/channel/123
            r't\.me/([^/]+)/(\d+)',  # t.me/channel/123
        ]

        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                chat_username = match.group(1)
                message_id = int(match.group(2))

                # Проверяем что это не одиночная буква (как @c)
                if len(chat_username) < 5:
                    print(f"⚠️ Подозрительно короткое имя канала: {chat_username}")
                    return None, None

                # Если это username, добавляем @
                if not chat_username.startswith('@') and not chat_username.startswith('-'):
                    chat_username = f"@{chat_username}"

                return chat_username, message_id

        # Если URL не распознан, попробуем извлечь вручную
        print(f"❌ Не удалось парсить URL: {url}")
        return None, None

    except Exception as e:
        print(f"❌ Ошибка парсинга URL: {e}")
        return None, None


async def run_sequential_comments(chat_id, message_id, accounts, male_comments, female_comments, delay_seconds, antispam_mode):
    """Выполнение последовательных комментариев"""
    import random

    # Настройки антиспама
    antispam_delays = {
        'safe': (60, 120),
        'normal': (30, 90),
        'fast': (15, 45)
    }
    min_delay, max_delay = antispam_delays.get(antispam_mode, (60, 120))

    all_comments = []

    # Распределяем комментарии по аккаунтам
    for account in accounts:
        if account.gender == 'male' and male_comments:
            comment = random.choice(male_comments)
            all_comments.append((account.id, comment))
        elif account.gender == 'female' and female_comments:
            comment = random.choice(female_comments)
            all_comments.append((account.id, comment))
        elif male_comments:  # Fallback для аккаунтов без гендера
            comment = random.choice(male_comments + female_comments)
            all_comments.append((account.id, comment))

    # Перемешиваем для случайности
    random.shuffle(all_comments)

    print(f"🚀 Запуск последовательного комментирования: {len(all_comments)} комментариев")

    for i, (account_id, comment) in enumerate(all_comments):
        try:
            print(f"💬 Комментарий {i+1}/{len(all_comments)} от аккаунта {account_id}")

            # Отправляем комментарий
            result = await telegram_manager.send_comment(
                account_id=account_id,
                chat_id=chat_id,
                message_id=message_id,
                comment=comment
            )

            if result["status"] == "success":
                print(f"✅ Комментарий отправлен: {comment[:30]}...")
            else:
                print(f"❌ Ошибка комментария: {result.get('message')}")

            # Умная задержка с антиспамом
            if i < len(all_comments) - 1:  # Не ждем после последнего
                actual_delay = random.randint(min_delay, max_delay)
                print(f"⏱️ Ожидание {actual_delay} секунд...")
                await asyncio.sleep(actual_delay)

        except Exception as e:
            print(f"❌ Ошибка с аккаунтом {account_id}: {e}")
            continue

    print("🎉 Последовательное комментирование завершено")

async def run_multiple_reactions(chat_id, message_id, accounts, reactions, total_count, delay_seconds):
    """Выполнение множественных реакций"""
    import random

    # Распределяем реакции равномерно
    reactions_per_emoji = total_count // len(reactions)
    remainder = total_count % len(reactions)

    reaction_plan = []
    for i, emoji in enumerate(reactions):
        count = reactions_per_emoji + (1 if i < remainder else 0)
        reaction_plan.extend([emoji] * count)

    # Перемешиваем план
    random.shuffle(reaction_plan)

    # Выбираем случайные аккаунты
    selected_accounts = random.sample(accounts, min(len(accounts), len(reaction_plan)))

    print(f"🎭 Запуск реакций: {len(reaction_plan)} реакций от {len(selected_accounts)} аккаунтов")

    for i, (account, emoji) in enumerate(zip(selected_accounts, reaction_plan)):
        try:
            result = await telegram_manager.send_reaction(
                account_id=account.id,
                chat_id=chat_id,
                message_id=message_id,
                emoji=emoji
            )

            if result["status"] == "success":
                print(f"✅ Реакция {emoji} от аккаунта {account.id}")
            else:
                print(f"❌ Ошибка реакции: {result.get('message')}")

            if i < len(reaction_plan) - 1:
                await asyncio.sleep(delay_seconds + random.randint(-5, 10))

        except Exception as e:
            print(f"❌ Ошибка реакции от аккаунта {account.id}: {e}")
            continue

    print("🎉 Множественные реакции завершены")

async def run_post_views(chat_id, message_id, accounts, delay_seconds):
    """Выполнение просмотров постов"""
    import random

    print(f"👀 Запуск просмотров: {len(accounts)} аккаунтов")

    for i, account in enumerate(accounts):
        try:
            result = await telegram_manager.view_message(
                account_id=account.id,
                chat_id=chat_id,
                message_id=message_id
            )

            if result["status"] == "success":
                print(f"✅ Просмотр от аккаунта {account.id}")
            else:
                print(f"❌ Ошибка просмотра: {result.get('message')}")

            if i < len(accounts) - 1:
                await asyncio.sleep(delay_seconds + random.randint(-2, 5))

        except Exception as e:
            print(f"❌ Ошибка просмотра от аккаунта {account.id}: {e}")
            continue

    print("🎉 Просмотры завершены")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)