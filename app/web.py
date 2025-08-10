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

    # Статистика
    total_accounts = len(accounts)
    active_accounts = len([a for a in accounts if a.is_active and a.status == "online"])
    total_campaigns = len(campaigns)
    messages_sent_today = db.query(SendLog).filter(
        SendLog.sent_at >= datetime.utcnow().date(),
        SendLog.account_id.in_([a.id for a in accounts]) if accounts else False
    ).count()

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "accounts": accounts,
        "campaigns": campaigns,
        "current_user": current_user,
        "stats": {
            "total_accounts": total_accounts,
            "active_accounts": active_accounts,
            "total_campaigns": total_campaigns,
            "messages_sent_today": messages_sent_today
        }
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

        result = await telegram_manager.add_account(phone, proxy)
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

        result = await telegram_manager.verify_code(phone, clean_code, phone_code_hash, session_name, proxy)

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
    proxy: Optional[str] = Form(None),
    current_user: User = Depends(get_current_user)
):
    """Подтверждение пароля 2FA"""
    result = await telegram_manager.verify_password(phone, password, session_name, proxy)
    return JSONResponse(result)

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

@app.get("/api/stats")
async def get_stats(db: Session = Depends(get_db)):
    """API для получения статистики"""
    accounts = db.query(Account).all()
    campaigns = db.query(Campaign).all()

    return JSONResponse({
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
    })

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
async def start_contacts_campaign(
    account_id: int = Form(...),
    message: str = Form(...),
    delay_seconds: int = Form(5),
    start_in_minutes: Optional[int] = Form(None),
    attachment_path: Optional[str] = Form(None),
    db: Session = Depends(get_db)
):
    """Создание и запуск кампании рассылки по контактам с поддержкой файлов"""
    try:
        if not account_id or not message:
            return JSONResponse({"status": "error", "message": "Не указан аккаунт или сообщение"})

        # Проверяем активность аккаунта
        account = db.query(Account).filter(Account.id == account_id).first()
        if not account or not account.is_active:
            return JSONResponse({"status": "error", "message": "Аккаунт неактивен или не найден"})

        # Проверяем существование файла если он указан
        if attachment_path and not os.path.exists(attachment_path):
            return JSONResponse({"status": "error", "message": "Указанный файл не найден"})

        result = await message_sender.start_contacts_campaign(
            account_id, message, delay_seconds, start_in_minutes, attachment_path
        )

        return JSONResponse(result)

    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)})

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



if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)