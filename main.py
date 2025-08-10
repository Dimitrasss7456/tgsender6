
import asyncio
import uvicorn
from app.web import app

if __name__ == "__main__":
    # Запуск веб-сервера
    uvicorn.run(app, host="0.0.0.0", port=5000)
