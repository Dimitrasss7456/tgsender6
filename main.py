import uvicorn
from app.web import app

if __name__ == "__main__":
    # Проверяем что API ключи установлены
    from app.config import API_ID, API_HASH

    if not API_ID or not API_HASH:
        print("❌ ОШИБКА: API_ID и API_HASH должны быть установлены в файле .env")
        print("📝 Убедитесь что в .env файле есть:")
        print("   API_ID=24599932")
        print("   API_HASH=51bec1393e6f753d6dfcf48bb9c4119d")
        exit(1)

    print(f"✅ API_ID установлен: {API_ID}")
    print(f"✅ API_HASH установлен: {'*' * (len(str(API_HASH)) - 4) + str(API_HASH)[-4:]}")

    uvicorn.run(app, host="0.0.0.0", port=5000)