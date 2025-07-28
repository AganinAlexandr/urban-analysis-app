"""
Проверка настроек Yandex GPT API
"""

import os
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv('env_data.env')

def check_yandex_gpt_setup():
    """Проверка настроек Yandex GPT API"""
    print("=== ПРОВЕРКА НАСТРОЕК YANDEX GPT API ===")
    
    folder_id = os.getenv('YANDEX_GPT_FOLDER_ID')
    token = os.getenv('YANDEX_GPT_OAUTH_TOKEN')
    
    print(f"Folder ID: {folder_id}")
    print(f"Token: {token[:20]}...{token[-10:]}")
    print(f"Token length: {len(token)}")
    
    # Проверяем формат токена
    if token.startswith('y0_'):
        print("✅ Токен имеет правильный формат (начинается с y0_)")
    else:
        print("❌ Токен имеет неправильный формат")
    
    print("\n=== ИНСТРУКЦИИ ПО НАСТРОЙКЕ ===")
    print("1. Перейдите в Yandex Cloud Console: https://console.cloud.yandex.ru/")
    print("2. Выберите ваш проект/каталог")
    print("3. Перейдите в раздел 'Сервисные аккаунты'")
    print("4. Создайте новый сервисный аккаунт или выберите существующий")
    print("5. Назначьте роли:")
    print("   - ai.languageModels.user (для доступа к Yandex GPT)")
    print("   - ai.languageModels.invoker (для вызова API)")
    print("6. Создайте API-ключ для этого сервисного аккаунта")
    print("7. Скопируйте полученный ключ в env_data.env")
    
    print("\n=== АЛЬТЕРНАТИВНЫЙ СПОСОБ ===")
    print("Если у вас есть IAM-токен:")
    print("1. Перейдите в Yandex Cloud Console")
    print("2. Нажмите на ваш профиль в правом верхнем углу")
    print("3. Выберите 'Создать новый токен'")
    print("4. Скопируйте полученный IAM-токен")
    print("5. Используйте его вместо API-ключа")
    
    print("\n=== ПРОВЕРКА ДОСТУПА К API ===")
    print("Убедитесь, что в вашем каталоге:")
    print("1. Включен сервис 'Yandex GPT'")
    print("2. Есть активная подписка на использование API")
    print("3. Не превышены лимиты использования")

if __name__ == "__main__":
    check_yandex_gpt_setup() 