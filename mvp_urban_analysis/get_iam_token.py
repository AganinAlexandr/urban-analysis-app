"""
Получение IAM-токена для Yandex Cloud
"""

import requests
import json
import os
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv('env_data.env')

def get_iam_token_with_oauth():
    """Получение IAM-токена через OAuth"""
    print("=== ПОЛУЧЕНИЕ IAM-ТОКЕНА ЧЕРЕЗ OAUTH ===")
    
    # Используем существующий OAuth токен
    oauth_token = os.getenv('YANDEX_GPT_OAUTH_TOKEN')
    
    if not oauth_token or oauth_token == 'your_iam_token_here':
        print("❌ OAuth токен не установлен или имеет placeholder значение")
        print("Установите реальный OAuth токен в env_data.env")
        return None
    
    print(f"OAuth Token: {oauth_token[:20]}...{oauth_token[-10:]}")
    
    url = "https://iam.api.cloud.yandex.net/iam/v1/tokens"
    
    headers = {
        "Content-Type": "application/json"
    }
    
    data = {
        "yandexPassportOauthToken": oauth_token
    }
    
    try:
        response = requests.post(url, headers=headers, json=data, timeout=10)
        
        print(f"Статус: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            iam_token = result.get('iamToken')
            expires_at = result.get('expiresAt')
            
            print("✅ IAM-токен получен успешно!")
            print(f"IAM Token: {iam_token[:20]}...{iam_token[-10:]}")
            print(f"Expires at: {expires_at}")
            
            # Сохраняем IAM-токен в env_data.env
            update_env_file(iam_token)
            
            return iam_token
        else:
            print(f"❌ Ошибка получения IAM-токена: {response.text}")
            return None
            
    except Exception as e:
        print(f"❌ Исключение: {e}")
        return None

def update_env_file(iam_token):
    """Обновление файла env_data.env с IAM-токеном"""
    env_file = 'env_data.env'
    
    try:
        # Читаем текущий файл
        with open(env_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        # Ищем строку с YANDEX_IAM_TOKEN
        updated = False
        for i, line in enumerate(lines):
            if line.startswith('YANDEX_IAM_TOKEN='):
                lines[i] = f'YANDEX_IAM_TOKEN={iam_token}\n'
                updated = True
                break
        
        # Если не нашли, добавляем новую строку
        if not updated:
            lines.append(f'YANDEX_IAM_TOKEN={iam_token}\n')
        
        # Записываем обновленный файл
        with open(env_file, 'w', encoding='utf-8') as f:
            f.writelines(lines)
        
        print(f"✅ IAM-токен сохранен в {env_file}")
        
    except Exception as e:
        print(f"❌ Ошибка при обновлении {env_file}: {e}")

def check_oauth_token():
    """Проверка OAuth токена"""
    print("=== ПРОВЕРКА OAUTH ТОКЕНА ===")
    
    oauth_token = os.getenv('YANDEX_GPT_OAUTH_TOKEN')
    
    if not oauth_token:
        print("❌ OAuth токен не установлен")
        return False
    
    if oauth_token == 'your_iam_token_here':
        print("❌ OAuth токен имеет placeholder значение")
        return False
    
    print(f"✅ OAuth токен установлен: {oauth_token[:20]}...{oauth_token[-10:]}")
    return True

if __name__ == "__main__":
    print("Проверяем OAuth токен...")
    
    if check_oauth_token():
        print("\nПолучаем IAM-токен...")
        iam_token = get_iam_token_with_oauth()
        
        if iam_token:
            print("\n✅ IAM-токен получен и сохранен!")
            print("Теперь можно создать API-ключ")
        else:
            print("\n❌ Не удалось получить IAM-токен")
            print("Проверьте правильность OAuth токена")
    else:
        print("\n❌ Нужно установить правильный OAuth токен")
        print("Получите OAuth токен в Yandex Cloud Console") 