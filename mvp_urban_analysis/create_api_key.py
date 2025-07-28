"""
Создание API-ключа для Yandex GPT через REST API
"""

import requests
import json
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv('env_data.env')

def create_api_key():
    """Создание API-ключа через REST API"""
    print("=== СОЗДАНИЕ API-КЛЮЧА ДЛЯ YANDEX GPT ===")
    
    # Получаем данные из переменных окружения
    service_account_id = os.getenv('YANDEX_SERVICE_ACCOUNT_ID')
    iam_token = os.getenv('YANDEX_IAM_TOKEN')
    folder_id = os.getenv('YANDEX_GPT_FOLDER_ID')
    
    print(f"Service Account ID: {service_account_id}")
    print(f"IAM Token: {iam_token[:20] if iam_token else 'НЕ УСТАНОВЛЕН'}...")
    print(f"Folder ID: {folder_id}")
    
    if not service_account_id:
        print("❌ Ошибка: YANDEX_SERVICE_ACCOUNT_ID не установлен")
        print("Добавьте в env_data.env:")
        print("YANDEX_SERVICE_ACCOUNT_ID=ваш_идентификатор_сервисного_аккаунта")
        return None
    
    if not iam_token:
        print("❌ Ошибка: YANDEX_IAM_TOKEN не установлен")
        print("Добавьте в env_data.env:")
        print("YANDEX_IAM_TOKEN=ваш_iam_токен")
        return None
    
    # Устанавливаем срок действия ключа (1 год)
    expires_at = (datetime.now() + timedelta(days=365)).isoformat() + "Z"
    
    url = "https://iam.api.cloud.yandex.net/iam/v1/apiKeys"
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {iam_token}"
    }
    
    data = {
        "serviceAccountId": service_account_id,
        "expiresAt": expires_at
    }
    
    print(f"\nОтправляем запрос на создание API-ключа...")
    print(f"URL: {url}")
    print(f"Scopes: не указаны (будут использованы области действия по умолчанию)")
    print(f"Expires at: {expires_at}")
    
    try:
        response = requests.post(url, headers=headers, json=data, timeout=30)
        
        print(f"Статус ответа: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print("✅ API-ключ успешно создан!")
            print(f"ID ключа: {result.get('id')}")
            print(f"Secret: {result.get('secret')}")
            
            # Сохраняем новый ключ в env_data.env
            update_env_file(result.get('secret'))
            
            return result.get('secret')
        else:
            print(f"❌ Ошибка создания API-ключа: {response.text}")
            return None
            
    except Exception as e:
        print(f"❌ Исключение при создании API-ключа: {e}")
        return None

def update_env_file(api_key):
    """Обновление файла env_data.env с новым API-ключом"""
    env_file = 'env_data.env'
    
    try:
        # Читаем текущий файл
        with open(env_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        # Ищем строку с YANDEX_GPT_OAUTH_TOKEN
        updated = False
        for i, line in enumerate(lines):
            if line.startswith('YANDEX_GPT_OAUTH_TOKEN='):
                lines[i] = f'YANDEX_GPT_OAUTH_TOKEN={api_key}\n'
                updated = True
                break
        
        # Если не нашли, добавляем новую строку
        if not updated:
            lines.append(f'YANDEX_GPT_OAUTH_TOKEN={api_key}\n')
        
        # Записываем обновленный файл
        with open(env_file, 'w', encoding='utf-8') as f:
            f.writelines(lines)
        
        print(f"✅ API-ключ сохранен в {env_file}")
        
    except Exception as e:
        print(f"❌ Ошибка при обновлении {env_file}: {e}")

def get_service_account_info():
    """Получение информации о сервисных аккаунтах"""
    print("\n=== ПОЛУЧЕНИЕ ИНФОРМАЦИИ О СЕРВИСНЫХ АККАУНТАХ ===")
    
    iam_token = os.getenv('YANDEX_IAM_TOKEN')
    folder_id = os.getenv('YANDEX_GPT_FOLDER_ID')
    
    if not iam_token:
        print("❌ YANDEX_IAM_TOKEN не установлен")
        return
    
    url = f"https://iam.api.cloud.yandex.net/iam/v1/serviceAccounts?folderId={folder_id}"
    
    headers = {
        "Authorization": f"Bearer {iam_token}"
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        
        print(f"Статус: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            service_accounts = result.get('serviceAccounts', [])
            
            print(f"Найдено сервисных аккаунтов: {len(service_accounts)}")
            
            for i, account in enumerate(service_accounts, 1):
                print(f"\n{i}. ID: {account.get('id')}")
                print(f"   Имя: {account.get('name')}")
                print(f"   Описание: {account.get('description', 'Нет описания')}")
                print(f"   Создан: {account.get('createdAt')}")
                
                # Проверяем роли
                roles_url = f"https://iam.api.cloud.yandex.net/iam/v1/serviceAccounts/{account.get('id')}/roles"
                roles_response = requests.get(roles_url, headers=headers, timeout=10)
                
                if roles_response.status_code == 200:
                    roles_result = roles_response.json()
                    roles = roles_result.get('roles', [])
                    print(f"   Роли: {', '.join([role.get('roleId') for role in roles])}")
                else:
                    print(f"   Роли: не удалось получить")
        else:
            print(f"❌ Ошибка: {response.text}")
            
    except Exception as e:
        print(f"❌ Исключение: {e}")

if __name__ == "__main__":
    print("Выберите действие:")
    print("1. Получить информацию о сервисных аккаунтах")
    print("2. Создать API-ключ")
    
    choice = input("Введите номер (1 или 2): ").strip()
    
    if choice == "1":
        get_service_account_info()
    elif choice == "2":
        api_key = create_api_key()
        if api_key:
            print(f"\n✅ Новый API-ключ создан и сохранен!")
            print("Теперь можно протестировать подключение к Yandex GPT API")
    else:
        print("Неверный выбор") 