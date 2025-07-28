"""
Скрипт для проверки переменных окружения
"""

import os
from dotenv import load_dotenv

def check_environment_variables():
    """Проверка переменных окружения"""
    print("=== ПРОВЕРКА ПЕРЕМЕННЫХ ОКРУЖЕНИЯ ===")
    
    # Загружаем переменные из файла .env
    load_dotenv('env_data.env')
    
    # Список переменных для проверки
    env_vars = {
        'YANDEX_GEOCODER_API_KEY': 'Яндекс.Геокодер API ключ',
        'YANDEX_MAPS_API_KEY': 'Яндекс.Карты API ключ',
        'YANDEX_GPT_FOLDER_ID': 'Yandex GPT Folder ID',
        'YANDEX_GPT_OAUTH_TOKEN': 'Yandex GPT OAuth токен',
        'OPENAI_API_KEY': 'OpenAI API ключ',
        'GOOGLE_GEMINI_API_KEY': 'Google Gemini API ключ',
        'YANDEXGPT_API_KEY': 'YandexGPT API ключ',
        'GIGACHAT_API_KEY': 'GigaChat API ключ',
        'QWEN_API_KEY': 'Qwen API ключ',
        'DEEPSEEK_API_KEY': 'DeepSeek API ключ'
    }
    
    print("\nПроверяем переменные окружения:")
    print("-" * 60)
    
    all_ok = True
    
    for var_name, description in env_vars.items():
        value = os.getenv(var_name)
        
        if value and value != 'your_..._here':
            # Скрываем часть значения для безопасности
            if 'TOKEN' in var_name or 'KEY' in var_name:
                display_value = f"{value[:10]}...{value[-5:]}" if len(value) > 15 else "***"
            else:
                display_value = value
            
            print(f"✅ {description}: {display_value}")
        else:
            print(f"❌ {description}: НЕ НАСТРОЕН")
            all_ok = False
    
    print("-" * 60)
    
    if all_ok:
        print("✅ Все обязательные переменные окружения настроены!")
    else:
        print("⚠️  Некоторые переменные окружения не настроены")
        print("\nДля настройки Yandex GPT API убедитесь, что в файле env_data.env указаны:")
        print("YANDEX_GPT_FOLDER_ID=ваш_folder_id")
        print("YANDEX_GPT_OAUTH_TOKEN=ваш_oauth_token")
    
    return all_ok

def test_yandex_gpt_config():
    """Тестирование конфигурации Yandex GPT"""
    print("\n=== ТЕСТИРОВАНИЕ КОНФИГУРАЦИИ YANDEX GPT ===")
    
    try:
        from app.core.yandex_gpt_analyzer import YandexGPTAnalyzer
        
        # Пытаемся создать анализатор
        analyzer = YandexGPTAnalyzer()
        
        print("✅ Yandex GPT Analyzer создан успешно")
        print(f"   Folder ID: {analyzer.folder_id}")
        print(f"   OAuth Token: {analyzer.oauth_token[:10]}...{analyzer.oauth_token[-5:]}")
        
        # Тестируем подключение
        print("\nТестируем подключение к API...")
        if analyzer.test_connection():
            print("✅ Подключение к Yandex GPT API успешно!")
            return True
        else:
            print("❌ Не удалось подключиться к Yandex GPT API")
            return False
            
    except ValueError as e:
        print(f"❌ Ошибка конфигурации: {e}")
        return False
    except Exception as e:
        print(f"❌ Неожиданная ошибка: {e}")
        return False

if __name__ == "__main__":
    # Проверяем переменные окружения
    env_ok = check_environment_variables()
    
    if env_ok:
        # Тестируем конфигурацию Yandex GPT
        test_yandex_gpt_config()
    
    print("\nПроверка завершена!") 