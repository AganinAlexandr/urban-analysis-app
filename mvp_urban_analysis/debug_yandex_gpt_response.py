"""
Детальный анализ ответа от Yandex GPT API
"""

import os
import json
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv('env_data.env')

def debug_yandex_gpt_response():
    """Детальный анализ ответа от Yandex GPT API"""
    print("=== ДЕТАЛЬНЫЙ АНАЛИЗ ОТВЕТА YANDEX GPT ===")
    
    try:
        from app.core.yandex_gpt_analyzer import YandexGPTAnalyzer
        
        # Создаем анализатор
        analyzer = YandexGPTAnalyzer()
        
        # Простой тестовый запрос
        test_prompt = "Ответь одним словом: работает ли API?"
        print(f"Тестовый промпт: {test_prompt}")
        
        # Выполняем запрос напрямую
        response = analyzer._make_request(test_prompt, max_tokens=50)
        
        print(f"\nСтатус ответа: {response is not None}")
        
        if response:
            print(f"\nПолная структура ответа:")
            print(json.dumps(response, indent=2, ensure_ascii=False))
            
            if 'result' in response:
                result = response['result']
                print(f"\nКлючи в 'result': {list(result.keys())}")
                
                if 'alternatives' in result:
                    alternatives = result['alternatives']
                    print(f"Количество alternatives: {len(alternatives)}")
                    
                    for i, alt in enumerate(alternatives):
                        print(f"\nAlternative {i}:")
                        print(f"  Ключи: {list(alt.keys())}")
                        if 'text' in alt:
                            print(f"  Текст: '{alt['text']}'")
                        else:
                            print(f"  Текст отсутствует")
                else:
                    print("Ключ 'alternatives' отсутствует в result")
                    
                    # Проверяем другие возможные ключи
                    for key, value in result.items():
                        print(f"  {key}: {type(value)} = {value}")
            else:
                print("Ключ 'result' отсутствует в ответе")
        else:
            print("Ответ пустой или None")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка при анализе: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_simple_request():
    """Простой тест запроса"""
    print("\n=== ПРОСТОЙ ТЕСТ ЗАПРОСА ===")
    
    try:
        import requests
        
        folder_id = os.getenv('YANDEX_GPT_FOLDER_ID')
        oauth_token = os.getenv('YANDEX_GPT_OAUTH_TOKEN')
        
        if oauth_token.startswith('Bearer '):
            oauth_token = oauth_token[7:]
        
        url = "https://llm.api.cloud.yandex.net/foundationModels/v1/completion"
        headers = {
            "Authorization": f"Bearer {oauth_token}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "modelUri": f"gpt://{folder_id}/yandexgpt-lite",
            "completionOptions": {
                "maxTokens": 50,
                "temperature": 0.3
            },
            "messages": [
                {
                    "role": "user",
                    "text": "Ответь одним словом: работает ли API?"
                }
            ]
        }
        
        print(f"URL: {url}")
        print(f"Model URI: {payload['modelUri']}")
        print(f"Отправляем запрос...")
        
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        
        print(f"Статус код: {response.status_code}")
        print(f"Заголовки ответа: {dict(response.headers)}")
        
        if response.status_code == 200:
            response_data = response.json()
            print(f"Ответ успешен:")
            print(json.dumps(response_data, indent=2, ensure_ascii=False))
        else:
            print(f"Ошибка: {response.text}")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка при простом тесте: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    debug_yandex_gpt_response()
    test_simple_request() 