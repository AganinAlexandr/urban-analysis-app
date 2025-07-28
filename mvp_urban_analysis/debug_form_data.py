"""
Отладка данных формы при загрузке файла
"""

import os
import json
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv('env_data.env')

def debug_form_data():
    """Отлаживает данные формы при загрузке файла"""
    print("=== ОТЛАДКА ДАННЫХ ФОРМЫ ===")
    
    try:
        # Имитируем данные формы с разными вариантами
        test_cases = [
            {
                'name': 'Только classical',
                'data': {'analysis_methods': json.dumps(['classical'])}
            },
            {
                'name': 'classical + yandex_gpt',
                'data': {'analysis_methods': json.dumps(['classical', 'yandex_gpt'])}
            },
            {
                'name': 'Только yandex_gpt',
                'data': {'analysis_methods': json.dumps(['yandex_gpt'])}
            },
            {
                'name': 'Пустая строка',
                'data': {'analysis_methods': ''}
            },
            {
                'name': 'Отсутствует',
                'data': {}
            }
        ]
        
        for test_case in test_cases:
            print(f"\n--- {test_case['name']} ---")
            
            # Имитируем обработку как в app.py
            analysis_methods = test_case['data'].get('analysis_methods', 'classical')
            print(f"Получены методы анализа из формы: {analysis_methods} (тип: {type(analysis_methods)})")
            
            if isinstance(analysis_methods, str):
                try:
                    # Пытаемся распарсить JSON
                    analysis_methods = json.loads(analysis_methods)
                    print(f"Распарсенные методы из JSON: {analysis_methods}")
                except:
                    # Если не JSON, то это просто строка
                    analysis_methods = [analysis_methods]
                    print(f"Методы как список строк: {analysis_methods}")
            elif not analysis_methods:
                analysis_methods = ['classical']
                print(f"Используем классический метод по умолчанию: {analysis_methods}")
            
            print(f"Финальные методы анализа: {analysis_methods}")
            
            # Создаем анализатор
            from app.core.llm_analysis import LLMAnalyzer
            
            api_keys = {
                'openai': os.getenv('OPENAI_API_KEY'),
                'gemini': os.getenv('GEMINI_API_KEY'),
                'yandex': os.getenv('YANDEX_GPT_OAUTH_TOKEN'),
                'gigachat': os.getenv('GIGACHAT_API_KEY'),
                'qwen': os.getenv('QWEN_API_KEY'),
                'deepseek': os.getenv('DEEPSEEK_API_KEY')
            }
            
            llm_analyzer = LLMAnalyzer(api_keys=api_keys)
            
            # Проверяем, какие методы будут использованы
            available_methods = [m for m in analysis_methods if m in llm_analyzer.available_methods]
            print(f"Доступные из запрошенных: {available_methods}")
            
            if not available_methods:
                print("Ни один из запрошенных методов не доступен, используем классический")
                available_methods = ['classical']
            
            print(f"Итоговые методы для анализа: {available_methods}")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка при отладке: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = debug_form_data()
    
    if success:
        print("\n✅ Отладка завершена успешно!")
    else:
        print("\n❌ Проблемы с отладкой") 