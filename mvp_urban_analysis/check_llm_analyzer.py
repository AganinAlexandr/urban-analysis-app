#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для проверки LLMAnalyzer
"""

import os
import sys
sys.path.append('.')

from app.core.llm_analysis import LLMAnalyzer

def check_llm_analyzer():
    """Проверяем LLMAnalyzer"""
    
    print("=== ПРОВЕРКА LLM ANALYZER ===\n")
    
    # Получаем API ключи
    api_keys = {
        'openai': os.getenv('OPENAI_API_KEY'),
        'gemini': os.getenv('GEMINI_API_KEY'),
        'yandex': os.getenv('YANDEX_GPT_OAUTH_TOKEN'),
        'gigachat': os.getenv('GIGACHAT_API_KEY'),
        'qwen': os.getenv('QWEN_API_KEY'),
        'deepseek': os.getenv('DEEPSEEK_API_KEY')
    }
    
    print("1. API ключи:")
    for key, value in api_keys.items():
        status = "✅ ЕСТЬ" if value else "❌ НЕТ"
        print(f"  {key}: {status}")
    
    print()
    
    # Проверяем переменные окружения Yandex
    yandex_folder_id = os.getenv('YANDEX_GPT_FOLDER_ID')
    yandex_oauth_token = os.getenv('YANDEX_GPT_OAUTH_TOKEN')
    
    print("2. Переменные окружения Yandex:")
    print(f"  YANDEX_GPT_FOLDER_ID: {'✅ ЕСТЬ' if yandex_folder_id else '❌ НЕТ'}")
    print(f"  YANDEX_GPT_OAUTH_TOKEN: {'✅ ЕСТЬ' if yandex_oauth_token else '❌ НЕТ'}")
    
    print()
    
    # Создаем анализатор
    print("3. Создание LLMAnalyzer...")
    try:
        analyzer = LLMAnalyzer(api_keys=api_keys)
        print("✅ LLMAnalyzer создан успешно")
    except Exception as e:
        print(f"❌ Ошибка создания LLMAnalyzer: {e}")
        return
    
    print()
    
    # Проверяем доступные методы
    print("4. Доступные методы:")
    print(f"  available_methods: {analyzer.available_methods}")
    
    print()
    
    # Проверяем, есть ли yandex_gpt в доступных методах
    if 'yandex_gpt' in analyzer.available_methods:
        print("✅ yandex_gpt доступен в LLMAnalyzer")
    else:
        print("❌ yandex_gpt НЕ доступен в LLMAnalyzer")
    
    print()
    
    # Проверяем, есть ли llm_yandex в доступных методах
    if 'llm_yandex' in analyzer.available_methods:
        print("✅ llm_yandex доступен в LLMAnalyzer")
    else:
        print("❌ llm_yandex НЕ доступен в LLMAnalyzer")
    
    print()
    
    # Пробуем проанализировать тестовый текст
    print("5. Тестовый анализ:")
    test_text = "Отличный сервис, очень доволен!"
    
    try:
        if 'yandex_gpt' in analyzer.available_methods:
            result = analyzer.analyze_sentiment_yandex(test_text)
            print(f"  yandex_gpt результат: {result}")
        else:
            print("  yandex_gpt недоступен для тестирования")
    except Exception as e:
        print(f"  ❌ Ошибка анализа yandex_gpt: {e}")

if __name__ == "__main__":
    check_llm_analyzer()






