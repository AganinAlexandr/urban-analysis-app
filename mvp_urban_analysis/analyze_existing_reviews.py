#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для анализа существующих отзывов с помощью YandexGPT
"""

import sqlite3
import sys
import os
sys.path.append('.')

from app.core.llm_analysis import LLMAnalyzer

def normalize_sentiment(sentiment):
    """Приводим сентимент к стандартному виду (positive, neutral, negative)"""
    
    sentiment = str(sentiment).lower().strip()
    
    # Маппинг различных вариантов сентимента
    sentiment_map = {
        # Положительные
        'positive': 'positive',
        'положительный': 'positive',
        'позитивный': 'positive',
        'хороший': 'positive',
        'отличный': 'positive',
        'великолепный': 'positive',
        'замечательный': 'positive',
        
        # Нейтральные
        'neutral': 'neutral',
        'нейтральный': 'neutral',
        'средний': 'neutral',
        'обычный': 'neutral',
        
        # Отрицательные
        'negative': 'negative',
        'отрицательный': 'negative',
        'негативный': 'negative',
        'плохой': 'negative',
        'ужасный': 'negative'
    }
    
    return sentiment_map.get(sentiment, 'neutral')  # по умолчанию neutral

def translate_review_type(russian_type):
    """Переводим русские типы отзывов в английские для соответствия БД"""
    
    translation_map = {
        'благодарность': 'gratitude',
        'жалоба': 'complaint',
        'предложение': 'suggestion',
        'информационный': 'informational'
    }
    
    return translation_map.get(russian_type.lower(), 'informational')

def analyze_existing_reviews():
    """Анализируем существующие отзывы с помощью YandexGPT"""
    
    print("=== АНАЛИЗ СУЩЕСТВУЮЩИХ ОТЗЫВОВ ===\n")
    
    # 1. Подключаемся к БД
    conn = sqlite3.connect('urban_analysis_fixed.db')
    cursor = conn.cursor()
    
    # 2. Получаем отзывы, которые еще не проанализированы YandexGPT
    print("1. Поиск отзывов для анализа...")
    
    cursor.execute("""
        SELECT r.id, r.review_text, r.rating, o.name as object_name
        FROM reviews r
        JOIN objects o ON r.object_id = o.id
        WHERE r.id NOT IN (
            SELECT DISTINCT ar.review_id 
            FROM analysis_results ar
            JOIN processing_methods pm ON ar.method_id = pm.id
            WHERE pm.method_name = 'yandex_gpt'
        )
        ORDER BY r.id
        LIMIT 10  -- Начинаем с 10 отзывов для тестирования
    """)
    
    reviews_to_analyze = cursor.fetchall()
    print(f"Найдено отзывов для анализа: {len(reviews_to_analyze)}")
    
    if not reviews_to_analyze:
        print("✅ Все отзывы уже проанализированы YandexGPT!")
        conn.close()
        return
    
    # 3. Создаем LLM анализатор
    print("\n2. Создание LLM анализатора...")
    
    api_keys = {
        'yandex': os.getenv('YANDEX_GPT_OAUTH_TOKEN')
    }
    
    try:
        analyzer = LLMAnalyzer(api_keys=api_keys)
        print(f"✅ LLMAnalyzer создан. Доступные методы: {analyzer.available_methods}")
        
        if 'yandex_gpt' not in analyzer.available_methods:
            print("❌ yandex_gpt недоступен в анализаторе!")
            conn.close()
            return
            
    except Exception as e:
        print(f"❌ Ошибка создания LLMAnalyzer: {e}")
        conn.close()
        return
    
    # 4. Получаем ID метода yandex_gpt
    cursor.execute("SELECT id FROM processing_methods WHERE method_name = 'yandex_gpt'")
    yandex_method_id = cursor.fetchone()[0]
    print(f"ID метода yandex_gpt: {yandex_method_id}")
    
    # 5. Анализируем отзывы
    print(f"\n3. Анализ отзывов (начинаем с {len(reviews_to_analyze)}):")
    
    success_count = 0
    error_count = 0
    
    for i, (review_id, review_text, rating, object_name) in enumerate(reviews_to_analyze, 1):
        print(f"\n  Отзыв {i}/{len(reviews_to_analyze)} (ID: {review_id}):")
        print(f"    Объект: {object_name}")
        print(f"    Текст: {review_text[:100]}...")
        
        try:
            # Анализируем с помощью YandexGPT
            result = analyzer.analyze_sentiment_yandex(review_text)
            print(f"    📝 Исходный сентимент: {result['sentiment']}")
            print(f"    📝 Тип отзыва (рус): {result['review_type']}")
            
            # Нормализуем сентимент
            normalized_sentiment = normalize_sentiment(result['sentiment'])
            print(f"    🔄 Нормализованный сентимент: {normalized_sentiment}")
            
            # Переводим тип отзыва в английский
            english_review_type = translate_review_type(result['review_type'])
            print(f"    🔄 Тип отзыва (англ): {english_review_type}")
            
            # Сохраняем результат в БД
            cursor.execute("""
                INSERT INTO analysis_results (review_id, method_id, sentiment, confidence, review_type)
                VALUES (?, ?, ?, ?, ?)
            """, (
                review_id, 
                yandex_method_id, 
                normalized_sentiment,      # используем нормализованный сентимент
                result['confidence'], 
                english_review_type        # используем переведенный тип отзыва
            ))
            
            success_count += 1
            print(f"    💾 Сохранено в БД!")
            
        except Exception as e:
            print(f"    ❌ Ошибка анализа: {e}")
            error_count += 1
        
        # Делаем паузу между запросами (чтобы не перегружать API)
        if i < len(reviews_to_analyze):
            import time
            time.sleep(1)
    
    # 6. Сохраняем изменения
    conn.commit()
    
    print(f"\n4. Результаты анализа:")
    print(f"  ✅ Успешно: {success_count}")
    print(f"  ❌ Ошибок: {error_count}")
    
    # 7. Проверяем итоговую статистику
    print(f"\n5. Итоговая статистика:")
    cursor.execute("""
        SELECT COUNT(*) FROM analysis_results ar
        JOIN processing_methods pm ON ar.method_id = pm.id
        WHERE pm.method_name = 'yandex_gpt'
    """)
    
    total_yandex_results = cursor.fetchone()[0]
    print(f"  Всего результатов YandexGPT: {total_yandex_results}")
    
    conn.close()
    print("\n✅ Анализ завершен!")

if __name__ == "__main__":
    analyze_existing_reviews()
