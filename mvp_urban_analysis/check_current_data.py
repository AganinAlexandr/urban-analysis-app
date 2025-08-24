#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для проверки текущего состояния БД
"""

import sqlite3

def check_current_data():
    """Проверяем текущее состояние БД"""
    
    conn = sqlite3.connect('urban_analysis_fixed.db')
    cursor = conn.cursor()
    
    print("=== ПРОВЕРКА ТЕКУЩЕГО СОСТОЯНИЯ БД ===\n")
    
    # 1. Проверяем методы обработки
    print("1. Методы обработки:")
    cursor.execute("""
        SELECT id, method_name, is_active, description 
        FROM processing_methods 
        ORDER BY id
    """)
    
    methods = cursor.fetchall()
    for method in methods:
        status = "✅ АКТИВЕН" if method[2] else "❌ НЕАКТИВЕН"
        print(f"  ID {method[0]}: {method[1]} - {status}")
        print(f"    Описание: {method[3]}")
    
    print()
    
    # 2. Проверяем общую статистику
    print("2. Общая статистика:")
    cursor.execute("SELECT COUNT(*) FROM reviews")
    total_reviews = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM analysis_results")
    total_analysis = cursor.fetchone()[0]
    
    print(f"  Всего отзывов: {total_reviews}")
    print(f"  Всего результатов анализа: {total_analysis}")
    
    print()
    
    # 3. Проверяем результаты по методам
    print("3. Результаты анализа по методам:")
    cursor.execute("""
        SELECT pm.method_name, COUNT(*) as count
        FROM analysis_results ar
        JOIN processing_methods pm ON ar.method_id = pm.id
        GROUP BY pm.method_name
        ORDER BY count DESC
    """)
    
    method_results = cursor.fetchall()
    for method_result in method_results:
        print(f"  {method_result[0]}: {method_result[1]} результатов")
    
    print()
    
    # 4. Проверяем несколько примеров отзывов
    print("4. Примеры отзывов (первые 3):")
    cursor.execute("""
        SELECT r.id, r.review_text, r.rating, o.name as object_name
        FROM reviews r
        JOIN objects o ON r.object_id = o.id
        ORDER BY r.id
        LIMIT 3
    """)
    
    sample_reviews = cursor.fetchall()
    for review in sample_reviews:
        print(f"  Review {review[0]}: '{review[1][:50]}...' (рейтинг: {review[2]})")
        print(f"    Объект: {review[3]}")
        
        # Показываем результаты анализа для этого отзыва
        cursor.execute("""
            SELECT pm.method_name, ar.sentiment, ar.confidence
            FROM analysis_results ar
            JOIN processing_methods pm ON ar.method_id = pm.id
            WHERE ar.review_id = ?
            ORDER BY pm.method_name
        """, (review[0],))
        
        analysis_results = cursor.fetchall()
        if analysis_results:
            print(f"    Результаты анализа:")
            for result in analysis_results:
                print(f"      {result[0]}: {result[1]} (уверенность: {result[2]})")
        else:
            print(f"    Результаты анализа: НЕТ")
        print()
    
    # 5. Проверяем, есть ли результаты YandexGPT
    print("5. Проверка результатов YandexGPT:")
    cursor.execute("""
        SELECT COUNT(*) 
        FROM analysis_results ar
        JOIN processing_methods pm ON ar.method_id = pm.id
        WHERE pm.method_name = 'yandex_gpt'
    """)
    
    yandex_count = cursor.fetchone()[0]
    print(f"  Результатов YandexGPT: {yandex_count}")
    
    if yandex_count > 0:
        print("  Примеры результатов YandexGPT:")
        cursor.execute("""
            SELECT ar.review_id, ar.sentiment, ar.confidence, r.review_text[:30]
            FROM analysis_results ar
            JOIN processing_methods pm ON ar.method_id = pm.id
            JOIN reviews r ON ar.review_id = r.id
            WHERE pm.method_name = 'yandex_gpt'
            ORDER BY ar.review_id
            LIMIT 3
        """)
        
        yandex_examples = cursor.fetchall()
        for example in yandex_examples:
            print(f"    Review {example[0]}: {example[1]} (уверенность: {example[2]})")
            print(f"      Текст: '{example[3]}...'")
    
    conn.close()

if __name__ == "__main__":
    check_current_data()
