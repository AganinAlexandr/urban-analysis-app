#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для отладки функции get_chart_table
"""

import sqlite3

def debug_chart_table():
    """Отладка функции get_chart_table"""
    
    conn = sqlite3.connect('urban_analysis_fixed.db')
    cursor = conn.cursor()
    
    print("=== ОТЛАДКА get_chart_table ===\n")
    
    # 1. Проверяем базовый запрос для получения объектов
    print("1. Базовый запрос для получения объектов:")
    base_query = """
        SELECT DISTINCT o.id, o.name, o.address, o.latitude, o.longitude, 
               og.group_type as group_type, dg.group_type as determined_group
        FROM objects o
        LEFT JOIN object_groups og ON o.group_id = og.id
        LEFT JOIN detected_groups dg ON o.detected_group_id = dg.id
        WHERE o.latitude IS NOT NULL AND o.longitude IS NOT NULL
    """
    
    cursor.execute(base_query)
    filtered_objects = cursor.fetchall()
    print(f"Найдено объектов: {len(filtered_objects)}")
    
    # Показываем первые 5 объектов
    for i, obj in enumerate(filtered_objects[:5]):
        print(f"  Object {i+1}: ID={obj[0]}, Name='{obj[1]}', Lat={obj[3]}, Lon={obj[4]}")
    
    print()
    
    # 2. Проверяем, есть ли наши объекты в результате
    object_ids = [obj[0] for obj in filtered_objects]
    print(f"2. ID объектов для фильтрации: {object_ids[:10]}...")
    
    # Проверяем, есть ли объекты 1, 2, 3
    test_objects = [1, 2, 3]
    for obj_id in test_objects:
        if obj_id in object_ids:
            print(f"  Object ID {obj_id} найден в фильтрации ✅")
        else:
            print(f"  Object ID {obj_id} НЕ найден в фильтрации ❌")
    
    print()
    
    # 3. Проверяем основной запрос для получения отзывов
    if test_objects:
        test_object_ids_str = ','.join(['?' for _ in test_objects])
        chart_query = """
            SELECT 
                r.id as review_id,
                r.review_text,
                r.rating,
                o.name as object_name,
                og.group_type as object_group,
                pm.method_name,
                ar.sentiment,
                ar.confidence,
                ar.review_type,
                COALESCE(mr.sentiment, '') as master_sentiment
            FROM reviews r
            JOIN objects o ON r.object_id = o.id
            LEFT JOIN object_groups og ON o.group_id = og.id
            LEFT JOIN analysis_results ar ON r.id = ar.review_id
            LEFT JOIN processing_methods pm ON ar.method_id = pm.id
            LEFT JOIN master_ratings mr ON r.id = mr.review_id
            WHERE r.object_id IN ({})
            ORDER BY r.id, pm.method_name
        """.format(test_object_ids_str)
        
        print("3. Основной запрос для отзывов 1-5:")
        cursor.execute(chart_query, test_objects)
        chart_data = cursor.fetchall()
        print(f"Найдено записей: {len(chart_data)}")
        
        # Группируем по отзывам
        reviews_data = {}
        methods_set = set()
        
        for row in chart_data:
            review_id = row[0]
            method_name = row[5]
            sentiment = row[6]
            
            if review_id not in reviews_data:
                reviews_data[review_id] = {'methods': {}}
            
            if method_name:
                methods_set.add(method_name)
                print(f"  Review {review_id}: {method_name} -> {sentiment}")
        
        print(f"\nНайденные методы: {sorted(list(methods_set))}")
    
    print()
    
    # 4. Проверяем, есть ли результаты YandexGPT для отзывов 1-5
    print("4. Проверка результатов YandexGPT:")
    cursor.execute("""
        SELECT ar.review_id, ar.sentiment, pm.method_name
        FROM analysis_results ar
        JOIN processing_methods pm ON ar.method_id = pm.id
        WHERE pm.method_name = 'llm_yandex' AND ar.review_id IN (1,2,3,4,5)
        ORDER BY ar.review_id
    """)
    
    yandex_results = cursor.fetchall()
    print(f"Результатов YandexGPT для отзывов 1-5: {len(yandex_results)}")
    
    for result in yandex_results:
        print(f"  Review {result[0]}: {result[2]} -> {result[1]}")
    
    conn.close()

if __name__ == "__main__":
    debug_chart_table()






