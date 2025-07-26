#!/usr/bin/env python3
"""
Скрипт для отладки создания выборки
"""
import sqlite3
import pandas as pd
from app.core.sample_manager import SampleManager

def debug_sample_creation():
    """Отладка создания выборки"""
    
    # Создаем менеджер выборки
    sample_manager = SampleManager()
    
    # Проверяем данные в БД
    print("=== ДАННЫЕ В БД ===")
    with sample_manager.get_connection() as conn:
        # Проверяем объекты и их группы
        cursor = conn.execute("""
            SELECT o.id, o.name, o.address, og.group_type, dg.group_type as detected_group_type
            FROM objects o
            LEFT JOIN object_groups og ON o.group_id = og.id
            LEFT JOIN detected_groups dg ON o.detected_group_id = dg.id
            LIMIT 10
        """)
        objects = cursor.fetchall()
        print("Объекты в БД:")
        for obj in objects:
            print(f"  ID: {obj[0]}, Name: {obj[1]}, Group: {obj[3]}, Detected: {obj[4]}")
        
        # Проверяем группы от поставщика
        cursor = conn.execute("SELECT group_type FROM object_groups")
        supplier_groups = [row[0] for row in cursor.fetchall()]
        print(f"\nГруппы от поставщика: {supplier_groups}")
        
        # Проверяем определяемые группы
        cursor = conn.execute("SELECT group_type FROM detected_groups")
        detected_groups = [row[0] for row in cursor.fetchall()]
        print(f"Определяемые группы: {detected_groups}")
        
        # Проверяем отзывы
        cursor = conn.execute("SELECT COUNT(*) FROM reviews")
        reviews_count = cursor.fetchone()[0]
        print(f"\nВсего отзывов: {reviews_count}")
        
        # Проверяем отзывы с объектами
        cursor = conn.execute("""
            SELECT r.id, o.name, og.group_type, dg.group_type as detected_group_type
            FROM reviews r
            JOIN objects o ON r.object_id = o.id
            LEFT JOIN object_groups og ON o.group_id = og.id
            LEFT JOIN detected_groups dg ON o.detected_group_id = dg.id
            LIMIT 5
        """)
        reviews = cursor.fetchall()
        print("\nОтзывы с группами:")
        for review in reviews:
            print(f"  Review ID: {review[0]}, Object: {review[1]}, Group: {review[2]}, Detected: {review[3]}")
    
    # Тестируем создание выборки с разными фильтрами
    print("\n=== ТЕСТИРОВАНИЕ СОЗДАНИЯ ВЫБОРКИ ===")
    
    # Тест 1: Без фильтров
    print("\nТест 1: Без фильтров")
    filters1 = {
        'group_filters': [],
        'color_scheme': 'group',
        'sentiment_method': 'rating',
        'group_type': 'supplier'
    }
    result1 = sample_manager.create_sample_from_filters(filters1)
    print(f"Результат: {result1}")
    
    # Тест 2: С фильтрами по группам от поставщика
    if supplier_groups:
        print(f"\nТест 2: С фильтрами по группам от поставщика: {supplier_groups[:2]}")
        filters2 = {
            'group_filters': supplier_groups[:2],
            'color_scheme': 'group',
            'sentiment_method': 'rating',
            'group_type': 'supplier'
        }
        result2 = sample_manager.create_sample_from_filters(filters2)
        print(f"Результат: {result2}")
    
    # Тест 3: С фильтрами по определяемым группам
    if detected_groups:
        print(f"\nТест 3: С фильтрами по определяемым группам: {detected_groups[:2]}")
        filters3 = {
            'group_filters': detected_groups[:2],
            'color_scheme': 'group',
            'sentiment_method': 'rating',
            'group_type': 'determined'
        }
        result3 = sample_manager.create_sample_from_filters(filters3)
        print(f"Результат: {result3}")

if __name__ == "__main__":
    debug_sample_creation() 