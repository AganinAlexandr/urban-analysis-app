#!/usr/bin/env python3
"""
Скрипт для диагностики проблем с маппингом полей и расчетом заполненности
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
from app.core.data_processor import DataProcessor
from app.core.json_processor import JSONProcessor

def test_json_field_mapping():
    """Тестирование маппинга полей в JSON процессоре"""
    
    print("=== Тест маппинга полей JSON ===")
    
    # Создаем тестовые данные с разными названиями полей
    test_data = {
        'company_info': {
            'name': 'Тестовая компания',
            'address': 'ул. Тестовая, 1',
            'group': 'school',
            'rating': 4.5
        },
        'company_reviews': [
            {
                'text': 'Отличный отзыв',
                'date': 1640995200,  # Unix timestamp
                'user_name': 'Пользователь',
                'stars': 5,  # Поле называется stars, а не rating
                'answer': 'Спасибо за отзыв!'
            },
            {
                'text': 'Хороший отзыв',
                'date': 1641081600,
                'user_name': 'Другой пользователь',
                'stars': 4,
                'answer': ''
            }
        ]
    }
    
    # Сохраняем тестовый JSON файл
    import json
    test_file = 'test_debug_mapping.json'
    with open(test_file, 'w', encoding='utf-8') as f:
        json.dump(test_data, f, ensure_ascii=False, indent=2)
    
    print(f"Создан тестовый файл: {test_file}")
    print("Исходные данные:")
    print(f"  - Поле рейтинга: 'stars' (значения: 5, 4)")
    print(f"  - Поле ответа: 'answer' (значения: 'Спасибо за отзыв!', '')")
    print()
    
    # Обрабатываем через JSON процессор
    json_processor = JSONProcessor()
    df = json_processor.process_json_file_or_directory(test_file)
    
    print("Результат обработки JSON:")
    print(f"  - Колонки: {list(df.columns)}")
    print(f"  - Записей: {len(df)}")
    
    if 'rating' in df.columns:
        print(f"  - Поле 'rating': {df['rating'].tolist()}")
    else:
        print("  - Поле 'rating' отсутствует!")
    
    if 'answer_text' in df.columns:
        print(f"  - Поле 'answer_text': {df['answer_text'].tolist()}")
    else:
        print("  - Поле 'answer_text' отсутствует!")
    
    # Очищаем тестовый файл
    os.remove(test_file)
    
    return df

def test_completeness_calculation():
    """Тестирование расчета заполненности полей"""
    
    print("\n=== Тест расчета заполненности ===")
    
    # Создаем тестовые данные с разной заполненностью
    test_data = {
        'group': ['school', 'hospital', 'pharmacy'],
        'name': ['Школа №1', 'Больница №2', 'Аптека №3'],
        'address': ['ул. Ленина, 1', 'ул. Пушкина, 10', 'ул. Гагарина, 5'],
        'review_text': ['Отзыв 1', 'Отзыв 2', 'Отзыв 3'],
        'rating': [5.0, None, 4.0],  # Одно пустое значение
        'answer_text': ['Ответ 1', None, ''],  # Одно None, одно пустая строка
        'date': ['2024-01-15', '2024-01-16', '2024-01-17']
    }
    
    df = pd.DataFrame(test_data)
    
    print("Исходные данные:")
    print(f"  - Всего записей: {len(df)}")
    print(f"  - Поле 'rating': {df['rating'].tolist()}")
    print(f"  - Поле 'answer_text': {df['answer_text'].tolist()}")
    print()
    
    # Тестируем расчет заполненности
    processor = DataProcessor()
    completeness = processor._calculate_field_completeness(df)
    
    print("Результат расчета заполненности:")
    for field, percentage in completeness.items():
        print(f"  - {field}: {percentage}%")
    
    return completeness

def test_archive_completeness():
    """Тестирование заполненности в архиве"""
    
    print("\n=== Тест заполненности в архиве ===")
    
    processor = DataProcessor()
    
    # Загружаем архив
    df = processor.load_archive()
    
    if df.empty:
        print("Архив пуст")
        return
    
    print(f"Архив содержит {len(df)} записей")
    print(f"Колонки в архиве: {list(df.columns)}")
    
    # Проверяем заполненность
    completeness = processor._calculate_field_completeness(df)
    
    print("\nЗаполненность полей в архиве:")
    for field, percentage in completeness.items():
        print(f"  - {field}: {percentage}%")
    
    # Проверяем, есть ли поля latitude, longitude, district в исходных данных
    print("\nПроблема: поля latitude, longitude, district показывают 100% заполненность")
    print("Это происходит потому, что эти поля добавляются автоматически в процессе обработки")
    print("Нужно изменить логику расчета заполненности")

def main():
    """Основная функция диагностики"""
    
    print("🔍 ДИАГНОСТИКА ПРОБЛЕМ С ПОЛЯМИ")
    print("=" * 50)
    
    # Тест 1: Маппинг полей JSON
    df_json = test_json_field_mapping()
    
    # Тест 2: Расчет заполненности
    completeness = test_completeness_calculation()
    
    # Тест 3: Заполненность в архиве
    test_archive_completeness()
    
    print("\n" + "=" * 50)
    print("📋 ВЫВОДЫ:")
    print("1. Проверить маппинг поля 'stars' -> 'rating' в JSON процессоре")
    print("2. Изменить логику расчета заполненности для полей, добавляемых автоматически")
    print("3. Возможно, нужно отслеживать исходные поля отдельно от обработанных")

if __name__ == "__main__":
    main() 