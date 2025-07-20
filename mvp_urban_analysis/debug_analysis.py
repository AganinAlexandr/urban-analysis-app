#!/usr/bin/env python3
"""
Отладочный скрипт для понимания структуры данных анализа
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
from app.core.text_analyzer import TextAnalyzer

def debug_analysis():
    """Отладка анализа"""
    
    print("🔍 ОТЛАДКА АНАЛИЗА ТЕКСТА")
    print("=" * 60)
    
    # Создаем простые тестовые данные
    test_data = {
        'group': ['hospital', 'school'],
        'name': ['ГКБ № 29', 'Школа № 1'],
        'address': ['ул. Ленина, 1', 'ул. Пушкина, 10'],
        'review_text': [
            'Очень хорошая поликлиника. Вежливый персонал.',
            'Плохое обслуживание, долгие очереди.'
        ],
        'rating': [5, 2],
        'date': ['2024-01-15', '2024-01-16']
    }
    
    df = pd.DataFrame(test_data)
    
    print("📊 Исходные данные:")
    print(f"  - Записей: {len(df)}")
    print(f"  - Колонки: {list(df.columns)}")
    print()
    
    # Анализируем текст
    print("1️⃣ Анализ текста...")
    text_analyzer = TextAnalyzer()
    analyzed_df = text_analyzer.analyze_dataframe(df)
    
    print(f"   ✅ Проанализировано {len(analyzed_df)} записей")
    print(f"   📊 Колонки после анализа: {list(analyzed_df.columns)}")
    
    # Показываем все данные
    print("\n2️⃣ Данные после анализа:")
    for i, row in analyzed_df.iterrows():
        print(f"   Запись {i}:")
        print(f"     - group: {row.get('group', 'N/A')}")
        print(f"     - review_text: {row.get('review_text', 'N/A')[:50]}...")
        print(f"     - sentiment: {row.get('sentiment', 'N/A')}")
        print(f"     - sentiment_score: {row.get('sentiment_score', 'N/A')}")
        print(f"     - review_type: {row.get('review_type', 'N/A')}")
        print(f"     - positive_words_count: {row.get('positive_words_count', 'N/A')}")
        print(f"     - negative_words_count: {row.get('negative_words_count', 'N/A')}")
        print(f"     - is_complex_part: {row.get('is_complex_part', 'N/A')}")
        print(f"     - part_index: {row.get('part_index', 'N/A')}")
        print()
    
    # Проверяем, какие записи имеют поля анализа
    print("3️⃣ Проверка полей анализа:")
    analysis_fields = ['sentiment', 'sentiment_score', 'review_type', 'positive_words_count', 'negative_words_count']
    
    for field in analysis_fields:
        if field in analyzed_df.columns:
            non_null_count = analyzed_df[field].notna().sum()
            total_count = len(analyzed_df)
            print(f"   - {field}: {non_null_count}/{total_count} ({non_null_count/total_count*100:.1f}%)")
        else:
            print(f"   - {field}: отсутствует")
    
    # Проверяем, есть ли записи без полей анализа
    print("\n4️⃣ Записи без полей анализа:")
    missing_analysis = []
    for i, row in analyzed_df.iterrows():
        missing_fields = []
        for field in analysis_fields:
            if field in analyzed_df.columns and pd.isna(row[field]):
                missing_fields.append(field)
        if missing_fields:
            missing_analysis.append((i, missing_fields))
    
    if missing_analysis:
        print(f"   Найдено {len(missing_analysis)} записей с отсутствующими полями анализа:")
        for idx, fields in missing_analysis:
            print(f"     - Запись {idx}: отсутствуют {fields}")
    else:
        print("   Все записи имеют поля анализа")

if __name__ == "__main__":
    debug_analysis() 