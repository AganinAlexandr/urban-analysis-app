#!/usr/bin/env python3
"""
Исправление существующего архива
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
from app.core.data_processor import DataProcessor
from app.core.csv_processor import CSVProcessor
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fix_archive():
    """Исправление архива"""
    
    print("🔧 ИСПРАВЛЕНИЕ АРХИВА")
    print("=" * 50)
    
    # Загружаем архив
    processor = DataProcessor(geocoder_api_key="4a8fda1a-c9ca-4e3c-97da-e7bd2a15621a")
    df = processor.load_archive()
    
    if df.empty:
        print("❌ Архив пустой")
        return
    
    print(f"📊 Загружено {len(df)} записей")
    
    # 1. Исправляем районы
    print("\n1️⃣ Исправление районов...")
    df_fixed = processor.process_districts(df)
    
    # Проверяем результаты
    district_counts = df_fixed['district'].value_counts()
    print(f"  Распределение районов после исправления:")
    for district, count in district_counts.items():
        print(f"    {district}: {count}")
    
    # 2. Очищаем текстовые поля
    print("\n2️⃣ Очистка текстовых полей...")
    csv_processor = CSVProcessor()
    
    text_fields = ['review_text', 'answer_text', 'name', 'address']
    for field in text_fields:
        if field in df_fixed.columns:
            before_count = df_fixed[field].astype(str).str.contains('\n').sum()
            df_fixed[field] = df_fixed[field].apply(csv_processor.clean_text_field)
            after_count = df_fixed[field].astype(str).str.contains('\n').sum()
            print(f"  {field}: переводов строк {before_count} -> {after_count}")
    
    # 3. Исправляем группы (если есть пустые)
    print("\n3️⃣ Исправление групп...")
    empty_groups = df_fixed[df_fixed['group'].isna() | (df_fixed['group'] == '')]
    print(f"  Записей с пустыми группами: {len(empty_groups)}")
    
    if len(empty_groups) > 0:
        print("  Примеры записей с пустыми группами:")
        for idx, row in empty_groups.head(3).iterrows():
            name = row.get('name', 'N/A')
            print(f"    Строка {idx}: {name}")
    
    # 4. Удаляем проблемные записи (без обязательных полей)
    print("\n4️⃣ Удаление проблемных записей...")
    required_fields = ['group', 'name', 'address', 'review_text']
    valid_mask = df_fixed[required_fields].notna().all(axis=1)
    
    before_count = len(df_fixed)
    df_cleaned = df_fixed[valid_mask].copy()
    after_count = len(df_cleaned)
    
    print(f"  Записей до очистки: {before_count}")
    print(f"  Записей после очистки: {after_count}")
    print(f"  Удалено записей: {before_count - after_count}")
    
    # 5. Сохраняем исправленный архив
    print("\n5️⃣ Сохранение исправленного архива...")
    
    # Создаем резервную копию
    backup_path = processor.archive_file.replace('.csv', '_backup.csv')
    df.to_csv(backup_path, index=False, encoding='utf-8-sig')
    print(f"  Создана резервная копия: {backup_path}")
    
    # Сохраняем исправленный архив
    df_cleaned.to_csv(processor.archive_file, index=False, encoding='utf-8-sig')
    print(f"  Исправленный архив сохранен: {processor.archive_file}")
    
    # 6. Финальная статистика
    print("\n6️⃣ Финальная статистика:")
    print(f"  Всего записей: {len(df_cleaned)}")
    print(f"  Уникальных районов: {len(df_cleaned['district'].value_counts())}")
    print(f"  Уникальных групп: {len(df_cleaned['group'].value_counts())}")
    
    # Показываем топ-5 районов
    print(f"  Топ-5 районов:")
    for district, count in df_cleaned['district'].value_counts().head(5).items():
        print(f"    {district}: {count}")
    
    print("\n✅ ИСПРАВЛЕНИЕ ЗАВЕРШЕНО")

if __name__ == "__main__":
    fix_archive() 