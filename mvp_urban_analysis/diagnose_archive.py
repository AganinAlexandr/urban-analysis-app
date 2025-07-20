#!/usr/bin/env python3
"""
Диагностика проблем с архивом
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
from app.core.data_processor import DataProcessor
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def diagnose_archive():
    """Диагностика проблем с архивом"""
    
    print("🔍 ДИАГНОСТИКА АРХИВА")
    print("=" * 60)
    
    # Загружаем архив
    processor = DataProcessor()
    df = processor.load_archive()
    
    if df.empty:
        print("❌ Архив пустой")
        return
    
    print(f"📊 Общая статистика:")
    print(f"  - Всего строк: {len(df)}")
    print(f"  - Колонки: {list(df.columns)}")
    print()
    
    # 1. Анализ районов
    print("1️⃣ АНАЛИЗ РАЙОНОВ:")
    if 'district' in df.columns:
        district_counts = df['district'].value_counts()
        print(f"  - Уникальных районов: {len(district_counts)}")
        print(f"  - Распределение районов:")
        for district, count in district_counts.head(10).items():
            print(f"    {district}: {count}")
        
        # Проверяем координаты
        if 'latitude' in df.columns and 'longitude' in df.columns:
            with_coords = df[(df['latitude'] != 0.0) & (df['longitude'] != 0.0)]
            print(f"  - Записей с координатами: {len(with_coords)}")
            print(f"  - Записей без координат: {len(df) - len(with_coords)}")
            
            # Проверяем записи с координатами, но без района
            with_coords_no_district = with_coords[
                (with_coords['district'].isna()) | 
                (with_coords['district'] == '') | 
                (with_coords['district'] == 'Неизвестный район')
            ]
            print(f"  - Записей с координатами, но без района: {len(with_coords_no_district)}")
            
            if len(with_coords_no_district) > 0:
                print("  - Примеры координат без района:")
                for idx, row in with_coords_no_district.head(3).iterrows():
                    print(f"    Строка {idx}: lat={row['latitude']}, lon={row['longitude']}, district='{row['district']}'")
    else:
        print("  ❌ Колонка 'district' отсутствует")
    print()
    
    # 2. Анализ групп
    print("2️⃣ АНАЛИЗ ГРУПП:")
    if 'group' in df.columns:
        group_counts = df['group'].value_counts()
        print(f"  - Уникальных групп: {len(group_counts)}")
        print(f"  - Распределение групп:")
        for group, count in group_counts.items():
            print(f"    {group}: {count}")
        
        # Проверяем пустые группы
        empty_groups = df[df['group'].isna() | (df['group'] == '')]
        print(f"  - Записей с пустой группой: {len(empty_groups)}")
        
        if len(empty_groups) > 0:
            print("  - Примеры записей с пустой группой:")
            for idx, row in empty_groups.head(3).iterrows():
                print(f"    Строка {idx}: name='{row.get('name', 'N/A')}', text='{str(row.get('review_text', ''))[:50]}...'")
    else:
        print("  ❌ Колонка 'group' отсутствует")
    print()
    
    # 3. Анализ хэш-ключей
    print("3️⃣ АНАЛИЗ ХЭШ-КЛЮЧЕЙ:")
    if 'hash_key' in df.columns:
        hash_counts = df['hash_key'].value_counts()
        print(f"  - Уникальных хэш-ключей: {len(hash_counts)}")
        print(f"  - Дубликатов: {len(df) - len(hash_counts)}")
        
        # Проверяем пустые хэши
        empty_hashes = df[df['hash_key'].isna() | (df['hash_key'] == '')]
        print(f"  - Записей с пустым хэшем: {len(empty_hashes)}")
        
        if len(empty_hashes) > 0:
            print("  - Примеры записей с пустым хэшем:")
            for idx, row in empty_hashes.head(3).iterrows():
                print(f"    Строка {idx}: group='{row.get('group', 'N/A')}', name='{row.get('name', 'N/A')}'")
    else:
        print("  ❌ Колонка 'hash_key' отсутствует")
    print()
    
    # 4. Анализ текстовых полей
    print("4️⃣ АНАЛИЗ ТЕКСТОВЫХ ПОЛЕЙ:")
    text_fields = ['review_text', 'answer_text', 'name', 'address']
    
    for field in text_fields:
        if field in df.columns:
            # Проверяем наличие переводов строк
            with_newlines = df[df[field].astype(str).str.contains('\n', na=False)]
            print(f"  - {field}: записей с переводами строк: {len(with_newlines)}")
            
            # Проверяем наличие кавычек
            with_quotes = df[df[field].astype(str).str.contains('"', na=False)]
            print(f"  - {field}: записей с кавычками: {len(with_quotes)}")
            
            if len(with_newlines) > 0:
                print(f"    Примеры {field} с переводами строк:")
                for idx, row in with_newlines.head(2).iterrows():
                    text = str(row[field])
                    print(f"      Строка {idx}: {text[:100]}...")
    
    print()
    
    # 5. Анализ структуры данных
    print("5️⃣ АНАЛИЗ СТРУКТУРЫ ДАННЫХ:")
    print(f"  - Записей с полным набором полей: {len(df.dropna(subset=['group', 'name', 'address', 'review_text']))}")
    print(f"  - Записей с частичными данными: {len(df) - len(df.dropna(subset=['group', 'name', 'address', 'review_text']))}")
    
    # Показываем примеры проблемных записей
    problematic = df[df['group'].isna() | df['name'].isna() | df['review_text'].isna()]
    if len(problematic) > 0:
        print(f"  - Проблемных записей: {len(problematic)}")
        print("  - Примеры проблемных записей:")
        for idx, row in problematic.head(3).iterrows():
            missing_fields = []
            for field in ['group', 'name', 'review_text']:
                if pd.isna(row.get(field, '')):
                    missing_fields.append(field)
            print(f"    Строка {idx}: отсутствуют поля {missing_fields}")
    
    print("\n✅ ДИАГНОСТИКА ЗАВЕРШЕНА")

if __name__ == "__main__":
    diagnose_archive() 