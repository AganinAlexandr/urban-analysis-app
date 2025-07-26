#!/usr/bin/env python3
"""
Исправление проблем с цветами в режиме "Определенные группы"
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.database_fixed import db_manager_fixed
import pandas as pd

def fix_determined_groups_colors():
    """Исправляет проблемы с цветами в режиме определенных групп"""
    print("=== ИСПРАВЛЕНИЕ ПРОБЛЕМ С ЦВЕТАМИ ===")
    
    try:
        # Получаем данные из БД
        df = db_manager_fixed.export_to_dataframe(include_analysis=True)
        print(f"Всего записей в БД: {len(df)}")
        
        # Проверяем дубликаты
        print(f"\n=== ПРОВЕРКА ДУБЛИКАТОВ ===")
        duplicates = df.duplicated(subset=['name', 'address', 'latitude', 'longitude'], keep=False)
        duplicate_count = duplicates.sum()
        print(f"Найдено дубликатов: {duplicate_count}")
        
        if duplicate_count > 0:
            print("Примеры дубликатов:")
            duplicate_df = df[duplicates].head(10)
            for i, row in duplicate_df.iterrows():
                print(f"  - {row.get('name', 'N/A')} ({row.get('latitude')}, {row.get('longitude')})")
        
        # Анализируем проблему с цветами
        print(f"\n=== АНАЛИЗ ПРОБЛЕМЫ С ЦВЕТАМИ ===")
        
        # Проверяем логику API карты
        print("Проблема: В режиме 'Определенные' карта группирует по detected_group_type,")
        print("но цвет определяется по group_type (группа от поставщика)")
        print("Это приводит к неправильным цветам!")
        
        # Показываем примеры
        if 'detected_group_type' in df.columns and 'group_type' in df.columns:
            print(f"\nПримеры неправильных цветов:")
            sample_df = df.head(5)
            for i, row in sample_df.iterrows():
                supplier_group = row.get('group_type', 'N/A')
                determined_group = row.get('detected_group_type', 'N/A')
                print(f"  {row.get('name', 'N/A')}")
                print(f"    Группа от поставщика: '{supplier_group}' (определяет цвет)")
                print(f"    Определенная группа: '{determined_group}' (определяет группировку)")
                print(f"    ❌ Цвет будет от группы '{supplier_group}', а не от '{determined_group}'")
                print()
        
        print("=== РЕШЕНИЕ ===")
        print("Нужно исправить логику в app.py:")
        print("1. В режиме 'Определенные' цвет должен определяться по detected_group_type")
        print("2. В режиме 'От поставщика' цвет определяется по group_type")
        
        return {
            'total_records': len(df),
            'duplicates': duplicate_count,
            'supplier_groups': df['group_type'].value_counts().to_dict() if 'group_type' in df.columns else {},
            'determined_groups': df['detected_group_type'].value_counts().to_dict() if 'detected_group_type' in df.columns else {}
        }
        
    except Exception as e:
        print(f"❌ Ошибка анализа: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    fix_determined_groups_colors() 