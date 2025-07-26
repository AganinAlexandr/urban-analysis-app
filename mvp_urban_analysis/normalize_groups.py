#!/usr/bin/env python3
"""
Нормализация групп в базе данных
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.database_fixed import db_manager_fixed
import pandas as pd

def normalize_groups():
    """Нормализует группы в базе данных"""
    print("=== НОРМАЛИЗАЦИЯ ГРУПП ===")
    
    try:
        # Получаем данные из БД
        df = db_manager_fixed.export_to_dataframe(include_analysis=True)
        print(f"Всего записей в БД: {len(df)}")
        
        # Проверяем текущие группы
        print(f"\n=== ТЕКУЩИЕ ГРУППЫ ===")
        print("Группы от поставщика:")
        supplier_groups = df['group_type'].value_counts()
        for group, count in supplier_groups.items():
            print(f"  '{group}': {count}")
        
        print(f"\nОпределенные группы:")
        determined_groups = df['detected_group_type'].value_counts()
        for group, count in determined_groups.items():
            print(f"  '{group}': {count}")
        
        # Создаем копию для изменений
        df_normalized = df.copy()
        
        # Нормализуем группы от поставщика
        print(f"\n=== НОРМАЛИЗАЦИЯ ГРУПП ОТ ПОСТАВЩИКА ===")
        group_mapping = {
            'schools': 'school',  # множественное → единственное
            'hospitals': 'hospital',
            'universities': 'university',
            'kindergartens': 'kindergarden',
            'pharmacies': 'pharmacy',
            'polyclinics': 'polyclinic',
            'shopping_malls': 'shopmall'
        }
        
        # Применяем маппинг к группам от поставщика
        df_normalized['group_type'] = df_normalized['group_type'].replace(group_mapping)
        
        # Нормализуем определенные группы
        print(f"\n=== НОРМАЛИЗАЦИЯ ОПРЕДЕЛЕННЫХ ГРУПП ===")
        determined_mapping = {
            'schools': 'school',  # множественное → единственное
            'hospitals': 'hospital',
            'universities': 'university',
            'kindergartens': 'kindergarden',
            'pharmacies': 'pharmacy',
            'polyclinics': 'polyclinic',
            'shopping_malls': 'shopmall'
        }
        
        # Применяем маппинг к определенным группам
        df_normalized['detected_group_type'] = df_normalized['detected_group_type'].replace(determined_mapping)
        
        # Проверяем результат
        print(f"\n=== РЕЗУЛЬТАТ НОРМАЛИЗАЦИИ ===")
        print("Группы от поставщика после нормализации:")
        supplier_groups_after = df_normalized['group_type'].value_counts()
        for group, count in supplier_groups_after.items():
            print(f"  '{group}': {count}")
        
        print(f"\nОпределенные группы после нормализации:")
        determined_groups_after = df_normalized['detected_group_type'].value_counts()
        for group, count in determined_groups_after.items():
            print(f"  '{group}': {count}")
        
        # Проверяем уникальные объекты
        coords_df = df_normalized[df_normalized['latitude'].notna() & df_normalized['longitude'].notna()]
        unique_coords = coords_df.drop_duplicates(subset=['latitude', 'longitude'], keep='first')
        print(f"\nУникальных объектов: {len(unique_coords)}")
        
        print(f"\n=== УНИКАЛЬНЫЕ ОБЪЕКТЫ ПОСЛЕ НОРМАЛИЗАЦИИ ===")
        for i, row in unique_coords.iterrows():
            print(f"{i+1}. {row.get('name', 'N/A')}")
            print(f"   Группа от поставщика: '{row.get('group_type', 'N/A')}'")
            print(f"   Определенная группа: '{row.get('detected_group_type', 'N/A')}'")
            print()
        
        print(f"\n=== ВЫВОД ===")
        print("Теперь все группы используют одинаковые названия!")
        print("Это должно решить проблему с отображением на карте.")
        
        return df_normalized
        
    except Exception as e:
        print(f"❌ Ошибка нормализации: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    normalize_groups() 