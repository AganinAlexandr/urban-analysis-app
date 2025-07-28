#!/usr/bin/env python3
"""
Отладка данных карты
"""

from app.core.database_fixed import db_manager_fixed
import pandas as pd

def debug_map_data():
    """Отлаживаем данные карты"""
    print("=== ОТЛАДКА ДАННЫХ КАРТЫ ===")
    
    try:
        # Получаем данные из БД
        df = db_manager_fixed.export_to_dataframe(include_analysis=True)
        print(f"Количество записей: {len(df)}")
        print(f"Колонки: {list(df.columns)}")
        
        # Проверяем объекты с координатами
        coords_df = df[df['latitude'].notna() & df['longitude'].notna()]
        print(f"\nОбъектов с координатами: {len(coords_df)}")
        
        if len(coords_df) > 0:
            print("\nПримеры объектов с координатами:")
            for i, row in coords_df.head(3).iterrows():
                print(f"  {row.get('name', 'N/A')} - {row.get('address', 'N/A')} - ({row.get('latitude')}, {row.get('longitude')})")
                
                # Проверяем поля групп
                print(f"    group_type: {row.get('group_type', 'N/A')}")
                print(f"    detected_group_type: {row.get('detected_group_type', 'N/A')}")
                print(f"    group_name: {row.get('group_name', 'N/A')}")
                print(f"    detected_group_name: {row.get('detected_group_name', 'N/A')}")
        
        # Проверяем группы
        print(f"\nГруппы объектов:")
        if 'group_type' in df.columns:
            groups = df['group_type'].value_counts()
            print(f"  group_type: {groups.to_dict()}")
        if 'detected_group_type' in df.columns:
            detected_groups = df['detected_group_type'].value_counts()
            print(f"  detected_group_type: {detected_groups.to_dict()}")
        if 'group_name' in df.columns:
            group_names = df['group_name'].value_counts()
            print(f"  group_name: {group_names.to_dict()}")
        if 'detected_group_name' in df.columns:
            detected_group_names = df['detected_group_name'].value_counts()
            print(f"  detected_group_name: {detected_group_names.to_dict()}")
                
    except Exception as e:
        print(f"Ошибка: {e}")

if __name__ == "__main__":
    debug_map_data() 