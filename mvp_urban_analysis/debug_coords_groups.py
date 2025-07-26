#!/usr/bin/env python3
"""
Отладка связи координат и групп
"""

from app.core.database_fixed import db_manager_fixed
import pandas as pd

def debug_coords_groups():
    """Отлаживаем связь координат и групп"""
    print("=== ОТЛАДКА СВЯЗИ КООРДИНАТ И ГРУПП ===")
    
    try:
        # Получаем данные из БД
        df = db_manager_fixed.export_to_dataframe(include_analysis=True)
        
        # Проверяем объекты с координатами
        coords_df = df[df['latitude'].notna() & df['longitude'].notna()]
        print(f"Объектов с координатами: {len(coords_df)}")
        
        # Проверяем группы для объектов с координатами
        print(f"\nГруппы для объектов с координатами:")
        if 'group_type' in coords_df.columns:
            groups = coords_df['group_type'].value_counts()
            print(f"  group_type: {groups.to_dict()}")
        
        if 'detected_group_type' in coords_df.columns:
            detected_groups = coords_df['detected_group_type'].value_counts()
            print(f"  detected_group_type: {detected_groups.to_dict()}")
        
        # Показываем несколько примеров
        print(f"\nПримеры объектов с координатами:")
        for i, row in coords_df.head(5).iterrows():
            print(f"  {row.get('name', 'N/A')}")
            print(f"    Координаты: ({row.get('latitude')}, {row.get('longitude')})")
            print(f"    group_type: {row.get('group_type', 'N/A')}")
            print(f"    detected_group_type: {row.get('detected_group_type', 'N/A')}")
            print(f"    group_name: {row.get('group_name', 'N/A')}")
            print(f"    detected_group_name: {row.get('detected_group_name', 'N/A')}")
            print()
                
    except Exception as e:
        print(f"Ошибка: {e}")

if __name__ == "__main__":
    debug_coords_groups() 