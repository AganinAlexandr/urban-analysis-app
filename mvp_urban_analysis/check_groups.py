#!/usr/bin/env python3
"""
Проверка групп в базе данных
"""

from app.core.database_fixed import db_manager_fixed
import pandas as pd

def check_groups():
    """Проверка групп в базе данных"""
    print("=== ПРОВЕРКА ГРУПП В БАЗЕ ДАННЫХ ===")
    
    try:
        # Получаем данные из БД
        df = db_manager_fixed.export_to_dataframe(include_analysis=True)
        print(f"Записей в БД: {len(df)}")
        
        # Проверяем поля групп
        print(f"\nПоля в данных:")
        for field in df.columns:
            if 'group' in field.lower():
                print(f"  {field}")
        
        # Проверяем значения групп
        if 'group_type' in df.columns:
            print(f"\nЗначения group_type:")
            groups = df['group_type'].value_counts()
            print(f"  {groups.to_dict()}")
            
            # Проверяем пустые значения
            empty_groups = df[df['group_type'].isna()]
            print(f"  Пустых group_type: {len(empty_groups)}")
        
        if 'detected_group_type' in df.columns:
            print(f"\nЗначения detected_group_type:")
            detected_groups = df['detected_group_type'].value_counts()
            print(f"  {detected_groups.to_dict()}")
            
            # Проверяем пустые значения
            empty_detected = df[df['detected_group_type'].isna()]
            print(f"  Пустых detected_group_type: {len(empty_detected)}")
        
        # Проверяем объекты с координатами
        coords_df = df[df['latitude'].notna() & df['longitude'].notna()]
        print(f"\nОбъектов с координатами: {len(coords_df)}")
        
        if not coords_df.empty:
            print(f"Группы у объектов с координатами:")
            if 'group_type' in coords_df.columns:
                coords_groups = coords_df['group_type'].value_counts()
                print(f"  group_type: {coords_groups.to_dict()}")
            
            if 'detected_group_type' in coords_df.columns:
                coords_detected = coords_df['detected_group_type'].value_counts()
                print(f"  detected_group_type: {coords_detected.to_dict()}")
        
        # Показываем первые несколько записей
        print(f"\nПервые 3 записи:")
        for i, row in df.head(3).iterrows():
            print(f"\nЗапись {i}:")
            print(f"  name: {row.get('name', 'N/A')}")
            print(f"  group_type: {row.get('group_type', 'N/A')}")
            print(f"  detected_group_type: {row.get('detected_group_type', 'N/A')}")
            print(f"  latitude: {row.get('latitude', 'N/A')}")
            print(f"  longitude: {row.get('longitude', 'N/A')}")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    check_groups() 