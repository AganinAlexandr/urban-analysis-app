#!/usr/bin/env python3
"""
Проверка уникальных объектов
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.database_fixed import db_manager_fixed
import pandas as pd

def check_unique_objects():
    """Проверяет уникальные объекты"""
    print("=== ПРОВЕРКА УНИКАЛЬНЫХ ОБЪЕКТОВ ===")
    
    try:
        # Получаем данные из БД
        df = db_manager_fixed.export_to_dataframe(include_analysis=True)
        print(f"Всего записей в БД: {len(df)}")
        
        # Проверяем объекты с координатами
        coords_df = df[df['latitude'].notna() & df['longitude'].notna()]
        print(f"Объектов с координатами: {len(coords_df)}")
        
        # Проверяем уникальные объекты по координатам
        unique_coords = coords_df.drop_duplicates(subset=['latitude', 'longitude'])
        print(f"Уникальных объектов по координатам: {len(unique_coords)}")
        
        # Проверяем уникальные объекты по имени и адресу
        unique_name_addr = coords_df.drop_duplicates(subset=['name', 'address'])
        print(f"Уникальных объектов по имени и адресу: {len(unique_name_addr)}")
        
        print(f"\n=== УНИКАЛЬНЫЕ ОБЪЕКТЫ ===")
        for i, row in unique_coords.iterrows():
            print(f"{i+1}. {row.get('name', 'N/A')}")
            print(f"   Адрес: {row.get('address', 'N/A')}")
            print(f"   Координаты: ({row.get('latitude')}, {row.get('longitude')})")
            print(f"   Группа от поставщика: '{row.get('group_type', 'N/A')}'")
            print(f"   Определенная группа: '{row.get('detected_group_type', 'N/A')}'")
            print()
        
        # Проверяем дубликаты
        print(f"\n=== ДУБЛИКАТЫ ===")
        duplicates = coords_df.duplicated(subset=['name', 'address', 'latitude', 'longitude'], keep=False)
        duplicate_count = duplicates.sum()
        print(f"Найдено дубликатов: {duplicate_count}")
        
        if duplicate_count > 0:
            print("Примеры дубликатов:")
            duplicate_df = coords_df[duplicates].head(10)
            for i, row in duplicate_df.iterrows():
                print(f"  - {row.get('name', 'N/A')} ({row.get('latitude')}, {row.get('longitude')})")
        
        print(f"\n=== ВЫВОД ===")
        print(f"У вас {len(unique_coords)} уникальных объектов, но {len(coords_df)} записей в БД")
        print(f"Это объясняет, почему видны не все объекты - есть дубликаты!")
        
    except Exception as e:
        print(f"❌ Ошибка проверки: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_unique_objects() 