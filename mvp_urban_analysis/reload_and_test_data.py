#!/usr/bin/env python3
"""
Загрузка данных и проверка нормализации групп
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.data_processor_v2 import DataProcessorV2
from app.core.database_fixed import db_manager_fixed
import pandas as pd

def reload_and_test_data():
    """Загружает данные и проверяет нормализацию"""
    print("=== ЗАГРУЗКА ДАННЫХ И ПРОВЕРКА НОРМАЛИЗАЦИИ ===")
    
    try:
        # Инициализируем процессор данных
        processor = DataProcessorV2()
        
        # Загружаем данные из JSON папок
        print("Загружаем данные из JSON папок...")
        
        # Пути к папкам с данными
        data_folders = [
            'mvp_urban_analysis/data/initial_data/json/schools_parse',
            'mvp_urban_analysis/data/initial_data/json/hospital_yandex',
            'mvp_urban_analysis/data/initial_data/json/kindergarden_parse',
            'mvp_urban_analysis/data/initial_data/json/university_parse'
        ]
        
        total_loaded = 0
        
        for folder in data_folders:
            if os.path.exists(folder):
                print(f"Обрабатываем папку: {folder}")
                try:
                    loaded_count = processor.load_and_process_data(folder)
                    total_loaded += loaded_count
                    print(f"  Загружено записей: {loaded_count}")
                except Exception as e:
                    print(f"  ❌ Ошибка загрузки {folder}: {e}")
            else:
                print(f"  ⚠️ Папка не найдена: {folder}")
        
        print(f"\nВсего загружено записей: {total_loaded}")
        
        # Проверяем результат
        print(f"\n=== ПРОВЕРКА РЕЗУЛЬТАТА ===")
        df = db_manager_fixed.export_to_dataframe(include_analysis=True)
        print(f"Всего записей в БД: {len(df)}")
        
        if len(df) > 0:
            # Проверяем группы
            print(f"\nГруппы от поставщика:")
            supplier_groups = df['group_type'].value_counts()
            for group, count in supplier_groups.items():
                print(f"  '{group}': {count}")
            
            print(f"\nОпределенные группы:")
            determined_groups = df['detected_group_type'].value_counts()
            for group, count in determined_groups.items():
                print(f"  '{group}': {count}")
            
            # Проверяем уникальные объекты
            coords_df = df[df['latitude'].notna() & df['longitude'].notna()]
            unique_coords = coords_df.drop_duplicates(subset=['latitude', 'longitude'], keep='first')
            print(f"\nУникальных объектов: {len(unique_coords)}")
            
            print(f"\n=== УНИКАЛЬНЫЕ ОБЪЕКТЫ ===")
            for i, row in unique_coords.iterrows():
                print(f"{i+1}. {row.get('name', 'N/A')}")
                print(f"   Группа от поставщика: '{row.get('group_type', 'N/A')}'")
                print(f"   Определенная группа: '{row.get('detected_group_type', 'N/A')}'")
                print()
            
            # Проверяем на множественное число
            print(f"\n=== ПРОВЕРКА НА МНОЖЕСТВЕННОЕ ЧИСЛО ===")
            plural_patterns = ['schools', 'hospitals', 'universities', 'kindergartens', 'pharmacies', 'polyclinics', 'shopping_malls']
            
            found_plural = False
            for group in supplier_groups.index:
                if group in plural_patterns:
                    print(f"  ❌ Найдена группа с множественным числом: '{group}'")
                    found_plural = True
            
            for group in determined_groups.index:
                if group in plural_patterns:
                    print(f"  ❌ Найдена определенная группа с множественным числом: '{group}'")
                    found_plural = True
            
            if not found_plural:
                print("  ✅ Все группы в единственном числе!")
            
        else:
            print("❌ Данные не загружены!")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    reload_and_test_data() 