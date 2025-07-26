#!/usr/bin/env python3
"""
Загрузка маленьких JSON файлов для тестирования нормализации
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.data_processor_v2 import DataProcessorV2
from app.core.database_fixed import db_manager_fixed
import pandas as pd
import json

def load_small_test_data():
    """Загружает только маленькие JSON файлы для тестирования"""
    print("=== ЗАГРУЗКА МАЛЕНЬКИХ JSON ФАЙЛОВ ===")
    
    # Список маленьких файлов для тестирования
    small_files = [
        'mvp_urban_analysis/data/initial_data/json/schools_parse/_121799612834.json',
        'mvp_urban_analysis/data/initial_data/json/schools_parse/__1023354102.json',
        'mvp_urban_analysis/data/initial_data/json/hospital_yandex/_82660959957.json',
        'mvp_urban_analysis/data/initial_data/json/hospital_yandex/__244436085018.json',
        'mvp_urban_analysis/data/initial_data/json/hospital_yandex/___1169118578.json',
        'mvp_urban_analysis/data/initial_data/json/kindergarden_parse/_1104101235.json',
        'mvp_urban_analysis/data/initial_data/json/kindergarden_parse/__1001031984.json',
        'mvp_urban_analysis/data/initial_data/json/university_parse/_26751147904.json',
        'mvp_urban_analysis/data/initial_data/json/university_parse/__192254443551.json'
    ]
    
    try:
        # Инициализируем процессор данных
        processor = DataProcessorV2()
        
        total_loaded = 0
        
        for file_path in small_files:
            if os.path.exists(file_path):
                print(f"Обрабатываем файл: {os.path.basename(file_path)}")
                
                # Проверяем размер файла
                file_size = os.path.getsize(file_path)
                print(f"  Размер: {file_size} байт")
                
                try:
                    # Загружаем данные из файла
                    df = processor.load_data(file_path)
                    print(f"  Загружено записей: {len(df)}")
                    
                    # Обрабатываем данные и сохраняем в БД
                    result = processor.process_data_to_database(df, source=os.path.basename(file_path))
                    
                    if result['success']:
                        loaded_count = result['stats']['reviews_processed']
                        total_loaded += loaded_count
                        print(f"  Обработано отзывов: {loaded_count}")
                        print(f"  Обработано объектов: {result['stats']['objects_processed']}")
                    else:
                        print(f"  ❌ Ошибка обработки: {result.get('error', 'Неизвестная ошибка')}")
                        
                except Exception as e:
                    print(f"  ❌ Ошибка загрузки: {e}")
            else:
                print(f"  ⚠️ Файл не найден: {file_path}")
        
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
    load_small_test_data() 