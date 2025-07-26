#!/usr/bin/env python3
"""
Упрощенная перезагрузка данных с использованием старого DataProcessor
"""

import os
from app.core.database_fixed import db_manager_fixed
from app.core.data_processor import DataProcessor

def simple_reload():
    """Упрощенная перезагрузка данных"""
    print("=== УПРОЩЕННАЯ ПЕРЕЗАГРУЗКА ===")
    
    try:
        # 1. Очищаем базу данных
        print("1. Очищаем базу данных...")
        db_manager_fixed.clear_all_data()
        print("✅ База данных очищена")
        
        # 2. Создаем старый процессор данных
        print("2. Создаем процессор данных...")
        processor = DataProcessor()
        
        # 3. Загружаем данные из одной папки для теста
        data_path = "data/initial_data/json/schools_parse"
        print(f"3. Загружаем данные из {data_path}...")
        
        if os.path.exists(data_path):
            print(f"   Загружаем schools_parse...")
            try:
                # Загружаем данные
                df = processor.load_data(data_path, file_type='json')
                print(f"   Загружено {len(df)} записей")
                
                if not df.empty:
                    # Сохраняем в архив (как раньше работало)
                    success = processor.save_to_archive(df)
                    print(f"   Сохранено в архив: {success}")
                    
                    # Мигрируем из архива в БД
                    print("   Мигрируем в БД...")
                    result = db_manager_fixed.migrate_csv_to_database(df, source='schools_parse')
                    print(f"   Результат миграции: {result}")
                else:
                    print(f"   ⚠️ Данные не загружены")
            except Exception as e:
                print(f"   ❌ Ошибка загрузки: {e}")
        else:
            print(f"   ⚠️ Папка {data_path} не найдена")
        
        # 4. Проверяем результат
        print("4. Проверяем результат...")
        df = db_manager_fixed.export_to_dataframe(include_analysis=True)
        print(f"   Записей в БД: {len(df)}")
        
        # Проверяем координаты
        coords_df = df[df['latitude'].notna() & df['longitude'].notna()]
        print(f"   Записей с координатами: {len(coords_df)}")
        
        # Проверяем группы
        if 'group_type' in df.columns:
            groups = df['group_type'].value_counts()
            print(f"   Группы (group_type): {groups.to_dict()}")
        
        if 'detected_group_type' in df.columns:
            detected_groups = df['detected_group_type'].value_counts()
            print(f"   Группы (detected_group_type): {detected_groups.to_dict()}")
        
        print("\n✅ Упрощенная перезагрузка завершена!")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    simple_reload() 