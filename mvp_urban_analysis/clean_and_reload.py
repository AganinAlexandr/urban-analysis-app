#!/usr/bin/env python3
"""
Полная очистка базы данных и загрузка данных заново
"""

import os
import shutil
from app.core.database_fixed import db_manager_fixed
from app.core.data_processor_v2 import DataProcessorV2

def clean_and_reload():
    """Очищаем базу данных и загружаем данные заново"""
    print("=== ПОЛНАЯ ОЧИСТКА И ПЕРЕЗАГРУЗКА ===")
    
    try:
        # 1. Очищаем базу данных
        print("1. Очищаем базу данных...")
        db_manager_fixed.clear_all_data()
        print("✅ База данных очищена")
        
        # 2. Создаем новый процессор данных
        print("2. Создаем процессор данных...")
        processor = DataProcessorV2()
        
        # 3. Загружаем данные из всех папок
        data_path = "data/initial_data/json"
        print(f"3. Загружаем данные из {data_path}...")
        
        # Список папок для загрузки
        folders = [
            "hospital_yandex",
            "schools_parse", 
            "pharmacy_parse",
            "kindergarden_parse",
            "university_parse",
            "shopmall_parse",
            "polyclinic_parse"
        ]
        
        total_loaded = 0
        
        for folder in folders:
            folder_path = os.path.join(data_path, folder)
            if os.path.exists(folder_path):
                print(f"   Загружаем {folder}...")
                try:
                    # Определяем тип группы на основе имени папки
                    group_type = folder.replace('_parse', '').replace('_yandex', '')
                    if group_type == 'kindergarden':
                        group_type = 'kindergartens'
                    elif group_type == 'shopmall':
                        group_type = 'shopping_malls'
                    
                    # Загружаем данные
                    df = processor.load_data(folder_path, file_type='json')
                    if not df.empty:
                        # Обрабатываем данные и сохраняем в БД
                        result = processor.process_data_to_database(df, source=folder)
                        loaded_count = result.get('objects_processed', 0)
                        total_loaded += loaded_count
                        print(f"   ✅ {folder}: {loaded_count} записей")
                    else:
                        print(f"   ⚠️ {folder}: данные не загружены")
                except Exception as e:
                    print(f"   ❌ Ошибка загрузки {folder}: {e}")
            else:
                print(f"   ⚠️ Папка {folder} не найдена")
        
        print(f"\n✅ Всего загружено: {total_loaded} записей")
        
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
        
        print("\n✅ Перезагрузка завершена успешно!")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    clean_and_reload() 