#!/usr/bin/env python3
"""
Перезагрузка данных школ с реальным геокодированием
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.database_fixed import db_manager_fixed
from app.core.data_processor_v2 import DataProcessorV2
from dotenv import load_dotenv

def reload_schools_with_geocoding():
    """Перезагружает данные школ с реальным геокодированием"""
    print("=== ПЕРЕЗАГРУЗКА ДАННЫХ ШКОЛ С ГЕОКОДИРОВАНИЕМ ===")
    
    # Загружаем переменные окружения
    load_dotenv('env_data.env')
    
    # Получаем API ключ
    geocoder_api_key = os.getenv('YANDEX_GEOCODER_API_KEY')
    print(f"API ключ геокодера: {geocoder_api_key[:10]}..." if geocoder_api_key else "API ключ не найден")
    
    # Создаем процессор данных с API ключом
    data_processor = DataProcessorV2(geocoder_api_key=geocoder_api_key)
    
    # Очищаем существующие данные школ
    print("\n1. ОЧИСТКА СУЩЕСТВУЮЩИХ ДАННЫХ ШКОЛ...")
    with db_manager_fixed.get_connection() as conn:
        cursor = conn.execute("""
            DELETE FROM objects 
            WHERE group_id IN (
                SELECT id FROM object_groups WHERE group_type = 'schools'
            )
        """)
        deleted_count = cursor.rowcount
        print(f"Удалено объектов школ: {deleted_count}")
    
    # Загружаем данные школ заново
    print("\n2. ЗАГРУЗКА ДАННЫХ ШКОЛ...")
    schools_folder = "data/initial_data/json/schools_parse"
    
    if os.path.exists(schools_folder):
        print(f"Найдена папка с данными школ: {schools_folder}")
        
        # Обрабатываем каждый файл
        for filename in os.listdir(schools_folder):
            if filename.endswith('.json'):
                file_path = os.path.join(schools_folder, filename)
                print(f"Обрабатываем файл: {filename}")
                
                try:
                    # Загружаем данные
                    df = data_processor.load_data(file_path, file_type='json')
                    
                    if not df.empty:
                        # Обрабатываем данные
                        result = data_processor.process_data_to_database(df, source=f"schools_{filename}")
                        print(f"  Обработано записей: {result.get('total_records', 0)}")
                        print(f"  Создано объектов: {result.get('objects_created', 0)}")
                        print(f"  Добавлено отзывов: {result.get('reviews_added', 0)}")
                    else:
                        print(f"  Файл пуст: {filename}")
                        
                except Exception as e:
                    print(f"  Ошибка обработки {filename}: {e}")
    else:
        print(f"Папка не найдена: {schools_folder}")
    
    # Проверяем результат
    print("\n3. ПРОВЕРКА РЕЗУЛЬТАТА...")
    df = db_manager_fixed.export_to_dataframe(include_analysis=True)
    
    # Фильтруем школы
    schools_df = df[df['group_type'] == 'schools']
    print(f"Всего школ в БД: {len(schools_df)}")
    
    # Проверяем координаты
    coords_df = schools_df[schools_df['latitude'].notna() & schools_df['longitude'].notna()]
    print(f"Школ с координатами: {len(coords_df)}")
    
    if not coords_df.empty:
        print("\nПримеры школ с координатами:")
        for i, row in coords_df.head(3).iterrows():
            print(f"  {row['name']}")
            print(f"    Координаты: ({row['latitude']}, {row['longitude']})")
            print(f"    Район: {row.get('district', 'N/A')}")
            print()
    
    print("=== ПЕРЕЗАГРУЗКА ЗАВЕРШЕНА ===")

if __name__ == "__main__":
    reload_schools_with_geocoding() 