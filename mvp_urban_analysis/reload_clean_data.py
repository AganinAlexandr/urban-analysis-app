#!/usr/bin/env python3
"""
Загрузка данных заново после очистки дубликатов
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.data_processor_v2 import DataProcessorV2
from dotenv import load_dotenv
import os

def reload_clean_data():
    """Загружает данные заново"""
    print("=== ЗАГРУЗКА ДАННЫХ ЗАНОВО ===")
    
    try:
        # Загружаем переменные окружения
        load_dotenv('env_data.env')
        geocoder_api_key = os.getenv('YANDEX_GEOCODER_API_KEY')
        print(f"API ключ геокодера: {geocoder_api_key[:10]}..." if geocoder_api_key else "API ключ не найден")
        
        # Создаем процессор данных
        data_processor = DataProcessorV2(geocoder_api_key=geocoder_api_key)
        
        # Загружаем данные из папок
        data_folders = [
            "data/initial_data/json/schools_parse",
            "data/initial_data/json/hospital_yandex", 
            "data/initial_data/json/kindergarden_parse",
            "data/initial_data/json/university_parse"
        ]
        
        total_processed = 0
        
        for folder in data_folders:
            if os.path.exists(folder):
                print(f"\n📁 Обрабатываем папку: {folder}")
                folder_name = os.path.basename(folder)
                
                for filename in os.listdir(folder):
                    if filename.endswith('.json'):
                        file_path = os.path.join(folder, filename)
                        print(f"  📄 Обрабатываем файл: {filename}")
                        
                        try:
                            df = data_processor.load_data(file_path, file_type='json')
                            if not df.empty:
                                result = data_processor.process_data_to_database(df, source=f"{folder_name}_{filename}")
                                print(f"    ✅ Обработано записей: {result.get('total_records', 0)}")
                                print(f"    ✅ Создано объектов: {result.get('objects_created', 0)}")
                                print(f"    ✅ Добавлено отзывов: {result.get('reviews_added', 0)}")
                                total_processed += result.get('total_records', 0)
                            else:
                                print(f"    ⚠️  Файл пуст: {filename}")
                        except Exception as e:
                            print(f"    ❌ Ошибка обработки {filename}: {e}")
            else:
                print(f"⚠️  Папка не найдена: {folder}")
        
        print(f"\n=== ЗАГРУЗКА ЗАВЕРШЕНА ===")
        print(f"Всего обработано записей: {total_processed}")
        print("Теперь можно проверить карту!")
        
    except Exception as e:
        print(f"❌ Ошибка загрузки: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    reload_clean_data() 