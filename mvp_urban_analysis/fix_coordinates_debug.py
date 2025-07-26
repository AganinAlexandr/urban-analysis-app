#!/usr/bin/env python3
"""
Отладка проблемы с обновлением координат
"""
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.data_processor_v2 import DataProcessorV2
from app.core.database_fixed import db_manager_fixed
import pandas as pd

def fix_coordinates_debug():
    """Отлаживает проблему с обновлением координат"""
    print("=== ОТЛАДКА ОБНОВЛЕНИЯ КООРДИНАТ ===")
    
    # Прямо указываем API ключ
    yandex_geocoder_key = "4a8fda1a-c9ca-4e3c-97da-e7bd2a15621a"
    
    try:
        # Получаем данные из БД
        df = db_manager_fixed.export_to_dataframe(include_analysis=True)
        print(f"Всего записей в БД: {len(df)}")
        
        if len(df) > 0:
            # Получаем уникальные объекты
            unique_objects = df.drop_duplicates(subset=['name', 'address'])
            print(f"Уникальных объектов: {len(unique_objects)}")
            
            # Проверяем структуру данных
            print(f"\n=== СТРУКТУРА ДАННЫХ ===")
            print("Колонки в данных:")
            for col in unique_objects.columns:
                print(f"  - {col}")
            
            print(f"\n=== ПЕРВЫЕ 3 ОБЪЕКТА ===")
            for i, row in unique_objects.head(3).iterrows():
                print(f"{i+1}. {row.get('name', 'N/A')}")
                print(f"   ID: {row.get('id', 'N/A')}")
                print(f"   Адрес: {row.get('address', 'N/A')}")
                print(f"   Координаты: ({row.get('latitude')}, {row.get('longitude')})")
                print()
            
            # Инициализируем процессор данных с API ключом
            processor = DataProcessorV2(geocoder_api_key=yandex_geocoder_key)
            
            # Тестируем обновление для первого объекта
            print(f"\n=== ТЕСТИРОВАНИЕ ОБНОВЛЕНИЯ ===")
            
            first_object = unique_objects.iloc[0]
            name = first_object.get('name', '')
            address = first_object.get('address', '')
            object_id = first_object.get('id')
            
            print(f"Тестируем объект: {name}")
            print(f"ID объекта: {object_id}")
            print(f"Адрес: {address}")
            
            if object_id:
                try:
                    # Получаем координаты
                    lat, lon, district = processor._get_location_data(name, address)
                    
                    if lat and lon:
                        print(f"Получены координаты: ({lat}, {lon})")
                        print(f"Район: {district}")
                        
                        # Обновляем координаты в БД
                        with db_manager_fixed.get_connection() as conn:
                            cursor = conn.cursor()
                            
                            cursor.execute("""
                                UPDATE objects 
                                SET latitude = ?, longitude = ?, district = ?
                                WHERE id = ?
                            """, (lat, lon, district, object_id))
                            
                            updated_rows = cursor.rowcount
                            conn.commit()
                            
                            print(f"Обновлено строк: {updated_rows}")
                            
                            if updated_rows > 0:
                                print("✅ Обновление прошло успешно!")
                            else:
                                print("❌ Обновление не произошло!")
                                
                                # Проверяем, существует ли объект с таким ID
                                cursor.execute("SELECT id, name, address FROM objects WHERE id = ?", (object_id,))
                                existing_object = cursor.fetchone()
                                
                                if existing_object:
                                    print(f"Объект найден в БД: {existing_object}")
                                else:
                                    print(f"Объект с ID {object_id} не найден в БД!")
                                    
                    else:
                        print("❌ Не удалось получить координаты")
                        
                except Exception as e:
                    print(f"❌ Ошибка обновления: {e}")
                    import traceback
                    traceback.print_exc()
            else:
                print("❌ ID объекта не найден!")
            
            # Проверяем результат
            print(f"\n=== ПРОВЕРКА РЕЗУЛЬТАТА ===")
            df_after = db_manager_fixed.export_to_dataframe(include_analysis=True)
            coords_df = df_after[df_after['latitude'].notna() & df_after['longitude'].notna()]
            unique_coords = coords_df.drop_duplicates(subset=['latitude', 'longitude'], keep='first')
            
            print(f"Записей с координатами: {len(coords_df)}")
            print(f"Уникальных объектов с координатами: {len(unique_coords)}")
            
        else:
            print("❌ Данные не загружены!")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    fix_coordinates_debug() 