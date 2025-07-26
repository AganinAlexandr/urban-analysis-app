#!/usr/bin/env python3
"""
Финальное исправление координат с правильным названием колонки ID
"""
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.data_processor_v2 import DataProcessorV2
from app.core.database_fixed import db_manager_fixed
import pandas as pd

def fix_coordinates_final():
    """Исправляет координаты с правильным названием колонки ID"""
    print("=== ФИНАЛЬНОЕ ИСПРАВЛЕНИЕ КООРДИНАТ ===")
    
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
            
            # Инициализируем процессор данных с API ключом
            processor = DataProcessorV2(geocoder_api_key=yandex_geocoder_key)
            
            # Исправляем координаты для всех объектов
            print(f"\n=== ИСПРАВЛЕНИЕ КООРДИНАТ ===")
            
            with db_manager_fixed.get_connection() as conn:
                cursor = conn.cursor()
                
                updated_count = 0
                
                for i, row in unique_objects.iterrows():
                    name = row.get('name', '')
                    address = row.get('address', '')
                    object_id = row.get('object_id')  # Используем правильное название колонки
                    
                    if not object_id:
                        print(f"  ❌ ID объекта не найден для: {name}")
                        continue
                    
                    try:
                        # Получаем координаты
                        lat, lon, district = processor._get_location_data(name, address)
                        
                        if lat and lon:
                            # Обновляем координаты в БД
                            cursor.execute("""
                                UPDATE objects 
                                SET latitude = ?, longitude = ?, district = ?
                                WHERE id = ?
                            """, (lat, lon, district, object_id))
                            
                            updated_count += 1
                            print(f"  ✅ Обновлен объект: {name}")
                        else:
                            print(f"  ❌ Не удалось получить координаты для: {name}")
                            
                    except Exception as e:
                        print(f"  ❌ Ошибка для {name}: {e}")
                
                conn.commit()
                print(f"\nОбновлено объектов: {updated_count}")
            
            # Проверяем результат
            print(f"\n=== ПРОВЕРКА РЕЗУЛЬТАТА ===")
            df_after = db_manager_fixed.export_to_dataframe(include_analysis=True)
            coords_df = df_after[df_after['latitude'].notna() & df_after['longitude'].notna()]
            unique_coords = coords_df.drop_duplicates(subset=['latitude', 'longitude'], keep='first')
            
            print(f"Записей с координатами: {len(coords_df)}")
            print(f"Уникальных объектов с координатами: {len(unique_coords)}")
            
            if len(unique_coords) > 0:
                print(f"\n=== ОБЪЕКТЫ С КООРДИНАТАМИ ===")
                for i, row in unique_coords.iterrows():
                    print(f"{i+1}. {row.get('name', 'N/A')}")
                    print(f"   Координаты: ({row.get('latitude')}, {row.get('longitude')})")
                    print(f"   Адрес: {row.get('address', 'N/A')}")
                    print(f"   Группа: '{row.get('group_type', 'N/A')}'")
                    print()
                
                print(f"\n🎉 УСПЕХ! Теперь можно проверить карту в приложении!")
            else:
                print(f"\n❌ Координаты не были добавлены!")
            
        else:
            print("❌ Данные не загружены!")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    fix_coordinates_final() 