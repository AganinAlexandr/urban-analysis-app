#!/usr/bin/env python3
"""
Диагностика проблемы с координатами
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.database_fixed import db_manager_fixed
import pandas as pd

def check_coordinates_issue():
    """Проверяет проблему с координатами"""
    print("=== ДИАГНОСТИКА КООРДИНАТ ===")
    
    try:
        # Получаем данные из БД
        df = db_manager_fixed.export_to_dataframe(include_analysis=True)
        print(f"Всего записей в БД: {len(df)}")
        
        if len(df) > 0:
            # Проверяем координаты
            print(f"\n=== ПРОВЕРКА КООРДИНАТ ===")
            coords_df = df[df['latitude'].notna() & df['longitude'].notna()]
            no_coords_df = df[df['latitude'].isna() | df['longitude'].isna()]
            
            print(f"Записей с координатами: {len(coords_df)}")
            print(f"Записей без координат: {len(no_coords_df)}")
            
            if len(coords_df) > 0:
                print(f"\nПримеры записей с координатами:")
                for i, row in coords_df.head(3).iterrows():
                    print(f"  {row.get('name', 'N/A')}")
                    print(f"    Координаты: ({row.get('latitude')}, {row.get('longitude')})")
                    print(f"    Адрес: {row.get('address', 'N/A')}")
                    print()
            else:
                print(f"\n❌ НЕТ ЗАПИСЕЙ С КООРДИНАТАМИ!")
                
                # Проверяем адреса
                print(f"\n=== ПРОВЕРКА АДРЕСОВ ===")
                addresses = df['address'].value_counts().head(5)
                print("Топ-5 адресов:")
                for addr, count in addresses.items():
                    print(f"  '{addr}': {count}")
                
                # Проверяем названия объектов
                print(f"\n=== ПРОВЕРКА НАЗВАНИЙ ===")
                names = df['name'].value_counts().head(5)
                print("Топ-5 названий:")
                for name, count in names.items():
                    print(f"  '{name}': {count}")
                
                # Проверяем уникальные объекты по названию и адресу
                print(f"\n=== УНИКАЛЬНЫЕ ОБЪЕКТЫ (по названию и адресу) ===")
                unique_objects = df.drop_duplicates(subset=['name', 'address'])
                print(f"Уникальных объектов: {len(unique_objects)}")
                
                for i, row in unique_objects.head(5).iterrows():
                    print(f"{i+1}. {row.get('name', 'N/A')}")
                    print(f"   Адрес: {row.get('address', 'N/A')}")
                    print(f"   Группа: '{row.get('group_type', 'N/A')}'")
                    print(f"   Определенная группа: '{row.get('detected_group_type', 'N/A')}'")
                    print()
            
        else:
            print("❌ Данные не загружены!")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_coordinates_issue() 