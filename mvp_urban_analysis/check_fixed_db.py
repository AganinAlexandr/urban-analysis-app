#!/usr/bin/env python3
"""
Проверка данных в новой базе данных
"""

from app.core.database_fixed import db_manager_fixed
import pandas as pd

def check_fixed_db():
    """Проверяем данные в новой базе данных"""
    print("=== ПРОВЕРКА НОВОЙ БАЗЫ ДАННЫХ ===")
    
    try:
        # Получаем статистику
        stats = db_manager_fixed.get_statistics()
        print(f"Статистика БД: {stats}")
        
        # Экспортируем данные
        df = db_manager_fixed.export_to_dataframe(include_analysis=True)
        print(f"\nКоличество записей: {len(df)}")
        print(f"Колонки: {list(df.columns)}")
        
        # Проверяем объекты с координатами
        coords_df = df[df['latitude'].notna() & df['longitude'].notna()]
        print(f"\nОбъектов с координатами: {len(coords_df)}")
        
        if len(coords_df) > 0:
            print("\nПримеры объектов с координатами:")
            for i, row in coords_df.head(3).iterrows():
                print(f"  {row.get('name', 'N/A')} - {row.get('address', 'N/A')} - ({row.get('latitude')}, {row.get('longitude')})")
        else:
            print("\n❌ НЕТ ОБЪЕКТОВ С КООРДИНАТАМИ!")
            
        # Проверяем группы
        if 'group_type' in df.columns:
            groups = df['group_type'].value_counts()
            print(f"\nГруппы объектов:")
            for group, count in groups.items():
                print(f"  {group}: {count}")
                
    except Exception as e:
        print(f"Ошибка: {e}")

if __name__ == "__main__":
    check_fixed_db() 