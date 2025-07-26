#!/usr/bin/env python3
"""
Простая очистка дубликатов в базе данных
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.database_fixed import db_manager_fixed
import pandas as pd
import sqlite3

def clean_duplicates_simple():
    """Простая очистка дубликатов"""
    print("=== ПРОСТАЯ ОЧИСТКА ДУБЛИКАТОВ ===")
    
    try:
        # Получаем данные из БД
        df = db_manager_fixed.export_to_dataframe(include_analysis=True)
        print(f"Всего записей до очистки: {len(df)}")
        
        # Проверяем дубликаты
        duplicates = df.duplicated(subset=['name', 'address', 'latitude', 'longitude'], keep=False)
        duplicate_count = duplicates.sum()
        print(f"Найдено дубликатов: {duplicate_count}")
        
        if duplicate_count == 0:
            print("✅ Дубликатов не найдено!")
            return
        
        # Удаляем дубликаты
        print(f"\nУдаляем дубликаты...")
        df_clean = df.drop_duplicates(subset=['name', 'address', 'latitude', 'longitude'], keep='first')
        print(f"Записей после очистки: {len(df_clean)}")
        print(f"Удалено дубликатов: {len(df) - len(df_clean)}")
        
        # Показываем уникальные объекты
        print(f"\nУникальные объекты после очистки:")
        for i, row in df_clean.iterrows():
            print(f"  {i+1}. {row.get('name', 'N/A')}")
            print(f"     Адрес: {row.get('address', 'N/A')}")
            print(f"     Координаты: ({row.get('latitude')}, {row.get('longitude')})")
            print(f"     Группа от поставщика: '{row.get('group_type', 'N/A')}'")
            print(f"     Определенная группа: '{row.get('detected_group_type', 'N/A')}'")
            print()
        
        print("=== ОЧИСТКА ЗАВЕРШЕНА ===")
        print("Теперь нужно перезапустить приложение и загрузить данные заново!")
        
    except Exception as e:
        print(f"❌ Ошибка очистки: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    clean_duplicates_simple() 