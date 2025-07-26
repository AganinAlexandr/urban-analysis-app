#!/usr/bin/env python3
"""
Диагностика отсутствующих объектов в режиме "Определенные"
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.database_fixed import db_manager_fixed
import pandas as pd

def debug_missing_objects():
    """Диагностирует отсутствующие объекты"""
    print("=== ДИАГНОСТИКА ОТСУТСТВУЮЩИХ ОБЪЕКТОВ ===")
    
    try:
        # Получаем данные из БД
        df = db_manager_fixed.export_to_dataframe(include_analysis=True)
        print(f"Всего записей в БД: {len(df)}")
        
        # Проверяем объекты с координатами
        coords_df = df[df['latitude'].notna() & df['longitude'].notna()]
        print(f"Объектов с координатами: {len(coords_df)}")
        
        print(f"\n=== ВСЕ ОБЪЕКТЫ С КООРДИНАТАМИ ===")
        for i, row in coords_df.iterrows():
            print(f"{i+1}. {row.get('name', 'N/A')}")
            print(f"   Адрес: {row.get('address', 'N/A')}")
            print(f"   Координаты: ({row.get('latitude')}, {row.get('longitude')})")
            print(f"   Группа от поставщика: '{row.get('group_type', 'N/A')}'")
            print(f"   Определенная группа: '{row.get('detected_group_type', 'N/A')}'")
            print()
        
        # Анализируем режим "Определенные"
        print(f"\n=== РЕЖИМ 'ОПРЕДЕЛЕННЫЕ' ===")
        group_field = 'detected_group_type'
        
        # Разделяем на объекты с группой и без группы
        has_group = coords_df[group_field].notna() & (coords_df[group_field] != '') & (coords_df[group_field] != 'None')
        with_group = coords_df[has_group]
        without_group = coords_df[~has_group]
        
        print(f"Объектов с определенной группой: {len(with_group)}")
        print(f"Объектов без определенной группы: {len(without_group)}")
        
        print(f"\n--- ОБЪЕКТЫ С ОПРЕДЕЛЕННОЙ ГРУППОЙ ---")
        for i, row in with_group.iterrows():
            print(f"{i+1}. {row.get('name', 'N/A')} - '{row.get('detected_group_type', 'N/A')}'")
        
        print(f"\n--- ОБЪЕКТЫ БЕЗ ОПРЕДЕЛЕННОЙ ГРУППЫ (должны показываться при 'снять выбор') ---")
        for i, row in without_group.iterrows():
            print(f"{i+1}. {row.get('name', 'N/A')}")
            print(f"   Определенная группа: '{row.get('detected_group_type', 'N/A')}'")
            print(f"   Группа от поставщика: '{row.get('group_type', 'N/A')}'")
        
        # Проверяем логику API карты
        print(f"\n=== ЛОГИКА API КАРТЫ ===")
        
        # Симулируем запрос без фильтров
        print("Без фильтров (должны быть объекты без определенной группы):")
        if len(without_group) > 0:
            print(f"✅ Должно показать {len(without_group)} объектов без группы")
            for i, row in without_group.iterrows():
                print(f"  - {row.get('name', 'N/A')}")
        else:
            print("❌ Нет объектов без определенной группы")
        
        # Симулируем запрос с фильтрами
        print(f"\nС фильтрами (например, 'school'):")
        active_filters = ['school']
        filtered = with_group[with_group[group_field].isin(active_filters)]
        print(f"Объектов с фильтром 'school': {len(filtered)}")
        for i, row in filtered.iterrows():
            print(f"  - {row.get('name', 'N/A')}")
        
        print(f"\n=== ДИАГНОСТИКА ЗАВЕРШЕНА ===")
        
    except Exception as e:
        print(f"❌ Ошибка диагностики: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_missing_objects() 