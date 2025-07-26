#!/usr/bin/env python3
"""
Проверка согласованности групп
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.database_fixed import db_manager_fixed
from app.core.config import GROUP_CONFIG
import pandas as pd

def check_group_consistency():
    """Проверяет согласованность групп"""
    print("=== ПРОВЕРКА СОГЛАСОВАННОСТИ ГРУПП ===")
    
    try:
        # Получаем данные из БД
        df = db_manager_fixed.export_to_dataframe(include_analysis=True)
        print(f"Всего записей в БД: {len(df)}")
        
        # Проверяем объекты с координатами
        coords_df = df[df['latitude'].notna() & df['longitude'].notna()]
        unique_coords = coords_df.drop_duplicates(subset=['latitude', 'longitude'], keep='first')
        print(f"Уникальных объектов: {len(unique_coords)}")
        
        print(f"\n=== ГРУППЫ ОТ ПОСТАВЩИКА ===")
        supplier_groups = unique_coords['group_type'].value_counts()
        print("Уникальные значения group_type:")
        for group, count in supplier_groups.items():
            print(f"  '{group}': {count}")
        
        print(f"\n=== ОПРЕДЕЛЕННЫЕ ГРУППЫ ===")
        determined_groups = unique_coords['detected_group_type'].value_counts()
        print("Уникальные значения detected_group_type:")
        for group, count in determined_groups.items():
            print(f"  '{group}': {count}")
        
        print(f"\n=== КОНФИГУРАЦИЯ ФИЛЬТРОВ ===")
        print("Группы от поставщика в конфигурации:")
        for group in GROUP_CONFIG['supplier_groups']:
            print(f"  '{group}'")
        
        print(f"\nОпределенные группы в конфигурации:")
        for group in GROUP_CONFIG['determined_groups']:
            print(f"  '{group}'")
        
        print(f"\n=== ПРОВЕРКА СООТВЕТСТВИЯ ===")
        
        # Проверяем группы от поставщика
        missing_supplier = []
        for group in supplier_groups.index:
            if group not in GROUP_CONFIG['supplier_groups']:
                missing_supplier.append(group)
        
        if missing_supplier:
            print(f"❌ Группы от поставщика, отсутствующие в конфигурации: {missing_supplier}")
        else:
            print("✅ Все группы от поставщика есть в конфигурации")
        
        # Проверяем определенные группы
        missing_determined = []
        for group in determined_groups.index:
            if group not in GROUP_CONFIG['determined_groups']:
                missing_determined.append(group)
        
        if missing_determined:
            print(f"❌ Определенные группы, отсутствующие в конфигурации: {missing_determined}")
        else:
            print("✅ Все определенные группы есть в конфигурации")
        
        print(f"\n=== РЕЗУЛЬТАТ ===")
        print(f"Всего уникальных объектов: {len(unique_coords)}")
        print(f"Объектов с группами от поставщика: {len(supplier_groups)}")
        print(f"Объектов с определенными группами: {len(determined_groups)}")
        
        if missing_supplier or missing_determined:
            print(f"\n⚠️  Нужно добавить недостающие группы в конфигурацию!")
        else:
            print(f"\n✅ Все группы согласованы!")
        
    except Exception as e:
        print(f"❌ Ошибка проверки: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_group_consistency() 