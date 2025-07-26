#!/usr/bin/env python3
"""
Проверка всех групп на множественное число
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.database_fixed import db_manager_fixed
import pandas as pd

def check_all_groups():
    """Проверяет все группы на множественное число"""
    print("=== ПРОВЕРКА ВСЕХ ГРУПП НА МНОЖЕСТВЕННОЕ ЧИСЛО ===")
    
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
        
        # Проверяем на множественное число
        print(f"\n=== ПРОВЕРКА НА МНОЖЕСТВЕННОЕ ЧИСЛО ===")
        
        # Словарь для проверки множественного числа
        plural_patterns = {
            'schools': 'school',
            'hospitals': 'hospital', 
            'universities': 'university',
            'kindergartens': 'kindergarden',
            'pharmacies': 'pharmacy',
            'polyclinics': 'polyclinic',
            'shopping_malls': 'shopmall',
            'resident_complexes': 'resident_complex'
        }
        
        # Проверяем группы от поставщика
        print("Группы от поставщика с множественным числом:")
        supplier_plural = []
        for group in supplier_groups.index:
            if group in plural_patterns:
                supplier_plural.append((group, plural_patterns[group]))
                print(f"  ❌ '{group}' → должно быть '{plural_patterns[group]}'")
        
        if not supplier_plural:
            print("  ✅ Все группы от поставщика в единственном числе")
        
        # Проверяем определенные группы
        print(f"\nОпределенные группы с множественным числом:")
        determined_plural = []
        for group in determined_groups.index:
            if group in plural_patterns:
                determined_plural.append((group, plural_patterns[group]))
                print(f"  ❌ '{group}' → должно быть '{plural_patterns[group]}'")
        
        if not determined_plural:
            print("  ✅ Все определенные группы в единственном числе")
        
        # Проверяем несоответствия между группами от поставщика и определенными
        print(f"\n=== НЕСООТВЕТСТВИЯ МЕЖДУ ГРУППАМИ ===")
        mismatches = []
        for i, row in unique_coords.iterrows():
            supplier_group = row.get('group_type', '')
            determined_group = row.get('detected_group_type', '')
            
            if supplier_group and determined_group and supplier_group != determined_group:
                # Проверяем, не является ли это нормализацией множественного числа
                if determined_group in plural_patterns and supplier_group == plural_patterns[determined_group]:
                    continue  # Это нормализация, не несоответствие
                
                mismatches.append({
                    'name': row.get('name', 'N/A'),
                    'supplier': supplier_group,
                    'determined': determined_group
                })
        
        if mismatches:
            print("Объекты с несоответствием групп:")
            for mismatch in mismatches:
                print(f"  - {mismatch['name']}")
                print(f"    Группа от поставщика: '{mismatch['supplier']}'")
                print(f"    Определенная группа: '{mismatch['determined']}'")
                print()
        else:
            print("✅ Несоответствий между группами не найдено")
        
        # Статистика
        print(f"\n=== СТАТИСТИКА ===")
        print(f"Всего уникальных объектов: {len(unique_coords)}")
        print(f"Групп от поставщика с множественным числом: {len(supplier_plural)}")
        print(f"Определенных групп с множественным числом: {len(determined_plural)}")
        print(f"Несоответствий между группами: {len(mismatches)}")
        
        if supplier_plural or determined_plural:
            print(f"\n⚠️  НАЙДЕНЫ ГРУППЫ С МНОЖЕСТВЕННЫМ ЧИСЛОМ!")
            print("Нужно применить нормализацию ко всем группам.")
        else:
            print(f"\n✅ ВСЕ ГРУППЫ В ЕДИНСТВЕННОМ ЧИСЛЕ!")
        
    except Exception as e:
        print(f"❌ Ошибка проверки: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_all_groups() 