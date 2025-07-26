#!/usr/bin/env python3
"""
Диагностика проблем с режимом "Определенные группы"
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.database_fixed import db_manager_fixed
import pandas as pd

def debug_determined_groups():
    """Диагностирует проблемы с определенными группами"""
    print("=== ДИАГНОСТИКА РЕЖИМА 'ОПРЕДЕЛЕННЫЕ ГРУППЫ' ===")
    
    try:
        # Получаем данные из БД
        df = db_manager_fixed.export_to_dataframe(include_analysis=True)
        print(f"Всего записей в БД: {len(df)}")
        
        # Проверяем колонки
        print(f"\nКолонки в данных:")
        for col in df.columns:
            if 'group' in col.lower():
                print(f"  {col}")
        
        # Анализируем группы от поставщика
        print(f"\n=== ГРУППЫ ОТ ПОСТАВЩИКА ===")
        if 'group_type' in df.columns:
            supplier_groups = df['group_type'].value_counts()
            print(f"Уникальные значения group_type:")
            for group, count in supplier_groups.items():
                print(f"  '{group}': {count}")
        else:
            print("❌ Колонка group_type отсутствует")
        
        # Анализируем определенные группы
        print(f"\n=== ОПРЕДЕЛЕННЫЕ ГРУППЫ ===")
        if 'detected_group_type' in df.columns:
            determined_groups = df['detected_group_type'].value_counts()
            print(f"Уникальные значения detected_group_type:")
            for group, count in determined_groups.items():
                print(f"  '{group}': {count}")
        else:
            print("❌ Колонка detected_group_type отсутствует")
        
        # Проверяем объекты с координатами
        print(f"\n=== ОБЪЕКТЫ С КООРДИНАТАМИ ===")
        coords_df = df[df['latitude'].notna() & df['longitude'].notna()]
        print(f"Объектов с координатами: {len(coords_df)}")
        
        if len(coords_df) > 0:
            print(f"\nПримеры объектов:")
            for i, row in coords_df.head(5).iterrows():
                print(f"\n  {i+1}. {row.get('name', 'N/A')}")
                print(f"     Адрес: {row.get('address', 'N/A')}")
                print(f"     Координаты: ({row.get('latitude')}, {row.get('longitude')})")
                print(f"     Группа от поставщика: '{row.get('group_type', 'N/A')}'")
                print(f"     Определенная группа: '{row.get('detected_group_type', 'N/A')}'")
        
        # Анализируем проблемные случаи
        print(f"\n=== АНАЛИЗ ПРОБЛЕМНЫХ СЛУЧАЕВ ===")
        
        # 1. Объекты, которые исчезают в режиме "Определенные"
        if 'detected_group_type' in df.columns:
            missing_in_determined = coords_df[
                coords_df['detected_group_type'].isna() | 
                (coords_df['detected_group_type'] == '') | 
                (coords_df['detected_group_type'] == 'None')
            ]
            print(f"Объектов без определенной группы: {len(missing_in_determined)}")
            if len(missing_in_determined) > 0:
                print("Примеры объектов без определенной группы:")
                for i, row in missing_in_determined.head(3).iterrows():
                    print(f"  - {row.get('name', 'N/A')} (группа от поставщика: '{row.get('group_type', 'N/A')}')")
        
        # 2. Объекты с неправильными цветами
        print(f"\n=== АНАЛИЗ ЦВЕТОВ ===")
        if 'detected_group_type' in df.columns:
            # Проверяем объекты, которые должны быть школами, но отображаются как торговые центры
            schools_as_malls = coords_df[
                (coords_df['group_type'] == 'schools') & 
                (coords_df['detected_group_type'] == 'shopmall')
            ]
            print(f"Школ, определенных как торговые центры: {len(schools_as_malls)}")
            if len(schools_as_malls) > 0:
                print("Примеры:")
                for i, row in schools_as_malls.head(3).iterrows():
                    print(f"  - {row.get('name', 'N/A')}")
                    print(f"    Группа от поставщика: '{row.get('group_type', 'N/A')}'")
                    print(f"    Определенная группа: '{row.get('detected_group_type', 'N/A')}'")
        
        # 3. Проверяем логику API карты
        print(f"\n=== ТЕСТ ЛОГИКИ API КАРТЫ ===")
        
        # Симулируем запрос для режима "Определенные"
        group_type = 'determined'
        group_field = 'detected_group_type'
        
        print(f"Поле группировки для режима '{group_type}': {group_field}")
        
        if group_field in coords_df.columns:
            # Фильтруем объекты с определенной группой
            has_determined_group = coords_df[
                coords_df[group_field].notna() & 
                (coords_df[group_field] != '') & 
                (coords_df[group_field] != 'None')
            ]
            print(f"Объектов с определенной группой: {len(has_determined_group)}")
            
            # Группируем по определенным группам
            determined_groups_count = has_determined_group[group_field].value_counts()
            print(f"Распределение по определенным группам:")
            for group, count in determined_groups_count.items():
                print(f"  '{group}': {count}")
        
        print(f"\n=== ДИАГНОСТИКА ЗАВЕРШЕНА ===")
        
    except Exception as e:
        print(f"❌ Ошибка диагностики: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_determined_groups() 