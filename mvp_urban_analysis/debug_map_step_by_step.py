#!/usr/bin/env python3
"""
Пошаговая диагностика проблемы с картой
"""

import sys
import os
import pandas as pd
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.database_fixed import DatabaseManager

def debug_map_step_by_step():
    """Пошаговая диагностика проблемы с картой"""
    print("=== ПОШАГОВАЯ ДИАГНОСТИКА КАРТЫ ===")
    
    try:
        # Инициализируем менеджер БД
        db_manager = DatabaseManager()
        
        # Шаг 1: Получаем сырые данные из БД
        print("\n1️⃣ ШАГ 1: Получаем сырые данные из БД")
        df_raw = db_manager.export_to_dataframe(include_analysis=True)
        print(f"   Всего строк в сырых данных: {len(df_raw)}")
        print(f"   Колонки: {list(df_raw.columns)}")
        
        # Шаг 2: Проверяем объекты с координатами
        print("\n2️⃣ ШАГ 2: Проверяем объекты с координатами")
        coords_df = df_raw[
            df_raw['latitude'].notna() & df_raw['longitude'].notna() &
            (df_raw['latitude'] != '') & (df_raw['longitude'] != '') &
            (df_raw['latitude'] != 0) & (df_raw['longitude'] != 0)
        ]
        print(f"   Объектов с координатами: {len(coords_df)}")
        
        # Шаг 3: Удаляем дубликаты по координатам
        print("\n3️⃣ ШАГ 3: Удаляем дубликаты по координатам")
        coords_no_duplicates = coords_df.drop_duplicates(subset=['latitude', 'longitude'], keep='first')
        print(f"   После удаления дубликатов: {len(coords_no_duplicates)} объектов")
        
        # Шаг 4: Проверяем группы
        print("\n4️⃣ ШАГ 4: Проверяем группы")
        if 'group_name' in coords_no_duplicates.columns:
            group_counts = coords_no_duplicates['group_name'].value_counts()
            print("   Распределение по group_name:")
            for group, count in group_counts.items():
                print(f"     {group}: {count} объектов")
        
        # Шаг 5: Проверяем, какие объекты попадают в каждую группу
        print("\n5️⃣ ШАГ 5: Детали по группам")
        if 'group_name' in coords_no_duplicates.columns:
            for group_name in coords_no_duplicates['group_name'].unique():
                if pd.isna(group_name) or group_name == '' or group_name == 'None':
                    continue
                group_objects = coords_no_duplicates[coords_no_duplicates['group_name'] == group_name]
                print(f"\n   Группа '{group_name}':")
                for _, obj in group_objects.iterrows():
                    print(f"     ID {obj.get('object_id', 'N/A')}: {obj.get('name', 'N/A')} - ({obj.get('latitude')}, {obj.get('longitude')})")
        
        # Шаг 6: Проверяем, что происходит при применении фильтров
        print("\n6️⃣ ШАГ 6: Тест применения фильтров")
        active_filters = ['school', 'hospital', 'pharmacy', 'kindergarden', 'polyclinic', 'university', 'shopmall', 'resident_complex']
        print(f"   Активные фильтры: {active_filters}")
        
        if 'group_name' in coords_no_duplicates.columns:
            available_groups = coords_no_duplicates['group_name'].unique()
            valid_filters = [f for f in active_filters if f in available_groups]
            print(f"   Доступные группы: {available_groups}")
            print(f"   Валидные фильтры: {valid_filters}")
            
            if valid_filters:
                filtered_objects = coords_no_duplicates[coords_no_duplicates['group_name'].isin(valid_filters)]
                print(f"   Объектов после фильтрации: {len(filtered_objects)}")
                
                for group in valid_filters:
                    count = len(filtered_objects[filtered_objects['group_name'] == group])
                    print(f"     {group}: {count} объектов")
            else:
                print("   ⚠️ Ни один из активных фильтров не найден в данных!")
        
        # Шаг 7: Проверяем, что происходит в get_map_data
        print("\n7️⃣ ШАГ 7: Симуляция get_map_data")
        if 'group_name' in coords_no_duplicates.columns:
            group_field = 'group_name'
            has_group = coords_no_duplicates[group_field].notna() & (coords_no_duplicates[group_field] != '') & (coords_no_duplicates[group_field] != 'None')
            with_group = coords_no_duplicates[has_group]
            without_group = coords_no_duplicates[~has_group]
            
            print(f"   Объектов с группами: {len(with_group)}")
            print(f"   Объектов без групп: {len(without_group)}")
            
            if len(with_group) > 0:
                print(f"   Группы в данных: {with_group[group_field].unique()}")
                
                # Симулируем создание archive_data
                archive_data = []
                for group, group_data in with_group.groupby(group_field):
                    points = []
                    for _, row in group_data.iterrows():
                        points.append({
                            'name': row.get('name', ''),
                            'latitude': float(row.get('latitude', 0)),
                            'longitude': float(row.get('longitude', 0)),
                            'group': row.get('group_name', '')
                        })
                    if points:
                        archive_data.append({'group': group, 'points': points})
                
                print(f"   Создано групп для карты: {len(archive_data)}")
                total_points = sum(len(group['points']) for group in archive_data)
                print(f"   Всего точек для карты: {total_points}")
                
                for group_info in archive_data:
                    print(f"     Группа {group_info['group']}: {len(group_info['points'])} точек")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_map_step_by_step()






