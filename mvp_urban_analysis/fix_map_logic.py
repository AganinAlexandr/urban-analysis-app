#!/usr/bin/env python3
"""
Исправление логики карты
"""
import sys
import os
import pandas as pd

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def test_fixed_map_logic():
    """Тестирует исправленную логику карты"""
    print("=== ТЕСТ ИСПРАВЛЕННОЙ ЛОГИКИ КАРТЫ ===")
    
    try:
        from app.core.database_fixed import db_manager_fixed
        
        # Получаем данные
        df = db_manager_fixed.export_to_dataframe(include_analysis=True)
        df_converted = df.copy()
        
        print(f"1. Исходные данные: {len(df)} записей")
        
        # Фильтруем координаты
        coords_df = df_converted[
            df_converted['latitude'].notna() & df_converted['longitude'].notna() &
            (df_converted['latitude'] != '') & (df_converted['longitude'] != '') &
            (df_converted['latitude'] != 0) & (df_converted['longitude'] != 0)
        ]
        print(f"2. С координатами: {len(coords_df)} записей")
        
        # Удаляем дубликаты по координатам
        coords_df = coords_df.drop_duplicates(subset=['latitude', 'longitude'], keep='first')
        print(f"3. После дедупликации: {len(coords_df)} уникальных объектов")
        
        # Проверяем группы
        print(f"\n4. Анализ групп:")
        print(f"  group_type уникальные: {coords_df['group_type'].unique()}")
        print(f"  detected_group_type уникальные: {coords_df['detected_group_type'].unique()}")
        
        # Тестируем логику для режима "от поставщика"
        print(f"\n5. Тест режима 'от поставщика':")
        group_field = 'group_type'
        has_group = coords_df[group_field].notna() & (coords_df[group_field] != '') & (coords_df[group_field] != 'None')
        with_group = coords_df[has_group]
        without_group = coords_df[~has_group]
        
        print(f"  С группой: {len(with_group)} объектов")
        print(f"  Без группы: {len(without_group)} объектов")
        
        # Показываем объекты с группами
        if len(with_group) > 0:
            print(f"  Объекты с группами:")
            for _, row in with_group.iterrows():
                print(f"    - {row.get('name')}: {row.get('group_type')}")
        
        # Тестируем логику для режима "определенные"
        print(f"\n6. Тест режима 'определенные':")
        group_field = 'detected_group_type'
        has_group = coords_df[group_field].notna() & (coords_df[group_field] != '') & (coords_df[group_field] != 'None')
        with_group = coords_df[has_group]
        without_group = coords_df[~has_group]
        
        print(f"  С группой: {len(with_group)} объектов")
        print(f"  Без группы: {len(without_group)} объектов")
        
        # Показываем объекты с группами
        if len(with_group) > 0:
            print(f"  Объекты с группами:")
            for _, row in with_group.iterrows():
                print(f"    - {row.get('name')}: {row.get('detected_group_type')}")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_fixed_map_logic() 