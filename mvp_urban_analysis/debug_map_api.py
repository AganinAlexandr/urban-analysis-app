#!/usr/bin/env python3
"""
Отладка API карты
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.database_fixed import db_manager_fixed
import pandas as pd

def debug_map_api():
    """Отлаживает логику API карты"""
    print("=== ОТЛАДКА API КАРТЫ ===")
    
    try:
        # Получаем данные как API карты
        df = db_manager_fixed.export_to_dataframe(include_analysis=True)
        
        print(f"1. Размер данных: {df.shape}")
        print(f"2. Колонки: {list(df.columns)}")
        
        # Проверяем параметры как в API
        group_type = 'supplier'  # По умолчанию
        data_source = 'database'
        
        print(f"\n3. Параметры API:")
        print(f"   group_type: {group_type}")
        print(f"   data_source: {data_source}")
        
        # Логика определения поля группировки (как в API)
        if group_type == 'supplier':
            group_field = 'group_supplier' if data_source == 'sample' else 'group_type'
        else:
            group_field = 'group_determined' if data_source == 'sample' else 'detected_group_type'
        
        print(f"4. Выбранное поле группировки: {group_field}")
        
        # Проверяем, есть ли это поле в данных
        if group_field in df.columns:
            print(f"✅ Поле '{group_field}' найдено в данных")
            
            # Проверяем значения
            unique_values = df[group_field].dropna().unique()
            print(f"   Уникальные значения: {unique_values}")
            
            # Проверяем объекты с координатами
            coords_df = df[df['latitude'].notna() & df['longitude'].notna()]
            print(f"   Объектов с координатами: {len(coords_df)}")
            
            # Проверяем объекты с группами
            with_groups = coords_df[coords_df[group_field].notna()]
            print(f"   Объектов с группами: {len(with_groups)}")
            
            # Группируем по группам
            grouped = with_groups.groupby(group_field)
            print(f"   Групп найдено: {len(grouped)}")
            
            for group_name, group_data in grouped:
                print(f"     Группа '{group_name}': {len(group_data)} объектов")
                
        else:
            print(f"❌ Поле '{group_field}' НЕ найдено в данных")
            
            # Ищем альтернативные поля
            group_fields = [col for col in df.columns if 'group' in col.lower()]
            print(f"   Доступные поля групп: {group_fields}")
            
            # Пробуем использовать первое доступное
            if group_fields:
                alternative_field = group_fields[0]
                print(f"   Используем альтернативное поле: {alternative_field}")
                
                unique_values = df[alternative_field].dropna().unique()
                print(f"   Уникальные значения: {unique_values}")
        
        # Проверяем логику fallback как в API
        print(f"\n5. ПРОВЕРКА FALLBACK ЛОГИКИ:")
        
        # Если основное поле пустое, используем альтернативное
        if group_type == 'supplier' and ('group_type' not in df.columns or df['group_type'].isna().all()):
            if 'detected_group_type' in df.columns:
                group_field = 'detected_group_type'
                print(f"⚠️ Поле 'group_type' пустое, используем 'detected_group_type'")
        elif group_type != 'supplier' and ('detected_group_type' not in df.columns or df['detected_group_type'].isna().all()):
            if 'group_type' in df.columns:
                group_field = 'group_type'
                print(f"⚠️ Поле 'detected_group_type' пустое, используем 'group_type'")
        
        print(f"   Финальное поле группировки: {group_field}")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    debug_map_api() 