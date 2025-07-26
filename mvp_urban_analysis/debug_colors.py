#!/usr/bin/env python3
"""
Отладка цветов в API карты
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.database_fixed import db_manager_fixed
from app.core.config import GROUP_CONFIG
import pandas as pd

def debug_colors():
    """Отлаживает цвета в API карты"""
    print("=== ОТЛАДКА ЦВЕТОВ ===")
    
    try:
        # Получаем данные
        df = db_manager_fixed.export_to_dataframe(include_analysis=True)
        
        print(f"1. Конфигурация цветов:")
        print(f"   Доступные группы: {list(GROUP_CONFIG['colors'].keys())}")
        
        # Проверяем объекты с координатами
        coords_df = df[df['latitude'].notna() & df['longitude'].notna()]
        
        print(f"\n2. Объекты с координатами:")
        for _, row in coords_df.head(3).iterrows():
            name = row['name']
            group_type = row.get('group_type', '')
            
            # Проверяем цвет
            color = GROUP_CONFIG['colors'].get(group_type, '#6c757d')
            
            print(f"   {name}")
            print(f"     group_type: '{group_type}'")
            print(f"     цвет: {color}")
            print(f"     найден в конфиге: {group_type in GROUP_CONFIG['colors']}")
        
        # Проверяем уникальные группы
        unique_groups = coords_df['group_type'].dropna().unique()
        print(f"\n3. Уникальные группы в данных:")
        for group in unique_groups:
            color = GROUP_CONFIG['colors'].get(group, '#6c757d')
            found = group in GROUP_CONFIG['colors']
            print(f"   '{group}' -> {color} (найден: {found})")
        
        # Проверяем функцию get_point_color
        print(f"\n4. Тест функции get_point_color:")
        
        # Имитируем вызов как в API
        for _, row in coords_df.head(2).iterrows():
            group = row.get('group_type', '')
            color = GROUP_CONFIG['colors'].get(group, '#6c757d')
            print(f"   group='{group}' -> color='{color}'")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    debug_colors() 