#!/usr/bin/env python3
"""
Отладочный скрипт для проверки логики определения группы
"""

import os
import sys
import json

# Добавляем путь к модулям приложения
sys.path.append('mvp_urban_analysis')

from app.core.json_processor import JSONProcessor

def debug_group_logic():
    """Отладка логики определения группы"""
    processor = JSONProcessor()
    
    # Тестируем конкретный файл
    test_file = 'parsed/yandex/shopmall_parse/1000339903.json'
    
    print(f"Отладка файла: {test_file}")
    print("=" * 80)
    
    # 1. Проверяем извлечение группы из пути
    group_from_path = processor._extract_group_from_path(test_file)
    print(f"1. Группа из пути: '{group_from_path}'")
    
    # 2. Читаем JSON файл
    with open(test_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # 3. Проверяем company_info
    company_info = data.get('company_info', {})
    print(f"2. Поля в company_info: {list(company_info.keys())}")
    
    # 4. Проверяем поле group
    group_in_data = company_info.get('group', '')
    print(f"3. Группа в данных: '{group_in_data}'")
    print(f"4. Тип группы в данных: {type(group_in_data)}")
    print(f"5. Пустая ли группа в данных: {group_in_data == ''}")
    print(f"6. None ли группа в данных: {group_in_data is None}")
    
    # 5. Проверяем логику выбора группы
    final_group = group_in_data or group_from_path
    print(f"7. Финальная группа: '{final_group}'")
    
    # 6. Проверяем обработку через процессор
    print("\n8. Обработка через процессор:")
    df = processor.process_json_file_or_directory(test_file)
    if not df.empty:
        print(f"   Обработано записей: {len(df)}")
        if 'group' in df.columns:
            unique_groups = df['group'].unique()
            print(f"   Группы в результате: {unique_groups}")
            
            # Проверяем первые несколько записей
            print("\n   Первые 3 записи:")
            for i, row in df.head(3).iterrows():
                print(f"   {i+1}. Группа: '{row.get('group', 'N/A')}'")
                print(f"      Название: '{row.get('name', 'N/A')}'")
                print(f"      Адрес: '{row.get('address', 'N/A')}'")
                print()
    else:
        print("   Файл не обработан")

if __name__ == "__main__":
    debug_group_logic() 