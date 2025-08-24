#!/usr/bin/env python3
"""
Проверка заполненности поля detected_group_id
"""

import sqlite3
import pandas as pd

def check_detected_groups():
    """Проверяем заполненность поля detected_group_id"""
    print("=== ПРОВЕРКА ПОЛЯ DETECTED_GROUP_ID ===")
    
    # Подключаемся к БД
    conn = sqlite3.connect('urban_analysis_fixed.db')
    
    # Получаем все объекты с координатами
    query = """
    SELECT DISTINCT 
        o.id,
        o.name,
        o.address,
        o.latitude,
        o.longitude,
        o.group_id,
        og.group_name,
        og.group_type,
        o.detected_group_id,
        dg.group_name as detected_group_name,
        dg.group_type as detected_group_type
    FROM objects o
    LEFT JOIN object_groups og ON o.group_id = og.id
    LEFT JOIN detected_groups dg ON o.detected_group_id = dg.id
    WHERE o.latitude IS NOT NULL 
        AND o.longitude IS NOT NULL 
        AND o.latitude != '' 
        AND o.longitude != ''
        AND o.latitude != 0 
        AND o.longitude != 0
    ORDER BY o.id
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    print(f"Всего объектов с координатами: {len(df)}")
    
    # Проверяем заполненность detected_group_id
    print("\n=== АНАЛИЗ ПОЛЯ DETECTED_GROUP_ID ===")
    
    # Объекты с заполненным detected_group_id
    with_detected = df[df['detected_group_id'].notna() & (df['detected_group_id'] != '') & (df['detected_group_id'] != 'None') & (df['detected_group_id'] != 0)]
    print(f"Объектов с заполненным detected_group_id: {len(with_detected)}")
    
    # Объекты БЕЗ заполненного detected_group_id
    without_detected = df[df['detected_group_id'].isna() | (df['detected_group_id'] == '') | (df['detected_group_id'] == 'None') | (df['detected_group_id'] == 0)]
    print(f"Объектов БЕЗ заполненного detected_group_id: {len(without_detected)}")
    
    if len(without_detected) > 0:
        print("\n=== ОБЪЕКТЫ БЕЗ DETECTED_GROUP_ID ===")
        for _, row in without_detected.iterrows():
            print(f"ID {row['id']}: {row['name']}")
            print(f"  Адрес: {row['address']}")
            print(f"  Координаты: ({row['latitude']}, {row['longitude']})")
            print(f"  Группа от поставщика: {row['group_name']} ({row['group_type']})")
            print(f"  detected_group_id: {row['detected_group_id']}")
            print(f"  detected_group_name: {row['detected_group_name']}")
            print(f"  detected_group_type: {row['detected_group_type']}")
            print()
    
    # Проверяем распределение по группам
    print("=== РАСПРЕДЕЛЕНИЕ ПО ГРУППАМ ===")
    
    # По группам от поставщика
    print("\nПо группам от поставщика (group_name):")
    supplier_groups = df['group_name'].value_counts()
    for group, count in supplier_groups.items():
        print(f"  {group}: {count} объектов")
    
    # По определяемым группам
    print("\nПо определяемым группам (detected_group_type):")
    detected_groups = with_detected['detected_group_type'].value_counts()
    for group, count in detected_groups.items():
        print(f"  {group}: {count} объектов")
    
    # Проверяем, какие объекты будут видны в режиме "Определенные"
    print("\n=== СИМУЛЯЦИЯ РЕЖИМА 'ОПРЕДЕЛЕННЫЕ' ===")
    
    # Фильтруем только объекты с заполненным detected_group_id
    visible_in_determined = df[df['detected_group_id'].notna() & (df['detected_group_id'] != '') & (df['detected_group_id'] != 'None') & (df['detected_group_id'] != 0)]
    
    print(f"Объектов, видимых в режиме 'Определенные': {len(visible_in_determined)}")
    
    if len(visible_in_determined) > 0:
        print("\nГруппы в режиме 'Определенные':")
        for group, group_data in visible_in_determined.groupby('detected_group_type'):
            print(f"  {group}: {len(group_data)} объектов")
            for _, row in group_data.iterrows():
                print(f"    - ID {row['id']}: {row['name']}")
    
    # Проверяем, какие объекты будут видны в режиме "Группы от поставщика"
    print("\n=== СИМУЛЯЦИЯ РЕЖИМА 'ГРУППЫ ОТ ПОСТАВЩИКА' ===")
    
    # Фильтруем только объекты с заполненным group_id
    visible_in_supplier = df[df['group_id'].notna() & (df['group_id'] != '') & (df['group_id'] != 'None') & (df['group_id'] != 0)]
    
    print(f"Объектов, видимых в режиме 'Группы от поставщика': {len(visible_in_supplier)}")
    
    if len(visible_in_supplier) > 0:
        print("\nГруппы в режиме 'Группы от поставщика':")
        for group, group_data in visible_in_supplier.groupby('group_name'):
            print(f"  {group}: {len(group_data)} объектов")
            for _, row in group_data.iterrows():
                print(f"    - ID {row['id']}: {row['name']}")

if __name__ == "__main__":
    check_detected_groups()
