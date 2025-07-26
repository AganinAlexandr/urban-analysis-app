#!/usr/bin/env python3
"""
Проверка групп в сложной БД
"""

import sqlite3
import pandas as pd

def check_complex_groups():
    """Проверяет группы в сложной БД"""
    print("=== ПРОВЕРКА ГРУПП В СЛОЖНОЙ БД ===")
    
    try:
        with sqlite3.connect("urban_analysis_fixed.db") as conn:
            # Проверяем таблицу object_groups
            print("1. Проверяем таблицу object_groups:")
            groups_query = "SELECT * FROM object_groups"
            groups_df = pd.read_sql_query(groups_query, conn)
            print(f"   Записей: {len(groups_df)}")
            if not groups_df.empty:
                print(f"   Группы: {groups_df.to_dict('records')}")
            
            # Проверяем таблицу detected_groups
            print("\n2. Проверяем таблицу detected_groups:")
            detected_query = "SELECT * FROM detected_groups"
            detected_df = pd.read_sql_query(detected_query, conn)
            print(f"   Записей: {len(detected_df)}")
            if not detected_df.empty:
                print(f"   Группы: {detected_df.to_dict('records')}")
            
            # Проверяем объекты
            print("\n3. Проверяем объекты:")
            objects_query = """
            SELECT 
                o.id,
                o.name,
                o.group_id,
                o.detected_group_id,
                og.group_type as object_group_type,
                dg.group_type as detected_group_type
            FROM objects o
            LEFT JOIN object_groups og ON o.group_id = og.id
            LEFT JOIN detected_groups dg ON o.detected_group_id = dg.id
            """
            objects_df = pd.read_sql_query(objects_query, conn)
            print(f"   Объектов: {len(objects_df)}")
            
            for _, row in objects_df.iterrows():
                print(f"   {row['name']}:")
                print(f"     group_id: {row['group_id']}")
                print(f"     detected_group_id: {row['detected_group_id']}")
                print(f"     object_group_type: {row['object_group_type']}")
                print(f"     detected_group_type: {row['detected_group_type']}")
            
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    check_complex_groups() 