#!/usr/bin/env python3
"""
Финальная диагностика проблемы с группами
"""

import sqlite3
import pandas as pd

def check_database_groups():
    """Проверяет состояние групп в базе данных"""
    print("=== ДИАГНОСТИКА ГРУПП В БД ===")
    
    db_path = 'urban_analysis_fixed.db'
    
    try:
        conn = sqlite3.connect(db_path)
        
        # 1. Проверяем структуру таблицы objects
        print("\n1. СТРУКТУРА ТАБЛИЦЫ OBJECTS:")
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(objects)")
        columns = cursor.fetchall()
        for col in columns:
            print(f"  {col[1]} ({col[2]})")
        
        # 2. Проверяем таблицы групп
        print("\n2. ТАБЛИЦЫ ГРУПП:")
        
        # object_groups
        cursor.execute("SELECT * FROM object_groups")
        object_groups = cursor.fetchall()
        print(f"  object_groups: {len(object_groups)} записей")
        for group in object_groups:
            print(f"    ID: {group[0]}, Тип: {group[1]}, Название: {group[2]}")
        
        # detected_groups  
        cursor.execute("SELECT * FROM detected_groups")
        detected_groups = cursor.fetchall()
        print(f"  detected_groups: {len(detected_groups)} записей")
        for group in detected_groups:
            print(f"    ID: {group[0]}, Тип: {group[1]}, Название: {group[2]}")
        
        # 3. Проверяем объекты (используем правильные колонки)
        print("\n3. ОБЪЕКТЫ:")
        cursor.execute("""
            SELECT id, name, group_id, detected_group_id
            FROM objects 
            LIMIT 10
        """)
        objects = cursor.fetchall()
        print(f"  Всего объектов: {len(objects)} (показано 10)")
        
        for obj in objects:
            obj_id, name, group_id, detected_group_id = obj
            print(f"    ID: {obj_id}")
            print(f"      Название: {name}")
            print(f"      group_id: {group_id}")
            print(f"      detected_group_id: {detected_group_id}")
        
        # 4. Проверяем связь объектов с группами
        print("\n4. СВЯЗЬ ОБЪЕКТОВ С ГРУППАМИ:")
        cursor.execute("""
            SELECT o.id, o.name, o.group_id, o.detected_group_id,
                   og.group_type as og_type, og.group_name as og_name,
                   dg.group_type as dg_type, dg.group_name as dg_name
            FROM objects o
            LEFT JOIN object_groups og ON o.group_id = og.id
            LEFT JOIN detected_groups dg ON o.detected_group_id = dg.id
            LIMIT 5
        """)
        linked_objects = cursor.fetchall()
        
        for obj in linked_objects:
            obj_id, name, group_id, detected_group_id, og_type, og_name, dg_type, dg_name = obj
            print(f"    Объект {obj_id}: {name}")
            print(f"      group_id: {group_id} -> связан с: {og_type} ({og_name})")
            print(f"      detected_group_id: {detected_group_id} -> связан с: {dg_type} ({dg_name})")
        
        # 5. Проверяем объекты с координатами
        print("\n5. ОБЪЕКТЫ С КООРДИНАТАМИ:")
        cursor.execute("""
            SELECT id, name, latitude, longitude, group_id, detected_group_id
            FROM objects 
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
            LIMIT 5
        """)
        coords_objects = cursor.fetchall()
        
        for obj in coords_objects:
            obj_id, name, lat, lon, group_id, detected_group_id = obj
            print(f"    {name}: ({lat}, {lon})")
            print(f"      group_id: {group_id}")
            print(f"      detected_group_id: {detected_group_id}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    check_database_groups() 