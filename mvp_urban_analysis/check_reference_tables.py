#!/usr/bin/env python3
"""
Проверка справочных таблиц групп
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.database_fixed import db_manager_fixed

def check_reference_tables():
    """Проверяет содержимое справочных таблиц групп"""
    print("=== ПРОВЕРКА СПРАВОЧНЫХ ТАБЛИЦ ГРУПП ===")
    
    try:
        with db_manager_fixed.get_connection() as conn:
            cursor = conn.cursor()
            
            # Проверяем таблицу object_groups
            print("=== ТАБЛИЦА OBJECT_GROUPS ===")
            cursor.execute("SELECT * FROM object_groups ORDER BY group_type")
            object_groups = cursor.fetchall()
            
            print("Содержимое object_groups:")
            for group in object_groups:
                print(f"  ID: {group[0]}, Type: '{group[1]}', Name: '{group[2]}'")
            
            # Проверяем таблицу detected_groups
            print(f"\n=== ТАБЛИЦА DETECTED_GROUPS ===")
            cursor.execute("SELECT * FROM detected_groups ORDER BY group_type")
            detected_groups = cursor.fetchall()
            
            print("Содержимое detected_groups:")
            for group in detected_groups:
                print(f"  ID: {group[0]}, Type: '{group[1]}', Name: '{group[2]}'")
            
            # Проверяем структуру таблиц
            print(f"\n=== СТРУКТУРА OBJECT_GROUPS ===")
            cursor.execute("PRAGMA table_info(object_groups)")
            columns = cursor.fetchall()
            for col in columns:
                print(f"  {col[1]} ({col[2]}) - {'NOT NULL' if col[3] else 'NULL'} - {'UNIQUE' if col[5] else 'NOT UNIQUE'}")
            
            print(f"\n=== СТРУКТУРА DETECTED_GROUPS ===")
            cursor.execute("PRAGMA table_info(detected_groups)")
            columns = cursor.fetchall()
            for col in columns:
                print(f"  {col[1]} ({col[2]}) - {'NOT NULL' if col[3] else 'NULL'} - {'UNIQUE' if col[5] else 'NOT UNIQUE'}")
                
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_reference_tables() 