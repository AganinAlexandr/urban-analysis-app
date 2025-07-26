#!/usr/bin/env python3
"""
Проверка структуры таблицы objects
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.database_fixed import db_manager_fixed

def check_table_structure():
    """Проверяет структуру таблицы objects"""
    print("=== ПРОВЕРКА СТРУКТУРЫ ТАБЛИЦЫ OBJECTS ===")
    
    try:
        with db_manager_fixed.get_connection() as conn:
            cursor = conn.cursor()
            
            # Получаем информацию о таблице objects
            cursor.execute("PRAGMA table_info(objects)")
            columns = cursor.fetchall()
            
            print("Колонки таблицы objects:")
            for col in columns:
                print(f"  {col[1]} ({col[2]}) - {'NOT NULL' if col[3] else 'NULL'}")
            
            # Проверяем данные
            cursor.execute("SELECT * FROM objects LIMIT 3")
            rows = cursor.fetchall()
            
            print(f"\nПримеры данных:")
            for i, row in enumerate(rows, 1):
                print(f"Запись {i}: {row}")
            
            # Проверяем все таблицы
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            
            print(f"\nВсе таблицы в БД:")
            for table in tables:
                print(f"  {table[0]}")
                
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_table_structure() 