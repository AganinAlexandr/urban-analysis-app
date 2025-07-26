import sqlite3
import os

def check_table_structure():
    db_path = "urban_analysis.db"
    
    print(f"🔍 Проверяем структуру таблиц в БД: {db_path}")
    
    conn = sqlite3.connect(db_path)
    
    # Проверяем структуру таблицы objects
    cursor = conn.execute("PRAGMA table_info(objects)")
    objects_columns = cursor.fetchall()
    
    print(f"\n📋 Структура таблицы objects:")
    for col in objects_columns:
        print(f"  {col[1]} ({col[2]}) - {col[3]}")
    
    # Проверяем структуру таблицы object_groups
    cursor = conn.execute("PRAGMA table_info(object_groups)")
    groups_columns = cursor.fetchall()
    
    print(f"\n📋 Структура таблицы object_groups:")
    for col in groups_columns:
        print(f"  {col[1]} ({col[2]}) - {col[3]}")
    
    # Проверяем структуру таблицы detected_groups
    cursor = conn.execute("PRAGMA table_info(detected_groups)")
    detected_columns = cursor.fetchall()
    
    print(f"\n📋 Структура таблицы detected_groups:")
    for col in detected_columns:
        print(f"  {col[1]} ({col[2]}) - {col[3]}")
    
    # Проверяем данные в таблицах
    cursor = conn.execute("SELECT * FROM objects LIMIT 3")
    objects_data = cursor.fetchall()
    
    print(f"\n📋 Данные в таблице objects (первые 3 записи):")
    for obj in objects_data:
        print(f"  {obj}")
    
    cursor = conn.execute("SELECT * FROM object_groups")
    groups_data = cursor.fetchall()
    
    print(f"\n📋 Данные в таблице object_groups:")
    for group in groups_data:
        print(f"  {group}")
    
    cursor = conn.execute("SELECT * FROM detected_groups")
    detected_data = cursor.fetchall()
    
    print(f"\n📋 Данные в таблице detected_groups:")
    for detected in detected_data:
        print(f"  {detected}")
    
    conn.close()

if __name__ == "__main__":
    check_table_structure() 