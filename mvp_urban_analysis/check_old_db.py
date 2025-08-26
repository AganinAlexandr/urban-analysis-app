#!/usr/bin/env python3
"""
Проверка старой базы данных urban_analysis.db
"""

import sqlite3
import os

def check_old_database():
    """Проверяет старую базу данных urban_analysis.db"""
    
    db_path = 'urban_analysis.db'
    
    if not os.path.exists(db_path):
        print(f"❌ База данных '{db_path}' не найдена")
        return
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        print(f"=== ПРОВЕРКА БАЗЫ ДАННЫХ: {db_path} ===")
        
        # Получаем список всех таблиц
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        print(f"📋 Все таблицы: {', '.join(tables)}")
        
        # Ищем таблицы с ключевыми словами
        keyword_tables = [table for table in tables if 'keyword' in table.lower()]
        if keyword_tables:
            print(f"\n🎯 Найдены таблицы с ключевыми словами: {', '.join(keyword_tables)}")
            
            for table in keyword_tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"  📊 Таблица '{table}': {count} записей")
                
                if count > 0:
                    cursor.execute(f"PRAGMA table_info({table})")
                    columns = cursor.fetchall()
                    print(f"    Структура: {[col[1] for col in columns]}")
                    
                    cursor.execute(f"SELECT * FROM {table} LIMIT 3")
                    examples = cursor.fetchall()
                    print(f"    Примеры:")
                    for i, example in enumerate(examples):
                        print(f"      {i+1}. {example}")
        else:
            print("\n❌ Таблицы с ключевыми словами не найдены")
        
        # Проверяем, есть ли данные о группах
        if 'objects' in tables:
            cursor.execute("SELECT COUNT(*) FROM objects")
            objects_count = cursor.fetchone()[0]
            print(f"\n📊 Объектов в таблице objects: {objects_count}")
            
            if objects_count > 0:
                cursor.execute("SELECT * FROM objects LIMIT 3")
                examples = cursor.fetchall()
                print("  Примеры объектов:")
                for i, example in enumerate(examples):
                    print(f"    {i+1}. {example}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_old_database()




