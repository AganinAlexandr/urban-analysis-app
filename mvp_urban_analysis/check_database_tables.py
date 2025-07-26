#!/usr/bin/env python3
"""
Проверка всех таблиц в базе данных
"""
import sys
import os
import sqlite3

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def check_database_tables():
    """Проверяет все таблицы в базе данных"""
    print("=== ПРОВЕРКА ВСЕХ ТАБЛИЦ В БД ===")
    
    try:
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        # Получаем список всех таблиц
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        print(f"Всего таблиц в БД: {len(tables)}")
        print("\nСписок таблиц:")
        for table in tables:
            print(f"  - {table[0]}")
        
        # Проверяем структуру каждой таблицы
        print("\n=== СТРУКТУРА ТАБЛИЦ ===")
        
        for table in tables:
            table_name = table[0]
            print(f"\n📋 Таблица: {table_name}")
            
            try:
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = cursor.fetchall()
                
                print(f"  Колонки:")
                for col in columns:
                    print(f"    - {col[1]} ({col[2]})")
                
                # Подсчитываем количество записей
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]
                print(f"  Записей: {count}")
                
                # Если есть записи, показываем примеры
                if count > 0 and count <= 5:
                    cursor.execute(f"SELECT * FROM {table_name} LIMIT 3")
                    rows = cursor.fetchall()
                    print(f"  Примеры записей:")
                    for i, row in enumerate(rows, 1):
                        print(f"    {i}. {row}")
                elif count > 5:
                    print(f"  (показаны первые 3 из {count} записей)")
                    
            except Exception as e:
                print(f"  ❌ Ошибка при проверке таблицы {table_name}: {e}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_database_tables() 