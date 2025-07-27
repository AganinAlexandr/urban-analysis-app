#!/usr/bin/env python3
"""
Проверка структуры таблицы analysis_results
"""
import sys
import os
import sqlite3

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def check_analysis_results_structure():
    """Проверить структуру таблицы analysis_results"""
    try:
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        print("=== СТРУКТУРА ТАБЛИЦЫ analysis_results ===")
        print()
        
        # Проверяем структуру таблицы
        cursor.execute("PRAGMA table_info(analysis_results)")
        columns = cursor.fetchall()
        
        print("Колонки таблицы analysis_results:")
        for col in columns:
            print(f"  - {col[1]} ({col[2]})")
        print()
        
        # Проверяем количество записей
        cursor.execute("SELECT COUNT(*) FROM analysis_results")
        count = cursor.fetchone()[0]
        print(f"Записей в таблице: {count}")
        
        if count > 0:
            print("\nПримеры записей:")
            cursor.execute("SELECT * FROM analysis_results LIMIT 3")
            records = cursor.fetchall()
            for i, record in enumerate(records, 1):
                print(f"  Запись {i}: {record}")
        
        conn.close()
        
    except Exception as e:
        print(f"Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_analysis_results_structure() 