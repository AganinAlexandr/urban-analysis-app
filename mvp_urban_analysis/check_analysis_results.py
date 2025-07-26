#!/usr/bin/env python3
"""
Проверка методов в таблице analysis_results
"""
import sys
import os
import sqlite3

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def check_analysis_results():
    """Проверяет методы в таблице analysis_results"""
    print("=== ПРОВЕРКА МЕТОДОВ В ANALYSIS_RESULTS ===")
    
    try:
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        # Проверяем структуру таблицы analysis_results
        cursor.execute("PRAGMA table_info(analysis_results)")
        columns = cursor.fetchall()
        print(f"Структура таблицы analysis_results:")
        for col in columns:
            print(f"  - {col[1]} ({col[2]})")
        
        # Получаем все уникальные методы из analysis_results
        cursor.execute("""
            SELECT DISTINCT method_id 
            FROM analysis_results 
            ORDER BY method_id
        """)
        
        method_ids = cursor.fetchall()
        print(f"\nУникальных method_id: {len(method_ids)}")
        
        if method_ids:
            print("Method IDs:")
            for method_id in method_ids:
                print(f"  - {method_id[0]}")
        
        # Получаем информацию о методах с их названиями
        cursor.execute("""
            SELECT DISTINCT ar.method_id, pm.method_name, pm.description
            FROM analysis_results ar
            JOIN processing_methods pm ON ar.method_id = pm.id
            ORDER BY pm.method_name
        """)
        
        methods_with_names = cursor.fetchall()
        print(f"\nМетоды с названиями:")
        
        if methods_with_names:
            for method_id, method_name, description in methods_with_names:
                print(f"  - ID: {method_id}, Название: {method_name}")
                if description:
                    print(f"    Описание: {description}")
        else:
            print("❌ Методы не найдены!")
        
        # Подсчитываем количество записей для каждого метода
        cursor.execute("""
            SELECT pm.method_name, COUNT(*) as count
            FROM analysis_results ar
            JOIN processing_methods pm ON ar.method_id = pm.id
            GROUP BY pm.method_name
            ORDER BY count DESC
        """)
        
        method_counts = cursor.fetchall()
        print(f"\nКоличество записей по методам:")
        
        if method_counts:
            for method_name, count in method_counts:
                print(f"  - {method_name}: {count} записей")
        else:
            print("❌ Данные не найдены!")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_analysis_results() 