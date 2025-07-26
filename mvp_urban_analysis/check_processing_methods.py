#!/usr/bin/env python3
"""
Проверка методов обработки в базе данных
"""
import sys
import os
import sqlite3

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def check_processing_methods():
    """Проверяет методы обработки в базе данных"""
    print("=== ПРОВЕРКА МЕТОДОВ ОБРАБОТКИ В БД ===")
    
    try:
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        # Проверяем структуру таблицы processing_methods
        cursor.execute("PRAGMA table_info(processing_methods)")
        columns = cursor.fetchall()
        print(f"Структура таблицы processing_methods:")
        for col in columns:
            print(f"  - {col[1]} ({col[2]})")
        
        # Получаем все методы обработки
        cursor.execute("SELECT * FROM processing_methods ORDER BY method_name")
        methods = cursor.fetchall()
        
        print(f"\nВсего методов в БД: {len(methods)}")
        
        if methods:
            print(f"\nДоступные методы:")
            for method in methods:
                method_id, method_name, description, is_active, created_at = method
                status = "✅ Активен" if is_active else "❌ Неактивен"
                print(f"  - {method_name} ({status})")
                if description:
                    print(f"    Описание: {description}")
        else:
            print("❌ Методы не найдены в БД!")
        
        # Проверяем активные методы
        cursor.execute("""
            SELECT method_name 
            FROM processing_methods 
            WHERE is_active = 1
            ORDER BY method_name
        """)
        
        active_methods = cursor.fetchall()
        print(f"\nАктивных методов: {len(active_methods)}")
        
        if active_methods:
            print("Активные методы:")
            for method in active_methods:
                print(f"  - {method[0]}")
        else:
            print("❌ Активных методов не найдено!")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_processing_methods() 