#!/usr/bin/env python3
"""
Детальный анализ методов сентимента в базе данных
"""
import sys
import os
import sqlite3

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def debug_sentiment_methods():
    """Детальный анализ методов сентимента"""
    try:
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        print("=== ДЕТАЛЬНЫЙ АНАЛИЗ МЕТОДОВ СЕНТИМЕНТА ===")
        print()
        
        # 1. Проверяем структуру таблицы processing_methods
        print("1. СТРУКТУРА ТАБЛИЦЫ processing_methods:")
        cursor.execute("PRAGMA table_info(processing_methods)")
        columns = cursor.fetchall()
        for col in columns:
            print(f"   - {col[1]} ({col[2]})")
        print()
        
        # 2. Все методы в processing_methods
        print("2. ВСЕ МЕТОДЫ В processing_methods:")
        cursor.execute("SELECT id, method_name, is_active, created_at FROM processing_methods ORDER BY method_name")
        all_methods = cursor.fetchall()
        for method_id, method_name, is_active, created_at in all_methods:
            status = "✅ АКТИВЕН" if is_active else "❌ НЕАКТИВЕН"
            print(f"   ID: {method_id}, Название: {method_name}, Статус: {status}, Создан: {created_at}")
        print()
        
        # 3. Методы, которые реально используются в analysis_results
        print("3. МЕТОДЫ, ИСПОЛЬЗУЕМЫЕ В analysis_results:")
        cursor.execute("""
            SELECT DISTINCT pm.method_name, COUNT(ar.id) as usage_count
            FROM processing_methods pm
            LEFT JOIN analysis_results ar ON pm.id = ar.method_id
            GROUP BY pm.id, pm.method_name
            HAVING usage_count > 0
            ORDER BY usage_count DESC
        """)
        used_methods = cursor.fetchall()
        if used_methods:
            for method_name, usage_count in used_methods:
                print(f"   - {method_name}: {usage_count} использований")
        else:
            print("   ❌ Нет методов с использованием в analysis_results")
        print()
        
        # 4. Общая статистика
        print("4. ОБЩАЯ СТАТИСТИКА:")
        cursor.execute("SELECT COUNT(*) FROM processing_methods WHERE is_active = 1")
        active_methods = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM analysis_results")
        total_results = cursor.fetchone()[0]
        print(f"   Активных методов: {active_methods}")
        print(f"   Записей в analysis_results: {total_results}")
        print()
        
        # 5. Проверяем, откуда взялись эти методы
        print("5. ИСТОРИЯ СОЗДАНИЯ МЕТОДОВ:")
        cursor.execute("""
            SELECT method_name, created_at, is_active
            FROM processing_methods 
            ORDER BY created_at
        """)
        history = cursor.fetchall()
        for method_name, created_at, is_active in history:
            status = "активен" if is_active else "неактивен"
            print(f"   {created_at}: {method_name} ({status})")
        
        conn.close()
        
    except Exception as e:
        print(f"Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_sentiment_methods() 