#!/usr/bin/env python3
"""
Проверка методов сентимента в базе данных
"""
import sys
import os
import sqlite3

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def check_sentiment_methods():
    """Проверить доступные методы сентимента в БД"""
    try:
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        # Получаем методы, которые реально используются в analysis_results
        cursor.execute("""
            SELECT DISTINCT pm.method_name, pm.is_active, COUNT(ar.id) as usage_count
            FROM processing_methods pm
            LEFT JOIN analysis_results ar ON pm.id = ar.method_id
            WHERE pm.is_active = 1
            GROUP BY pm.id, pm.method_name, pm.is_active
            ORDER BY pm.method_name
        """)
        
        methods = cursor.fetchall()
        
        print("=== ДОСТУПНЫЕ МЕТОДЫ СЕНТИМЕНТА ===")
        print(f"Всего методов: {len(methods)}")
        print()
        
        for method_name, is_active, usage_count in methods:
            status = "✅ АКТИВЕН" if is_active else "❌ НЕАКТИВЕН"
            print(f"📊 {method_name}")
            print(f"   Статус: {status}")
            print(f"   Использований: {usage_count}")
            print()
        
        # Проверяем общее количество записей в analysis_results
        cursor.execute("SELECT COUNT(*) FROM analysis_results")
        total_results = cursor.fetchone()[0]
        print(f"Всего записей в analysis_results: {total_results}")
        
        conn.close()
        
    except Exception as e:
        print(f"Ошибка: {e}")

if __name__ == "__main__":
    check_sentiment_methods() 