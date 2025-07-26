#!/usr/bin/env python3
"""
Исправление схемы БД - добавление колонки user_name
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.database_fixed import db_manager_fixed
import sqlite3

def fix_database_schema():
    """Исправляет схему БД"""
    print("=== ИСПРАВЛЕНИЕ СХЕМЫ БД ===")
    
    try:
        with db_manager_fixed.get_connection() as conn:
            cursor = conn.cursor()
            
            # Проверяем, есть ли колонка user_name в таблице reviews
            cursor.execute("PRAGMA table_info(reviews)")
            columns = cursor.fetchall()
            column_names = [col[1] for col in columns]
            
            print(f"Текущие колонки в таблице reviews: {column_names}")
            
            if 'user_name' not in column_names:
                print("Добавляем колонку user_name в таблицу reviews...")
                cursor.execute("ALTER TABLE reviews ADD COLUMN user_name TEXT")
                print("✅ Колонка user_name добавлена")
            else:
                print("✅ Колонка user_name уже существует")
            
            # Проверяем результат
            cursor.execute("PRAGMA table_info(reviews)")
            columns_after = cursor.fetchall()
            column_names_after = [col[1] for col in columns_after]
            print(f"Колонки после исправления: {column_names_after}")
            
            print("=== СХЕМА БД ИСПРАВЛЕНА ===")
            
    except Exception as e:
        print(f"❌ Ошибка исправления схемы: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    fix_database_schema() 