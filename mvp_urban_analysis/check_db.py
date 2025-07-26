#!/usr/bin/env python3
"""
Скрипт для проверки состояния базы данных
"""
import sqlite3
import pandas as pd

def check_database():
    """Проверяет состояние базы данных"""
    db_path = "urban_analysis_fixed.db"
    
    try:
        # Подключаемся к БД
        conn = sqlite3.connect(db_path)
        
        # Проверяем структуру таблицы reviews
        print("=== СТРУКТУРА ТАБЛИЦЫ REVIEWS ===")
        cursor = conn.execute("PRAGMA table_info(reviews)")
        columns = cursor.fetchall()
        for col in columns:
            print(f"Колонка: {col[1]}, Тип: {col[2]}, NULL: {col[3]}, Значение по умолчанию: {col[4]}")
        
        # Проверяем количество записей
        print("\n=== КОЛИЧЕСТВО ЗАПИСЕЙ ===")
        cursor = conn.execute("SELECT COUNT(*) as total FROM reviews")
        total = cursor.fetchone()[0]
        print(f"Всего записей в reviews: {total}")
        
        # Проверяем поле в_Выборке
        print("\n=== СОСТОЯНИЕ ПОЛЯ В_ВЫБОРКЕ ===")
        cursor = conn.execute("SELECT в_Выборке, COUNT(*) as count FROM reviews GROUP BY в_Выборке")
        sample_status = cursor.fetchall()
        for status, count in sample_status:
            print(f"в_Выборке = '{status}': {count} записей")
        
        # Проверяем несколько записей
        print("\n=== ПРИМЕРЫ ЗАПИСЕЙ ===")
        cursor = conn.execute("""
            SELECT id, object_id, review_text[:50] as text_preview, в_Выборке 
            FROM reviews 
            LIMIT 5
        """)
        records = cursor.fetchall()
        for record in records:
            print(f"ID: {record[0]}, Object ID: {record[1]}, Text: {record[2]}..., в_Выборке: '{record[3]}'")
        
        # Проверяем объекты
        print("\n=== ОБЪЕКТЫ ===")
        cursor = conn.execute("SELECT COUNT(*) as total FROM objects")
        objects_total = cursor.fetchone()[0]
        print(f"Всего объектов: {objects_total}")
        
        # Проверяем группы
        print("\n=== ГРУППЫ ===")
        cursor = conn.execute("SELECT group_type, COUNT(*) as count FROM object_groups GROUP BY group_type")
        groups = cursor.fetchall()
        for group_type, count in groups:
            print(f"Группа '{group_type}': {count} записей")
        
        conn.close()
        
    except Exception as e:
        print(f"Ошибка при проверке БД: {e}")

if __name__ == "__main__":
    check_database() 