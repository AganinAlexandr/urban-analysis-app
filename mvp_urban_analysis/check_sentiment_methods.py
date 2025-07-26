#!/usr/bin/env python3
"""
Проверка методов сентимента в базе данных
"""
import sys
import os
import sqlite3

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def check_sentiment_methods():
    """Проверяет методы сентимента в базе данных"""
    print("=== ПРОВЕРКА МЕТОДОВ СЕНТИМЕНТА В БД ===")
    
    try:
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        # Проверяем структуру таблицы reviews
        cursor.execute("PRAGMA table_info(reviews)")
        columns = cursor.fetchall()
        print(f"Структура таблицы reviews:")
        for col in columns:
            print(f"  - {col[1]} ({col[2]})")
        
        # Проверяем, есть ли данные в таблице reviews с методами сентимента
        cursor.execute("""
            SELECT DISTINCT sentiment_method 
            FROM reviews 
            WHERE sentiment_method IS NOT NULL 
            AND sentiment_method != ''
            ORDER BY sentiment_method
        """)
        
        review_methods = cursor.fetchall()
        print(f"\nМетоды сентимента в отзывах: {len(review_methods)}")
        
        if review_methods:
            print("Доступные методы в отзывах:")
            for method in review_methods:
                print(f"  - {method[0]}")
        else:
            print("❌ Методы сентимента в отзывах не найдены!")
            
            # Проверяем общее количество отзывов
            cursor.execute("SELECT COUNT(*) FROM reviews")
            total_reviews = cursor.fetchone()[0]
            print(f"Всего отзывов в БД: {total_reviews}")
            
            # Проверяем отзывы с пустыми методами сентимента
            cursor.execute("SELECT COUNT(*) FROM reviews WHERE sentiment_method IS NULL OR sentiment_method = ''")
            empty_methods = cursor.fetchone()[0]
            print(f"Отзывов с пустыми методами сентимента: {empty_methods}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_sentiment_methods() 