#!/usr/bin/env python3
"""
Проверка структуры таблицы reviews
"""
import sqlite3

def check_reviews_structure():
    """Проверяет структуру таблицы reviews"""
    print("=== ПРОВЕРКА СТРУКТУРЫ ТАБЛИЦЫ REVIEWS ===")
    
    try:
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        # Проверяем структуру таблицы reviews
        print("1. Структура таблицы reviews:")
        cursor.execute("PRAGMA table_info(reviews)")
        columns = cursor.fetchall()
        for col in columns:
            print(f"  {col[1]} ({col[2]})")
        
        # Проверяем данные в таблице reviews
        print("\n2. Примеры данных в reviews:")
        cursor.execute("SELECT * FROM reviews LIMIT 3")
        reviews = cursor.fetchall()
        
        for i, review in enumerate(reviews, 1):
            print(f"   Запись {i}: {review}")
        
        # Проверяем общее количество отзывов
        print("\n3. Общая статистика:")
        cursor.execute("SELECT COUNT(*) FROM reviews")
        total_reviews = cursor.fetchone()[0]
        print(f"   Всего отзывов: {total_reviews}")
        
        # Проверяем, есть ли колонка rating
        cursor.execute("PRAGMA table_info(reviews)")
        columns = [col[1] for col in cursor.fetchall()]
        
        if 'rating' in columns:
            print("\n4. Статистика по рейтингам:")
            cursor.execute("""
                SELECT rating, COUNT(*) as count
                FROM reviews
                WHERE rating IS NOT NULL
                GROUP BY rating
                ORDER BY rating
            """)
            ratings = cursor.fetchall()
            
            for rating, count in ratings:
                print(f"   Рейтинг {rating}: {count} отзывов")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_reviews_structure() 