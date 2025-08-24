#!/usr/bin/env python3
"""
Проверка данных Master_Rating
"""
import sqlite3

def check_master_rating_data():
    """Проверяет данные Master_Rating"""
    print("=== ПРОВЕРКА ДАННЫХ MASTER_RATING ===")
    
    try:
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        # 1. Проверяем отзывы с Master_Rating
        print("1. Отзывы с Master_Rating:")
        cursor.execute("""
            SELECT COUNT(*) FROM reviews 
            WHERE master_rating IS NOT NULL AND master_rating > 0
        """)
        master_rating_count = cursor.fetchone()[0]
        print(f"   Отзывов с Master_Rating: {master_rating_count}")
        
        if master_rating_count > 0:
            cursor.execute("""
                SELECT r.id, r.review_text, r.master_rating, o.name as object_name
                FROM reviews r
                LEFT JOIN objects o ON r.object_id = o.id
                WHERE r.master_rating IS NOT NULL AND r.master_rating > 0
                LIMIT 5
            """)
            master_rating_reviews = cursor.fetchall()
            
            print("   Примеры отзывов с Master_Rating:")
            for review_id, text, rating, object_name in master_rating_reviews:
                print(f"     ID {review_id}: {object_name} - рейтинг {rating}")
                print(f"       Текст: {text[:50]}...")
        
        # 2. Проверяем распределение Master_Rating
        print("\n2. Распределение Master_Rating:")
        cursor.execute("""
            SELECT master_rating, COUNT(*) as count
            FROM reviews
            WHERE master_rating IS NOT NULL AND master_rating > 0
            GROUP BY master_rating
            ORDER BY master_rating
        """)
        rating_distribution = cursor.fetchall()
        
        for rating, count in rating_distribution:
            print(f"   Рейтинг {rating}: {count} отзывов")
        
        # 3. Проверяем объекты с Master_Rating
        print("\n3. Объекты с Master_Rating:")
        cursor.execute("""
            SELECT o.id, o.name, COUNT(r.id) as reviews_count,
                   AVG(r.master_rating) as avg_rating
            FROM objects o
            JOIN reviews r ON o.id = r.object_id
            WHERE r.master_rating IS NOT NULL AND r.master_rating > 0
            GROUP BY o.id, o.name
            ORDER BY avg_rating DESC
        """)
        objects_with_master_rating = cursor.fetchall()
        
        print("   Объекты с Master_Rating:")
        for obj_id, name, reviews_count, avg_rating in objects_with_master_rating:
            print(f"     ID {obj_id}: {name} - {reviews_count} отзывов, средний рейтинг {avg_rating:.2f}")
        
        # 4. Проверяем общую статистику
        print("\n4. Общая статистика:")
        cursor.execute("SELECT COUNT(*) FROM reviews")
        total_reviews = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM reviews WHERE master_rating IS NOT NULL")
        reviews_with_master_rating = cursor.fetchone()[0]
        
        print(f"   Всего отзывов: {total_reviews}")
        print(f"   Отзывов с Master_Rating: {reviews_with_master_rating}")
        print(f"   Процент отзывов с Master_Rating: {(reviews_with_master_rating/total_reviews*100):.1f}%")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_master_rating_data() 