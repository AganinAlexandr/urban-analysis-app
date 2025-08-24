#!/usr/bin/env python3
"""
Анализ связей между таблицами reviews и master_ratings
"""
import sqlite3

def check_table_relationships():
    """Анализирует связи между таблицами reviews и master_ratings"""
    print("=== АНАЛИЗ СВЯЗЕЙ МЕЖДУ ТАБЛИЦАМИ ===")
    
    try:
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        # 1. Структура таблицы reviews
        print("1. Структура таблицы reviews:")
        cursor.execute("PRAGMA table_info(reviews)")
        reviews_columns = cursor.fetchall()
        for col in reviews_columns:
            print(f"   {col[1]} ({col[2]})")
        
        # 2. Структура таблицы master_ratings
        print("\n2. Структура таблицы master_ratings:")
        cursor.execute("PRAGMA table_info(master_ratings)")
        master_ratings_columns = cursor.fetchall()
        for col in master_ratings_columns:
            print(f"   {col[1]} ({col[2]})")
        
        # 3. Анализ связей
        print("\n3. Анализ связей:")
        
        # Проверяем, есть ли внешний ключ от master_ratings к reviews
        cursor.execute("PRAGMA foreign_key_list(master_ratings)")
        foreign_keys = cursor.fetchall()
        
        if foreign_keys:
            print("   Внешние ключи для master_ratings:")
            for fk in foreign_keys:
                print(f"     {fk}")
        else:
            print("   Внешние ключи для master_ratings не найдены")
        
        # 4. Проверяем данные
        print("\n4. Данные в таблицах:")
        
        # Отзывы
        cursor.execute("SELECT COUNT(*) FROM reviews")
        reviews_count = cursor.fetchone()[0]
        print(f"   Отзывов: {reviews_count}")
        
        # Master_ratings
        cursor.execute("SELECT COUNT(*) FROM master_ratings")
        master_ratings_count = cursor.fetchone()[0]
        print(f"   Master_ratings: {master_ratings_count}")
        
        # 5. Проверяем связи данных
        print("\n5. Проверка связей данных:")
        
        if master_ratings_count > 0:
            # Проверяем, есть ли master_ratings без соответствующих отзывов
            cursor.execute("""
                SELECT COUNT(*)
                FROM master_ratings mr
                LEFT JOIN reviews r ON mr.review_id = r.id
                WHERE r.id IS NULL
            """)
            orphaned_master_ratings = cursor.fetchone()[0]
            print(f"   Master_ratings без отзывов: {orphaned_master_ratings}")
            
            # Проверяем, есть ли отзывы без master_ratings
            cursor.execute("""
                SELECT COUNT(*)
                FROM reviews r
                LEFT JOIN master_ratings mr ON r.id = mr.review_id
                WHERE mr.id IS NULL
            """)
            reviews_without_master_ratings = cursor.fetchone()[0]
            print(f"   Отзывов без master_ratings: {reviews_without_master_ratings}")
            
            # Примеры связанных данных
            cursor.execute("""
                SELECT r.id, r.review_text[:50], mr.sentiment, mr.rated_by
                FROM reviews r
                JOIN master_ratings mr ON r.id = mr.review_id
                LIMIT 3
            """)
            examples = cursor.fetchall()
            
            print("   Примеры связанных данных:")
            for review_id, text, sentiment, rated_by in examples:
                print(f"     Отзыв {review_id}: {text}... -> Master: {sentiment} (by {rated_by})")
        else:
            print("   Данных в master_ratings нет")
        
        # 6. Рекомендации по структуре
        print("\n6. Рекомендации по структуре:")
        print("   Связь: master_ratings.review_id -> reviews.id")
        print("   Тип связи: Один к одному (один отзыв может иметь один master_rating)")
        print("   Ограничения: review_id в master_ratings должен существовать в reviews")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_table_relationships() 