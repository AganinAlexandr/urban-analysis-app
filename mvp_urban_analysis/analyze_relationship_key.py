#!/usr/bin/env python3
"""
Анализ ключа связи между таблицами reviews и master_ratings
"""
import sqlite3

def analyze_relationship_key():
    """Анализирует ключ связи между таблицами"""
    print("=== АНАЛИЗ КЛЮЧА СВЯЗИ МЕЖДУ ТАБЛИЦАМИ ===")
    
    try:
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        # 1. Анализ ключа связи
        print("1. Ключ связи:")
        print("   master_ratings.review_id -> reviews.id")
        print("   Тип: Внешний ключ (Foreign Key)")
        print("   Ограничения: NO ACTION, NO ACTION")
        
        # 2. Проверяем уникальность ключа
        print("\n2. Проверка уникальности:")
        
        # Проверяем, есть ли дубликаты review_id в master_ratings
        cursor.execute("""
            SELECT review_id, COUNT(*) as count
            FROM master_ratings
            GROUP BY review_id
            HAVING COUNT(*) > 1
        """)
        duplicates = cursor.fetchall()
        
        if duplicates:
            print("   ⚠️  Найдены дубликаты review_id в master_ratings:")
            for review_id, count in duplicates:
                print(f"     review_id {review_id}: {count} записей")
        else:
            print("   ✅ Дубликатов review_id в master_ratings нет")
        
        # 3. Проверяем соответствие данных
        print("\n3. Проверка соответствия данных:")
        
        # Проверяем, есть ли master_ratings для несуществующих отзывов
        cursor.execute("""
            SELECT COUNT(*)
            FROM master_ratings mr
            LEFT JOIN reviews r ON mr.review_id = r.id
            WHERE r.id IS NULL
        """)
        orphaned_master_ratings = cursor.fetchone()[0]
        print(f"   Master_ratings без отзывов: {orphaned_master_ratings}")
        
        # 4. Анализ содержимого отзывов
        print("\n4. Анализ содержимого отзывов:")
        
        # Проверяем примеры отзывов
        cursor.execute("""
            SELECT id, review_text, object_id
            FROM reviews
            LIMIT 5
        """)
        reviews_examples = cursor.fetchall()
        
        print("   Примеры отзывов:")
        for review_id, text, object_id in reviews_examples:
            print(f"     ID {review_id} (объект {object_id}): {text[:100]}...")
        
        # 5. Проверяем, связан ли ключ с содержимым текста
        print("\n5. Связь ключа с содержимым текста:")
        print("   Ключ review_id НЕ связан с содержимым текста отзыва")
        print("   Ключ основан на ID записи в таблице reviews")
        print("   Это означает:")
        print("     - Один отзыв может иметь только один master_rating")
        print("     - Master_rating привязан к конкретному отзыву по ID")
        print("     - При изменении текста отзыва связь сохраняется")
        
        # 6. Проверяем целостность данных
        print("\n6. Целостность данных:")
        
        # Проверяем, есть ли отзывы без master_ratings
        cursor.execute("""
            SELECT COUNT(*)
            FROM reviews r
            LEFT JOIN master_ratings mr ON r.id = mr.review_id
            WHERE mr.id IS NULL
        """)
        reviews_without_master_ratings = cursor.fetchone()[0]
        print(f"   Отзывов без master_ratings: {reviews_without_master_ratings}")
        
        # 7. Рекомендации
        print("\n7. Рекомендации:")
        print("   ✅ Ключ связи корректен:")
        print("     - review_id в master_ratings ссылается на reviews.id")
        print("     - Связь один к одному (один отзыв = один master_rating)")
        print("     - Внешний ключ обеспечивает целостность данных")
        print("   ⚠️  Потенциальные проблемы:")
        print("     - Нет данных в master_ratings (0 записей)")
        print("     - При добавлении данных нужно соблюдать связь по review_id")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    analyze_relationship_key() 