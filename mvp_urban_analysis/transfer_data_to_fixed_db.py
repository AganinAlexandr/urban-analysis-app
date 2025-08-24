#!/usr/bin/env python3
"""
Перенос данных из старой базы urban_analysis.db в новую urban_analysis_fixed.db
"""
import sqlite3
import os

def transfer_data():
    """Переносит данные из старой базы в новую"""
    print("=== ПЕРЕНОС ДАННЫХ В ИСПРАВЛЕННУЮ БАЗУ ===")
    
    old_db = 'urban_analysis.db'
    new_db = 'urban_analysis_fixed.db'
    
    if not os.path.exists(old_db):
        print(f"❌ Старая база данных {old_db} не найдена")
        return
    
    if not os.path.exists(new_db):
        print(f"❌ Новая база данных {new_db} не найдена")
        return
    
    try:
        # Подключаемся к старой базе
        old_conn = sqlite3.connect(old_db)
        old_cursor = old_conn.cursor()
        
        # Подключаемся к новой базе
        new_conn = sqlite3.connect(new_db)
        new_cursor = new_conn.cursor()
        
        print("1. Проверка структуры старой базы...")
        
        # Проверяем таблицы в старой базе
        old_cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        old_tables = [row[0] for row in old_cursor.fetchall()]
        print(f"   Таблицы в старой базе: {', '.join(old_tables)}")
        
        # Проверяем таблицы в новой базе
        new_cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        new_tables = [row[0] for row in new_cursor.fetchall()]
        print(f"   Таблицы в новой базе: {', '.join(new_tables)}")
        
        print("\n2. Перенос данных...")
        
        # Переносим объекты
        print("   Перенос объектов...")
        old_cursor.execute("SELECT COUNT(*) FROM objects")
        objects_count = old_cursor.fetchone()[0]
        print(f"     Найдено объектов: {objects_count}")
        
        if objects_count > 0:
            old_cursor.execute("SELECT * FROM objects")
            objects = old_cursor.fetchall()
            
            for obj in objects:
                try:
                    new_cursor.execute("""
                        INSERT OR IGNORE INTO objects 
                        (id, name, address, object_key, latitude, longitude, district, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, obj)
                except Exception as e:
                    print(f"     ⚠️  Ошибка при переносе объекта {obj[0]}: {e}")
            
            print(f"     ✅ Перенесено объектов: {objects_count}")
        
        # Переносим группы объектов
        print("   Перенос групп объектов...")
        old_cursor.execute("SELECT COUNT(*) FROM object_groups")
        groups_count = old_cursor.fetchone()[0]
        print(f"     Найдено групп: {groups_count}")
        
        if groups_count > 0:
            old_cursor.execute("SELECT * FROM object_groups")
            groups = old_cursor.fetchall()
            
            for group in groups:
                try:
                    new_cursor.execute("""
                        INSERT OR IGNORE INTO object_groups 
                        (id, group_name, group_type, description, created_at)
                        VALUES (?, ?, ?, ?, ?)
                    """, group)
                except Exception as e:
                    print(f"     ⚠️  Ошибка при переносе группы {group[0]}: {e}")
            
            print(f"     ✅ Перенесено групп: {groups_count}")
        
        # Переносим отзывы
        print("   Перенос отзывов...")
        old_cursor.execute("SELECT COUNT(*) FROM reviews")
        reviews_count = old_cursor.fetchone()[0]
        print(f"     Найдено отзывов: {reviews_count}")
        
        if reviews_count > 0:
            old_cursor.execute("SELECT * FROM reviews")
            reviews = old_cursor.fetchall()
            
            for review in reviews:
                try:
                    new_cursor.execute("""
                        INSERT OR IGNORE INTO reviews 
                        (id, object_id, review_text, rating, review_date, user_name, source, external_id, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, review)
                except Exception as e:
                    print(f"     ⚠️  Ошибка при переносе отзыва {review[0]}: {e}")
            
            print(f"     ✅ Перенесено отзывов: {reviews_count}")
        
        # Переносим результаты анализа (только для существующих методов)
        print("   Перенос результатов анализа...")
        old_cursor.execute("SELECT COUNT(*) FROM analysis_results")
        analysis_count = old_cursor.fetchone()[0]
        print(f"     Найдено результатов анализа: {analysis_count}")
        
        if analysis_count > 0:
            # Получаем список методов в новой базе
            new_cursor.execute("SELECT id, method_name FROM processing_methods")
            new_methods = {row[1]: row[0] for row in new_cursor.fetchall()}
            
            old_cursor.execute("""
                SELECT ar.*, pm.method_name 
                FROM analysis_results ar
                JOIN processing_methods pm ON ar.method_id = pm.id
            """)
            
            transferred_count = 0
            for analysis in old_cursor.fetchall():
                method_name = analysis[-1]  # method_name из JOIN
                
                if method_name in new_methods:
                    try:
                        new_cursor.execute("""
                            INSERT OR IGNORE INTO analysis_results 
                            (id, review_id, method_id, sentiment, confidence, review_type, keywords, topics, processed_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, analysis[:-1])  # Исключаем method_name
                        transferred_count += 1
                    except Exception as e:
                        print(f"     ⚠️  Ошибка при переносе результата {analysis[0]}: {e}")
                else:
                    print(f"     ⚠️  Пропущен результат для метода {method_name} (не найден в новой базе)")
            
            print(f"     ✅ Перенесено результатов анализа: {transferred_count}")
        
        # Переносим мастер-рейтинги
        print("   Перенос мастер-рейтингов...")
        old_cursor.execute("SELECT COUNT(*) FROM master_ratings")
        ratings_count = old_cursor.fetchone()[0]
        print(f"     Найдено мастер-рейтингов: {ratings_count}")
        
        if ratings_count > 0:
            old_cursor.execute("SELECT * FROM master_ratings")
            ratings = old_cursor.fetchall()
            
            for rating in ratings:
                try:
                    new_cursor.execute("""
                        INSERT OR IGNORE INTO master_ratings 
                        (id, review_id, sentiment, rated_by, rated_at)
                        VALUES (?, ?, ?, ?, ?)
                    """, rating)
                except Exception as e:
                    print(f"     ⚠️  Ошибка при переносе рейтинга {rating[0]}: {e}")
            
            print(f"     ✅ Перенесено мастер-рейтингов: {ratings_count}")
        
        # Подтверждаем изменения
        new_conn.commit()
        
        print("\n3. Проверка результата...")
        
        # Проверяем количество записей в новой базе
        new_cursor.execute("SELECT COUNT(*) FROM objects")
        new_objects = new_cursor.fetchone()[0]
        
        new_cursor.execute("SELECT COUNT(*) FROM reviews")
        new_reviews = new_cursor.fetchone()[0]
        
        new_cursor.execute("SELECT COUNT(*) FROM analysis_results")
        new_analysis = new_cursor.fetchone()[0]
        
        print(f"   Объектов в новой базе: {new_objects}")
        print(f"   Отзывов в новой базе: {new_reviews}")
        print(f"   Результатов анализа в новой базе: {new_analysis}")
        
        old_conn.close()
        new_conn.close()
        
        print("\n✅ Перенос данных завершен!")
        
    except Exception as e:
        print(f"❌ Ошибка при переносе данных: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    transfer_data()

