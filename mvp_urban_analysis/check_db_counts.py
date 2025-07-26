import sqlite3
import os

def check_db_counts():
    db_path = "urban_analysis.db"
    
    print(f"📊 Проверяем количество записей в БД: {db_path}")
    
    conn = sqlite3.connect(db_path)
    
    # Проверяем количество записей в каждой таблице
    tables = ['objects', 'reviews', 'analysis_results']
    
    for table in tables:
        cursor = conn.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"📋 Таблица {table}: {count} записей")
    
    # Проверяем уникальные объекты
    cursor = conn.execute("""
        SELECT o.id, o.name, o.address, COUNT(r.id) as reviews_count
        FROM objects o
        LEFT JOIN reviews r ON o.id = r.object_id
        GROUP BY o.id
        ORDER BY o.id
    """)
    objects = cursor.fetchall()
    
    print(f"\n🏢 Объекты и их отзывы:")
    for obj in objects:
        obj_id, name, address, reviews_count = obj
        print(f"  ID: {obj_id}, Имя: {name}, Адрес: {address}, Отзывов: {reviews_count}")
    
    # Проверяем дубликаты отзывов
    cursor = conn.execute("""
        SELECT object_id, review_text, COUNT(*) as count
        FROM reviews
        GROUP BY object_id, review_text
        HAVING COUNT(*) > 1
    """)
    duplicates = cursor.fetchall()
    
    if duplicates:
        print(f"\n⚠️ Найдены дубликаты отзывов:")
        for dup in duplicates:
            print(f"  Объект ID: {dup[0]}, Текст: {dup[1][:50]}..., Количество: {dup[2]}")
    else:
        print(f"\n✅ Дубликатов отзывов не найдено")
    
    conn.close()

if __name__ == "__main__":
    check_db_counts() 