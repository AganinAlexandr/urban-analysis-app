import sqlite3
import os

def check_db():
    db_path = "urban_analysis.db"
    
    if not os.path.exists(db_path):
        print(f"❌ База данных не найдена: {db_path}")
        return
    
    print(f"📊 Проверяем БД: {db_path}")
    
    conn = sqlite3.connect(db_path)
    
    # Проверяем таблицы
    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    print(f"📋 Таблицы: {[t[0] for t in tables]}")
    
    # Проверяем структуру reviews
    cursor = conn.execute("PRAGMA table_info(reviews)")
    columns = cursor.fetchall()
    print(f"📝 Поля таблицы reviews:")
    for col in columns:
        print(f"  - {col[1]} ({col[2]})")
    
    # Количество записей
    cursor = conn.execute("SELECT COUNT(*) FROM objects")
    obj_count = cursor.fetchone()[0]
    print(f"🏢 Объектов: {obj_count}")
    
    cursor = conn.execute("SELECT COUNT(*) FROM reviews")
    rev_count = cursor.fetchone()[0]
    print(f"💬 Отзывов: {rev_count}")
    
    # Примеры данных
    cursor = conn.execute("SELECT id, name, group_type FROM objects LIMIT 3")
    objects = cursor.fetchall()
    print(f"\n🏢 Примеры объектов:")
    for obj in objects:
        print(f"  - ID: {obj[0]}, Имя: {obj[1]}, Группа: {obj[2]}")
    
    cursor = conn.execute("""
        SELECT r.id, r.review_text[:30], r.rating, o.name 
        FROM reviews r 
        JOIN objects o ON r.object_id = o.id 
        LIMIT 3
    """)
    reviews = cursor.fetchall()
    print(f"\n💬 Примеры отзывов:")
    for rev in reviews:
        print(f"  - ID: {rev[0]}, Текст: {rev[1]}..., Рейтинг: {rev[2]}, Объект: {rev[3]}")
    
    conn.close()

if __name__ == "__main__":
    check_db() 