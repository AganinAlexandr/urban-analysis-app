import sqlite3
import os

def clean_duplicates():
    db_path = "urban_analysis.db"
    
    print(f"🧹 Очищаем дубликаты в БД: {db_path}")
    
    conn = sqlite3.connect(db_path)
    
    # Проверяем дубликаты перед очисткой
    cursor = conn.execute("""
        SELECT object_id, review_text, COUNT(*) as count
        FROM reviews
        GROUP BY object_id, review_text
        HAVING COUNT(*) > 1
    """)
    duplicates = cursor.fetchall()
    
    print(f"📊 Найдено {len(duplicates)} групп дубликатов:")
    for dup in duplicates:
        print(f"  Объект ID: {dup[0]}, Текст: {dup[1][:50]}..., Количество: {dup[2]}")
    
    if duplicates:
        # Удаляем дубликаты, оставляя только первую запись
        cursor = conn.execute("""
            DELETE FROM reviews 
            WHERE id NOT IN (
                SELECT MIN(id) 
                FROM reviews 
                GROUP BY object_id, review_text
            )
        """)
        
        deleted_count = cursor.rowcount
        print(f"🗑️ Удалено {deleted_count} дубликатов")
        
        # Проверяем результат
        cursor = conn.execute("SELECT COUNT(*) FROM reviews")
        remaining_count = cursor.fetchone()[0]
        print(f"📋 Осталось {remaining_count} отзывов")
        
        # Проверяем, что дубликатов больше нет
        cursor = conn.execute("""
            SELECT object_id, review_text, COUNT(*) as count
            FROM reviews
            GROUP BY object_id, review_text
            HAVING COUNT(*) > 1
        """)
        remaining_duplicates = cursor.fetchall()
        
        if remaining_duplicates:
            print(f"⚠️ Остались дубликаты: {len(remaining_duplicates)}")
        else:
            print(f"✅ Дубликаты успешно удалены")
    else:
        print(f"✅ Дубликатов не найдено")
    
    conn.commit()
    conn.close()

if __name__ == "__main__":
    clean_duplicates() 