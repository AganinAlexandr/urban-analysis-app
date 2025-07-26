import sqlite3
import pandas as pd
import os

def check_database():
    """Проверяем состояние базы данных"""
    db_path = "data/urban_analysis.db"
    
    if not os.path.exists(db_path):
        print(f"❌ База данных не найдена: {db_path}")
        return
    
    print(f"📊 Проверяем базу данных: {db_path}")
    
    conn = sqlite3.connect(db_path)
    
    # Проверяем таблицы
    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    print(f"📋 Таблицы в БД: {[table[0] for table in tables]}")
    
    # Проверяем структуру таблицы reviews
    cursor = conn.execute("PRAGMA table_info(reviews)")
    columns = cursor.fetchall()
    print(f"📝 Структура таблицы reviews:")
    for col in columns:
        print(f"  - {col[1]} ({col[2]})")
    
    # Проверяем данные в таблицах
    cursor = conn.execute("SELECT COUNT(*) FROM objects")
    objects_count = cursor.fetchone()[0]
    print(f"🏢 Объектов в БД: {objects_count}")
    
    cursor = conn.execute("SELECT COUNT(*) FROM reviews")
    reviews_count = cursor.fetchone()[0]
    print(f"💬 Отзывов в БД: {reviews_count}")
    
    # Проверяем поле в_Выборке
    cursor = conn.execute("SELECT COUNT(*) FROM reviews WHERE в_Выборке IS NOT NULL")
    sample_count = cursor.fetchone()[0]
    print(f"✅ Записей в выборке: {sample_count}")
    
    # Показываем несколько примеров объектов
    cursor = conn.execute("""
        SELECT o.id, o.name, o.address, o.group_type, o.detected_group_type, 
               COUNT(r.id) as reviews_count
        FROM objects o
        LEFT JOIN reviews r ON o.id = r.object_id
        GROUP BY o.id
        LIMIT 5
    """)
    objects = cursor.fetchall()
    print(f"\n🏢 Примеры объектов:")
    for obj in objects:
        print(f"  - ID: {obj[0]}, Имя: {obj[1]}, Адрес: {obj[2]}")
        print(f"    Группа: {obj[3]}, Определенная группа: {obj[4]}, Отзывов: {obj[5]}")
    
    # Показываем несколько примеров отзывов
    cursor = conn.execute("""
        SELECT r.id, r.review_text[:50], r.rating, r.review_date, r.в_Выборке,
               o.name, o.group_type
        FROM reviews r
        JOIN objects o ON r.object_id = o.id
        LIMIT 5
    """)
    reviews = cursor.fetchall()
    print(f"\n💬 Примеры отзывов:")
    for review in reviews:
        print(f"  - ID: {review[0]}, Текст: {review[1]}..., Рейтинг: {review[2]}")
        print(f"    Дата: {review[3]}, В выборке: {review[4]}, Объект: {review[5]}, Группа: {review[6]}")
    
    conn.close()

def test_sample_creation():
    """Тестируем создание выборки"""
    print(f"\n🧪 Тестируем создание выборки...")
    
    # Импортируем SampleManager
    import sys
    sys.path.append('app/core')
    
    try:
        from sample_manager import SampleManager
        sample_manager = SampleManager()
        
        # Тестируем с разными фильтрами
        filters = [
            {},  # Без фильтров
            {'group_type': 'Кафе'},  # По группе
            {'sentiment_method': 'textblob'},  # По методу анализа
            {'color_scheme': 'rating'}  # По цветовой схеме
        ]
        
        for i, filters in enumerate(filters):
            print(f"\n🔍 Тест {i+1}: Фильтры = {filters}")
            try:
                count = sample_manager.create_sample_from_filters(filters)
                print(f"   ✅ Создана выборка из {count} записей")
            except Exception as e:
                print(f"   ❌ Ошибка: {e}")
        
    except ImportError as e:
        print(f"❌ Не удалось импортировать SampleManager: {e}")
        print("Возможно, файл sample_manager.py был удален")

if __name__ == "__main__":
    check_database()
    test_sample_creation() 