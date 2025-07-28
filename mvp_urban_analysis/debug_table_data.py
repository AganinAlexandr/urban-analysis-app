import sqlite3
import json

def debug_table_data():
    """Отладка данных для таблиц"""
    
    print("=== ОТЛАДКА ДАННЫХ ТАБЛИЦ ===")
    
    # Подключаемся к БД
    conn = sqlite3.connect('urban_analysis_fixed.db')
    cursor = conn.cursor()
    
    # 1. Проверяем общее количество записей
    print("\n1. ОБЩАЯ СТАТИСТИКА:")
    cursor.execute("SELECT COUNT(*) FROM objects")
    objects_count = cursor.fetchone()[0]
    print(f"Объектов: {objects_count}")
    
    cursor.execute("SELECT COUNT(*) FROM reviews")
    reviews_count = cursor.fetchone()[0]
    print(f"Отзывов: {reviews_count}")
    
    cursor.execute("SELECT COUNT(*) FROM analysis_results")
    analysis_count = cursor.fetchone()[0]
    print(f"Результатов анализа: {analysis_count}")
    
    # 2. Проверяем данные для "Таблица БД"
    print("\n2. ДАННЫЕ ДЛЯ 'ТАБЛИЦА БД':")
    query = """
        SELECT 
            o.name,
            o.address,
            og.group_name as group_type,
            dg.group_name as determined_group,
            r.review_text,
            r.rating,
            COALESCE(ar.sentiment, 'neutral') as sentiment
        FROM objects o
        LEFT JOIN object_groups og ON o.group_id = og.id
        LEFT JOIN detected_groups dg ON o.detected_group_id = dg.id
        LEFT JOIN reviews r ON o.id = r.object_id
        LEFT JOIN analysis_results ar ON r.id = ar.review_id
        ORDER BY o.name, r.id
        LIMIT 10
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    print(f"Записей в результате: {len(results)}")
    
    for i, row in enumerate(results[:3]):  # Показываем первые 3
        name, address, group_type, determined_group, review_text, rating, sentiment = row
        print(f"  {i+1}. {name} | {group_type} | {sentiment}")
    
    # 3. Проверяем данные для "Таблица Новая" (последние записи)
    print("\n3. ДАННЫЕ ДЛЯ 'ТАБЛИЦА НОВАЯ':")
    query = """
        SELECT 
            o.name,
            o.address,
            og.group_name as group_type,
            dg.group_name as determined_group,
            r.review_text,
            r.rating,
            COALESCE(ar.sentiment, 'neutral') as sentiment
        FROM objects o
        LEFT JOIN object_groups og ON o.group_id = og.id
        LEFT JOIN detected_groups dg ON o.detected_group_id = dg.id
        LEFT JOIN reviews r ON o.id = r.object_id
        LEFT JOIN analysis_results ar ON r.id = ar.review_id
        ORDER BY o.id DESC, r.id DESC
        LIMIT 10
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    print(f"Записей в результате: {len(results)}")
    
    for i, row in enumerate(results[:3]):  # Показываем первые 3
        name, address, group_type, determined_group, review_text, rating, sentiment = row
        print(f"  {i+1}. {name} | {group_type} | {sentiment}")
    
    conn.close()

if __name__ == "__main__":
    debug_table_data() 