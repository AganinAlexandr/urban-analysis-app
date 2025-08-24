#!/usr/bin/env python3
"""
Проверка структуры базы данных и связей между таблицами
"""
import sqlite3

def check_database_structure():
    """Проверяет структуру базы данных и связи"""
    print("=== ПРОВЕРКА СТРУКТУРЫ БАЗЫ ДАННЫХ ===")
    
    try:
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        # 1. Проверяем структуру таблицы objects
        print("1. Структура таблицы objects:")
        cursor.execute("PRAGMA table_info(objects)")
        columns = cursor.fetchall()
        for col in columns:
            print(f"  {col[1]} ({col[2]})")
        
        # 2. Проверяем методы обработки и их связи
        print("\n2. Методы обработки и их связи:")
        cursor.execute("""
            SELECT pm.id, pm.method_name, pm.is_active, COUNT(ar.id) as usage_count
            FROM processing_methods pm
            LEFT JOIN analysis_results ar ON pm.id = ar.method_id
            GROUP BY pm.id, pm.method_name, pm.is_active
            ORDER BY pm.id
        """)
        methods = cursor.fetchall()
        
        for method_id, method_name, is_active, usage_count in methods:
            status = "активен" if is_active else "неактивен"
            print(f"  ID {method_id}: {method_name} - {status} (используется {usage_count} раз)")
        
        # 3. Проверяем результаты анализа
        print("\n3. Результаты анализа:")
        cursor.execute("SELECT COUNT(*) FROM analysis_results")
        analysis_count = cursor.fetchone()[0]
        print(f"  Всего записей: {analysis_count}")
        
        if analysis_count > 0:
            cursor.execute("""
                SELECT pm.method_name, COUNT(ar.id) as count
                FROM analysis_results ar
                JOIN processing_methods pm ON ar.method_id = pm.id
                GROUP BY pm.method_name
                ORDER BY count DESC
            """)
            analysis_by_method = cursor.fetchall()
            
            print("  По методам:")
            for method_name, count in analysis_by_method:
                print(f"    {method_name}: {count}")
        
        # 4. Проверяем объекты
        print("\n4. Объекты:")
        cursor.execute("SELECT COUNT(*) FROM objects")
        objects_count = cursor.fetchone()[0]
        print(f"  Всего объектов: {objects_count}")
        
        if objects_count > 0:
            cursor.execute("""
                SELECT id, name, address, detected_group_type
                FROM objects
                LIMIT 5
            """)
            objects = cursor.fetchall()
            
            print("  Примеры объектов:")
            for obj_id, name, address, group_type in objects:
                print(f"    ID {obj_id}: {name} - {group_type}")
        
        # 5. Проверяем отзывы
        print("\n5. Отзывы:")
        cursor.execute("SELECT COUNT(*) FROM reviews")
        reviews_count = cursor.fetchone()[0]
        print(f"  Всего отзывов: {reviews_count}")
        
        if reviews_count > 0:
            cursor.execute("""
                SELECT object_id, COUNT(*) as count
                FROM reviews
                GROUP BY object_id
                ORDER BY count DESC
                LIMIT 5
            """)
            reviews_by_object = cursor.fetchall()
            
            print("  Отзывы по объектам:")
            for object_id, count in reviews_by_object:
                print(f"    Объект {object_id}: {count} отзывов")
        
        # 6. Проверяем связи между таблицами
        print("\n6. Связи между таблицами:")
        
        # Проверяем, есть ли отзывы без объектов
        cursor.execute("""
            SELECT COUNT(*)
            FROM reviews r
            LEFT JOIN objects o ON r.object_id = o.id
            WHERE o.id IS NULL
        """)
        orphaned_reviews = cursor.fetchone()[0]
        print(f"  Отзывов без объектов: {orphaned_reviews}")
        
        # Проверяем, есть ли результаты анализа без отзывов
        cursor.execute("""
            SELECT COUNT(*)
            FROM analysis_results ar
            LEFT JOIN reviews r ON ar.review_id = r.id
            WHERE r.id IS NULL
        """)
        orphaned_analysis = cursor.fetchone()[0]
        print(f"  Результатов анализа без отзывов: {orphaned_analysis}")
        
        # Проверяем, есть ли результаты анализа без методов
        cursor.execute("""
            SELECT COUNT(*)
            FROM analysis_results ar
            LEFT JOIN processing_methods pm ON ar.method_id = pm.id
            WHERE pm.id IS NULL
        """)
        orphaned_methods = cursor.fetchone()[0]
        print(f"  Результатов анализа без методов: {orphaned_methods}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_database_structure() 