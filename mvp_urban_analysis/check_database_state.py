#!/usr/bin/env python3
"""
Проверка текущего состояния базы данных
"""
import sqlite3

def check_database_state():
    """Проверяет текущее состояние базы данных"""
    print("=== ПРОВЕРКА СОСТОЯНИЯ БАЗЫ ДАННЫХ ===")
    
    try:
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        # Проверяем структуру таблицы objects
        print("1. Структура таблицы objects:")
        cursor.execute("PRAGMA table_info(objects)")
        columns = cursor.fetchall()
        for col in columns:
            print(f"  {col[1]} ({col[2]})")
        
        # Проверяем объекты
        print("\n2. Объекты:")
        cursor.execute("SELECT COUNT(*) FROM objects")
        objects_count = cursor.fetchone()[0]
        print(f"  Всего объектов: {objects_count}")
        
        if objects_count > 0:
            # Проверяем, есть ли колонка detected_group_type
            cursor.execute("PRAGMA table_info(objects)")
            columns = [col[1] for col in cursor.fetchall()]
            
            if 'detected_group_type' in columns:
                cursor.execute("""
                    SELECT detected_group_type, COUNT(*) as count
                    FROM objects
                    GROUP BY detected_group_type
                    ORDER BY count DESC
                """)
                groups = cursor.fetchall()
                for group_type, count in groups:
                    print(f"    {group_type}: {count}")
            else:
                print("    Колонка detected_group_type не найдена")
        
        # Проверяем отзывы
        print("\n3. Отзывы:")
        cursor.execute("SELECT COUNT(*) FROM reviews")
        reviews_count = cursor.fetchone()[0]
        print(f"  Всего отзывов: {reviews_count}")
        
        if reviews_count > 0:
            cursor.execute("""
                SELECT rating, COUNT(*) as count
                FROM reviews
                WHERE rating IS NOT NULL
                GROUP BY rating
                ORDER BY rating
            """)
            ratings = cursor.fetchall()
            for rating, count in ratings:
                print(f"    Рейтинг {rating}: {count}")
        
        # Проверяем методы
        print("\n4. Методы обработки:")
        cursor.execute("""
            SELECT id, method_name, is_active
            FROM processing_methods
            ORDER BY id
        """)
        methods = cursor.fetchall()
        for method_id, method_name, is_active in methods:
            status = "активен" if is_active else "неактивен"
            print(f"  ID {method_id}: {method_name} - {status}")
        
        # Проверяем результаты анализа
        print("\n5. Результаты анализа:")
        cursor.execute("""
            SELECT pm.method_name, COUNT(ar.id) as count
            FROM analysis_results ar
            JOIN processing_methods pm ON ar.method_id = pm.id
            GROUP BY pm.method_name
            ORDER BY count DESC
        """)
        analysis_results = cursor.fetchall()
        for method_name, count in analysis_results:
            print(f"  {method_name}: {count} записей")
        
        # Проверяем координаты объектов
        print("\n6. Координаты объектов:")
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 1 END) as with_coords,
                COUNT(CASE WHEN latitude IS NULL OR longitude IS NULL THEN 1 END) as without_coords
            FROM objects
        """)
        coords_stats = cursor.fetchone()
        print(f"  Всего объектов: {coords_stats[0]}")
        print(f"  С координатами: {coords_stats[1]}")
        print(f"  Без координат: {coords_stats[2]}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_database_state() 