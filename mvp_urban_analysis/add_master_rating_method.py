#!/usr/bin/env python3
"""
Добавление Master_Rating как метода обработки
"""
import sqlite3

def add_master_rating_method():
    """Добавляет Master_Rating как метод обработки"""
    print("=== ДОБАВЛЕНИЕ MASTER_RATING КАК МЕТОДА ОБРАБОТКИ ===")
    
    try:
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        # 1. Проверяем текущие методы
        print("1. Текущие методы обработки:")
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
        
        # 2. Добавляем Master_Rating как метод
        print("\n2. Добавление Master_Rating метода...")
        
        # Проверяем, есть ли уже Master_Rating
        cursor.execute("SELECT COUNT(*) FROM processing_methods WHERE method_name = 'master_rating'")
        exists = cursor.fetchone()[0]
        
        if exists == 0:
            cursor.execute("""
                INSERT INTO processing_methods (id, method_name, description, is_active, created_at)
                VALUES (5, 'master_rating', 'Master Rating - специальный метод оценки', True, CURRENT_TIMESTAMP)
            """)
            print("   Добавлен метод: ID 5 - master_rating")
        else:
            print("   Метод master_rating уже существует")
        
        # 3. Проверяем результат
        print("\n3. Методы после добавления:")
        cursor.execute("""
            SELECT pm.id, pm.method_name, pm.is_active, COUNT(ar.id) as usage_count
            FROM processing_methods pm
            LEFT JOIN analysis_results ar ON pm.id = ar.method_id
            GROUP BY pm.id, pm.method_name, pm.is_active
            ORDER BY pm.id
        """)
        updated_methods = cursor.fetchall()
        
        for method_id, method_name, is_active, usage_count in updated_methods:
            status = "активен" if is_active else "неактивен"
            print(f"  ID {method_id}: {method_name} - {status} (используется {usage_count} раз)")
        
        conn.commit()
        conn.close()
        
        print("\n✅ Master_Rating добавлен как метод обработки!")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    add_master_rating_method() 