#!/usr/bin/env python3
"""
Очистка неиспользуемых методов обработки
"""
import sqlite3

def clean_unused_methods():
    """Удаляет неиспользуемые методы обработки"""
    print("=== ОЧИСТКА НЕИСПОЛЬЗУЕМЫХ МЕТОДОВ ОБРАБОТКИ ===")
    
    try:
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        # Проверяем текущие методы
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
        
        # Удаляем неиспользуемые методы (ID 5, 6, 7, 8)
        print("\n2. Удаление неиспользуемых методов...")
        cursor.execute("""
            DELETE FROM processing_methods 
            WHERE id IN (5, 6, 7, 8)
        """)
        deleted_count = cursor.rowcount
        print(f"   Удалено {deleted_count} методов")
        
        # Проверяем результат
        print("\n3. Методы после очистки:")
        cursor.execute("""
            SELECT pm.id, pm.method_name, pm.is_active, COUNT(ar.id) as usage_count
            FROM processing_methods pm
            LEFT JOIN analysis_results ar ON pm.id = ar.method_id
            GROUP BY pm.id, pm.method_name, pm.is_active
            ORDER BY pm.id
        """)
        remaining_methods = cursor.fetchall()
        
        for method_id, method_name, is_active, usage_count in remaining_methods:
            status = "активен" if is_active else "неактивен"
            print(f"  ID {method_id}: {method_name} - {status} (используется {usage_count} раз)")
        
        # Проверяем результаты анализа
        print("\n4. Результаты анализа:")
        cursor.execute("SELECT COUNT(*) FROM analysis_results")
        analysis_count = cursor.fetchone()[0]
        print(f"   Всего записей: {analysis_count}")
        
        if analysis_count > 0:
            cursor.execute("""
                SELECT pm.method_name, COUNT(ar.id) as count
                FROM analysis_results ar
                JOIN processing_methods pm ON ar.method_id = pm.id
                GROUP BY pm.method_name
                ORDER BY count DESC
            """)
            analysis_by_method = cursor.fetchall()
            
            print("   По методам:")
            for method_name, count in analysis_by_method:
                print(f"     {method_name}: {count}")
        
        conn.commit()
        conn.close()
        
        print("\n✅ Очистка завершена!")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    clean_unused_methods() 