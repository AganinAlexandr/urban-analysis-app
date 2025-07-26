#!/usr/bin/env python3
"""
Проверка записей в analysis_results с несуществующими method_id
"""
import sqlite3

def check_orphaned_records():
    """Проверяет записи в analysis_results с несуществующими method_id"""
    print("=== ПРОВЕРКА ЗАПИСЕЙ С НЕСУЩЕСТВУЮЩИМИ METHOD_ID ===")
    
    try:
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        # Проверяем все записи в analysis_results
        print("1. Все записи в analysis_results:")
        cursor.execute("""
            SELECT COUNT(*) FROM analysis_results
        """)
        
        total_records = cursor.fetchone()[0]
        print(f"Всего записей: {total_records}")
        
        # Проверяем записи с существующими method_id
        print("\n2. Записи с существующими method_id:")
        cursor.execute("""
            SELECT pm.method_name, COUNT(ar.id) as count
            FROM analysis_results ar
            JOIN processing_methods pm ON ar.method_id = pm.id
            GROUP BY pm.method_name
            ORDER BY count DESC
        """)
        
        valid_records = cursor.fetchall()
        for method_name, count in valid_records:
            print(f"  {method_name}: {count} записей")
        
        # Проверяем записи с несуществующими method_id
        print("\n3. Записи с несуществующими method_id:")
        cursor.execute("""
            SELECT ar.method_id, COUNT(ar.id) as count
            FROM analysis_results ar
            LEFT JOIN processing_methods pm ON ar.method_id = pm.id
            WHERE pm.id IS NULL
            GROUP BY ar.method_id
            ORDER BY count DESC
        """)
        
        orphaned_records = cursor.fetchall()
        if orphaned_records:
            for method_id, count in orphaned_records:
                print(f"  method_id {method_id}: {count} записей")
        else:
            print("  Нет записей с несуществующими method_id")
        
        # Показываем примеры записей с несуществующими method_id
        if orphaned_records:
            print("\n4. Примеры записей с несуществующими method_id:")
            cursor.execute("""
                SELECT ar.id, ar.method_id, ar.sentiment, ar.confidence
                FROM analysis_results ar
                LEFT JOIN processing_methods pm ON ar.method_id = pm.id
                WHERE pm.id IS NULL
                LIMIT 5
            """)
            
            examples = cursor.fetchall()
            for record_id, method_id, sentiment, confidence in examples:
                print(f"  ID {record_id}: method_id={method_id}, sentiment={sentiment}, confidence={confidence}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_orphaned_records() 