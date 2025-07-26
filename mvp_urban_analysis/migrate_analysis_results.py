#!/usr/bin/env python3
"""
Миграция записей analysis_results к новым ID методов
"""
import sqlite3

def migrate_analysis_results():
    """Мигрирует записи analysis_results к новым ID методов"""
    print("=== МИГРАЦИЯ ANALYSIS_RESULTS К НОВЫМ ID МЕТОДОВ ===")
    
    try:
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        # Получаем текущие методы и их ID
        print("1. Текущие методы в БД:")
        cursor.execute("""
            SELECT id, method_name
            FROM processing_methods
            ORDER BY id
        """)
        
        methods = cursor.fetchall()
        method_id_map = {}
        for method_id, method_name in methods:
            method_id_map[method_name] = method_id
            print(f"  ID {method_id}: {method_name}")
        
        # Проверяем текущие записи в analysis_results
        print("\n2. Текущие записи в analysis_results:")
        cursor.execute("""
            SELECT pm.method_name, COUNT(ar.id) as count
            FROM analysis_results ar
            JOIN processing_methods pm ON ar.method_id = pm.id
            GROUP BY pm.method_name
            ORDER BY count DESC
        """)
        
        current_usage = cursor.fetchall()
        for method_name, count in current_usage:
            print(f"  {method_name}: {count} записей")
        
        # Обновляем записи для user_rating (старый ID -> новый ID)
        print("\n3. Обновляем записи user_rating...")
        
        # Находим старые записи user_rating
        cursor.execute("""
            SELECT ar.id, ar.review_id, ar.sentiment, ar.confidence, ar.review_type, ar.processed_at
            FROM analysis_results ar
            JOIN processing_methods pm ON ar.method_id = pm.id
            WHERE pm.method_name = 'user_rating'
        """)
        
        user_rating_records = cursor.fetchall()
        print(f"Найдено записей user_rating: {len(user_rating_records)}")
        
        if user_rating_records:
            # Получаем новый ID для user_rating
            new_user_rating_id = method_id_map.get('user_rating')
            
            if new_user_rating_id:
                # Удаляем старые записи
                cursor.execute("""
                    DELETE FROM analysis_results 
                    WHERE method_id IN (
                        SELECT id FROM processing_methods WHERE method_name = 'user_rating'
                    )
                """)
                
                # Добавляем записи с новым ID
                for record in user_rating_records:
                    cursor.execute("""
                        INSERT INTO analysis_results 
                        (review_id, method_id, sentiment, confidence, review_type, processed_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (record[1], new_user_rating_id, record[2], record[3], record[4], record[5]))
                
                print(f"✅ Обновлено записей user_rating: {len(user_rating_records)}")
            else:
                print("❌ Не найден новый ID для user_rating")
        
        # Обновляем записи для nlp_vader (если есть)
        print("\n4. Обновляем записи nlp_vader...")
        
        cursor.execute("""
            SELECT ar.id, ar.review_id, ar.sentiment, ar.confidence, ar.review_type, ar.processed_at
            FROM analysis_results ar
            JOIN processing_methods pm ON ar.method_id = pm.id
            WHERE pm.method_name = 'nlp_vader'
        """)
        
        nlp_vader_records = cursor.fetchall()
        print(f"Найдено записей nlp_vader: {len(nlp_vader_records)}")
        
        if nlp_vader_records:
            new_nlp_vader_id = method_id_map.get('nlp_vader')
            
            if new_nlp_vader_id:
                # Удаляем старые записи
                cursor.execute("""
                    DELETE FROM analysis_results 
                    WHERE method_id IN (
                        SELECT id FROM processing_methods WHERE method_name = 'nlp_vader'
                    )
                """)
                
                # Добавляем записи с новым ID
                for record in nlp_vader_records:
                    cursor.execute("""
                        INSERT INTO analysis_results 
                        (review_id, method_id, sentiment, confidence, review_type, processed_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (record[1], new_nlp_vader_id, record[2], record[3], record[4], record[5]))
                
                print(f"✅ Обновлено записей nlp_vader: {len(nlp_vader_records)}")
            else:
                print("❌ Не найден новый ID для nlp_vader")
        
        conn.commit()
        
        # Проверяем результат
        print("\n5. Проверяем результат:")
        cursor.execute("""
            SELECT pm.method_name, COUNT(ar.id) as count
            FROM processing_methods pm
            LEFT JOIN analysis_results ar ON pm.id = ar.method_id
            GROUP BY pm.id, pm.method_name
            ORDER BY count DESC
        """)
        
        final_usage = cursor.fetchall()
        for method_name, count in final_usage:
            if count > 0:
                print(f"  {method_name}: {count} записей")
            else:
                print(f"  {method_name}: 0 записей")
        
        conn.close()
        print("\n✅ Миграция завершена!")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    migrate_analysis_results() 