#!/usr/bin/env python3
"""
Проверка метода user_rating в базе данных
"""
import sys
import os
import sqlite3

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def check_user_rating_method():
    """Проверяет метод user_rating в базе данных"""
    print("=== ПРОВЕРКА МЕТОДА USER_RATING ===")
    
    try:
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        # Находим ID метода user_rating
        cursor.execute("""
            SELECT id, method_name, description, is_active
            FROM processing_methods 
            WHERE method_name = 'user_rating'
        """)
        
        user_rating_method = cursor.fetchone()
        
        if user_rating_method:
            method_id, method_name, description, is_active = user_rating_method
            print(f"✅ Метод user_rating найден:")
            print(f"  - ID: {method_id}")
            print(f"  - Название: {method_name}")
            print(f"  - Описание: {description}")
            print(f"  - Активен: {is_active}")
            
            # Проверяем, есть ли записи с этим методом в analysis_results
            cursor.execute("""
                SELECT COUNT(*) 
                FROM analysis_results 
                WHERE method_id = ?
            """, (method_id,))
            
            count = cursor.fetchone()[0]
            print(f"  - Записей в analysis_results: {count}")
            
            if count > 0:
                print("✅ Есть записи с методом user_rating!")
                
                # Показываем несколько примеров
                cursor.execute("""
                    SELECT ar.id, ar.sentiment, ar.confidence, ar.review_type
                    FROM analysis_results ar
                    WHERE ar.method_id = ?
                    LIMIT 3
                """, (method_id,))
                
                examples = cursor.fetchall()
                print("  Примеры записей:")
                for i, example in enumerate(examples, 1):
                    result_id, sentiment, confidence, review_type = example
                    print(f"    {i}. ID: {result_id}, Сентимент: {sentiment}, Уверенность: {confidence}, Тип: {review_type}")
            else:
                print("❌ Нет записей с методом user_rating в analysis_results")
        else:
            print("❌ Метод user_rating не найден в processing_methods")
        
        # Проверяем все методы и их использование
        print(f"\n=== ВСЕ МЕТОДЫ И ИХ ИСПОЛЬЗОВАНИЕ ===")
        cursor.execute("""
            SELECT pm.id, pm.method_name, pm.description, pm.is_active,
                   COUNT(ar.id) as usage_count
            FROM processing_methods pm
            LEFT JOIN analysis_results ar ON pm.id = ar.method_id
            GROUP BY pm.id, pm.method_name, pm.description, pm.is_active
            ORDER BY pm.method_name
        """)
        
        all_methods = cursor.fetchall()
        
        for method in all_methods:
            method_id, method_name, description, is_active, usage_count = method
            status = "✅ Активен" if is_active else "❌ Неактивен"
            print(f"  - {method_name} ({status}): {usage_count} записей")
            if description:
                print(f"    Описание: {description}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_user_rating_method() 