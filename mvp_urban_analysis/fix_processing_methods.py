#!/usr/bin/env python3
"""
Исправление методов обработки - полная очистка и восстановление
"""
import sqlite3

def fix_processing_methods():
    """Полностью очищает и восстанавливает методы обработки"""
    print("=== ИСПРАВЛЕНИЕ МЕТОДОВ ОБРАБОТКИ ===")
    
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
        
        # 2. Полностью очищаем таблицу processing_methods
        print("\n2. Полная очистка таблицы processing_methods...")
        cursor.execute("DELETE FROM processing_methods")
        deleted_count = cursor.rowcount
        print(f"   Удалено {deleted_count} методов")
        
        # 3. Восстанавливаем только нужные методы
        print("\n3. Восстановление нужных методов...")
        
        # Список нужных методов
        needed_methods = [
            (1, 'user_rating', 'Пользовательский рейтинг', True),
            (2, 'nlp_vader', 'Анализ сентимента VADER', True),
            (3, 'llm_yandex', 'Анализ через Yandex LLM', True),
            (4, 'llm_openai', 'Анализ через OpenAI LLM', True)
        ]
        
        for method_id, method_name, description, is_active in needed_methods:
            cursor.execute("""
                INSERT INTO processing_methods (id, method_name, description, is_active, created_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (method_id, method_name, description, is_active))
            print(f"   Добавлен метод: ID {method_id} - {method_name}")
        
        # 4. Проверяем результат
        print("\n4. Методы после восстановления:")
        cursor.execute("""
            SELECT pm.id, pm.method_name, pm.is_active, COUNT(ar.id) as usage_count
            FROM processing_methods pm
            LEFT JOIN analysis_results ar ON pm.id = ar.method_id
            GROUP BY pm.id, pm.method_name, pm.is_active
            ORDER BY pm.id
        """)
        restored_methods = cursor.fetchall()
        
        for method_id, method_name, is_active, usage_count in restored_methods:
            status = "активен" if is_active else "неактивен"
            print(f"  ID {method_id}: {method_name} - {status} (используется {usage_count} раз)")
        
        # 5. Проверяем результаты анализа
        print("\n5. Результаты анализа:")
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
        
        print("\n✅ Исправление методов завершено!")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    fix_processing_methods() 