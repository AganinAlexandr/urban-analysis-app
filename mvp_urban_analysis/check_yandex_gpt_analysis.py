#!/usr/bin/env python3
"""
Проверка наличия метода llm_yandex (YandexGPT) и его результатов анализа
"""
import sqlite3
import os

def check_yandex_gpt_analysis():
    """Проверяет наличие метода llm_yandex (YandexGPT) и его результатов анализа"""
    print("=== ПРОВЕРКА YANDEX GPT АНАЛИЗА (llm_yandex) ===")
    
    db_path = 'urban_analysis_fixed.db'
    
    if not os.path.exists(db_path):
        print(f"❌ База данных {db_path} не найдена")
        return
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 1. Проверяем наличие метода llm_yandex (YandexGPT)
        print("1. Проверка метода llm_yandex (YandexGPT)...")
        cursor.execute("""
            SELECT id, method_name, description, is_active 
            FROM processing_methods 
            WHERE method_name = 'llm_yandex'
        """)
        
        method = cursor.fetchone()
        
        if method:
            print(f"✅ Метод llm_yandex (YandexGPT) найден:")
            print(f"  ID: {method[0]}")
            print(f"  Название: {method[1]}")
            print(f"  Описание: {method[2]}")
            print(f"  Активен: {'Да' if method[3] else 'Нет'}")
            method_id = method[0]
        else:
            print("❌ Метод llm_yandex (YandexGPT) не найден в базе данных")
            print("   Нужно добавить метод llm_yandex")
            return
        
        # 2. Проверяем результаты анализа от llm_yandex (YandexGPT)
        print("\n2. Проверка результатов анализа от llm_yandex (YandexGPT)...")
        cursor.execute("""
            SELECT COUNT(*) 
            FROM analysis_results 
            WHERE method_id = ?
        """, (method_id,))
        
        results_count = cursor.fetchone()[0]
        print(f"   Результатов анализа от llm_yandex (YandexGPT): {results_count}")
        
        if results_count > 0:
            # Показываем примеры результатов
            cursor.execute("""
                SELECT ar.id, ar.review_id, ar.sentiment, ar.confidence, ar.review_type,
                       substr(r.review_text, 1, 50)
                FROM analysis_results ar
                JOIN reviews r ON ar.review_id = r.id
                WHERE ar.method_id = ?
                LIMIT 5
            """, (method_id,))
            
            results = cursor.fetchall()
            print(f"\n   Примеры результатов:")
            for result in results:
                print(f"     ID: {result[0]}, Отзыв: {result[1]}, Сентимент: {result[2]}, Уверенность: {result[3]}, Тип: {result[4]}")
                print(f"     Текст: {result[5]}...")
        else:
            print("   ❌ Нет результатов анализа от llm_yandex (YandexGPT)")
            print("   Нужно запустить анализ отзывов через YandexGPT")
        
        # 3. Проверяем все методы и их результаты
        print("\n3. Общая статистика методов анализа...")
        cursor.execute("""
            SELECT pm.method_name, pm.is_active, COUNT(ar.id) as results_count
            FROM processing_methods pm
            LEFT JOIN analysis_results ar ON pm.id = ar.method_id
            GROUP BY pm.id, pm.method_name, pm.is_active
            ORDER BY pm.id
        """)
        
        methods_stats = cursor.fetchall()
        print(f"   Всего методов: {len(methods_stats)}")
        
        total_results = 0
        for method_name, is_active, results_count in methods_stats:
            status = "✅ Активен" if is_active else "❌ Неактивен"
            print(f"     {method_name}: {status} - {results_count} результатов")
            total_results += results_count
        
        print(f"   Общее количество результатов: {total_results}")
        
        # 4. Проверяем отзывы без результатов анализа
        print("\n4. Проверка отзывов без результатов анализа...")
        cursor.execute("""
            SELECT COUNT(*) 
            FROM reviews r
            LEFT JOIN analysis_results ar ON r.id = ar.review_id
            WHERE ar.id IS NULL
        """)
        
        reviews_without_analysis = cursor.fetchone()[0]
        print(f"   Отзывов без результатов анализа: {reviews_without_analysis}")
        
        if reviews_without_analysis > 0:
            print("   ❌ Есть отзывы без результатов анализа")
            print("   Нужно запустить анализ для всех отзывов")
        
        conn.close()
        
        # 5. Рекомендации
        print("\n5. РЕКОМЕНДАЦИИ:")
        if not method:
            print("   - Добавить метод llm_yandex в таблицу processing_methods")
        if results_count == 0:
            print("   - Запустить анализ отзывов через YandexGPT")
        if reviews_without_analysis > 0:
            print("   - Запустить анализ для всех отзывов")
        
        print("\n✅ Проверка завершена")
        
    except Exception as e:
        print(f"❌ Ошибка при проверке: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_yandex_gpt_analysis()
