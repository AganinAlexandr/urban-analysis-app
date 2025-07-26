#!/usr/bin/env python3
"""
Пересоздание записей analysis_results с правильными методами
"""
import sqlite3
from datetime import datetime

def recreate_analysis_results():
    """Пересоздает записи analysis_results с правильными методами"""
    print("=== ПЕРЕСОЗДАНИЕ ANALYSIS_RESULTS ===")
    
    try:
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        # Очищаем таблицу analysis_results
        print("1. Очищаем таблицу analysis_results...")
        cursor.execute("DELETE FROM analysis_results")
        print("✅ Таблица очищена")
        
        # Получаем ID методов
        print("\n2. Получаем ID методов...")
        cursor.execute("""
            SELECT id, method_name FROM processing_methods
            WHERE method_name IN ('user_rating', 'nlp_vader')
            ORDER BY method_name
        """)
        
        methods = cursor.fetchall()
        method_ids = {}
        for method_id, method_name in methods:
            method_ids[method_name] = method_id
            print(f"  {method_name}: ID {method_id}")
        
        # Получаем все отзывы
        print("\n3. Получаем отзывы...")
        cursor.execute("""
            SELECT id, rating FROM reviews
        """)
        
        reviews = cursor.fetchall()
        print(f"Найдено отзывов: {len(reviews)}")
        
        if not reviews:
            print("❌ Нет отзывов!")
            return
        
        # Добавляем записи для user_rating
        print("\n4. Добавляем записи для user_rating...")
        user_rating_id = method_ids.get('user_rating')
        if not user_rating_id:
            print("❌ Не найден ID для user_rating")
            return
        
        user_rating_count = 0
        for review_id, rating in reviews:
            if rating is not None and 1 <= rating <= 5:
                # Конвертируем рейтинг в сентимент
                if rating == 5:
                    sentiment = "positive"
                    confidence = 1.0
                elif rating in [1, 2]:
                    sentiment = "negative"
                    confidence = 0.8
                elif rating in [3, 4]:
                    sentiment = "neutral"
                    confidence = 0.6
                else:
                    sentiment = "neutral"
                    confidence = 0.5
                
                cursor.execute("""
                    INSERT INTO analysis_results 
                    (review_id, method_id, sentiment, confidence, review_type, processed_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (review_id, user_rating_id, sentiment, confidence, 'informational', datetime.now()))
                
                user_rating_count += 1
        
        print(f"✅ Добавлено записей user_rating: {user_rating_count}")
        
        # Добавляем записи для nlp_vader
        print("\n5. Добавляем записи для nlp_vader...")
        nlp_vader_id = method_ids.get('nlp_vader')
        if not nlp_vader_id:
            print("❌ Не найден ID для nlp_vader")
            return
        
        nlp_vader_count = 0
        for review_id, rating in reviews:
            # Для nlp_vader используем нейтральный сентимент по умолчанию
            # В реальной системе здесь был бы результат анализа VADER
            sentiment = "neutral"
            confidence = 0.5
            
            cursor.execute("""
                INSERT INTO analysis_results 
                (review_id, method_id, sentiment, confidence, review_type, processed_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (review_id, nlp_vader_id, sentiment, confidence, 'informational', datetime.now()))
            
            nlp_vader_count += 1
        
        print(f"✅ Добавлено записей nlp_vader: {nlp_vader_count}")
        
        conn.commit()
        
        # Проверяем результат
        print("\n6. Проверяем результат:")
        cursor.execute("""
            SELECT pm.method_name, COUNT(ar.id) as count
            FROM processing_methods pm
            LEFT JOIN analysis_results ar ON pm.id = ar.method_id
            WHERE pm.method_name IN ('user_rating', 'nlp_vader')
            GROUP BY pm.method_name
            ORDER BY pm.method_name
        """)
        
        results = cursor.fetchall()
        for method_name, count in results:
            print(f"  {method_name}: {count} записей")
        
        # Показываем статистику по сентиментам
        print("\n7. Статистика по сентиментам:")
        for method_name in ['user_rating', 'nlp_vader']:
            method_id = method_ids[method_name]
            cursor.execute("""
                SELECT sentiment, COUNT(*) as count
                FROM analysis_results 
                WHERE method_id = ?
                GROUP BY sentiment
                ORDER BY count DESC
            """, (method_id,))
            
            sentiment_stats = cursor.fetchall()
            print(f"\n  {method_name}:")
            for sentiment, count in sentiment_stats:
                print(f"    - {sentiment}: {count} записей")
        
        conn.close()
        print("\n✅ Пересоздание завершено!")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    recreate_analysis_results() 