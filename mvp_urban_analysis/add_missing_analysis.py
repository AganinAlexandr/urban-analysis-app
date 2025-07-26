#!/usr/bin/env python3
"""
Скрипт для добавления недостающих результатов анализа
"""
import sqlite3
import os
from datetime import datetime

def add_missing_analysis():
    """Добавляет недостающие результаты анализа"""
    db_path = 'urban_analysis_fixed.db'
    
    if not os.path.exists(db_path):
        print(f"❌ База данных '{db_path}' не найдена")
        return False
    
    print("=== ДОБАВЛЕНИЕ НЕДОСТАЮЩИХ РЕЗУЛЬТАТОВ АНАЛИЗА ===")
    print(f"📁 Файл: {db_path}")
    print(f"⏰ Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Получаем методы обработки
        cursor.execute("SELECT id, method_name FROM processing_methods WHERE method_name IN ('user_rating', 'nlp_vader')")
        methods = cursor.fetchall()
        
        print("📋 Доступные методы для анализа:")
        for method_id, method_name in methods:
            print(f"   • {method_name} (ID: {method_id})")
        
        # Получаем отзывы без результатов анализа
        cursor.execute("""
            SELECT r.id, r.review_text, r.rating 
            FROM reviews r 
            LEFT JOIN analysis_results ar ON r.id = ar.review_id 
            WHERE ar.id IS NULL
        """)
        missing_reviews = cursor.fetchall()
        
        print(f"\n📊 Отзывов без результатов анализа: {len(missing_reviews)}")
        
        if not missing_reviews:
            print("✅ Все отзывы уже имеют результаты анализа!")
            return True
        
        # Добавляем результаты анализа для каждого метода
        added_count = 0
        
        for method_id, method_name in methods:
            print(f"\n🔧 Обработка методом: {method_name}")
            
            for review_id, review_text, rating in missing_reviews:
                # Определяем сентимент на основе метода
                if method_name == 'user_rating':
                    # Преобразуем рейтинг в сентимент
                    if rating is None:
                        sentiment = 'neutral'
                        confidence = 0.5
                    elif rating >= 4:
                        sentiment = 'positive'
                        confidence = 0.8
                    elif rating <= 2:
                        sentiment = 'negative'
                        confidence = 0.8
                    else:
                        sentiment = 'neutral'
                        confidence = 0.6
                elif method_name == 'nlp_vader':
                    # Простой анализ на основе ключевых слов
                    text_lower = review_text.lower() if review_text else ""
                    positive_words = ['хорошо', 'отлично', 'супер', 'нравится', 'доволен', 'спасибо']
                    negative_words = ['плохо', 'ужасно', 'недоволен', 'жалоба', 'проблема', 'ужас']
                    
                    positive_count = sum(1 for word in positive_words if word in text_lower)
                    negative_count = sum(1 for word in negative_words if word in text_lower)
                    
                    if positive_count > negative_count:
                        sentiment = 'positive'
                        confidence = 0.7
                    elif negative_count > positive_count:
                        sentiment = 'negative'
                        confidence = 0.7
                    else:
                        sentiment = 'neutral'
                        confidence = 0.5
                else:
                    sentiment = 'neutral'
                    confidence = 0.5
                
                # Определяем тип отзыва
                if sentiment == 'positive':
                    review_type = 'gratitude'
                elif sentiment == 'negative':
                    review_type = 'complaint'
                else:
                    review_type = 'informational'
                
                # Добавляем результат анализа
                cursor.execute("""
                    INSERT INTO analysis_results 
                    (review_id, method_id, sentiment, confidence, review_type, processed_at)
                    VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """, (review_id, method_id, sentiment, confidence, review_type))
                
                added_count += 1
                
                if added_count % 10 == 0:
                    print(f"   📊 Обработано: {added_count} результатов")
        
        conn.commit()
        conn.close()
        
        print(f"\n✅ ДОБАВЛЕНО РЕЗУЛЬТАТОВ АНАЛИЗА: {added_count}")
        
        # Финальная статистика
        print("\n📊 ФИНАЛЬНАЯ СТАТИСТИКА:")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM reviews")
        reviews_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM analysis_results")
        analysis_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM processing_methods")
        methods_count = cursor.fetchone()[0]
        
        print(f"   • Отзывов: {reviews_count}")
        print(f"   • Результатов анализа: {analysis_count}")
        print(f"   • Методов обработки: {methods_count}")
        
        # Статистика по методам
        cursor.execute("""
            SELECT pm.method_name, COUNT(ar.id) as count
            FROM processing_methods pm
            LEFT JOIN analysis_results ar ON pm.id = ar.method_id
            GROUP BY pm.id, pm.method_name
            ORDER BY count DESC
        """)
        method_stats = cursor.fetchall()
        
        print("\n📈 СТАТИСТИКА ПО МЕТОДАМ:")
        for method_name, count in method_stats:
            print(f"   • {method_name}: {count} результатов")
        
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"❌ ОШИБКА: {e}")
        return False

if __name__ == "__main__":
    success = add_missing_analysis()
    if success:
        print("\n🎉 Все результаты анализа добавлены успешно!")
    else:
        print("\n❌ Произошли ошибки при добавлении результатов") 