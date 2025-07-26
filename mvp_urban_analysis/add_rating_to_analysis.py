#!/usr/bin/env python3
"""
Добавление записей рейтинга в таблицу analysis_results
"""
import sys
import os
import sqlite3
from datetime import datetime

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def convert_rating_to_sentiment(rating):
    """Конвертирует рейтинг в сентимент по схеме:
    5 - "positive"
    3 и 4 - "neutral" 
    1 и 2 - "negative"
    """
    if rating == 5:
        return "positive"
    elif rating in [3, 4]:
        return "neutral"
    elif rating in [1, 2]:
        return "negative"
    else:
        return "neutral"  # для неизвестных значений

def add_rating_to_analysis():
    """Добавляет записи рейтинга в таблицу analysis_results"""
    print("=== ДОБАВЛЕНИЕ ЗАПИСЕЙ РЕЙТИНГА В ANALYSIS_RESULTS ===")
    
    try:
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        # Получаем ID метода user_rating
        cursor.execute("""
            SELECT id FROM processing_methods 
            WHERE method_name = 'user_rating'
        """)
        
        user_rating_id = cursor.fetchone()
        
        if not user_rating_id:
            print("❌ Метод user_rating не найден в processing_methods")
            return
        
        user_rating_id = user_rating_id[0]
        print(f"✅ Найден метод user_rating с ID: {user_rating_id}")
        
        # Проверяем, есть ли уже записи с этим методом
        cursor.execute("""
            SELECT COUNT(*) FROM analysis_results 
            WHERE method_id = ?
        """, (user_rating_id,))
        
        existing_count = cursor.fetchone()[0]
        print(f"Существующих записей с user_rating: {existing_count}")
        
        if existing_count > 0:
            print("⚠️ Записи с user_rating уже существуют!")
            response = input("Хотите удалить существующие записи и создать новые? (y/n): ")
            if response.lower() == 'y':
                cursor.execute("DELETE FROM analysis_results WHERE method_id = ?", (user_rating_id,))
                print("✅ Существующие записи удалены")
            else:
                print("Операция отменена")
                return
        
        # Получаем все отзывы с рейтингом
        cursor.execute("""
            SELECT id, rating 
            FROM reviews 
            WHERE rating IS NOT NULL 
            AND rating BETWEEN 1 AND 5
        """)
        
        reviews_with_rating = cursor.fetchall()
        print(f"Найдено отзывов с рейтингом: {len(reviews_with_rating)}")
        
        if not reviews_with_rating:
            print("❌ Нет отзывов с рейтингом!")
            return
        
        # Добавляем записи в analysis_results
        added_count = 0
        
        for review_id, rating in reviews_with_rating:
            sentiment = convert_rating_to_sentiment(rating)
            
            # Рассчитываем уверенность на основе рейтинга
            if rating == 5:
                confidence = 1.0  # максимальная уверенность для отличного рейтинга
            elif rating in [1, 2]:
                confidence = 0.8  # высокая уверенность для плохого рейтинга
            elif rating in [3, 4]:
                confidence = 0.6  # средняя уверенность для нейтрального рейтинга
            else:
                confidence = 0.5  # низкая уверенность для неизвестных значений
            
            cursor.execute("""
                INSERT INTO analysis_results 
                (review_id, method_id, sentiment, confidence, review_type, processed_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (review_id, user_rating_id, sentiment, confidence, 'informational', datetime.now()))
            
            added_count += 1
        
        conn.commit()
        print(f"✅ Добавлено записей: {added_count}")
        
        # Проверяем результат
        cursor.execute("""
            SELECT COUNT(*) FROM analysis_results 
            WHERE method_id = ?
        """, (user_rating_id,))
        
        final_count = cursor.fetchone()[0]
        print(f"Всего записей с user_rating: {final_count}")
        
        # Показываем статистику по сентиментам
        cursor.execute("""
            SELECT sentiment, COUNT(*) as count
            FROM analysis_results 
            WHERE method_id = ?
            GROUP BY sentiment
            ORDER BY count DESC
        """, (user_rating_id,))
        
        sentiment_stats = cursor.fetchall()
        print(f"\nСтатистика по сентиментам:")
        for sentiment, count in sentiment_stats:
            print(f"  - {sentiment}: {count} записей")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    add_rating_to_analysis() 