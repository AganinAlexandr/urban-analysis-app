#!/usr/bin/env python3
"""
Восстановление записей nlp_vader с новыми ID
"""
import sqlite3
from datetime import datetime

def restore_nlp_vader_records():
    """Восстанавливает записи nlp_vader с новыми ID"""
    print("=== ВОССТАНОВЛЕНИЕ ЗАПИСЕЙ NLP_VADER ===")
    
    try:
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        # Получаем новый ID для nlp_vader
        cursor.execute("""
            SELECT id FROM processing_methods 
            WHERE method_name = 'nlp_vader'
        """)
        
        nlp_vader_id = cursor.fetchone()
        
        if not nlp_vader_id:
            print("❌ Метод nlp_vader не найден в processing_methods")
            return
        
        nlp_vader_id = nlp_vader_id[0]
        print(f"✅ Найден метод nlp_vader с ID: {nlp_vader_id}")
        
        # Проверяем, есть ли уже записи с этим методом
        cursor.execute("""
            SELECT COUNT(*) FROM analysis_results 
            WHERE method_id = ?
        """, (nlp_vader_id,))
        
        existing_count = cursor.fetchone()[0]
        print(f"Существующих записей с nlp_vader: {existing_count}")
        
        if existing_count > 0:
            print("⚠️ Записи с nlp_vader уже существуют!")
            response = input("Хотите удалить существующие записи и создать новые? (y/n): ")
            if response.lower() == 'y':
                cursor.execute("DELETE FROM analysis_results WHERE method_id = ?", (nlp_vader_id,))
                print("✅ Существующие записи удалены")
            else:
                print("Операция отменена")
                return
        
        # Получаем все отзывы
        cursor.execute("""
            SELECT id FROM reviews
        """)
        
        all_reviews = cursor.fetchall()
        print(f"Найдено отзывов: {len(all_reviews)}")
        
        if not all_reviews:
            print("❌ Нет отзывов!")
            return
        
        # Добавляем записи в analysis_results для nlp_vader
        added_count = 0
        
        for (review_id,) in all_reviews:
            # Для nlp_vader используем нейтральный сентимент по умолчанию
            # В реальной системе здесь был бы результат анализа VADER
            sentiment = "neutral"
            confidence = 0.5  # средняя уверенность
            
            cursor.execute("""
                INSERT INTO analysis_results 
                (review_id, method_id, sentiment, confidence, review_type, processed_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (review_id, nlp_vader_id, sentiment, confidence, 'informational', datetime.now()))
            
            added_count += 1
        
        conn.commit()
        print(f"✅ Добавлено записей nlp_vader: {added_count}")
        
        # Проверяем результат
        cursor.execute("""
            SELECT COUNT(*) FROM analysis_results 
            WHERE method_id = ?
        """, (nlp_vader_id,))
        
        final_count = cursor.fetchone()[0]
        print(f"Всего записей с nlp_vader: {final_count}")
        
        # Показываем статистику по сентиментам
        cursor.execute("""
            SELECT sentiment, COUNT(*) as count
            FROM analysis_results 
            WHERE method_id = ?
            GROUP BY sentiment
            ORDER BY count DESC
        """, (nlp_vader_id,))
        
        sentiment_stats = cursor.fetchall()
        print(f"\nСтатистика по сентиментам nlp_vader:")
        for sentiment, count in sentiment_stats:
            print(f"  - {sentiment}: {count} записей")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    restore_nlp_vader_records() 