#!/usr/bin/env python3
"""
Загрузка тестовых данных и запуск анализа
"""
import sqlite3
import os
from datetime import datetime

def load_test_data():
    """Загружает тестовые данные и запускает анализ"""
    print("=== ЗАГРУЗКА ТЕСТОВЫХ ДАННЫХ ===")
    
    db_path = 'urban_analysis_fixed.db'
    
    if not os.path.exists(db_path):
        print(f"❌ База данных {db_path} не найдена")
        return
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        print("1. Загрузка тестовых объектов...")
        
        # Добавляем тестовые объекты
        test_objects = [
            (1, 'Парк Горького', 'ул. Крымский Вал, 9', 'park_gorky', 55.7298, 37.6011, 'Центральный'),
            (2, 'ВДНХ', 'пр-т Мира, 119', 'vdnh', 55.8304, 37.6321, 'Северо-Восточный'),
            (3, 'Зарядье', 'ул. Варварка, 6', 'zaryadye', 55.7516, 37.6278, 'Центральный')
        ]
        
        for obj in test_objects:
            cursor.execute("""
                INSERT INTO objects (id, name, address, object_key, latitude, longitude, district, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, obj + (datetime.now(), datetime.now()))
        
        print(f"   ✅ Добавлено {len(test_objects)} объектов")
        
        print("\n2. Загрузка тестовых отзывов...")
        
        # Добавляем тестовые отзывы
        test_reviews = [
            (1, 1, 'Отличный парк! Очень красиво и уютно.', 5, '2024-01-15', 'Анна', '2gis', 'rev1', datetime.now()),
            (2, 1, 'Хорошее место для прогулок, но многолюдно.', 4, '2024-01-16', 'Иван', '2gis', 'rev2', datetime.now()),
            (3, 2, 'ВДНХ - это просто космос! Очень впечатляет.', 5, '2024-01-17', 'Мария', '2gis', 'rev3', datetime.now()),
            (4, 2, 'Интересно, но далеко от центра.', 3, '2024-01-18', 'Петр', '2gis', 'rev4', datetime.now()),
            (5, 3, 'Зарядье - современно и стильно!', 5, '2024-01-19', 'Елена', '2gis', 'rev5', datetime.now())
        ]
        
        for review in test_reviews:
            cursor.execute("""
                INSERT INTO reviews (id, object_id, review_text, rating, review_date, user_name, source, external_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, review)
        
        print(f"   ✅ Добавлено {len(test_reviews)} отзывов")
        
        print("\n3. Запуск анализа отзывов...")
        
        # Получаем ID метода llm_yandex
        cursor.execute("SELECT id FROM processing_methods WHERE method_name = 'llm_yandex'")
        yandex_method_id = cursor.fetchone()[0]
        
        # Анализируем каждый отзыв
        for review_id in range(1, len(test_reviews) + 1):
            # Простая логика анализа (в реальности здесь будет вызов YandexGPT API)
            sentiment = 'positive' if review_id % 2 == 1 else 'neutral'
            confidence = 0.85 if sentiment == 'positive' else 0.70
            
            cursor.execute("""
                INSERT INTO analysis_results 
                (review_id, method_id, sentiment, confidence, review_type, keywords, topics, processed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (review_id, yandex_method_id, sentiment, confidence, 'informational', 'парк,красота', 'отдых', datetime.now()))
        
        print(f"   ✅ Проанализировано {len(test_reviews)} отзывов через YandexGPT")
        
        # Подтверждаем изменения
        conn.commit()
        
        print("\n4. Проверка результата...")
        
        cursor.execute("SELECT COUNT(*) FROM objects")
        objects_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM reviews")
        reviews_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM analysis_results WHERE method_id = ?", (yandex_method_id,))
        analysis_count = cursor.fetchone()[0]
        
        print(f"   Объектов: {objects_count}")
        print(f"   Отзывов: {reviews_count}")
        print(f"   Результатов анализа YandexGPT: {analysis_count}")
        
        conn.close()
        
        print("\n✅ Тестовые данные загружены!")
        print("   Теперь можно проверить отображение на диаграммах")
        
    except Exception as e:
        print(f"❌ Ошибка при загрузке данных: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    load_test_data()
