#!/usr/bin/env python3
"""
Скрипт для инициализации таблицы master_ratings в базе данных
"""

import sqlite3
import os
from pathlib import Path

def init_master_ratings_table():
    """Инициализация таблицы master_ratings"""
    
    db_path = "urban_analysis_fixed.db"
    
    if not os.path.exists(db_path):
        print(f"❌ База данных {db_path} не найдена")
        return False
    
    try:
        # Подключаемся к БД
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Проверяем, существует ли таблица
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='master_ratings'
        """)
        
        if cursor.fetchone():
            print("✅ Таблица master_ratings уже существует")
            return True
        
        # Создаем таблицу master_ratings
        cursor.execute("""
            CREATE TABLE master_ratings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                review_id INTEGER NOT NULL,
                sentiment TEXT NOT NULL CHECK (sentiment IN ('positive', 'negative', 'neutral')),
                rated_by TEXT DEFAULT 'user',
                rated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (review_id) REFERENCES reviews(id),
                UNIQUE(review_id)
            )
        """)
        
        # Создаем индекс для производительности
        cursor.execute("""
            CREATE INDEX idx_master_ratings_review_id ON master_ratings(review_id)
        """)
        
        conn.commit()
        print("✅ Таблица master_ratings успешно создана")
        
        # Проверяем количество отзывов в БД
        cursor.execute("SELECT COUNT(*) FROM reviews")
        reviews_count = cursor.fetchone()[0]
        print(f"📊 Всего отзывов в БД: {reviews_count}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Ошибка при создании таблицы master_ratings: {e}")
        return False

if __name__ == "__main__":
    print("=== ИНИЦИАЛИЗАЦИЯ ТАБЛИЦЫ MASTER_RATINGS ===")
    
    success = init_master_ratings_table()
    
    if success:
        print("\n✅ Инициализация завершена успешно!")
        print("Теперь можно использовать мастер-рейтинги в приложении")
    else:
        print("\n❌ Инициализация завершена с ошибками") 