#!/usr/bin/env python3
"""
Скрипт для инициализации базы данных с полями для эмбеддингов.
Добавляет необходимые колонки в таблицу reviews.
"""

import sqlite3
import os
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def init_embeddings_table(db_path: str = "urban_analysis_fixed.db"):
    """
    Инициализирует таблицу reviews, добавляя поля для эмбеддингов.
    
    Args:
        db_path: Путь к базе данных
    """
    try:
        if not os.path.exists(db_path):
            logger.error(f"❌ База данных {db_path} не найдена")
            return False
        
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            
            # Проверяем существование полей для эмбеддингов
            cursor.execute("PRAGMA table_info(reviews)")
            columns = [column[1] for column in cursor.fetchall()]
            
            logger.info(f"📋 Текущие колонки в таблице reviews: {columns}")
            
            # Добавляем поле для эмбеддинга отзыва, если его нет
            if 'review_embedding' not in columns:
                cursor.execute("ALTER TABLE reviews ADD COLUMN review_embedding TEXT")
                logger.info("✅ Добавлено поле review_embedding в таблицу reviews")
            else:
                logger.info("⏭️ Поле review_embedding уже существует")
            
            # Добавляем поле для эмбеддинга объекта, если его нет
            if 'object_embedding' not in columns:
                cursor.execute("ALTER TABLE reviews ADD COLUMN object_embedding TEXT")
                logger.info("✅ Добавлено поле object_embedding в таблицу reviews")
            else:
                logger.info("⏭️ Поле object_embedding уже существует")
            
            conn.commit()
            
            # Проверяем результат
            cursor.execute("PRAGMA table_info(reviews)")
            updated_columns = [column[1] for column in cursor.fetchall()]
            logger.info(f"📋 Обновленные колонки: {updated_columns}")
            
            logger.info("✅ Таблица reviews готова для работы с эмбеддингами")
            return True
            
    except Exception as e:
        logger.error(f"❌ Ошибка при инициализации таблицы: {str(e)}")
        return False

def check_database_structure(db_path: str = "urban_analysis_fixed.db"):
    """
    Проверяет структуру базы данных.
    
    Args:
        db_path: Путь к базе данных
    """
    try:
        if not os.path.exists(db_path):
            logger.error(f"❌ База данных {db_path} не найдена")
            return
        
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            
            # Получаем список всех таблиц
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [table[0] for table in cursor.fetchall()]
            
            logger.info(f"📊 Таблицы в базе данных: {tables}")
            
            # Проверяем структуру таблицы reviews
            if 'reviews' in tables:
                cursor.execute("PRAGMA table_info(reviews)")
                columns = cursor.fetchall()
                
                logger.info("📋 Структура таблицы reviews:")
                for col in columns:
                    col_id, name, type_name, not_null, default_value, pk = col
                    logger.info(f"  {name} ({type_name}){' NOT NULL' if not_null else ''}{' PRIMARY KEY' if pk else ''}")
            else:
                logger.warning("⚠️ Таблица reviews не найдена")
            
            # Проверяем количество записей
            if 'reviews' in tables:
                cursor.execute("SELECT COUNT(*) FROM reviews")
                reviews_count = cursor.fetchone()[0]
                logger.info(f"📊 Количество отзывов: {reviews_count}")
                
                if reviews_count > 0:
                    cursor.execute("SELECT COUNT(*) FROM reviews WHERE review_embedding IS NOT NULL")
                    reviews_with_embeddings = cursor.fetchone()[0]
                    logger.info(f"📊 Отзывов с эмбеддингами: {reviews_with_embeddings}")
                    
                    cursor.execute("SELECT COUNT(*) FROM reviews WHERE object_embedding IS NOT NULL")
                    reviews_with_object_embeddings = cursor.fetchone()[0]
                    logger.info(f"📊 Отзывов с эмбеддингами объектов: {reviews_with_object_embeddings}")
            
    except Exception as e:
        logger.error(f"❌ Ошибка при проверке структуры базы данных: {str(e)}")

def main():
    """Основная функция скрипта."""
    logger.info("🚀 Инициализация базы данных для эмбеддингов")
    
    # Инициализируем таблицу
    if init_embeddings_table():
        logger.info("✅ Инициализация завершена успешно")
        
        # Проверяем структуру
        check_database_structure()
    else:
        logger.error("❌ Инициализация не удалась")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
