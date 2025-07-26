"""
Скрипт для миграции базы данных - добавление поля в_Выборке
"""
import sqlite3
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def migrate_add_sample_field():
    """
    Добавляет поле в_Выборке в таблицу reviews
    """
    db_path = "urban_analysis_fixed.db"
    
    try:
        with sqlite3.connect(db_path) as conn:
            # Проверяем, существует ли уже поле в_Выборке
            cursor = conn.execute("PRAGMA table_info(reviews)")
            columns = [column[1] for column in cursor.fetchall()]
            
            if 'в_Выборке' in columns:
                logger.info("Поле в_Выборке уже существует в таблице reviews")
                return True
            
            # Добавляем поле в_Выборке
            logger.info("Добавляем поле в_Выборке в таблицу reviews...")
            conn.execute("ALTER TABLE reviews ADD COLUMN в_Выборке TEXT DEFAULT NULL")
            
            logger.info("Поле в_Выборке успешно добавлено")
            return True
            
    except Exception as e:
        logger.error(f"Ошибка при добавлении поля в_Выборке: {e}")
        return False

def verify_migration():
    """
    Проверяет успешность миграции
    """
    db_path = "urban_analysis_fixed.db"
    
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.execute("PRAGMA table_info(reviews)")
            columns = [column[1] for column in cursor.fetchall()]
            
            if 'в_Выборке' in columns:
                logger.info("✓ Миграция успешна: поле в_Выборке добавлено")
                return True
            else:
                logger.error("✗ Миграция не удалась: поле в_Выборке отсутствует")
                return False
                
    except Exception as e:
        logger.error(f"Ошибка при проверке миграции: {e}")
        return False

if __name__ == "__main__":
    logger.info("Начинаем миграцию базы данных...")
    
    if migrate_add_sample_field():
        if verify_migration():
            logger.info("Миграция завершена успешно!")
        else:
            logger.error("Миграция завершена с ошибками!")
    else:
        logger.error("Не удалось выполнить миграцию!") 