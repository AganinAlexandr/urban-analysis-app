"""
Модуль для очистки дублирующихся данных в базе данных
"""

import sqlite3
import logging
from typing import Dict, List, Tuple, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class DatabaseCleaner:
    """
    Класс для очистки дублирующихся данных в базе данных
    """
    
    def __init__(self, db_path: str = 'urban_analysis_fixed.db'):
        """
        Инициализация очистителя базы данных
        
        Args:
            db_path: Путь к файлу базы данных
        """
        self.db_path = db_path
        self.connection = None
        
    def connect(self) -> None:
        """Установка соединения с базой данных"""
        try:
            self.connection = sqlite3.connect(self.db_path)
            self.connection.row_factory = sqlite3.Row
            logger.info(f"Подключение к БД {self.db_path} установлено")
        except Exception as e:
            logger.error(f"Ошибка подключения к БД: {e}")
            raise
    
    def disconnect(self) -> None:
        """Закрытие соединения с базой данных"""
        if self.connection:
            self.connection.close()
            logger.info("Соединение с БД закрыто")
    
    def get_duplicate_groups(self) -> List[Dict]:
        """
        Поиск дублирующихся групп объектов
        
        Returns:
            Список дублирующихся групп
        """
        query = """
        SELECT group_name, group_type, COUNT(*) as count
        FROM object_groups
        GROUP BY group_name, group_type
        HAVING COUNT(*) > 1
        ORDER BY group_name, group_type
        """
        
        cursor = self.connection.cursor()
        cursor.execute(query)
        duplicates = cursor.fetchall()
        
        logger.info(f"Найдено {len(duplicates)} типов дублирующихся групп")
        return [dict(row) for row in duplicates]
    
    def get_duplicate_objects(self) -> List[Dict]:
        """
        Поиск дублирующихся объектов
        
        Returns:
            Список дублирующихся объектов
        """
        query = """
        SELECT name, address, object_key, COUNT(*) as count
        FROM objects
        GROUP BY name, address, object_key
        HAVING COUNT(*) > 1
        ORDER BY name, address
        """
        
        cursor = self.connection.cursor()
        cursor.execute(query)
        duplicates = cursor.fetchall()
        
        logger.info(f"Найдено {len(duplicates)} типов дублирующихся объектов")
        return [dict(row) for row in duplicates]
    
    def clean_duplicate_groups(self) -> Dict[str, int]:
        """
        Очистка дублирующихся групп объектов
        
        Returns:
            Статистика очистки
        """
        logger.info("Начинаем очистку дублирующихся групп...")
        
        # Получаем дублирующиеся группы
        duplicates = self.get_duplicate_groups()
        
        if not duplicates:
            logger.info("Дублирующихся групп не найдено")
            return {"cleaned": 0, "kept": 0}
        
        cleaned_count = 0
        kept_count = 0
        
        for duplicate in duplicates:
            group_name = duplicate['group_name']
            group_type = duplicate['group_type']
            count = duplicate['count']
            
            logger.info(f"Обрабатываем группу '{group_name}' ({group_type}): {count} дубликатов")
            
            # Получаем все записи этой группы
            query = """
            SELECT id, created_at
            FROM object_groups
            WHERE group_name = ? AND group_type = ?
            ORDER BY created_at ASC
            """
            
            cursor = self.connection.cursor()
            cursor.execute(query, (group_name, group_type))
            records = cursor.fetchall()
            
            # Оставляем самую старую запись, удаляем остальные
            if len(records) > 1:
                keep_id = records[0]['id']
                delete_ids = [r['id'] for r in records[1:]]
                
                # Обновляем ссылки на удаляемые группы
                self._update_group_references(delete_ids, keep_id)
                
                # Удаляем дублирующиеся записи
                placeholders = ','.join(['?' for _ in delete_ids])
                delete_query = f"DELETE FROM object_groups WHERE id IN ({placeholders})"
                cursor.execute(delete_query, delete_ids)
                
                cleaned_count += len(delete_ids)
                kept_count += 1
                
                logger.info(f"Удалено {len(delete_ids)} дубликатов группы '{group_name}'")
        
        self.connection.commit()
        logger.info(f"Очистка групп завершена: удалено {cleaned_count}, оставлено {kept_count}")
        
        return {"cleaned": cleaned_count, "kept": kept_count}
    
    def clean_duplicate_objects(self) -> Dict[str, int]:
        """
        Очистка дублирующихся объектов
        
        Returns:
            Статистика очистки
        """
        logger.info("Начинаем очистку дублирующихся объектов...")
        
        # Получаем дублирующиеся объекты
        duplicates = self.get_duplicate_objects()
        
        if not duplicates:
            logger.info("Дублирующихся объектов не найдено")
            return {"cleaned": 0, "kept": 0}
        
        cleaned_count = 0
        kept_count = 0
        
        for duplicate in duplicates:
            name = duplicate['name']
            address = duplicate['address']
            object_key = duplicate['object_key']
            count = duplicate['count']
            
            logger.info(f"Обрабатываем объект '{name}' ({address}): {count} дубликатов")
            
            # Получаем все записи этого объекта
            query = """
            SELECT id, created_at
            FROM objects
            WHERE name = ? AND address = ? AND object_key = ?
            ORDER BY created_at ASC
            """
            
            cursor = self.connection.cursor()
            cursor.execute(query, (name, address, object_key))
            records = cursor.fetchall()
            
            # Оставляем самую старую запись, удаляем остальные
            if len(records) > 1:
                keep_id = records[0]['id']
                delete_ids = [r['id'] for r in records[1:]]
                
                # Обновляем ссылки на удаляемые объекты
                self._update_object_references(delete_ids, keep_id)
                
                # Удаляем дублирующиеся записи
                placeholders = ','.join(['?' for _ in delete_ids])
                delete_query = f"DELETE FROM objects WHERE id IN ({placeholders})"
                cursor.execute(delete_query, delete_ids)
                
                cleaned_count += len(delete_ids)
                kept_count += 1
                
                logger.info(f"Удалено {len(delete_ids)} дубликатов объекта '{name}'")
        
        self.connection.commit()
        logger.info(f"Очистка объектов завершена: удалено {cleaned_count}, оставлено {kept_count}")
        
        return {"cleaned": cleaned_count, "kept": kept_count}
    
    def _update_group_references(self, old_ids: List[int], new_id: int) -> None:
        """
        Обновление ссылок на группы объектов
        
        Args:
            old_ids: ID удаляемых групп
            new_id: ID группы, которую оставляем
        """
        # Обновляем ссылки в таблице objects
        placeholders = ','.join(['?' for _ in old_ids])
        update_query = f"""
        UPDATE objects 
        SET group_id = ? 
        WHERE group_id IN ({placeholders})
        """
        
        cursor = self.connection.cursor()
        cursor.execute(update_query, [new_id] + old_ids)
        
        logger.info(f"Обновлено {cursor.rowcount} ссылок на группы")
    
    def _update_object_references(self, old_ids: List[int], new_id: int) -> None:
        """
        Обновление ссылок на объекты
        
        Args:
            old_ids: ID удаляемых объектов
            new_id: ID объекта, который оставляем
        """
        # Обновляем ссылки в таблице reviews
        placeholders = ','.join(['?' for _ in old_ids])
        update_query = f"""
        UPDATE reviews 
        SET object_id = ? 
        WHERE object_id IN ({placeholders})
        """
        
        cursor = self.connection.cursor()
        cursor.execute(update_query, [new_id] + old_ids)
        
        logger.info(f"Обновлено {cursor.rowcount} ссылок на объекты")
    
    def create_unique_indexes(self) -> None:
        """Создание уникальных индексов для предотвращения дублирования"""
        logger.info("Создание уникальных индексов...")
        
        indexes = [
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_object_groups_name_type ON object_groups(group_name, group_type)",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_objects_key ON objects(object_key)",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_objects_name_address ON objects(name, address)",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_processing_methods_name ON processing_methods(method_name)",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_analysis_results_review_method ON analysis_results(review_id, method_id)"
        ]
        
        cursor = self.connection.cursor()
        for index_query in indexes:
            try:
                cursor.execute(index_query)
                logger.info(f"Создан индекс: {index_query.split('IF NOT EXISTS ')[1].split(' ON ')[0]}")
            except Exception as e:
                logger.warning(f"Ошибка создания индекса: {e}")
        
        self.connection.commit()
        logger.info("Создание индексов завершено")
    
    def get_database_stats(self) -> Dict[str, int]:
        """
        Получение статистики базы данных
        
        Returns:
            Словарь со статистикой
        """
        stats = {}
        
        tables = ['objects', 'reviews', 'analysis_results', 'processing_methods', 
                 'object_groups', 'detected_groups']
        
        cursor = self.connection.cursor()
        
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                stats[f"{table}_count"] = count
            except Exception as e:
                logger.warning(f"Не удалось получить статистику для таблицы {table}: {e}")
                stats[f"{table}_count"] = 0
        
        return stats
    
    def clean_all_duplicates(self) -> Dict[str, Dict[str, int]]:
        """
        Полная очистка всех дублирующихся данных
        
        Returns:
            Статистика очистки по типам данных
        """
        logger.info("=== НАЧАЛО ПОЛНОЙ ОЧИСТКИ БАЗЫ ДАННЫХ ===")
        
        try:
            self.connect()
            
            # Получаем статистику до очистки
            stats_before = self.get_database_stats()
            logger.info(f"Статистика до очистки: {stats_before}")
            
            # Очищаем дублирующиеся группы
            groups_cleanup = self.clean_duplicate_groups()
            
            # Очищаем дублирующиеся объекты
            objects_cleanup = self.clean_duplicate_objects()
            
            # Создаем уникальные индексы
            self.create_unique_indexes()
            
            # Получаем статистику после очистки
            stats_after = self.get_database_stats()
            logger.info(f"Статистика после очистки: {stats_after}")
            
            result = {
                "groups_cleanup": groups_cleanup,
                "objects_cleanup": objects_cleanup,
                "stats_before": stats_before,
                "stats_after": stats_after
            }
            
            logger.info("=== ОЧИСТКА БАЗЫ ДАННЫХ ЗАВЕРШЕНА ===")
            return result
            
        except Exception as e:
            logger.error(f"Ошибка при очистке БД: {e}")
            raise
        finally:
            self.disconnect()


def main():
    """Основная функция для запуска очистки"""
    import sys
    
    if len(sys.argv) > 1:
        db_path = sys.argv[1]
    else:
        db_path = 'urban_analysis_fixed.db'
    
    cleaner = DatabaseCleaner(db_path)
    
    try:
        result = cleaner.clean_all_duplicates()
        
        print("\n=== РЕЗУЛЬТАТЫ ОЧИСТКИ ===")
        print(f"Очистка групп: удалено {result['groups_cleanup']['cleaned']}, оставлено {result['groups_cleanup']['kept']}")
        print(f"Очистка объектов: удалено {result['objects_cleanup']['cleaned']}, оставлено {result['objects_cleanup']['kept']}")
        
        print("\n=== СТАТИСТИКА ===")
        print("До очистки:")
        for key, value in result['stats_before'].items():
            print(f"  {key}: {value}")
        
        print("\nПосле очистки:")
        for key, value in result['stats_after'].items():
            print(f"  {key}: {value}")
            
    except Exception as e:
        print(f"Ошибка: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 