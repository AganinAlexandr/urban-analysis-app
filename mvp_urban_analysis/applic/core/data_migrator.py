"""
Модуль для миграции данных из CSV в базу данных
"""
import pandas as pd
from typing import Dict, List, Optional
from pathlib import Path
import logging
from .database import db_manager

logger = logging.getLogger(__name__)


class DataMigrator:
    """Класс для миграции данных из различных форматов в базу данных"""
    
    def __init__(self):
        self.db_manager = db_manager
    
    def migrate_csv_file(self, file_path: str, source: str = "csv") -> Dict[str, int]:
        """Миграция данных из CSV файла в базу данных"""
        try:
            # Читаем CSV файл
            df = pd.read_csv(file_path, encoding='utf-8')
            logger.info(f"Загружен CSV файл: {len(df)} записей")
            
            # Выполняем миграцию
            stats = self.db_manager.migrate_csv_to_database(df, source)
            
            logger.info(f"Миграция завершена: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Ошибка при миграции CSV файла: {e}")
            raise
    
    def migrate_excel_file(self, file_path: str, sheet_name: str = None, source: str = "excel") -> Dict[str, int]:
        """Миграция данных из Excel файла в базу данных"""
        try:
            # Читаем Excel файл
            if sheet_name:
                df = pd.read_excel(file_path, sheet_name=sheet_name)
            else:
                df = pd.read_excel(file_path)
            
            logger.info(f"Загружен Excel файл: {len(df)} записей")
            
            # Выполняем миграцию
            stats = self.db_manager.migrate_csv_to_database(df, source)
            
            logger.info(f"Миграция завершена: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Ошибка при миграции Excel файла: {e}")
            raise
    
    def migrate_json_file(self, file_path: str, source: str = "json") -> Dict[str, int]:
        """Миграция данных из JSON файла в базу данных"""
        try:
            # Читаем JSON файл
            df = pd.read_json(file_path)
            logger.info(f"Загружен JSON файл: {len(df)} записей")
            
            # Выполняем миграцию
            stats = self.db_manager.migrate_csv_to_database(df, source)
            
            logger.info(f"Миграция завершена: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Ошибка при миграции JSON файла: {e}")
            raise
    
    def migrate_directory(self, directory_path: str) -> Dict[str, Dict[str, int]]:
        """Миграция всех файлов из директории"""
        directory = Path(directory_path)
        results = {}
        
        # Обрабатываем CSV файлы
        for csv_file in directory.glob("*.csv"):
            try:
                source = f"csv_{csv_file.stem}"
                stats = self.migrate_csv_file(str(csv_file), source)
                results[str(csv_file)] = stats
            except Exception as e:
                logger.error(f"Ошибка при обработке {csv_file}: {e}")
                results[str(csv_file)] = {"error": str(e)}
        
        # Обрабатываем Excel файлы
        for excel_file in directory.glob("*.xlsx"):
            try:
                source = f"excel_{excel_file.stem}"
                stats = self.migrate_excel_file(str(excel_file), source=source)
                results[str(excel_file)] = stats
            except Exception as e:
                logger.error(f"Ошибка при обработке {excel_file}: {e}")
                results[str(excel_file)] = {"error": str(e)}
        
        # Обрабатываем JSON файлы
        for json_file in directory.glob("*.json"):
            try:
                source = f"json_{json_file.stem}"
                stats = self.migrate_json_file(str(json_file), source)
                results[str(json_file)] = stats
            except Exception as e:
                logger.error(f"Ошибка при обработке {json_file}: {e}")
                results[str(json_file)] = {"error": str(e)}
        
        return results
    
    def get_migration_summary(self) -> Dict[str, int]:
        """Получение сводки по миграции"""
        stats = self.db_manager.get_statistics()
        
        summary = {
            "total_objects": stats.get('objects_count', 0),
            "total_reviews": stats.get('reviews_count', 0),
            "total_analysis_results": stats.get('analysis_results_count', 0),
            "total_methods": stats.get('methods_count', 0)
        }
        
        return summary
    
    def export_migrated_data(self, output_path: str, include_analysis: bool = True):
        """Экспорт мигрированных данных в CSV"""
        try:
            df = self.db_manager.export_to_dataframe(include_analysis)
            df.to_csv(output_path, index=False, encoding='utf-8')
            logger.info(f"Данные экспортированы в {output_path}: {len(df)} записей")
        except Exception as e:
            logger.error(f"Ошибка при экспорте данных: {e}")
            raise
    
    def validate_migration(self) -> Dict[str, bool]:
        """Валидация миграции данных"""
        validation_results = {}
        
        try:
            stats = self.db_manager.get_statistics()
            
            # Проверяем наличие данных
            validation_results['has_objects'] = stats.get('objects_count', 0) > 0
            validation_results['has_reviews'] = stats.get('reviews_count', 0) > 0
            validation_results['has_analysis'] = stats.get('analysis_results_count', 0) > 0
            
            # Проверяем целостность связей
            with self.db_manager.get_connection() as conn:
                # Проверяем, что все отзывы связаны с объектами
                cursor = conn.execute("""
                    SELECT COUNT(*) as count 
                    FROM reviews r 
                    LEFT JOIN objects o ON r.object_id = o.id 
                    WHERE o.id IS NULL
                """)
                orphan_reviews = cursor.fetchone()['count']
                validation_results['no_orphan_reviews'] = orphan_reviews == 0
                
                # Проверяем, что все результаты анализа связаны с отзывами
                cursor = conn.execute("""
                    SELECT COUNT(*) as count 
                    FROM analysis_results ar 
                    LEFT JOIN reviews r ON ar.review_id = r.id 
                    WHERE r.id IS NULL
                """)
                orphan_analysis = cursor.fetchone()['count']
                validation_results['no_orphan_analysis'] = orphan_analysis == 0
            
        except Exception as e:
            logger.error(f"Ошибка при валидации: {e}")
            validation_results['validation_error'] = True
        
        return validation_results


# Синглтон для глобального доступа
data_migrator = DataMigrator() 