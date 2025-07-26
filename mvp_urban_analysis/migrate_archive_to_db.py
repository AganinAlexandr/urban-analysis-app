#!/usr/bin/env python3
"""
Миграция данных из архива в БД
"""
import pandas as pd
import os
from app.core.database_fixed import db_manager_fixed

def migrate_archive_to_db():
    """Мигрирует данные из архива в БД"""
    
    archive_path = "data/archives/processed_reviews.csv"
    
    if not os.path.exists(archive_path):
        print("Архивный файл не найден")
        return
    
    print("=== МИГРАЦИЯ ДАННЫХ ИЗ АРХИВА В БД ===")
    
    # Читаем архив
    df = pd.read_csv(archive_path, encoding='utf-8-sig')
    print(f"Загружено {len(df)} записей из архива")
    
    # Мигрируем в БД
    try:
        stats = db_manager_fixed.migrate_csv_to_database(df, source="archive")
        print(f"Миграция завершена:")
        print(f"  - Создано объектов: {stats['objects_created']}")
        print(f"  - Создано отзывов: {stats['reviews_created']}")
        print(f"  - Создано результатов анализа: {stats['analysis_results_created']}")
        
        # Проверяем результат
        df_db = db_manager_fixed.export_to_dataframe(include_analysis=True)
        print(f"Теперь в БД: {len(df_db)} записей")
        
    except Exception as e:
        print(f"Ошибка миграции: {e}")

if __name__ == "__main__":
    migrate_archive_to_db() 