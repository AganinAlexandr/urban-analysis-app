#!/usr/bin/env python3
"""
Проверка состояния БД и архива
"""
import sqlite3
import pandas as pd
import os

def check_data_sources():
    """Проверяет источники данных"""
    
    print("=== ПРОВЕРКА ИСТОЧНИКОВ ДАННЫХ ===")
    
    # Проверяем архивный файл
    archive_path = "data/archives/processed_reviews.csv"
    if os.path.exists(archive_path):
        df_archive = pd.read_csv(archive_path, encoding='utf-8-sig')
        print(f"Архивный файл: {len(df_archive)} записей")
        print(f"Колонки: {list(df_archive.columns)}")
    else:
        print("Архивный файл не найден")
    
    # Проверяем БД
    db_path = "urban_analysis_fixed.db"
    if os.path.exists(db_path):
        conn = sqlite3.connect(db_path)
        
        # Проверяем таблицы
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        print(f"\nТаблицы в БД: {tables}")
        
        # Проверяем количество записей
        for table in ['objects', 'reviews', 'object_groups', 'detected_groups']:
            if table in tables:
                cursor = conn.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"{table}: {count} записей")
        
        # Проверяем данные из БД
        try:
            from app.core.database_fixed import db_manager_fixed
            df_db = db_manager_fixed.export_to_dataframe(include_analysis=True)
            print(f"\nДанные из БД: {len(df_db)} записей")
            if not df_db.empty:
                print(f"Колонки БД: {list(df_db.columns)}")
        except Exception as e:
            print(f"Ошибка экспорта из БД: {e}")
        
        conn.close()
    else:
        print("Файл БД не найден")

if __name__ == "__main__":
    check_data_sources() 