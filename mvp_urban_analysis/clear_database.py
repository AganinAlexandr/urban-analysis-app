#!/usr/bin/env python3
"""
Скрипт для очистки базы данных, сохраняя только master_ratings
"""

import sqlite3
import os

def clear_database():
    """Очищает все таблицы кроме master_ratings"""
    
    db_path = 'urban_analysis_fixed.db'
    
    if not os.path.exists(db_path):
        print(f"❌ База данных {db_path} не найдена")
        return
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        print("🧹 Очистка базы данных...")
        
        # Список таблиц для очистки (кроме master_ratings)
        tables_to_clear = [
            'objects',
            'object_groups', 
            'detected_groups',
            'sentiment_results',
            'correlation_data'
        ]
        
        for table in tables_to_clear:
            try:
                cursor.execute(f"DELETE FROM {table}")
                deleted_count = cursor.rowcount
                print(f"✅ Очищена таблица {table}: {deleted_count} записей")
            except sqlite3.OperationalError as e:
                print(f"⚠️ Таблица {table} не существует или не может быть очищена: {e}")
        
        # Сброс автоинкремента для таблиц
        for table in tables_to_clear:
            try:
                cursor.execute(f"DELETE FROM sqlite_sequence WHERE name = '{table}'")
                print(f"🔄 Сброшен автоинкремент для {table}")
            except:
                pass
        
        conn.commit()
        print("✅ База данных очищена успешно!")
        
        # Проверяем, что master_ratings остались
        cursor.execute("SELECT COUNT(*) FROM master_ratings")
        master_count = cursor.fetchone()[0]
        print(f"📊 Записей в master_ratings сохранено: {master_count}")
        
    except Exception as e:
        print(f"❌ Ошибка при очистке базы данных: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    clear_database() 