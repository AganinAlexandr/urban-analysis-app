"""
Проверка структуры таблицы processing_methods
"""

import sqlite3
import os

def check_table_structure():
    """Проверка структуры таблицы processing_methods"""
    print("=== ПРОВЕРКА СТРУКТУРЫ ТАБЛИЦЫ PROCESSING_METHODS ===")
    
    db_path = 'urban_analysis_fixed.db'
    
    if not os.path.exists(db_path):
        print(f"❌ База данных {db_path} не найдена")
        return
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Получаем информацию о структуре таблицы
        cursor.execute("PRAGMA table_info(processing_methods)")
        columns = cursor.fetchall()
        
        print(f"Структура таблицы processing_methods:")
        for col in columns:
            print(f"  {col[1]} ({col[2]}) - {'NOT NULL' if col[3] else 'NULL'} - {col[4] if col[4] else 'No default'}")
        
        # Показываем существующие записи
        cursor.execute("SELECT * FROM processing_methods LIMIT 5")
        records = cursor.fetchall()
        
        print(f"\nСуществующие записи (первые 5):")
        for i, record in enumerate(records, 1):
            print(f"  {i}. {record}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    check_table_structure() 