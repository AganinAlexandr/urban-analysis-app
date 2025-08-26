#!/usr/bin/env python3
"""
Поиск таблицы ключевых слов в базе данных
"""

import sqlite3
import os
import glob

def find_keywords_table():
    """Ищет таблицу ключевых слов в базе данных"""
    
    # Ищем все файлы баз данных
    db_files = glob.glob("*.db") + glob.glob("*.sqlite") + glob.glob("*.sqlite3")
    
    print("=== ПОИСК ТАБЛИЦЫ КЛЮЧЕВЫХ СЛОВ ===")
    print(f"Найдено файлов БД: {len(db_files)}")
    
    for db_file in db_files:
        print(f"\n🔍 Проверяем файл: {db_file}")
        
        try:
            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()
            
            # Получаем список всех таблиц
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            
            print(f"  📋 Таблицы в БД: {', '.join(tables)}")
            
            # Ищем таблицы, содержащие 'keyword' в названии
            keyword_tables = [table for table in tables if 'keyword' in table.lower()]
            if keyword_tables:
                print(f"  🎯 Найдены таблицы с ключевыми словами: {', '.join(keyword_tables)}")
                
                # Проверяем содержимое каждой таблицы с ключевыми словами
                for table in keyword_tables:
                    try:
                        cursor.execute(f"SELECT COUNT(*) FROM {table}")
                        count = cursor.fetchone()[0]
                        print(f"    📊 Таблица '{table}': {count} записей")
                        
                        # Показываем структуру таблицы
                        cursor.execute(f"PRAGMA table_info({table})")
                        columns = cursor.fetchall()
                        print(f"      Структура: {[col[1] for col in columns]}")
                        
                        # Показываем несколько примеров
                        if count > 0:
                            cursor.execute(f"SELECT * FROM {table} LIMIT 3")
                            examples = cursor.fetchall()
                            print(f"      Примеры записей:")
                            for i, example in enumerate(examples):
                                print(f"        {i+1}. {example}")
                        
                    except Exception as e:
                        print(f"      ❌ Ошибка чтения таблицы '{table}': {e}")
            
            # Специально ищем таблицу initial_keywords
            if 'initial_keywords' in tables:
                print(f"  ✅ Найдена таблица 'initial_keywords'!")
                cursor.execute("SELECT COUNT(*) FROM initial_keywords")
                count = cursor.fetchone()[0]
                print(f"    📊 Записей в initial_keywords: {count}")
                
                if count > 0:
                    # Показываем примеры ключевых слов
                    cursor.execute("""
                        SELECT group_type, keyword_type, keyword, weight
                        FROM initial_keywords
                        LIMIT 10
                    """)
                    examples = cursor.fetchall()
                    print(f"    📝 Примеры ключевых слов:")
                    for example in examples:
                        print(f"      {example[0]}.{example[1]}: '{example[2]}' (вес: {example[3]})")
            
            conn.close()
            
        except Exception as e:
            print(f"  ❌ Ошибка чтения БД {db_file}: {e}")
    
    print(f"\n=== ПОИСК ЗАВЕРШЕН ===")

if __name__ == "__main__":
    find_keywords_table()




