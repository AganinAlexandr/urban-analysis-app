#!/usr/bin/env python3
"""
Проверка всех таблиц в базе данных
"""
import sqlite3

def check_all_tables():
    """Проверяет все таблицы в базе данных"""
    print("=== ПРОВЕРКА ВСЕХ ТАБЛИЦ В БАЗЕ ДАННЫХ ===")
    
    try:
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        # 1. Получаем список всех таблиц
        print("1. Все таблицы в базе данных:")
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        for table in tables:
            table_name = table[0]
            print(f"   {table_name}")
            
            # Проверяем количество записей в каждой таблице
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            print(f"     Записей: {count}")
            
            # Проверяем структуру таблицы
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            print(f"     Колонки:")
            for col in columns:
                print(f"       {col[1]} ({col[2]})")
            print()
        
        # 2. Проверяем таблицы, которые могут содержать Master_Rating
        print("2. Поиск таблиц с Master_Rating:")
        
        # Ищем таблицы с названиями, содержащими "master", "rating", "sentiment"
        master_rating_tables = []
        for table in tables:
            table_name = table[0].lower()
            if any(keyword in table_name for keyword in ['master', 'rating', 'sentiment']):
                master_rating_tables.append(table[0])
        
        if master_rating_tables:
            print("   Найдены потенциальные таблицы:")
            for table_name in master_rating_tables:
                print(f"     {table_name}")
                
                # Проверяем данные в этих таблицах
                cursor.execute(f"SELECT * FROM {table_name} LIMIT 3")
                data = cursor.fetchall()
                print(f"       Примеры данных:")
                for i, row in enumerate(data, 1):
                    print(f"         {i}: {row}")
                print()
        else:
            print("   Таблицы с Master_Rating не найдены")
        
        # 3. Проверяем связи между таблицами
        print("3. Проверка связей между таблицами:")
        
        # Проверяем, есть ли внешние ключи
        cursor.execute("PRAGMA foreign_key_list(reviews)")
        foreign_keys = cursor.fetchall()
        
        if foreign_keys:
            print("   Внешние ключи для таблицы reviews:")
            for fk in foreign_keys:
                print(f"     {fk}")
        else:
            print("   Внешние ключи не найдены")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_all_tables() 