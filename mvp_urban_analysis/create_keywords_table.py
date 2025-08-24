#!/usr/bin/env python3
"""
Создание таблицы начальных ключевых слов
"""

from initial_keywords_system import create_initial_keywords_table

def main():
    print("=== СОЗДАНИЕ ТАБЛИЦЫ КЛЮЧЕВЫХ СЛОВ ===")
    
    if create_initial_keywords_table():
        print("✅ Таблица ключевых слов успешно создана!")
        
        # Проверяем, что таблица создалась
        import sqlite3
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM initial_keywords")
        count = cursor.fetchone()[0]
        print(f"📊 Добавлено ключевых слов: {count}")
        
        # Показываем примеры по группам
        cursor.execute("""
            SELECT group_type, COUNT(*) 
            FROM initial_keywords 
            GROUP BY group_type 
            ORDER BY group_type
        """)
        
        print("\n📁 Распределение по группам:")
        for group, count in cursor.fetchall():
            print(f"  {group}: {count} ключевых слов")
        
        conn.close()
        
    else:
        print("❌ Ошибка создания таблицы ключевых слов")

if __name__ == "__main__":
    main()



