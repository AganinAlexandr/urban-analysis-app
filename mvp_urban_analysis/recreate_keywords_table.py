#!/usr/bin/env python3
"""
Пересоздание таблицы ключевых слов с добавлением resident_complexes
"""
import sqlite3
from initial_keywords_system import create_initial_keywords_table

def recreate_keywords_table():
    """Пересоздает таблицу ключевых слов"""
    print("=== ПЕРЕСОЗДАНИЕ ТАБЛИЦЫ КЛЮЧЕВЫХ СЛОВ ===")
    
    try:
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        # Удаляем старую таблицу
        print("1. Удаляем старую таблицу initial_keywords...")
        cursor.execute("DROP TABLE IF EXISTS initial_keywords")
        
        # Создаем новую таблицу
        print("2. Создаем новую таблицу с обновленными ключевыми словами...")
        if create_initial_keywords_table():
            print("   ✅ Таблица успешно создана")
            
            # Проверяем количество записей
            cursor.execute("SELECT COUNT(*) FROM initial_keywords")
            count = cursor.fetchone()[0]
            print(f"   📊 Всего записей: {count}")
            
            # Проверяем группы
            cursor.execute("SELECT DISTINCT group_type FROM initial_keywords ORDER BY group_type")
            groups = cursor.fetchall()
            print("   📄 Группы в таблице:")
            for group in groups:
                cursor.execute("SELECT COUNT(*) FROM initial_keywords WHERE group_type = ?", (group[0],))
                group_count = cursor.fetchone()[0]
                print(f"     • {group[0]}: {group_count} ключевых слов")
                
        else:
            print("   ❌ Ошибка создания таблицы")
            
        conn.commit()
        conn.close()
        
        print("\n✅ Таблица ключевых слов пересоздана успешно!")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    recreate_keywords_table() 