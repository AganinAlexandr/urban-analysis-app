#!/usr/bin/env python3
"""
Скрипт для инициализации групп в единственном числе
"""

import sqlite3

def init_groups():
    """Инициализирует группы в единственном числе"""
    
    try:
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        print("🏗️ Инициализация групп...")
        
        # Группы в единственном числе
        groups = [
            'hospital',
            'school', 
            'kindergarten',
            'polyclinic',
            'pharmacy',
            'shopping_mall',
            'university'
        ]
        
        # Вставляем группы
        for group in groups:
            cursor.execute("""
                INSERT INTO object_groups (group_type, group_name) 
                VALUES (?, ?)
            """, (group, group))
            print(f"✅ Добавлена группа: {group}")
        
        conn.commit()
        print("✅ Группы инициализированы успешно!")
        
        # Проверяем результат
        cursor.execute("SELECT id, group_type FROM object_groups ORDER BY id")
        results = cursor.fetchall()
        print("📋 Созданные группы:")
        for row in results:
            print(f"  ID: {row[0]}, Group: {row[1]}")
        
    except Exception as e:
        print(f"❌ Ошибка при инициализации групп: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    init_groups() 