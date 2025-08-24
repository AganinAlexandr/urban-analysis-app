#!/usr/bin/env python3
"""
Проверка групп в БД для диагностики проблемы фильтров
"""

import sqlite3

def check_groups():
    """Проверяем группы в БД"""
    print("🔍 ПРОВЕРКА ГРУПП В БД")
    print("=" * 40)
    
    conn = sqlite3.connect('urban_analysis_fixed.db')
    cursor = conn.cursor()
    
    # Проверяем группы в object_groups
    cursor.execute('SELECT id, group_name, group_type FROM object_groups ORDER BY group_type')
    print("📋 Группы в object_groups:")
    for row in cursor.fetchall():
        print(f"  ID:{row[0]} | {row[1]} | {row[2]}")
    
    print()
    
    # Проверяем группы у объектов
    cursor.execute('''
        SELECT DISTINCT og.group_type, og.group_name, COUNT(*) as count
        FROM objects o 
        JOIN object_groups og ON o.group_id = og.id 
        GROUP BY og.group_type, og.group_name
        ORDER BY og.group_type
    ''')
    print("📊 Группы у объектов:")
    for row in cursor.fetchall():
        print(f"  {row[0]} | {row[1]} | {row[2]} объектов")
    
    print()
    
    # Проверяем фильтр 'university'
    cursor.execute('''
        SELECT o.id, o.name, og.group_type, og.group_name
        FROM objects o
        LEFT JOIN object_groups og ON o.group_id = og.id
        WHERE og.group_type = 'university'
        LIMIT 3
    ''')
    print("🎓 Объекты с group_type='university':")
    for row in cursor.fetchall():
        print(f"  ID:{row[0]} | {row[1][:50]}... | {row[2]} | {row[3]}")
    
    conn.close()

if __name__ == "__main__":
    check_groups()