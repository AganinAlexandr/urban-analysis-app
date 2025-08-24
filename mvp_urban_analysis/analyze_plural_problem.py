#!/usr/bin/env python3
"""
Анализ проблемы с множественным числом групп
"""
import sqlite3

def analyze_plural_groups():
    """Анализируем откуда берутся группы во множественном числе"""
    print("=== АНАЛИЗ ПРОБЛЕМЫ С МНОЖЕСТВЕННЫМ ЧИСЛОМ ===")
    
    conn = sqlite3.connect('urban_analysis_fixed.db')
    cursor = conn.cursor()
    
    # Ищем все проблемные группы
    print("\n1. ПРОБЛЕМНЫЕ ГРУППЫ:")
    cursor.execute('''
        SELECT id, group_name, group_type 
        FROM object_groups 
        WHERE group_type LIKE "%universities%" 
           OR group_type LIKE "%schools%" 
           OR group_type LIKE "%hospitals%" 
           OR group_type LIKE "%pharmacies%"
           OR group_type LIKE "%polyclinics%"
           OR group_type LIKE "%kindergartens%"
           OR group_type LIKE "%shopping_malls%"
    ''')
    bad_groups = cursor.fetchall()
    print(f"Найдено {len(bad_groups)} проблемных групп:")
    for group in bad_groups:
        print(f"  ID:{group[0]} | {group[1]} | {group[2]}")
    
    # Проверяем объекты с этими группами
    print("\n2. ОБЪЕКТЫ С ПРОБЛЕМНЫМИ ГРУППАМИ:")
    if bad_groups:
        bad_group_ids = [str(group[0]) for group in bad_groups]
        cursor.execute(f'''
            SELECT id, name, group_id 
            FROM objects 
            WHERE group_id IN ({','.join(bad_group_ids)})
        ''')
        objects = cursor.fetchall()
        print(f"Объектов с проблемными группами: {len(objects)}")
        for obj in objects:
            print(f"  {obj[0]} - {obj[1]} (group_id: {obj[2]})")
    
    # Проверяем detected_groups
    print("\n3. ПРОБЛЕМНЫЕ ОПРЕДЕЛЯЕМЫЕ ГРУППЫ:")
    cursor.execute('''
        SELECT id, group_name, group_type 
        FROM detected_groups 
        WHERE group_type LIKE "%universities%" 
           OR group_type LIKE "%schools%" 
           OR group_type LIKE "%hospitals%"
    ''')
    bad_detected = cursor.fetchall()
    print(f"Найдено {len(bad_detected)} проблемных определяемых групп:")
    for group in bad_detected:
        print(f"  ID:{group[0]} | {group[1]} | {group[2]}")
    
    # Показываем все группы
    print("\n4. ВСЕ ГРУППЫ В БД:")
    cursor.execute('SELECT id, group_name, group_type FROM object_groups ORDER BY group_type')
    all_groups = cursor.fetchall()
    for group in all_groups:
        marker = "❌" if any(plural in group[2] for plural in ['universities', 'schools', 'hospitals', 'pharmacies', 'polyclinics', 'kindergartens', 'shopping_malls']) else "✅"
        print(f"  {marker} ID:{group[0]} | {group[1]} | {group[2]}")
    
    conn.close()

def fix_plural_groups():
    """Удаляем проблемные группы"""
    print("\n=== ИСПРАВЛЕНИЕ ПРОБЛЕМЫ ===")
    
    conn = sqlite3.connect('urban_analysis_fixed.db')
    cursor = conn.cursor()
    
    # Сначала проверяем, есть ли объекты с проблемными группами
    cursor.execute('''
        SELECT COUNT(*) FROM objects o
        JOIN object_groups og ON o.group_id = og.id
        WHERE og.group_type LIKE "%universities%" 
           OR og.group_type LIKE "%schools%" 
           OR og.group_type LIKE "%hospitals%"
    ''')
    object_count = cursor.fetchone()[0]
    
    if object_count > 0:
        print(f"⚠️ Найдено {object_count} объектов с проблемными группами")
        print("Нужно сначала перенести их на правильные группы")
        
        # Переносим объекты на правильные группы
        print("Переносим объекты...")
        cursor.execute('''
            UPDATE objects 
            SET group_id = (SELECT id FROM object_groups WHERE group_type = 'university' LIMIT 1)
            WHERE group_id IN (SELECT id FROM object_groups WHERE group_type = 'universities')
        ''')
        print(f"✅ Перенесено {cursor.rowcount} объектов с universities на university")
    
    # Удаляем проблемные группы
    cursor.execute('''
        DELETE FROM object_groups 
        WHERE group_type LIKE "%universities%" 
           OR group_type LIKE "%schools%" 
           OR group_type LIKE "%hospitals%"
           OR group_type LIKE "%pharmacies%"
           OR group_type LIKE "%polyclinics%"
           OR group_type LIKE "%kindergartens%"
           OR group_type LIKE "%shopping_malls%"
    ''')
    deleted_count = cursor.rowcount
    print(f"✅ Удалено {deleted_count} проблемных групп")
    
    # Удаляем из detected_groups
    cursor.execute('''
        DELETE FROM detected_groups 
        WHERE group_type LIKE "%universities%" 
           OR group_type LIKE "%schools%" 
           OR group_type LIKE "%hospitals%"
    ''')
    deleted_detected = cursor.rowcount
    print(f"✅ Удалено {deleted_detected} проблемных определяемых групп")
    
    conn.commit()
    conn.close()
    
    print("\n🎉 ПРОБЛЕМА ИСПРАВЛЕНА!")

if __name__ == "__main__":
    analyze_plural_groups()
    
    print("\n" + "="*50)
    answer = input("Исправить проблему? (y/n): ")
    if answer.lower() == 'y':
        fix_plural_groups()
        print("\nПроверяем результат...")
        analyze_plural_groups()