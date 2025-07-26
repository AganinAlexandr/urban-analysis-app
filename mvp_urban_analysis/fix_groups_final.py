#!/usr/bin/env python3
"""
Исправление проблем с группами в БД
"""

import sqlite3
import pandas as pd

def fix_database_groups():
    """Исправляет проблемы с группами в базе данных"""
    print("=== ИСПРАВЛЕНИЕ ГРУПП В БД ===")
    
    db_path = 'urban_analysis_fixed.db'
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 1. Создаем резервную копию
        print("\n1. СОЗДАНИЕ РЕЗЕРВНОЙ КОПИИ...")
        cursor.execute("CREATE TABLE IF NOT EXISTS object_groups_backup AS SELECT * FROM object_groups")
        cursor.execute("CREATE TABLE IF NOT EXISTS detected_groups_backup AS SELECT * FROM detected_groups")
        print("✅ Резервные копии созданы")
        
        # 2. Очищаем дублирующиеся группы
        print("\n2. ОЧИСТКА ДУБЛИРУЮЩИХСЯ ГРУПП...")
        
        # Удаляем дублирующуюся группу ID 3 (Школы)
        cursor.execute("DELETE FROM object_groups WHERE id = 3")
        print("✅ Удалена дублирующаяся группа ID 3 (Школы)")
        
        # 3. Исправляем связь объектов с группами
        print("\n3. ИСПРАВЛЕНИЕ СВЯЗИ ОБЪЕКТОВ...")
        
        # Проверяем объекты без групп
        cursor.execute("""
            SELECT id, name FROM objects 
            WHERE group_id IS NULL OR detected_group_id IS NULL
        """)
        unlinked_objects = cursor.fetchall()
        
        print(f"Найдено объектов без групп: {len(unlinked_objects)}")
        
        for obj_id, name in unlinked_objects:
            # Определяем группу по названию
            group_id = None
            detected_group_id = None
            
            if 'университет' in name.lower() or 'высшая школа' in name.lower():
                group_id = 8  # universities
                detected_group_id = 1  # schools (временное решение)
                print(f"  {name} -> universities (ID: 8)")
            elif 'торговый' in name.lower() or 'молл' in name.lower() or 'плаза' in name.lower():
                group_id = 7  # shopping_malls
                detected_group_id = 1  # schools (временное решение)
                print(f"  {name} -> shopping_malls (ID: 7)")
            else:
                # По умолчанию относим к школам
                group_id = 1  # schools
                detected_group_id = 1  # schools
                print(f"  {name} -> schools (ID: 1)")
            
            # Обновляем объект
            cursor.execute("""
                UPDATE objects 
                SET group_id = ?, detected_group_id = ?
                WHERE id = ?
            """, (group_id, detected_group_id, obj_id))
        
        # 4. Проверяем результат
        print("\n4. ПРОВЕРКА РЕЗУЛЬТАТА...")
        
        # Проверяем группы
        cursor.execute("SELECT * FROM object_groups ORDER BY id")
        groups = cursor.fetchall()
        print("Группы после исправления:")
        for group in groups:
            print(f"  ID: {group[0]}, Тип: {group[1]}, Название: {group[2]}")
        
        # Проверяем объекты с группами
        cursor.execute("""
            SELECT o.id, o.name, o.group_id, o.detected_group_id,
                   og.group_type as og_type, og.group_name as og_name
            FROM objects o
            LEFT JOIN object_groups og ON o.group_id = og.id
            WHERE o.latitude IS NOT NULL AND o.longitude IS NOT NULL
        """)
        objects_with_groups = cursor.fetchall()
        
        print(f"\nОбъекты с координатами и группами:")
        for obj in objects_with_groups:
            obj_id, name, group_id, detected_group_id, og_type, og_name = obj
            print(f"  {name}: {og_type} ({og_name})")
        
        # 5. Подсчитываем статистику
        cursor.execute("SELECT COUNT(*) FROM objects WHERE group_id IS NOT NULL")
        linked_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM objects")
        total_count = cursor.fetchone()[0]
        
        print(f"\n📊 СТАТИСТИКА:")
        print(f"  Всего объектов: {total_count}")
        print(f"  Связано с группами: {linked_count}")
        print(f"  Без групп: {total_count - linked_count}")
        
        # Сохраняем изменения
        conn.commit()
        conn.close()
        
        print("\n✅ Исправление завершено!")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        conn.rollback()
        conn.close()

if __name__ == "__main__":
    fix_database_groups() 