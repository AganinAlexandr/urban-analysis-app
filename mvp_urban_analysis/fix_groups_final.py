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
        
        # Нормализуем группы школ - заменяем 'school' на 'schools'
        cursor.execute("""
            UPDATE object_groups 
            SET group_type = 'schools', group_name = 'schools' 
            WHERE group_type = 'school'
        """)
        print("✅ Нормализованы группы школ в object_groups")
        
        cursor.execute("""
            UPDATE detected_groups 
            SET group_type = 'schools', group_name = 'schools' 
            WHERE group_type = 'school'
        """)
        print("✅ Нормализованы группы школ в detected_groups")
        
        # Удаляем дублирующиеся группы
        cursor.execute("""
            DELETE FROM object_groups 
            WHERE id NOT IN (
                SELECT MIN(id) FROM object_groups 
                GROUP BY group_type
            )
        """)
        print("✅ Удалены дублирующиеся группы в object_groups")
        
        cursor.execute("""
            DELETE FROM detected_groups 
            WHERE id NOT IN (
                SELECT MIN(id) FROM detected_groups 
                GROUP BY group_type
            )
        """)
        print("✅ Удалены дублирующиеся группы в detected_groups")
        
        # 3. Исправляем связь объектов с группами
        print("\n3. ИСПРАВЛЕНИЕ СВЯЗИ ОБЪЕКТОВ...")
        
        # Обновляем group_id для объектов с группой 'school'
        cursor.execute("""
            UPDATE objects 
            SET group_id = (
                SELECT id FROM object_groups WHERE group_type = 'schools'
            )
            WHERE group_id = (
                SELECT id FROM object_groups WHERE group_type = 'school'
            )
        """)
        
        # Обновляем detected_group_id для объектов с группой 'school'
        cursor.execute("""
            UPDATE objects 
            SET detected_group_id = (
                SELECT id FROM detected_groups WHERE group_type = 'schools'
            )
            WHERE detected_group_id = (
                SELECT id FROM detected_groups WHERE group_type = 'school'
            )
        """)
        
        print("✅ Обновлены связи объектов с группами")
        
        # 4. Проверяем результат
        print("\n4. ПРОВЕРКА РЕЗУЛЬТАТА...")
        
        cursor.execute("SELECT group_type, COUNT(*) FROM object_groups GROUP BY group_type")
        groups = cursor.fetchall()
        print("Группы в object_groups:")
        for group_type, count in groups:
            print(f"  {group_type}: {count}")
        
        cursor.execute("SELECT group_type, COUNT(*) FROM detected_groups GROUP BY group_type")
        groups = cursor.fetchall()
        print("Группы в detected_groups:")
        for group_type, count in groups:
            print(f"  {group_type}: {count}")
        
        conn.commit()
        print("\n✅ ИСПРАВЛЕНИЕ ЗАВЕРШЕНО УСПЕШНО")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    fix_database_groups() 