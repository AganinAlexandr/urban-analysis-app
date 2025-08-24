#!/usr/bin/env python3
"""
Финальная очистка БД от оставшихся групп во множественном числе
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.database_fixed import db_manager_fixed
import sqlite3

def final_cleanup():
    """Финальная очистка"""
    print("=== ФИНАЛЬНАЯ ОЧИСТКА БД ===")
    
    try:
        with db_manager_fixed.get_connection() as conn:
            # 1. Удаляем записи с типами во множественном числе
            plural_types = ['kindergartens', 'universities', 'schools', 'hospitals', 'pharmacies', 'polyclinics', 'shopping_malls']
            
            for plural_type in plural_types:
                # Удаляем из object_groups
                cursor = conn.execute("DELETE FROM object_groups WHERE group_type = ?", (plural_type,))
                if cursor.rowcount > 0:
                    print(f"✅ Удалено из object_groups: {plural_type}")
                
                # Удаляем из detected_groups
                cursor = conn.execute("DELETE FROM detected_groups WHERE group_type = ?", (plural_type,))
                if cursor.rowcount > 0:
                    print(f"✅ Удалено из detected_groups: {plural_type}")
            
            # 2. Также удаляем дубликаты названий
            print("\n=== УДАЛЕНИЕ ДУБЛИКАТОВ ===")
            
            # Получаем все группы с одинаковым типом
            cursor = conn.execute("""
                SELECT group_type, COUNT(*) as count 
                FROM object_groups 
                GROUP BY group_type 
                HAVING COUNT(*) > 1
            """)
            duplicates = cursor.fetchall()
            
            for group_type, count in duplicates:
                print(f"Найдено {count} дубликатов для типа: {group_type}")
                
                # Оставляем только первую запись
                cursor = conn.execute("""
                    DELETE FROM object_groups 
                    WHERE id NOT IN (
                        SELECT MIN(id) 
                        FROM object_groups 
                        WHERE group_type = ?
                    ) AND group_type = ?
                """, (group_type, group_type))
                
                print(f"✅ Удалено {cursor.rowcount} дубликатов для {group_type}")
        
        # 3. Показываем финальное состояние
        print("\n=== ФИНАЛЬНОЕ СОСТОЯНИЕ ===")
        with db_manager_fixed.get_connection() as conn:
            cursor = conn.execute("SELECT id, group_name, group_type FROM object_groups ORDER BY group_type")
            groups = cursor.fetchall()
            print(f"Осталось групп: {len(groups)}")
            for group in groups:
                print(f"  ID: {group['id']}, Name: {group['group_name']}, Type: {group['group_type']}")
        
        print("\n✅ Финальная очистка завершена!")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    final_cleanup()