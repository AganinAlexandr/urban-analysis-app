#!/usr/bin/env python3
"""
Восстановление потерянных данных групп от поставщика
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.database_fixed import db_manager_fixed
import sqlite3

def restore_group_data():
    """Восстанавливаем потерянные данные групп"""
    print("=== ВОССТАНОВЛЕНИЕ ГРУПП ОТ ПОСТАВЩИКА ===")
    
    try:
        with db_manager_fixed.get_connection() as conn:
            # 1. Проверяем текущее состояние
            cursor = conn.execute("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(group_id) as with_group,
                    COUNT(*) - COUNT(group_id) as without_group
                FROM objects
            """)
            stats = cursor.fetchone()
            print(f"Всего объектов: {stats['total']}")
            print(f"С группой от поставщика: {stats['with_group']}")
            print(f"Без группы от поставщика: {stats['without_group']}")
            
            if stats['without_group'] == 0:
                print("✅ Все объекты уже имеют группы от поставщика")
                return
            
            # 2. Находим объекты без group_id, но с detected_group_id
            cursor = conn.execute("""
                SELECT 
                    o.id, o.name, o.address,
                    o.group_id, o.detected_group_id,
                    dg.group_type as detected_group_type
                FROM objects o
                LEFT JOIN detected_groups dg ON o.detected_group_id = dg.id
                WHERE o.group_id IS NULL AND o.detected_group_id IS NOT NULL
                LIMIT 10
            """)
            
            candidates = cursor.fetchall()
            print(f"\nНайдено {len(candidates)} объектов для восстановления (показываем первые 10):")
            
            # 3. Пытаемся восстановить группы на основе определенных групп
            restored_count = 0
            
            for obj in candidates:
                obj_id = obj['id']
                detected_group_type = obj['detected_group_type']
                
                # Ищем соответствующую группу в object_groups
                cursor = conn.execute("""
                    SELECT id FROM object_groups 
                    WHERE group_type = ?
                    LIMIT 1
                """, (detected_group_type,))
                
                group_result = cursor.fetchone()
                if group_result:
                    group_id = group_result['id']
                    
                    # Обновляем объект
                    cursor = conn.execute("""
                        UPDATE objects 
                        SET group_id = ?
                        WHERE id = ?
                    """, (group_id, obj_id))
                    
                    restored_count += 1
                    print(f"  ✅ {obj['name']}: восстановлена группа {detected_group_type}")
                else:
                    print(f"  ⚠️ {obj['name']}: не найдена группа {detected_group_type}")
            
            print(f"\n✅ Восстановлено {restored_count} групп от поставщика")
            
            # 4. Проверяем финальное состояние
            cursor = conn.execute("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(group_id) as with_group,
                    COUNT(*) - COUNT(group_id) as without_group
                FROM objects
            """)
            final_stats = cursor.fetchone()
            print(f"\nФинальная статистика:")
            print(f"Всего объектов: {final_stats['total']}")
            print(f"С группой от поставщика: {final_stats['with_group']}")
            print(f"Без группы от поставщика: {final_stats['without_group']}")
            
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

def check_api_groups():
    """Проверяем что API групп возвращает"""
    print("\n=== ПРОВЕРКА API ГРУПП ===")
    
    try:
        with db_manager_fixed.get_connection() as conn:
            cursor = conn.execute("""
                SELECT group_name, group_type 
                FROM object_groups 
                ORDER BY group_type
            """)
            groups = cursor.fetchall()
            
            print(f"Найдено {len(groups)} групп в object_groups:")
            for group in groups:
                print(f"  {group['group_type']} - {group['group_name']}")
                
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    restore_group_data()
    check_api_groups()