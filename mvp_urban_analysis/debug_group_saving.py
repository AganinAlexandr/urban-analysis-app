#!/usr/bin/env python3
"""
Отладка сохранения групп в БД
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.database_fixed import db_manager_fixed
import pandas as pd

def debug_group_saving():
    """Отлаживаем сохранение групп в БД"""
    print("=== ОТЛАДКА СОХРАНЕНИЯ ГРУПП ===")
    
    try:
        # Тест 1: Проверяем существующие группы в object_groups
        print("\n1. Проверяем существующие группы в object_groups:")
        with db_manager_fixed.get_connection() as conn:
            cursor = conn.execute("SELECT id, group_name, group_type FROM object_groups ORDER BY group_type")
            groups = cursor.fetchall()
            for group in groups:
                print(f"  ID: {group['id']}, Name: {group['group_name']}, Type: {group['group_type']}")
        
        # Тест 2: Создаем тестовый объект с группой
        print("\n2. Создаем тестовый объект с группой 'school':")
        test_object_id = db_manager_fixed.insert_object(
            name="Тестовая школа №123",
            address="Тестовый адрес, 456",
            latitude=55.123456,
            longitude=37.654321,
            district="Тестовый район",
            group_type="school",  # Группа от пользователя
            detected_group_type="school"  # Автоматически определенная группа
        )
        
        if test_object_id:
            print(f"  ✅ Объект создан с ID: {test_object_id}")
        else:
            print(f"  ❌ Не удалось создать объект")
            return
        
        # Тест 3: Проверяем сохраненные данные объекта
        print("\n3. Проверяем сохраненные данные объекта:")
        with db_manager_fixed.get_connection() as conn:
            cursor = conn.execute("""
                SELECT 
                    o.id, o.name, o.address,
                    o.group_id, o.detected_group_id,
                    og.group_type as object_group_type,
                    dg.group_type as detected_group_type
                FROM objects o
                LEFT JOIN object_groups og ON o.group_id = og.id
                LEFT JOIN detected_groups dg ON o.detected_group_id = dg.id
                WHERE o.id = ?
            """, (test_object_id,))
            
            obj_data = cursor.fetchone()
            if obj_data:
                print(f"  ID: {obj_data['id']}")
                print(f"  Название: {obj_data['name']}")
                print(f"  Адрес: {obj_data['address']}")
                print(f"  group_id: {obj_data['group_id']}")
                print(f"  detected_group_id: {obj_data['detected_group_id']}")
                print(f"  object_group_type: {obj_data['object_group_type']}")
                print(f"  detected_group_type: {obj_data['detected_group_type']}")
            else:
                print(f"  ❌ Объект не найден")
        
        # Тест 4: Проверяем экспорт в DataFrame
        print("\n4. Проверяем экспорт в DataFrame:")
        df = db_manager_fixed.export_to_dataframe(include_analysis=True)
        
        test_object_data = df[df['object_id'] == test_object_id]
        if not test_object_data.empty:
            row = test_object_data.iloc[0]
            print(f"  В DataFrame найден объект:")
            print(f"    object_id: {row.get('object_id')}")
            print(f"    name: {row.get('name')}")
            print(f"    group_type: {row.get('group_type')}")
            print(f"    detected_group_type: {row.get('detected_group_type')}")
        else:
            print(f"  ❌ Объект не найден в DataFrame")
        
        # Тест 5: Удаляем тестовый объект
        print("\n5. Удаляем тестовый объект:")
        with db_manager_fixed.get_connection() as conn:
            cursor = conn.execute("DELETE FROM objects WHERE id = ?", (test_object_id,))
            if cursor.rowcount > 0:
                print(f"  ✅ Тестовый объект удален")
            else:
                print(f"  ❌ Не удалось удалить тестовый объект")
        
    except Exception as e:
        print(f"❌ Ошибка теста: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_group_saving()