#!/usr/bin/env python3
"""
Отладка проблем с группами
"""

from app.core.database_fixed import db_manager_fixed
import pandas as pd

def debug_groups():
    """Отладка проблем с группами"""
    print("=== ОТЛАДКА ГРУПП ===")
    
    try:
        # Получаем данные из БД
        df = db_manager_fixed.export_to_dataframe(include_analysis=True)
        print(f"Записей в БД: {len(df)}")
        
        # Проверяем все поля, связанные с группами
        group_fields = [col for col in df.columns if 'group' in col.lower()]
        print(f"\nПоля групп в данных: {group_fields}")
        
        for field in group_fields:
            print(f"\nПоле '{field}':")
            if field in df.columns:
                unique_values = df[field].unique()
                print(f"  Уникальные значения: {unique_values}")
                print(f"  Количество непустых значений: {df[field].notna().sum()}")
                
                # Показываем несколько примеров
                non_null = df[df[field].notna()]
                if not non_null.empty:
                    print(f"  Примеры значений:")
                    for i, value in enumerate(non_null[field].head(3)):
                        print(f"    {i+1}. {value}")
            else:
                print(f"  Поле отсутствует в данных")
        
        # Проверяем таблицы групп в БД
        print(f"\n=== ПРОВЕРКА ТАБЛИЦ ГРУПП ===")
        with db_manager_fixed.get_connection() as conn:
            # Проверяем таблицу object_groups
            cursor = conn.execute("SELECT * FROM object_groups")
            groups = cursor.fetchall()
            print(f"Записей в object_groups: {len(groups)}")
            for group in groups:
                print(f"  ID: {group['id']}, Тип: {group['group_type']}, Название: {group['group_name']}")
            
            # Проверяем таблицу detected_groups
            cursor = conn.execute("SELECT * FROM detected_groups")
            detected_groups = cursor.fetchall()
            print(f"Записей в detected_groups: {len(detected_groups)}")
            for group in detected_groups:
                print(f"  ID: {group['id']}, Тип: {group['group_type']}, Название: {group['group_name']}")
            
            # Проверяем связь объектов с группами
            cursor = conn.execute("""
                SELECT o.id, o.name, o.group_id, o.detected_group_id, 
                       og.group_type as group_type, og.group_name as group_name,
                       dg.group_type as detected_group_type, dg.group_name as detected_group_name
                FROM objects o
                LEFT JOIN object_groups og ON o.group_id = og.id
                LEFT JOIN detected_groups dg ON o.detected_group_id = dg.id
            """)
            objects = cursor.fetchall()
            print(f"\nОбъекты и их группы:")
            for obj in objects:
                print(f"  ID: {obj['id']}, Название: {obj['name']}")
                print(f"    group_id: {obj['group_id']}, group_type: {obj['group_type']}, group_name: {obj['group_name']}")
                print(f"    detected_group_id: {obj['detected_group_id']}, detected_group_type: {obj['detected_group_type']}, detected_group_name: {obj['detected_group_name']}")
                print()
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    debug_groups() 