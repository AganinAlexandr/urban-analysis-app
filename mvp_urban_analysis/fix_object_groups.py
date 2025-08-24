#!/usr/bin/env python3
"""
Исправление привязок объектов к группам после нормализации
"""

import sqlite3
import pandas as pd

def fix_object_groups():
    """Исправляет привязки объектов к группам"""
    print("=== ИСПРАВЛЕНИЕ ПРИВЯЗОК ОБЪЕКТОВ К ГРУППАМ ===")
    
    # Подключаемся к БД
    conn = sqlite3.connect('urban_analysis_fixed.db')
    
    try:
        # Проверяем текущее состояние
        print("1. Текущее состояние привязок:")
        df_objects = pd.read_sql_query("""
        SELECT o.id, o.name, o.detected_group_id, dg.group_name, dg.group_type
        FROM objects o
        LEFT JOIN detected_groups dg ON o.detected_group_id = dg.id
        ORDER BY o.id
        """, conn)
        print(df_objects.to_string(index=False))
        
        # Анализируем проблемы
        print(f"\n2. Анализ проблем:")
        
        # Проблема 1: Морозовская ДГКБ привязана к university
        problem_objects = df_objects[
            (df_objects['name'].str.contains('ДГКБ', na=False)) | 
            (df_objects['name'].str.contains('больница', case=False, na=False)) |
            (df_objects['name'].str.contains('клиника', case=False, na=False))
        ]
        
        print("Объекты, которые должны быть в группе 'hospital':")
        for _, obj in problem_objects.iterrows():
            print(f"  ID {obj['id']}: {obj['name']} -> текущая группа: {obj['group_type']}")
        
        # Проблема 2: Школы привязаны к school (это правильно)
        school_objects = df_objects[df_objects['name'].str.contains('школа', case=False, na=False)]
        print(f"\nОбъекты, которые должны быть в группе 'school':")
        for _, obj in school_objects.iterrows():
            print(f"  ID {obj['id']}: {obj['name']} -> текущая группа: {obj['group_type']}")
        
        # Проблема 3: Университеты привязаны к university (это правильно)
        university_objects = df_objects[
            (df_objects['name'].str.contains('университет', case=False, na=False)) |
            (df_objects['name'].str.contains('институт', case=False, na=False)) |
            (df_objects['name'].str.contains('академия', case=False, na=False))
        ]
        print(f"\nОбъекты, которые должны быть в группе 'university':")
        for _, obj in university_objects.iterrows():
            print(f"  ID {obj['id']}: {obj['name']} -> текущая группа: {obj['group_type']}")
        
        # Исправляем привязки
        print(f"\n3. Исправление привязок:")
        
        # Получаем ID групп
        groups_df = pd.read_sql_query("SELECT * FROM detected_groups ORDER BY id", conn)
        print("Доступные группы:")
        for _, group in groups_df.iterrows():
            print(f"  ID {group['id']}: {group['group_name']} -> {group['group_type']}")
        
        # Исправляем Морозовскую ДГКБ (ID 20) - должна быть hospital (ID 3)
        print(f"\nИсправляем ID 20 (Морозовская ДГКБ) -> hospital (ID 3)")
        conn.execute("UPDATE objects SET detected_group_id = 3 WHERE id = 20")
        
        # Проверяем результат
        print(f"\n4. Результат исправления:")
        df_objects_fixed = pd.read_sql_query("""
        SELECT o.id, o.name, o.detected_group_id, dg.group_name, dg.group_type
        FROM objects o
        LEFT JOIN detected_groups dg ON o.detected_group_id = dg.id
        ORDER BY o.id
        """, conn)
        print(df_objects_fixed.to_string(index=False))
        
        # Статистика после исправления
        print(f"\n5. Статистика после исправления:")
        print(f"Всего объектов: {len(df_objects_fixed)}")
        print(f"Объектов с группами: {df_objects_fixed['detected_group_id'].notna().sum()}")
        print(f"Объектов без групп: {df_objects_fixed['detected_group_id'].isna().sum()}")
        
        # Распределение по группам
        groups_dist = df_objects_fixed['group_type'].value_counts()
        print(f"\nРаспределение по группам:")
        for group, count in groups_dist.items():
            if pd.notna(group):
                print(f"  {group}: {count} объектов")
        
        # Проверяем корректность привязок
        print(f"\n6. Проверка корректности привязок:")
        
        # Проверяем hospital
        hospital_objects = df_objects_fixed[df_objects_fixed['group_type'] == 'Больница']
        print(f"Группа 'Больница':")
        for _, obj in hospital_objects.iterrows():
            print(f"  ID {obj['id']}: {obj['name']}")
        
        # Проверяем school
        school_objects = df_objects_fixed[df_objects_fixed['group_type'] == 'Школа']
        print(f"\nГруппа 'Школа':")
        for _, obj in school_objects.iterrows():
            print(f"  ID {obj['id']}: {obj['name']}")
        
        # Проверяем university
        university_objects = df_objects_fixed[df_objects_fixed['group_type'] == 'Университет']
        print(f"\nГруппа 'Университет':")
        for _, obj in university_objects.iterrows():
            print(f"  ID {obj['id']}: {obj['name']}")
        
        conn.commit()
        print(f"\n✅ Исправление завершено!")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    fix_object_groups()


