#!/usr/bin/env python3
"""
Нормализация групп в таблице detected_groups
"""

import sqlite3
import pandas as pd

def normalize_detected_groups():
    """Нормализует группы в таблице detected_groups"""
    print("=== НОРМАЛИЗАЦИЯ ГРУПП В DETECTED_GROUPS ===")
    
    # Подключаемся к БД
    conn = sqlite3.connect('urban_analysis_fixed.db')
    
    try:
        # Проверяем текущее состояние
        print("1. Текущее состояние таблицы detected_groups:")
        df_groups = pd.read_sql_query("SELECT * FROM detected_groups ORDER BY id", conn)
        print(df_groups.to_string(index=False))
        
        # Анализируем проблемы
        print(f"\n2. Анализ проблем:")
        
        # Проблема 1: Дублирование по group_name
        duplicates_name = df_groups.groupby('group_name').size()
        print(f"Дублирование по group_name:")
        for name, count in duplicates_name.items():
            if count > 1:
                print(f"  '{name}': {count} записей")
        
        # Проблема 2: Дублирование по group_type
        duplicates_type = df_groups.groupby('group_type').size()
        print(f"Дублирование по group_type:")
        for type_name, count in duplicates_type.items():
            if count > 1:
                print(f"  '{type_name}': {count} записей")
        
        # Проблема 3: Несоответствие названий
        print(f"\nНесоответствие названий:")
        for _, row in df_groups.iterrows():
            if row['group_name'] != row['group_type']:
                print(f"  ID {row['id']}: group_name='{row['group_name']}' vs group_type='{row['group_type']}'")
        
        # Создаем нормализованную структуру
        print(f"\n3. Создание нормализованной структуры:")
        
        # Определяем правильные маппинги
        correct_mappings = {
            'university': 'Университет',
            'school': 'Школа', 
            'hospital': 'Больница',
            'pharmacy': 'Аптека',
            'kindergarden': 'Детский сад',
            'polyclinic': 'Поликлиника',
            'shopmall': 'Торговый центр',
            'resident_complex': 'Жилой комплекс'
        }
        
        # Создаем новую таблицу
        print("Создаем новую таблицу detected_groups_normalized...")
        
        # Удаляем старую таблицу если есть
        conn.execute("DROP TABLE IF EXISTS detected_groups_normalized")
        
        # Создаем новую таблицу
        conn.execute("""
        CREATE TABLE detected_groups_normalized (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_name TEXT NOT NULL UNIQUE,
            group_type TEXT NOT NULL,
            detection_method TEXT DEFAULT 'auto',
            confidence REAL DEFAULT 1.0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        # Заполняем нормализованными данными
        for group_name, group_type in correct_mappings.items():
            conn.execute("""
            INSERT INTO detected_groups_normalized (group_name, group_type, detection_method, confidence)
            VALUES (?, ?, 'auto', 1.0)
            """, (group_name, group_type))
            print(f"  Добавлена группа: {group_name} -> {group_type}")
        
        # Проверяем результат
        print(f"\n4. Результат нормализации:")
        df_normalized = pd.read_sql_query("SELECT * FROM detected_groups_normalized ORDER BY id", conn)
        print(df_normalized.to_string(index=False))
        
        # Обновляем ссылки в таблице objects
        print(f"\n5. Обновление ссылок в таблице objects...")
        
        # Создаем временную таблицу для маппинга
        conn.execute("DROP TABLE IF EXISTS temp_group_mapping")
        conn.execute("""
        CREATE TABLE temp_group_mapping AS
        SELECT 
            dg.id as old_id,
            dgn.id as new_id,
            dg.group_name as old_group_name,
            dgn.group_name as new_group_name
        FROM detected_groups dg
        JOIN detected_groups_normalized dgn ON dg.group_name = dgn.group_name
        """)
        
        # Показываем маппинг
        df_mapping = pd.read_sql_query("SELECT * FROM temp_group_mapping ORDER BY old_id", conn)
        print("Маппинг старых ID на новые:")
        print(df_mapping.to_string(index=False))
        
        # Обновляем detected_group_id в таблице objects
        update_query = """
        UPDATE objects 
        SET detected_group_id = (
            SELECT new_id 
            FROM temp_group_mapping 
            WHERE old_id = objects.detected_group_id
        )
        WHERE detected_group_id IN (
            SELECT old_id FROM temp_group_mapping
        )
        """
        
        cursor = conn.execute(update_query)
        updated_count = cursor.rowcount
        print(f"Обновлено объектов: {updated_count}")
        
        # Проверяем результат обновления
        print(f"\n6. Проверка обновления:")
        df_objects = pd.read_sql_query("""
        SELECT o.id, o.name, o.detected_group_id, dgn.group_name, dgn.group_type
        FROM objects o
        LEFT JOIN detected_groups_normalized dgn ON o.detected_group_id = dgn.id
        ORDER BY o.id
        """, conn)
        
        print("Объекты после обновления:")
        print(df_objects.to_string(index=False))
        
        # Заменяем старую таблицу на новую
        print(f"\n7. Замена таблицы detected_groups...")
        
        # Переименовываем таблицы
        conn.execute("ALTER TABLE detected_groups RENAME TO detected_groups_old")
        conn.execute("ALTER TABLE detected_groups_normalized RENAME TO detected_groups")
        
        # Удаляем временные таблицы
        conn.execute("DROP TABLE temp_group_mapping")
        
        print("✅ Нормализация завершена!")
        
        # Финальная проверка
        print(f"\n8. Финальная проверка:")
        df_final = pd.read_sql_query("SELECT * FROM detected_groups ORDER BY id", conn)
        print("Таблица detected_groups после нормализации:")
        print(df_final.to_string(index=False))
        
        # Проверяем объекты
        df_objects_final = pd.read_sql_query("""
        SELECT o.id, o.name, o.detected_group_id, dg.group_name, dg.group_type
        FROM objects o
        LEFT JOIN detected_groups dg ON o.detected_group_id = dg.id
        ORDER BY o.id
        """, conn)
        
        print(f"\nОбъекты после нормализации:")
        print(df_objects_final.to_string(index=False))
        
        # Статистика
        print(f"\n9. Статистика:")
        print(f"Всего объектов: {len(df_objects_final)}")
        print(f"Объектов с группами: {df_objects_final['detected_group_id'].notna().sum()}")
        print(f"Объектов без групп: {df_objects_final['detected_group_id'].isna().sum()}")
        
        # Распределение по группам
        groups_dist = df_objects_final['group_type'].value_counts()
        print(f"\nРаспределение по группам:")
        for group, count in groups_dist.items():
            if pd.notna(group):
                print(f"  {group}: {count} объектов")
        
        conn.commit()
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    normalize_detected_groups()






