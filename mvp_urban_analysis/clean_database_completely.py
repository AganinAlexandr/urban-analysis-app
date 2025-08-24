#!/usr/bin/env python3
"""
Полная очистка базы данных с сохранением словарей
"""
import sqlite3
import os
from datetime import datetime

def clean_database_completely():
    """Полностью очищает базу данных, сохраняя только словари"""
    print("=== ПОЛНАЯ ОЧИСТКА БАЗЫ ДАННЫХ ===")
    
    try:
        # Создаем резервную копию
        backup_name = f'urban_analysis_fixed.db.backup_clean_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
        if os.path.exists('urban_analysis_fixed.db'):
            import shutil
            shutil.copy('urban_analysis_fixed.db', backup_name)
            print(f"1. Создана резервная копия: {backup_name}")
        
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        # 2. Проверяем текущее состояние
        print("\n2. Текущее состояние БД:")
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        for table in tables:
            table_name = table[0]
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            print(f"   {table_name}: {count} записей")
        
        # 3. Определяем таблицы для очистки и сохранения
        tables_to_clean = [
            'reviews',
            'master_ratings', 
            'analysis_results',
            'objects'
        ]
        
        tables_to_preserve = [
            'processing_methods',
            'object_groups',
            'detected_groups'
        ]
        
        print(f"\n3. Таблицы для очистки: {tables_to_clean}")
        print(f"   Таблицы для сохранения: {tables_to_preserve}")
        
        # 4. Очищаем таблицы
        print("\n4. Очистка таблиц:")
        for table in tables_to_clean:
            try:
                cursor.execute(f"DELETE FROM {table}")
                deleted_count = cursor.rowcount
                print(f"   {table}: удалено {deleted_count} записей")
            except Exception as e:
                print(f"   {table}: ошибка - {e}")
        
        # 5. Сбрасываем автоинкремент
        print("\n5. Сброс автоинкремента:")
        for table in tables_to_clean:
            try:
                cursor.execute(f"DELETE FROM sqlite_sequence WHERE name = '{table}'")
                print(f"   {table}: автоинкремент сброшен")
            except Exception as e:
                print(f"   {table}: ошибка сброса - {e}")
        
        # 6. Проверяем результат
        print("\n6. Состояние после очистки:")
        for table in tables:
            table_name = table[0]
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            print(f"   {table_name}: {count} записей")
        
        # 7. Проверяем методы обработки
        print("\n7. Проверка методов обработки:")
        cursor.execute("""
            SELECT pm.id, pm.method_name, pm.is_active, COUNT(ar.id) as usage_count
            FROM processing_methods pm
            LEFT JOIN analysis_results ar ON pm.id = ar.method_id
            GROUP BY pm.id, pm.method_name, pm.is_active
            ORDER BY pm.id
        """)
        methods = cursor.fetchall()
        
        for method_id, method_name, is_active, usage_count in methods:
            status = "активен" if is_active else "неактивен"
            print(f"   ID {method_id}: {method_name} - {status} (используется {usage_count} раз)")
        
        # 8. Проверяем группы объектов
        print("\n8. Проверка групп объектов:")
        cursor.execute("SELECT COUNT(*) FROM object_groups")
        groups_count = cursor.fetchone()[0]
        print(f"   Групп объектов: {groups_count}")
        
        if groups_count > 0:
            cursor.execute("SELECT group_name, group_type FROM object_groups")
            groups = cursor.fetchall()
            for group_name, group_type in groups:
                print(f"     {group_name} ({group_type})")
        
        conn.commit()
        conn.close()
        
        print("\n✅ Полная очистка завершена!")
        print("   Сохранены:")
        print("     - Методы обработки")
        print("     - Группы объектов")
        print("     - Обнаруженные группы")
        print("   Очищены:")
        print("     - Отзывы")
        print("     - Master_ratings")
        print("     - Результаты анализа")
        print("     - Объекты")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    clean_database_completely() 