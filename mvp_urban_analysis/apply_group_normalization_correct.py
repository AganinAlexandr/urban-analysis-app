#!/usr/bin/env python3
"""
Применение нормализации групп к базе данных (правильная версия)
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.database_fixed import db_manager_fixed
import pandas as pd

def apply_group_normalization_correct():
    """Применяет нормализацию групп к базе данных"""
    print("=== ПРИМЕНЕНИЕ НОРМАЛИЗАЦИИ ГРУПП (ПРАВИЛЬНАЯ ВЕРСИЯ) ===")
    
    try:
        # Получаем данные из БД
        df = db_manager_fixed.export_to_dataframe(include_analysis=True)
        print(f"Всего записей в БД: {len(df)}")
        
        # Проверяем текущие группы
        print(f"\n=== ТЕКУЩИЕ ГРУППЫ ===")
        print("Группы от поставщика:")
        supplier_groups = df['group_type'].value_counts()
        for group, count in supplier_groups.items():
            print(f"  '{group}': {count}")
        
        print(f"\nОпределенные группы:")
        determined_groups = df['detected_group_type'].value_counts()
        for group, count in determined_groups.items():
            print(f"  '{group}': {count}")
        
        # Применяем нормализацию
        print(f"\n=== ПРИМЕНЯЕМ НОРМАЛИЗАЦИЮ ===")
        
        with db_manager_fixed.get_connection() as conn:
            cursor = conn.cursor()
            
            # Нормализуем группы в таблице object_groups
            print("Нормализуем справочник групп...")
            
            # Словарь для замены множественного числа на единственное
            plural_to_singular = {
                'schools': 'school',
                'hospitals': 'hospital',
                'universities': 'university',
                'kindergartens': 'kindergarden',
                'pharmacies': 'pharmacy',
                'polyclinics': 'polyclinic',
                'shopping_malls': 'shopmall'
            }
            
            # Обновляем существующие записи
            for plural, singular in plural_to_singular.items():
                print(f"  Обновляем '{plural}' → '{singular}'...")
                cursor.execute("""
                    UPDATE object_groups 
                    SET group_type = ?, group_name = ? 
                    WHERE group_type = ? OR group_name = ?
                """, (singular, singular, plural, plural))
                updated = cursor.rowcount
                if updated > 0:
                    print(f"    Обновлено записей: {updated}")
            
            # Нормализуем группы в таблице detected_groups
            print("Нормализуем справочник определенных групп...")
            
            # Сначала удаляем все записи с множественным числом
            for plural in plural_to_singular.keys():
                cursor.execute("DELETE FROM detected_groups WHERE group_type = ? OR group_name = ?", (plural, plural))
                deleted = cursor.rowcount
                if deleted > 0:
                    print(f"  Удалено записей с '{plural}': {deleted}")
            
            # Добавляем новые записи с единственным числом
            singular_groups = [
                ('school', 'school'),
                ('hospital', 'hospital'),
                ('university', 'university'),
                ('kindergarden', 'kindergarden'),
                ('pharmacy', 'pharmacy'),
                ('polyclinic', 'polyclinic'),
                ('shopmall', 'shopmall')
            ]
            
            for group_type, group_name in singular_groups:
                cursor.execute("""
                    INSERT OR IGNORE INTO detected_groups (group_type, group_name, detection_method, confidence)
                    VALUES (?, ?, 'normalization', 1.0)
                """, (group_type, group_name))
                inserted = cursor.rowcount
                if inserted > 0:
                    print(f"  Добавлена группа '{group_type}': {inserted}")
            
            conn.commit()
            print("✅ Изменения сохранены в БД")
        
        # Проверяем результат
        print(f"\n=== РЕЗУЛЬТАТ НОРМАЛИЗАЦИИ ===")
        df_after = db_manager_fixed.export_to_dataframe(include_analysis=True)
        
        print("Группы от поставщика после нормализации:")
        supplier_groups_after = df_after['group_type'].value_counts()
        for group, count in supplier_groups_after.items():
            print(f"  '{group}': {count}")
        
        print(f"\nОпределенные группы после нормализации:")
        determined_groups_after = df_after['detected_group_type'].value_counts()
        for group, count in determined_groups_after.items():
            print(f"  '{group}': {count}")
        
        # Проверяем уникальные объекты
        coords_df = df_after[df_after['latitude'].notna() & df_after['longitude'].notna()]
        unique_coords = coords_df.drop_duplicates(subset=['latitude', 'longitude'], keep='first')
        print(f"\nУникальных объектов: {len(unique_coords)}")
        
        print(f"\n=== УНИКАЛЬНЫЕ ОБЪЕКТЫ ПОСЛЕ НОРМАЛИЗАЦИИ ===")
        for i, row in unique_coords.iterrows():
            print(f"{i+1}. {row.get('name', 'N/A')}")
            print(f"   Группа от поставщика: '{row.get('group_type', 'N/A')}'")
            print(f"   Определенная группа: '{row.get('detected_group_type', 'N/A')}'")
            print()
        
        print(f"\n✅ Нормализация завершена!")
        print("Теперь все группы используют единственное число.")
        
    except Exception as e:
        print(f"❌ Ошибка нормализации: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    apply_group_normalization_correct() 