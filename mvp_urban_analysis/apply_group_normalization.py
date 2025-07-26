#!/usr/bin/env python3
"""
Применение нормализации групп к базе данных
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.database_fixed import db_manager_fixed
import pandas as pd

def apply_group_normalization():
    """Применяет нормализацию групп к базе данных"""
    print("=== ПРИМЕНЕНИЕ НОРМАЛИЗАЦИИ ГРУПП ===")
    
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
            
            # Нормализуем группы от поставщика
            print("Нормализуем группы от поставщика...")
            normalization_queries = [
                ("UPDATE objects SET group_type = 'school' WHERE group_type = 'schools'", "schools → school"),
                ("UPDATE objects SET group_type = 'hospital' WHERE group_type = 'hospitals'", "hospitals → hospital"),
                ("UPDATE objects SET group_type = 'university' WHERE group_type = 'universities'", "universities → university"),
                ("UPDATE objects SET group_type = 'kindergarden' WHERE group_type = 'kindergartens'", "kindergartens → kindergarden"),
                ("UPDATE objects SET group_type = 'pharmacy' WHERE group_type = 'pharmacies'", "pharmacies → pharmacy"),
                ("UPDATE objects SET group_type = 'polyclinic' WHERE group_type = 'polyclinics'", "polyclinics → polyclinic"),
                ("UPDATE objects SET group_type = 'shopmall' WHERE group_type = 'shopping_malls'", "shopping_malls → shopmall")
            ]
            
            supplier_updated = 0
            for query, description in normalization_queries:
                cursor.execute(query)
                updated = cursor.rowcount
                if updated > 0:
                    print(f"  {description}: {updated}")
                    supplier_updated += updated
            
            print(f"Обновлено групп от поставщика: {supplier_updated}")
            
            # Нормализуем определенные группы
            print("Нормализуем определенные группы...")
            determined_queries = [
                ("UPDATE objects SET detected_group_type = 'school' WHERE detected_group_type = 'schools'", "schools → school"),
                ("UPDATE objects SET detected_group_type = 'hospital' WHERE detected_group_type = 'hospitals'", "hospitals → hospital"),
                ("UPDATE objects SET detected_group_type = 'university' WHERE detected_group_type = 'universities'", "universities → university"),
                ("UPDATE objects SET detected_group_type = 'kindergarden' WHERE detected_group_type = 'kindergartens'", "kindergartens → kindergarden"),
                ("UPDATE objects SET detected_group_type = 'pharmacy' WHERE detected_group_type = 'pharmacies'", "pharmacies → pharmacy"),
                ("UPDATE objects SET detected_group_type = 'polyclinic' WHERE detected_group_type = 'polyclinics'", "polyclinics → polyclinic"),
                ("UPDATE objects SET detected_group_type = 'shopmall' WHERE detected_group_type = 'shopping_malls'", "shopping_malls → shopmall")
            ]
            
            determined_updated = 0
            for query, description in determined_queries:
                cursor.execute(query)
                updated = cursor.rowcount
                if updated > 0:
                    print(f"  {description}: {updated}")
                    determined_updated += updated
            
            print(f"Обновлено определенных групп: {determined_updated}")
            
            # Нормализуем группы в таблице object_groups
            print("Нормализуем справочник групп...")
            groups_queries = [
                ("UPDATE object_groups SET group_type = 'school', group_name = 'school' WHERE group_type = 'schools'", "schools → school"),
                ("UPDATE object_groups SET group_type = 'hospital', group_name = 'hospital' WHERE group_type = 'hospitals'", "hospitals → hospital"),
                ("UPDATE object_groups SET group_type = 'university', group_name = 'university' WHERE group_type = 'universities'", "universities → university"),
                ("UPDATE object_groups SET group_type = 'kindergarden', group_name = 'kindergarden' WHERE group_type = 'kindergartens'", "kindergartens → kindergarden"),
                ("UPDATE object_groups SET group_type = 'pharmacy', group_name = 'pharmacy' WHERE group_type = 'pharmacies'", "pharmacies → pharmacy"),
                ("UPDATE object_groups SET group_type = 'polyclinic', group_name = 'polyclinic' WHERE group_type = 'polyclinics'", "polyclinics → polyclinic"),
                ("UPDATE object_groups SET group_type = 'shopmall', group_name = 'shopmall' WHERE group_type = 'shopping_malls'", "shopping_malls → shopmall")
            ]
            
            groups_updated = 0
            for query, description in groups_queries:
                cursor.execute(query)
                updated = cursor.rowcount
                if updated > 0:
                    print(f"  {description}: {updated}")
                    groups_updated += updated
            
            print(f"Обновлено в справочнике групп: {groups_updated}")
            
            # Нормализуем группы в таблице detected_groups
            print("Нормализуем справочник определенных групп...")
            detected_queries = [
                ("UPDATE detected_groups SET group_type = 'school', group_name = 'school' WHERE group_type = 'schools'", "schools → school"),
                ("UPDATE detected_groups SET group_type = 'hospital', group_name = 'hospital' WHERE group_type = 'hospitals'", "hospitals → hospital"),
                ("UPDATE detected_groups SET group_type = 'university', group_name = 'university' WHERE group_type = 'universities'", "universities → university"),
                ("UPDATE detected_groups SET group_type = 'kindergarden', group_name = 'kindergarden' WHERE group_type = 'kindergartens'", "kindergartens → kindergarden"),
                ("UPDATE detected_groups SET group_type = 'pharmacy', group_name = 'pharmacy' WHERE group_type = 'pharmacies'", "pharmacies → pharmacy"),
                ("UPDATE detected_groups SET group_type = 'polyclinic', group_name = 'polyclinic' WHERE group_type = 'polyclinics'", "polyclinics → polyclinic"),
                ("UPDATE detected_groups SET group_type = 'shopmall', group_name = 'shopmall' WHERE group_type = 'shopping_malls'", "shopping_malls → shopmall")
            ]
            
            detected_groups_updated = 0
            for query, description in detected_queries:
                cursor.execute(query)
                updated = cursor.rowcount
                if updated > 0:
                    print(f"  {description}: {updated}")
                    detected_groups_updated += updated
            
            print(f"Обновлено в справочнике определенных групп: {detected_groups_updated}")
            
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
        
        print(f"\n=== СТАТИСТИКА ИЗМЕНЕНИЙ ===")
        print(f"Обновлено групп от поставщика: {supplier_updated}")
        print(f"Обновлено определенных групп: {determined_updated}")
        print(f"Обновлено в справочнике групп: {groups_updated}")
        print(f"Обновлено в справочнике определенных групп: {detected_groups_updated}")
        
        print(f"\n✅ Нормализация завершена!")
        print("Теперь все группы используют единственное число.")
        
    except Exception as e:
        print(f"❌ Ошибка нормализации: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    apply_group_normalization() 