#!/usr/bin/env python3
"""
Скрипт для исправления существующих объектов в БД
Находит объекты без group_id и устанавливает правильные группы
"""

from app.core.database_fixed import db_manager_fixed
import sqlite3

def fix_existing_groups():
    """Исправляет существующие объекты без group_id"""
    print("Исправляем существующие группы в БД...")
    
    db_path = "urban_analysis_fixed.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Получаем объекты без group_id
    cursor.execute("""
        SELECT o.id, o.name, o.address 
        FROM objects o 
        WHERE o.group_id IS NULL
    """)
    
    objects_without_group = cursor.fetchall()
    print(f"Найдено объектов без группы: {len(objects_without_group)}")
    
    # Определяем группы по названию объекта
    group_mapping = {
        'школа': 'schools',
        'school': 'schools',
        'университет': 'universities',
        'university': 'universities',
        'больница': 'hospitals',
        'hospital': 'hospitals',
        'аптека': 'pharmacies',
        'pharmacy': 'pharmacies',
        'поликлиника': 'polyclinics',
        'polyclinic': 'polyclinics',
        'детский сад': 'kindergartens',
        'kindergarten': 'kindergartens',
        'торговый центр': 'shopping_malls',
        'mall': 'shopping_malls',
        'жилой комплекс': 'residential_complexes',
        'residential': 'residential_complexes'
    }
    
    updated_count = 0
    for object_id, name, address in objects_without_group:
        # Определяем группу по названию
        name_lower = name.lower()
        detected_group = None
        
        for keyword, group in group_mapping.items():
            if keyword in name_lower:
                detected_group = group
                break
        
        if detected_group:
            # Получаем или создаем group_id
            group_id = db_manager_fixed.get_group_id(detected_group)
            if group_id is None:
                group_id = db_manager_fixed.create_group(detected_group, detected_group)
            
            # Обновляем объект
            cursor.execute("""
                UPDATE objects 
                SET group_id = ?, detected_group_id = ?
                WHERE id = ?
            """, (group_id, group_id, object_id))
            
            print(f"  Обновлен объект {object_id}: '{name}' -> группа '{detected_group}'")
            updated_count += 1
        else:
            print(f"  Не удалось определить группу для объекта {object_id}: '{name}'")
    
    conn.commit()
    conn.close()
    
    print(f"✅ Исправлено объектов: {updated_count}")
    return updated_count

if __name__ == "__main__":
    fix_existing_groups() 