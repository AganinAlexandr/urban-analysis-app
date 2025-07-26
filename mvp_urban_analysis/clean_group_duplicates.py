#!/usr/bin/env python3
"""
Скрипт для автоматической чистки дублей групп в object_groups и detected_groups
"""
import sqlite3
from app.core.database_fixed import db_manager_fixed

def normalize_group_name(name):
    """Приводит название группы к унифицированному виду"""
    mapping = {
        'школа': 'schools', 'school': 'schools', 'scools': 'schools', 'школы': 'schools',
        'университет': 'universities', 'university': 'universities', 'университеты': 'universities',
        'больница': 'hospitals', 'hospital': 'hospitals', 'больницы': 'hospitals',
        'аптека': 'pharmacies', 'pharmacy': 'pharmacies', 'аптеки': 'pharmacies',
        'поликлиника': 'polyclinics', 'polyclinic': 'polyclinics', 'поликлиники': 'polyclinics',
        'детский сад': 'kindergartens', 'kindergarten': 'kindergartens', 'детские сады': 'kindergartens',
        'торговый центр': 'shopping_malls', 'mall': 'shopping_malls', 'торговые центры': 'shopping_malls',
        'жилой комплекс': 'residential_complexes', 'residential': 'residential_complexes', 'жилые комплексы': 'residential_complexes'
    }
    name = name.strip().lower()
    return mapping.get(name, name)

def clean_group_duplicates():
    db_path = "urban_analysis_fixed.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 1. Получаем все группы
    cursor.execute("SELECT id, group_name, group_type FROM object_groups")
    groups = cursor.fetchall()
    
    # 2. Строим маппинг: нормализованное имя -> основной group_id
    norm_to_id = {}
    id_to_norm = {}
    for group_id, group_name, group_type in groups:
        norm = normalize_group_name(group_type)
        if norm not in norm_to_id:
            norm_to_id[norm] = group_id
        id_to_norm[group_id] = norm
    
    # 3. Обновляем все объекты: ставим правильный group_id
    for group_id, group_name, group_type in groups:
        norm = normalize_group_name(group_type)
        main_id = norm_to_id[norm]
        if group_id != main_id:
            cursor.execute("UPDATE objects SET group_id = ? WHERE group_id = ?", (main_id, group_id))
    
    # 4. Удаляем дублирующиеся группы
    for group_id, group_name, group_type in groups:
        norm = normalize_group_name(group_type)
        main_id = norm_to_id[norm]
        if group_id != main_id:
            cursor.execute("DELETE FROM object_groups WHERE id = ?", (group_id,))
    
    # Аналогично для detected_groups
    cursor.execute("SELECT id, group_name, group_type FROM detected_groups")
    groups = cursor.fetchall()
    norm_to_id = {}
    id_to_norm = {}
    for group_id, group_name, group_type in groups:
        norm = normalize_group_name(group_type)
        if norm not in norm_to_id:
            norm_to_id[norm] = group_id
        id_to_norm[group_id] = norm
    for group_id, group_name, group_type in groups:
        norm = normalize_group_name(group_type)
        main_id = norm_to_id[norm]
        if group_id != main_id:
            cursor.execute("UPDATE objects SET detected_group_id = ? WHERE detected_group_id = ?", (main_id, group_id))
    for group_id, group_name, group_type in groups:
        norm = normalize_group_name(group_type)
        main_id = norm_to_id[norm]
        if group_id != main_id:
            cursor.execute("DELETE FROM detected_groups WHERE id = ?", (group_id,))
    
    conn.commit()
    conn.close()
    print("✅ Дубли групп очищены и ссылки обновлены!")

if __name__ == "__main__":
    clean_group_duplicates() 