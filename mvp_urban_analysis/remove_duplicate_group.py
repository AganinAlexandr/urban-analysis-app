#!/usr/bin/env python3
"""
Удаление дубликата resident_complexes
"""
import sqlite3

def remove_duplicate():
    """Удаляет дубликат resident_complexes"""
    print("🗑️ УДАЛЕНИЕ ДУБЛИКАТА resident_complexes")
    print("=" * 40)
    
    conn = sqlite3.connect('urban_analysis_fixed.db')
    cursor = conn.cursor()
    
    # Удаляем из object_groups
    cursor.execute('DELETE FROM object_groups WHERE group_type = "resident_complexes"')
    deleted_groups = cursor.rowcount
    print(f'✅ Удалено {deleted_groups} записей из object_groups')
    
    # Удаляем из detected_groups  
    cursor.execute('DELETE FROM detected_groups WHERE group_type = "resident_complexes"')
    deleted_detected = cursor.rowcount
    print(f'✅ Удалено {deleted_detected} записей из detected_groups')
    
    conn.commit()
    
    # Проверяем результат
    cursor.execute('SELECT group_type, group_name FROM object_groups ORDER BY group_type')
    groups = cursor.fetchall()
    
    print(f'\n=== ФИНАЛЬНЫЙ РЕЗУЛЬТАТ ===')
    print(f'Групп в БД: {len(groups)}')
    for group_type, group_name in groups:
        print(f'  {group_type} -> {group_name}')
    
    conn.close()
    print('\n🎉 ДУБЛИКАТ УДАЛЕН!')
    
    return len(groups) == 8

if __name__ == "__main__":
    success = remove_duplicate()
    if success:
        print("✅ Теперь в БД ровно 8 групп!")
    else:
        print("❌ Что-то пошло не так")