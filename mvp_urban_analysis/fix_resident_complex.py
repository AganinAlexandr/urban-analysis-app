#!/usr/bin/env python3
"""
Исправляем "Жилые комплексы" на "Жилой комплекс" в единственном числе
"""
import sqlite3

def fix_resident_complex():
    """Исправляем название жилых комплексов"""
    print("=== ИСПРАВЛЕНИЕ НАЗВАНИЯ ГРУППЫ ===")
    
    try:
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        # Обновляем название в object_groups
        cursor.execute("UPDATE object_groups SET group_name = 'Жилой комплекс' WHERE group_type = 'resident_complexes'")
        print(f'✅ Обновлено {cursor.rowcount} записей в object_groups')
        
        # Обновляем название в detected_groups (используем тот же тип, но другое имя)
        cursor.execute("UPDATE detected_groups SET group_name = 'resident_complex' WHERE group_type = 'resident_complexes'")
        print(f'✅ Обновлено {cursor.rowcount} записей в detected_groups')
        
        conn.commit()
        
        # Проверяем результат
        cursor.execute("SELECT group_type, group_name FROM object_groups WHERE group_type = 'resident_complexes'")
        result = cursor.fetchone()
        print(f'Результат в object_groups: {result}')
        
        cursor.execute("SELECT group_type, group_name FROM detected_groups WHERE group_type = 'resident_complexes'")
        result2 = cursor.fetchone()
        print(f'Результат в detected_groups: {result2}')
        
        conn.close()
        print('\n🎉 Исправлено на единственное число!')
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    fix_resident_complex()