#!/usr/bin/env python3
"""
Исправление названий групп с множественного числа на единственное
"""
import sqlite3

def fix_group_naming():
    """Исправляет названия групп в БД"""
    print("=== ИСПРАВЛЕНИЕ НАЗВАНИЙ ГРУПП ===")
    
    try:
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        # 1. Текущее состояние
        print("1. Текущие группы (множественное число):")
        cursor.execute("SELECT group_name, group_type FROM object_groups")
        current_groups = cursor.fetchall()
        
        for group_name, group_type in current_groups:
            print(f"   {group_name} -> {group_type}")
        
        # 2. Маппинг для исправления
        group_mapping = {
            'schools': 'school',
            'hospitals': 'hospital', 
            'pharmacies': 'pharmacy',
            'kindergartens': 'kindergarden',  # исправляем опечатку
            'polyclinics': 'polyclinic',
            'universities': 'university',
            'shopping_malls': 'shopmall'
        }
        
        print("\n2. Маппинг для исправления:")
        for old, new in group_mapping.items():
            print(f"   {old} -> {new}")
        
        # 3. Исправляем группы
        print("\n3. Исправление групп:")
        for old_group, new_group in group_mapping.items():
            cursor.execute("""
                UPDATE object_groups 
                SET group_type = ? 
                WHERE group_type = ?
            """, (new_group, old_group))
            
            affected = cursor.rowcount
            if affected > 0:
                print(f"   {old_group} -> {new_group}: обновлено {affected} записей")
        
        # 4. Добавляем недостающие группы
        print("\n4. Добавление недостающих групп:")
        missing_groups = [
            ('Жилые комплексы', 'resident_complexes')
        ]
        
        for group_name, group_type in missing_groups:
            cursor.execute("""
                INSERT INTO object_groups (group_name, group_type, created_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
            """, (group_name, group_type))
            print(f"   Добавлена: {group_name} -> {group_type}")
        
        # 5. Проверяем результат
        print("\n5. Результат после исправления:")
        cursor.execute("SELECT group_name, group_type FROM object_groups ORDER BY group_type")
        fixed_groups = cursor.fetchall()
        
        for group_name, group_type in fixed_groups:
            print(f"   {group_name} -> {group_type}")
        
        # 6. Проверяем соответствие с кодом приложения
        print("\n6. Проверка соответствия с кодом приложения:")
        app_groups = [
            'school', 'hospital', 'pharmacy', 'kindergarden', 
            'polyclinic', 'university', 'shopmall', 'resident_complexes'
        ]
        
        for app_group in app_groups:
            cursor.execute("SELECT COUNT(*) FROM object_groups WHERE group_type = ?", (app_group,))
            count = cursor.fetchone()[0]
            status = "✅ найдена" if count > 0 else "❌ НЕ НАЙДЕНА"
            print(f"   {app_group}: {status}")
        
        conn.commit()
        conn.close()
        
        print("\n✅ Исправление названий групп завершено!")
        print("   Теперь все группы используют единственное число")
        print("   и соответствуют коду приложения")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    fix_group_naming() 