#!/usr/bin/env python3
"""
Проверка проблем с названиями групп и detected_groups
"""
import sqlite3

def check_group_naming_issues():
    """Проверяет проблемы с названиями групп"""
    print("=== ПРОВЕРКА ПРОБЛЕМ С НАЗВАНИЯМИ ГРУПП ===")
    
    try:
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        # 1. Проверяем detected_groups
        print("1. Проверка detected_groups:")
        cursor.execute("SELECT COUNT(*) FROM detected_groups")
        detected_count = cursor.fetchone()[0]
        print(f"   Записей в detected_groups: {detected_count}")
        
        if detected_count > 0:
            cursor.execute("SELECT * FROM detected_groups LIMIT 5")
            detected = cursor.fetchall()
            for row in detected:
                print(f"     {row}")
        
        # 2. Проверяем object_groups
        print("\n2. Проверка object_groups:")
        cursor.execute("SELECT group_name, group_type FROM object_groups")
        groups = cursor.fetchall()
        
        for group_name, group_type in groups:
            print(f"   {group_name} -> {group_type}")
        
        # 3. Проверяем, какие группы используются в коде
        print("\n3. Группы, используемые в приложении:")
        app_groups = [
            'school', 'hospital', 'pharmacy', 'kindergarden', 
            'polyclinic', 'university', 'shopmall', 'resident_complexes'
        ]
        
        for app_group in app_groups:
            cursor.execute("SELECT COUNT(*) FROM object_groups WHERE group_type = ?", (app_group,))
            count = cursor.fetchone()[0]
            status = "✅ найдена" if count > 0 else "❌ НЕ НАЙДЕНА"
            print(f"   {app_group}: {status}")
        
        # 4. Проверяем множественное число
        print("\n4. Проверка множественного числа:")
        plural_groups = [
            'schools', 'hospitals', 'pharmacies', 'kindergardens',
            'polyclinics', 'universities', 'shopping_malls'
        ]
        
        for plural_group in plural_groups:
            cursor.execute("SELECT COUNT(*) FROM object_groups WHERE group_type = ?", (plural_group,))
            count = cursor.fetchone()[0]
            if count > 0:
                print(f"   ❌ Найдено множественное число: {plural_group}")
        
        # 5. Проверяем, есть ли объекты с неправильными группами
        print("\n5. Проверка объектов с неправильными группами:")
        cursor.execute("""
            SELECT o.name, o.detected_group_type, og.group_type
            FROM objects o
            LEFT JOIN object_groups og ON o.detected_group_type = og.group_type
            WHERE og.group_type IS NULL AND o.detected_group_type IS NOT NULL
        """)
        mismatched = cursor.fetchall()
        
        if mismatched:
            print("   Объекты с неправильными группами:")
            for name, detected, expected in mismatched:
                print(f"     {name}: {detected} (ожидалось: {expected})")
        else:
            print("   Все группы объектов корректны")
        
        # 6. Проверяем логику определения групп
        print("\n6. Логика определения групп в коде:")
        print("   В district_detector.py используются:")
        print("     - school (не schools)")
        print("     - hospital (не hospitals)")
        print("     - pharmacy (не pharmacies)")
        print("     - kindergarden (не kindergardens)")
        print("     - polyclinic (не polyclinics)")
        print("     - university (не universities)")
        print("     - shopmall (не shopping_malls)")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_group_naming_issues() 