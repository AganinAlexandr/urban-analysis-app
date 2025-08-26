#!/usr/bin/env python3
"""
Исправление путаницы с названиями групп в базе данных
"""

import sqlite3

def fix_group_names():
    """Исправляет названия групп в базе данных"""
    print("🔧 ИСПРАВЛЕНИЕ НАЗВАНИЙ ГРУПП В БД")
    print("=" * 50)
    
    conn = sqlite3.connect('urban_analysis_fixed.db')
    cursor = conn.cursor()
    
    try:
        # 1. Показываем текущее состояние
        print("\n1. ТЕКУЩЕЕ СОСТОЯНИЕ:")
        cursor.execute("SELECT id, group_name, group_type FROM object_groups ORDER BY group_type")
        groups = cursor.fetchall()
        
        for group in groups:
            print(f"  ID: {group[0]}, group_name: '{group[1]}', group_type: '{group[2]}'")
        
        # 2. Создаем правильную структуру групп
        print("\n2. СОЗДАНИЕ ПРАВИЛЬНОЙ СТРУКТУРЫ:")
        
        # Удаляем все существующие группы
        cursor.execute("DELETE FROM object_groups")
        print("  ✅ Удалены все существующие группы")
        
        # Создаем правильные группы
        correct_groups = [
            ('hospital', 'Больница', 'Медицинские учреждения'),
            ('school', 'Школа', 'Образовательные учреждения'),
            ('kindergarden', 'Детский сад', 'Дошкольные учреждения'),
            ('polyclinic', 'Поликлиника', 'Амбулаторные медицинские учреждения'),
            ('pharmacy', 'Аптека', 'Фармацевтические учреждения'),
            ('shopmall', 'Торговый центр', 'Торговые комплексы'),
            ('university', 'Университет', 'Высшие учебные заведения'),
            ('resident_complex', 'Жилой комплекс', 'Жилые комплексы')
        ]
        
        for group_name, group_type, description in correct_groups:
            cursor.execute("""
                INSERT INTO object_groups (group_name, group_type, description)
                VALUES (?, ?, ?)
            """, (group_name, group_type, description))
            print(f"  ✅ Создана группа: {group_name} ({group_type})")
        
        # 3. Обновляем связи объектов с группами
        print("\n3. ОБНОВЛЕНИЕ СВЯЗЕЙ ОБЪЕКТОВ:")
        
        # Получаем все объекты
        cursor.execute("SELECT id, name FROM objects")
        objects = cursor.fetchall()
        
        # Обновляем group_id для каждого объекта
        updated_count = 0
        for obj_id, obj_name in objects:
            # Определяем группу по названию объекта
            group_name = None
            
            if any(word in obj_name.lower() for word in ['больница', 'госпиталь', 'медицинский']):
                group_name = 'hospital'
            elif any(word in obj_name.lower() for word in ['школа', 'лицей', 'гимназия']):
                group_name = 'school'
            elif any(word in obj_name.lower() for word in ['детский сад', 'сад']):
                group_name = 'kindergarden'
            elif any(word in obj_name.lower() for word in ['поликлиника', 'амбулатория']):
                group_name = 'polyclinic'
            elif any(word in obj_name.lower() for word in ['аптека', 'фармация']):
                group_name = 'pharmacy'
            elif any(word in obj_name.lower() for word in ['торговый', 'молл', 'центр', 'магазин']):
                group_name = 'shopmall'
            elif any(word in obj_name.lower() for word in ['университет', 'институт', 'академия', 'вуз']):
                group_name = 'university'
            elif any(word in obj_name.lower() for word in ['жилой', 'комплекс', 'дом']):
                group_name = 'resident_complex'
            
            if group_name:
                # Получаем ID группы
                cursor.execute("SELECT id FROM object_groups WHERE group_name = ?", (group_name,))
                group_result = cursor.fetchone()
                
                if group_result:
                    group_id = group_result[0]
                    cursor.execute("UPDATE objects SET group_id = ? WHERE id = ?", (group_id, obj_id))
                    updated_count += 1
                    print(f"  ✅ Объект '{obj_name[:30]}...' -> группа {group_name}")
                else:
                    print(f"  ❌ Группа {group_name} не найдена для объекта '{obj_name[:30]}...'")
            else:
                print(f"  ⚠️  Не удалось определить группу для объекта '{obj_name[:30]}...'")
        
        print(f"\n  📊 Обновлено объектов: {updated_count}")
        
        # 4. Проверяем результат
        print("\n4. ПРОВЕРКА РЕЗУЛЬТАТА:")
        cursor.execute("""
            SELECT og.group_name, og.group_type, COUNT(o.id) as object_count
            FROM object_groups og
            LEFT JOIN objects o ON og.id = o.group_id
            GROUP BY og.id, og.group_name, og.group_type
            ORDER BY og.group_name
        """)
        
        result_groups = cursor.fetchall()
        for group_name, group_type, count in result_groups:
            print(f"  {group_name} ({group_type}): {count} объектов")
        
        # 5. Фиксируем изменения
        conn.commit()
        print("\n✅ ИСПРАВЛЕНИЕ ЗАВЕРШЕНО!")
        
    except Exception as e:
        print(f"\n❌ ОШИБКА: {str(e)}")
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    fix_group_names()







