#!/usr/bin/env python3
"""
Полная очистка от групп во множественном числе
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.database_fixed import db_manager_fixed
import sqlite3

def cleanup_plural_groups():
    """Очищаем БД от групп во множественном числе"""
    print("=== ОЧИСТКА ГРУПП ВО МНОЖЕСТВЕННОМ ЧИСЛЕ ===")
    
    try:
        # 1. Показываем текущее состояние БД
        print("\n1. Текущие группы в object_groups:")
        with db_manager_fixed.get_connection() as conn:
            cursor = conn.execute("SELECT id, group_name, group_type FROM object_groups ORDER BY group_type")
            groups = cursor.fetchall()
            plural_groups = []
            singular_groups = []
            
            for group in groups:
                print(f"  ID: {group['id']}, Name: {group['group_name']}, Type: {group['group_type']}")
                if group['group_type'] in ['schools', 'hospitals', 'kindergartens', 'polyclinics', 'pharmacies', 'universities', 'shopping_malls']:
                    plural_groups.append(group)
                else:
                    singular_groups.append(group)
        
        print(f"\nНайдено {len(plural_groups)} групп во множественном числе")
        print(f"Найдено {len(singular_groups)} групп в единственном числе")
        
        # 2. Проверяем detected_groups
        print("\n2. Текущие группы в detected_groups:")
        with db_manager_fixed.get_connection() as conn:
            cursor = conn.execute("SELECT id, group_name, group_type FROM detected_groups ORDER BY group_type")
            detected_groups = cursor.fetchall()
            plural_detected = []
            singular_detected = []
            
            for group in detected_groups:
                print(f"  ID: {group['id']}, Name: {group['group_name']}, Type: {group['group_type']}")
                if group['group_type'] in ['schools', 'hospitals', 'kindergartens', 'polyclinics', 'pharmacies', 'universities', 'shopping_malls']:
                    plural_detected.append(group)
                else:
                    singular_detected.append(group)
        
        print(f"\nНайдено {len(plural_detected)} определяемых групп во множественном числе")
        print(f"Найдено {len(singular_detected)} определяемых групп в единственном числе")
        
        # 3. Маппинг для преобразования
        plural_to_singular = {
            'schools': 'school',
            'hospitals': 'hospital', 
            'kindergartens': 'kindergarden',
            'polyclinics': 'polyclinic',
            'pharmacies': 'pharmacy',
            'universities': 'university',
            'shopping_malls': 'shopmall'
        }
        
        # 4. Обновляем ссылки в objects на правильные группы
        print("\n3. Обновляем ссылки в objects:")
        with db_manager_fixed.get_connection() as conn:
            for plural, singular in plural_to_singular.items():
                # Находим ID единственного числа
                cursor = conn.execute("SELECT id FROM object_groups WHERE group_type = ?", (singular,))
                singular_result = cursor.fetchone()
                if not singular_result:
                    print(f"  ⚠️ Не найдена группа {singular} в единственном числе")
                    continue
                singular_id = singular_result['id']
                
                # Находим ID множественного числа
                cursor = conn.execute("SELECT id FROM object_groups WHERE group_type = ?", (plural,))
                plural_result = cursor.fetchone()
                if not plural_result:
                    continue
                plural_id = plural_result['id']
                
                # Обновляем объекты
                cursor = conn.execute("UPDATE objects SET group_id = ? WHERE group_id = ?", (singular_id, plural_id))
                updated_objects = cursor.rowcount
                
                print(f"  ✅ Обновлено {updated_objects} объектов: {plural} → {singular}")
        
        # 5. Аналогично для detected_groups
        print("\n4. Обновляем ссылки в objects для detected_groups:")
        with db_manager_fixed.get_connection() as conn:
            for plural, singular in plural_to_singular.items():
                # Находим ID единственного числа
                cursor = conn.execute("SELECT id FROM detected_groups WHERE group_type = ?", (singular,))
                singular_result = cursor.fetchone()
                if not singular_result:
                    print(f"  ⚠️ Не найдена определяемая группа {singular} в единственном числе")
                    continue
                singular_id = singular_result['id']
                
                # Находим ID множественного числа
                cursor = conn.execute("SELECT id FROM detected_groups WHERE group_type = ?", (plural,))
                plural_result = cursor.fetchone()
                if not plural_result:
                    continue
                plural_id = plural_result['id']
                
                # Обновляем объекты
                cursor = conn.execute("UPDATE objects SET detected_group_id = ? WHERE detected_group_id = ?", (singular_id, plural_id))
                updated_objects = cursor.rowcount
                
                print(f"  ✅ Обновлено {updated_objects} объектов: {plural} → {singular}")
        
        # 6. Удаляем группы во множественном числе
        print("\n5. Удаляем группы во множественном числе:")
        with db_manager_fixed.get_connection() as conn:
            for plural in plural_to_singular.keys():
                # Удаляем из object_groups
                cursor = conn.execute("DELETE FROM object_groups WHERE group_type = ?", (plural,))
                deleted_groups = cursor.rowcount
                if deleted_groups > 0:
                    print(f"  ✅ Удалена группа {plural} из object_groups")
                
                # Удаляем из detected_groups
                cursor = conn.execute("DELETE FROM detected_groups WHERE group_type = ?", (plural,))
                deleted_detected = cursor.rowcount
                if deleted_detected > 0:
                    print(f"  ✅ Удалена группа {plural} из detected_groups")
        
        # 7. Показываем финальное состояние
        print("\n6. Финальное состояние object_groups:")
        with db_manager_fixed.get_connection() as conn:
            cursor = conn.execute("SELECT id, group_name, group_type FROM object_groups ORDER BY group_type")
            final_groups = cursor.fetchall()
            for group in final_groups:
                print(f"  ID: {group['id']}, Name: {group['group_name']}, Type: {group['group_type']}")
        
        print("\n7. Финальное состояние detected_groups:")
        with db_manager_fixed.get_connection() as conn:
            cursor = conn.execute("SELECT id, group_name, group_type FROM detected_groups ORDER BY group_type")
            final_detected = cursor.fetchall()
            for group in final_detected:
                print(f"  ID: {group['id']}, Name: {group['group_name']}, Type: {group['group_type']}")
        
        print("\n✅ Очистка БД завершена!")
        
    except Exception as e:
        print(f"❌ Ошибка очистки БД: {e}")
        import traceback
        traceback.print_exc()

def update_config_file():
    """Обновляем файл конфигурации"""
    print("\n=== ОБНОВЛЕНИЕ КОНФИГУРАЦИИ ===")
    
    config_path = "app/core/config.py"
    
    try:
        # Читаем файл
        with open(config_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Новая конфигурация групп без множественного числа
        new_group_config = """# Настройки групп
GROUP_CONFIG = {
    'supplier_groups': ['school', 'hospital', 'pharmacy', 'kindergarden', 'polyclinic', 'university', 'shopmall', 'resident_complexes'],
    'determined_groups': ['school', 'hospital', 'pharmacy', 'kindergarden', 'polyclinic', 'university', 'shopmall', 'resident_complexes'],
    'colors': {
        'school': '#007bff',
        'hospital': '#dc3545', 
        'pharmacy': '#28a745',
        'kindergarden': '#ffc107',
        'polyclinic': '#17a2b8',
        'university': '#6f42c1',
        'shopmall': '#fd7e14',
        'resident_complexes': '#e83e8c'
    }
}"""
        
        # Заменяем старую конфигурацию
        import re
        pattern = r'# Настройки групп\nGROUP_CONFIG = \{.*?\}'
        new_content = re.sub(pattern, new_group_config, content, flags=re.DOTALL)
        
        # Записываем файл
        with open(config_path, 'w', encoding='utf-8') as f:
            f.write(new_content)
        
        print(f"✅ Конфигурация обновлена в {config_path}")
        
    except Exception as e:
        print(f"❌ Ошибка обновления конфигурации: {e}")

def update_html_template():
    """Обновляем HTML шаблон"""
    print("\n=== ОБНОВЛЕНИЕ HTML ШАБЛОНА ===")
    
    html_path = "templates/index.html"
    
    try:
        # Читаем файл
        with open(html_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Заменяем опции во множественном числе
        replacements = {
            '<option value="universities">Университеты</option>': '<option value="university">Университет</option>',
            '<option value="schools">Школы</option>': '<option value="school">Школа</option>',
            '<option value="hospitals">Больницы</option>': '<option value="hospital">Больница</option>',
            '<option value="pharmacies">Аптеки</option>': '<option value="pharmacy">Аптека</option>',
            '<option value="kindergartens">Детские сады</option>': '<option value="kindergarden">Детский сад</option>',
            '<option value="polyclinics">Поликлиники</option>': '<option value="polyclinic">Поликлиника</option>',
            '<option value="shopping_malls">Торговые центры</option>': '<option value="shopmall">Торговый центр</option>'
        }
        
        new_content = content
        for old, new in replacements.items():
            new_content = new_content.replace(old, new)
        
        # Записываем файл
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(new_content)
        
        print(f"✅ HTML шаблон обновлен в {html_path}")
        
    except Exception as e:
        print(f"❌ Ошибка обновления HTML: {e}")

if __name__ == "__main__":
    print("Начинаем полную очистку от групп во множественном числе...")
    
    # 1. Очищаем БД
    cleanup_plural_groups()
    
    # 2. Обновляем конфигурацию
    update_config_file()
    
    # 3. Обновляем HTML
    update_html_template()
    
    print("\n🎉 ПОЛНАЯ ОЧИСТКА ЗАВЕРШЕНА!")
    print("\nТеперь нужно:")
    print("1. Перезапустить приложение")
    print("2. Проверить работу модального окна")
    print("3. Загрузить тестовые данные")