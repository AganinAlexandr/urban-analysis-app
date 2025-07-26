#!/usr/bin/env python3
"""
Отладка фильтров в API карты
"""

import requests
import json

def debug_filters():
    """Отлаживает фильтры в API карты"""
    print("=== ОТЛАДКА ФИЛЬТРОВ ===")
    
    try:
        # Тестируем с разными фильтрами
        test_cases = [
            {
                'name': 'Без фильтров',
                'params': {
                    'group_type': 'supplier',
                    'filters': '',
                    'data_source': 'database'
                }
            },
            {
                'name': 'Только школы',
                'params': {
                    'group_type': 'supplier',
                    'filters': 'school',
                    'data_source': 'database'
                }
            },
            {
                'name': 'Только больницы',
                'params': {
                    'group_type': 'supplier',
                    'filters': 'hospital',
                    'data_source': 'database'
                }
            },
            {
                'name': 'Школы и университеты',
                'params': {
                    'group_type': 'supplier',
                    'filters': 'school,university',
                    'data_source': 'database'
                }
            },
            {
                'name': 'Определенные группы - только школы',
                'params': {
                    'group_type': 'determined',
                    'filters': 'school',
                    'data_source': 'database'
                }
            }
        ]
        
        for test_case in test_cases:
            print(f"\n--- {test_case['name']} ---")
            
            url = "http://localhost:5000/map/data"
            response = requests.get(url, params=test_case['params'])
            
            print(f"URL: {url}")
            print(f"Параметры: {test_case['params']}")
            print(f"Статус: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                
                if 'archive' in data:
                    archive_data = data['archive']
                    print(f"Групп: {len(archive_data)}")
                    
                    total_objects = 0
                    for group in archive_data:
                        group_name = group.get('group', 'unknown')
                        points = group.get('points', [])
                        print(f"  Группа '{group_name}': {len(points)} объектов")
                        total_objects += len(points)
                        
                        # Показываем первые 2 объекта в группе
                        for i, obj in enumerate(points[:2]):
                            print(f"    {i+1}. {obj.get('name')}")
                            print(f"       Координаты: {obj.get('latitude')}, {obj.get('longitude')}")
                            print(f"       Группа от поставщика: {obj.get('group')}")
                            print(f"       Определенная группа: {obj.get('determined_group')}")
                            print(f"       Цвет: {obj.get('color')}")
                    
                    print(f"Всего объектов: {total_objects}")
                    
                    # Проверяем соответствие фильтрам
                    if test_case['params']['filters']:
                        expected_filters = test_case['params']['filters'].split(',')
                        print(f"Ожидаемые фильтры: {expected_filters}")
                        
                        found_groups = [group.get('group') for group in archive_data]
                        print(f"Найденные группы: {found_groups}")
                        
                        # Проверяем, что все найденные группы соответствуют фильтрам
                        all_match = all(group in expected_filters for group in found_groups)
                        if all_match:
                            print("✅ Все группы соответствуют фильтрам")
                        else:
                            print("❌ Некоторые группы не соответствуют фильтрам")
                else:
                    print("❌ Нет данных 'archive'")
            else:
                print(f"❌ Ошибка: {response.text}")
                
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    debug_filters() 