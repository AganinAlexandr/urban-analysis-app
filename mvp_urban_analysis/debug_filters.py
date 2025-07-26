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
                'name': 'С фильтрами schools,universities',
                'params': {
                    'group_type': 'supplier',
                    'filters': 'schools,universities',
                    'data_source': 'database'
                }
            },
            {
                'name': 'С фильтрами schools',
                'params': {
                    'group_type': 'supplier',
                    'filters': 'schools',
                    'data_source': 'database'
                }
            },
            {
                'name': 'С фильтрами universities',
                'params': {
                    'group_type': 'supplier',
                    'filters': 'universities',
                    'data_source': 'database'
                }
            }
        ]
        
        for test_case in test_cases:
            print(f"\n--- {test_case['name']} ---")
            
            url = "http://localhost:5000/map/data"
            response = requests.get(url, params=test_case['params'])
            
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
                        
                        # Показываем цвета первых объектов
                        for i, obj in enumerate(points[:1]):
                            print(f"    Объект: {obj.get('name')}")
                            print(f"      Цвет: {obj.get('color')}")
                    
                    print(f"Всего объектов: {total_objects}")
                else:
                    print("❌ Нет данных 'archive'")
            else:
                print(f"❌ Ошибка: {response.text}")
                
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    debug_filters() 