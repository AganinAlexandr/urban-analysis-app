#!/usr/bin/env python3
"""
Детальная отладка ответа API карты
"""
import requests
import json

def debug_api_response():
    """Детально отлаживает ответ API карты"""
    print("=== ДЕТАЛЬНАЯ ОТЛАДКА ОТВЕТА API ===")
    
    try:
        # Тестируем API endpoint карты
        response = requests.get('http://localhost:5000/map/data')
        
        print(f"Статус ответа: {response.status_code}")
        print(f"Заголовки: {dict(response.headers)}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"\nСтруктура ответа:")
            print(f"  Ключи: {list(data.keys())}")
            
            if 'archive' in data:
                print(f"  archive: {len(data['archive'])} групп")
                for i, group in enumerate(data['archive']):
                    print(f"    Группа {i+1}: {group.get('group', 'N/A')} - {len(group.get('points', []))} точек")
                    
                    if group.get('points'):
                        print(f"      Примеры точек:")
                        for j, point in enumerate(group['points'][:2]):
                            print(f"        {j+1}. {point.get('name', 'N/A')} - ({point.get('latitude')}, {point.get('longitude')})")
            
            if 'new' in data:
                print(f"  new: {len(data['new'])} элементов")
            
            if 'error' in data:
                print(f"  error: {data['error']}")
                
            # Проверяем с параметрами
            print(f"\nПроверяем с параметрами...")
            
            # Тест режима "от поставщика"
            response_supplier = requests.get('http://localhost:5000/map/data?mode=supplier')
            if response_supplier.status_code == 200:
                data_supplier = response_supplier.json()
                print(f"Режим 'от поставщика':")
                print(f"  archive: {len(data_supplier.get('archive', []))} групп")
                if data_supplier.get('archive'):
                    for group in data_supplier['archive']:
                        print(f"    {group.get('group', 'N/A')}: {len(group.get('points', []))} точек")
            else:
                print(f"❌ Ошибка режима 'от поставщика': {response_supplier.status_code}")
            
            # Тест режима "определенные"
            response_determined = requests.get('http://localhost:5000/map/data?mode=determined')
            if response_determined.status_code == 200:
                data_determined = response_determined.json()
                print(f"Режим 'определенные':")
                print(f"  archive: {len(data_determined.get('archive', []))} групп")
                if data_determined.get('archive'):
                    for group in data_determined['archive']:
                        print(f"    {group.get('group', 'N/A')}: {len(group.get('points', []))} точек")
            else:
                print(f"❌ Ошибка режима 'определенные': {response_determined.status_code}")
                
        else:
            print(f"❌ Ошибка API: {response.status_code}")
            print(f"Ответ: {response.text}")
            
    except requests.exceptions.ConnectionError:
        print("❌ Не удалось подключиться к серверу")
        print("Убедитесь, что приложение запущено на http://localhost:5000")
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    debug_api_response() 