#!/usr/bin/env python3
"""
Скрипт для отладки проблемы с выпадающим списком в модальном окне
"""

import requests
import json

def test_api_groups():
    """Тестируем API групп"""
    print("🔍 ТЕСТИРОВАНИЕ API /api/object-groups")
    print("=" * 50)
    
    try:
        response = requests.get('http://localhost:5000/api/object-groups')
        print(f"Статус код: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"Получено групп: {len(data)}")
            print("\nСписок групп:")
            for i, group in enumerate(data, 1):
                print(f"  {i}. type: '{group.get('type')}', name: '{group.get('name')}'")
            
            # Проверяем формат данных
            print(f"\nФормат первой группы:")
            if data:
                first_group = data[0]
                print(f"  Ключи: {list(first_group.keys())}")
                print(f"  Значения: {first_group}")
        else:
            print(f"❌ Ошибка API: {response.status_code}")
            print(f"❌ Ответ: {response.text}")
            
    except Exception as e:
        print(f"❌ Ошибка запроса: {e}")

def check_html_datalist():
    """Проверяем HTML шаблон на наличие проблем с datalist"""
    print("\n🔍 ПРОВЕРКА HTML ШАБЛОНА")
    print("=" * 50)
    
    html_file = 'templates/index.html'
    
    try:
        with open(html_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Ищем datalist для userGroup
        datalist_start = content.find('<datalist id="groupList">')
        if datalist_start == -1:
            print("❌ Не найден <datalist id=\"groupList\">")
            return
        
        datalist_end = content.find('</datalist>', datalist_start)
        if datalist_end == -1:
            print("❌ Не найден закрывающий </datalist>")
            return
        
        datalist_content = content[datalist_start:datalist_end + 11]
        print("✅ Найден datalist:")
        print(datalist_content[:500] + "..." if len(datalist_content) > 500 else datalist_content)
        
        # Ищем JavaScript код заполнения datalist
        js_patterns = [
            'groupList.innerHTML',
            'populateGroupDatalist',
            'availableObjectGroups'
        ]
        
        print(f"\n🔍 Поиск JavaScript кода заполнения:")
        for pattern in js_patterns:
            count = content.count(pattern)
            print(f"  '{pattern}': найдено {count} раз")
            
    except Exception as e:
        print(f"❌ Ошибка чтения HTML: {e}")

if __name__ == "__main__":
    print("🚀 ОТЛАДКА ПРОБЛЕМЫ С ВЫПАДАЮЩИМ СПИСКОМ")
    print("=" * 60)
    
    test_api_groups()
    check_html_datalist()
    
    print("\n📋 РЕКОМЕНДАЦИИ:")
    print("1. Убедитесь что Flask приложение запущено")
    print("2. Откройте браузер на localhost:5000")
    print("3. Откройте DevTools (F12) -> Console")
    print("4. Попробуйте загрузить файл и посмотрите на console.log")
    print("5. Проверьте что window.availableObjectGroups содержит все группы")