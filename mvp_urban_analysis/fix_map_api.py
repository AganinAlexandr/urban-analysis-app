#!/usr/bin/env python3
"""
Исправление API карты для работы с БД
"""

def fix_map_api():
    """Исправляет API карты для работы с данными из БД"""
    print("=== ИСПРАВЛЕНИЕ API КАРТЫ ===")
    
    # Читаем файл
    with open('app.py', 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Заменяем строки
    old_string = "'group': row.get('group_supplier', row.get('group', '')),"
    new_string = "'group': row.get('group_supplier', row.get('group', row.get('group_type', ''))),"
    
    # Заменяем все вхождения
    content = content.replace(old_string, new_string)
    
    # Записываем обратно
    with open('app.py', 'w', encoding='utf-8') as f:
        f.write(content)
    
    print("✅ API карты исправлен!")
    print("   Теперь поле 'group' будет браться из 'group_type' если нет 'group_supplier' или 'group'")

if __name__ == "__main__":
    fix_map_api() 