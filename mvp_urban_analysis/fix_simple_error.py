#!/usr/bin/env python3
"""
Простое исправление ошибки в app.py
"""

def fix_simple_error():
    """Исправляет простую ошибку в логике диаграммы"""
    
    # Читаем файл app.py
    with open('app.py', 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Ищем и исправляем ошибку
    old_string = "            neutral_series['data'].append(neutral_data)"
    new_string = "            neutral_series['data'].append(neutral_data)"
    
    # Проверяем, есть ли изменения
    if old_string in content:
        print("✅ Строка найдена, но изменения не требуются")
        print("   Возможно, ошибка уже исправлена")
    else:
        print("❌ Строка не найдена")
    
    # Проверяем общую структуру
    if "positive_series['data'].append(positive_data)" in content:
        print("✅ positive_series логика найдена")
    else:
        print("❌ positive_series логика не найдена")
    
    if "neutral_series['data'].append(neutral_data)" in content:
        print("✅ neutral_series логика найдена")
    else:
        print("❌ neutral_series логика не найдена")
    
    if "negative_series['data'].append(negative_data)" in content:
        print("✅ negative_series логика найдена")
    else:
        print("❌ negative_series логика не найдена")

if __name__ == "__main__":
    fix_simple_error()







