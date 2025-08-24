#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для исправления названий методов в базе данных
"""

import sqlite3

def fix_method_names():
    """Исправляем названия методов в базе данных"""
    
    conn = sqlite3.connect('urban_analysis_fixed.db')
    cursor = conn.cursor()
    
    print("=== ИСПРАВЛЕНИЕ НАЗВАНИЙ МЕТОДОВ ===\n")
    
    # 1. Показываем текущие методы
    print("1. Текущие методы в базе:")
    cursor.execute("SELECT id, method_name, description FROM processing_methods ORDER BY id")
    methods = cursor.fetchall()
    
    for method in methods:
        print(f"  ID {method[0]}: {method[1]} - {method[2]}")
    
    print()
    
    # 2. Проверяем, какие методы нужно переименовать
    print("2. Анализ несоответствий:")
    
    # Словарь соответствий: старое_название -> новое_название
    method_mapping = {
        'llm_yandex': 'yandex_gpt',
        'llm_openai': 'openai_gpt', 
        'llm_gemini': 'google_gemini',
        'llm_gigachat': 'gigachat',
        'llm_deepseek': 'deepseek_chat'
    }
    
    methods_to_rename = []
    for old_name, new_name in method_mapping.items():
        cursor.execute("SELECT COUNT(*) FROM processing_methods WHERE method_name = ?", (old_name,))
        if cursor.fetchone()[0] > 0:
            methods_to_rename.append((old_name, new_name))
            print(f"  ❌ {old_name} -> {new_name}")
        else:
            print(f"  ✅ {old_name} уже переименован или не существует")
    
    print()
    
    if not methods_to_rename:
        print("✅ Все методы уже имеют правильные названия!")
        conn.close()
        return
    
    # 3. Переименовываем методы
    print("3. Переименование методов:")
    
    for old_name, new_name in methods_to_rename:
        try:
            cursor.execute("UPDATE processing_methods SET method_name = ? WHERE method_name = ?", 
                         (new_name, old_name))
            print(f"  ✅ {old_name} -> {new_name}")
        except Exception as e:
            print(f"  ❌ Ошибка переименования {old_name}: {e}")
    
    # Сохраняем изменения
    conn.commit()
    
    print()
    
    # 4. Проверяем результат
    print("4. Результат после переименования:")
    cursor.execute("SELECT id, method_name, description FROM processing_methods ORDER BY id")
    updated_methods = cursor.fetchall()
    
    for method in updated_methods:
        print(f"  ID {method[0]}: {method[1]} - {method[2]}")
    
    print()
    
    # 5. Проверяем, есть ли результаты анализа для переименованных методов
    print("5. Проверка результатов анализа:")
    
    for old_name, new_name in methods_to_rename:
        cursor.execute("""
            SELECT COUNT(*) FROM analysis_results ar
            JOIN processing_methods pm ON ar.method_id = pm.id
            WHERE pm.method_name = ?
        """, (new_name,))
        
        count = cursor.fetchone()[0]
        print(f"  {new_name}: {count} результатов")
    
    conn.close()
    print("\n✅ Переименование завершено!")

if __name__ == "__main__":
    fix_method_names()






