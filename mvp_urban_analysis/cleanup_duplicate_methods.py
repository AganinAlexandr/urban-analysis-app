#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для очистки дублирующихся методов в базе данных
"""

import sqlite3

def cleanup_duplicate_methods():
    """Удаляем дублирующиеся методы и переносим результаты анализа"""
    
    conn = sqlite3.connect('urban_analysis_fixed.db')
    cursor = conn.cursor()
    
    print("=== ОЧИСТКА ДУБЛИРУЮЩИХСЯ МЕТОДОВ ===\n")
    
    # 1. Анализируем текущую ситуацию
    print("1. Текущие методы:")
    cursor.execute("SELECT id, method_name, description FROM processing_methods ORDER BY id")
    methods = cursor.fetchall()
    
    for method in methods:
        print(f"  ID {method[0]}: {method[1]} - {method[2]}")
    
    print()
    
    # 2. Находим семантически дублирующиеся методы
    print("2. Анализ семантического дублирования:")
    
    # Определяем группы семантически одинаковых методов
    semantic_groups = {
        'yandex': ['yandex_gpt', 'llm_yandex'],
        'openai': ['openai_gpt', 'llm_openai'],
        'gemini': ['google_gemini', 'llm_gemini'],
        'gigachat': ['gigachat', 'llm_gigachat'],
        'deepseek': ['deepseek_chat', 'llm_deepseek']
    }
    
    duplicates = {}
    for group_name, method_names in semantic_groups.items():
        found_methods = []
        for method in methods:
            if method[1] in method_names:
                found_methods.append(method)
        
        if len(found_methods) > 1:
            duplicates[group_name] = found_methods
            print(f"  ❌ {group_name}: {len(found_methods)} дубликатов")
            for method in found_methods:
                print(f"      ID {method[0]}: {method[1]}")
        else:
            print(f"  ✅ {group_name}: без дубликатов")
    
    print()
    
    if not duplicates:
        print("✅ Семантически дублирующихся методов не найдено!")
        conn.close()
        return
    
    # 3. Планируем очистку
    print("3. План очистки:")
    
    # Определяем, какие методы оставляем (новые правильные названия)
    keep_methods = {
        'yandex': 'yandex_gpt',      # оставляем yandex_gpt
        'openai': 'openai_gpt',      # оставляем openai_gpt
        'gemini': 'google_gemini',   # оставляем google_gemini
        'gigachat': 'gigachat',      # оставляем gigachat
        'deepseek': 'deepseek_chat'  # оставляем deepseek_chat
    }
    
    for group_name, method_names in semantic_groups.items():
        if group_name in duplicates:
            keep_name = keep_methods[group_name]
            keep_method = None
            old_methods = []
            
            for method in duplicates[group_name]:
                if method[1] == keep_name:
                    keep_method = method
                else:
                    old_methods.append(method)
            
            if keep_method:
                print(f"  {group_name}: оставляем {keep_name} (ID {keep_method[0]}), удаляем: {[m[1] for m in old_methods]}")
    
    print()
    
    # 4. Удаляем дублирующиеся методы
    print("4. Удаление дублирующихся методов...")
    
    for group_name, method_names in semantic_groups.items():
        if group_name in duplicates:
            keep_name = keep_methods[group_name]
            old_methods = []
            
            for method in duplicates[group_name]:
                if method[1] != keep_name:
                    old_methods.append(method)
            
            for old_method in old_methods:
                old_id = old_method[0]
                old_name = old_method[1]
                
                # Считаем результаты для старого метода
                cursor.execute("SELECT COUNT(*) FROM analysis_results WHERE method_id = ?", (old_id,))
                old_count = cursor.fetchone()[0]
                
                if old_count > 0:
                    print(f"  ⚠️  {old_name} (ID {old_id}): имеет {old_count} результатов - ПРОПУСКАЕМ!")
                    continue
                
                # Удаляем старый метод (только если у него нет результатов)
                cursor.execute("DELETE FROM processing_methods WHERE id = ?", (old_id,))
                print(f"  ✅ {old_name} (ID {old_id}): удален")
    
    print()
    
    # 5. Проверяем результат
    print("5. Результат после очистки:")
    cursor.execute("SELECT id, method_name, description FROM processing_methods ORDER BY id")
    updated_methods = cursor.fetchall()
    
    for method in updated_methods:
        print(f"  ID {method[0]}: {method[1]} - {method[2]}")
    
    print()
    
    # 6. Проверяем статистику YandexGPT
    print("6. Проверка статистики YandexGPT:")
    cursor.execute("""
        SELECT COUNT(*) FROM analysis_results ar
        JOIN processing_methods pm ON ar.method_id = pm.id
        WHERE pm.method_name = 'yandex_gpt'
    """)
    
    yandex_count = cursor.fetchone()[0]
    print(f"  Результатов YandexGPT: {yandex_count}")
    
    # Сохраняем изменения
    conn.commit()
    conn.close()
    
    print("\n✅ Очистка завершена!")

if __name__ == "__main__":
    cleanup_duplicate_methods()
