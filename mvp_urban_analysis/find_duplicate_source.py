#!/usr/bin/env python3
"""
Поиск источника дубликата resident_complexes
"""
import sqlite3

def analyze_duplicate():
    """Анализирует и удаляет дубликат"""
    print("🔍 АНАЛИЗ ДУБЛИКАТА resident_complexes")
    print("=" * 40)
    
    conn = sqlite3.connect('urban_analysis_fixed.db')
    cursor = conn.cursor()
    
    # Проверяем есть ли объекты с этой группой
    cursor.execute('''
        SELECT COUNT(*) FROM objects 
        WHERE group_id = (
            SELECT id FROM object_groups 
            WHERE group_type = "resident_complexes"
        )
    ''')
    objects_count = cursor.fetchone()[0]
    print(f'Объектов с resident_complexes: {objects_count}')
    
    # Проверяем detected_groups
    cursor.execute('SELECT COUNT(*) FROM detected_groups WHERE group_type = "resident_complexes"')
    detected_count = cursor.fetchone()[0]
    print(f'detected_groups с resident_complexes: {detected_count}')
    
    # Проверяем ID групп
    cursor.execute('SELECT id, group_name FROM object_groups WHERE group_type LIKE "%resident%"')
    groups = cursor.fetchall()
    print(f'\nВсе группы с "resident":')
    for group_id, name in groups:
        print(f'  ID:{group_id} -> {name}')
    
    # Удаляем дубликат
    print(f'\n=== УДАЛЕНИЕ ===')
    cursor.execute('DELETE FROM object_groups WHERE group_type = "resident_complexes"')
    deleted1 = cursor.rowcount
    cursor.execute('DELETE FROM detected_groups WHERE group_type = "resident_complexes"')
    deleted2 = cursor.rowcount
    
    print(f'Удалено: {deleted1} из object_groups, {deleted2} из detected_groups')
    
    conn.commit()
    
    # Финальная проверка
    cursor.execute('SELECT COUNT(*) FROM object_groups')
    final_count = cursor.fetchone()[0]
    print(f'\n✅ Итого групп: {final_count}')
    
    conn.close()
    return final_count == 8

def check_potential_sources():
    """Проверяем потенциальные источники создания дубликата"""
    print("\n" + "=" * 40)
    print("🔍 ПОИСК ИСТОЧНИКОВ ПРОБЛЕМЫ")
    
    import os
    import re
    
    # Ищем файлы которые могли запускаться недавно
    problematic_patterns = [
        r'resident_complexes',
        r'create.*group.*resident',
        r'INSERT.*resident_complexes'
    ]
    
    python_files = []
    for root, dirs, files in os.walk('.'):
        # Пропускаем некоторые папки
        dirs[:] = [d for d in dirs if not d.startswith('.') and d != '__pycache__']
        for file in files:
            if file.endswith('.py'):
                python_files.append(os.path.join(root, file))
    
    print(f"Проверяем {len(python_files)} Python файлов...")
    
    suspicious_files = []
    for file_path in python_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                for pattern in problematic_patterns:
                    if re.search(pattern, content, re.IGNORECASE):
                        suspicious_files.append(file_path)
                        break
        except:
            pass
    
    print(f"\n🚨 Подозрительные файлы ({len(suspicious_files)}):")
    for file_path in suspicious_files[:10]:  # Показываем первые 10
        print(f"  {file_path}")
    
    if len(suspicious_files) > 10:
        print(f"  ... и еще {len(suspicious_files) - 10} файлов")

if __name__ == "__main__":
    success = analyze_duplicate()
    if success:
        print("✅ БД очищена!")
        check_potential_sources()
    else:
        print("❌ Что-то пошло не так")