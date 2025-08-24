#!/usr/bin/env python3
"""
Отладка создания групп - добавляем логирование
"""
import sqlite3

def patch_database_manager():
    """Добавляем логирование в database_fixed.py"""
    print("🔧 ПАТЧИНГ СИСТЕМЫ ДЛЯ ОТЛАДКИ")
    print("=" * 40)
    
    # Читаем текущий файл
    with open('app/core/database_fixed.py', 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Ищем функцию create_group
    if 'print(f"🚨 СОЗДАЕМ ГРУППУ:' not in content:
        # Добавляем логирование в create_group
        old_create_group = '''    def create_group(self, group_name: str, group_type: str) -> int:
        """Создание группы в таблице object_groups"""
        with self.get_connection() as conn:
            cursor = conn.execute("""
                INSERT INTO object_groups (group_name, group_type)
                VALUES (?, ?)
            """, (group_name, group_type))
            return cursor.lastrowid'''
        
        new_create_group = '''    def create_group(self, group_name: str, group_type: str) -> int:
        """Создание группы в таблице object_groups"""
        print(f"🚨 СОЗДАЕМ ГРУППУ: '{group_name}' (type: {group_type})")
        import traceback
        traceback.print_stack()
        with self.get_connection() as conn:
            cursor = conn.execute("""
                INSERT INTO object_groups (group_name, group_type)
                VALUES (?, ?)
            """, (group_name, group_type))
            return cursor.lastrowid'''
        
        content = content.replace(old_create_group, new_create_group)
        
        # Записываем обратно
        with open('app/core/database_fixed.py', 'w', encoding='utf-8') as f:
            f.write(content)
        
        print("✅ Добавлено логирование в create_group")
    else:
        print("✅ Логирование уже добавлено")

def remove_current_duplicate():
    """Удаляем текущий дубликат"""
    print("\n=== УДАЛЕНИЕ ТЕКУЩЕГО ДУБЛИКАТА ===")
    
    conn = sqlite3.connect('urban_analysis_fixed.db')
    cursor = conn.cursor()
    
    cursor.execute('DELETE FROM object_groups WHERE group_type = "resident_complexes"')
    deleted = cursor.rowcount
    print(f"✅ Удалено {deleted} дубликатов")
    
    conn.commit()
    conn.close()
    
    return deleted > 0

if __name__ == "__main__":
    patch_database_manager()
    remove_current_duplicate()
    
    print("\n" + "=" * 40)
    print("🎯 ИНСТРУКЦИЯ:")
    print("1. Перезапустите приложение: python app.py")
    print("2. Откройте браузер на localhost:5000")
    print("3. Смотрите в терминал - появится стек-трейс при создании группы")
    print("4. Найдем точное место где создается resident_complexes!")