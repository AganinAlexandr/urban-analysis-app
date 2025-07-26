#!/usr/bin/env python3
"""
Скрипт для проверки структуры базы данных
"""
import sqlite3
import os

def check_database_structure():
    """Проверяет структуру базы данных"""
    db_path = 'urban_analysis_fixed.db'
    
    if not os.path.exists(db_path):
        print(f"❌ База данных '{db_path}' не найдена")
        return
    
    print("=== СТРУКТУРА БАЗЫ ДАННЫХ ===")
    print(f"📁 Файл: {db_path}")
    print(f"📊 Размер: {os.path.getsize(db_path) / 1024:.1f} KB")
    print()
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Получаем список таблиц
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        print(f"📋 Всего таблиц: {len(tables)}")
        print()
        
        for table in tables:
            table_name = table[0]
            print(f"📄 Таблица: {table_name}")
            
            # Получаем структуру таблицы
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            
            print("   Структура:")
            for col in columns:
                col_id, col_name, col_type, not_null, default_val, pk = col
                pk_str = " PRIMARY KEY" if pk else ""
                not_null_str = " NOT NULL" if not_null else ""
                print(f"     • {col_name} ({col_type}){not_null_str}{pk_str}")
            
            # Получаем количество записей
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            print(f"   📊 Записей: {count}")
            print()
        
        # Проверяем внешние ключи
        print("🔗 ВНЕШНИЕ КЛЮЧИ:")
        cursor.execute("PRAGMA foreign_key_list")
        foreign_keys = cursor.fetchall()
        
        if foreign_keys:
            for fk in foreign_keys:
                table_name, id, seq, fk_table, from_col, to_col, on_update, on_delete, match = fk
                print(f"   • {table_name}.{from_col} → {fk_table}.{to_col}")
        else:
            print("   Нет внешних ключей")
        
        print()
        
        # Проверяем индексы
        print("📈 ИНДЕКСЫ:")
        cursor.execute("SELECT name, tbl_name, sql FROM sqlite_master WHERE type='index'")
        indexes = cursor.fetchall()
        
        if indexes:
            for idx in indexes:
                idx_name, tbl_name, sql = idx
                print(f"   • {idx_name} (таблица: {tbl_name})")
        else:
            print("   Нет индексов")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка при проверке БД: {e}")

if __name__ == "__main__":
    check_database_structure() 