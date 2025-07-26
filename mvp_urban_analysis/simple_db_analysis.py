#!/usr/bin/env python3
"""
Простой анализ структуры базы данных
"""
import sqlite3
import os

def simple_db_analysis():
    """Простой анализ структуры БД"""
    db_path = 'urban_analysis_fixed.db'
    
    if not os.path.exists(db_path):
        print(f"❌ База данных '{db_path}' не найдена")
        return
    
    print("=== АНАЛИЗ СТРУКТУРЫ БАЗЫ ДАННЫХ ===")
    print(f"📁 Файл: {db_path}")
    print(f"📊 Размер: {os.path.getsize(db_path) / 1024:.1f} KB")
    print()
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Получаем список таблиц (исключаем системные)
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name != 'sqlite_sequence'")
        tables = cursor.fetchall()
        
        print(f"📋 КОЛИЧЕСТВО ТАБЛИЦ: {len(tables)}")
        print()
        
        # Основные таблицы
        main_tables = []
        for table in tables:
            table_name = table[0]
            main_tables.append(table_name)
            print(f"📄 {table_name}")
        
        print()
        print("=== СТРУКТУРА ТАБЛИЦ ===")
        
        for table_name in main_tables:
            print(f"\n📄 ТАБЛИЦА: {table_name}")
            
            # Структура
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            
            print("   Структура:")
            for col in columns:
                col_id, col_name, col_type, not_null, default_val, pk = col
                pk_str = " (PK)" if pk else ""
                not_null_str = " NOT NULL" if not_null else ""
                print(f"     • {col_name} ({col_type}){not_null_str}{pk_str}")
            
            # Количество записей
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            print(f"   📊 Записей: {count}")
        
        print()
        print("=== СВЯЗИ МЕЖДУ ТАБЛИЦАМИ ===")
        print("Согласно архитектуре проекта:")
        print("• object_groups (1) ←→ (many) objects")
        print("• detected_groups (1) ←→ (many) objects") 
        print("• objects (1) ←→ (many) reviews")
        print("• reviews (1) ←→ (many) analysis_results")
        print("• processing_methods (1) ←→ (many) analysis_results")
        
        print()
        print("=== СТАТИСТИКА ===")
        total_records = 0
        for table_name in main_tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            total_records += count
            print(f"• {table_name}: {count} записей")
        
        print(f"\n📊 Общее количество записей: {total_records}")
        
        conn.close()
        print("\n✅ Анализ завершен!")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    simple_db_analysis() 