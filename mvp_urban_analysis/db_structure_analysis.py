#!/usr/bin/env python3
"""
Подробный анализ структуры базы данных и связей между таблицами
"""
import sqlite3
import os
from datetime import datetime

def analyze_database_structure():
    """Подробный анализ структуры БД"""
    db_path = 'urban_analysis_fixed.db'
    
    if not os.path.exists(db_path):
        print(f"❌ База данных '{db_path}' не найдена")
        return
    
    print("=== ПОДРОБНЫЙ АНАЛИЗ СТРУКТУРЫ БАЗЫ ДАННЫХ ===")
    print(f"📁 Файл: {db_path}")
    print(f"📊 Размер: {os.path.getsize(db_path) / 1024:.1f} KB")
    print(f"⏰ Время анализа: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Получаем список таблиц
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name != 'sqlite_sequence'")
        tables = cursor.fetchall()
        
        print(f"📋 ОСНОВНЫЕ ТАБЛИЦЫ: {len(tables)}")
        print()
        
        # Анализируем каждую таблицу
        for table in tables:
            table_name = table[0]
            print(f"📄 ТАБЛИЦА: {table_name}")
            
            # Структура таблицы
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            
            print("   📝 Структура:")
            foreign_keys = []
            primary_keys = []
            
            for col in columns:
                col_id, col_name, col_type, not_null, default_val, pk = col
                pk_str = " 🔑 PRIMARY KEY" if pk else ""
                not_null_str = " NOT NULL" if not_null else ""
                default_str = f" DEFAULT {default_val}" if default_val else ""
                
                print(f"     • {col_name} ({col_type}){not_null_str}{default_str}{pk_str}")
                
                if pk:
                    primary_keys.append(col_name)
            
            # Количество записей
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            print(f"   📊 Записей: {count}")
            
            # Проверяем внешние ключи для этой таблицы
            cursor.execute(f"PRAGMA foreign_key_list({table_name})")
            fk_list = cursor.fetchall()
            
            if fk_list:
                print("   🔗 Внешние ключи:")
                for fk in fk_list:
                    if len(fk) >= 6:  # Проверяем количество элементов
                        table_name_fk, id, seq, fk_table, from_col, to_col = fk[:6]
                        print(f"     • {from_col} → {fk_table}.{to_col}")
                        foreign_keys.append((from_col, fk_table, to_col))
            
            print()
        
        # Анализ связей между таблицами
        print("🔗 АНАЛИЗ СВЯЗЕЙ МЕЖДУ ТАБЛИЦАМИ:")
        print()
        
        connections = {
            'object_groups': [],
            'detected_groups': [],
            'objects': [],
            'reviews': [],
            'processing_methods': [],
            'analysis_results': []
        }
        
        # Определяем связи на основе внешних ключей
        cursor.execute("PRAGMA foreign_key_list")
        all_foreign_keys = cursor.fetchall()
        
        for fk in all_foreign_keys:
            if len(fk) >= 6:  # Проверяем количество элементов
                table_name, id, seq, fk_table, from_col, to_col = fk[:6]
                if table_name in connections:
                    connections[table_name].append((from_col, fk_table, to_col))
        
        # Выводим связи
        for table, fks in connections.items():
            if fks:
                print(f"📄 {table}:")
                for from_col, fk_table, to_col in fks:
                    print(f"   • {from_col} → {fk_table}.{to_col}")
                print()
        
        # Схема связей
        print("📊 СХЕМА СВЯЗЕЙ:")
        print("   object_groups (1) ←→ (many) objects")
        print("   detected_groups (1) ←→ (many) objects")
        print("   objects (1) ←→ (many) reviews")
        print("   reviews (1) ←→ (many) analysis_results")
        print("   processing_methods (1) ←→ (many) analysis_results")
        print()
        
        # Статистика по таблицам
        print("📈 СТАТИСТИКА ПО ТАБЛИЦАМ:")
        total_records = 0
        for table in tables:
            table_name = table[0]
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            total_records += count
            print(f"   • {table_name}: {count} записей")
        
        print(f"\n📊 Общее количество записей: {total_records}")
        
        # Проверка индексов
        print("\n📈 ИНДЕКСЫ:")
        cursor.execute("SELECT name, tbl_name, sql FROM sqlite_master WHERE type='index'")
        indexes = cursor.fetchall()
        
        for idx in indexes:
            idx_name, tbl_name, sql = idx
            print(f"   • {idx_name} (таблица: {tbl_name})")
        
        conn.close()
        
        print("\n✅ Анализ завершен успешно!")
        
    except Exception as e:
        print(f"❌ Ошибка при анализе БД: {e}")

if __name__ == "__main__":
    analyze_database_structure() 