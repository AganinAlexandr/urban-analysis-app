#!/usr/bin/env python3
"""
Проверка содержимого таблицы ключевых слов
"""

import sqlite3
import os

def check_keywords_table():
    """Проверяет содержимое таблицы initial_keywords"""
    
    db_path = 'urban_analysis_fixed.db'
    
    if not os.path.exists(db_path):
        print(f"❌ База данных '{db_path}' не найдена")
        return
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Проверяем структуру таблицы
        cursor.execute("PRAGMA table_info(initial_keywords)")
        columns = cursor.fetchall()
        
        print("=== СТРУКТУРА ТАБЛИЦЫ INITIAL_KEYWORDS ===")
        for col in columns:
            print(f"  {col[1]} ({col[2]}) - {'NOT NULL' if col[3] else 'NULL'}")
        
        # Проверяем количество записей
        cursor.execute("SELECT COUNT(*) FROM initial_keywords")
        total_count = cursor.fetchone()[0]
        print(f"\nВсего записей в таблице: {total_count}")
        
        if total_count == 0:
            print("❌ Таблица пуста! Это объясняет, почему определение групп не работает.")
            return
        
        # Показываем примеры ключевых слов по группам
        cursor.execute("""
            SELECT group_type, keyword_type, keyword, weight
            FROM initial_keywords
            ORDER BY group_type, keyword_type, keyword
        """)
        
        keywords = cursor.fetchall()
        
        print(f"\n=== ПРИМЕРЫ КЛЮЧЕВЫХ СЛОВ ===")
        current_group = None
        current_type = None
        
        for group_type, keyword_type, keyword, weight in keywords:
            if group_type != current_group:
                current_group = group_type
                print(f"\n📁 Группа: {group_type}")
                current_type = None
            
            if keyword_type != current_type:
                current_type = keyword_type
                print(f"  📝 Тип: {keyword_type}")
            
            print(f"    • '{keyword}' (вес: {weight})")
        
        # Проверяем конкретные примеры для тестирования
        print(f"\n=== ТЕСТИРОВАНИЕ ОПРЕДЕЛЕНИЯ ГРУПП ===")
        
        test_cases = [
            ("Тестовая школа №1", "Отличная школа, хорошие учителя"),
            ("Больница им. Боткина", "Хорошая больница, квалифицированные врачи"),
            ("Университет Синергия", "Современный университет"),
            ("Аптека на углу", "Удобная аптека"),
            ("Детский сад Солнышко", "Замечательный детский сад")
        ]
        
        from initial_keywords_system import detect_group_by_initial_keywords
        
        for name, review in test_cases:
            detected_group, confidence = detect_group_by_initial_keywords(name, review)
            print(f"  '{name}' + '{review[:30]}...' -> {detected_group} (уверенность: {confidence:.2f})")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_keywords_table()



