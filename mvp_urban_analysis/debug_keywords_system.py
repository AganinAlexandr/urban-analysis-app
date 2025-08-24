#!/usr/bin/env python3
"""
Диагностика системы ключевых слов
"""
import sqlite3
from initial_keywords_system import detect_group_by_initial_keywords, create_initial_keywords_table

def debug_keywords_system():
    """Диагностирует систему ключевых слов"""
    print("=== ДИАГНОСТИКА СИСТЕМЫ КЛЮЧЕВЫХ СЛОВ ===")
    
    try:
        # 1. Проверяем существование таблицы
        print("1. Проверка таблицы initial_keywords:")
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='initial_keywords'")
        table_exists = cursor.fetchone()
        
        if table_exists:
            print("   ✅ Таблица initial_keywords существует")
            
            # Проверяем количество записей
            cursor.execute("SELECT COUNT(*) FROM initial_keywords")
            count = cursor.fetchone()[0]
            print(f"   📊 Записей в таблице: {count}")
            
            if count == 0:
                print("   ⚠️  Таблица пустая - создаем начальные ключевые слова")
                if create_initial_keywords_table():
                    print("   ✅ Начальные ключевые слова созданы")
                else:
                    print("   ❌ Ошибка создания ключевых слов")
            else:
                print("   ✅ Ключевые слова загружены")
                
        else:
            print("   ❌ Таблица initial_keywords не существует")
            print("   🔧 Создаем таблицу...")
            if create_initial_keywords_table():
                print("   ✅ Таблица и ключевые слова созданы")
            else:
                print("   ❌ Ошибка создания таблицы")
        
        # 2. Проверяем ключевые слова по группам
        print("\n2. Проверка ключевых слов по группам:")
        cursor.execute("""
            SELECT group_type, keyword_type, keyword, is_initial
            FROM initial_keywords
            ORDER BY group_type, keyword_type, keyword
        """)
        keywords = cursor.fetchall()
        
        if keywords:
            current_group = None
            for group_type, keyword_type, keyword, is_initial in keywords:
                if current_group != group_type:
                    print(f"\n   📄 {group_type}:")
                    current_group = group_type
                
                source = "Начальные" if is_initial else "Извлеченные"
                print(f"     • {keyword_type}: '{keyword}' ({source})")
        else:
            print("   ❌ Ключевые слова не найдены")
        
        # 3. Тестируем определение групп
        print("\n3. Тест определения групп:")
        test_cases = [
            ("Московская школа №1", ""),
            ("Центральная больница", ""),
            ("Аптека на углу", ""),
            ("Детский сад Солнышко", ""),
            ("Поликлиника №5", ""),
            ("МГУ имени Ломоносова", ""),
            ("Торговый центр Мега", ""),
            ("Жилой комплекс Парк", ""),
        ]
        
        for object_name, review_text in test_cases:
            try:
                detected_group, confidence = detect_group_by_initial_keywords(object_name, review_text)
                status = "✅" if detected_group != 'undetected' else "❌"
                print(f"   {status} '{object_name}' -> {detected_group} (уверенность: {confidence})")
            except Exception as e:
                print(f"   ❌ Ошибка для '{object_name}': {e}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка диагностики: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_keywords_system() 