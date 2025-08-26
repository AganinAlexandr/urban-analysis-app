#!/usr/bin/env python3
"""
Проверка ключевых слов детского сада в базе данных
"""

import sqlite3
import os

def check_kindergarden_keywords():
    """Проверяет ключевые слова для детского сада"""
    
    db_path = 'urban_analysis_fixed.db'
    
    if not os.path.exists(db_path):
        print(f"❌ База данных '{db_path}' не найдена")
        return
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        print(f"=== ПРОВЕРКА КЛЮЧЕВЫХ СЛОВ ДЕТСКОГО САДА ===")
        
        # Получаем все ключевые слова для детского сада
        cursor.execute("""
            SELECT keyword_type, keyword, weight
            FROM initial_keywords 
            WHERE group_type = 'kindergarden'
            ORDER BY keyword_type, keyword
        """)
        
        keywords = cursor.fetchall()
        
        if not keywords:
            print("❌ Ключевые слова для детского сада не найдены!")
            return
        
        print(f"📋 Найдено ключевых слов: {len(keywords)}")
        print()
        
        # Группируем по типам
        by_type = {}
        for keyword_type, keyword, weight in keywords:
            if keyword_type not in by_type:
                by_type[keyword_type] = []
            by_type[keyword_type].append((keyword, weight))
        
        # Показываем по типам
        for keyword_type, keywords_list in by_type.items():
            print(f"🔍 Тип: {keyword_type}")
            for keyword, weight in keywords_list:
                print(f"  ✅ '{keyword}' (вес: {weight})")
            print()
        
        # Проверяем конкретные ключевые слова
        test_keywords = ['воспитатель', 'дошкольный', 'детский сад', 'сад']
        print(f"🔍 Проверка конкретных ключевых слов:")
        
        for test_keyword in test_keywords:
            cursor.execute("""
                SELECT group_type, keyword_type, keyword, weight
                FROM initial_keywords 
                WHERE keyword = ?
            """, (test_keyword,))
            
            results = cursor.fetchall()
            if results:
                for group_type, keyword_type, keyword, weight in results:
                    print(f"  ✅ '{keyword}' найден в группе '{group_type}' как '{keyword_type}' (вес: {weight})")
            else:
                print(f"  ❌ '{test_keyword}' НЕ найден в базе")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_kindergarden_keywords()




