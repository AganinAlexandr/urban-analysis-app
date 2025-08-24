#!/usr/bin/env python3
"""
Добавление дополнительных ключевых слов для детского сада
"""

import sqlite3
import os

def add_kindergarden_keywords():
    """Добавляет дополнительные ключевые слова для детского сада"""
    
    db_path = 'urban_analysis_fixed.db'
    
    if not os.path.exists(db_path):
        print(f"❌ База данных '{db_path}' не найдена")
        return False
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        print(f"=== ДОБАВЛЕНИЕ КЛЮЧЕВЫХ СЛОВ ДЛЯ ДЕТСКОГО САДА ===")
        
        # Сначала проверим, какие ключевые слова уже есть
        cursor.execute("""
            SELECT keyword FROM initial_keywords 
            WHERE group_type = 'kindergarden'
        """)
        existing_keywords = [row[0] for row in cursor.fetchall()]
        
        print(f"📋 Существующие ключевые слова для детского сада:")
        for keyword in existing_keywords:
            print(f"  ✅ {keyword}")
        
        # Новые ключевые слова для добавления
        new_keywords = [
            ('kindergarden', 'name_keywords', 'дошкольный', 2.0),
            ('kindergarden', 'name_keywords', 'садик', 2.0),
            ('kindergarden', 'name_keywords', 'детсад', 2.0),
            ('kindergarden', 'text_keywords', 'няня', 1.5),
            ('kindergarden', 'text_keywords', 'нянечка', 1.5),
            ('kindergarden', 'text_keywords', 'воспитатель', 1.5),
        ]
        
        # Фильтруем только те, которых еще нет
        keywords_to_add = []
        for group_type, keyword_type, keyword, weight in new_keywords:
            if keyword not in existing_keywords:
                keywords_to_add.append((group_type, keyword_type, keyword, weight))
                print(f"  ➕ Добавим: {keyword}")
            else:
                print(f"  ⚠️ Уже есть: {keyword}")
        
        if not keywords_to_add:
            print(f"\n✅ Все ключевые слова уже существуют!")
            conn.close()
            return True
        
        # Добавляем новые ключевые слова
        cursor.executemany("""
            INSERT INTO initial_keywords (group_type, keyword_type, keyword, weight)
            VALUES (?, ?, ?, ?)
        """, keywords_to_add)
        
        # Подтверждаем изменения
        conn.commit()
        
        # Проверяем результат
        cursor.execute("""
            SELECT COUNT(*) FROM initial_keywords 
            WHERE group_type = 'kindergarden'
        """)
        total_count = cursor.fetchone()[0]
        
        print(f"\n✅ Добавлено новых ключевых слов: {len(keywords_to_add)}")
        print(f"📊 Всего ключевых слов для детского сада: {total_count}")
        
        # Показываем обновленный список
        cursor.execute("""
            SELECT keyword_type, keyword, weight
            FROM initial_keywords 
            WHERE group_type = 'kindergarden'
            ORDER BY keyword_type, keyword
        """)
        
        print(f"\n📋 Обновленный список ключевых слов для детского сада:")
        for keyword_type, keyword, weight in cursor.fetchall():
            print(f"  {keyword_type}: '{keyword}' (вес: {weight})")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = add_kindergarden_keywords()
    if success:
        print(f"\n🎉 Ключевые слова для детского сада успешно обновлены!")
    else:
        print(f"\n❌ Не удалось обновить ключевые слова")



