#!/usr/bin/env python3
"""
Исправление таблицы initial_keywords - удаление множественных форм
"""
import sqlite3
from initial_keywords_system import create_initial_keywords_table, INITIAL_KEYWORDS

def fix_keywords_table():
    """Пересоздает таблицу с правильными группами"""
    print("🔧 ИСПРАВЛЕНИЕ ТАБЛИЦЫ КЛЮЧЕВЫХ СЛОВ")
    print("=" * 50)
    
    db_path = 'urban_analysis_fixed.db'
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 1. Проверяем текущее состояние
        print("1. Текущее состояние таблицы:")
        cursor.execute("SELECT DISTINCT group_type FROM initial_keywords ORDER BY group_type")
        current_groups = cursor.fetchall()
        print(f"   Найдено {len(current_groups)} групп:")
        for group, in current_groups:
            print(f"   - {group}")
        
        # 2. Удаляем старую таблицу
        print("\n2. Удаляем старую таблицу...")
        cursor.execute("DROP TABLE IF EXISTS initial_keywords")
        print("   ✅ Таблица удалена")
        
        # 3. Создаем новую таблицу
        print("\n3. Создаем новую таблицу...")
        cursor.execute("""
            CREATE TABLE initial_keywords (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_type TEXT NOT NULL,
                keyword_type TEXT NOT NULL,
                keyword TEXT NOT NULL,
                weight REAL DEFAULT 1.0,
                is_initial BOOLEAN DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(group_type, keyword_type, keyword)
            )
        """)
        print("   ✅ Таблица создана")
        
        # 4. Загружаем новые ключевые слова
        print("\n4. Загружаем исправленные ключевые слова...")
        total_keywords = 0
        
        for group_type, keywords_data in INITIAL_KEYWORDS.items():
            print(f"   Группа: {group_type}")
            
            for keyword_type, keywords_list in keywords_data.items():
                for keyword in keywords_list:
                    cursor.execute("""
                        INSERT INTO initial_keywords (group_type, keyword_type, keyword, weight, is_initial)
                        VALUES (?, ?, ?, 1.0, 1)
                    """, (group_type, keyword_type, keyword))
                    total_keywords += 1
            
            print(f"     ✅ Загружено {len(keywords_data['name_keywords']) + len(keywords_data['text_keywords']) + len(keywords_data['negative_keywords'])} ключевых слов")
        
        conn.commit()
        
        # 5. Проверяем результат
        print(f"\n5. Проверка результата:")
        cursor.execute("SELECT DISTINCT group_type FROM initial_keywords ORDER BY group_type")
        new_groups = cursor.fetchall()
        print(f"   Создано {len(new_groups)} групп:")
        for group, in new_groups:
            cursor.execute("SELECT COUNT(*) FROM initial_keywords WHERE group_type = ?", (group,))
            count = cursor.fetchone()[0]
            print(f"   ✅ {group}: {count} ключевых слов")
        
        conn.close()
        
        print(f"\n🎉 УСПЕШНО ИСПРАВЛЕНО!")
        print(f"   Всего загружено: {total_keywords} ключевых слов")
        print(f"   Групп: {len(new_groups)}")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return False

def test_detection_after_fix():
    """Тестируем определение после исправления"""
    print("\n" + "=" * 50)
    print("🧪 ТЕСТ ОПРЕДЕЛЕНИЯ ПОСЛЕ ИСПРАВЛЕНИЯ")
    
    from initial_keywords_system import detect_group_by_initial_keywords
    
    test_cases = [
        {
            'name': 'Национальный исследовательский университет ВШЭ',
            'text': 'студенты преподаватели лекции',
            'expected': 'university'
        },
        {
            'name': 'Аптека 36,6',
            'text': 'лекарства фармацевт рецепт',
            'expected': 'pharmacy'
        }
    ]
    
    for i, case in enumerate(test_cases, 1):
        print(f"\n{i}. Тест: {case['name']}")
        result, confidence = detect_group_by_initial_keywords(case['name'], case['text'])
        
        success = "✅" if result == case['expected'] else "❌"
        print(f"   Ожидалось: {case['expected']}")
        print(f"   Получено:  {result} (уверенность: {confidence}) {success}")

if __name__ == "__main__":
    if fix_keywords_table():
        test_detection_after_fix()
    else:
        print("❌ Не удалось исправить таблицу")