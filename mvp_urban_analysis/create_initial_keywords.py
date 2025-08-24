#!/usr/bin/env python3
"""
Создание таблицы initial_keywords с базовыми ключевыми словами
"""

import sqlite3
import os

def create_initial_keywords_table():
    """Создает таблицу initial_keywords с базовыми ключевыми словами"""
    
    db_path = 'urban_analysis_fixed.db'
    
    if not os.path.exists(db_path):
        print(f"❌ База данных '{db_path}' не найдена")
        return False
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        print(f"=== СОЗДАНИЕ ТАБЛИЦЫ INITIAL_KEYWORDS В {db_path} ===")
        
        # Создаем таблицу
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS initial_keywords (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_type TEXT NOT NULL,
                keyword_type TEXT NOT NULL,
                keyword TEXT NOT NULL,
                weight REAL DEFAULT 1.0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Базовые ключевые слова для каждой группы
        keywords_data = [
            # Школы
            ('school', 'name_keywords', 'школа', 2.0),
            ('school', 'name_keywords', 'школы', 2.0),
            ('school', 'name_keywords', 'гимназия', 2.0),
            ('school', 'name_keywords', 'лицей', 2.0),
            ('school', 'text_keywords', 'учитель', 1.5),
            ('school', 'text_keywords', 'ученик', 1.5),
            ('school', 'text_keywords', 'урок', 1.0),
            ('school', 'text_keywords', 'класс', 1.0),
            ('school', 'text_keywords', 'директор', 1.0),
            
            # Больницы
            ('hospital', 'name_keywords', 'больница', 2.0),
            ('hospital', 'name_keywords', 'больницы', 2.0),
            ('hospital', 'name_keywords', 'госпиталь', 2.0),
            ('hospital', 'name_keywords', 'клиника', 2.0),
            ('hospital', 'text_keywords', 'врач', 1.5),
            ('hospital', 'text_keywords', 'медик', 1.5),
            ('hospital', 'text_keywords', 'пациент', 1.0),
            ('hospital', 'text_keywords', 'лечение', 1.0),
            ('hospital', 'text_keywords', 'операция', 1.0),
            
            # Университеты
            ('university', 'name_keywords', 'университет', 2.0),
            ('university', 'name_keywords', 'университеты', 2.0),
            ('university', 'name_keywords', 'институт', 2.0),
            ('university', 'name_keywords', 'академия', 2.0),
            ('university', 'text_keywords', 'студент', 1.5),
            ('university', 'text_keywords', 'преподаватель', 1.5),
            ('university', 'text_keywords', 'лекция', 1.0),
            ('university', 'text_keywords', 'сессия', 1.0),
            ('university', 'text_keywords', 'факультет', 1.0),
            
            # Аптеки
            ('pharmacy', 'name_keywords', 'аптека', 2.0),
            ('pharmacy', 'name_keywords', 'аптеки', 2.0),
            ('pharmacy', 'name_keywords', 'фармация', 1.5),
            ('pharmacy', 'text_keywords', 'лекарство', 1.5),
            ('pharmacy', 'text_keywords', 'медикамент', 1.5),
            ('pharmacy', 'text_keywords', 'рецепт', 1.0),
            ('pharmacy', 'text_keywords', 'фармацевт', 1.0),
            
            # Детские сады
            ('kindergarden', 'name_keywords', 'детский сад', 2.0),
            ('kindergarden', 'name_keywords', 'детские сады', 2.0),
            ('kindergarden', 'name_keywords', 'сад', 1.5),
            ('kindergarden', 'text_keywords', 'воспитатель', 1.5),
            ('kindergarden', 'text_keywords', 'ребенок', 1.0),
            ('kindergarden', 'text_keywords', 'игра', 1.0),
            ('kindergarden', 'text_keywords', 'группа', 1.0),
            
            # Поликлиники
            ('polyclinic', 'name_keywords', 'поликлиника', 2.0),
            ('polyclinic', 'name_keywords', 'поликлиники', 2.0),
            ('polyclinic', 'text_keywords', 'терапевт', 1.5),
            ('polyclinic', 'text_keywords', 'осмотр', 1.0),
            ('polyclinic', 'text_keywords', 'прием', 1.0),
            ('polyclinic', 'text_keywords', 'диагноз', 1.0),
            
            # Торговые центры
            ('shopmall', 'name_keywords', 'торговый центр', 2.0),
            ('shopmall', 'name_keywords', 'торговые центры', 2.0),
            ('shopmall', 'name_keywords', 'молл', 2.0),
            ('shopmall', 'name_keywords', 'галерея', 1.5),
            ('shopmall', 'text_keywords', 'магазин', 1.5),
            ('shopmall', 'text_keywords', 'покупка', 1.0),
            ('shopmall', 'text_keywords', 'товар', 1.0),
            ('shopmall', 'text_keywords', 'скидка', 1.0),
            
            # Жилые комплексы
            ('resident_complex', 'name_keywords', 'жилой комплекс', 2.0),
            ('resident_complex', 'name_keywords', 'жилые комплексы', 2.0),
            ('resident_complex', 'name_keywords', 'жк', 2.0),
            ('resident_complex', 'name_keywords', 'дом', 1.5),
            ('resident_complex', 'text_keywords', 'квартира', 1.5),
            ('resident_complex', 'text_keywords', 'жилье', 1.0),
            ('resident_complex', 'text_keywords', 'ремонт', 1.0),
            ('resident_complex', 'text_keywords', 'сосед', 1.0),
        ]
        
        # Очищаем таблицу если она уже существует
        cursor.execute("DELETE FROM initial_keywords")
        
        # Вставляем данные
        cursor.executemany("""
            INSERT INTO initial_keywords (group_type, keyword_type, keyword, weight)
            VALUES (?, ?, ?, ?)
        """, keywords_data)
        
        # Создаем индексы для оптимизации
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_group_type ON initial_keywords(group_type)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_keyword_type ON initial_keywords(keyword_type)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_keyword ON initial_keywords(keyword)")
        
        # Подтверждаем изменения
        conn.commit()
        
        # Проверяем результат
        cursor.execute("SELECT COUNT(*) FROM initial_keywords")
        count = cursor.fetchone()[0]
        
        print(f"✅ Таблица создана успешно!")
        print(f"📊 Добавлено ключевых слов: {count}")
        
        # Показываем статистику по группам
        cursor.execute("""
            SELECT group_type, COUNT(*) as count
            FROM initial_keywords
            GROUP BY group_type
            ORDER BY group_type
        """)
        
        print(f"\n📋 Статистика по группам:")
        for group_type, count in cursor.fetchall():
            print(f"  {group_type}: {count} ключевых слов")
        
        # Показываем несколько примеров
        cursor.execute("""
            SELECT group_type, keyword_type, keyword, weight
            FROM initial_keywords
            ORDER BY group_type, keyword_type
            LIMIT 10
        """)
        
        print(f"\n🔍 Примеры ключевых слов:")
        for i, (group_type, keyword_type, keyword, weight) in enumerate(cursor.fetchall()):
            print(f"  {i+1}. {group_type}.{keyword_type} = '{keyword}' (вес: {weight})")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = create_initial_keywords_table()
    if success:
        print(f"\n🎉 Таблица initial_keywords успешно создана!")
        print(f"Теперь система сможет определять группы объектов по ключевым словам.")
    else:
        print(f"\n❌ Не удалось создать таблицу initial_keywords")



