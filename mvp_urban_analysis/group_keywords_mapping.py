#!/usr/bin/env python3
"""
Система ключевых слов для определения групп объектов
"""
import sqlite3
import os
from datetime import datetime

# Словарь ключевых слов для каждой группы
GROUP_KEYWORDS = {
    'hospitals': {
        'name_keywords': ['больница', 'госпиталь', 'медицинский центр', 'клиника', 'медцентр', 'поликлиника'],
        'text_keywords': ['врач', 'лечение', 'пациент', 'медицина', 'здоровье', 'болезнь', 'симптом', 'диагноз', 'операция', 'терапия'],
        'negative_keywords': ['очередь', 'запись', 'прием', 'анализ', 'результат', 'направление']
    },
    'schools': {
        'name_keywords': ['школа', 'лицей', 'гимназия', 'образовательный центр', 'учебное заведение'],
        'text_keywords': ['учитель', 'ученик', 'урок', 'класс', 'образование', 'обучение', 'директор', 'завуч', 'предмет', 'экзамен'],
        'negative_keywords': ['домашка', 'контрольная', 'оценка', 'двойка', 'тройка', 'четверка', 'пятерка']
    },
    'kindergartens': {
        'name_keywords': ['детский сад', 'сад', 'дошкольное учреждение', 'ясли', 'детсад'],
        'text_keywords': ['воспитатель', 'ребенок', 'группа', 'игра', 'развитие', 'занятие', 'прогулка', 'сон', 'еда', 'адаптация'],
        'negative_keywords': ['плач', 'каприз', 'не хочу', 'не буду', 'страшно', 'боюсь']
    },
    'polyclinics': {
        'name_keywords': ['поликлиника', 'амбулатория', 'медицинская консультация', 'диспансер'],
        'text_keywords': ['терапевт', 'специалист', 'консультация', 'осмотр', 'направление', 'справка', 'больничный', 'рецепт'],
        'negative_keywords': ['очередь', 'запись', 'прием', 'анализ', 'результат', 'направление']
    },
    'pharmacies': {
        'name_keywords': ['аптека', 'фармация', 'лекарство', 'медикамент'],
        'text_keywords': ['лекарство', 'таблетка', 'сироп', 'мазь', 'рецепт', 'фармацевт', 'провизор', 'цена', 'стоимость', 'аналог'],
        'negative_keywords': ['дорого', 'нет в наличии', 'заменитель', 'побочный эффект']
    },
    'shopping_malls': {
        'name_keywords': ['торговый центр', 'молл', 'галерея', 'пассаж', 'торговый комплекс', 'шоппинг'],
        'text_keywords': ['магазин', 'покупка', 'товар', 'цена', 'скидка', 'акция', 'касса', 'продавец', 'консультант', 'размер'],
        'negative_keywords': ['дорого', 'очередь', 'нет размера', 'не подходит', 'не нравится']
    },
    'universities': {
        'name_keywords': ['университет', 'институт', 'академия', 'вуз', 'высшее образование'],
        'text_keywords': ['студент', 'преподаватель', 'лекция', 'семинар', 'экзамен', 'сессия', 'диплом', 'кафедра', 'факультет', 'ректор'],
        'negative_keywords': ['сложно', 'трудно', 'не понимаю', 'завалил', 'не сдал']
    }
}

def create_keywords_table():
    """Создает таблицу ключевых слов в базе данных"""
    db_path = 'urban_analysis_fixed.db'
    
    if not os.path.exists(db_path):
        print(f"❌ База данных '{db_path}' не найдена")
        return False
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Создаем таблицу ключевых слов
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS group_keywords (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_type TEXT NOT NULL,
                keyword_type TEXT NOT NULL, -- 'name', 'text', 'negative'
                keyword TEXT NOT NULL,
                weight REAL DEFAULT 1.0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(group_type, keyword_type, keyword)
            )
        """)
        
        # Очищаем существующие ключевые слова
        cursor.execute("DELETE FROM group_keywords")
        
        # Добавляем ключевые слова для каждой группы
        added_count = 0
        for group_type, keywords in GROUP_KEYWORDS.items():
            for keyword_type, words in keywords.items():
                for word in words:
                    cursor.execute("""
                        INSERT INTO group_keywords (group_type, keyword_type, keyword, weight)
                        VALUES (?, ?, ?, ?)
                    """, (group_type, keyword_type, word, 1.0))
                    added_count += 1
        
        conn.commit()
        conn.close()
        
        print(f"✅ Добавлено ключевых слов: {added_count}")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return False

def detect_group_by_keywords(object_name, review_text):
    """Определяет группу объекта на основе ключевых слов"""
    db_path = 'urban_analysis_fixed.db'
    
    if not os.path.exists(db_path):
        return 'undetected', 0.0
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Получаем все ключевые слова
        cursor.execute("""
            SELECT group_type, keyword_type, keyword, weight 
            FROM group_keywords 
            ORDER BY group_type, keyword_type
        """)
        keywords = cursor.fetchall()
        
        # Группируем ключевые слова по группам
        group_scores = {}
        
        for group_type, keyword_type, keyword, weight in keywords:
            if group_type not in group_scores:
                group_scores[group_type] = {'name_score': 0, 'text_score': 0, 'negative_score': 0}
            
            # Проверяем совпадения в названии объекта
            if keyword_type == 'name' and object_name:
                if keyword.lower() in object_name.lower():
                    group_scores[group_type]['name_score'] += weight
            
            # Проверяем совпадения в тексте отзыва
            if keyword_type == 'text' and review_text:
                if keyword.lower() in review_text.lower():
                    group_scores[group_type]['text_score'] += weight
            
            # Проверяем отрицательные ключевые слова
            if keyword_type == 'negative' and review_text:
                if keyword.lower() in review_text.lower():
                    group_scores[group_type]['negative_score'] += weight
        
        # Вычисляем итоговый балл для каждой группы
        best_group = 'undetected'
        best_score = 0.0
        
        for group_type, scores in group_scores.items():
            # Формула: (name_score * 2) + text_score - negative_score
            total_score = (scores['name_score'] * 2) + scores['text_score'] - scores['negative_score']
            
            if total_score > best_score:
                best_score = total_score
                best_group = group_type
        
        # Нормализуем уверенность (0.0 - 1.0)
        confidence = min(best_score / 10.0, 1.0) if best_score > 0 else 0.0
        
        conn.close()
        return best_group, confidence
        
    except Exception as e:
        print(f"❌ Ошибка определения группы: {e}")
        return 'undetected', 0.0

def update_detected_groups():
    """Обновляет detected_groups на основе ключевых слов"""
    db_path = 'urban_analysis_fixed.db'
    
    if not os.path.exists(db_path):
        print(f"❌ База данных '{db_path}' не найдена")
        return False
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Получаем все объекты с их отзывами
        cursor.execute("""
            SELECT o.id, o.name, o.group_id, 
                   GROUP_CONCAT(r.review_text, ' | ') as all_reviews
            FROM objects o
            LEFT JOIN reviews r ON o.id = r.object_id
            GROUP BY o.id, o.name, o.group_id
        """)
        objects = cursor.fetchall()
        
        updated_count = 0
        
        for obj_id, obj_name, current_group_id, reviews_text in objects:
            # Определяем группу на основе ключевых слов
            detected_group, confidence = detect_group_by_keywords(obj_name, reviews_text or "")
            
            # Получаем ID определяемой группы
            cursor.execute("SELECT id FROM detected_groups WHERE group_type = ?", (detected_group,))
            detected_group_result = cursor.fetchone()
            
            if detected_group_result:
                detected_group_id = detected_group_result[0]
                
                # Обновляем объект
                cursor.execute("""
                    UPDATE objects 
                    SET detected_group_id = ? 
                    WHERE id = ?
                """, (detected_group_id, obj_id))
                
                updated_count += 1
                print(f"   ✅ Объект '{obj_name}': {detected_group} (уверенность: {confidence:.2f})")
        
        conn.commit()
        conn.close()
        
        print(f"\n✅ Обновлено объектов: {updated_count}")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка обновления групп: {e}")
        return False

def show_keywords_statistics():
    """Показывает статистику по ключевым словам"""
    db_path = 'urban_analysis_fixed.db'
    
    if not os.path.exists(db_path):
        print(f"❌ База данных '{db_path}' не найдена")
        return
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        print("📊 СТАТИСТИКА КЛЮЧЕВЫХ СЛОВ:")
        
        # Статистика по группам
        cursor.execute("""
            SELECT group_type, keyword_type, COUNT(*) as count
            FROM group_keywords
            GROUP BY group_type, keyword_type
            ORDER BY group_type, keyword_type
        """)
        stats = cursor.fetchall()
        
        current_group = None
        for group_type, keyword_type, count in stats:
            if current_group != group_type:
                print(f"\n📄 {group_type.upper()}:")
                current_group = group_type
            
            type_name = {
                'name': 'Названия',
                'text': 'Текст отзывов', 
                'negative': 'Отрицательные'
            }.get(keyword_type, keyword_type)
            
            print(f"   • {type_name}: {count} слов")
        
        # Показываем примеры ключевых слов
        print("\n📝 ПРИМЕРЫ КЛЮЧЕВЫХ СЛОВ:")
        for group_type in GROUP_KEYWORDS:
            print(f"\n🏥 {group_type.upper()}:")
            keywords = GROUP_KEYWORDS[group_type]
            
            for keyword_type, words in keywords.items():
                type_name = {
                    'name': 'Названия',
                    'text': 'Текст отзывов',
                    'negative': 'Отрицательные'
                }.get(keyword_type, keyword_type)
                
                print(f"   • {type_name}: {', '.join(words[:5])}{'...' if len(words) > 5 else ''}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")

def main():
    """Основная функция"""
    print("=== СИСТЕМА КЛЮЧЕВЫХ СЛОВ ДЛЯ ОПРЕДЕЛЕНИЯ ГРУПП ===")
    print(f"⏰ Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Создаем таблицу ключевых слов
    print("🔧 Создание таблицы ключевых слов...")
    if create_keywords_table():
        print("✅ Таблица ключевых слов создана")
    else:
        print("❌ Ошибка создания таблицы")
        return
    
    # Показываем статистику
    print("\n📊 Статистика ключевых слов:")
    show_keywords_statistics()
    
    # Обновляем detected_groups
    print("\n🔧 Обновление detected_groups...")
    if update_detected_groups():
        print("✅ Группы обновлены")
    else:
        print("❌ Ошибка обновления групп")
    
    print("\n🎉 Система ключевых слов настроена!")

if __name__ == "__main__":
    main() 