#!/usr/bin/env python3
"""
Автоматическое извлечение ключевых слов на основе анализа данных
"""
import sqlite3
import os
import re
from collections import Counter, defaultdict
from datetime import datetime

def clean_text(text):
    """Очищает текст от лишних символов"""
    if not text:
        return ""
    
    # Приводим к нижнему регистру
    text = text.lower()
    
    # Удаляем пунктуацию и цифры
    text = re.sub(r'[^\w\s]', ' ', text)
    text = re.sub(r'\d+', ' ', text)
    
    # Удаляем лишние пробелы
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text

def extract_words(text):
    """Извлекает слова из текста"""
    if not text:
        return []
    
    cleaned_text = clean_text(text)
    words = cleaned_text.split()
    
    # Фильтруем короткие слова (менее 3 символов)
    words = [word for word in words if len(word) >= 3]
    
    return words

def get_group_data():
    """Получает данные для каждой группы из БД"""
    db_path = 'urban_analysis_fixed.db'
    
    if not os.path.exists(db_path):
        print(f"❌ База данных '{db_path}' не найдена")
        return {}
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Получаем все объекты с их группами и отзывами
        cursor.execute("""
            SELECT 
                og.group_type,
                o.name,
                GROUP_CONCAT(r.review_text, ' | ') as all_reviews
            FROM objects o
            LEFT JOIN object_groups og ON o.group_id = og.id
            LEFT JOIN reviews r ON o.id = r.object_id
            WHERE og.group_type IS NOT NULL
            GROUP BY og.group_type, o.name
        """)
        
        group_data = defaultdict(lambda: {'names': [], 'reviews': []})
        
        for group_type, name, reviews_text in cursor.fetchall():
            if group_type:
                group_data[group_type]['names'].append(name)
                if reviews_text:
                    group_data[group_type]['reviews'].append(reviews_text)
        
        conn.close()
        return group_data
        
    except Exception as e:
        print(f"❌ Ошибка получения данных: {e}")
        return {}

def analyze_keywords():
    """Анализирует ключевые слова для каждой группы"""
    print("🔍 АНАЛИЗ КЛЮЧЕВЫХ СЛОВ")
    print("=" * 50)
    
    group_data = get_group_data()
    
    if not group_data:
        print("❌ Нет данных для анализа")
        return {}
    
    # Собираем все слова для каждой группы
    group_words = {}
    all_words = Counter()
    
    for group_type, data in group_data.items():
        print(f"\n📊 Анализ группы: {group_type}")
        print(f"   Объектов: {len(data['names'])}")
        print(f"   Отзывов: {len(data['reviews'])}")
        
        # Собираем слова из названий объектов
        name_words = []
        for name in data['names']:
            name_words.extend(extract_words(name))
        
        # Собираем слова из отзывов
        review_words = []
        for review in data['reviews']:
            review_words.extend(extract_words(review))
        
        # Подсчитываем частоту слов
        name_counter = Counter(name_words)
        review_counter = Counter(review_words)
        
        # Объединяем все слова группы
        group_counter = name_counter + review_counter
        group_words[group_type] = group_counter
        
        # Добавляем в общий счетчик
        all_words.update(group_counter)
        
        print(f"   Уникальных слов: {len(group_counter)}")
        print(f"   Всего слов: {sum(group_counter.values())}")
    
    return group_words, all_words

def find_unique_keywords(group_words, all_words, min_frequency=2):
    """Находит уникальные ключевые слова для каждой группы"""
    print("\n🎯 ПОИСК УНИКАЛЬНЫХ КЛЮЧЕВЫХ СЛОВ")
    print("=" * 50)
    
    unique_keywords = {}
    
    for group_type, group_counter in group_words.items():
        print(f"\n📄 Группа: {group_type}")
        
        # Находим слова, которые встречаются только в этой группе
        unique_words = []
        
        for word, count in group_counter.most_common():
            # Проверяем минимальную частоту
            if count < min_frequency:
                continue
            
            # Проверяем, что слово не встречается в других группах
            is_unique = True
            for other_group, other_counter in group_words.items():
                if other_group != group_type and word in other_counter:
                    is_unique = False
                    break
            
            if is_unique:
                unique_words.append((word, count))
        
        # Сортируем по частоте
        unique_words.sort(key=lambda x: x[1], reverse=True)
        
        # Берем топ-10 слов
        top_words = unique_words[:10]
        
        unique_keywords[group_type] = top_words
        
        print(f"   Найдено уникальных слов: {len(unique_words)}")
        print(f"   Топ-10: {[word for word, count in top_words]}")
    
    return unique_keywords

def create_keywords_mapping(unique_keywords):
    """Создает маппинг ключевых слов для БД"""
    keywords_mapping = {}
    
    for group_type, words in unique_keywords.items():
        keywords_mapping[group_type] = {
            'name_keywords': [word for word, count in words if count >= 3],  # Более частые слова для названий
            'text_keywords': [word for word, count in words],  # Все слова для текста
            'negative_keywords': []  # Пока пустой, можно добавить позже
        }
    
    return keywords_mapping

def save_keywords_to_db(keywords_mapping):
    """Сохраняет ключевые слова в БД"""
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
                keyword_type TEXT NOT NULL,
                keyword TEXT NOT NULL,
                frequency INTEGER DEFAULT 1,
                weight REAL DEFAULT 1.0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(group_type, keyword_type, keyword)
            )
        """)
        
        # Очищаем существующие ключевые слова
        cursor.execute("DELETE FROM group_keywords")
        
        # Добавляем новые ключевые слова
        added_count = 0
        for group_type, keywords in keywords_mapping.items():
            for keyword_type, words in keywords.items():
                for word in words:
                    cursor.execute("""
                        INSERT INTO group_keywords (group_type, keyword_type, keyword, weight)
                        VALUES (?, ?, ?, ?)
                    """, (group_type, keyword_type, word, 1.0))
                    added_count += 1
        
        conn.commit()
        conn.close()
        
        print(f"\n✅ Сохранено ключевых слов: {added_count}")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка сохранения: {e}")
        return False

def show_keywords_summary(keywords_mapping):
    """Показывает сводку по ключевым словам"""
    print("\n📋 СВОДКА КЛЮЧЕВЫХ СЛОВ")
    print("=" * 50)
    
    for group_type, keywords in keywords_mapping.items():
        print(f"\n🏥 {group_type.upper()}:")
        
        name_count = len(keywords['name_keywords'])
        text_count = len(keywords['text_keywords'])
        
        print(f"   • Названия: {name_count} слов")
        if name_count > 0:
            print(f"     Примеры: {', '.join(keywords['name_keywords'][:5])}")
        
        print(f"   • Текст отзывов: {text_count} слов")
        if text_count > 0:
            print(f"     Примеры: {', '.join(keywords['text_keywords'][:5])}")

def main():
    """Основная функция"""
    print("=== АВТОМАТИЧЕСКОЕ ИЗВЛЕЧЕНИЕ КЛЮЧЕВЫХ СЛОВ ===")
    print(f"⏰ Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Анализируем данные
    group_words, all_words = analyze_keywords()
    
    if not group_words:
        print("❌ Не удалось получить данные для анализа")
        return
    
    # Находим уникальные ключевые слова
    unique_keywords = find_unique_keywords(group_words, all_words, min_frequency=2)
    
    if not unique_keywords:
        print("❌ Не найдено уникальных ключевых слов")
        return
    
    # Создаем маппинг
    keywords_mapping = create_keywords_mapping(unique_keywords)
    
    # Показываем сводку
    show_keywords_summary(keywords_mapping)
    
    # Сохраняем в БД
    print("\n💾 Сохранение в базу данных...")
    if save_keywords_to_db(keywords_mapping):
        print("✅ Ключевые слова сохранены в БД")
    else:
        print("❌ Ошибка сохранения")
    
    print("\n🎉 Автоматическое извлечение ключевых слов завершено!")

if __name__ == "__main__":
    main() 