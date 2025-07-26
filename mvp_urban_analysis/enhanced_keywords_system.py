#!/usr/bin/env python3
"""
Улучшенная система ключевых слов с нормализацией и лемматизацией
"""
import sqlite3
import os
import re
import pymorphy2
from collections import Counter, defaultdict
from datetime import datetime

class EnhancedKeywordProcessor:
    """Улучшенный процессор ключевых слов с нормализацией и лемматизацией"""
    
    def __init__(self):
        self.morph = pymorphy2.MorphAnalyzer()
        
        # Словарь для нормализации
        self.normalization_dict = {
            'больница': 'больница',
            'госпиталь': 'больница',
            'медцентр': 'больница',
            'клиника': 'больница',
            'поликлиника': 'поликлиника',
            'амбулатория': 'поликлиника',
            'школа': 'школа',
            'лицей': 'школа',
            'гимназия': 'школа',
            'университет': 'университет',
            'институт': 'университет',
            'академия': 'университет',
            'вуз': 'университет',
            'аптека': 'аптека',
            'фармация': 'аптека',
            'детский сад': 'детский сад',
            'сад': 'детский сад',
            'детсад': 'детский сад',
            'торговый центр': 'торговый центр',
            'молл': 'торговый центр',
            'галерея': 'торговый центр'
        }
        
        # Стоп-слова для исключения
        self.stop_words = {
            'это', 'тот', 'такой', 'какой', 'где', 'когда', 'как', 'что', 'кто',
            'очень', 'много', 'мало', 'больше', 'меньше', 'все', 'всегда', 'никогда',
            'хорошо', 'плохо', 'лучше', 'хуже', 'новый', 'старый', 'большой', 'маленький'
        }
    
    def normalize_word(self, word):
        """Нормализует слово, приводя к базовой форме"""
        if not word:
            return ""
        
        # Приводим к нижнему регистру
        word = word.lower().strip()
        
        # Проверяем словарь нормализации
        if word in self.normalization_dict:
            return self.normalization_dict[word]
        
        # Используем pymorphy2 для лемматизации
        try:
            parsed = self.morph.parse(word)
            if parsed:
                # Берем нормальную форму (инфинитив)
                return parsed[0].normal_form
        except:
            pass
        
        return word
    
    def clean_text(self, text):
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
    
    def extract_normalized_words(self, text):
        """Извлекает и нормализует слова из текста"""
        if not text:
            return []
        
        cleaned_text = self.clean_text(text)
        words = cleaned_text.split()
        
        # Фильтруем короткие слова и стоп-слова
        normalized_words = []
        for word in words:
            if len(word) >= 3 and word not in self.stop_words:
                normalized_word = self.normalize_word(word)
                if normalized_word and len(normalized_word) >= 3:
                    normalized_words.append(normalized_word)
        
        return normalized_words

def create_enhanced_keywords_table():
    """Создает улучшенную таблицу ключевых слов"""
    db_path = 'urban_analysis_fixed.db'
    
    if not os.path.exists(db_path):
        print(f"❌ База данных '{db_path}' не найдена")
        return False
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Создаем улучшенную таблицу ключевых слов
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS enhanced_group_keywords (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_type TEXT NOT NULL,
                keyword_type TEXT NOT NULL,
                original_keyword TEXT NOT NULL,
                normalized_keyword TEXT NOT NULL,
                frequency INTEGER DEFAULT 1,
                weight REAL DEFAULT 1.0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(group_type, keyword_type, normalized_keyword)
            )
        """)
        
        # Очищаем существующие ключевые слова
        cursor.execute("DELETE FROM enhanced_group_keywords")
        
        conn.commit()
        conn.close()
        
        print("✅ Улучшенная таблица ключевых слов создана")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка создания таблицы: {e}")
        return False

def analyze_enhanced_keywords():
    """Анализирует ключевые слова с нормализацией"""
    print("🔍 АНАЛИЗ УЛУЧШЕННЫХ КЛЮЧЕВЫХ СЛОВ")
    print("=" * 50)
    
    processor = EnhancedKeywordProcessor()
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
                name_words.extend(processor.extract_normalized_words(name))
            
            # Собираем слова из отзывов
            review_words = []
            for review in data['reviews']:
                review_words.extend(processor.extract_normalized_words(review))
            
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
            
            # Показываем топ-5 слов
            top_words = group_counter.most_common(5)
            print(f"   Топ-5 слов: {[word for word, count in top_words]}")
        
        conn.close()
        return group_words, all_words
        
    except Exception as e:
        print(f"❌ Ошибка получения данных: {e}")
        return {}

def find_enhanced_unique_keywords(group_words, all_words, min_frequency=2):
    """Находит уникальные ключевые слова с нормализацией"""
    print("\n🎯 ПОИСК УНИКАЛЬНЫХ КЛЮЧЕВЫХ СЛОВ (С НОРМАЛИЗАЦИЕЙ)")
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

def save_enhanced_keywords_to_db(unique_keywords):
    """Сохраняет улучшенные ключевые слова в БД"""
    db_path = 'urban_analysis_fixed.db'
    
    if not os.path.exists(db_path):
        print(f"❌ База данных '{db_path}' не найдена")
        return False
    
    processor = EnhancedKeywordProcessor()
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Очищаем существующие ключевые слова
        cursor.execute("DELETE FROM enhanced_group_keywords")
        
        # Добавляем новые ключевые слова
        added_count = 0
        for group_type, words in unique_keywords.items():
            for word, count in words:
                # Нормализуем слово
                normalized_word = processor.normalize_word(word)
                
                # Определяем тип ключевого слова
                keyword_type = 'name' if count >= 3 else 'text'
                
                cursor.execute("""
                    INSERT INTO enhanced_group_keywords 
                    (group_type, keyword_type, original_keyword, normalized_keyword, frequency, weight)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (group_type, keyword_type, word, normalized_word, count, 1.0))
                added_count += 1
        
        conn.commit()
        conn.close()
        
        print(f"\n✅ Сохранено улучшенных ключевых слов: {added_count}")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка сохранения: {e}")
        return False

def detect_group_by_enhanced_keywords(object_name, review_text):
    """Определяет группу объекта с улучшенным алгоритмом"""
    db_path = 'urban_analysis_fixed.db'
    
    if not os.path.exists(db_path):
        return 'undetected', 0.0
    
    processor = EnhancedKeywordProcessor()
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Получаем все ключевые слова
        cursor.execute("""
            SELECT group_type, keyword_type, normalized_keyword, weight 
            FROM enhanced_group_keywords 
            ORDER BY group_type, keyword_type
        """)
        keywords = cursor.fetchall()
        
        # Нормализуем входные тексты
        normalized_object_name = ' '.join(processor.extract_normalized_words(object_name or ""))
        normalized_review_text = ' '.join(processor.extract_normalized_words(review_text or ""))
        
        # Группируем ключевые слова по группам
        group_scores = {}
        
        for group_type, keyword_type, normalized_keyword, weight in keywords:
            if group_type not in group_scores:
                group_scores[group_type] = {'name_score': 0, 'text_score': 0, 'negative_score': 0}
            
            # Проверяем совпадения в названии объекта
            if keyword_type == 'name' and normalized_object_name:
                if normalized_keyword in normalized_object_name:
                    group_scores[group_type]['name_score'] += weight
                    print(f"   ✅ Найдено в названии: '{normalized_keyword}' в '{normalized_object_name}'")
            
            # Проверяем совпадения в тексте отзыва
            if keyword_type == 'text' and normalized_review_text:
                if normalized_keyword in normalized_review_text:
                    group_scores[group_type]['text_score'] += weight
                    print(f"   ✅ Найдено в отзыве: '{normalized_keyword}' в '{normalized_review_text}'")
        
        # Вычисляем итоговый балл для каждой группы
        best_group = 'undetected'
        best_score = 0.0
        
        print(f"\n📊 Результаты подсчета баллов:")
        for group_type, scores in group_scores.items():
            # Формула: (name_score * 2) + text_score - negative_score
            total_score = (scores['name_score'] * 2) + scores['text_score'] - scores['negative_score']
            
            print(f"   {group_type}: name={scores['name_score']}, text={scores['text_score']}, negative={scores['negative_score']}, total={total_score}")
            
            if total_score > best_score:
                best_score = total_score
                best_group = group_type
        
        # Нормализуем уверенность (0.0 - 1.0)
        confidence = min(best_score / 5.0, 1.0) if best_score > 0 else 0.0
        
        conn.close()
        return best_group, confidence
        
    except Exception as e:
        print(f"❌ Ошибка определения группы: {e}")
        return 'undetected', 0.0

def test_enhanced_system():
    """Тестирует улучшенную систему"""
    print("=== ТЕСТИРОВАНИЕ УЛУЧШЕННОЙ СИСТЕМЫ ===")
    print(f"⏰ Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    test_cases = [
        ("Городская больница №1", "Отличная больница с профессиональными врачами"),
        ("Аптека на углу", "Удобная аптека, всегда есть нужные лекарства"),
        ("Школа №15", "Хорошая школа, но есть проблемы с питанием"),
        ("Тестовая больница", "Отличная больница"),
        ("Тестовая школа", "Хорошая школа"),
        ("Тестовая аптека", "Удобная аптека")
    ]
    
    for i, (object_name, review_text) in enumerate(test_cases, 1):
        print(f"\n🧪 Тест {i}:")
        print(f"📄 Объект: '{object_name}'")
        print(f"📝 Отзыв: '{review_text}'")
        
        detected_group, confidence = detect_group_by_enhanced_keywords(object_name, review_text)
        
        print(f"🎯 Результат: {detected_group} (уверенность: {confidence:.2f})")
        print("-" * 50)

def main():
    """Основная функция"""
    print("=== УЛУЧШЕННАЯ СИСТЕМА КЛЮЧЕВЫХ СЛОВ ===")
    print(f"⏰ Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Создаем улучшенную таблицу
    print("🔧 Создание улучшенной таблицы ключевых слов...")
    if not create_enhanced_keywords_table():
        print("❌ Ошибка создания таблицы")
        return
    
    # Анализируем данные
    group_words, all_words = analyze_enhanced_keywords()
    
    if not group_words:
        print("❌ Не удалось получить данные для анализа")
        return
    
    # Находим уникальные ключевые слова
    unique_keywords = find_enhanced_unique_keywords(group_words, all_words, min_frequency=2)
    
    if not unique_keywords:
        print("❌ Не найдено уникальных ключевых слов")
        return
    
    # Сохраняем в БД
    print("\n💾 Сохранение в базу данных...")
    if save_enhanced_keywords_to_db(unique_keywords):
        print("✅ Улучшенные ключевые слова сохранены в БД")
    else:
        print("❌ Ошибка сохранения")
    
    # Тестируем систему
    print("\n🧪 Тестирование улучшенной системы...")
    test_enhanced_system()
    
    print("\n🎉 Улучшенная система ключевых слов настроена!")

if __name__ == "__main__":
    main() 