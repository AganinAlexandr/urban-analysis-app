#!/usr/bin/env python3
"""
Система начальных ключевых слов с автоматическим улучшением
"""
import sqlite3
import os
import re
from collections import Counter, defaultdict
from datetime import datetime

# Начальные ключевые слова для каждой группы
INITIAL_KEYWORDS = {
    'hospital': {
        'name_keywords': ['больница', 'госпиталь', 'медицинский центр', 'клиника', 'медцентр', 'поликлиника'],
        'text_keywords': ['врач', 'лечение', 'пациент', 'медицина', 'здоровье', 'болезнь', 'симптом', 'диагноз', 'операция', 'терапия'],
        'negative_keywords': ['очередь', 'запись', 'прием', 'анализ', 'результат', 'направление']
    },
    'school': {
        'name_keywords': ['школа', 'лицей', 'гимназия', 'образовательный центр', 'учебное заведение'],
        'text_keywords': ['учитель', 'ученик', 'урок', 'класс', 'образование', 'обучение', 'директор', 'завуч', 'предмет', 'экзамен'],
        'negative_keywords': ['домашка', 'контрольная', 'оценка', 'двойка', 'тройка', 'четверка', 'пятерка']
    },
    'kindergarden': {
        'name_keywords': ['детский сад', 'сад', 'дошкольное учреждение', 'дошкольное образование', 'ясли', 'детсад'],
        'text_keywords': ['воспитатель', 'ребенок', 'группа', 'игра', 'развитие', 'занятие', 'прогулка', 'сон', 'еда', 'адаптация'],
        'negative_keywords': ['плач', 'каприз', 'не хочу', 'не буду', 'страшно', 'боюсь']
    },
    'polyclinic': {
        'name_keywords': ['поликлиника', 'амбулатория', 'медицинская консультация', 'диспансер'],
        'text_keywords': ['терапевт', 'специалист', 'консультация', 'осмотр', 'направление', 'справка', 'больничный', 'рецепт'],
        'negative_keywords': ['очередь', 'запись', 'прием', 'анализ', 'результат', 'направление']
    },
    'pharmacy': {
        'name_keywords': ['аптека', 'фармация', 'лекарство', 'медикамент'],
        'text_keywords': ['лекарство', 'таблетка', 'сироп', 'мазь', 'рецепт', 'фармацевт', 'провизор', 'цена', 'стоимость', 'аналог'],
        'negative_keywords': ['дорого', 'нет в наличии', 'заменитель', 'побочный эффект']
    },
    'shopmall': {
        'name_keywords': ['торговый центр', 'молл', 'галерея', 'пассаж', 'торговый комплекс', 'шоппинг', 'тц'],
        'text_keywords': ['магазин', 'покупка', 'товар', 'цена', 'скидка', 'акция', 'касса', 'продавец', 'консультант', 'размер'],
        'negative_keywords': ['дорого', 'очередь', 'нет размера', 'не подходит', 'не нравится']
    },
    'university': {
        'name_keywords': ['университет', 'институт', 'академия', 'вуз', 'высшее образование', 'высшая школа'],
        'text_keywords': ['студент', 'преподаватель', 'лекция', 'семинар', 'экзамен', 'сессия', 'диплом', 'кафедра', 'факультет', 'ректор', 'лектор'],
        'negative_keywords': ['сложно', 'трудно', 'не понимаю', 'завалил', 'не сдал']
    },
    'resident_complex': {
        'name_keywords': ['жилой комплекс', 'жилой дом', 'многоквартирный дом', 'жилье', 'квартира', 'дом'],
        'text_keywords': ['квартира', 'дом', 'жилье', 'ремонт', 'соседи', 'управляющая компания', 'жкх', 'коммунальные услуги', 'лифт', 'парковка'],
        'negative_keywords': ['шум', 'грязь', 'ремонт', 'соседи', 'дорого', 'плохо']
    }
}

class InitialKeywordProcessor:
    """Процессор начальных ключевых слов с возможностью улучшения"""
    
    def __init__(self):
        # Словарь для нормализации (только для синонимов, не для групп)
        self.normalization_dict = {
            'больница': 'больница',
            'госпиталь': 'госпиталь',      # Оставляем как есть
            'медцентр': 'медцентр',        # Оставляем как есть
            'клиника': 'клиника',          # Оставляем как есть
            'медицинский': 'медицинский',  # Оставляем как есть
            'поликлиника': 'поликлиника',
            'амбулатория': 'амбулатория',  # Оставляем как есть
            'школа': 'школа',
            'лицей': 'лицей',              # Оставляем как есть
            'гимназия': 'гимназия',        # Оставляем как есть
            'университет': 'университет',
            'институт': 'институт',        # Оставляем как есть
            'академия': 'академия',        # Оставляем как есть
            'вуз': 'вуз',                  # Оставляем как есть
            'высшая школа': 'высшая школа', # Оставляем как есть
            'аптека': 'аптека',
            'фармация': 'фармация',        # Оставляем как есть
            'детский сад': 'детский сад',
            'сад': 'сад',                  # Оставляем как есть
            'детсад': 'детсад',            # Оставляем как есть
            'дошкольное образование': 'дошкольное образование', # Оставляем как есть
            'торговый центр': 'торговый центр',
            'молл': 'молл',                # Оставляем как есть
            'галерея': 'галерея',          # Оставляем как есть
            'магазин': 'магазин',          # Оставляем как есть
            'тц': 'тц',                    # Оставляем как есть
            'жилой комплекс': 'жилой комплекс',
            'жилой дом': 'жилой дом',       # Оставляем как есть
            'многоквартирный дом': 'многоквартирный дом', # Оставляем как есть
            'жилье': 'жилье',              # Оставляем как есть
            'квартира': 'квартира',        # Оставляем как есть
            'дом': 'дом'                   # Оставляем как есть
        }
        
        # Стоп-слова для исключения
        self.stop_words = {
            'это', 'тот', 'такой', 'какой', 'где', 'когда', 'как', 'что', 'кто',
            'очень', 'много', 'мало', 'больше', 'меньше', 'все', 'всегда', 'никогда',
            'хорошо', 'плохо', 'лучше', 'хуже', 'новый', 'старый', 'большой', 'маленький',
            'есть', 'был', 'была', 'были', 'быть', 'стать', 'стал', 'стала'
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
        
        # Нормализация окончаний для приведения к базовой форме
        # Убираем окончания прилагательных и существительных
        
        # Прилагательные - убираем окончания полностью
        if word.endswith('ого'):
            word = word[:-3]
        elif word.endswith('его'):
            word = word[:-3]
        elif word.endswith('ому'):
            word = word[:-3]
        elif word.endswith('ему'):
            word = word[:-3]
        elif word.endswith('ым'):
            word = word[:-2]
        elif word.endswith('им'):
            word = word[:-2]
        elif word.endswith('ой'):
            word = word[:-2]
        elif word.endswith('ый'):
            word = word[:-2]
        elif word.endswith('ая'):
            word = word[:-2]
        elif word.endswith('ое'):
            word = word[:-2]
        
        # Существительные - убираем падежные окончания
        elif word.endswith('ания'):  # образования → образован
            word = word[:-4]
        elif word.endswith('ения'):  # учреждения → учрежден
            word = word[:-4]
        elif word.endswith('ости'):  # активности → активност
            word = word[:-3]
        elif word.endswith('сти'):   # части → част
            word = word[:-3]
        elif word.endswith('ия'):    # здания → здан
            word = word[:-2]
        elif word.endswith('ая'):    # улица → улиц
            word = word[:-2]
        elif word.endswith('ое'):    # здание → здан
            word = word[:-2]
        elif word.endswith('ой'):    # домой → дом
            word = word[:-2]
        elif word.endswith('ом'):    # домом → дом
            word = word[:-2]
        elif word.endswith('ой'):    # школой → школ
            word = word[:-2]
        elif word.endswith('ом'):    # школом → школ
            word = word[:-2]
        
        return word
    
    def clean_text(self, text):
        """Очищает текст от лишних символов и приводит к базовой форме"""
        if not text:
            return ""
        
        # Приводим к нижнему регистру
        text = text.lower()
        
        # Удаляем пунктуацию и цифры
        text = re.sub(r'[^\w\s]', ' ', text)
        text = re.sub(r'\d+', ' ', text)
        
        # Удаляем лишние пробелы
        text = re.sub(r'\s+', ' ', text).strip()
        
        # Приводим слова к базовой форме (убираем окончания)
        words = text.split()
        normalized_words = []
        
        for word in words:
            if len(word) >= 3:
                # Убираем окончания для приведения к базовой форме
                normalized_word = self.normalize_word(word)
                if normalized_word:
                    normalized_words.append(normalized_word)
        
        return ' '.join(normalized_words)
    
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

def create_initial_keywords_table():
    """Создает таблицу начальных ключевых слов"""
    db_path = 'urban_analysis_fixed.db'
    
    if not os.path.exists(db_path):
        print(f"❌ База данных '{db_path}' не найдена")
        return False
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Создаем таблицу начальных ключевых слов
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS initial_keywords (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_type TEXT NOT NULL,
                keyword_type TEXT NOT NULL,
                keyword TEXT NOT NULL,
                weight REAL DEFAULT 1.0,
                is_initial BOOLEAN DEFAULT 1,
                frequency INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(group_type, keyword_type, keyword)
            )
        """)
        
        # Очищаем существующие ключевые слова
        cursor.execute("DELETE FROM initial_keywords")
        
        # Добавляем начальные ключевые слова
        added_count = 0
        for group_type, keywords in INITIAL_KEYWORDS.items():
            for keyword_type, words in keywords.items():
                for word in words:
                    cursor.execute("""
                        INSERT INTO initial_keywords 
                        (group_type, keyword_type, keyword, weight, is_initial, frequency)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (group_type, keyword_type, word, 1.0, 1, 1))
                    added_count += 1
        
        conn.commit()
        conn.close()
        
        print(f"✅ Добавлено начальных ключевых слов: {added_count}")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка создания таблицы: {e}")
        return False

def detect_group_by_initial_keywords(object_name, review_text):
    """Определяет группу объекта на основе начальных ключевых слов"""
    db_path = 'urban_analysis_fixed.db'
    
    if not os.path.exists(db_path):
        print(f"❌ База данных '{db_path}' не найдена")
        return 'undetected', 0.0
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Получаем все ключевые слова
        cursor.execute("""
            SELECT group_type, keyword_type, keyword, weight
            FROM initial_keywords
            ORDER BY group_type, keyword_type, keyword
        """)
        
        keywords = cursor.fetchall()
        
        if not keywords:
            print("❌ Нет ключевых слов в базе данных")
            conn.close()
            return 'undetected', 0.0
        
        # Нормализуем входные тексты
        processor = InitialKeywordProcessor()
        normalized_object_name = processor.clean_text(object_name or "")
        normalized_review_text = processor.clean_text(review_text or "")
        
        # print(f"🔍 Анализ объекта: '{object_name}'")
        # print(f"📝 Отзыв: '{review_text}'")
        # print(f"🔧 Нормализованное название: '{normalized_object_name}'")
        # print(f"🔧 Нормализованный отзыв: '{normalized_review_text}'")
        
        # Подсчитываем баллы для каждой группы
        group_scores = defaultdict(lambda: {'name_score': 0, 'text_score': 0, 'negative_score': 0})
        
        for group_type, keyword_type, keyword, weight in keywords:
            # Нормализуем ключевое слово так же, как и входящий текст
            normalized_keyword = processor.clean_text(keyword)
            
            # Отладочная информация для первых нескольких ключевых слов
            if len([k for k in keywords if k[1] == 'name' and k[0] == 'universities']) <= 5:
                # print(f"   🔍 Обрабатываем: {group_type}.{keyword_type} = '{keyword}' -> '{normalized_keyword}'")
                pass
            
            # Проверяем совпадения в названии объекта (для name_keywords и text_keywords)
            if keyword_type in ['name_keywords', 'text_keywords'] and normalized_object_name:
                if normalized_keyword in normalized_object_name:
                    if keyword_type == 'name_keywords':
                        group_scores[group_type]['name_score'] += weight
                        # print(f"   ✅ Найдено в названии (name_keywords): '{normalized_keyword}' в '{normalized_object_name}'")
                    else:  # text_keywords
                        group_scores[group_type]['text_score'] += weight
                        # print(f"   ✅ Найдено в названии (text_keywords): '{normalized_keyword}' в '{normalized_object_name}'")
            
            # Проверяем совпадения в тексте отзыва (для ВСЕХ ключевых слов)
            if normalized_review_text:
                if normalized_keyword in normalized_review_text:
                    if keyword_type == 'name_keywords':
                        group_scores[group_type]['name_score'] += weight * 0.5  # Сниженный вес для отзыва
                        # print(f"   ✅ Найдено в отзыве (name_keywords): '{normalized_keyword}' в '{normalized_review_text}'")
                    elif keyword_type == 'text_keywords':
                        group_scores[group_type]['text_score'] += weight * 0.5  # Сниженный вес для отзыва
                        # print(f"   ✅ Найдено в отзыве (text_keywords): '{normalized_keyword}' в '{normalized_review_text}'")
            
            # Проверяем отрицательные ключевые слова
            if keyword_type == 'negative_keywords':
                if normalized_object_name and normalized_keyword in normalized_object_name:
                    group_scores[group_type]['negative_score'] += weight
                    # print(f"   ❌ Отрицательное в названии: '{normalized_keyword}' в '{normalized_object_name}'")
                if normalized_review_text and normalized_keyword in normalized_review_text:
                    group_scores[group_type]['negative_score'] += weight
                    # print(f"   ❌ Отрицательное в отзыве: '{normalized_keyword}' в '{normalized_review_text}'")
            
            # Дополнительная проверка для извлеченных ключевых слов
            if keyword_type == 'text' and not keyword.startswith('text_'):
                # Проверяем извлеченные ключевые слова
                if normalized_object_name and normalized_keyword in normalized_object_name:
                    group_scores[group_type]['name_score'] += weight * 0.5
                    # print(f"   ✅ Извлеченное в названии: '{normalized_keyword}' в '{normalized_object_name}'")
                if normalized_review_text and normalized_keyword in normalized_review_text:
                    group_scores[group_type]['text_score'] += weight
                    # print(f"   ✅ Извлеченное в отзыве: '{normalized_keyword}' в '{normalized_review_text}'")
        
        # Отладочная информация (отключена для стабильности)
        # print(f"\n🔍 Отладочная информация:")
        # print(f"   Нормализованное название: '{normalized_object_name}'")
        # print(f"   Нормализованный отзыв: '{normalized_review_text}'")
        # print(f"   Количество ключевых слов: {len(keywords)}")
        
        # Показываем несколько примеров ключевых слов
        # print(f"   Примеры ключевых слов:")
        # for i, (group_type, keyword_type, keyword, weight) in enumerate(keywords[:5]):
        #     normalized_keyword = processor.clean_text(keyword)
        #     print(f"      {i+1}. {group_type}.{keyword_type}: '{keyword}' -> '{normalized_keyword}'")
        
        # Вычисляем итоговый балл для каждой группы
        best_group = 'undetected'
        best_score = 0.0
        tied_groups = []  # Группы с одинаковыми максимальными баллами
        
        # print(f"\n📊 Результаты подсчета баллов:")
        for group_type, scores in group_scores.items():
            # Формула: name_score + text_score - negative_score (без умножения)
            total_score = scores['name_score'] + scores['text_score'] - scores['negative_score']
            
            # print(f"   {group_type}: name={scores['name_score']}, text={scores['text_score']}, negative={scores['negative_score']}, total={total_score}")
            
            # Выбираем группу с максимальным баллом (без приоритетов)
            if total_score > best_score:
                best_score = total_score
                best_group = group_type
                tied_groups = [group_type]  # Сбрасываем список связанных групп
            elif total_score == best_score and best_score > 0:
                tied_groups.append(group_type)  # Добавляем группу с одинаковым баллом
        
        # Если несколько групп набрали одинаковые баллы, возвращаем 'undetected'
        if len(tied_groups) > 1:
            best_group = 'undetected'
            # print(f"⚠️ Несколько групп с одинаковыми баллами: {', '.join(tied_groups)}")
        
        # Нормализуем уверенность (0.0 - 1.0)
        confidence = min(best_score / 5.0, 1.0) if best_score > 0 else 0.0
        
        conn.close()
        return best_group, confidence
        
    except Exception as e:
        print(f"❌ Ошибка определения группы: {e}")
        return 'undetected', 0.0

def update_initial_keywords_from_data():
    """Обновляет ключевые слова на основе накопленных данных"""
    print("🔄 ОБНОВЛЕНИЕ КЛЮЧЕВЫХ СЛОВ НА ОСНОВЕ ДАННЫХ")
    print("=" * 60)
    
    db_path = 'urban_analysis_fixed.db'
    
    if not os.path.exists(db_path):
        print(f"❌ База данных '{db_path}' не найдена")
        return False
    
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
        
        if not group_data:
            print("❌ Недостаточно данных для обновления ключевых слов")
            conn.close()
            return False
        
        # Анализируем слова для каждой группы
        processor = InitialKeywordProcessor()
        group_words = {}
        all_words = Counter()
        
        print(f"\n📊 Анализ данных по группам:")
        for group_type, data in group_data.items():
            print(f"\n🏥 {group_type.upper()}:")
            print(f"   📄 Названий объектов: {len(data['names'])}")
            print(f"   📝 Отзывов: {len(data['reviews'])}")
            
            # Собираем слова из названий
            name_words = []
            for name in data['names']:
                name_words.extend(processor.extract_normalized_words(name))
            
            # Собираем слова из отзывов
            review_words = []
            for reviews_text in data['reviews']:
                review_words.extend(processor.extract_normalized_words(reviews_text))
            
            # Подсчитываем частоту слов
            name_word_counts = Counter(name_words)
            review_word_counts = Counter(review_words)
            
            # Объединяем все слова для группы
            group_words[group_type] = {
                'name_words': name_word_counts,
                'review_words': review_word_counts,
                'all_words': name_word_counts + review_word_counts
            }
            
            # Добавляем в общий счетчик
            all_words.update(name_word_counts)
            all_words.update(review_word_counts)
            
            print(f"   🔤 Уникальных слов в названиях: {len(name_word_counts)}")
            print(f"   🔤 Уникальных слов в отзывах: {len(review_word_counts)}")
        
        # Находим уникальные ключевые слова для каждой группы
        unique_keywords = {}
        min_frequency = 2
        
        print(f"\n🔍 Поиск уникальных ключевых слов (мин. частота: {min_frequency}):")
        
        for group_type, words_data in group_words.items():
            print(f"\n🏥 {group_type.upper()}:")
            
            # Слова, которые встречаются только в этой группе
            group_unique_words = []
            
            for word, count in words_data['all_words'].items():
                if count >= min_frequency:
                    # Проверяем, что слово не встречается в других группах
                    is_unique = True
                    for other_group, other_words_data in group_words.items():
                        if other_group != group_type:
                            if word in other_words_data['all_words']:
                                is_unique = False
                                break
                    
                    if is_unique:
                        group_unique_words.append((word, count))
            
            # Сортируем по частоте
            group_unique_words.sort(key=lambda x: x[1], reverse=True)
            
            # Берем топ-10 слов
            unique_keywords[group_type] = group_unique_words[:10]
            
            print(f"   ✅ Найдено уникальных слов: {len(group_unique_words)}")
            for word, count in group_unique_words[:5]:
                print(f"      • '{word}' (частота: {count})")
        
        # Обновляем базу данных
        if unique_keywords:
            print(f"\n💾 Обновление базы данных...")
            
            # Добавляем новые ключевые слова (не удаляем старые)
            added_count = 0
            for group_type, words in unique_keywords.items():
                for word, frequency in words:
                    try:
                        cursor.execute("""
                            INSERT INTO initial_keywords 
                            (group_type, keyword_type, keyword, weight, is_initial, frequency)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, (group_type, 'text', word, 1.0, 0, frequency))
                        added_count += 1
                    except sqlite3.IntegrityError:
                        # Слово уже существует, обновляем частоту
                        cursor.execute("""
                            UPDATE initial_keywords 
                            SET frequency = ?, updated_at = CURRENT_TIMESTAMP
                            WHERE group_type = ? AND keyword_type = ? AND keyword = ?
                        """, (frequency, group_type, 'text', word))
            
            conn.commit()
            print(f"✅ Добавлено/обновлено ключевых слов: {added_count}")
        else:
            print("❌ Не найдено новых уникальных ключевых слов")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Ошибка обновления ключевых слов: {e}")
        return False

def show_initial_keywords_status():
    """Показывает статус начальных ключевых слов"""
    db_path = 'urban_analysis_fixed.db'
    
    if not os.path.exists(db_path):
        print(f"❌ База данных '{db_path}' не найдена")
        return
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        print("📊 СТАТУС НАЧАЛЬНЫХ КЛЮЧЕВЫХ СЛОВ:")
        print("=" * 50)
        
        # Статистика по группам
        cursor.execute("""
            SELECT group_type, keyword_type, is_initial, COUNT(*) as count
            FROM initial_keywords
            GROUP BY group_type, keyword_type, is_initial
            ORDER BY group_type, keyword_type, is_initial
        """)
        
        stats = cursor.fetchall()
        
        current_group = None
        for group_type, keyword_type, is_initial, count in stats:
            if current_group != group_type:
                print(f"\n📄 {group_type.upper()}:")
                current_group = group_type
            
            type_name = {
                'name': 'Названия',
                'text': 'Текст отзывов', 
                'negative': 'Отрицательные'
            }.get(keyword_type, keyword_type)
            
            source = "Начальные" if is_initial else "Извлеченные"
            print(f"   • {type_name} ({source}): {count} слов")
        
        # Показываем примеры ключевых слов
        print("\n📝 ПРИМЕРЫ КЛЮЧЕВЫХ СЛОВ:")
        for group_type in INITIAL_KEYWORDS:
            print(f"\n🏥 {group_type.upper()}:")
            
            cursor.execute("""
                SELECT keyword_type, keyword, is_initial, frequency
                FROM initial_keywords
                WHERE group_type = ?
                ORDER BY keyword_type, is_initial DESC, frequency DESC
                LIMIT 5
            """, (group_type,))
            
            keywords = cursor.fetchall()
            
            for keyword_type, keyword, is_initial, frequency in keywords:
                type_name = {
                    'name': 'Названия',
                    'text': 'Текст отзывов',
                    'negative': 'Отрицательные'
                }.get(keyword_type, keyword_type)
                
                source = "Начальные" if is_initial else "Извлеченные"
                print(f"   • {type_name} ({source}): '{keyword}' (частота: {frequency})")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")

def test_initial_system():
    """Тестирует систему начальных ключевых слов"""
    print("=== ТЕСТИРОВАНИЕ СИСТЕМЫ НАЧАЛЬНЫХ КЛЮЧЕВЫХ СЛОВ ===")
    print(f"⏰ Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    test_cases = [
        ("Городская больница №1", "Отличная больница с профессиональными врачами"),
        ("Аптека на углу", "Удобная аптека, всегда есть нужные лекарства"),
        ("Школа №15", "Хорошая школа, но есть проблемы с питанием"),
        ("Детский сад Солнышко", "Воспитатели очень внимательные к детям"),
        ("Торговый центр Мега", "Большой выбор магазинов и товаров"),
        ("Университет МГУ", "Сложные экзамены, но качественное образование")
    ]
    
    for i, (object_name, review_text) in enumerate(test_cases, 1):
        print(f"\n🧪 Тест {i}:")
        print(f"📄 Объект: '{object_name}'")
        print(f"📝 Отзыв: '{review_text}'")
        
        detected_group, confidence = detect_group_by_initial_keywords(object_name, review_text)
        
        print(f"🎯 Результат: {detected_group} (уверенность: {confidence:.2f})")
        print("-" * 50)

def main():
    """Основная функция"""
    print("=== СИСТЕМА НАЧАЛЬНЫХ КЛЮЧЕВЫХ СЛОВ ===")
    print(f"⏰ Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Создаем таблицу начальных ключевых слов
    print("🔧 Создание таблицы начальных ключевых слов...")
    if not create_initial_keywords_table():
        print("❌ Ошибка создания таблицы")
        return
    
    # Показываем статус
    print("\n📊 Статус начальных ключевых слов:")
    show_initial_keywords_status()
    
    # Тестируем систему
    print("\n🧪 Тестирование системы начальных ключевых слов...")
    test_initial_system()
    
    # Обновляем ключевые слова на основе данных (если есть)
    print("\n🔄 Обновление ключевых слов на основе данных...")
    if update_initial_keywords_from_data():
        print("✅ Ключевые слова обновлены")
        
        # Показываем обновленный статус
        print("\n📊 Обновленный статус ключевых слов:")
        show_initial_keywords_status()
    else:
        print("ℹ️ Недостаточно данных для обновления")
    
    print("\n🎉 Система начальных ключевых слов настроена!")

if __name__ == "__main__":
    main() 