#!/usr/bin/env python3
"""
Детальный анализ конкретного случая с дошкольным образованием
"""

import sys
import os

# Добавляем путь к модулям приложения
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from initial_keywords_system import detect_group_by_initial_keywords, InitialKeywordProcessor

def debug_specific_case():
    """Детально анализирует конкретный случай"""
    
    print("=== ДЕТАЛЬНЫЙ АНАЛИЗ КОНКРЕТНОГО СЛУЧАЯ ===")
    
    # Тестовый объект
    object_name = "Школа № 1265, здание дошкольного образования № 4"
    review_text = "Замечательный детский сад. Обожаем его. Особенно мы любим нашу воспитательницу Анастасию Сергеевну. От воспитателя ОЧЕНЬ много зависит. Еще нам нравится Наталья Михайловна, методист. В саду очень дружная атмосфера. Дочка обожает свой садик и Анастасию Сергеевну. Придумывают много разных мероприятий и стараются для наших деток. Огромная благодарность от всей души. Низкий поклон за наших деток, за то, что душу вкладываете в них. Праздники замечательные. Дети в этом саду в дружелюбной атмосфере. Кормят вкусно. Режим четко соблюдают. Я очень рада, что мы попали в этот сад. До этого перепробовали другие. Но здесь просто чудо. Мы рады говорим большое СПАСИБО!!"
    
    print(f"🔍 Анализируем объект:")
    print(f"   Название: {object_name}")
    print(f"   Отзыв: {review_text[:100]}...")
    print()
    
    # Нормализуем тексты
    processor = InitialKeywordProcessor()
    normalized_name = processor.clean_text(object_name)
    normalized_review = processor.clean_text(review_text)
    
    print(f"🔧 Нормализованные тексты:")
    print(f"   Название: '{normalized_name}'")
    print(f"   Отзыв: '{normalized_review[:100]}...'")
    print()
    
    # Получаем результат
    detected_group, confidence = detect_group_by_initial_keywords(object_name, review_text)
    
    print(f"🎯 Результат определения:")
    print(f"   Определенная группа: {detected_group}")
    print(f"   Уверенность: {confidence:.2f}")
    print()
    
    # Теперь детально анализируем каждую группу
    print("=== ДЕТАЛЬНЫЙ АНАЛИЗ ПО ГРУППАМ ===")
    
    import sqlite3
    
    db_path = 'urban_analysis_fixed.db'
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Получаем все ключевые слова
    cursor.execute("""
        SELECT group_type, keyword_type, keyword, weight
        FROM initial_keywords
        ORDER BY group_type, keyword_type, keyword
    """)
    
    keywords = cursor.fetchall()
    
    # Группируем по группам
    group_keywords = {}
    for group_type, keyword_type, keyword, weight in keywords:
        if group_type not in group_keywords:
            group_keywords[group_type] = []
        group_keywords[group_type].append((keyword_type, keyword, weight))
    
    # Анализируем каждую группу
    group_scores = {}
    
    for group_type, keywords_list in group_keywords.items():
        print(f"\n📊 Группа: {group_type}")
        
        name_score = 0
        text_score = 0
        matches = []
        
        for keyword_type, keyword, weight in keywords_list:
            normalized_keyword = processor.clean_text(keyword)
            
            # Проверяем совпадения в названии
            if keyword_type == 'name_keywords' and normalized_keyword in normalized_name:
                name_score += weight
                matches.append(f"✅ '{keyword}' → '{normalized_keyword}' в названии (+{weight})")
            
            # Проверяем совпадения в отзыве
            elif keyword_type == 'text_keywords' and normalized_keyword in normalized_review:
                text_score += weight
                matches.append(f"✅ '{keyword}' → '{normalized_keyword}' в отзыве (+{weight})")
            
            # Проверяем извлеченные ключевые слова
            elif keyword_type == 'text' and not keyword.startswith('text_'):
                if normalized_keyword in normalized_name:
                    name_score += weight * 0.5
                    matches.append(f"✅ '{keyword}' → '{normalized_keyword}' в названии (извлеченное, +{weight*0.5})")
                if normalized_keyword in normalized_review:
                    text_score += weight
                    matches.append(f"✅ '{keyword}' → '{normalized_keyword}' в отзыве (извлеченное, +{weight})")
        
        total_score = name_score + text_score
        group_scores[group_type] = total_score
        
        print(f"   Совпадения:")
        if matches:
            for match in matches:
                print(f"     {match}")
        else:
            print(f"     ❌ Совпадений не найдено")
        
        print(f"   Баллы: название={name_score:.1f}, отзыв={text_score:.1f}, всего={total_score:.1f}")
    
    conn.close()
    
    # Показываем итоговую таблицу
    print(f"\n=== ИТОГОВАЯ ТАБЛИЦА БАЛЛОВ ===")
    sorted_groups = sorted(group_scores.items(), key=lambda x: x[1], reverse=True)
    
    for i, (group_type, score) in enumerate(sorted_groups):
        marker = "🥇" if i == 0 else "🥈" if i == 1 else "🥉" if i == 2 else "  "
        print(f"{marker} {group_type}: {score:.1f} баллов")
    
    print(f"\n🎯 Ожидаемый результат: kindergarden (должен быть первым)")
    print(f"🔍 Фактический результат: {detected_group}")
    
    if sorted_groups[0][0] == detected_group:
        print(f"✅ Система работает корректно!")
    else:
        print(f"❌ Система определила неправильно!")

if __name__ == "__main__":
    debug_specific_case()




