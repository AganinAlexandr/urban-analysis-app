#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Детальный анализ определения групп для Smile Fish
"""

import sys
import os

# Добавляем путь к модулям
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from initial_keywords_system import InitialKeywordProcessor, detect_group_by_initial_keywords

def debug_smile_fish_detailed():
    """Детальный анализ определения групп"""
    
    print("=== ДЕТАЛЬНЫЙ АНАЛИЗ SMILE FISH ===")
    
    # Тестовые данные
    test_cases = [
        {
            'name': 'Smile Fish',
            'review': 'Очень понравился детский сад! Чисто, уютно, много различных занятий, даже английский язык. Понравилось, что это семейный бизнес, большая включенность и контроль со стороны руководителей.'
        },
        {
            'name': 'Smile Fish',
            'review': 'Отличный сад! Лучший сад, в который ходил мой ребенок. С первого дня у сына возникла любовь с этим детским садом, каждый день идёт с радостью. По выходным и в отпуске скучает по воспитателям.'
        }
    ]
    
    # Создаем процессор для анализа
    processor = InitialKeywordProcessor()
    
    for i, case in enumerate(test_cases, 1):
        print(f"\n🔍 ТЕСТ {i}:")
        print(f"Название: {case['name']}")
        print(f"Отзыв: {case['review']}")
        
        # Нормализуем тексты
        normalized_name = processor.clean_text(case['name'])
        normalized_review = processor.clean_text(case['review'])
        
        print(f"\n📝 Нормализованные тексты:")
        print(f"Название: '{normalized_name}'")
        print(f"Отзыв: '{normalized_review}'")
        
        # Проверяем каждую группу отдельно
        print(f"\n🎯 АНАЛИЗ ПО ГРУППАМ:")
        
        groups_to_check = ['kindergarden', 'resident_complex', 'school', 'hospital']
        
        for group_type in groups_to_check:
            print(f"\n📊 Группа: {group_type}")
            
            try:
                # Получаем ключевые слова для группы
                import sqlite3
                conn = sqlite3.connect('urban_analysis_fixed.db')
                cursor = conn.cursor()
                
                cursor.execute("""
                    SELECT keyword_type, keyword, weight 
                    FROM initial_keywords 
                    WHERE group_type = ?
                """, (group_type,))
                
                keywords = cursor.fetchall()
                conn.close()
                
                if not keywords:
                    print(f"  ❌ Ключевых слов не найдено")
                    continue
                
                # Анализируем каждое ключевое слово
                name_score = 0
                text_score = 0
                found_keywords = []
                
                for keyword_type, keyword, weight in keywords:
                    normalized_keyword = processor.clean_text(keyword)
                    
                    # Ищем в названии
                    if normalized_keyword in normalized_name:
                        name_score += weight
                        found_keywords.append(f"'{keyword}' в названии (+{weight})")
                    
                    # Ищем в отзыве
                    if normalized_keyword in normalized_review:
                        text_score += weight
                        found_keywords.append(f"'{keyword}' в отзыве (+{weight})")
                
                total_score = name_score + text_score
                
                if found_keywords:
                    print(f"  ✅ Найдены ключевые слова:")
                    for found in found_keywords:
                        print(f"    {found}")
                    print(f"  📊 Баллы: название={name_score}, отзыв={text_score}, всего={total_score}")
                else:
                    print(f"  ❌ Ключевые слова не найдены")
                    print(f"  📊 Баллы: название={name_score}, отзыв={text_score}, всего={total_score}")
                
            except Exception as e:
                print(f"  ❌ Ошибка анализа: {e}")
        
        # Финальный результат
        try:
            detected_group, confidence = detect_group_by_initial_keywords(case['name'], case['review'])
            print(f"\n🎯 ФИНАЛЬНЫЙ РЕЗУЛЬТАТ:")
            print(f"Определенная группа: {detected_group}")
            print(f"Уверенность: {confidence}")
        except Exception as e:
            print(f"❌ Ошибка определения группы: {e}")

if __name__ == "__main__":
    debug_smile_fish_detailed()



