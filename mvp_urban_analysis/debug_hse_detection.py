#!/usr/bin/env python3
"""
Детальная отладка определения группы для ВШЭ
"""
from initial_keywords_system import detect_group_by_initial_keywords

def debug_hse_detection():
    """Детально отлаживает определение группы для ВШЭ"""
    print("=== ДЕТАЛЬНАЯ ОТЛАДКА ОПРЕДЕЛЕНИЯ ГРУППЫ ДЛЯ ВШЭ ===")
    
    object_name = "Национальный исследовательский университет Высшая школа экономики"
    review_text = ""
    
    print(f"🔍 Анализируем: '{object_name}'")
    print(f"📝 Отзыв: '{review_text}'")
    print("=" * 80)
    
    # Запускаем определение с полной отладкой
    detected_group, confidence = detect_group_by_initial_keywords(object_name, review_text)
    
    print("=" * 80)
    print(f"🎯 РЕЗУЛЬТАТ: {detected_group} (уверенность: {confidence})")
    
    # Дополнительный анализ
    print("\n🔍 ДОПОЛНИТЕЛЬНЫЙ АНАЛИЗ:")
    print("Слова в названии:")
    words = object_name.lower().split()
    for word in words:
        print(f"  • '{word}'")
    
    print("\nКлючевые слова для universities:")
    university_keywords = [
        'университет', 'институт', 'академия', 'вуз', 'высшее образование',
        'студент', 'преподаватель', 'лекция', 'семинар', 'экзамен', 'сессия', 
        'диплом', 'кафедра', 'факультет', 'ректор', 'лектор'
    ]
    for keyword in university_keywords:
        if keyword in object_name.lower():
            print(f"  ✅ '{keyword}' найдено")
        else:
            print(f"  ❌ '{keyword}' не найдено")
    
    print("\nКлючевые слова для schools:")
    school_keywords = [
        'школа', 'лицей', 'гимназия', 'образовательный центр', 'учебное заведение',
        'учитель', 'ученик', 'урок', 'класс', 'образование', 'обучение', 
        'директор', 'завуч', 'предмет', 'экзамен'
    ]
    for keyword in school_keywords:
        if keyword in object_name.lower():
            print(f"  ✅ '{keyword}' найдено")
        else:
            print(f"  ❌ '{keyword}' не найдено")

if __name__ == "__main__":
    debug_hse_detection() 