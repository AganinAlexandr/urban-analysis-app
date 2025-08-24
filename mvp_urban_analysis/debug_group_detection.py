#!/usr/bin/env python3
"""
Отладочный анализ определения группы
"""
from initial_keywords_system import detect_group_by_initial_keywords, InitialKeywordProcessor

def debug_group_detection():
    """Детальный анализ проблемы с определением группы"""
    
    # Тестовые данные
    object_name = "Институт органической химии имени Н.Д. Зелинского"
    review_text = "Отличный институт, хорошие преподаватели"
    
    print("🔍 Детальный анализ определения группы")
    print(f"📝 Исходное название: '{object_name}'")
    print(f"📝 Исходный отзыв: '{review_text}'")
    print("-" * 50)
    
    # Создаем процессор для анализа
    processor = InitialKeywordProcessor()
    
    # Анализируем очистку текста
    cleaned_object = processor.clean_text(object_name)
    cleaned_review = processor.clean_text(review_text)
    
    print(f"🔧 Очищенное название: '{cleaned_object}'")
    print(f"🔧 Очищенный отзыв: '{cleaned_review}'")
    print("-" * 50)
    
    # Проверяем, содержит ли очищенный текст слово "институт"
    if "институт" in cleaned_object:
        print("✅ Слово 'институт' найдено в очищенном названии")
    else:
        print("❌ Слово 'институт' НЕ найдено в очищенном названии")
    
    if "институт" in cleaned_review:
        print("✅ Слово 'институт' найдено в очищенном отзыве")
    else:
        print("❌ Слово 'институт' НЕ найдено в очищенном отзыве")
    
    print("-" * 50)
    
    # Проверяем ключевые слова в базе данных
    import sqlite3
    conn = sqlite3.connect('urban_analysis_fixed.db')
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT keyword, keyword_type FROM initial_keywords 
        WHERE group_type = 'universities' AND keyword_type = 'name_keywords'
    """)
    
    universities_keywords = cursor.fetchall()
    print("🔍 Ключевые слова для universities (name_keywords):")
    for keyword, keyword_type in universities_keywords:
        print(f"   - '{keyword}' ({keyword_type})")
    
    print("-" * 50)
    
    # Проверяем каждое ключевое слово
    for keyword, keyword_type in universities_keywords:
        if keyword in cleaned_object:
            print(f"✅ Найдено ключевое слово '{keyword}' в названии")
        else:
            print(f"❌ Ключевое слово '{keyword}' НЕ найдено в названии")
    
    conn.close()
    
    print("-" * 50)
    
    # Запускаем полный тест
    detected_group, confidence = detect_group_by_initial_keywords(object_name, review_text)
    
    print(f"\n✅ Финальный результат:")
    print(f"   Определенная группа: {detected_group}")
    print(f"   Уверенность: {confidence}")

if __name__ == "__main__":
    debug_group_detection() 