#!/usr/bin/env python3
"""
Анализ проблем целостности данных и предложения решений
"""
import sqlite3

def analyze_data_integrity_issues():
    """Анализирует проблемы целостности данных и предлагает решения"""
    print("=== АНАЛИЗ ПРОБЛЕМ ЦЕЛОСТНОСТИ ДАННЫХ ===")
    
    try:
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        # 1. Анализ текущей проблемы
        print("1. Проблема с ключами связи:")
        print("   ❌ Ключ master_ratings.review_id основан на reviews.id")
        print("   ❌ При очистке reviews ID изменяются")
        print("   ❌ Master_ratings теряют связь с отзывами")
        print("   ❌ Дубликаты отзывов не учитываются")
        
        # 2. Проверяем дубликаты отзывов
        print("\n2. Проверка дубликатов отзывов:")
        cursor.execute("""
            SELECT review_text, COUNT(*) as count
            FROM reviews
            GROUP BY review_text
            HAVING COUNT(*) > 1
            ORDER BY count DESC
        """)
        duplicates = cursor.fetchall()
        
        if duplicates:
            print("   Найдены дубликаты отзывов:")
            for text, count in duplicates:
                print(f"     '{text[:50]}...' - {count} раз")
        else:
            print("   Дубликатов отзывов не найдено")
        
        # 3. Анализ вариантов решения
        print("\n3. Варианты решения:")
        
        print("   Вариант A: Удаление master_ratings при очистке")
        print("     ✅ Простота реализации")
        print("     ❌ Потеря ценных данных master_ratings")
        print("     ❌ Необходимость повторного ввода")
        
        print("\n   Вариант B: Сохранение отзывов с master_ratings")
        print("     ✅ Сохранение всех данных")
        print("     ❌ Сложность реализации")
        print("     ❌ Возможные конфликты при обновлении")
        
        print("\n   Вариант C: Ключ на основе хеша текста")
        print("     ✅ Связь по содержанию, а не по ID")
        print("     ✅ Устойчивость к изменениям ID")
        print("     ❌ Необходимость изменения структуры БД")
        
        print("\n   Вариант D: Составной ключ (текст + объект)")
        print("     ✅ Уникальная идентификация отзыва")
        print("     ✅ Устойчивость к изменениям ID")
        print("     ❌ Сложность реализации")
        
        # 4. Рекомендуемое решение
        print("\n4. Рекомендуемое решение:")
        print("   🎯 Вариант C: Ключ на основе хеша текста")
        print("   Причины:")
        print("     - Master_ratings привязаны к содержанию отзыва")
        print("     - Устойчивость к изменениям ID в reviews")
        print("     - Автоматическое разрешение дубликатов")
        print("     - Простота миграции данных")
        
        # 5. План реализации
        print("\n5. План реализации:")
        print("   1. Добавить колонку text_hash в master_ratings")
        print("   2. Создать функцию хеширования текста отзывов")
        print("   3. Мигрировать существующие данные")
        print("   4. Обновить логику связывания")
        print("   5. Добавить индекс по text_hash")
        
        # 6. Проверка текущих данных
        print("\n6. Текущее состояние данных:")
        cursor.execute("SELECT COUNT(*) FROM reviews")
        reviews_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM master_ratings")
        master_ratings_count = cursor.fetchone()[0]
        
        print(f"   Отзывов: {reviews_count}")
        print(f"   Master_ratings: {master_ratings_count}")
        print(f"   Соотношение: {master_ratings_count/reviews_count*100:.1f}%")
        
        # 7. Примеры хеширования
        print("\n7. Примеры хеширования отзывов:")
        cursor.execute("SELECT review_text FROM reviews LIMIT 3")
        examples = cursor.fetchall()
        
        import hashlib
        for i, (text,) in enumerate(examples, 1):
            text_hash = hashlib.md5(text.encode('utf-8')).hexdigest()[:16]
            print(f"     Отзыв {i}: {text[:50]}...")
            print(f"     Хеш: {text_hash}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    analyze_data_integrity_issues() 