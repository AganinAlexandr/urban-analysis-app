#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для удаления тестовых отзывов 1-5
"""

import sqlite3

def remove_test_data():
    """Удаляем тестовые отзывы 1-5 и связанные данные"""
    
    conn = sqlite3.connect('urban_analysis_fixed.db')
    cursor = conn.cursor()
    
    print("=== УДАЛЕНИЕ ТЕСТОВЫХ ДАННЫХ ===\n")
    
    # Проверяем, что есть для удаления
    cursor.execute("SELECT COUNT(*) FROM reviews WHERE id IN (1,2,3,4,5)")
    reviews_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM analysis_results WHERE review_id IN (1,2,3,4,5)")
    analysis_count = cursor.fetchone()[0]
    
    print(f"Найдено для удаления:")
    print(f"  Отзывов: {reviews_count}")
    print(f"  Результатов анализа: {analysis_count}")
    
    if reviews_count == 0:
        print("✅ Тестовые данные уже удалены!")
        conn.close()
        return
    
    print("\nУдаляем тестовые данные...")
    
    # Удаляем в правильном порядке (сначала зависимые записи)
    
    # 1. Удаляем результаты анализа
    cursor.execute("DELETE FROM analysis_results WHERE review_id IN (1,2,3,4,5)")
    print(f"  Удалено результатов анализа: {cursor.rowcount}")
    
    # 2. Удаляем master_ratings (если есть)
    cursor.execute("DELETE FROM master_ratings WHERE review_id IN (1,2,3,4,5)")
    print(f"  Удалено master_ratings: {cursor.rowcount}")
    
    # 3. Удаляем отзывы
    cursor.execute("DELETE FROM reviews WHERE id IN (1,2,3,4,5)")
    print(f"  Удалено отзывов: {cursor.rowcount}")
    
    # 4. Удаляем объекты (если они больше не используются)
    cursor.execute("""
        DELETE FROM objects 
        WHERE id IN (1,2,3) 
        AND id NOT IN (SELECT DISTINCT object_id FROM reviews)
    """)
    print(f"  Удалено неиспользуемых объектов: {cursor.rowcount}")
    
    # Сохраняем изменения
    conn.commit()
    
    print("\nПроверяем результат...")
    
    # Проверяем, что удалилось
    cursor.execute("SELECT COUNT(*) FROM reviews WHERE id IN (1,2,3,4,5)")
    remaining_reviews = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM analysis_results WHERE review_id IN (1,2,3,4,5)")
    remaining_analysis = cursor.fetchone()[0]
    
    print(f"Осталось тестовых данных:")
    print(f"  Отзывов: {remaining_reviews}")
    print(f"  Результатов анализа: {remaining_analysis}")
    
    if remaining_reviews == 0 and remaining_analysis == 0:
        print("✅ Тестовые данные успешно удалены!")
    else:
        print("❌ Что-то осталось!")
    
    # Показываем общую статистику
    cursor.execute("SELECT COUNT(*) FROM reviews")
    total_reviews = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM analysis_results")
    total_analysis = cursor.fetchone()[0]
    
    print(f"\nОбщая статистика БД:")
    print(f"  Всего отзывов: {total_reviews}")
    print(f"  Всего результатов анализа: {total_analysis}")
    
    conn.close()

if __name__ == "__main__":
    remove_test_data()






