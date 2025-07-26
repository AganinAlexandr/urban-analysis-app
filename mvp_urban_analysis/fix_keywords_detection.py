#!/usr/bin/env python3
"""
Исправление алгоритма определения групп по ключевым словам
"""
import sqlite3
import os
import re
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

def detect_group_by_keywords_improved(object_name, review_text):
    """Улучшенное определение группы объекта на основе ключевых слов"""
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
        
        # Очищаем входные тексты
        clean_object_name = clean_text(object_name or "")
        clean_review_text = clean_text(review_text or "")
        
        # Группируем ключевые слова по группам
        group_scores = {}
        
        for group_type, keyword_type, keyword, weight in keywords:
            if group_type not in group_scores:
                group_scores[group_type] = {'name_score': 0, 'text_score': 0, 'negative_score': 0}
            
            clean_keyword = clean_text(keyword)
            
            # Проверяем совпадения в названии объекта
            if keyword_type == 'name' and clean_object_name:
                if clean_keyword in clean_object_name:
                    group_scores[group_type]['name_score'] += weight
                    print(f"   ✅ Найдено в названии: '{clean_keyword}' в '{clean_object_name}'")
            
            # Проверяем совпадения в тексте отзыва
            if keyword_type == 'text' and clean_review_text:
                if clean_keyword in clean_review_text:
                    group_scores[group_type]['text_score'] += weight
                    print(f"   ✅ Найдено в отзыве: '{clean_keyword}' в '{clean_review_text}'")
            
            # Проверяем отрицательные ключевые слова
            if keyword_type == 'negative' and clean_review_text:
                if clean_keyword in clean_review_text:
                    group_scores[group_type]['negative_score'] += weight
                    print(f"   ❌ Найдено отрицательное: '{clean_keyword}' в '{clean_review_text}'")
        
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

def test_improved_detection():
    """Тестирует улучшенную систему определения групп"""
    print("=== ТЕСТИРОВАНИЕ УЛУЧШЕННОЙ СИСТЕМЫ ===")
    print(f"⏰ Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    test_cases = [
        ("Городская больница №1", "Отличная больница с профессиональными врачами"),
        ("Аптека на углу", "Удобная аптека, всегда есть нужные лекарства"),
        ("Школа №15", "Хорошая школа, но есть проблемы с питанием"),
        ("Неизвестный объект", "Какой-то непонятный текст"),
        ("Медицинский центр", "Лечение и диагностика"),
        ("Торговый центр", "Магазины и покупки"),
        ("Тестовая больница", "Отличная больница"),
        ("Тестовая школа", "Хорошая школа"),
        ("Тестовая аптека", "Удобная аптека")
    ]
    
    for i, (object_name, review_text) in enumerate(test_cases, 1):
        print(f"\n🧪 Тест {i}:")
        print(f"📄 Объект: '{object_name}'")
        print(f"📝 Отзыв: '{review_text}'")
        
        detected_group, confidence = detect_group_by_keywords_improved(object_name, review_text)
        
        print(f"🎯 Результат: {detected_group} (уверенность: {confidence:.2f})")
        print("-" * 50)

def update_detected_groups_improved():
    """Обновляет detected_groups с улучшенным алгоритмом"""
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
            print(f"\n🔍 Анализ объекта: '{obj_name}'")
            
            # Определяем группу на основе ключевых слов
            detected_group, confidence = detect_group_by_keywords_improved(obj_name, reviews_text or "")
            
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
                print(f"   ✅ Обновлено: {detected_group} (уверенность: {confidence:.2f})")
            else:
                print(f"   ⚠️  Группа '{detected_group}' не найдена в detected_groups")
        
        conn.commit()
        conn.close()
        
        print(f"\n✅ Обновлено объектов: {updated_count}")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка обновления групп: {e}")
        return False

def main():
    """Основная функция"""
    print("=== ИСПРАВЛЕНИЕ АЛГОРИТМА ОПРЕДЕЛЕНИЯ ГРУПП ===")
    print(f"⏰ Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Тестируем улучшенную систему
    test_improved_detection()
    
    # Обновляем detected_groups
    print("\n🔧 Обновление detected_groups...")
    if update_detected_groups_improved():
        print("✅ Группы обновлены")
    else:
        print("❌ Ошибка обновления групп")
    
    print("\n🎉 Исправление завершено!")

if __name__ == "__main__":
    main() 