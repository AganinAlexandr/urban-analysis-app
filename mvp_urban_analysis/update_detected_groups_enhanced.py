#!/usr/bin/env python3
"""
Обновление detected_groups с улучшенным алгоритмом
"""
import sqlite3
import os
from datetime import datetime
from simple_enhanced_keywords import SimpleKeywordProcessor, detect_group_by_simple_enhanced_keywords

def update_detected_groups_enhanced():
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
            detected_group, confidence = detect_group_by_simple_enhanced_keywords(obj_name, reviews_text or "")
            
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

def show_detection_results():
    """Показывает результаты определения групп"""
    db_path = 'urban_analysis_fixed.db'
    
    if not os.path.exists(db_path):
        print(f"❌ База данных '{db_path}' не найдена")
        return
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        print("\n📊 РЕЗУЛЬТАТЫ ОПРЕДЕЛЕНИЯ ГРУПП:")
        print("=" * 50)
        
        cursor.execute("""
            SELECT o.name, og.group_name, og.group_type, 
                   dg.group_name as detected_group_name, dg.group_type as detected_group_type
            FROM objects o
            LEFT JOIN object_groups og ON o.group_id = og.id
            LEFT JOIN detected_groups dg ON o.detected_group_id = dg.id
            ORDER BY o.name
        """)
        
        objects = cursor.fetchall()
        
        for obj_name, group_name, group_type, detected_group_name, detected_group_type in objects:
            print(f"\n🏢 Объект: '{obj_name}'")
            print(f"   Исходная группа: {group_name} ({group_type})")
            print(f"   Определенная группа: {detected_group_name} ({detected_group_type})")
            
            # Проверяем совпадение
            if group_type == detected_group_type:
                print(f"   ✅ Совпадение: ДА")
            else:
                print(f"   ❌ Совпадение: НЕТ")
        
        # Статистика
        cursor.execute("""
            SELECT 
                COUNT(*) as total_objects,
                SUM(CASE WHEN og.group_type = dg.group_type THEN 1 ELSE 0 END) as matched,
                SUM(CASE WHEN og.group_type != dg.group_type THEN 1 ELSE 0 END) as mismatched
            FROM objects o
            LEFT JOIN object_groups og ON o.group_id = og.id
            LEFT JOIN detected_groups dg ON o.detected_group_id = dg.id
        """)
        
        stats = cursor.fetchone()
        if stats:
            total, matched, mismatched = stats
            accuracy = (matched / total * 100) if total > 0 else 0
            print(f"\n📈 СТАТИСТИКА:")
            print(f"   Всего объектов: {total}")
            print(f"   Совпадений: {matched}")
            print(f"   Несовпадений: {mismatched}")
            print(f"   Точность: {accuracy:.1f}%")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")

def main():
    """Основная функция"""
    print("=== ОБНОВЛЕНИЕ DETECTED_GROUPS С УЛУЧШЕННЫМ АЛГОРИТМОМ ===")
    print(f"⏰ Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Обновляем detected_groups
    print("🔧 Обновление detected_groups...")
    if update_detected_groups_enhanced():
        print("✅ Группы обновлены")
    else:
        print("❌ Ошибка обновления групп")
    
    # Показываем результаты
    show_detection_results()
    
    print("\n🎉 Обновление завершено!")

if __name__ == "__main__":
    main() 