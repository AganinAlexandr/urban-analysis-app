#!/usr/bin/env python3
"""
Скрипт для правильной очистки базы данных
Оставляет только данные, связанные с master_ratings
"""

import sqlite3
import os
from datetime import datetime

def fix_database_cleanup():
    """Правильная очистка базы данных"""
    
    db_path = "urban_analysis_fixed.db"
    
    if not os.path.exists(db_path):
        print(f"❌ База данных {db_path} не найдена")
        return False
    
    try:
        # Подключаемся к БД
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        print("🔍 Анализ текущего состояния БД...")
        
        # Получаем статистику до очистки
        cursor.execute("SELECT COUNT(*) FROM master_ratings")
        master_ratings_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM reviews")
        reviews_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM objects")
        objects_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM analysis_results")
        analysis_results_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM processing_methods")
        processing_methods_count = cursor.fetchone()[0]
        
        print(f"📊 Статистика до очистки:")
        print(f"   - Master ratings: {master_ratings_count}")
        print(f"   - Reviews: {reviews_count}")
        print(f"   - Objects: {objects_count}")
        print(f"   - Analysis results: {analysis_results_count}")
        print(f"   - Processing methods: {processing_methods_count}")
        
        # Находим отзывы, которые имеют master_ratings
        cursor.execute("""
            SELECT DISTINCT r.id, r.object_id, r.review_text, r.rating, r.source
            FROM reviews r
            JOIN master_ratings mr ON r.id = mr.review_id
        """)
        
        reviews_with_master_ratings = cursor.fetchall()
        print(f"✅ Найдено отзывов с master_ratings: {len(reviews_with_master_ratings)}")
        
        if not reviews_with_master_ratings:
            print("❌ Нет отзывов с master_ratings!")
            return False
        
        # Создаем резервную копию
        backup_path = f"urban_analysis_fixed.db.backup_cleanup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        conn.execute("VACUUM INTO ?", (backup_path,))
        print(f"💾 Создана резервная копия: {backup_path}")
        
        # Получаем уникальные object_id из отзывов с master_ratings
        object_ids_with_master_ratings = set()
        for review_id, object_id, review_text, rating, source in reviews_with_master_ratings:
            object_ids_with_master_ratings.add(object_id)
        
        print(f"📋 Уникальных объектов с master_ratings: {len(object_ids_with_master_ratings)}")
        
        # Начинаем очистку
        print("\n🧹 Начинаем очистку БД...")
        print("💾 Сохраняем словари: processing_methods, object_groups, detected_groups")
        
        # 1. Удаляем все analysis_results
        cursor.execute("DELETE FROM analysis_results")
        print("✅ Удалены все analysis_results")
        
        # 2. Удаляем отзывы, которые НЕ имеют master_ratings
        cursor.execute("""
            DELETE FROM reviews 
            WHERE id NOT IN (
                SELECT DISTINCT review_id FROM master_ratings
            )
        """)
        print("✅ Удалены отзывы без master_ratings")
        
        # 3. Удаляем объекты, которые НЕ связаны с отзывами, имеющими master_ratings
        object_ids_str = ','.join(map(str, object_ids_with_master_ratings))
        cursor.execute(f"""
            DELETE FROM objects 
            WHERE id NOT IN ({object_ids_str})
        """)
        print("✅ Удалены объекты без master_ratings")
        
        # 4. Удаляем группы, которые НЕ связаны с оставшимися объектами
        cursor.execute("""
            DELETE FROM object_groups 
            WHERE id NOT IN (
                SELECT DISTINCT group_id FROM objects WHERE group_id IS NOT NULL
            )
        """)
        print("✅ Удалены неиспользуемые object_groups")
        
        cursor.execute("""
            DELETE FROM detected_groups 
            WHERE id NOT IN (
                SELECT DISTINCT detected_group_id FROM objects WHERE detected_group_id IS NOT NULL
            )
        """)
        print("✅ Удалены неиспользуемые detected_groups")
        
        # Получаем статистику после очистки
        cursor.execute("SELECT COUNT(*) FROM master_ratings")
        master_ratings_after = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM reviews")
        reviews_after = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM objects")
        objects_after = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM analysis_results")
        analysis_results_after = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM processing_methods")
        processing_methods_after = cursor.fetchone()[0]
        
        print(f"\n📊 Статистика после очистки:")
        print(f"   - Master ratings: {master_ratings_after}")
        print(f"   - Reviews: {reviews_after}")
        print(f"   - Objects: {objects_after}")
        print(f"   - Analysis results: {analysis_results_after}")
        print(f"   - Processing methods: {processing_methods_after}")
        
        # Проверяем целостность связей
        cursor.execute("""
            SELECT COUNT(*) FROM reviews r
            LEFT JOIN objects o ON r.object_id = o.id
            WHERE o.id IS NULL
        """)
        orphaned_reviews = cursor.fetchone()[0]
        
        if orphaned_reviews == 0:
            print("✅ Все связи целы - нет отзывов без объектов")
        else:
            print(f"❌ Найдено отзывов без объектов: {orphaned_reviews}")
        
        conn.commit()
        print("\n🎉 Очистка БД завершена успешно!")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка при очистке БД: {str(e)}")
        return False
    
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    print("🔧 Исправление очистки базы данных")
    print("=" * 50)
    
    success = fix_database_cleanup()
    
    if success:
        print("\n✅ База данных успешно очищена!")
    else:
        print("\n❌ Ошибка при очистке базы данных") 