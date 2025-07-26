#!/usr/bin/env python3
"""
Скрипт для исправления проблем в базе данных
"""
import sqlite3
import os
import shutil
from datetime import datetime

def fix_database_issues():
    """Исправляет проблемы в базе данных"""
    db_path = 'urban_analysis_fixed.db'
    
    if not os.path.exists(db_path):
        print(f"❌ База данных '{db_path}' не найдена")
        return False
    
    print("=== ИСПРАВЛЕНИЕ ПРОБЛЕМ В БАЗЕ ДАННЫХ ===")
    print(f"📁 Файл: {db_path}")
    print(f"⏰ Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Создаем резервную копию
    backup_path = f"{db_path}.backup_fix_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    try:
        shutil.copy2(db_path, backup_path)
        print(f"💾 Создана резервная копия: {backup_path}")
    except Exception as e:
        print(f"❌ Ошибка создания резервной копии: {e}")
        return False
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        print("🔧 ИСПРАВЛЕНИЕ 1: Синхронизация detected_groups с object_groups")
        
        # Получаем все группы из object_groups
        cursor.execute("SELECT id, group_name, group_type FROM object_groups")
        object_groups = cursor.fetchall()
        
        # Проверяем detected_groups
        cursor.execute("SELECT COUNT(*) FROM detected_groups")
        detected_count = cursor.fetchone()[0]
        print(f"   📊 Текущих записей в detected_groups: {detected_count}")
        
        # Добавляем недостающие группы в detected_groups
        added_count = 0
        for group_id, group_name, group_type in object_groups:
            cursor.execute("SELECT COUNT(*) FROM detected_groups WHERE group_name = ? AND group_type = ?", 
                         (group_name, group_type))
            exists = cursor.fetchone()[0]
            
            if not exists:
                cursor.execute("""
                    INSERT INTO detected_groups (group_name, group_type, detection_method, confidence, created_at)
                    VALUES (?, ?, 'manual_mapping', 1.0, CURRENT_TIMESTAMP)
                """, (group_name, group_type))
                added_count += 1
                print(f"   ✅ Добавлена группа: {group_name} ({group_type})")
        
        # Добавляем группу "undetected"
        cursor.execute("SELECT COUNT(*) FROM detected_groups WHERE group_name = 'undetected'")
        undetected_exists = cursor.fetchone()[0]
        
        if not undetected_exists:
            cursor.execute("""
                INSERT INTO detected_groups (group_name, group_type, detection_method, confidence, created_at)
                VALUES ('undetected', 'unknown', 'manual_mapping', 0.0, CURRENT_TIMESTAMP)
            """)
            print("   ✅ Добавлена группа: undetected")
            added_count += 1
        
        print(f"   📊 Добавлено новых групп: {added_count}")
        
        print("\n🔧 ИСПРАВЛЕНИЕ 2: Добавление поля answer_text в таблицу reviews")
        
        # Проверяем, есть ли уже поле answer_text
        cursor.execute("PRAGMA table_info(reviews)")
        columns = [col[1] for col in cursor.fetchall()]
        
        if 'answer_text' not in columns:
            cursor.execute("ALTER TABLE reviews ADD COLUMN answer_text TEXT")
            print("   ✅ Добавлено поле answer_text в таблицу reviews")
        else:
            print("   ℹ️  Поле answer_text уже существует")
        
        print("\n🔧 ИСПРАВЛЕНИЕ 3: Анализ проблемы с результатами анализа")
        
        # Проверяем количество отзывов и результатов анализа
        cursor.execute("SELECT COUNT(*) FROM reviews")
        reviews_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM analysis_results")
        analysis_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM processing_methods")
        methods_count = cursor.fetchone()[0]
        
        print(f"   📊 Отзывов: {reviews_count}")
        print(f"   📊 Результатов анализа: {analysis_count}")
        print(f"   📊 Методов обработки: {methods_count}")
        
        # Проверяем методы обработки
        cursor.execute("SELECT method_name FROM processing_methods")
        methods = cursor.fetchall()
        print("   📋 Доступные методы:")
        for method in methods:
            print(f"     • {method[0]}")
        
        # Проверяем, какие отзывы не имеют результатов анализа
        cursor.execute("""
            SELECT r.id, r.review_text, r.rating 
            FROM reviews r 
            LEFT JOIN analysis_results ar ON r.id = ar.review_id 
            WHERE ar.id IS NULL
        """)
        missing_analysis = cursor.fetchall()
        
        print(f"   ⚠️  Отзывов без результатов анализа: {len(missing_analysis)}")
        
        if missing_analysis:
            print("   📝 Примеры отзывов без анализа:")
            for i, (review_id, text, rating) in enumerate(missing_analysis[:3]):
                print(f"     • ID {review_id}: {text[:50]}... (рейтинг: {rating})")
        
        # Проверяем, какие методы не используются
        cursor.execute("""
            SELECT pm.method_name 
            FROM processing_methods pm 
            LEFT JOIN analysis_results ar ON pm.id = ar.method_id 
            WHERE ar.id IS NULL
        """)
        unused_methods = cursor.fetchall()
        
        if unused_methods:
            print("   📋 Неиспользуемые методы:")
            for method in unused_methods:
                print(f"     • {method[0]}")
        
        conn.commit()
        conn.close()
        
        print("\n✅ ИСПРАВЛЕНИЯ ЗАВЕРШЕНЫ УСПЕШНО!")
        
        # Финальная статистика
        print("\n📊 ФИНАЛЬНАЯ СТАТИСТИКА:")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM detected_groups")
        detected_final = cursor.fetchone()[0]
        print(f"   • detected_groups: {detected_final} записей")
        
        cursor.execute("SELECT COUNT(*) FROM object_groups")
        object_groups_final = cursor.fetchone()[0]
        print(f"   • object_groups: {object_groups_final} записей")
        
        cursor.execute("SELECT COUNT(*) FROM reviews")
        reviews_final = cursor.fetchone()[0]
        print(f"   • reviews: {reviews_final} записей")
        
        cursor.execute("SELECT COUNT(*) FROM analysis_results")
        analysis_final = cursor.fetchone()[0]
        print(f"   • analysis_results: {analysis_final} записей")
        
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"❌ ОШИБКА ПРИ ИСПРАВЛЕНИИ: {e}")
        return False

if __name__ == "__main__":
    success = fix_database_issues()
    if success:
        print("\n🎉 Все исправления применены успешно!")
    else:
        print("\n❌ Произошли ошибки при исправлении") 