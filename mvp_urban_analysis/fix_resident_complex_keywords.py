#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Исправление слишком общих ключевых слов для resident_complex
"""

import sqlite3
import os

def fix_resident_complex_keywords():
    """Удаляем слишком общие ключевые слова для resident_complex"""
    
    db_path = 'urban_analysis_fixed.db'
    
    if not os.path.exists(db_path):
        print(f"❌ База данных {db_path} не найдена")
        return
    
    # Слишком общие ключевые слова для удаления
    keywords_to_remove = [
        'дом',      # слишком общее
        'квартира', # слишком общее
        'жилье',    # слишком общее
        'сосед'     # может встречаться в любом контексте
    ]
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        print("=== ИСПРАВЛЕНИЕ КЛЮЧЕВЫХ СЛОВ ДЛЯ RESIDENT_COMPLEX ===")
        
        # Показываем текущие ключевые слова
        print("\n🔍 Текущие ключевые слова для resident_complex:")
        cursor.execute("SELECT * FROM initial_keywords WHERE group_type = 'resident_complex'")
        rows = cursor.fetchall()
        
        for row in rows:
            print(f"  {row[1]} ({row[2]}): {row[3]} (вес: {row[4]})")
        
        # Удаляем слишком общие ключевые слова
        print(f"\n🗑️ Удаляем слишком общие ключевые слова: {keywords_to_remove}")
        
        for keyword in keywords_to_remove:
            cursor.execute("""
                DELETE FROM initial_keywords 
                WHERE group_type = 'resident_complex' AND keyword = ?
            """, (keyword,))
            
            deleted_count = cursor.rowcount
            if deleted_count > 0:
                print(f"  ✅ Удалено: '{keyword}'")
            else:
                print(f"  ⚠️ Не найдено: '{keyword}'")
        
        # Показываем обновленные ключевые слова
        print("\n🔍 Обновленные ключевые слова для resident_complex:")
        cursor.execute("SELECT * FROM initial_keywords WHERE group_type = 'resident_complex'")
        rows = cursor.fetchall()
        
        for row in rows:
            print(f"  {row[1]} ({row[2]}): {row[3]} (вес: {row[4]})")
        
        # Сохраняем изменения
        conn.commit()
        print(f"\n✅ Изменения сохранены в базе данных")
        
        # Проверяем общее количество ключевых слов
        cursor.execute("SELECT COUNT(*) FROM initial_keywords")
        total_count = cursor.fetchone()[0]
        print(f"📊 Всего ключевых слов в базе: {total_count}")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    fix_resident_complex_keywords()




