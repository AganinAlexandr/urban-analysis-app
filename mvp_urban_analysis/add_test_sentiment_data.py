#!/usr/bin/env python3
"""
Добавление тестовых данных сентимента в базу данных
"""
import sys
import os
import sqlite3
from datetime import datetime

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def add_test_sentiment_data():
    """Добавить тестовые данные сентимента"""
    try:
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        print("=== ДОБАВЛЕНИЕ ТЕСТОВЫХ ДАННЫХ СЕНТИМЕНТА ===")
        print()
        
        # 1. Проверяем текущее состояние
        print("1. ТЕКУЩЕЕ СОСТОЯНИЕ:")
        cursor.execute("SELECT COUNT(*) FROM analysis_results")
        current_results = cursor.fetchone()[0]
        print(f"   Записей в analysis_results: {current_results}")
        
        cursor.execute("SELECT COUNT(*) FROM objects")
        current_objects = cursor.fetchone()[0]
        print(f"   Объектов в БД: {current_objects}")
        print()
        
        if current_objects == 0:
            print("❌ Нет объектов в БД. Сначала добавьте объекты.")
            return
        
        # 2. Получаем методы и объекты
        cursor.execute("SELECT id, method_name FROM processing_methods WHERE is_active = 1")
        methods = cursor.fetchall()
        
        cursor.execute("SELECT id FROM objects LIMIT 5")
        objects = cursor.fetchall()
        
        print(f"2. ДОСТУПНЫЕ МЕТОДЫ: {len(methods)}")
        for method_id, method_name in methods:
            print(f"   - {method_name} (ID: {method_id})")
        print()
        
        print(f"3. ОБЪЕКТЫ ДЛЯ ТЕСТИРОВАНИЯ: {len(objects)}")
        print()
        
        # 3. Добавляем тестовые данные для нескольких методов
        test_methods = ['nlp_vader', 'llm_yandex', 'openai']  # Выбираем 3 метода для теста
        
        added_count = 0
        for method_name in test_methods:
            # Находим ID метода
            cursor.execute("SELECT id FROM processing_methods WHERE method_name = ?", (method_name,))
            method_result = cursor.fetchone()
            
            if method_result:
                method_id = method_result[0]
                print(f"4. ДОБАВЛЯЕМ ДАННЫЕ ДЛЯ МЕТОДА: {method_name}")
                
                # Добавляем данные для каждого объекта
                for obj_id, in objects:
                    cursor.execute("""
                        INSERT INTO analysis_results (review_id, method_id, sentiment, confidence, processed_at)
                        VALUES (?, ?, ?, ?, ?)
                    """, (obj_id, method_id, 'positive', 0.75, datetime.now()))
                    added_count += 1
                    print(f"   ✅ Добавлена запись для объекта {obj_id}")
                
                print()
        
        # 4. Сохраняем изменения
        conn.commit()
        print(f"✅ Добавлено записей: {added_count}")
        
        # 5. Проверяем результат
        cursor.execute("SELECT COUNT(*) FROM analysis_results")
        final_results = cursor.fetchone()[0]
        print(f"Всего записей в analysis_results: {final_results}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    add_test_sentiment_data() 