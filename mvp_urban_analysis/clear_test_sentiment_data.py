#!/usr/bin/env python3
"""
Очистка тестовых данных сентимента из базы данных
"""
import sys
import os
import sqlite3

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def clear_test_sentiment_data():
    """Очистить тестовые данные сентимента"""
    try:
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        print("=== ОЧИСТКА ТЕСТОВЫХ ДАННЫХ СЕНТИМЕНТА ===")
        print()
        
        # 1. Проверяем текущее состояние
        print("1. ТЕКУЩЕЕ СОСТОЯНИЕ:")
        cursor.execute("SELECT COUNT(*) FROM analysis_results")
        current_results = cursor.fetchone()[0]
        print(f"   Записей в analysis_results: {current_results}")
        print()
        
        if current_results == 0:
            print("✅ Данные уже очищены")
            return
        
        # 2. Удаляем тестовые данные
        print("2. УДАЛЕНИЕ ТЕСТОВЫХ ДАННЫХ:")
        
        # Удаляем данные для тестовых методов
        test_methods = ['nlp_vader', 'llm_yandex', 'openai']
        
        for method_name in test_methods:
            cursor.execute("""
                DELETE FROM analysis_results 
                WHERE method_id IN (
                    SELECT id FROM processing_methods 
                    WHERE method_name = ?
                )
            """, (method_name,))
            deleted_count = cursor.rowcount
            print(f"   Удалено записей для {method_name}: {deleted_count}")
        
        # 3. Сохраняем изменения
        conn.commit()
        print()
        
        # 4. Проверяем результат
        cursor.execute("SELECT COUNT(*) FROM analysis_results")
        final_results = cursor.fetchone()[0]
        print(f"✅ Осталось записей в analysis_results: {final_results}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    clear_test_sentiment_data() 