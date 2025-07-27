#!/usr/bin/env python3
"""
Полная очистка всех данных сентимента из базы данных
"""
import sys
import os
import sqlite3

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def clear_all_sentiment_data():
    """Очистить все данные сентимента"""
    try:
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        print("=== ПОЛНАЯ ОЧИСТКА ДАННЫХ СЕНТИМЕНТА ===")
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
        
        # 2. Удаляем ВСЕ данные
        print("2. УДАЛЕНИЕ ВСЕХ ДАННЫХ:")
        cursor.execute("DELETE FROM analysis_results")
        deleted_count = cursor.rowcount
        print(f"   Удалено записей: {deleted_count}")
        
        # 3. Сохраняем изменения
        conn.commit()
        print()
        
        # 4. Проверяем результат
        cursor.execute("SELECT COUNT(*) FROM analysis_results")
        final_results = cursor.fetchone()[0]
        print(f"✅ Осталось записей в analysis_results: {final_results}")
        
        # 5. Проверяем методы
        cursor.execute("SELECT method_name FROM processing_methods WHERE is_active = 1")
        active_methods = cursor.fetchall()
        print(f"✅ Активных методов обработки: {len(active_methods)}")
        for method in active_methods:
            print(f"   - {method[0]}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    clear_all_sentiment_data() 