#!/usr/bin/env python3
"""
Очистка и унификация методов сентимента в базе данных
"""
import sys
import os
import sqlite3
from datetime import datetime

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def clean_and_unify_sentiment_methods():
    """Очистить старые методы и добавить только унифицированные"""
    try:
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        print("=== ОЧИСТКА И УНИФИКАЦИЯ МЕТОДОВ СЕНТИМЕНТА ===")
        print()
        
        # 1. Проверяем текущее состояние
        print("1. ТЕКУЩЕЕ СОСТОЯНИЕ:")
        cursor.execute("SELECT COUNT(*) FROM processing_methods")
        old_count = cursor.fetchone()[0]
        print(f"   Методов в БД: {old_count}")
        
        cursor.execute("SELECT COUNT(*) FROM analysis_results")
        results_count = cursor.fetchone()[0]
        print(f"   Записей в analysis_results: {results_count}")
        print()
        
        # 2. Удаляем все старые методы (если нет данных в analysis_results)
        if results_count == 0:
            print("2. УДАЛЕНИЕ СТАРЫХ МЕТОДОВ:")
            cursor.execute("DELETE FROM processing_methods")
            deleted_count = cursor.rowcount
            print(f"   Удалено методов: {deleted_count}")
        else:
            print("2. ПРЕДУПРЕЖДЕНИЕ: Есть данные в analysis_results, пропускаем удаление")
        print()
        
        # 3. Добавляем только унифицированные методы
        print("3. ДОБАВЛЕНИЕ УНИФИЦИРОВАННЫХ МЕТОДОВ:")
        
        unified_methods = [
            ('user_rating', 'Пользовательский рейтинг'),
            ('nlp_vader', 'VADER анализ'),
            ('llm_yandex', 'YandexGPT анализ'),
            ('llm_sber', 'GigaChat анализ'),
            ('llm_qwen', 'Qwen анализ'),
            ('llm_deepseek', 'DeepSeek анализ'),
            ('openai', 'OpenAI анализ'),
            ('gemini', 'Google Gemini анализ')
        ]
        
        for method_name, description in unified_methods:
            cursor.execute("""
                INSERT INTO processing_methods (method_name, description, is_active, created_at)
                VALUES (?, ?, ?, ?)
            """, (method_name, description, True, datetime.now()))
            print(f"   ✅ Добавлен: {method_name}")
        
        # 4. Проверяем результат
        print()
        print("4. РЕЗУЛЬТАТ:")
        cursor.execute("SELECT method_name, is_active FROM processing_methods ORDER BY method_name")
        final_methods = cursor.fetchall()
        print(f"   Всего методов после очистки: {len(final_methods)}")
        for method_name, is_active in final_methods:
            status = "✅ АКТИВЕН" if is_active else "❌ НЕАКТИВЕН"
            print(f"   - {method_name}: {status}")
        
        # 5. Сохраняем изменения
        conn.commit()
        print()
        print("✅ Унификация завершена успешно!")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    clean_and_unify_sentiment_methods() 