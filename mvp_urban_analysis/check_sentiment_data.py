#!/usr/bin/env python3
"""
Проверка данных сентимента в базе данных
"""
import sys
import os
import sqlite3

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def check_sentiment_data():
    """Проверить данные сентимента в БД"""
    try:
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        print("=== ПРОВЕРКА ДАННЫХ СЕНТИМЕНТА ===")
        print()
        
        # 1. Проверяем общее количество записей
        cursor.execute("SELECT COUNT(*) FROM analysis_results")
        total_results = cursor.fetchone()[0]
        print(f"1. Всего записей в analysis_results: {total_results}")
        print()
        
        # 2. Проверяем данные по методам
        cursor.execute("""
            SELECT pm.method_name, COUNT(ar.id) as count
            FROM processing_methods pm
            LEFT JOIN analysis_results ar ON pm.id = ar.method_id
            WHERE pm.is_active = 1
            GROUP BY pm.id, pm.method_name
            ORDER BY pm.method_name
        """)
        
        methods_data = cursor.fetchall()
        print("2. Данные по методам:")
        for method_name, count in methods_data:
            status = f"✅ {count} записей" if count > 0 else "❌ Нет данных"
            print(f"   - {method_name}: {status}")
        print()
        
        # 3. Проверяем маппинг методов
        print("3. МАППИНГ МЕТОДОВ:")
        mapping = {
            'user_rating': 'rating',
            'nlp_vader': 'classical_sentiment',
            'llm_yandex': 'yandexgpt_sentiment',
            'llm_sber': 'gigachat_sentiment',
            'llm_qwen': 'qwen_sentiment',
            'llm_deepseek': 'deepseek_sentiment',
            'openai': 'openai_sentiment',
            'gemini': 'google_gemini_sentiment'
        }
        
        for db_method, ui_method in mapping.items():
            # Проверяем, есть ли данные для этого метода
            cursor.execute("""
                SELECT COUNT(*) FROM analysis_results ar
                JOIN processing_methods pm ON ar.method_id = pm.id
                WHERE pm.method_name = ?
            """, (db_method,))
            count = cursor.fetchone()[0]
            status = "✅ Есть данные" if count > 0 else "❌ Нет данных"
            print(f"   {db_method} → {ui_method}: {status}")
        
        conn.close()
        
    except Exception as e:
        print(f"Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_sentiment_data() 