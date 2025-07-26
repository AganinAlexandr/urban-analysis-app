#!/usr/bin/env python3
"""
Проверка текущих методов в базе данных
"""
import sqlite3

def check_current_methods():
    """Проверяет текущие методы в базе данных"""
    print("=== ПРОВЕРКА ТЕКУЩИХ МЕТОДОВ В БД ===")
    
    try:
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        # Проверяем таблицу processing_methods
        print("\n1. Методы в таблице processing_methods:")
        cursor.execute("""
            SELECT id, method_name, description, is_active
            FROM processing_methods
            ORDER BY id
        """)
        
        methods = cursor.fetchall()
        for method_id, method_name, description, is_active in methods:
            status = "✅ активен" if is_active else "❌ неактивен"
            print(f"  ID {method_id}: {method_name} - {status}")
            if description:
                print(f"    Описание: {description}")
        
        # Проверяем использование методов в analysis_results
        print("\n2. Использование методов в analysis_results:")
        cursor.execute("""
            SELECT pm.method_name, COUNT(ar.id) as usage_count
            FROM processing_methods pm
            LEFT JOIN analysis_results ar ON pm.id = ar.method_id
            GROUP BY pm.id, pm.method_name
            ORDER BY pm.id
        """)
        
        usage_stats = cursor.fetchall()
        for method_name, usage_count in usage_stats:
            print(f"  {method_name}: {usage_count} записей")
        
        # Проверяем маппинг в API
        print("\n3. Текущий маппинг методов в API:")
        method_mapping = {
            'user_rating': 'rating',
            'nlp_vader': 'classical_sentiment',
            'textblob': 'classical_sentiment',
            'transformers': 'classical_sentiment',
            'custom_rule_based': 'classical_sentiment',
            'ensemble': 'classical_sentiment',
            'llm_openai': 'openai_sentiment',
            'llm_yandex': 'yandexgpt_sentiment'
        }
        
        for db_method, ui_method in method_mapping.items():
            print(f"  {db_method} -> {ui_method}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_current_methods() 