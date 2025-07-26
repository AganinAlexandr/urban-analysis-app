#!/usr/bin/env python3
"""
Стандартизация методов сентимента
"""
import sqlite3
from datetime import datetime

def standardize_methods():
    """Стандартизирует методы сентимента согласно новому списку"""
    print("=== СТАНДАРТИЗАЦИЯ МЕТОДОВ СЕНТИМЕНТА ===")
    
    # Новый стандартный список методов
    standard_methods = [
        ('user_rating', 'Рейтинг пользователя'),
        ('nlp_vader', 'VADER анализ'),
        ('llm_yandex', 'YandexGPT'),
        ('llm_sber', 'GigaChat'),
        ('llm_qwen', 'Qwen'),
        ('llm_deepseek', 'DeepSeek'),
        ('openai', 'OpenAI GPT'),
        ('gemini', 'Google Gemini')
    ]
    
    try:
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        print("1. Очищаем таблицу processing_methods...")
        cursor.execute("DELETE FROM processing_methods")
        print("✅ Таблица очищена")
        
        print("\n2. Добавляем стандартные методы...")
        for method_name, description in standard_methods:
            cursor.execute("""
                INSERT INTO processing_methods 
                (method_name, description, is_active, created_at)
                VALUES (?, ?, ?, ?)
            """, (method_name, description, 1, datetime.now()))
            print(f"✅ Добавлен: {method_name}")
        
        conn.commit()
        print(f"\n✅ Добавлено методов: {len(standard_methods)}")
        
        # Показываем результат
        print("\n3. Проверяем результат:")
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
        
        # Обновляем маппинг в API
        print("\n4. Новый маппинг для API:")
        new_mapping = {
            'user_rating': 'rating',
            'nlp_vader': 'classical_sentiment',
            'llm_yandex': 'yandexgpt_sentiment',
            'llm_sber': 'gigachat_sentiment',
            'llm_qwen': 'qwen_sentiment',
            'llm_deepseek': 'deepseek_sentiment',
            'openai': 'openai_sentiment',
            'gemini': 'google_gemini_sentiment'
        }
        
        for db_method, ui_method in new_mapping.items():
            print(f"  {db_method} -> {ui_method}")
        
        conn.close()
        
        print("\n✅ Стандартизация завершена!")
        print("\nСледующие шаги:")
        print("1. Обновить маппинг в app.py")
        print("2. Обновить названия методов в index.html")
        print("3. Перезапустить приложение")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    standardize_methods() 