"""
Добавление метода yandex_gpt в базу данных
"""

import sqlite3
import os
from dotenv import load_dotenv
from datetime import datetime

# Загружаем переменные окружения
load_dotenv('env_data.env')

def add_yandex_gpt_method():
    """Добавляет метод yandex_gpt в таблицу processing_methods"""
    
    # Путь к базе данных
    db_path = 'urban_analysis_fixed.db'
    
    if not os.path.exists(db_path):
        print(f"❌ База данных {db_path} не найдена")
        return False
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Проверяем, существует ли уже метод yandex_gpt
        cursor.execute("SELECT method_name FROM processing_methods WHERE method_name = ?", ('yandex_gpt',))
        existing = cursor.fetchone()
        
        if existing:
            print(f"✅ Метод 'yandex_gpt' уже существует в базе данных")
            return True
        
        # Добавляем новый метод
        cursor.execute("""
            INSERT INTO processing_methods (method_name, description, is_active, created_at)
            VALUES (?, ?, ?, datetime('now'))
        """, ('yandex_gpt', 'Yandex GPT API для анализа сентимента', 1))
        
        conn.commit()
        print(f"✅ Метод 'yandex_gpt' успешно добавлен в базу данных")
        
        # Показываем все методы
        cursor.execute("SELECT method_name, description, is_active FROM processing_methods ORDER BY id")
        methods = cursor.fetchall()
        
        print(f"\n📋 Все методы в базе данных:")
        for method in methods:
            status = "✅ Активен" if method[2] else "❌ Неактивен"
            print(f"  - {method[0]}: {method[1]} ({status})")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка при добавлении метода: {e}")
        return False
    finally:
        if conn:
            conn.close()

def test_yandex_gpt_integration():
    """Тестирование интеграции с Yandex GPT"""
    print("\n=== ТЕСТИРОВАНИЕ ИНТЕГРАЦИИ ===")
    
    try:
        from app.core.yandex_gpt_analyzer import YandexGPTAnalyzer
        
        # Создаем анализатор
        analyzer = YandexGPTAnalyzer()
        
        # Тестируем анализ
        test_text = "Отличная больница, очень внимательные врачи!"
        result = analyzer.analyze_sentiment(test_text)
        
        print(f"Тестовый текст: {test_text}")
        print(f"Результат анализа:")
        print(f"  Метод: {result.get('method')}")
        print(f"  Сентимент: {result.get('sentiment')}")
        print(f"  Уверенность: {result.get('confidence')}")
        print(f"  Тип отзыва: {result.get('review_type')}")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка при тестировании: {e}")
        return False

if __name__ == "__main__":
    print("=== ДОБАВЛЕНИЕ МЕТОДА YANDEX GPT ===")
    success = add_yandex_gpt_method()
    
    if success:
        print("\n✅ Метод yandex_gpt готов к использованию!")
    else:
        print("\n❌ Не удалось добавить метод yandex_gpt") 