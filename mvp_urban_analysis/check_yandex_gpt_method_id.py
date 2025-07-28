"""
Проверка ID метода yandex_gpt в базе данных
"""

import sqlite3
import os

def check_yandex_gpt_method_id():
    """Проверяет ID метода yandex_gpt в базе данных"""
    print("=== ПРОВЕРКА ID МЕТОДА YANDEX GPT ===")
    
    db_path = 'urban_analysis_fixed.db'
    
    if not os.path.exists(db_path):
        print(f"❌ База данных {db_path} не найдена")
        return
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Ищем метод yandex_gpt
        cursor.execute("""
            SELECT id, method_name, description, is_active 
            FROM processing_methods 
            WHERE method_name = 'yandex_gpt'
        """)
        
        method = cursor.fetchone()
        
        if method:
            print(f"✅ Метод yandex_gpt найден:")
            print(f"  ID: {method[0]}")
            print(f"  Название: {method[1]}")
            print(f"  Описание: {method[2]}")
            print(f"  Активен: {'Да' if method[3] else 'Нет'}")
        else:
            print("❌ Метод yandex_gpt не найден в базе данных")
        
        # Показываем все методы для сравнения
        cursor.execute("""
            SELECT id, method_name, description, is_active 
            FROM processing_methods 
            ORDER BY id
        """)
        
        methods = cursor.fetchall()
        print(f"\n📋 Все методы в базе данных:")
        for method in methods:
            status = "✅ Активен" if method[3] else "❌ Неактивен"
            print(f"  {method[0]}. {method[1]}: {method[2]} ({status})")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка при проверке: {e}")

if __name__ == "__main__":
    check_yandex_gpt_method_id() 