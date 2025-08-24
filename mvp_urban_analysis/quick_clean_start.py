#!/usr/bin/env python3
"""
Быстрая очистка базы данных - оставляем только структуру и словари
"""
import sqlite3
import os

def quick_clean():
    """Быстро очищает базу данных, оставляя только структуру"""
    print("=== БЫСТРАЯ ОЧИСТКА БАЗЫ ДАННЫХ ===")
    
    db_path = 'urban_analysis_fixed.db'
    
    if not os.path.exists(db_path):
        print(f"❌ База данных {db_path} не найдена")
        return
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        print("1. Очистка данных...")
        
        # Очищаем все таблицы с данными, оставляем только структуру
        tables_to_clean = [
            'objects',
            'reviews', 
            'analysis_results',
            'object_groups',
            'detected_groups',
            'master_ratings'
        ]
        
        for table in tables_to_clean:
            try:
                cursor.execute(f"DELETE FROM {table}")
                print(f"   ✅ Очищена таблица {table}")
            except Exception as e:
                print(f"   ⚠️  Таблица {table}: {e}")
        
        # Сбрасываем автоинкремент
        cursor.execute("DELETE FROM sqlite_sequence")
        print("   ✅ Сброшен автоинкремент")
        
        # Подтверждаем изменения
        conn.commit()
        
        print("\n2. Проверка результата...")
        
        # Проверяем количество записей
        for table in tables_to_clean:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"   {table}: {count} записей")
            except:
                print(f"   {table}: таблица не существует")
        
        conn.close()
        
        print("\n✅ База данных очищена!")
        print("   Теперь можно загружать новые данные")
        
    except Exception as e:
        print(f"❌ Ошибка при очистке: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    quick_clean()

