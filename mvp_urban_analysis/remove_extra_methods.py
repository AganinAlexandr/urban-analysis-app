#!/usr/bin/env python3
"""
Удаление лишних методов обработки отзывов
"""
import sqlite3
import os

def remove_extra_methods():
    """Удаляет лишние методы обработки отзывов с ID 533-536"""
    print("=== УДАЛЕНИЕ ЛИШНИХ МЕТОДОВ ОБРАБОТКИ ===")
    
    db_path = 'urban_analysis_fixed.db'
    
    if not os.path.exists(db_path):
        print(f"❌ База данных {db_path} не найдена")
        return
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Проверяем текущие методы
        print("1. Текущие методы в базе данных:")
        cursor.execute("""
            SELECT id, method_name, description, is_active 
            FROM processing_methods 
            ORDER BY id
        """)
        
        methods = cursor.fetchall()
        for method in methods:
            status = "✅ Активен" if method[3] else "❌ Неактивен"
            print(f"   {method[0]}. {method[1]}: {method[2]} ({status})")
        
        # Удаляем лишние методы
        print("\n2. Удаление методов с ID 533-536...")
        
        extra_methods = [533, 534, 535, 536]
        
        for method_id in extra_methods:
            # Проверяем, есть ли результаты анализа для этого метода
            cursor.execute("""
                SELECT COUNT(*) FROM analysis_results WHERE method_id = ?
            """, (method_id,))
            
            results_count = cursor.fetchone()[0]
            
            if results_count > 0:
                print(f"   ⚠️  Удаляем {results_count} результатов анализа для метода {method_id}")
                cursor.execute("DELETE FROM analysis_results WHERE method_id = ?", (method_id,))
            
            # Удаляем сам метод
            cursor.execute("DELETE FROM processing_methods WHERE id = ?", (method_id,))
            print(f"   ✅ Удален метод с ID {method_id}")
        
        # Подтверждаем изменения
        conn.commit()
        
        # Проверяем результат
        print("\n3. Методы после удаления:")
        cursor.execute("""
            SELECT id, method_name, description, is_active 
            FROM processing_methods 
            ORDER BY id
        """)
        
        methods_after = cursor.fetchall()
        for method in methods_after:
            status = "✅ Активен" if method[3] else "❌ Неактивен"
            print(f"   {method[0]}. {method[1]}: {method[2]} ({status})")
        
        print(f"\n✅ Удалено {len(extra_methods)} лишних методов")
        print("✅ База данных обновлена")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка при удалении методов: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    remove_extra_methods() 