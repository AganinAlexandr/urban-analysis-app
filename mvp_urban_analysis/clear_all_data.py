#!/usr/bin/env python3
"""
Полная очистка всех данных, оставляем только справочники
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.database_fixed import db_manager_fixed
import sqlite3

def clear_all_data():
    """Очищаем все данные, оставляем только справочники"""
    print("=== ПОЛНАЯ ОЧИСТКА ДАННЫХ ===")
    
    try:
        with db_manager_fixed.get_connection() as conn:
            # 1. Показываем текущее состояние
            cursor = conn.execute("SELECT COUNT(*) as count FROM objects")
            objects_count = cursor.fetchone()['count']
            
            cursor = conn.execute("SELECT COUNT(*) as count FROM reviews")
            reviews_count = cursor.fetchone()['count']
            
            cursor = conn.execute("SELECT COUNT(*) as count FROM analysis_results")
            analysis_count = cursor.fetchone()['count']
            
            cursor = conn.execute("SELECT COUNT(*) as count FROM object_groups")
            groups_count = cursor.fetchone()['count']
            
            cursor = conn.execute("SELECT COUNT(*) as count FROM detected_groups")
            detected_count = cursor.fetchone()['count']
            
            print(f"Текущее состояние БД:")
            print(f"  Объекты: {objects_count}")
            print(f"  Отзывы: {reviews_count}")
            print(f"  Результаты анализа: {analysis_count}")
            print(f"  Группы объектов: {groups_count}")
            print(f"  Определяемые группы: {detected_count}")
            
            # 2. Удаляем все данные (но НЕ справочники)
            print(f"\n=== УДАЛЕНИЕ ДАННЫХ ===")
            
            # Удаляем результаты анализа
            cursor = conn.execute("DELETE FROM analysis_results")
            print(f"✅ Удалено {cursor.rowcount} результатов анализа")
            
            # Удаляем отзывы
            cursor = conn.execute("DELETE FROM reviews")
            print(f"✅ Удалено {cursor.rowcount} отзывов")
            
            # Удаляем объекты
            cursor = conn.execute("DELETE FROM objects")
            print(f"✅ Удалено {cursor.rowcount} объектов")
            
            # 3. Очищаем справочники групп и создаем правильные
            print(f"\n=== ОЧИСТКА И СОЗДАНИЕ СПРАВОЧНИКОВ ===")
            
            # Удаляем все группы
            cursor = conn.execute("DELETE FROM object_groups")
            print(f"✅ Удалено {cursor.rowcount} групп объектов")
            
            cursor = conn.execute("DELETE FROM detected_groups")
            print(f"✅ Удалено {cursor.rowcount} определяемых групп")
            
            # Создаем правильные группы (только 8!)
            correct_groups = [
                ('hospital', 'Больница'),
                ('school', 'Школа'),
                ('kindergarden', 'Детский сад'),
                ('polyclinic', 'Поликлиника'),
                ('pharmacy', 'Аптека'),
                ('university', 'Университет'),
                ('shopmall', 'Торговый центр'),
                ('resident_complex', 'Жилой комплекс')
            ]
            
            print(f"\nСоздаем правильные группы:")
            for group_type, group_name in correct_groups:
                # Создаем в object_groups
                cursor = conn.execute("""
                    INSERT INTO object_groups (group_name, group_type, description)
                    VALUES (?, ?, ?)
                """, (group_name, group_type, f"{group_name}"))
                print(f"  ✅ {group_type} - {group_name}")
                
                # Создаем в detected_groups
                cursor = conn.execute("""
                    INSERT INTO detected_groups (group_name, group_type, detection_method, confidence)
                    VALUES (?, ?, ?, ?)
                """, (group_type, group_type, 'auto', 1.0))
            
            # 4. Сбрасываем счетчики автоинкремента
            print(f"\n=== СБРОС СЧЕТЧИКОВ ===")
            tables_to_reset = ['objects', 'reviews', 'analysis_results']
            for table in tables_to_reset:
                cursor = conn.execute(f"DELETE FROM sqlite_sequence WHERE name='{table}'")
                print(f"✅ Сброшен счетчик для таблицы {table}")
            
            # 5. Показываем финальное состояние
            print(f"\n=== ФИНАЛЬНОЕ СОСТОЯНИЕ ===")
            
            cursor = conn.execute("SELECT COUNT(*) as count FROM objects")
            final_objects = cursor.fetchone()['count']
            
            cursor = conn.execute("SELECT COUNT(*) as count FROM reviews")
            final_reviews = cursor.fetchone()['count']
            
            cursor = conn.execute("SELECT COUNT(*) as count FROM object_groups")
            final_groups = cursor.fetchone()['count']
            
            cursor = conn.execute("SELECT COUNT(*) as count FROM detected_groups")
            final_detected = cursor.fetchone()['count']
            
            print(f"Финальное состояние БД:")
            print(f"  Объекты: {final_objects}")
            print(f"  Отзывы: {final_reviews}")
            print(f"  Группы объектов: {final_groups}")
            print(f"  Определяемые группы: {final_detected}")
            
            # Показываем созданные группы
            print(f"\nСозданные группы:")
            cursor = conn.execute("SELECT group_type, group_name FROM object_groups ORDER BY group_type")
            groups = cursor.fetchall()
            for group in groups:
                print(f"  {group['group_type']} - {group['group_name']}")
            
            print(f"\n🎉 БАЗА ДАННЫХ ОЧИЩЕНА И ГОТОВА К ЗАГРУЗКЕ НОВЫХ ДАННЫХ!")
            print(f"📝 Теперь можно загружать файлы без групп - модальное окно будет работать корректно")
            
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    clear_all_data()