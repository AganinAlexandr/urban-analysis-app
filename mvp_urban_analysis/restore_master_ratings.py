#!/usr/bin/env python3
"""
Восстановление данных Master_Rating из резервных копий
"""
import sqlite3
import os
import shutil

def restore_master_ratings():
    """Восстанавливает данные Master_Rating из резервных копий"""
    print("=== ВОССТАНОВЛЕНИЕ ДАННЫХ MASTER_RATING ===")
    
    try:
        # Проверяем наличие резервных копий
        backup_files = []
        for file in os.listdir('.'):
            if file.startswith('urban_analysis_fixed.db.backup') and 'master' in file.lower():
                backup_files.append(file)
        
        print(f"1. Найдены резервные копии: {backup_files}")
        
        if not backup_files:
            print("   Резервные копии с Master_Rating не найдены")
            return
        
        # Проверяем данные Master_Rating в каждой резервной копии
        for backup_file in backup_files:
            print(f"\n2. Проверка {backup_file}:")
            
            # Создаем временную копию для проверки
            temp_db = f"temp_{backup_file}"
            shutil.copy(backup_file, temp_db)
            
            try:
                conn = sqlite3.connect(temp_db)
                cursor = conn.cursor()
                
                # Проверяем данные в master_ratings
                cursor.execute("SELECT COUNT(*) FROM master_ratings")
                master_ratings_count = cursor.fetchone()[0]
                print(f"   Записей в master_ratings: {master_ratings_count}")
                
                if master_ratings_count > 0:
                    print(f"   Найдены данные Master_Rating в {backup_file}")
                    
                    # Получаем примеры данных
                    cursor.execute("SELECT * FROM master_ratings LIMIT 3")
                    examples = cursor.fetchall()
                    print("   Примеры данных:")
                    for i, example in enumerate(examples, 1):
                        print(f"     {i}: {example}")
                    
                    # Восстанавливаем данные
                    print(f"\n3. Восстановление данных из {backup_file}...")
                    
                    # Копируем данные из резервной копии в основную БД
                    cursor.execute("SELECT * FROM master_ratings")
                    master_ratings_data = cursor.fetchall()
                    
                    conn.close()
                    
                    # Подключаемся к основной БД и вставляем данные
                    main_conn = sqlite3.connect('urban_analysis_fixed.db')
                    main_cursor = main_conn.cursor()
                    
                    # Очищаем существующие данные
                    main_cursor.execute("DELETE FROM master_ratings")
                    
                    # Вставляем данные
                    for record in master_ratings_data:
                        main_cursor.execute("""
                            INSERT INTO master_ratings (id, review_id, sentiment, rated_by, rated_at)
                            VALUES (?, ?, ?, ?, ?)
                        """, record)
                    
                    main_conn.commit()
                    main_conn.close()
                    
                    print(f"   Восстановлено {len(master_ratings_data)} записей Master_Rating")
                    break
                
                conn.close()
                
            except Exception as e:
                print(f"   Ошибка при проверке {backup_file}: {e}")
            finally:
                # Удаляем временную копию
                if os.path.exists(temp_db):
                    os.remove(temp_db)
        
        # Проверяем результат
        print("\n4. Проверка восстановленных данных:")
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM master_ratings")
        restored_count = cursor.fetchone()[0]
        print(f"   Записей в master_ratings после восстановления: {restored_count}")
        
        if restored_count > 0:
            cursor.execute("SELECT * FROM master_ratings LIMIT 3")
            restored_examples = cursor.fetchall()
            print("   Примеры восстановленных данных:")
            for i, example in enumerate(restored_examples, 1):
                print(f"     {i}: {example}")
        
        conn.close()
        
        print("\n✅ Восстановление завершено!")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    restore_master_ratings() 