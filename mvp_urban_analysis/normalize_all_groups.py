#!/usr/bin/env python3
"""
Нормализация всех групп к единому стандарту (множественное число)
"""
import sqlite3

def normalize_groups():
    """Нормализует все группы к единому стандарту"""
    
    # Маппинг для нормализации к стандарту множественного числа
    group_mapping = {
        'hospital': 'hospitals',
        'school': 'schools', 
        'kindergarden': 'kindergartens',
        'university': 'universities'
    }
    
    db_path = 'urban_analysis_fixed.db'
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        print("🔄 Нормализация групп в базе данных...")
        print("=" * 50)
        
        # Показываем текущее состояние
        cursor.execute("SELECT DISTINCT group_type FROM object_groups ORDER BY group_type")
        current_groups = [row[0] for row in cursor.fetchall()]
        print("📋 Текущие группы в object_groups:")
        for group in current_groups:
            print(f"   - {group}")
        
        print("\n🔄 Обновление групп...")
        
        # Обновляем каждую группу
        total_updated = 0
        for old_group, new_group in group_mapping.items():
            cursor.execute("""
                UPDATE object_groups 
                SET group_type = ?, group_name = ? 
                WHERE group_type = ?
            """, (new_group, new_group, old_group))
            
            updated_count = cursor.rowcount
            if updated_count > 0:
                print(f"   ✅ {old_group} → {new_group}: {updated_count} записей")
                total_updated += updated_count
        
        # Обновляем также в таблице detected_groups
        print("\n🔄 Обновление групп в detected_groups...")
        for old_group, new_group in group_mapping.items():
            cursor.execute("""
                UPDATE detected_groups 
                SET group_type = ?, group_name = ? 
                WHERE group_type = ?
            """, (new_group, new_group, old_group))
            
            updated_count = cursor.rowcount
            if updated_count > 0:
                print(f"   ✅ {old_group} → {new_group}: {updated_count} записей")
                total_updated += updated_count
        
        conn.commit()
        
        print(f"\n✅ Всего обновлено записей: {total_updated}")
        
        # Показываем финальное состояние
        cursor.execute("SELECT DISTINCT group_type FROM object_groups ORDER BY group_type")
        final_groups = [row[0] for row in cursor.fetchall()]
        print("\n📋 Финальные группы в object_groups:")
        for group in final_groups:
            print(f"   - {group}")
        
        conn.close()
        print("\n✅ Нормализация завершена!")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    normalize_groups() 