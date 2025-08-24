#!/usr/bin/env python3
"""
Обновление групп в initial_keywords к единственному числу
"""
import sqlite3

def update_initial_keywords():
    """Обновляет группы в initial_keywords к единственному числу"""
    
    # Маппинг для нормализации к стандарту единственного числа
    group_mapping = {
        'hospitals': 'hospital',
        'schools': 'school', 
        'kindergartens': 'kindergarten',
        'universities': 'university',
        'pharmacies': 'pharmacy',
        'polyclinics': 'polyclinic',
        'shopping_malls': 'shopping_mall'
    }
    
    db_path = 'urban_analysis_fixed.db'
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        print("🔄 Обновление групп в initial_keywords...")
        print("=" * 50)
        
        # Показываем текущее состояние
        cursor.execute("SELECT DISTINCT group_type FROM initial_keywords ORDER BY group_type")
        current_groups = [row[0] for row in cursor.fetchall()]
        print("📋 Текущие группы в initial_keywords:")
        for group in current_groups:
            print(f"   - {group}")
        
        print("\n🔄 Обновление групп...")
        
        # Обновляем каждую группу
        total_updated = 0
        for old_group, new_group in group_mapping.items():
            cursor.execute("""
                UPDATE initial_keywords 
                SET group_type = ? 
                WHERE group_type = ?
            """, (new_group, old_group))
            
            updated_count = cursor.rowcount
            if updated_count > 0:
                print(f"   ✅ {old_group} → {new_group}: {updated_count} записей")
                total_updated += updated_count
        
        conn.commit()
        
        print(f"\n✅ Всего обновлено записей: {total_updated}")
        
        # Показываем финальное состояние
        cursor.execute("SELECT DISTINCT group_type FROM initial_keywords ORDER BY group_type")
        final_groups = [row[0] for row in cursor.fetchall()]
        print("\n📋 Финальные группы в initial_keywords:")
        for group in final_groups:
            print(f"   - {group}")
        
        conn.close()
        print("\n✅ Обновление завершено!")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    update_initial_keywords() 