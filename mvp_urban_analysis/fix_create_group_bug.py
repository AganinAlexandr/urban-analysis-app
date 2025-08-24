#!/usr/bin/env python3
"""
Исправление критической ошибки в create_group
"""

def fix_create_group_calls():
    """Исправляем вызовы create_group в database_fixed.py"""
    print("🐛 ИСПРАВЛЕНИЕ КРИТИЧЕСКОЙ ОШИБКИ")
    print("=" * 50)
    
    # Маппинг group_type -> правильное название
    group_names = {
        'hospital': 'Больница',
        'school': 'Школа', 
        'kindergarden': 'Детский сад',
        'polyclinic': 'Поликлиника',
        'pharmacy': 'Аптека',
        'university': 'Университет',
        'shopmall': 'Торговый центр',
        'resident_complex': 'Жилой комплекс'
    }
    
    print("Проблема:")
    print("  create_group(group_type, group_type)  ❌")
    print("  Должно быть:")
    print("  create_group(правильное_название, group_type)  ✅")
    
    print(f"\nПравильные названия:")
    for group_type, group_name in group_names.items():
        print(f"  {group_type} -> '{group_name}'")
    
    return group_names

if __name__ == "__main__":
    group_names = fix_create_group_calls()
    
    print("\n🔧 Нужно исправить функцию insert_object в database_fixed.py:")
    print("   Строка 78: group_id = self.create_group(group_type, group_type)")
    print("   Строка 86: detected_group_id = self.create_detected_group(group_type, group_type)")
    print("\n💡 Решение: создать функцию get_group_display_name(group_type)")