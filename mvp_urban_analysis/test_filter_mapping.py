#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Тест маппинга английских фильтров на русские названия
"""

def test_filter_mapping():
    """Тест маппинга фильтров"""
    
    # Маппинг английских фильтров на русские названия для режима 'determined'
    filter_mapping = {
        'school': 'Школа',
        'hospital': 'Больница',
        'university': 'Университет',
        'pharmacy': 'Аптека',
        'kindergarden': 'Детский сад',
        'polyclinic': 'Поликлиника',
        'shopmall': 'Торговый центр',
        'resident_complex': 'Жилой комплекс'
    }
    
    print("=== ТЕСТ МАППИНГА ФИЛЬТРОВ ===")
    
    # Тестовые фильтры
    test_filters = ['school', 'hospital', 'university', 'pharmacy', 'kindergarden', 'polyclinic', 'shopmall', 'resident_complex']
    
    print("1. Английские фильтры:")
    for f in test_filters:
        print(f"  {f}")
    
    print("\n2. Маппинг на русские названия:")
    for f in test_filters:
        russian = filter_mapping.get(f, f)
        print(f"  {f} -> {russian}")
    
    print("\n3. Симуляция фильтрации:")
    
    # Симулируем данные из БД
    available_groups = ['Школа', 'Больница', 'Университет', 'Аптека', 'Детский сад', 'Поликлиника', 'Торговый центр', 'Жилой комплекс']
    
    print(f"Доступные группы в БД: {available_groups}")
    
    # Применяем маппинг
    mapped_filters = [filter_mapping.get(f, f) for f in test_filters]
    print(f"Маппинг фильтров: {test_filters} -> {mapped_filters}")
    
    # Проверяем валидность
    valid_filters = [f for f in mapped_filters if f in available_groups]
    print(f"Валидные фильтры после маппинга: {valid_filters}")
    
    # Проверяем, что все фильтры валидны
    all_valid = len(valid_filters) == len(test_filters)
    print(f"Все фильтры валидны: {'✅' if all_valid else '❌'}")
    
    if not all_valid:
        invalid_filters = [f for f in mapped_filters if f not in available_groups]
        print(f"Невалидные фильтры: {invalid_filters}")
    
    return all_valid

if __name__ == "__main__":
    success = test_filter_mapping()
    if success:
        print("\n✅ Тест маппинга прошел успешно!")
    else:
        print("\n❌ Тест маппинга не прошел!")
