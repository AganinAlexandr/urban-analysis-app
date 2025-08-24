#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Тест исправленной функции API карты с маппингом фильтров
"""

import sys
import os

# Добавляем путь к модулям приложения
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from core.database_fixed import DatabaseManager

def test_map_api_fixed():
    """Тест исправленной логики API карты"""
    
    print("=== ТЕСТ ИСПРАВЛЕННОЙ ЛОГИКИ API КАРТЫ ===")
    
    # Инициализируем менеджер БД
    db_manager = DatabaseManager()
    
    try:
        # Получаем данные из БД
        df = db_manager.export_to_dataframe(include_analysis=True)
        print(f"1. Данные из БД: {len(df)} записей")
        
        if df.empty:
            print("❌ Данные из БД пусты!")
            return False
        
        # Проверяем наличие координат
        coords_df = df[
            df['latitude'].notna() & df['longitude'].notna() &
            (df['latitude'] != '') & (df['longitude'] != '') &
            (df['latitude'] != 0) & (df['longitude'] != 0)
        ]
        print(f"2. С координатами: {len(coords_df)} записей")
        
        # Удаляем дубликаты по координатам
        coords_df = coords_df.drop_duplicates(subset=['latitude', 'longitude'], keep='first')
        print(f"3. После дедупликации: {len(coords_df)} уникальных объектов")
        
        # Маппинг английских фильтров на русские названия
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
        
        print("\n4. ТЕСТ РЕЖИМА 'ГРУППЫ ОТ ПОСТАВЩИКА':")
        group_field_supplier = 'group_name'
        has_group_supplier = coords_df[group_field_supplier].notna() & (coords_df[group_field_supplier] != '') & (coords_df[group_field_supplier] != 'None')
        with_group_supplier = coords_df[has_group_supplier]
        
        if not with_group_supplier.empty:
            supplier_groups = with_group_supplier[group_field_supplier].value_counts()
            print("Группы от поставщика:")
            for group, count in supplier_groups.items():
                print(f"  {group}: {count} объектов")
        else:
            print("  Нет групп от поставщика")
        
        print(f"\n5. ТЕСТ РЕЖИМА 'ОПРЕДЕЛЯЕМЫЕ':")
        group_field_determined = 'detected_group_type'
        has_group_determined = coords_df[group_field_determined].notna() & (coords_df[group_field_determined] != '') & (coords_df[group_field_determined] != 'None')
        with_group_determined = coords_df[has_group_determined]
        
        if not with_group_determined.empty:
            determined_groups = with_group_determined[group_field_determined].value_counts()
            print("Определяемые группы:")
            for group, count in determined_groups.items():
                print(f"  {group}: {count} объектов")
        else:
            print("  Нет определяемых групп")
        
        print(f"\n6. ТЕСТ ФИЛЬТРАЦИИ С МАППИНГОМ:")
        
        # Тестовые фильтры (английские)
        test_filters = ['school', 'hospital', 'university']
        print(f"Английские фильтры: {test_filters}")
        
        # Маппинг для режима 'determined'
        mapped_filters = [filter_mapping.get(f, f) for f in test_filters]
        print(f"Маппинг на русские: {test_filters} -> {mapped_filters}")
        
        # Проверяем доступные группы в режиме 'determined'
        available_groups = with_group_determined[group_field_determined].unique()
        print(f"Доступные группы в БД: {list(available_groups)}")
        
        # Применяем маппинг и проверяем валидность
        valid_filters = [f for f in mapped_filters if f in available_groups]
        print(f"Валидные фильтры после маппинга: {valid_filters}")
        
        if valid_filters:
            # Фильтруем объекты по валидным фильтрам
            filtered = with_group_determined[with_group_determined[group_field_determined].isin(valid_filters)]
            print(f"Объектов после фильтрации: {len(filtered)}")
            
            # Группируем по группам
            for group, group_data in filtered.groupby(group_field_determined):
                print(f"  {group}: {len(group_data)} объектов")
        else:
            print("❌ Нет валидных фильтров после маппинга!")
            return False
        
        print(f"\n7. СРАВНЕНИЕ РЕЗУЛЬТАТОВ:")
        print(f"Режим 'Группы от поставщика': {len(with_group_supplier)} объектов")
        print(f"Режим 'Определяемые': {len(with_group_determined)} объектов")
        
        # Проверяем, что количество объектов одинаково
        if len(with_group_supplier) == len(with_group_determined):
            print("✅ Количество объектов одинаково в обоих режимах!")
            return True
        else:
            print(f"❌ Количество объектов различается: {len(with_group_supplier)} vs {len(with_group_determined)}")
            return False
            
    except Exception as e:
        print(f"❌ Ошибка при тестировании: {e}")
        return False

if __name__ == "__main__":
    success = test_map_api_fixed()
    if success:
        print("\n✅ Тест API карты прошел успешно!")
    else:
        print("\n❌ Тест API карты не прошел!")

