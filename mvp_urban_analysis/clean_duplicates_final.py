#!/usr/bin/env python3
"""
Очистка дубликатов в базе данных
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.database_fixed import db_manager_fixed
import pandas as pd

def clean_duplicates_final():
    """Очищает дубликаты в базе данных"""
    print("=== ОЧИСТКА ДУБЛИКАТОВ ===")
    
    try:
        # Получаем данные из БД
        df = db_manager_fixed.export_to_dataframe(include_analysis=True)
        print(f"Всего записей до очистки: {len(df)}")
        
        # Проверяем дубликаты
        duplicates = df.duplicated(subset=['name', 'address', 'latitude', 'longitude'], keep=False)
        duplicate_count = duplicates.sum()
        print(f"Найдено дубликатов: {duplicate_count}")
        
        if duplicate_count == 0:
            print("✅ Дубликатов не найдено!")
            return
        
        # Показываем примеры дубликатов
        print(f"\nПримеры дубликатов:")
        duplicate_df = df[duplicates].head(10)
        for i, row in duplicate_df.iterrows():
            print(f"  - {row.get('name', 'N/A')} ({row.get('latitude')}, {row.get('longitude')})")
        
        # Удаляем дубликаты
        print(f"\nУдаляем дубликаты...")
        df_clean = df.drop_duplicates(subset=['name', 'address', 'latitude', 'longitude'], keep='first')
        print(f"Записей после очистки: {len(df_clean)}")
        print(f"Удалено дубликатов: {len(df) - len(df_clean)}")
        
        # Сохраняем очищенные данные обратно в БД
        print(f"\nСохраняем очищенные данные...")
        
        # Очищаем таблицы
        with db_manager_fixed.get_connection() as conn:
            cursor = conn.cursor()
            
            # Очищаем таблицы с данными
            cursor.execute("DELETE FROM objects")
            cursor.execute("DELETE FROM reviews")
            cursor.execute("DELETE FROM analysis_results")
            print("✅ Таблицы очищены")
            
            # Загружаем очищенные данные
            from app.core.data_processor_v2 import DataProcessorV2
            from dotenv import load_dotenv
            import os
            
            load_dotenv('env_data.env')
            geocoder_api_key = os.getenv('YANDEX_GEOCODER_API_KEY')
            data_processor = DataProcessorV2(geocoder_api_key=geocoder_api_key)
            
            # Конвертируем DataFrame в формат для загрузки
            df_to_load = df_clean.copy()
            
            # Обрабатываем данные
            result = data_processor.process_dataframe_to_database(df_to_load, source="cleaned_data")
            
            print(f"✅ Данные загружены обратно в БД")
            print(f"   Создано объектов: {result.get('objects_created', 0)}")
            print(f"   Добавлено отзывов: {result.get('reviews_added', 0)}")
        
        print(f"\n=== ОЧИСТКА ЗАВЕРШЕНА ===")
        
    except Exception as e:
        print(f"❌ Ошибка очистки: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    clean_duplicates_final() 