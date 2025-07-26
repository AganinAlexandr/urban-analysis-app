#!/usr/bin/env python3
"""
Отладка функции get_map_data
"""
import sys
import os
import pandas as pd

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def debug_map_data():
    """Отлаживает функцию get_map_data"""
    print("=== ОТЛАДКА GET_MAP_DATA ===")
    
    try:
        # Импортируем модули
        from app.core.database_fixed import db_manager_fixed
        
        print("1. Проверяем db_manager_fixed...")
        print(f"  db_manager_fixed: {db_manager_fixed}")
        
        print("\n2. Вызываем export_to_dataframe...")
        df = db_manager_fixed.export_to_dataframe(include_analysis=True)
        
        print(f"  DataFrame получен: {df is not None}")
        if df is not None:
            print(f"  Размер DataFrame: {df.shape}")
            print(f"  Колонки: {list(df.columns)}")
            
            if not df.empty:
                print(f"  Первые 3 строки:")
                print(df.head(3))
                
                # Проверяем координаты
                print(f"\n3. Проверяем координаты...")
                coords_df = df[
                    df['latitude'].notna() & df['longitude'].notna() &
                    (df['latitude'] != '') & (df['longitude'] != '') &
                    (df['latitude'] != 0) & (df['longitude'] != 0)
                ]
                print(f"  Объектов с координатами: {len(coords_df)}")
                
                if len(coords_df) > 0:
                    print(f"  Примеры координат:")
                    for i, (_, row) in enumerate(coords_df.head(3).iterrows()):
                        print(f"    {i+1}. {row.get('name', 'N/A')}: {row.get('latitude')}, {row.get('longitude')}")
            else:
                print("  DataFrame пуст!")
        else:
            print("  export_to_dataframe вернул None!")
            
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_map_data() 