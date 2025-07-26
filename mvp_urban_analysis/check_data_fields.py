#!/usr/bin/env python3
"""
Проверка полей в данных
"""

from app.core.database_fixed import db_manager_fixed
import pandas as pd

def check_data_fields():
    """Проверка полей в данных"""
    print("=== ПРОВЕРКА ПОЛЕЙ В ДАННЫХ ===")
    
    try:
        # Получаем данные из БД
        df = db_manager_fixed.export_to_dataframe(include_analysis=True)
        print(f"Записей в БД: {len(df)}")
        
        # Показываем все поля
        print(f"\nВсе поля в данных:")
        for i, field in enumerate(df.columns):
            print(f"  {i+1}. {field}")
        
        # Показываем первые несколько записей
        print(f"\nПервые 3 записи:")
        for i, row in df.head(3).iterrows():
            print(f"\nЗапись {i}:")
            for field in df.columns:
                value = row[field]
                if pd.notna(value):
                    print(f"  {field}: {value}")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    check_data_fields() 