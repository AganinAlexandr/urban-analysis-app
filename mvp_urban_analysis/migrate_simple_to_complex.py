#!/usr/bin/env python3
"""
Мигратор из простых таблиц в сложную структуру БД
"""

import sqlite3
import pandas as pd
from app.core.database_fixed import db_manager_fixed

class SimpleToComplexMigrator:
    """Мигратор из простых таблиц в сложную структуру"""
    
    def __init__(self, db_path: str = "urban_analysis_fixed.db"):
        self.db_path = db_path
        self.complex_db = db_manager_fixed
    
    def migrate_simple_to_complex(self):
        """Мигрирует данные из простых таблиц в сложную структуру"""
        print("=== МИГРАЦИЯ ИЗ ПРОСТЫХ ТАБЛИЦ В СЛОЖНУЮ СТРУКТУРУ ===")
        
        try:
            # Очищаем сложную БД
            print("1. Очищаем сложную БД...")
            self.complex_db.clear_all_data()
            print("✅ Сложная БД очищена")
            
            # Читаем данные из простых таблиц
            print("2. Читаем данные из простых таблиц...")
            simple_data = self.read_simple_data()
            
            if simple_data.empty:
                print("❌ Нет данных в простых таблицах")
                return False
            
            print(f"✅ Прочитано {len(simple_data)} записей из простых таблиц")
            
            # Мигрируем в сложную структуру
            print("3. Мигрируем в сложную структуру...")
            success = self.migrate_data(simple_data)
            
            if success:
                print("✅ Миграция завершена успешно!")
                
                # Проверяем результат
                self.check_migration_result()
            else:
                print("❌ Ошибка при миграции")
            
            return success
            
        except Exception as e:
            print(f"❌ Ошибка миграции: {e}")
            return False
    
    def read_simple_data(self) -> pd.DataFrame:
        """Читает данные из простых таблиц"""
        query = """
        SELECT 
            o.id as object_id,
            o.name,
            o.address,
            o.latitude,
            o.longitude,
            o.district,
            o.group_type,
            o.rating,
            o.count_rating,
            o.stars,
            o.external_id,
            r.id as review_id,
            r.reviewer_name,
            r.review_text,
            r.review_stars,
            r.review_date,
            r.answer_text
        FROM simple_objects o
        LEFT JOIN simple_reviews r ON o.id = r.object_id
        ORDER BY o.id, r.id
        """
        
        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql_query(query, conn)
        
        return df
    
    def migrate_data(self, df: pd.DataFrame) -> bool:
        """Мигрирует данные в сложную структуру"""
        try:
            # Группируем по объектам
            objects_df = df.groupby('object_id').first().reset_index()
            
            print(f"  Мигрируем {len(objects_df)} объектов...")
            
            # Сначала создаем группы
            print("  Создаем группы...")
            group_types = objects_df['group_type'].unique()
            for group_type in group_types:
                if group_type:
                    # Создаем группу в object_groups
                    group_id = self.complex_db.get_group_id(group_type)
                    if not group_id:
                        # Если группа не существует, создаем её
                        with sqlite3.connect(self.db_path) as conn:
                            cursor = conn.execute("""
                                INSERT INTO object_groups (group_name, group_type)
                                VALUES (?, ?)
                            """, (group_type, group_type))
                            group_id = cursor.lastrowid
                            print(f"    Создана группа: {group_type} (ID: {group_id})")
                    
                    # Создаем группу в detected_groups
                    detected_group_id = self.complex_db.get_detected_group_id(group_type)
                    if not detected_group_id:
                        # Если группа не существует, создаем её
                        with sqlite3.connect(self.db_path) as conn:
                            cursor = conn.execute("""
                                INSERT INTO detected_groups (group_name, group_type, detection_method, confidence)
                                VALUES (?, ?, ?, ?)
                            """, (group_type, group_type, 'simple_migration', 1.0))
                            detected_group_id = cursor.lastrowid
                            print(f"    Создана detected группа: {group_type} (ID: {detected_group_id})")
            
            # Мигрируем каждый объект
            for _, obj_row in objects_df.iterrows():
                # Получаем ID групп
                group_id = self.complex_db.get_group_id(obj_row['group_type'])
                detected_group_id = self.complex_db.get_detected_group_id(obj_row['group_type'])
                
                # Создаем объект в сложной БД
                object_id = self.complex_db.insert_object(
                    name=obj_row['name'],
                    address=obj_row['address'],
                    latitude=obj_row['latitude'],
                    longitude=obj_row['longitude'],
                    district=obj_row['district'],
                    group_type=obj_row['group_type'],
                    detected_group_type=obj_row['group_type']
                )
                
                print(f"    Создан объект: {obj_row['name']} (ID: {object_id})")
                
                # Получаем отзывы для этого объекта
                object_reviews = df[df['object_id'] == obj_row['object_id']]
                
                # Мигрируем отзывы
                for _, review_row in object_reviews.iterrows():
                    if pd.notna(review_row['review_id']):  # Если есть отзыв
                        review_id = self.complex_db.insert_review(
                            object_id=object_id,
                            review_text=review_row['review_text'],
                            rating=review_row['review_stars'],
                            review_date=review_row['review_date'],
                            source='simple_migration',
                            external_id=str(review_row['review_id'])
                        )
                        
                        print(f"      Создан отзыв: {review_row['reviewer_name']} (ID: {review_id})")
            
            return True
            
        except Exception as e:
            print(f"❌ Ошибка миграции данных: {e}")
            return False
    
    def check_migration_result(self):
        """Проверяет результат миграции"""
        print("\n4. Проверяем результат миграции...")
        
        try:
            # Получаем данные из сложной БД
            df = self.complex_db.export_to_dataframe(include_analysis=True)
            print(f"✅ Записей в сложной БД: {len(df)}")
            
            # Проверяем группы
            if 'group_type' in df.columns:
                groups = df['group_type'].value_counts()
                print(f"Группы: {groups.to_dict()}")
            
            # Проверяем объекты с координатами
            coords_df = df[df['latitude'].notna() & df['longitude'].notna()]
            print(f"Объектов с координатами: {len(coords_df)}")
            
            if not coords_df.empty:
                print("Объекты с координатами:")
                for _, row in coords_df.groupby('object_id').first().iterrows():
                    print(f"  {row['name']} - {row.get('group_type', 'N/A')}")
            
        except Exception as e:
            print(f"❌ Ошибка проверки результата: {e}")

def main():
    """Основная функция"""
    migrator = SimpleToComplexMigrator()
    
    # Выполняем миграцию
    success = migrator.migrate_simple_to_complex()
    
    if success:
        print("\n🎉 Миграция завершена успешно!")
        print("Теперь можно запускать основное приложение с данными в сложной структуре")
    else:
        print("\n❌ Миграция не удалась")

if __name__ == "__main__":
    main() 