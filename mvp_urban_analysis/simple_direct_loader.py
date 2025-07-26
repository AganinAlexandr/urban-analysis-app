#!/usr/bin/env python3
"""
Простой прямой загрузчик данных
"""

import json
import pandas as pd
import os
import sqlite3
from typing import List, Dict, Any

class SimpleDirectLoader:
    """Простой прямой загрузчик данных"""
    
    def __init__(self, db_path: str = "urban_analysis_fixed.db"):
        self.db_path = db_path
    
    def init_simple_database(self):
        """Создает простую структуру БД"""
        with sqlite3.connect(self.db_path) as conn:
            # Создаем простую таблицу объектов
            conn.execute("""
                CREATE TABLE IF NOT EXISTS simple_objects (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    address TEXT,
                    latitude REAL,
                    longitude REAL,
                    district TEXT,
                    group_type TEXT,
                    rating REAL,
                    count_rating INTEGER,
                    stars INTEGER,
                    external_id TEXT
                )
            """)
            
            # Создаем простую таблицу отзывов
            conn.execute("""
                CREATE TABLE IF NOT EXISTS simple_reviews (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    object_id INTEGER,
                    reviewer_name TEXT,
                    review_text TEXT,
                    review_stars INTEGER,
                    review_date REAL,
                    answer_text TEXT,
                    FOREIGN KEY (object_id) REFERENCES simple_objects (id)
                )
            """)
            
            conn.commit()
            print("✅ Простая структура БД создана")
    
    def clear_simple_database(self):
        """Очищает простую БД"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM simple_reviews")
            conn.execute("DELETE FROM simple_objects")
            conn.execute("DELETE FROM sqlite_sequence WHERE name IN ('simple_objects', 'simple_reviews')")
            conn.commit()
            print("✅ Простая БД очищена")
    
    def load_json_file(self, file_path: str, group_name: str, max_reviews: int = 3) -> tuple:
        """
        Загружает один JSON файл
        
        Args:
            file_path: Путь к JSON файлу
            group_name: Название группы
            max_reviews: Максимальное количество отзывов
            
        Returns:
            Кортеж (данные объекта, список отзывов)
        """
        print(f"Загружаем файл: {file_path}")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Извлекаем информацию о компании
            company_info = data.get('company_info', {})
            reviews = data.get('company_reviews', [])
            
            # Ограничиваем количество отзывов
            if len(reviews) > max_reviews:
                reviews = reviews[:max_reviews]
                print(f"  Ограничили отзывы до {max_reviews}")
            
            # Данные объекта
            object_data = {
                'name': company_info.get('name', ''),
                'address': company_info.get('address', ''),
                'latitude': 55.7558,  # Тестовые координаты
                'longitude': 37.6176,
                'district': 'Центральный район',
                'group_type': group_name,
                'rating': company_info.get('rating', 0),
                'count_rating': company_info.get('count_rating', 0),
                'stars': company_info.get('stars', 0),
                'external_id': company_info.get('id', '')
            }
            
            # Данные отзывов
            reviews_data = []
            for review in reviews:
                review_data = {
                    'reviewer_name': review.get('name', ''),
                    'review_text': review.get('text', ''),
                    'review_stars': review.get('stars', 0),
                    'review_date': review.get('date', 0),
                    'answer_text': review.get('answer', '')
                }
                reviews_data.append(review_data)
            
            print(f"  Объект: {object_data['name']}")
            print(f"  Отзывов: {len(reviews_data)}")
            
            return object_data, reviews_data
            
        except Exception as e:
            print(f"❌ Ошибка загрузки файла {file_path}: {e}")
            return None, []
    
    def save_to_simple_database(self, object_data: dict, reviews_data: list) -> bool:
        """
        Сохраняет данные в простую БД
        
        Args:
            object_data: Данные объекта
            reviews_data: Список отзывов
            
        Returns:
            True если успешно
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Сохраняем объект
                cursor = conn.execute("""
                    INSERT INTO simple_objects 
                    (name, address, latitude, longitude, district, group_type, rating, count_rating, stars, external_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    object_data['name'],
                    object_data['address'],
                    object_data['latitude'],
                    object_data['longitude'],
                    object_data['district'],
                    object_data['group_type'],
                    object_data['rating'],
                    object_data['count_rating'],
                    object_data['stars'],
                    object_data['external_id']
                ))
                
                object_id = cursor.lastrowid
                print(f"  Сохранен объект: {object_data['name']} (ID: {object_id})")
                
                # Сохраняем отзывы
                for review in reviews_data:
                    cursor = conn.execute("""
                        INSERT INTO simple_reviews 
                        (object_id, reviewer_name, review_text, review_stars, review_date, answer_text)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (
                        object_id,
                        review['reviewer_name'],
                        review['review_text'],
                        review['review_stars'],
                        review['review_date'],
                        review['answer_text']
                    ))
                    print(f"    Сохранен отзыв: {review['reviewer_name']}")
                
                conn.commit()
                return True
                
        except Exception as e:
            print(f"❌ Ошибка сохранения в БД: {e}")
            return False
    
    def load_folder(self, folder_path: str) -> bool:
        """
        Загружает все JSON файлы из папки
        
        Args:
            folder_path: Путь к папке
            
        Returns:
            True если успешно
        """
        print(f"=== ЗАГРУЗКА ДАННЫХ ИЗ: {folder_path} ===")
        
        # Определяем группу из названия папки
        group_name = os.path.basename(folder_path).replace('_parse', '')
        print(f"Группа: {group_name}")
        
        # Ищем все JSON файлы
        json_files = []
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                if file.endswith('.json'):
                    json_files.append(os.path.join(root, file))
        
        print(f"Найдено JSON файлов: {len(json_files)}")
        
        # Ограничиваем количество файлов для теста
        if len(json_files) > 3:
            json_files = json_files[:3]
            print(f"Ограничили до 3 файлов для тестирования")
        
        # Загружаем каждый файл
        success_count = 0
        for file_path in json_files:
            object_data, reviews_data = self.load_json_file(file_path, group_name)
            
            if object_data and reviews_data:
                if self.save_to_simple_database(object_data, reviews_data):
                    success_count += 1
        
        print(f"✅ Успешно загружено файлов: {success_count}")
        return success_count > 0
    
    def export_simple_data(self) -> pd.DataFrame:
        """Экспортирует данные из простой БД"""
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

def main():
    """Основная функция"""
    loader = SimpleDirectLoader()
    
    # Инициализируем простую БД
    loader.init_simple_database()
    
    # Очищаем БД
    loader.clear_simple_database()
    
    # Загружаем данные
    test_folder = "data/initial_data/json/schools_parse"
    
    if os.path.exists(test_folder):
        success = loader.load_folder(test_folder)
        if success:
            print("\n✅ Загрузка завершена успешно!")
            
            # Проверяем результат
            df = loader.export_simple_data()
            print(f"\nЗаписей в БД: {len(df)}")
            
            if 'group_type' in df.columns:
                groups = df['group_type'].value_counts()
                print(f"Группы: {groups.to_dict()}")
            
            # Показываем объекты с координатами
            coords_df = df[df['latitude'].notna() & df['longitude'].notna()]
            print(f"Объектов с координатами: {len(coords_df)}")
            
            if not coords_df.empty:
                print("Объекты с координатами:")
                for _, row in coords_df.groupby('object_id').first().iterrows():
                    print(f"  {row['name']} - {row['group_type']}")
        else:
            print("\n❌ Ошибка при загрузке данных")
    else:
        print(f"❌ Папка не найдена: {test_folder}")

if __name__ == "__main__":
    main() 