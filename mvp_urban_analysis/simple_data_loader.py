#!/usr/bin/env python3
"""
Простая система загрузки данных
"""

import json
import pandas as pd
import os
from typing import List, Dict, Any
from app.core.database_fixed import db_manager_fixed

class SimpleDataLoader:
    """Простой загрузчик данных"""
    
    def __init__(self):
        self.db = db_manager_fixed
    
    def load_json_file(self, file_path: str, group_name: str, max_reviews: int = 5) -> pd.DataFrame:
        """
        Загружает один JSON файл и преобразует в DataFrame
        
        Args:
            file_path: Путь к JSON файлу
            group_name: Название группы (schools, hospitals, etc.)
            max_reviews: Максимальное количество отзывов для загрузки
            
        Returns:
            DataFrame с данными
        """
        print(f"Загружаем файл: {file_path}")
        
        if not group_name:
            raise ValueError("Группа (group_name) должна быть явно указана!")

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Извлекаем информацию о компании
            company_info = data.get('company_info', {})
            reviews = data.get('company_reviews', [])
            
            # Ограничиваем количество отзывов
            if len(reviews) > max_reviews:
                reviews = reviews[:max_reviews]
                print(f"  Ограничили отзывы до {max_reviews} для тестирования")
            
            # Создаем основную запись об объекте
            object_data = {
                'name': company_info.get('name', ''),
                'rating': company_info.get('rating', 0),
                'count_rating': company_info.get('count_rating', 0),
                'stars': company_info.get('stars', 0),
                'id': company_info.get('id', ''),
                'address': company_info.get('address', ''),
                'group': group_name,  # Группа из названия папки
                'latitude': 55.7558,  # Тестовые координаты для Москвы
                'longitude': 37.6176,
                'district': 'Центральный район'
            }
            
            # Создаем записи для отзывов
            reviews_data = []
            for review in reviews:
                review_data = {
                    'object_id': company_info.get('id', ''),
                    'reviewer_name': review.get('name', ''),
                    'review_text': review.get('text', ''),
                    'review_stars': review.get('stars', 0),
                    'review_date': review.get('date', 0),
                    'answer_text': review.get('answer', '')
                }
                reviews_data.append(review_data)
            
            # Создаем DataFrame
            objects_df = pd.DataFrame([object_data])
            reviews_df = pd.DataFrame(reviews_data)
            
            print(f"  Объект: {object_data['name']}")
            print(f"  Отзывов: {len(reviews_data)}")
            
            return objects_df, reviews_df
            
        except Exception as e:
            print(f"❌ Ошибка загрузки файла {file_path}: {e}")
            return pd.DataFrame(), pd.DataFrame()
    
    def load_folder(self, folder_path: str, group_name: str) -> tuple:
        """
        Загружает все JSON файлы из папки
        
        Args:
            folder_path: Путь к папке с JSON файлами
            
        Returns:
            Кортеж (DataFrame объектов, DataFrame отзывов)
        """
        if not group_name:
            raise ValueError("Группа (group_name) должна быть явно указана!")
        print(f"Загружаем папку: {folder_path}")
        print(f"Группа: {group_name}")
        
        all_objects = []
        all_reviews = []
        
        # Ищем все JSON файлы в папке
        json_files = []
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                if file.endswith('.json'):
                    json_files.append(os.path.join(root, file))
        
        print(f"Найдено JSON файлов: {len(json_files)}")
        
        # Загружаем каждый файл
        for file_path in json_files:
            objects_df, reviews_df = self.load_json_file(file_path, group_name)
            
            if not objects_df.empty:
                all_objects.append(objects_df)
            if not reviews_df.empty:
                all_reviews.append(reviews_df)
        
        # Объединяем все данные
        if all_objects:
            objects_df = pd.concat(all_objects, ignore_index=True)
        else:
            objects_df = pd.DataFrame()
            
        if all_reviews:
            reviews_df = pd.concat(all_reviews, ignore_index=True)
        else:
            reviews_df = pd.DataFrame()
        
        print(f"Итого объектов: {len(objects_df)}")
        print(f"Итого отзывов: {len(reviews_df)}")
        
        return objects_df, reviews_df
    
    def save_to_database(self, objects_df: pd.DataFrame, reviews_df: pd.DataFrame) -> bool:
        """
        Сохраняет данные в базу данных
        
        Args:
            objects_df: DataFrame с объектами
            reviews_df: DataFrame с отзывами
            
        Returns:
            True если успешно, False иначе
        """
        try:
            print("Сохраняем в базу данных...")
            
            # Сохраняем объекты
            for _, row in objects_df.iterrows():
                object_id = self.db.insert_object(
                    name=row['name'],
                    address=row['address'],
                    latitude=row['latitude'],
                    longitude=row['longitude'],
                    district=row['district'],
                    group_type=row['group'],
                    detected_group_type=row['group']
                )
                print(f"  Сохранен объект: {row['name']} (ID: {object_id})")
            
            # Сохраняем отзывы
            for _, row in reviews_df.iterrows():
                review_id = self.db.insert_review(
                    object_id=row['object_id'],
                    review_text=row['review_text'],
                    rating=row['review_stars'],
                    review_date=row['review_date'],
                    source='json_loader',
                    external_id=row['object_id']
                )
                print(f"  Сохранен отзыв: {row['reviewer_name']} (ID: {review_id})")
            
            print("✅ Данные успешно сохранены в базу данных")
            return True
            
        except Exception as e:
            print(f"❌ Ошибка сохранения в БД: {e}")
            return False
    
    def load_and_save(self, folder_path: str, group_name: str) -> bool:
        """
        Загружает данные из папки и сохраняет в БД
        
        Args:
            folder_path: Путь к папке с данными
            
        Returns:
            True если успешно, False иначе
        """
        if not group_name:
            raise ValueError("Группа (group_name) должна быть явно указана!")
        print(f"=== ЗАГРУЗКА ДАННЫХ ИЗ: {folder_path} ===")
        
        # Загружаем данные
        objects_df, reviews_df = self.load_folder(folder_path, group_name)
        
        if objects_df.empty:
            print("❌ Нет данных для загрузки")
            return False
        
        # Нормализуем поля group и determined_group
        if 'group' in objects_df.columns:
            objects_df['group'] = objects_df['group'].astype(str).str.strip().str.lower()
            objects_df['group_type'] = objects_df['group']
        if 'determined_group' in objects_df.columns:
            objects_df['determined_group'] = objects_df['determined_group'].astype(str).str.strip().str.lower()
            objects_df['detected_group_type'] = objects_df['determined_group']

        # DEBUG: Выводим уникальные значения групп перед сохранением
        print("DEBUG: Уникальные значения group перед сохранением:", objects_df['group'].unique())
        print("DEBUG: Уникальные значения group_type перед сохранением:", objects_df['group_type'].unique() if 'group_type' in objects_df.columns else 'нет поля')

        # Сохраняем в БД
        success = self.save_to_database(objects_df, reviews_df)
        
        return success

def main():
    """Основная функция"""
    loader = SimpleDataLoader()
    
    # Очищаем базу данных
    print("Очищаем базу данных...")
    loader.db.clear_all_data()
    print("✅ База данных очищена")
    
    # Загружаем данные из одной папки для теста
    test_folder = "data/initial_data/json/schools_parse"
    
    if os.path.exists(test_folder):
        success = loader.load_and_save(test_folder, "schools") # Передаем группу явно
        if success:
            print("\n✅ Тестовая загрузка завершена успешно!")
            
            # Проверяем результат
            df = loader.db.export_to_dataframe(include_analysis=True)
            print(f"\nЗаписей в БД: {len(df)}")
            
            if 'group_type' in df.columns:
                groups = df['group_type'].value_counts()
                print(f"Группы: {groups.to_dict()}")
        else:
            print("\n❌ Ошибка при загрузке данных")
    else:
        print(f"❌ Папка не найдена: {test_folder}")

if __name__ == "__main__":
    main() 