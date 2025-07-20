import json
import os
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Union, Any
from datetime import datetime, timedelta
import re

def convert_timestamp_to_date(date_value: Union[str, float]) -> str:
    """
    Преобразует различные форматы дат в формат ДД.ММ.ГГГГ
    
    Args:
        date_value (Union[str, float]): Дата в одном из форматов:
            - "DD MMMM YYYY" (например, "10 июня 2025")
            - Unix timestamp (например, 1730132526.725)
            - Относительная дата (например, "шесть месяцев назад")
            
    Returns:
        str: Дата в формате "DD.MM.YYYY"
    """
    try:
        # Если это строка в формате "DD MMMM YYYY"
        if isinstance(date_value, str) and re.match(r'\d{1,2}\s+[а-яА-Я]+\s+\d{4}', date_value):
            # Словарь для преобразования русских названий месяцев
            months = {
                'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4,
                'мая': 5, 'июня': 6, 'июля': 7, 'августа': 8,
                'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12
            }
            
            # Разбиваем строку на компоненты
            day, month, year = date_value.split()
            month = months[month.lower()]
            
            # Создаем объект datetime
            date = datetime(int(year), month, int(day))
            
        # Если это Unix timestamp
        elif isinstance(date_value, (int, float)):
            date = datetime.fromtimestamp(date_value)
            
        # Если это относительная дата
        elif isinstance(date_value, str) and 'месяц' in date_value.lower():
            # Базовая дата - 1 мая 2025
            base_date = datetime(2025, 5, 1)
            
            # Извлекаем количество месяцев
            months_match = re.search(r'(\d+)\s+месяц', date_value.lower())
            if months_match:
                months = int(months_match.group(1))
                # Вычитаем месяцы из базовой даты
                date = base_date - timedelta(days=30 * months)
            else:
                return "Неизвестная дата"
        else:
            return "Неизвестная дата"
            
        # Форматируем дату в нужный формат
        return date.strftime("%d.%m.%Y")
        
    except Exception as e:
        print(f"Ошибка при обработке даты {date_value}: {str(e)}")
        return "Неизвестная дата"

class DataProcessor:
    def __init__(self, data_dir: str):
        """
        Инициализация обработчика данных
        
        Args:
            data_dir (str): Путь к директории с данными
        """
        self.data_dir = data_dir
        self.sources = ['2gis', 'yandex', 'другие']
        self.data = {}
        
    def load_json_file(self, file_path: str) -> Dict:
        """
        Загрузка JSON файла
        
        Args:
            file_path (str): Путь к файлу
            
        Returns:
            Dict: Содержимое JSON файла
        """
        try:
            print(f"Загрузка файла: {file_path}")
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                print(f"Успешно загружен файл: {file_path}")
                return data
        except Exception as e:
            print(f"Ошибка при загрузке файла {file_path}: {str(e)}")
            return {}

    def extract_object_info(self, data: dict, source: str, group: str) -> dict:
        """
        Извлечение информации об объекте из данных
        
        Args:
            data (dict): Данные из JSON файла
            source (str): Источник данных
            group (str): Группа объектов
            
        Returns:
            dict: Информация об объекте
        """
        print(f"\nИзвлечение информации об объекте из {source}/{group}")
        print(f"Ключи в данных: {list(data.keys())}")
        
        # Проверяем наличие информации о компании
        if 'company_info' in data:
            company_info = data['company_info']
            print(f"Найдена информация о компании: {company_info}")
            
            # Извлекаем основную информацию
            info = {
                'group': group,
                'source': source,
                'name': company_info.get('name', ''),
                'address': company_info.get('address', ''),
                'rating': company_info.get('rating', 0),
                'review_count': company_info.get('count_rating', 0)
            }
            print(f"Извлеченная информация: {info}")
            return info
            
        # Проверяем наличие информации о школе
        elif 'school_info' in data:
            school_info = data['school_info']
            print(f"Найдена информация о школе: {school_info}")
            
            # Извлекаем основную информацию
            info = {
                'group': group,
                'source': source,
                'name': school_info.get('name', ''),
                'address': school_info.get('address', ''),
                'rating': school_info.get('rating', 0),
                'review_count': school_info.get('count_rating', 0)
            }
            print(f"Извлеченная информация: {info}")
            return info
            
        print("Не найдена информация об объекте")
        return None

    def extract_reviews(self, data: Dict) -> List[Dict]:
        """
        Извлечение отзывов
        
        Args:
            data (Dict): Данные из JSON файла
            
        Returns:
            List[Dict]: Список отзывов
        """
        reviews = []
        
        if 'company_reviews' in data:
            print(f"Найдены отзывы компании: {len(data['company_reviews'])} отзывов")
            for review in data['company_reviews']:
                print(f"Обработка отзыва: {review}")
                # Пропускаем отзывы без текста
                if not review.get('text'):
                    print("Пропуск отзыва без текста")
                    continue
                    
                review_info = {
                    'text': review.get('text'),
                    'rating': review.get('stars') or review.get('rating'),
                    'user_name': review.get('name'),
                    'date': review.get('date'),
                    'answer': review.get('answer')
                }
                print(f"Извлеченная информация об отзыве: {review_info}")
                reviews.append(review_info)
        elif 'schools' in data:
            print("Найдены отзывы школ")
            for school in data['schools']:
                if 'reviews' in school:
                    for review in school['reviews']:
                        print(f"Обработка отзыва школы: {review}")
                        # Пропускаем отзывы без текста
                        if not review.get('text'):
                            print("Пропуск отзыва школы без текста")
                            continue
                            
                        review_info = {
                            'text': review.get('text'),
                            'rating': review.get('rating'),
                            'user_name': review.get('user_name'),
                            'date': review.get('date'),
                            'answer': None
                        }
                        print(f"Извлеченная информация об отзыве школы: {review_info}")
                        reviews.append(review_info)
        
        print(f"Извлечено {len(reviews)} отзывов")
        return reviews

    def process_directory(self, source: str):
        """
        Обработка директории с данными
        
        Args:
            source (str): Источник данных
        """
        source_dir = os.path.join(self.data_dir, source)
        if not os.path.exists(source_dir):
            print(f"Директория {source_dir} не существует")
            return

        print(f"\nОбработка директории: {source_dir}")
        if source not in self.data:
            self.data[source] = []
            print(f"Создан новый список для источника {source}")
            
        for group_dir in os.listdir(source_dir):
            group_path = os.path.join(source_dir, group_dir)
            if not os.path.isdir(group_path):
                continue

            print(f"\nОбработка группы: {group_dir}")
            for file_name in os.listdir(group_path):
                if not file_name.endswith('.json'):
                    continue

                file_path = os.path.join(group_path, file_name)
                data = self.load_json_file(file_path)
                
                if not data:
                    continue

                # Извлечение информации об объектах
                objects_info = self.extract_object_info(data, source, group_dir)
                
                # Обработка как генератора или как словаря
                if hasattr(objects_info, '__iter__') and not isinstance(objects_info, dict):
                    objects_info_list = list(objects_info)
                else:
                    objects_info_list = [objects_info]
                
                print(f"\nОбработка объектов из файла {file_name}:")
                print(f"Количество объектов: {len(objects_info_list)}")
                
                # Извлечение отзывов
                reviews = self.extract_reviews(data)
                print(f"Получено отзывов: {len(reviews)}")
                
                # Сохранение данных
                for obj_info in objects_info_list:
                    if obj_info:  # Проверяем, что информация об объекте не пустая
                        self.data[source].append({
                            'object_info': obj_info,
                            'reviews': reviews
                        })
                        print(f"Сохранен объект {obj_info.get('name', 'Без имени')} с {len(reviews)} отзывами")
                
                print(f"Текущее количество объектов в источнике {source}: {len(self.data[source])}")

    def process_all_data(self):
        """
        Обработка всех данных из всех источников
        """
        for source in self.sources:
            self.process_directory(source)

    def get_dataframe(self) -> pd.DataFrame:
        """
        Преобразование данных в DataFrame
        
        Returns:
            pd.DataFrame: DataFrame с данными
        """
        rows = []
        
        print("\nСоздание DataFrame:")
        print(f"Доступные источники: {list(self.data.keys())}")
        
        for source, objects in self.data.items():
            print(f"\nОбработка источника: {source}")
            print(f"Количество объектов: {len(objects)}")
            
            for obj in objects:
                obj_info = obj['object_info']
                reviews = obj['reviews']
                print(f"\nОбработка объекта: {obj_info.get('name', 'Без имени')}")
                print(f"Количество отзывов: {len(reviews)}")
                
                if not reviews:  # Если нет отзывов, создаем одну запись только с информацией об объекте
                    row = {
                        'source': source,
                        'group': obj_info.get('group', ''),
                        'object_name': obj_info.get('name', ''),
                        'address': obj_info.get('address', ''),
                        'object_rating': obj_info.get('rating', 0),
                        'review_count': obj_info.get('review_count', 0),
                        'review_text': '',
                        'review_rating': None,
                        'user_name': '',
                        'review_date': None,
                        'answer_text': ''
                    }
                    rows.append(row)
                    print(f"Добавлена строка без отзывов для объекта {obj_info.get('name', 'Без имени')}")
                else:
                    for review in reviews:
                        row = {
                            'source': source,
                            'group': obj_info.get('group', ''),
                            'object_name': obj_info.get('name', ''),
                            'address': obj_info.get('address', ''),
                            'object_rating': obj_info.get('rating', 0),
                            'review_count': obj_info.get('review_count', 0),
                            'review_text': review.get('text', ''),
                            'review_rating': review.get('rating', None),
                            'user_name': review.get('user_name', ''),
                            'review_date': convert_timestamp_to_date(review.get('date', 0)),
                            'answer_text': review.get('answer', '')
                        }
                        rows.append(row)
                        print(f"Добавлена строка с отзывом от {review.get('user_name', 'Аноним')}")
        
        df = pd.DataFrame(rows)
        print(f"\nСоздан DataFrame с {len(df)} строками")
        if len(df) > 0:
            print("\nКолонки в DataFrame:")
            print(df.columns.tolist())
            print("\nПервые несколько строк:")
            print(df.head())
        else:
            print("\nDataFrame пустой!")
            print("Проверьте, что данные были успешно загружены и сохранены в self.data")
            print(f"Текущее содержимое self.data: {self.data}")
        return df

if __name__ == "__main__":
    # Пример использования
    processor = DataProcessor("parsed")
    processor.process_all_data()
    df = processor.get_dataframe()
    print(f"Обработано {len(df)} записей") 