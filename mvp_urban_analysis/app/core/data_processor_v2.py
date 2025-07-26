"""
Модуль для обработки данных отзывов городской среды (версия 2.0)
Новый алгоритм работы с базой данных вместо архивных файлов
"""

import pandas as pd
import numpy as np
import hashlib
import os
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import logging

# Импортируем модули
from .csv_processor import CSVProcessor
from .json_processor import JSONProcessor
from .excel_processor import ExcelProcessor
from .district_detector import DistrictDetector
from .database_fixed import DatabaseManager
from .text_analyzer import TextAnalyzer
from .config import SENTIMENT_CONFIG

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataProcessorV2:
    """Класс для обработки данных отзывов (версия 2.0)"""
    
    def __init__(self, data_dir: str = "data", geocoder_api_key: str = None):
        """
        Инициализация процессора данных
        
        Args:
            data_dir: Путь к директории с данными
            geocoder_api_key: API ключ для геокодирования
        """
        self.data_dir = data_dir
        self.results_dir = os.path.join(data_dir, "results")
        
        # Создаем директории если их нет
        os.makedirs(self.results_dir, exist_ok=True)
        
        # Инициализируем модули
        self.csv_processor = CSVProcessor()
        self.json_processor = JSONProcessor()
        self.excel_processor = ExcelProcessor()
        self.district_detector = DistrictDetector(api_key=geocoder_api_key)
        self.database_manager = DatabaseManager()
        self.text_analyzer = TextAnalyzer()
        
        # Настройки сентимента
        self.sentiment_config = SENTIMENT_CONFIG
        
        # Группы объектов
        self.object_groups = [
            'resident_complexes', 'school', 'hospital', 'pharmacy',
            'kindergarden', 'polyclinic', 'university', 'shopmall'
        ]
        
        # Обязательные поля для обработки
        self.required_fields_processing = ['group', 'review_text']
        
        # Обязательные поля для сохранения в БД
        self.required_fields_database = ['group', 'name', 'address', 'review_text', 'date']

    def load_data(self, file_path: str, file_type: str = None, 
                  sheet_name: str = None, filters: Dict = None) -> pd.DataFrame:
        """
        Загрузка данных из файла (CSV, JSON, Excel)
        
        Args:
            file_path: Путь к файлу
            file_type: Тип файла (csv, json, excel)
            sheet_name: Имя листа для Excel
            filters: Фильтры для Excel
            
        Returns:
            DataFrame с данными
        """
        if not file_type:
            file_type = self._detect_file_type(file_path)
        
        try:
            if file_type == 'csv':
                return self.csv_processor.load_data(file_path)
            elif file_type == 'json':
                return self.json_processor.load_data(file_path)
            elif file_type == 'excel':
                return self.excel_processor.load_data(file_path, sheet_name, filters)
            else:
                raise ValueError(f"Неподдерживаемый тип файла: {file_type}")
                
        except Exception as e:
            logger.error(f"Ошибка загрузки файла {file_path}: {e}")
            raise

    def _detect_file_type(self, file_path: str) -> str:
        """Определение типа файла по расширению"""
        ext = os.path.splitext(file_path)[1].lower()
        if ext == '.csv':
            return 'csv'
        elif ext == '.json':
            return 'json'
        elif ext in ['.xlsx', '.xls']:
            return 'excel'
        else:
            raise ValueError(f"Неподдерживаемое расширение файла: {ext}")

    def validate_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Валидация данных
        
        Args:
            df: DataFrame с данными
            
        Returns:
            Кортеж (валидные данные, невалидные данные)
        """
        if df.empty:
            return pd.DataFrame(), pd.DataFrame()
        
        # Проверяем обязательные поля
        missing_fields = []
        for field in self.required_fields_processing:
            if field not in df.columns:
                missing_fields.append(field)
        
        if missing_fields:
            logger.warning(f"Отсутствуют обязательные поля: {missing_fields}")
            return pd.DataFrame(), df
        
        # Фильтруем записи с пустыми обязательными полями
        valid_mask = df[self.required_fields_processing].notna().all(axis=1)
        valid_data = df[valid_mask].copy()
        invalid_data = df[~valid_mask].copy()
        
        logger.info(f"Валидных записей: {len(valid_data)}, невалидных: {len(invalid_data)}")
        
        return valid_data, invalid_data

    def process_data_to_database(self, df: pd.DataFrame, source: str = "unknown") -> Dict:
        """
        Обработка данных и сохранение в базу данных согласно новому алгоритму
        
        Args:
            df: DataFrame с данными
            source: Источник данных
            
        Returns:
            Словарь с результатами обработки
        """
        try:
            # Валидация данных
            valid_data, invalid_data = self.validate_data(df)
            
            if valid_data.empty:
                return {
                    'success': False,
                    'error': 'Нет валидных данных для обработки',
                    'total_records': len(df),
                    'valid_records': 0,
                    'invalid_records': len(invalid_data)
                }
            
            # Статистика обработки
            stats = {
                'total_records': len(df),
                'valid_records': len(valid_data),
                'invalid_records': len(invalid_data),
                'objects_processed': 0,
                'objects_skipped': 0,
                'reviews_processed': 0,
                'reviews_skipped': 0,
                'group_conflicts': [],
                'duplicate_reviews': []
            }
            
            # Обработка каждой записи
            for idx, row in valid_data.iterrows():
                try:
                    result = self._process_single_record(row, source)
                    
                    if result['object_processed']:
                        stats['objects_processed'] += 1
                    else:
                        stats['objects_skipped'] += 1
                        if result.get('group_conflict'):
                            stats['group_conflicts'].append(result['group_conflict'])
                    
                    if result['review_processed']:
                        stats['reviews_processed'] += 1
                    else:
                        stats['reviews_skipped'] += 1
                        if result.get('duplicate_review'):
                            stats['duplicate_reviews'].append(result['duplicate_review'])
                            
                except Exception as e:
                    logger.error(f"Ошибка обработки записи {idx}: {e}")
                    stats['invalid_records'] += 1
            
            return {
                'success': True,
                'message': f"Обработано {stats['objects_processed']} объектов, {stats['reviews_processed']} отзывов",
                'stats': stats
            }
            
        except Exception as e:
            logger.error(f"Ошибка обработки данных: {e}")
            return {
                'success': False,
                'error': f'Ошибка обработки данных: {str(e)}'
            }

    def _process_single_record(self, row: pd.Series, source: str) -> Dict:
        """
        Обработка одной записи согласно новому алгоритму
        
        Args:
            row: Строка данных
            source: Источник данных
            
        Returns:
            Словарь с результатами обработки
        """
        result = {
            'object_processed': False,
            'review_processed': False,
            'group_conflict': None,
            'duplicate_review': None
        }
        
        try:
            # Извлекаем данные
            name = str(row.get('name', '')).strip()
            address = str(row.get('address', '')).strip()
            group = str(row.get('group', '')).strip()
            review_text = str(row.get('review_text', '')).strip()
            rating = row.get('rating')
            review_date = row.get('date')
            user_name = row.get('user_name', '')
            
            if not name or not address:
                logger.warning(f"Пропускаем запись без названия или адреса: {name}, {address}")
                return result
            
            # 1. Проверяем существование объекта в БД
            object_key = self.database_manager.create_object_key(name, address)
            
            with self.database_manager.get_connection() as conn:
                cursor = conn.execute("SELECT id, group_id FROM objects WHERE object_key = ?", (object_key,))
                existing_object = cursor.fetchone()
                
                if existing_object:
                    # Объект уже существует - проверяем группу
                    object_id = existing_object['id']
                    existing_group_id = existing_object['group_id']
                    
                    # Получаем группу из входных данных
                    input_group_id = self.database_manager.get_group_id(group)
                    
                    if existing_group_id != input_group_id:
                        # Конфликт групп
                        existing_group = self._get_group_name_by_id(existing_group_id)
                        conflict_msg = f"Группа объекта ({name}, {address}) ({existing_group}) не совпадает с Группой объекта из входного файла ({group})"
                        logger.warning(conflict_msg)
                        result['group_conflict'] = conflict_msg
                        return result
                    
                    # Группы совпадают - используем существующий объект
                    logger.info(f"Используем существующий объект: {name}")
                    
                else:
                    # Новый объект - определяем все параметры
                    logger.info(f"Создаем новый объект: {name}")
                    
                    # Определяем координаты и район
                    latitude, longitude, district = self._get_location_data(name, address)
                    
                    # Определяем группу из текста
                    detected_group = self._detect_group_from_text(review_text, name)
                    
                    # Создаем объект в БД
                    object_id = self.database_manager.insert_object(
                        name=name,
                        address=address,
                        latitude=latitude,
                        longitude=longitude,
                        district=district,
                        group_type=group,
                        detected_group_type=detected_group
                    )
                    
                    if object_id:
                        result['object_processed'] = True
                    else:
                        logger.error(f"Не удалось создать объект: {name}")
                        return result
                
                # 2. Обрабатываем отзыв
                if review_text:
                    # Проверяем существование отзыва
                    cursor = conn.execute("""
                        SELECT id FROM reviews 
                        WHERE object_id = ? AND review_date = ? AND user_name = ? AND rating = ?
                    """, (object_id, review_date, user_name, rating))
                    
                    existing_review = cursor.fetchone()
                    
                    if existing_review:
                        # Отзыв уже существует
                        duplicate_msg = f"Отзыв ({name}, {address}) ({user_name}, {review_date}, {review_text[:40]}...) совпадает с Отзывом из входного файла"
                        logger.warning(duplicate_msg)
                        result['duplicate_review'] = duplicate_msg
                        return result
                    
                    # Новый отзыв - добавляем в БД
                    review_id = self.database_manager.insert_review(
                        object_id=object_id,
                        review_text=review_text,
                        rating=rating,
                        review_date=review_date,
                        source=source,
                        external_id=f"{source}_{hashlib.md5(review_text.encode()).hexdigest()[:8]}"
                    )
                    
                    if review_id:
                        # Анализируем сентимент
                        self._analyze_sentiment(review_id, review_text)
                        result['review_processed'] = True
                        logger.info(f"Добавлен отзыв для объекта: {name}")
                    else:
                        logger.error(f"Не удалось добавить отзыв для объекта: {name}")
                
                return result
                
        except Exception as e:
            logger.error(f"Ошибка обработки записи: {e}")
            return result

    def _get_location_data(self, name: str, address: str) -> Tuple[Optional[float], Optional[float], Optional[str]]:
        """
        Получение координат и района для объекта
        
        Args:
            name: Название объекта
            address: Адрес объекта
            
        Returns:
            Кортеж (широта, долгота, район)
        """
        try:
            # Временно отключаем геокодирование для ускорения
            # Используем тестовые координаты для Москвы
            logger.info(f"Используем тестовые координаты для: {name}, {address}")
            return 55.7558, 37.6176, 'Центральный район'
            
            # Раскомментировать для включения геокодирования:
            # location_data = self.district_detector.get_location_info(f"{name}, {address}")
            # if location_data:
            #     return (
            #         location_data.get('latitude'),
            #         location_data.get('longitude'),
            #         location_data.get('district')
            #     )
            # else:
            #     logger.warning(f"Не удалось получить координаты для: {name}, {address}")
            #     return None, None, None
                
        except Exception as e:
            logger.error(f"Ошибка получения координат для {name}: {e}")
            return None, None, None

    def _detect_group_from_text(self, review_text: str, object_name: str) -> Optional[str]:
        """
        Определение группы из текста отзыва и названия объекта
        
        Args:
            review_text: Текст отзыва
            object_name: Название объекта
            
        Returns:
            Определенная группа или None
        """
        try:
            # Анализируем текст отзыва и название объекта
            combined_text = f"{object_name} {review_text}".lower()
            
            # Определяем группу по ключевым словам
            group_keywords = {
                'school': ['школа', 'учитель', 'ученик', 'класс', 'урок'],
                'hospital': ['больница', 'врач', 'пациент', 'лечение', 'медицинский'],
                'pharmacy': ['аптека', 'лекарство', 'препарат', 'фармацевт'],
                'kindergarden': ['детский сад', 'воспитатель', 'группа', 'игра'],
                'polyclinic': ['поликлиника', 'врач', 'прием', 'медицинский'],
                'university': ['университет', 'студент', 'преподаватель', 'лекция'],
                'shopmall': ['торговый центр', 'магазин', 'покупка', 'товар'],
                'resident_complexes': ['жилой комплекс', 'квартира', 'дом', 'жилье']
            }
            
            for group, keywords in group_keywords.items():
                if any(keyword in combined_text for keyword in keywords):
                    return group
            
            return None
            
        except Exception as e:
            logger.error(f"Ошибка определения группы: {e}")
            return None

    def _analyze_sentiment(self, review_id: int, review_text: str):
        """
        Анализ сентимента отзыва
        
        Args:
            review_id: ID отзыва
            review_text: Текст отзыва
        """
        try:
            # Получаем ID метода анализа
            method_id = self.database_manager.get_method_id('nlp_vader')
            
            if method_id:
                # Анализируем сентимент
                sentiment_result = self.text_analyzer.analyze_sentiment(review_text)
                
                if sentiment_result:
                    # Сохраняем результат анализа
                    self.database_manager.insert_analysis_result(
                        review_id=review_id,
                        method_id=method_id,
                        sentiment=sentiment_result.get('sentiment', 'neutral'),
                        confidence=sentiment_result.get('confidence', 0.0),
                        review_type=sentiment_result.get('review_type'),
                        keywords=sentiment_result.get('keywords'),
                        topics=sentiment_result.get('topics')
                    )
                    
        except Exception as e:
            logger.error(f"Ошибка анализа сентимента для отзыва {review_id}: {e}")

    def _get_group_name_by_id(self, group_id: int) -> str:
        """
        Получение названия группы по ID
        
        Args:
            group_id: ID группы
            
        Returns:
            Название группы
        """
        try:
            with self.database_manager.get_connection() as conn:
                cursor = conn.execute("SELECT group_name FROM object_groups WHERE id = ?", (group_id,))
                result = cursor.fetchone()
                return result['group_name'] if result else 'Неизвестная группа'
        except Exception as e:
            logger.error(f"Ошибка получения названия группы: {e}")
            return 'Неизвестная группа'

    def get_processing_statistics(self) -> Dict:
        """
        Получение статистики обработки данных
        
        Returns:
            Словарь со статистикой
        """
        try:
            return self.database_manager.get_statistics()
        except Exception as e:
            logger.error(f"Ошибка получения статистики: {e}")
            return {} 