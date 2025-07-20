"""
Модуль для обработки данных отзывов городской среды
"""

import pandas as pd
import numpy as np
import hashlib
import os
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import logging

# Импортируем новые модули
from .csv_processor import CSVProcessor
from .json_processor import JSONProcessor
from .excel_processor import ExcelProcessor
from .district_detector import DistrictDetector

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataProcessor:
    """Класс для обработки данных отзывов"""
    
    def __init__(self, data_dir: str = "data", geocoder_api_key: str = None):
        """
        Инициализация процессора данных
        
        Args:
            data_dir: Путь к директории с данными
            geocoder_api_key: API ключ для геокодирования
        """
        self.data_dir = data_dir
        self.archive_file = os.path.join(data_dir, "archives", "processed_reviews.csv")
        self.results_dir = os.path.join(data_dir, "results")
        
        # Создаем директории если их нет
        os.makedirs(os.path.dirname(self.archive_file), exist_ok=True)
        os.makedirs(self.results_dir, exist_ok=True)
        
        # Инициализируем новые модули
        self.csv_processor = CSVProcessor()
        self.json_processor = JSONProcessor()
        self.excel_processor = ExcelProcessor()
        self.district_detector = DistrictDetector(api_key=geocoder_api_key)
        
        # Отладочная информация об API ключе
        if geocoder_api_key:
            logger.info(f"API ключ для геокодирования установлен: {geocoder_api_key[:10]}...")
        else:
            logger.warning("API ключ для геокодирования НЕ установлен")
        
        # Группы объектов
        self.object_groups = [
            'resident_complexes', 'school', 'hospital', 'pharmacy',
            'kindergarden', 'polyclinic', 'university', 'shopmall'
        ]
        
        # Обязательные поля для обработки
        self.required_fields_processing = ['group', 'review_text']
        
        # Обязательные поля для сохранения в архив
        self.required_fields_archive = ['group', 'name', 'address', 'review_text', 'date']
    
    def load_data(self, file_path: str, file_type: str = None, 
                  sheet_name: str = None, filters: Dict = None) -> pd.DataFrame:
        """
        Загрузка данных из файла (CSV, JSON, Excel)
        
        Args:
            file_path: Путь к файлу
            file_type: Тип файла ('csv', 'json', 'excel', 'auto')
            sheet_name: Имя листа для Excel файлов
            filters: Фильтры для Excel файлов
            
        Returns:
            DataFrame с данными
        """
        try:
            logger.info(f"=== НАЧАЛО ЗАГРУЗКИ ДАННЫХ ===")
            logger.info(f"Файл: {file_path}")
            logger.info(f"Тип файла: {file_type}")
            
            # Определяем тип файла если не указан
            if file_type is None or file_type == 'auto':
                file_ext = os.path.splitext(file_path)[1].lower()
                if file_ext == '.csv':
                    file_type = 'csv'
                elif file_ext == '.json':
                    file_type = 'json'
                elif file_ext in ['.xlsx', '.xls']:
                    file_type = 'excel'
                else:
                    logger.error(f"Неподдерживаемый тип файла: {file_ext}")
                    return pd.DataFrame()
            
            # Обрабатываем в зависимости от типа
            if file_type == 'csv':
                logger.info("Обрабатываем как CSV файл")
                df = self.csv_processor.process_csv_file(file_path)
                processor = self.csv_processor
            elif file_type == 'json':
                logger.info("Обрабатываем как JSON файл")
                df = self.json_processor.process_json_file_or_directory(file_path)
                processor = self.json_processor
            elif file_type == 'excel':
                logger.info("Обрабатываем как Excel файл")
                df = self.excel_processor.process_excel_file(file_path, sheet_name, filters)
                processor = self.excel_processor
            else:
                logger.error(f"Неподдерживаемый тип файла: {file_type}")
                return pd.DataFrame()
            
            if df.empty:
                logger.error(f"Не удалось загрузить данные из {file_path}")
                return df
            
            # ПРОВЕРКА ГРУПП ПОСЛЕ ЗАГРУЗКИ
            logger.info("=== ПРОВЕРКА ГРУПП ПОСЛЕ ЗАГРУЗКИ ===")
            if 'group' in df.columns:
                logger.info(f"Колонка 'group' найдена в DataFrame")
                unique_groups = df['group'].unique()
                logger.info(f"Уникальные группы в данных: {unique_groups}")
                logger.info(f"Количество записей по группам:")
                for group in unique_groups:
                    count = len(df[df['group'] == group])
                    logger.info(f"  {group}: {count} записей")
                
                # Проверяем первые несколько записей
                logger.info("Первые 3 записи:")
                for i, row in df.head(3).iterrows():
                    logger.info(f"  {i+1}. Группа: '{row.get('group', 'N/A')}', Название: '{row.get('name', 'N/A')}'")
            else:
                logger.warning("Колонка 'group' НЕ найдена в DataFrame")
                logger.info(f"Доступные колонки: {list(df.columns)}")
            
            # Валидируем данные
            validation_result = processor.validate_dataframe(df)
            logger.info(f"Результаты валидации: {validation_result}")
            
            # Проверяем, нужно ли запросить группу
            if hasattr(processor, 'validate_dataframe') and 'needs_group_selection' in validation_result:
                if validation_result['needs_group_selection']:
                    logger.warning("Обнаружены записи с пустой группой - требуется выбор группы пользователем")
                    # Возвращаем DataFrame с пустой группой для обработки в основном коде
                    return df
            
            logger.info("=== ЗАВЕРШЕНИЕ ЗАГРУЗКИ ДАННЫХ ===")
            return df
            
        except Exception as e:
            logger.error(f"Ошибка загрузки файла {file_path}: {e}")
            return pd.DataFrame()
    
    def load_csv_data(self, file_path: str) -> pd.DataFrame:
        """
        Загрузка данных из CSV файла (для обратной совместимости)
        
        Args:
            file_path: Путь к CSV файлу
            
        Returns:
            DataFrame с данными
        """
        return self.load_data(file_path, 'csv')
    
    def validate_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Валидация данных
        
        Args:
            df: DataFrame для валидации
            
        Returns:
            Tuple[DataFrame, DataFrame]: (валидные данные, невалидные данные)
        """
        logger.info(f"=== НАЧАЛО ВАЛИДАЦИИ ДАННЫХ ===")
        logger.info(f"Исходное количество записей: {len(df)}")
        
        if df.empty:
            logger.warning("DataFrame пустой")
            return pd.DataFrame(), pd.DataFrame()
        
        # Проверяем обязательные поля для обработки
        missing_fields = [field for field in self.required_fields_processing 
                         if field not in df.columns]
        
        logger.info(f"Обязательные поля для обработки: {self.required_fields_processing}")
        logger.info(f"Найденные поля в DataFrame: {list(df.columns)}")
        logger.info(f"Отсутствующие поля: {missing_fields}")
        
        if missing_fields:
            logger.warning(f"Отсутствуют обязательные поля: {missing_fields}")
            return pd.DataFrame(), df
        
        # Детальная проверка заполненности обязательных полей
        logger.info("=== ДЕТАЛЬНАЯ ПРОВЕРКА ЗАПОЛНЕННОСТИ ПОЛЕЙ ===")
        for field in self.required_fields_processing:
            if field in df.columns:
                empty_count = df[field].isna().sum()
                empty_percent = (empty_count / len(df)) * 100
                logger.info(f"Поле '{field}': {empty_count} пустых записей ({empty_percent:.1f}%)")
                
                # Показываем примеры пустых записей
                if empty_count > 0:
                    empty_examples = df[df[field].isna()].head(3)
                    logger.info(f"Примеры записей с пустым полем '{field}':")
                    for idx, row in empty_examples.iterrows():
                        logger.info(f"  Строка {idx}: {dict(row)}")
            else:
                logger.warning(f"Поле '{field}' отсутствует в DataFrame")
        
        # Фильтруем записи с пустыми обязательными полями
        logger.info("=== ФИЛЬТРАЦИЯ ПО ОБЯЗАТЕЛЬНЫМ ПОЛЯМ ===")
        valid_mask = df[self.required_fields_processing].notna().all(axis=1)
        valid_df = df[valid_mask].copy()
        invalid_df = df[~valid_mask].copy()
        
        logger.info(f"Записей с заполненными обязательными полями: {len(valid_df)}")
        logger.info(f"Записей с пустыми обязательными полями: {len(invalid_df)}")
        
        # Показываем примеры невалидных записей
        if len(invalid_df) > 0:
            logger.info("Примеры невалидных записей (с пустыми обязательными полями):")
            for idx, row in invalid_df.head(5).iterrows():
                missing_fields = [field for field in self.required_fields_processing 
                                if pd.isna(row.get(field, ''))]
                logger.info(f"  Строка {idx}: отсутствуют поля {missing_fields}")
                logger.info(f"    Данные: {dict(row)}")
        
        # Проверяем адреса для сохранения в архив
        logger.info("=== ПРОВЕРКА АДРЕСОВ ДЛЯ АРХИВА ===")
        if 'address' in valid_df.columns:
            address_empty_count = valid_df['address'].isna().sum()
            address_empty_string_count = (valid_df['address'] == '').sum()
            total_addressless = address_empty_count + address_empty_string_count
            
            logger.info(f"Записей с пустым адресом (NaN): {address_empty_count}")
            logger.info(f"Записей с пустой строкой адреса: {address_empty_string_count}")
            logger.info(f"Всего записей без адреса: {total_addressless}")
            
            # Показываем примеры записей без адреса
            if total_addressless > 0:
                addressless_examples = valid_df[
                    valid_df['address'].isna() | (valid_df['address'] == '')
                ].head(3)
                logger.info("Примеры записей без адреса:")
                for idx, row in addressless_examples.iterrows():
                    logger.info(f"  Строка {idx}: {dict(row)}")
        
        archive_mask = valid_df['address'].notna() & (valid_df['address'] != '')
        valid_for_archive = valid_df[archive_mask].copy()
        addressless = valid_df[~archive_mask].copy()
        
        logger.info(f"Валидных записей: {len(valid_df)}")
        logger.info(f"Записей без адреса: {len(addressless)}")
        logger.info(f"Записей для архива: {len(valid_for_archive)}")
        logger.info("=== ЗАВЕРШЕНИЕ ВАЛИДАЦИИ ДАННЫХ ===")
        
        return valid_for_archive, addressless
    
    def process_districts(self, df: pd.DataFrame, batch_size: int = 10, delay: float = 0.1) -> pd.DataFrame:
        """
        Обработка данных для определения районов
        
        Args:
            df: DataFrame с данными
            batch_size: Размер пакета для обработки
            delay: Задержка между запросами (секунды)
            
        Returns:
            DataFrame с добавленной информацией о районах
        """
        if df.empty:
            logger.info("DataFrame пустой, пропускаем обработку районов")
            return df
        
        logger.info("=== НАЧАЛО ОБРАБОТКИ РАЙОНОВ ===")
        logger.info(f"Исходное количество записей: {len(df)}")
        
        # Проверяем наличие полей координат
        has_coordinates = 'latitude' in df.columns and 'longitude' in df.columns
        logger.info(f"Поля координат присутствуют: {has_coordinates}")
        
        if has_coordinates:
            # Показываем статистику координат
            non_zero_coords = df[(df['latitude'] != 0.0) & (df['longitude'] != 0.0)]
            logger.info(f"Записей с ненулевыми координатами: {len(non_zero_coords)}")
            
            if len(non_zero_coords) > 0:
                sample_coords = non_zero_coords[['latitude', 'longitude']].head(3)
                logger.info(f"Примеры координат:\n{sample_coords}")
        
        # Обрабатываем районы
        result_df = self.district_detector.process_dataframe_districts(
            df, batch_size=batch_size, delay=delay
        )
        
        # Получаем статистику
        stats = self.district_detector.get_district_statistics(result_df)
        logger.info(f"Статистика по районам: {stats}")
        
        logger.info("=== ЗАВЕРШЕНИЕ ОБРАБОТКИ РАЙОНОВ ===")
        return result_df
    
    def generate_hash_key(self, row: pd.Series) -> str:
        """
        Генерация хэш-ключа для записи
        
        Args:
            row: Строка данных
            
        Returns:
            Хэш-ключ
        """
        # Базовые поля для хэша
        hash_fields = [
            str(row.get('group', '')),
            str(row.get('name', '')),
            str(row.get('address', '')),
            str(row.get('user_name', '')),
            str(row.get('date', '')),
            str(row.get('review_text', ''))[:10]  # Первые 10 символов
        ]
        
        # Создаем строку для хэширования
        hash_string = '|'.join(hash_fields)
        return hashlib.md5(hash_string.encode('utf-8')).hexdigest()
    
    def generate_hash_key_for_part(self, base_hash: str, part_type: str) -> str:
        """
        Генерация хэш-ключа для части сложного отзыва
        
        Args:
            base_hash: Базовый хэш
            part_type: Тип части ('p' для положительной, 'n' для отрицательной)
            
        Returns:
            Хэш-ключ с суффиксом
        """
        return f"{base_hash}_{part_type}"
    
    def generate_hash_key_with_suffix(self, row: pd.Series, suffix: str = None) -> str:
        """
        Генерация хэш-ключа с возможным суффиксом
        
        Args:
            row: Строка данных
            suffix: Суффикс для части сложного отзыва ('_p' или '_n')
            
        Returns:
            Хэш-ключ с суффиксом
        """
        base_hash = self.generate_hash_key(row)
        if suffix:
            return f"{base_hash}{suffix}"
        return base_hash
    
    def load_archive(self) -> pd.DataFrame:
        """
        Загрузка архивного файла
        
        Returns:
            DataFrame с архивными данными
        """
        if os.path.exists(self.archive_file):
            try:
                df = pd.read_csv(self.archive_file, encoding='utf-8-sig')
                logger.info(f"Загружен архив: {len(df)} записей")
                return df
            except Exception as e:
                logger.error(f"Ошибка загрузки архива: {e}")
                return pd.DataFrame()
        else:
            logger.info("Архивный файл не найден, создается новый")
            return pd.DataFrame()
    
    def save_to_archive(self, df: pd.DataFrame) -> bool:
        """
        Сохранение данных в архив
        
        Args:
            df: DataFrame для сохранения
            
        Returns:
            True если сохранение успешно
        """
        try:
            logger.info("=== НАЧАЛО СОХРАНЕНИЯ В АРХИВ ===")
            logger.info(f"Исходное количество записей: {len(df)}")
            
            # Приводим к формату архива
            archive_df = self.csv_processor.convert_to_archive_format(df)
            logger.info(f"После конвертации в формат архива: {len(archive_df)} записей")
            
            # Добавляем хэш-ключи с учетом суффиксов для частей сложных отзывов
            hash_keys = []
            logger.info("=== ГЕНЕРАЦИЯ ХЭШ-КЛЮЧЕЙ ===")
            
            for idx, row in archive_df.iterrows():
                if row.get('is_complex_part', False) and 'part_type' in row:
                    # Для частей сложных отзывов используем суффикс
                    hash_key = self.generate_hash_key_with_suffix(row, row['part_type'])
                    logger.info(f"Строка {idx}: сложный отзыв, хэш с суффиксом: {hash_key}")
                else:
                    # Для обычных отзывов используем базовый хэш
                    hash_key = self.generate_hash_key(row)
                    logger.info(f"Строка {idx}: обычный отзыв, хэш: {hash_key}")
                    
                    # Показываем поля, используемые для хэша
                    hash_fields = [
                        str(row.get('group', '')),
                        str(row.get('name', '')),
                        str(row.get('address', '')),
                        str(row.get('user_name', '')),
                        str(row.get('date', '')),
                        str(row.get('review_text', ''))[:10]  # Первые 10 символов
                    ]
                    logger.info(f"  Поля для хэша: {hash_fields}")
                
                hash_keys.append(hash_key)
            
            archive_df['hash_key'] = hash_keys
            
            # Загружаем существующий архив
            existing_archive = self.load_archive()
            logger.info(f"Загружен существующий архив: {len(existing_archive)} записей")
            
            if not existing_archive.empty:
                # Проверяем дубликаты
                existing_hashes = set(existing_archive['hash_key'])
                logger.info(f"Количество уникальных хэшей в архиве: {len(existing_hashes)}")
                
                # Показываем несколько примеров хэшей из архива
                if len(existing_hashes) > 0:
                    sample_hashes = list(existing_hashes)[:5]
                    logger.info(f"Примеры хэшей в архиве: {sample_hashes}")
                
                new_records = archive_df[~archive_df['hash_key'].isin(existing_hashes)]
                
                logger.info(f"Новых записей: {len(new_records)}")
                logger.info(f"Дубликатов: {len(archive_df) - len(new_records)}")
                
                # Показываем примеры новых хэшей
                if len(new_records) > 0:
                    new_hashes = new_records['hash_key'].head(5).tolist()
                    logger.info(f"Примеры новых хэшей: {new_hashes}")
                
                # Показываем примеры дубликатов
                duplicates = archive_df[archive_df['hash_key'].isin(existing_hashes)]
                if len(duplicates) > 0:
                    logger.info("Примеры дубликатов:")
                    for idx, row in duplicates.head(3).iterrows():
                        logger.info(f"  Дубликат {idx}: хэш={row['hash_key']}, группа={row.get('group', '')}, текст={str(row.get('review_text', ''))[:50]}...")
                
                archive_df = new_records
            
            if not archive_df.empty:
                # Объединяем с архивом
                if not existing_archive.empty:
                    combined_df = pd.concat([existing_archive, archive_df], ignore_index=True)
                else:
                    combined_df = archive_df
                
                # Сохраняем
                combined_df.to_csv(self.archive_file, index=False, encoding='utf-8-sig')
                logger.info(f"Сохранено {len(archive_df)} новых записей в архив")
                logger.info("=== ЗАВЕРШЕНИЕ СОХРАНЕНИЯ В АРХИВ ===")
                return True
            else:
                logger.info("Нет новых записей для сохранения")
                logger.info("=== ЗАВЕРШЕНИЕ СОХРАНЕНИЯ В АРХИВ ===")
                return True
                
        except Exception as e:
            logger.error(f"Ошибка сохранения в архив: {e}")
            return False
    
    def get_archive_info(self) -> Dict:
        """
        Получение информации об архивном файле
        
        Returns:
            Словарь с информацией об архиве
        """
        df = self.load_archive()
        
        if df.empty:
            return {
                'total_records': 0,
                'groups': {},
                'determined_groups': {},
                'date_range': {'min': None, 'max': None},
                'field_completeness': {}
            }
        
        # Рассчитываем заполненность полей
        field_completeness = self._calculate_field_completeness(df)
        
        # Конвертируем числовые типы для JSON сериализации
        groups_dict = df['group'].value_counts().to_dict()
        # Конвертируем ключи и значения в строки
        groups_dict = {str(k): int(v) for k, v in groups_dict.items()}
        
        # Добавляем информацию о determined_groups
        determined_groups_dict = {}
        if 'determined_group' in df.columns:
            determined_groups_dict = df['determined_group'].value_counts().to_dict()
            # Конвертируем ключи и значения в строки
            determined_groups_dict = {str(k): int(v) for k, v in determined_groups_dict.items()}
        
        info = {
            'total_records': len(df),
            'groups': groups_dict,
            'determined_groups': determined_groups_dict,
            'date_range': {
                'min': str(df['date'].min()) if 'date' in df.columns and not pd.isna(df['date'].min()) else None,
                'max': str(df['date'].max()) if 'date' in df.columns and not pd.isna(df['date'].max()) else None
            },
            'field_completeness': field_completeness
        }
        
        return info
    
    def _calculate_field_completeness(self, df: pd.DataFrame) -> Dict[str, float]:
        """
        Расчет процента заполненности полей
        
        Args:
            df: DataFrame с данными
            
        Returns:
            Словарь с процентами заполненности для каждого поля
        """
        # Поля для анализа заполненности (только исходные поля)
        # Поля latitude, longitude, district добавляются автоматически в процессе обработки
        # поэтому их заполненность не отражает качество исходных данных
        fields_to_check = ['review_text', 'rating', 'answer_text']
        
        # Дополнительно проверяем поля координат и районов, но с пометкой
        # что они могли быть добавлены автоматически
        auto_generated_fields = ['latitude', 'longitude', 'district']
        
        # Поля анализа, которые добавляются в процессе обработки
        analysis_fields = ['sentiment', 'sentiment_score', 'review_type', 'positive_words_count', 'negative_words_count']
        
        completeness = {}
        total_records = len(df)
        
        if total_records == 0:
            return {field: 0.0 for field in fields_to_check + auto_generated_fields + analysis_fields}
        
        # Проверяем исходные поля
        for field in fields_to_check:
            if field in df.columns:
                # Подсчитываем непустые значения
                non_empty_count = df[field].notna().sum()
                # Дополнительно проверяем, что значение не пустая строка
                if df[field].dtype == 'object':
                    non_empty_count = ((df[field].notna()) & (df[field] != '')).sum()
                
                percentage = (non_empty_count / total_records) * 100
                completeness[field] = round(percentage, 1)
            else:
                completeness[field] = 0.0
        
        # Проверяем автоматически генерируемые поля с пометкой
        for field in auto_generated_fields:
            if field in df.columns:
                non_empty_count = df[field].notna().sum()
                if df[field].dtype == 'object':
                    non_empty_count = ((df[field].notna()) & (df[field] != '')).sum()
                
                percentage = (non_empty_count / total_records) * 100
                # Добавляем пометку, что поле могло быть добавлено автоматически
                completeness[f"{field}_auto"] = round(percentage, 1)
            else:
                completeness[f"{field}_auto"] = 0.0
        
        # Проверяем поля анализа
        for field in analysis_fields:
            if field in df.columns:
                # Для числовых полей (sentiment_score, positive_words_count, negative_words_count)
                # считаем заполненными все записи, где есть значение (включая 0)
                if field in ['sentiment_score', 'positive_words_count', 'negative_words_count']:
                    # Проверяем, что поле не пустое (включая 0)
                    non_empty_count = df[field].notna().sum()
                else:
                    # Для текстовых полей (sentiment, review_type) проверяем непустые значения
                    non_empty_count = df[field].notna().sum()
                    if df[field].dtype == 'object':
                        non_empty_count = ((df[field].notna()) & (df[field] != '')).sum()
                
                percentage = (non_empty_count / total_records) * 100
                completeness[field] = round(percentage, 1)
            else:
                completeness[field] = 0.0
        
        return completeness
    
    def clear_archive(self) -> bool:
        """
        Очистка архивного файла
        
        Returns:
            True если очистка успешна
        """
        try:
            if os.path.exists(self.archive_file):
                os.remove(self.archive_file)
                logger.info("Архивный файл очищен")
            return True
        except Exception as e:
            logger.error(f"Ошибка очистки архива: {e}")
            return False 