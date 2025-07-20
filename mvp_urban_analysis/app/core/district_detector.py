"""
Модуль для определения района (district) по координатам
"""

import pandas as pd
import logging
import time
from typing import Dict, Optional, Tuple
import requests
import json

logger = logging.getLogger(__name__)

class DistrictDetector:
    """Класс для определения района по координатам"""
    
    def __init__(self, api_key: str = None):
        """
        Инициализация детектора районов
        
        Args:
            api_key: API ключ для Яндекс.Геокодер
        """
        self.api_key = api_key
        self.base_url = "https://geocode-maps.yandex.ru/1.x/"
        self.cache = {}  # Простой кэш для результатов
        
    def get_district_from_coordinates(self, lat: float, lon: float) -> Optional[str]:
        """
        Определение района по координатам
        
        Args:
            lat: Широта
            lon: Долгота
            
        Returns:
            Название района или None
        """
        if lat == 0.0 and lon == 0.0:
            logger.warning(f"Координаты равны нулю: {lat}, {lon}")
            return None
            
        # Проверяем кэш
        cache_key = f"{lat:.6f}_{lon:.6f}"
        if cache_key in self.cache:
            logger.info(f"Найден в кэше район для координат {lat}, {lon}: {self.cache[cache_key]}")
            return self.cache[cache_key]
        
        try:
            # Формируем запрос к API
            params = {
                'format': 'json',
                'geocode': f"{lon},{lat}",
                'lang': 'ru_RU',
                'kind': 'district'
            }
            
            if self.api_key:
                params['apikey'] = self.api_key
                logger.info(f"API ключ установлен для геокодирования")
            else:
                logger.warning("API ключ не установлен для геокодирования")
                return None
            
            logger.info(f"Запрос к API для координат {lat}, {lon}")
            response = requests.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            # Извлекаем информацию о районе
            district = self._extract_district_from_response(data)
            
            # Сохраняем в кэш
            self.cache[cache_key] = district
            
            logger.info(f"Определен район для координат {lat}, {lon}: {district}")
            return district
            
        except Exception as e:
            logger.error(f"Ошибка определения района для координат {lat}, {lon}: {str(e)}")
            return None
    
    def _extract_district_from_response(self, data: Dict) -> Optional[str]:
        """
        Извлечение названия района из ответа API
        
        Args:
            data: Ответ от API
            
        Returns:
            Название района или None
        """
        try:
            features = data.get('response', {}).get('GeoObjectCollection', {}).get('featureMember', [])
            
            if not features:
                return None
            
            # Берем первый результат
            geo_object = features[0].get('GeoObject', {})
            
            # Ищем информацию о районе в компонентах адреса
            meta_data = geo_object.get('metaDataProperty', {}).get('GeocoderMetaData', {})
            address = meta_data.get('Address', {})
            components = address.get('Components', [])
            
            # Ищем компонент типа "район"
            for component in components:
                kind = component.get('kind', '')
                name = component.get('name', '')
                
                if kind in ['district', 'area'] and name:
                    return name
            
            # Если не нашли район, попробуем извлечь из полного адреса
            full_address = address.get('formatted', '')
            if full_address:
                # Простая логика извлечения района из адреса
                district = self._extract_district_from_address(full_address)
                if district:
                    return district
            
            return None
            
        except Exception as e:
            logger.error(f"Ошибка извлечения района из ответа API: {str(e)}")
            return None
    
    def _extract_district_from_address(self, address: str) -> Optional[str]:
        """
        Извлечение района из полного адреса
        
        Args:
            address: Полный адрес
            
        Returns:
            Название района или None
        """
        # Список возможных суффиксов районов
        district_suffixes = [
            'район', 'р-н', 'округ', 'округ', 'муниципальный округ',
            'административный округ', 'ао'
        ]
        
        # Разбиваем адрес на части
        parts = address.split(',')
        
        for part in parts:
            part = part.strip()
            for suffix in district_suffixes:
                if suffix in part.lower():
                    # Убираем суффикс и возвращаем название
                    district_name = part.replace(suffix, '').strip()
                    if district_name:
                        return district_name
        
        return None
    
    def process_dataframe_districts(self, df: pd.DataFrame, 
                                 batch_size: int = 10, 
                                 delay: float = 0.1) -> pd.DataFrame:
        """
        Обработка DataFrame для добавления информации о районах
        
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
        
        # Создаем копию DataFrame
        result_df = df.copy()
        
        # Проверяем наличие колонки district
        if 'district' not in result_df.columns:
            result_df['district'] = ""
        
        # Проверяем наличие полей координат
        has_coordinates = 'latitude' in result_df.columns and 'longitude' in result_df.columns
        
        if not has_coordinates:
            logger.info("Поля координат отсутствуют. Определение районов пропущено.")
            return result_df
        
        logger.info(f"Найдены поля координат: latitude={('latitude' in result_df.columns)}, longitude={('longitude' in result_df.columns)}")
        
        # Обрабатываем записи с координатами, но без района или с "Неизвестный район"
        mask = ((result_df['district'].isna() | (result_df['district'] == "") | 
                (result_df['district'] == "Неизвестный район")) & 
               (result_df['latitude'] != 0.0) & (result_df['longitude'] != 0.0))
        
        records_to_process = result_df[mask]
        
        if records_to_process.empty:
            logger.info("Нет записей для определения района")
            return result_df
        
        logger.info(f"Найдено {len(records_to_process)} записей для определения района")
        
        # Показываем несколько примеров координат
        sample_coords = records_to_process[['latitude', 'longitude']].head(3)
        logger.info(f"Примеры координат для обработки:\n{sample_coords}")
        
        # Обрабатываем пакетами
        for i in range(0, len(records_to_process), batch_size):
            batch = records_to_process.iloc[i:i+batch_size]
            
            for idx, row in batch.iterrows():
                lat = row['latitude']
                lon = row['longitude']
                
                logger.info(f"Обрабатываем запись {idx} с координатами {lat}, {lon}")
                district = self.get_district_from_coordinates(lat, lon)
                
                if district:
                    result_df.at[idx, 'district'] = district
                    logger.info(f"Определен район для записи {idx}: {district}")
                else:
                    logger.warning(f"Не удалось определить район для записи {idx}")
                
                # Задержка между запросами
                time.sleep(delay)
        
        logger.info(f"Обработано {len(records_to_process)} записей для определения района")
        return result_df
    
    def get_district_statistics(self, df: pd.DataFrame) -> Dict:
        """
        Получение статистики по районам
        
        Args:
            df: DataFrame с данными
            
        Returns:
            Словарь со статистикой
        """
        if df.empty or 'district' not in df.columns:
            return {}
        
        # Подсчитываем количество записей по районам
        district_counts = df['district'].value_counts()
        
        # Подсчитываем количество записей без района
        no_district_count = df['district'].isna().sum() + (df['district'] == "").sum()
        
        # Проверяем наличие полей координат
        has_coordinates = 'latitude' in df.columns and 'longitude' in df.columns
        
        # Подсчитываем количество записей с координатами, но без района
        if has_coordinates:
            with_coords_no_district = ((df['latitude'] != 0.0) & 
                                     (df['longitude'] != 0.0) & 
                                     (df['district'].isna() | (df['district'] == ""))).sum()
        else:
            with_coords_no_district = 0
        
        # Конвертируем числовые типы для JSON сериализации
        district_counts_raw = district_counts.to_dict()
        district_distribution = {str(k): int(v) for k, v in district_counts_raw.items()}
        
        return {
            'total_records': len(df),
            'districts_count': len(district_counts),
            'no_district_count': no_district_count,
            'with_coords_no_district': with_coords_no_district,
            'district_distribution': district_distribution
        } 