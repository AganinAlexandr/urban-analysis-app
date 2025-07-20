"""
Модуль геокодирования для MVP приложения
"""

import os
import json
import pandas as pd
from typing import Dict, Optional, Tuple
import requests
import logging
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

class MoscowGeocoder:
    def __init__(self, api_key: Optional[str] = None):
        """
        Инициализация геокодера
        
        Args:
            api_key (str, optional): API ключ для Яндекс.Геокодера
        """
        # Загружаем переменные окружения из .env файла
        load_dotenv()
        
        # Получаем API ключ из параметра или переменной окружения
        self.api_key = api_key or os.getenv('YANDEX_GEOCODER_API_KEY')
        
        if not self.api_key:
            logger.warning("API ключ для Яндекс.Геокодера не найден. Геокодирование будет пропущено.")
            self.api_key = None
            
        self.cache = {}
        self.cache_file = 'data/results/geocoder_cache.json'
        
        # Загружаем кэш, если он существует
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    self.cache = json.load(f)
                logger.info(f"Загружен кэш геокодера: {len(self.cache)} записей")
            except Exception as e:
                logger.error(f"Ошибка загрузки кэша геокодера: {e}")
                
    def geocode_address(self, address: str) -> Tuple[float, float, str]:
        """
        Геокодирование адреса
        
        Args:
            address (str): Адрес для геокодирования
            
        Returns:
            Tuple[float, float, str]: Широта, долгота и район
        """
        if not self.api_key:
            return 0.0, 0.0, "Геокодирование недоступно"
            
        # Проверяем кэш
        if address in self.cache:
            return tuple(self.cache[address])
            
        try:
            # Запрос к API
            url = "https://geocode-maps.yandex.ru/1.x/"
            params = {
                "apikey": self.api_key,
                "format": "json",
                "geocode": f"Москва, {address}",
                "results": 1,
                "lang": "ru_RU"
            }
            
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            features = data["response"]["GeoObjectCollection"]["featureMember"]
            
            if not features:
                return 0.0, 0.0, "Неизвестный район"
                
            # Получаем координаты
            pos = features[0]["GeoObject"]["Point"]["pos"]
            lon, lat = map(float, pos.split())
            
            # Получаем компоненты адреса
            components = features[0]["GeoObject"]["metaDataProperty"]["GeocoderMetaData"]["Address"]["Components"]
            
            # Ищем район в компонентах адреса
            district = None
            for component in components:
                if component["kind"] == "district":
                    district = component["name"]
                    break
            
            # Если район не найден, пробуем получить его из полного адреса
            if not district:
                full_address = features[0]["GeoObject"]["metaDataProperty"]["GeocoderMetaData"]["text"]
                address_parts = full_address.split(",")
                
                # Ищем часть адреса, содержащую слово "район"
                for part in address_parts:
                    part = part.strip()
                    if "район" in part.lower():
                        district = part.replace("район", "").strip()
                        break
            
            # Если район все еще не найден, используем административный округ
            if not district:
                for component in components:
                    if component["kind"] == "area":
                        district = component["name"]
                        break
            
            if not district:
                district = "Неизвестный район"
            
            logger.info(f"Геокодирование: {address} -> {lat}, {lon}, {district}")
            
            # Сохраняем в кэш
            self.cache[address] = [lat, lon, district]
            
            return lat, lon, district
            
        except Exception as e:
            logger.error(f"Ошибка при геокодировании адреса {address}: {str(e)}")
            return 0.0, 0.0, "Ошибка геокодирования"
            
    def process_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Обработка DataFrame с адресами
        
        Args:
            df (pd.DataFrame): DataFrame с адресами
            
        Returns:
            pd.DataFrame: DataFrame с добавленными координатами и районами
        """
        if 'address' not in df.columns:
            logger.warning("Колонка 'address' не найдена в данных")
            return df
            
        # Проверяем наличие координат
        has_coordinates = 'latitude' in df.columns and 'longitude' in df.columns
        
        if not self.api_key:
            logger.warning("API ключ не настроен. Геокодирование пропущено.")
            return df
            
        if has_coordinates:
            # Проверяем, есть ли уже координаты
            coordinates_exist = df['latitude'].notna().any() and df['longitude'].notna().any()
            if coordinates_exist:
                logger.info("Координаты уже присутствуют в данных. Геокодирование пропущено.")
                return df
            else:
                logger.info("Колонки координат есть, но данные пустые. Выполняется геокодирование.")
        else:
            logger.info("Колонки координат отсутствуют. Добавляются новые колонки.")
            df['latitude'] = 0.0
            df['longitude'] = 0.0
            
        # Добавляем колонку для района
        df['district'] = "Неизвестный район"
        
        # Обрабатываем каждый уникальный адрес
        unique_addresses = df['address'].dropna().unique()
        logger.info(f"Геокодирование {len(unique_addresses)} уникальных адресов...")
        
        for i, address in enumerate(unique_addresses, 1):
            if not address or pd.isna(address):
                continue
                
            logger.info(f"Обработка адреса {i}/{len(unique_addresses)}: {address}")
            lat, lon, district = self.geocode_address(address)
            
            # Обновляем все строки с этим адресом
            mask = df['address'] == address
            df.loc[mask, 'latitude'] = lat
            df.loc[mask, 'longitude'] = lon
            df.loc[mask, 'district'] = district
            
        # Сохраняем кэш
        self.save_cache()
        
        return df
        
    def save_cache(self, filename: str = None):
        """
        Сохранение кэша
        
        Args:
            filename (str, optional): Имя файла для сохранения
        """
        if filename is None:
            filename = self.cache_file
            
        try:
            # Создаем директорию, если она не существует
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(self.cache, f, ensure_ascii=False, indent=2)
            logger.info(f"Кэш геокодера сохранен: {filename}")
        except Exception as e:
            logger.error(f"Ошибка сохранения кэша геокодера: {e}")
            
    def get_coordinates_status(self, df: pd.DataFrame) -> Dict:
        """
        Получение статуса координат в данных
        
        Args:
            df (pd.DataFrame): DataFrame для проверки
            
        Returns:
            Dict: Статус координат
        """
        status = {
            'has_latitude_column': 'latitude' in df.columns,
            'has_longitude_column': 'longitude' in df.columns,
            'coordinates_exist': False,
            'coordinates_count': 0,
            'total_records': len(df)
        }
        
        if status['has_latitude_column'] and status['has_longitude_column']:
            # Проверяем наличие координат
            lat_not_null = df['latitude'].notna()
            lon_not_null = df['longitude'].notna()
            valid_coords = lat_not_null & lon_not_null & (df['latitude'] != 0.0) & (df['longitude'] != 0.0)
            
            status['coordinates_exist'] = valid_coords.any()
            status['coordinates_count'] = valid_coords.sum()
            
        return status 