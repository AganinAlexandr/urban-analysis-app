import os
import json
import pandas as pd
from typing import Dict, Optional, Tuple
import requests
from dotenv import load_dotenv

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
            raise ValueError("Необходимо указать API ключ для Яндекс.Геокодера")
            
        self.cache = {}
        self.cache_file = 'results/geocoder_cache.json'
        
        # Загружаем кэш, если он существует
        if os.path.exists(self.cache_file):
            with open(self.cache_file, 'r', encoding='utf-8') as f:
                self.cache = json.load(f)
                
    def geocode_address(self, address: str) -> Tuple[float, float, str]:
        """
        Геокодирование адреса
        
        Args:
            address (str): Адрес для геокодирования
            
        Returns:
            Tuple[float, float, str]: Широта, долгота и район
        """
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
            
            print(f"Адрес: {address}")
            print(f"Полный адрес: {features[0]['GeoObject']['metaDataProperty']['GeocoderMetaData']['text']}")
            print(f"Компоненты адреса: {[comp['name'] for comp in components]}")
            print(f"Найденный район: {district}")
            print("-" * 50)
            
            # Сохраняем в кэш
            self.cache[address] = [lat, lon, district]
            
            return lat, lon, district
            
        except Exception as e:
            print(f"Ошибка при геокодировании адреса {address}: {str(e)}")
            return 0.0, 0.0, "Неизвестный район"
            
    def process_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Обработка DataFrame с адресами
        
        Args:
            df (pd.DataFrame): DataFrame с адресами
            
        Returns:
            pd.DataFrame: DataFrame с добавленными координатами и районами
        """
        # Добавляем колонки для координат и района
        df['latitude'] = 0.0
        df['longitude'] = 0.0
        df['district'] = "Неизвестный район"
        
        # Обрабатываем каждый уникальный адрес
        unique_addresses = df['address'].unique()
        print(f"\nГеокодирование {len(unique_addresses)} уникальных адресов...")
        
        for i, address in enumerate(unique_addresses, 1):
            if not address:
                continue
                
            print(f"Обработка адреса {i}/{len(unique_addresses)}: {address}")
            lat, lon, district = self.geocode_address(address)
            
            # Обновляем все строки с этим адресом
            mask = df['address'] == address
            df.loc[mask, 'latitude'] = lat
            df.loc[mask, 'longitude'] = lon
            df.loc[mask, 'district'] = district
            
        return df
        
    def save_cache(self, filename: str = None):
        """
        Сохранение кэша
        
        Args:
            filename (str, optional): Имя файла для сохранения
        """
        if filename is None:
            filename = self.cache_file
            
        # Создаем директорию, если она не существует
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.cache, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    # Пример использования
    from data_processor import DataProcessor
    
    # Загрузка данных
    processor = DataProcessor("parsed")
    processor.process_all_data()
    df = processor.get_dataframe()
    
    # Геокодирование
    geocoder = MoscowGeocoder()
    df_with_districts = geocoder.process_dataframe(df)
    
    # Сохранение результатов
    df_with_districts.to_csv('data_with_districts.csv', index=False, encoding='utf-8')
    
    # Сохранение кэша
    geocoder.save_cache() 