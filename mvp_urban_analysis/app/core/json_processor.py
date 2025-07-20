"""
Модуль для обработки JSON файлов с данными отзывов
"""

import json
import pandas as pd
import os
import logging
from typing import Dict, List, Optional, Union
from datetime import datetime
import glob

# Настройка логирования
logger = logging.getLogger(__name__)

class JSONProcessor:
    """Класс для обработки JSON файлов с данными отзывов"""
    
    def __init__(self):
        """Инициализация процессора JSON"""
        # Маппинг полей для различных вариантов названий
        self.field_mapping = {
            # Основные поля
            'name': ['name', 'founded.name', 'object_name'],
            'address': ['address', 'addr'],
            'review_text': ['review_text', 'text', 'comment'],
            'date': ['date', 'review_date', 'timestamp'],
            'user_name': ['user_name', 'name', 'author'],
            'rating': ['rating', 'star', 'stars', 'review_rating', 'star_rating'],
            'answer_text': ['answer_text', 'answer', 'reply', 'response'],
            'rating_object': ['rating_object', 'object_rating', 'average_rating'],
            'review_count_from_api': ['review_count_from_api', 'review_count'],
            'source': ['source', 'sourse'],  # Учитываем опечатку
            'group': ['group', 'category', 'type']
        }
        
        # Поддерживаемые группы объектов
        self.object_groups = [
            'resident_complexes', 'school', 'hospital', 'pharmacy',
            'kindergarden', 'polyclinic', 'university', 'shopmall'
        ]
        
        # Маппинг названий каталогов к группам объектов
        self.directory_group_mapping = {
            # 2gis
            'hospital_2gis': 'hospital',
            'kindergarten_parse': 'kindergarden',
            'shopmalls_parse': 'shopmall',
            
            # Yandex
            'hospital_yandex': 'hospital',
            'kindergarden_parse': 'kindergarden',
            'pharmacy_parse': 'pharmacy',
            'polyclinic_parse': 'polyclinic',
            'schools_parse': 'school',
            'shopmall_parse': 'shopmall',
            'university_parse': 'university',
            
            # Другие возможные варианты
            'school_parse': 'school',
            'hospital_parse': 'hospital',
            'pharmacy_parse': 'pharmacy',
            'kindergarden_parse': 'kindergarden',
            'polyclinic_parse': 'polyclinic',
            'university_parse': 'university',
            'shopmall_parse': 'shopmall',
            'resident_complex_parse': 'resident_complexes',
            
            # Поддержка структуры с подкаталогом json
            'json/hospital_yandex': 'hospital',
            'json/kindergarden_parse': 'kindergarden',
            'json/pharmacy_parse': 'pharmacy',
            'json/polyclinic_parse': 'polyclinic',
            'json/schools_parse': 'school',
            'json/shopmall_parse': 'shopmall',
            'json/university_parse': 'university',
            
            # Поддержка структуры с подкаталогом initial_data/json
            'initial_data/json/hospital_yandex': 'hospital',
            'initial_data/json/kindergarden_parse': 'kindergarden',
            'initial_data/json/pharmacy_parse': 'pharmacy',
            'initial_data/json/polyclinic_parse': 'polyclinic',
            'initial_data/json/schools_parse': 'school',
            'initial_data/json/shopmall_parse': 'shopmall',
            'initial_data/json/university_parse': 'university'
        }
    
    def _extract_group_from_path(self, file_path: str) -> str:
        """
        Извлечение группы объекта из пути к файлу
        
        Args:
            file_path: Путь к JSON файлу
            
        Returns:
            Название группы объекта
        """
        try:
            # Получаем абсолютный путь
            abs_path = os.path.abspath(file_path)
            
            # Разбиваем путь на части
            path_parts = abs_path.split(os.sep)
            
            # Создаем варианты путей для поиска
            path_variants = []
            
            # Добавляем полный путь
            path_variants.append(abs_path)
            
            # Добавляем части пути (начиная с конца)
            for i in range(len(path_parts) - 1, 0, -1):
                path_variants.append(os.sep.join(path_parts[i:]))
            
            # Добавляем отдельные части пути
            path_variants.extend(path_parts)
            
            logger.info(f"Поиск группы для файла: {file_path}")
            logger.info(f"Варианты путей для поиска: {path_variants}")
            
            # Ищем точное соответствие
            for path_variant in path_variants:
                if path_variant in self.directory_group_mapping:
                    group = self.directory_group_mapping[path_variant]
                    logger.info(f"Определена группа '{group}' из пути '{path_variant}' для файла {file_path}")
                    return group
            
            # Если не найдено точное соответствие, ищем частичное
            # Но только для путей, которые содержат известные паттерны каталогов
            for path_variant in path_variants:
                for dir_name, group in self.directory_group_mapping.items():
                    # Проверяем, что путь содержит полное название каталога
                    if dir_name in path_variant:
                        logger.info(f"Определена группа '{group}' из пути '{path_variant}' (частичное соответствие) для файла {file_path}")
                        return group
            
            # Если группа не найдена из пути, пробуем определить из имени файла
            filename = os.path.basename(file_path)
            logger.info(f"Пробуем определить группу из имени файла: {filename}")
            
            # Проверяем, содержит ли имя файла паттерны групп
            for dir_name, group in self.directory_group_mapping.items():
                if dir_name in filename:
                    logger.info(f"Определена группа '{group}' из имени файла '{filename}'")
                    return group
            
            logger.warning(f"Не удалось определить группу из пути {file_path}")
            return ''
            
        except Exception as e:
            logger.error(f"Ошибка при извлечении группы из пути {file_path}: {e}")
            return ''
    
    def _extract_group_from_json_content(self, data: Dict) -> str:
        """
        Извлечение группы объекта из содержимого JSON
        
        Args:
            data: Данные JSON
            
        Returns:
            Название группы объекта
        """
        try:
            # Проверяем, есть ли группа в company_info
            if isinstance(data, dict) and 'company_info' in data:
                company_info = data['company_info']
                if isinstance(company_info, dict) and 'group' in company_info:
                    group = company_info['group']
                    if group:
                        logger.info(f"Определена группа '{group}' из company_info")
                        return group
            
            # Анализируем название объекта для определения группы
            if isinstance(data, dict) and 'company_info' in data:
                company_info = data['company_info']
                if isinstance(company_info, dict) and 'name' in company_info:
                    name = company_info['name'].lower()
                    
                    # Ключевые слова для определения групп
                    group_keywords = {
                        'university': ['университет', 'институт', 'академия', 'вуз', 'ран'],
                        'school': ['школа', 'лицей', 'гимназия', 'образовательное учреждение'],
                        'hospital': ['больница', 'клиника', 'медицинский центр', 'дгкб', 'гкб', 'медицинская'],
                        'pharmacy': ['аптека', 'фармация', 'лекарства'],
                        'kindergarden': ['детский сад', 'сад', 'дошкольное', 'ясли'],
                        'polyclinic': ['поликлиника', 'амбулатория', 'медицинская консультация'],
                        'shopmall': ['торговый центр', 'молл', 'тц', 'галерея', 'торговый', 'магазин'],
                        'resident_complexes': ['жилой комплекс', 'жк', 'квартал', 'жилой дом']
                    }
                    
                    for group, keywords in group_keywords.items():
                        for keyword in keywords:
                            if keyword in name:
                                logger.info(f"Определена группа '{group}' по ключевому слову '{keyword}' в названии '{company_info['name']}'")
                                return group
            
            # Анализируем тексты отзывов для определения группы
            if isinstance(data, dict) and 'company_reviews' in data:
                company_reviews = data['company_reviews']
                if isinstance(company_reviews, list) and company_reviews:
                    # Анализируем первые 50 отзывов
                    reviews_to_analyze = company_reviews[:50]
                    all_texts = []
                    
                    for review in reviews_to_analyze:
                        if isinstance(review, dict):
                            # Добавляем текст отзыва
                            if 'text' in review:
                                all_texts.append(review['text'].lower())
                            # Добавляем название объекта из company_info
                            if 'company_info' in data and isinstance(data['company_info'], dict):
                                if 'name' in data['company_info']:
                                    all_texts.append(data['company_info']['name'].lower())
                    
                    # Объединяем все тексты
                    combined_text = ' '.join(all_texts)
                    
                    # Ключевые слова для определения групп из текстов
                    text_group_keywords = {
                        'university': ['университет', 'институт', 'академия', 'вуз', 'студент', 'преподаватель', 'лекция', 'сессия'],
                        'school': ['школа', 'лицей', 'гимназия', 'ученик', 'учитель', 'урок', 'класс'],
                        'hospital': ['больница', 'клиника', 'врач', 'медицинский', 'лечение', 'пациент', 'прием'],
                        'pharmacy': ['аптека', 'лекарство', 'препарат', 'фармацевт'],
                        'kindergarden': ['детский сад', 'воспитатель', 'ребенок', 'игра', 'группа'],
                        'polyclinic': ['поликлиника', 'врач', 'прием', 'консультация'],
                        'shopmall': ['торговый центр', 'магазин', 'покупка', 'товар', 'цены'],
                        'resident_complexes': ['жилой комплекс', 'дом', 'квартира', 'жилье']
                    }
                    
                    # Подсчитываем совпадения для каждой группы
                    group_scores = {}
                    for group, keywords in text_group_keywords.items():
                        score = 0
                        for keyword in keywords:
                            if keyword in combined_text:
                                score += 1
                        if score > 0:
                            group_scores[group] = score
                    
                    # Выбираем группу с наибольшим количеством совпадений
                    if group_scores:
                        best_group = max(group_scores, key=group_scores.get)
                        best_score = group_scores[best_group]
                        logger.info(f"Определена группа '{best_group}' по анализу текстов (счет: {best_score})")
                        return best_group
            
            logger.info("Не удалось определить группу из содержимого JSON")
            return ''
            
        except Exception as e:
            logger.error(f"Ошибка при извлечении группы из содержимого JSON: {e}")
            return ''
    
    def _determine_group_from_content(self, data: Dict) -> str:
        """
        Определение группы из содержимого JSON (названия объектов и тексты отзывов)
        
        Args:
            data: Данные JSON
            
        Returns:
            Определенная группа объекта
        """
        try:
            # Анализируем название объекта для определения группы
            if isinstance(data, dict) and 'company_info' in data:
                company_info = data['company_info']
                if isinstance(company_info, dict) and 'name' in company_info:
                    name = company_info['name'].lower()
                    
                    # Ключевые слова для определения групп
                    group_keywords = {
                        'university': ['университет', 'институт', 'академия', 'вуз', 'ран'],
                        'school': ['школа', 'лицей', 'гимназия', 'образовательное учреждение'],
                        'hospital': ['больница', 'клиника', 'медицинский центр', 'дгкб', 'гкб', 'медицинская'],
                        'pharmacy': ['аптека', 'фармация', 'лекарства'],
                        'kindergarden': ['детский сад', 'сад', 'дошкольное', 'ясли'],
                        'polyclinic': ['поликлиника', 'амбулатория', 'медицинская консультация'],
                        'shopmall': ['торговый центр', 'молл', 'тц', 'галерея', 'торговый', 'магазин'],
                        'resident_complexes': ['жилой комплекс', 'жк', 'квартал', 'жилой дом']
                    }
                    
                    for group, keywords in group_keywords.items():
                        for keyword in keywords:
                            if keyword in name:
                                logger.info(f"Определена группа '{group}' по ключевому слову '{keyword}' в названии '{company_info['name']}'")
                                return group
            
            # Анализируем тексты отзывов для определения группы
            if isinstance(data, dict) and 'company_reviews' in data:
                company_reviews = data['company_reviews']
                if isinstance(company_reviews, list) and company_reviews:
                    # Анализируем первые 50 отзывов
                    reviews_to_analyze = company_reviews[:50]
                    all_texts = []
                    
                    for review in reviews_to_analyze:
                        if isinstance(review, dict):
                            # Добавляем текст отзыва
                            if 'text' in review:
                                all_texts.append(review['text'].lower())
                            # Добавляем название объекта из company_info
                            if 'company_info' in data and isinstance(data['company_info'], dict):
                                if 'name' in data['company_info']:
                                    all_texts.append(data['company_info']['name'].lower())
                    
                    # Объединяем все тексты
                    combined_text = ' '.join(all_texts)
                    
                    # Ключевые слова для определения групп из текстов
                    text_group_keywords = {
                        'university': ['университет', 'институт', 'академия', 'вуз', 'студент', 'преподаватель', 'лекция', 'сессия'],
                        'school': ['школа', 'лицей', 'гимназия', 'ученик', 'учитель', 'урок', 'класс'],
                        'hospital': ['больница', 'клиника', 'врач', 'медицинский', 'лечение', 'пациент', 'прием'],
                        'pharmacy': ['аптека', 'лекарство', 'препарат', 'фармацевт'],
                        'kindergarden': ['детский сад', 'воспитатель', 'ребенок', 'игра', 'группа'],
                        'polyclinic': ['поликлиника', 'врач', 'прием', 'консультация'],
                        'shopmall': ['торговый центр', 'магазин', 'покупка', 'товар', 'цены'],
                        'resident_complexes': ['жилой комплекс', 'дом', 'квартира', 'жилье']
                    }
                    
                    # Подсчитываем совпадения для каждой группы
                    group_scores = {}
                    for group, keywords in text_group_keywords.items():
                        score = 0
                        for keyword in keywords:
                            if keyword in combined_text:
                                score += 1
                        if score > 0:
                            group_scores[group] = score
                    
                    # Выбираем группу с наибольшим количеством совпадений
                    if group_scores:
                        best_group = max(group_scores, key=group_scores.get)
                        best_score = group_scores[best_group]
                        logger.info(f"Определена группа '{best_group}' по анализу текстов (счет: {best_score})")
                        return best_group
            
            logger.info("Не удалось определить группу из содержимого JSON")
            return ''
            
        except Exception as e:
            logger.error(f"Ошибка при определении группы из содержимого JSON: {e}")
            return ''
    
    def process_json_file_or_directory(self, path: str) -> pd.DataFrame:
        """
        Обработка JSON файла или директории с JSON файлами
        
        Args:
            path: Путь к файлу или директории
            
        Returns:
            DataFrame с обработанными данными
        """
        try:
            if os.path.isfile(path):
                return self._process_single_json_file(path)
            elif os.path.isdir(path):
                return self._process_json_directory(path)
            else:
                logger.error(f"Путь не существует: {path}")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Ошибка обработки JSON: {e}")
            return pd.DataFrame()
    
    def _process_single_json_file(self, file_path: str) -> pd.DataFrame:
        """
        Обработка одного JSON файла
        
        Args:
            file_path: Путь к JSON файлу
            
        Returns:
            DataFrame с данными
        """
        try:
            logger.info(f"=== ОБРАБОТКА JSON ФАЙЛА ===")
            logger.info(f"Файл: {file_path}")
            
            # Определяем группу из пути
            group_from_path = self._extract_group_from_path(file_path)
            logger.info(f"Группа из пути: '{group_from_path}'")
            
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            logger.info(f"JSON загружен, тип данных: {type(data)}")
            
            # НЕ определяем группу автоматически из содержимого для поля group
            # Группа должна быть указана поставщиком данных или пользователем
            logger.info(f"Используем группу из пути: '{group_from_path}' (не определяем автоматически)")
            
            # Определяем структуру данных
            if isinstance(data, dict):
                logger.info(f"Ключи в JSON: {list(data.keys())}")
                if 'company_info' in data and 'company_reviews' in data:
                    # Структура с company_info и company_reviews
                    logger.info("Обрабатываем как company_structure")
                    return self._process_company_structure(data, group_from_path)
                else:
                    # Обычная структура с одним объектом
                    logger.info("Обрабатываем как single_object")
                    return self._process_single_object(data, group_from_path)
            elif isinstance(data, list):
                # Список объектов
                logger.info(f"Обрабатываем как object_list, количество объектов: {len(data)}")
                return self._process_object_list(data, group_from_path)
            else:
                logger.error(f"Неподдерживаемая структура JSON в файле {file_path}")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Ошибка чтения JSON файла {file_path}: {e}")
            return pd.DataFrame()
    
    def _process_company_structure(self, data: Dict, group_from_path: str = '') -> pd.DataFrame:
        """
        Обработка структуры с company_info и company_reviews
        
        Args:
            data: Данные с company_info и company_reviews
            group_from_path: Группа, определенная из пути к файлу
            
        Returns:
            DataFrame с развернутыми отзывами
        """
        logger.info(f"=== ОБРАБОТКА COMPANY_STRUCTURE ===")
        logger.info(f"Группа из пути: '{group_from_path}'")
        
        company_info = data.get('company_info', {})
        company_reviews = data.get('company_reviews', [])
        
        logger.info(f"Поля в company_info: {list(company_info.keys())}")
        logger.info(f"Количество отзывов: {len(company_reviews)}")
        
        if not company_reviews:
            logger.warning("Нет отзывов в company_reviews")
            return pd.DataFrame()
        
        # Извлекаем адрес из company_info
        address = company_info.get('address', '')
        
        # Используем группу из пути, если она не указана в данных
        group_in_data = company_info.get('group', '')
        logger.info(f"Группа в данных: '{group_in_data}'")
        
        # Группа от поставщика данных - оставляем пустой если не указана
        group = group_in_data or group_from_path
        logger.info(f"Финальная группа от поставщика: '{group}'")
        
        # Определяем группу из содержимого для поля determined_group
        determined_group = self._determine_group_from_content(data)
        logger.info(f"Определенная группа из содержимого: '{determined_group}'")
        
        # Обрабатываем каждый отзыв
        processed_reviews = []
        
        for i, review in enumerate(company_reviews):
            if not isinstance(review, dict):
                continue
                
            # Создаем запись с данными компании и отзыва
            record = {
                'name': company_info.get('name', ''),
                'address': address,
                'group': group,  # Группа от поставщика данных (может быть пустой)
                'determined_group': determined_group,  # Автоматически определяемая группа
                'rating_object': company_info.get('rating', ''),
                'review_count_from_api': company_info.get('review_count', ''),
                'source': company_info.get('source', 'json')
            }
            
            # Добавляем данные отзыва
            record.update({
                'review_text': review.get('text', ''),
                'date': self._convert_timestamp(review.get('date', '')),
                'user_name': review.get('name', ''),  # В JSON поле называется 'name'
                'rating': review.get('stars', ''),    # В JSON поле называется 'stars'
                'answer_text': review.get('answer', '')
            })
            
            processed_reviews.append(record)
            
            # Логируем первые 3 записи
            if i < 3:
                logger.info(f"  Запись {i+1}: Группа от поставщика='{record['group']}', Определенная группа='{record['determined_group']}', Название='{record['name']}'")
        
        df = pd.DataFrame(processed_reviews)
        logger.info(f"Обработано {len(df)} отзывов из company_structure")
        
        return self._standardize_dataframe(df)
    
    def _process_single_object(self, data: Dict, group_from_path: str = '') -> pd.DataFrame:
        """
        Обработка одного объекта
        
        Args:
            data: Данные объекта
            group_from_path: Группа, определенная из пути к файлу
            
        Returns:
            DataFrame с данными
        """
        # НЕ добавляем группу автоматически - она должна быть указана поставщиком
        # Если группа не указана в данных, оставляем поле пустым
        logger.info(f"Обрабатываем объект с группой: '{data.get('group', '')}'")
        
        # Создаем DataFrame из одного объекта
        df = pd.DataFrame([data])
        return self._standardize_dataframe(df)
    
    def _process_object_list(self, data: List, group_from_path: str = '') -> pd.DataFrame:
        """
        Обработка списка объектов
        
        Args:
            data: Список объектов
            group_from_path: Группа, определенная из пути к файлу
            
        Returns:
            DataFrame с данными
        """
        # НЕ добавляем группу автоматически - она должна быть указана поставщиком
        logger.info(f"Обрабатываем список из {len(data)} объектов")
        
        # Создаем DataFrame из списка
        df = pd.DataFrame(data)
        return self._standardize_dataframe(df)
    
    def _process_json_directory(self, dir_path: str) -> pd.DataFrame:
        """
        Обработка директории с JSON файлами
        
        Args:
            dir_path: Путь к директории
            
        Returns:
            DataFrame с объединенными данными
        """
        logger.info(f"Обрабатываем директорию: {dir_path}")
        
        # Ищем все JSON файлы
        json_files = glob.glob(os.path.join(dir_path, "**/*.json"), recursive=True)
        
        if not json_files:
            logger.warning(f"JSON файлы не найдены в директории: {dir_path}")
            return pd.DataFrame()
        
        logger.info(f"Найдено {len(json_files)} JSON файлов")
        
        # Обрабатываем каждый файл
        all_dataframes = []
        
        for file_path in json_files:
            df = self._process_single_json_file(file_path)
            if not df.empty:
                all_dataframes.append(df)
        
        if not all_dataframes:
            logger.warning("Не удалось обработать ни одного JSON файла")
            return pd.DataFrame()
        
        # Объединяем все DataFrame
        result_df = pd.concat(all_dataframes, ignore_index=True)
        logger.info(f"Объединено {len(result_df)} записей из {len(all_dataframes)} файлов")
        
        return result_df
    
    def _convert_timestamp(self, timestamp: Union[str, int, float]) -> str:
        """
        Конвертация timestamp в строку даты
        
        Args:
            timestamp: Timestamp (может быть строкой, int или float)
            
        Returns:
            Строка с датой
        """
        if not timestamp:
            return ''
        
        try:
            # Если это Unix timestamp
            if isinstance(timestamp, (int, float)):
                dt = datetime.fromtimestamp(timestamp)
                return dt.strftime('%Y-%m-%d')
            elif isinstance(timestamp, str):
                # Пытаемся распарсить как timestamp
                try:
                    ts = float(timestamp)
                    dt = datetime.fromtimestamp(ts)
                    return dt.strftime('%Y-%m-%d')
                except ValueError:
                    # Возможно, это уже строка с датой
                    return timestamp
            else:
                return str(timestamp)
        except Exception as e:
            logger.warning(f"Ошибка конвертации timestamp {timestamp}: {e}")
            return str(timestamp)
    
    def _standardize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Стандартизация DataFrame
        
        Args:
            df: Исходный DataFrame
            
        Returns:
            Стандартизированный DataFrame
        """
        if df.empty:
            return df
        
        logger.info(f"Исходные колонки: {list(df.columns)}")
        
        # Переименовываем поля согласно маппингу
        for standard_name, variants in self.field_mapping.items():
            for variant in variants:
                if variant in df.columns:
                    if variant != standard_name:
                        df = df.rename(columns={variant: standard_name})
                        logger.info(f"Переименовано поле: {variant} -> {standard_name}")
                    break
        
        # Добавляем недостающие колонки
        for field in ['group', 'determined_group', 'name', 'address', 'review_text', 'date', 'user_name', 
                     'rating', 'answer_text', 'rating_object', 'review_count_from_api', 'source']:
            if field not in df.columns:
                df[field] = ''
        
        # Убираем лишние колонки, оставляем только поддерживаемые
        supported_columns = ['group', 'determined_group', 'name', 'address', 'review_text', 'date', 'user_name',
                           'rating', 'answer_text', 'rating_object', 'review_count_from_api', 'source']
        
        # Оставляем только поддерживаемые колонки
        existing_columns = [col for col in supported_columns if col in df.columns]
        df = df[existing_columns]
        
        # Добавляем геокодирование для получения координат
        if 'address' in df.columns and not df.empty:
            logger.info("Добавляем геокодирование для JSON данных...")
            try:
                from .geocoder import MoscowGeocoder
                geocoder = MoscowGeocoder()
                df = geocoder.process_dataframe(df)
                logger.info("Геокодирование завершено")
            except Exception as e:
                logger.error(f"Ошибка геокодирования: {e}")
                # Добавляем пустые координаты в случае ошибки
                df['latitude'] = 0.0
                df['longitude'] = 0.0
                df['district'] = "Неизвестный район"
        else:
            # Добавляем пустые координаты если нет адресов
            df['latitude'] = 0.0
            df['longitude'] = 0.0
            df['district'] = "Неизвестный район"
        
        logger.info(f"Поддерживаемые поля: {list(df.columns)}")
        logger.info(f"Обработан JSON файл: {len(df)} строк")
        
        return df
    
    def validate_dataframe(self, df: pd.DataFrame) -> Dict:
        """
        Валидация DataFrame
        
        Args:
            df: DataFrame для валидации
            
        Returns:
            Словарь с результатами валидации
        """
        if df.empty:
            return {
                'valid': False,
                'valid_records': 0,
                'invalid_records': 0,
                'valid_for_archive': 0,
                'addressless_records': 0,
                'missing_processing_fields': [],
                'missing_archive_fields': [],
                'total_records': 0,
                'needs_group_selection': False
            }
        
        total_records = len(df)
        
        # Проверяем обязательные поля для обработки
        required_processing = ['group', 'review_text']
        missing_processing = [field for field in required_processing if field not in df.columns]
        
        # Проверяем обязательные поля для архива
        required_archive = ['group', 'name', 'address', 'review_text', 'date']
        missing_archive = [field for field in required_archive if field not in df.columns]
        
        # Проверяем, нужно ли запросить группу
        needs_group_selection = False
        if 'group' in df.columns:
            # Проверяем, есть ли записи с пустой группой
            empty_group_mask = df['group'].isna() | (df['group'] == '')
            if empty_group_mask.any():
                needs_group_selection = True
                logger.warning(f"Найдено {empty_group_mask.sum()} записей с пустой группой")
        
        # Подсчитываем валидные записи
        if missing_processing:
            valid_records = 0
            invalid_records = total_records
        else:
            # Проверяем наличие данных в обязательных полях
            valid_mask = df[required_processing].notna().all(axis=1)
            valid_records = valid_mask.sum()
            invalid_records = total_records - valid_records
        
        # Проверяем записи с адресами для архива
        if 'address' in df.columns:
            address_mask = df['address'].notna() & (df['address'] != '')
            valid_for_archive = (valid_mask & address_mask).sum()
            addressless_records = valid_records - valid_for_archive
        else:
            valid_for_archive = 0
            addressless_records = valid_records
        
        result = {
            'valid': valid_records > 0,
            'valid_records': valid_records,
            'invalid_records': invalid_records,
            'valid_for_archive': valid_for_archive,
            'addressless_records': addressless_records,
            'missing_processing_fields': missing_processing,
            'missing_archive_fields': missing_archive,
            'total_records': total_records,
            'needs_group_selection': needs_group_selection
        }
        
        logger.info(f"Результаты валидации JSON: {result}")
        return result 