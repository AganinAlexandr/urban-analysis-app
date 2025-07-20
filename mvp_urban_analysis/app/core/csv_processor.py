"""
Модуль для обработки CSV файлов с кавычками и произвольным порядком полей
"""

import pandas as pd
import re
import logging
from typing import Dict, List, Optional
from io import StringIO

logger = logging.getLogger(__name__)

class CSVProcessor:
    """Класс для обработки CSV файлов с кавычками и произвольным порядком полей"""
    
    def __init__(self):
        """Инициализация процессора CSV"""
        # Определяем обязательные поля для обработки
        self.required_fields_processing = ['group', 'review_text']
        
        # Определяем обязательные поля для сохранения в архив
        self.required_fields_archive = ['group', 'name', 'address', 'review_text', 'date']
        
        # Определяем опциональные поля
        self.optional_fields = [
            'determined_group', 'user_name', 'rating', 'answer_text', 'latitude', 'longitude', 'district',
            'rating_object', 'review_count_from_api', 'review_count_fetched'
        ]
        
        # Маппинг полей для совместимости с разными форматами
        self.field_mapping = {
            # Ваши поля -> стандартные поля
            'object_name': 'name',
            'review_rating': 'rating',
            'review_date': 'date',
            'object_rating': 'rating_object',
            'review_count': 'review_count_from_api',
            'source': 'source'  # Дополнительное поле
        }
        
        # Все поддерживаемые поля
        self.supported_fields = self.required_fields_archive + self.optional_fields + ['source']
        
        # Стандартный порядок полей в архиве
        self.archive_field_order = [
            'group', 'determined_group', 'name', 'address', 'review_text', 'date', 'user_name',
            'rating', 'answer_text', 'latitude', 'longitude', 'district',
            'rating_object', 'review_count_from_api', 'review_count_fetched',
            'source', 'sentiment', 'sentiment_score', 'review_type', 'positive_words_count',
            'negative_words_count', 'hash_key'
        ]
    
    def clean_text_field(self, text: str) -> str:
        """
        Очистка текстового поля от проблемных символов
        
        Args:
            text: Исходный текст
            
        Returns:
            Очищенный текст
        """
        if pd.isna(text) or text is None:
            return ""
        
        text = str(text)
        
        # Удаляем лишние пробелы в начале и конце
        text = text.strip()
        
        # Заменяем множественные пробелы на один
        text = re.sub(r'\s+', ' ', text)
        
        # Заменяем проблемные символы
        replacements = {
            '\n': ' ',      # Перевод строки
            '\r': ' ',      # Возврат каретки
            '\t': ' ',      # Табуляция
            '"': '"',       # Двойные кавычки (оставляем как есть)
            "'": "'",       # Одинарные кавычки (оставляем как есть)
            ';': ',',       # Точка с запятой
            '–': '-',       # Длинное тире
            '—': '-',       # Короткое тире
            '…': '...',     # Многоточие
        }
        
        for old_char, new_char in replacements.items():
            text = text.replace(old_char, new_char)
        
        return text
    
    def _try_read_csv(self, file_path: str) -> pd.DataFrame:
        """
        Попытка чтения CSV файла разными методами
        
        Args:
            file_path: Путь к CSV файлу
            
        Returns:
            DataFrame с данными
        """
        methods = [
            # Метод 1: Стандартный с кавычками
            {
                'encoding': 'utf-8-sig',
                'quotechar': '"',
                'escapechar': '\\',
                'sep': ',',
                'skipinitialspace': True,
                'on_bad_lines': 'skip'
            },
            # Метод 2: Без кавычек
            {
                'encoding': 'utf-8-sig',
                'quoting': 3,  # QUOTE_NONE
                'sep': ',',
                'skipinitialspace': True,
                'on_bad_lines': 'skip',
                'engine': 'python'
            },
            # Метод 3: С Python парсером
            {
                'encoding': 'utf-8-sig',
                'quotechar': '"',
                'sep': ',',
                'skipinitialspace': True,
                'on_bad_lines': 'skip',
                'engine': 'python'
            },
            # Метод 4: С C парсером
            {
                'encoding': 'utf-8-sig',
                'quotechar': '"',
                'sep': ',',
                'skipinitialspace': True,
                'on_bad_lines': 'skip',
                'engine': 'c'
            }
        ]
        
        for i, method in enumerate(methods):
            try:
                logger.info(f"Попытка чтения CSV методом {i+1}")
                df = pd.read_csv(file_path, **method)
                
                # Проверяем, что получили разумное количество колонок
                if len(df.columns) >= 5:  # Минимум 5 колонок
                    logger.info(f"Успешно прочитано методом {i+1}: {len(df)} строк, {len(df.columns)} колонок")
                    return df
                else:
                    logger.warning(f"Метод {i+1} дал слишком мало колонок: {len(df.columns)}")
                    
            except Exception as e:
                logger.warning(f"Метод {i+1} не сработал: {str(e)}")
                continue
        
        # Если все методы не сработали, пробуем ручной парсинг
        logger.info("Пробуем ручной парсинг...")
        return self._manual_csv_parse(file_path)
    
    def _manual_csv_parse(self, file_path: str) -> pd.DataFrame:
        """
        Блочный парсер для сложных CSV файлов с многострочными полями review_text и answer_text
        Args:
            file_path: Путь к CSV файлу
        Returns:
            DataFrame с данными
        """
        try:
            with open(file_path, 'r', encoding='utf-8-sig') as f:
                lines = f.readlines()
            if len(lines) < 2:
                return pd.DataFrame()
            header_line = lines[0].strip()
            headers = [h.strip() for h in header_line.split(',')]
            num_fields = len(headers)
            # Индексы проблемных полей
            review_text_idx = None
            answer_text_idx = None
            for i, h in enumerate(headers):
                if h == 'review_text':
                    review_text_idx = i
                if h == 'answer_text':
                    answer_text_idx = i
            # Собираем блоки
            data = []
            buffer = ''
            field_count = 0
            for line in lines[1:]:
                if not line.strip():
                    continue
                if not buffer:
                    buffer = line.rstrip('\n')
                else:
                    buffer += '\n' + line.rstrip('\n')
                # Считаем запятые вне кавычек
                cnt = 0
                in_quotes = False
                for c in buffer:
                    if c == '"':
                        in_quotes = not in_quotes
                    elif c == ',' and not in_quotes:
                        cnt += 1
                if cnt == num_fields - 1:
                    # Похоже, что запись закончилась
                    row = self._parse_multiline_csv_line(buffer, headers, 0)
                    if row:
                        data.append(row)
                    buffer = ''
            # Последний буфер
            if buffer:
                row = self._parse_multiline_csv_line(buffer, headers, 0)
                if row:
                    data.append(row)
            if data:
                df = pd.DataFrame(data, columns=headers)
                logger.info(f"Блочный парсер: {len(df)} строк, {len(df.columns)} колонок")
                return df
        except Exception as e:
            logger.error(f"Ошибка блочного парсера: {str(e)}")
        return pd.DataFrame()
    
    def _parse_multiline_csv_line(self, line: str, headers: list, line_num: int) -> list:
        """
        Парсинг строки CSV с многострочными полями
        
        Args:
            line: Строка для парсинга
            headers: Заголовки колонок
            line_num: Номер строки
            
        Returns:
            Список значений для строки
        """
        # Используем регулярные выражения для парсинга CSV с кавычками
        import re
        
        # Паттерн для парсинга CSV с кавычками
        # Ищем поля в кавычках или без кавычек
        pattern = r'"([^"]*(?:""[^"]*)*)"|([^,]+)'
        
        matches = re.findall(pattern, line)
        
        if len(matches) < len(headers):
            logger.warning(f"Строка {line_num}: недостаточно полей ({len(matches)} < {len(headers)})")
            return None
        
        result = []
        for i, (quoted, unquoted) in enumerate(matches):
            if i >= len(headers):
                break
                
            if quoted:
                # Поле в кавычках - убираем экранированные кавычки
                value = quoted.replace('""', '"')
            else:
                # Поле без кавычек
                value = unquoted.strip()
            
            result.append(value)
        
        # Дополняем недостающие поля
        while len(result) < len(headers):
            result.append("")
        
        return result
    
    def _parse_csv_line_with_problematic_fields(self, line: str, headers: list, 
                                             review_text_idx: int, answer_text_idx: int, 
                                             line_num: int) -> list:
        """
        Парсинг строки CSV с учетом проблемных полей
        
        Args:
            line: Строка для парсинга
            headers: Заголовки колонок
            review_text_idx: Индекс поля review_text
            answer_text_idx: Индекс поля answer_text
            line_num: Номер строки для логирования
            
        Returns:
            Список значений для строки
        """
        # Разбиваем строку на части по запятой
        parts = line.split(',')
        
        if len(parts) < len(headers):
            logger.warning(f"Строка {line_num}: недостаточно частей ({len(parts)} < {len(headers)})")
            return None
        
        result = []
        i = 0  # Индекс в parts
        
        for col_idx, header in enumerate(headers):
            if col_idx == review_text_idx and review_text_idx is not None:
                # Обрабатываем проблемное поле review_text
                review_text = self._extract_problematic_field(parts, i, line_num, "review_text")
                if review_text is None:
                    return None
                result.append(review_text)
                i += 1  # Пропускаем обработанные части
                
            elif col_idx == answer_text_idx and answer_text_idx is not None:
                # Обрабатываем проблемное поле answer_text
                answer_text = self._extract_problematic_field(parts, i, line_num, "answer_text")
                if answer_text is None:
                    return None
                result.append(answer_text)
                i += 1  # Пропускаем обработанные части
                
            else:
                # Обычное поле - берем как есть
                if i < len(parts):
                    result.append(parts[i])
                    i += 1
                else:
                    result.append("")
        
        return result
    
    def _extract_problematic_field(self, parts: list, start_idx: int, line_num: int, field_name: str) -> str:
        """
        Извлечение проблемного поля (review_text или answer_text)
        
        Args:
            parts: Части строки
            start_idx: Начальный индекс
            line_num: Номер строки
            field_name: Название поля
            
        Returns:
            Извлеченное значение поля
        """
        if start_idx >= len(parts):
            return ""
        
        # Ищем начало проблемного поля (обычно в кавычках)
        current_part = parts[start_idx]
        
        # Если поле начинается с кавычки
        if current_part.startswith('"'):
            # Ищем закрывающую кавычку
            field_content = current_part[1:]  # Убираем открывающую кавычку
            
            # Ищем закрывающую кавычку в текущей части
            if '"' in field_content:
                # Кавычка в той же части
                end_quote_idx = field_content.find('"')
                field_content = field_content[:end_quote_idx]
                return field_content
            
            # Кавычка в следующих частях
            for i in range(start_idx + 1, len(parts)):
                if '"' in parts[i]:
                    # Нашли закрывающую кавычку
                    end_quote_idx = parts[i].find('"')
                    field_content += ',' + parts[i][:end_quote_idx]
                    return field_content
                else:
                    field_content += ',' + parts[i]
            
            # Не нашли закрывающую кавычку - берем до конца
            return field_content
        
        else:
            # Поле без кавычек - берем как есть
            return current_part
    
    def process_csv_file(self, file_path: str) -> pd.DataFrame:
        """
        Обработка CSV файла с кавычками и произвольным порядком полей
        
        Args:
            file_path: Путь к CSV файлу
            
        Returns:
            DataFrame с обработанными данными
        """
        try:
            # Пробуем разные методы чтения CSV
            df = self._try_read_csv(file_path)
            
            if df.empty:
                logger.error(f"Не удалось прочитать файл {file_path}")
                return df
            
            logger.info(f"Загружен CSV файл: {len(df)} строк, {len(df.columns)} колонок")
            logger.info(f"Найденные колонки: {list(df.columns)}")
            
            # Применяем маппинг полей
            df_renamed = df.copy()
            for old_name, new_name in self.field_mapping.items():
                if old_name in df_renamed.columns:
                    df_renamed = df_renamed.rename(columns={old_name: new_name})
                    logger.info(f"Переименовано поле: {old_name} -> {new_name}")
            
            # Фильтруем только поддерживаемые поля
            available_fields = [col for col in df_renamed.columns if col in self.supported_fields]
            logger.info(f"Поддерживаемые поля: {available_fields}")
            
            if not available_fields:
                logger.error("Не найдено ни одного поддерживаемого поля")
                return pd.DataFrame()
            
            # Оставляем только поддерживаемые поля
            df = df_renamed[available_fields]
            
            # Очищаем текстовые поля
            text_fields = ['review_text', 'answer_text', 'name', 'address', 'user_name']
            for field in text_fields:
                if field in df.columns:
                    df[field] = df[field].apply(self.clean_text_field)
            
            # Проверяем обязательные поля для обработки
            missing_required = [field for field in self.required_fields_processing 
                              if field not in df.columns]
            
            if missing_required:
                logger.error(f"Отсутствуют обязательные поля: {missing_required}")
                return pd.DataFrame()
            
            # Добавляем недостающие поля с пустыми значениями
            for field in self.supported_fields:
                if field not in df.columns:
                    df[field] = ""
            
            # Приводим к стандартному порядку полей
            df = df.reindex(columns=self.supported_fields)
            
            logger.info(f"Обработан CSV файл: {len(df)} строк")
            return df
            
        except Exception as e:
            logger.error(f"Ошибка обработки CSV файла {file_path}: {str(e)}")
            return pd.DataFrame()
    
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
                'error': 'DataFrame пустой',
                'valid_records': 0,
                'invalid_records': 0,
                'missing_fields': []
            }
        
        # Проверяем обязательные поля для обработки
        missing_processing = [field for field in self.required_fields_processing 
                            if field not in df.columns]
        
        # Проверяем обязательные поля для архива
        missing_archive = [field for field in self.required_fields_archive 
                          if field not in df.columns]
        
        # Проверяем наличие данных в обязательных полях
        valid_mask = df[self.required_fields_processing].notna().all(axis=1)
        valid_records = df[valid_mask]
        invalid_records = df[~valid_mask]
        
        # Проверяем адреса для архива
        archive_mask = valid_records['address'].notna() & (valid_records['address'] != '')
        valid_for_archive = valid_records[archive_mask]
        addressless = valid_records[~archive_mask]
        
        result = {
            'valid': len(missing_processing) == 0 and len(valid_records) > 0,
            'valid_records': len(valid_records),
            'invalid_records': len(invalid_records),
            'valid_for_archive': len(valid_for_archive),
            'addressless_records': len(addressless),
            'missing_processing_fields': missing_processing,
            'missing_archive_fields': missing_archive,
            'total_records': len(df)
        }
        
        return result
    
    def get_field_mapping(self, df: pd.DataFrame) -> Dict[str, str]:
        """
        Получение соответствия полей в CSV и стандартных полей
        
        Args:
            df: DataFrame с данными
            
        Returns:
            Словарь соответствия полей
        """
        mapping = {}
        for col in df.columns:
            if col in self.supported_fields:
                mapping[col] = col
            else:
                # Попробуем найти похожее поле
                for supported in self.supported_fields:
                    if col.lower() in supported.lower() or supported.lower() in col.lower():
                        mapping[col] = supported
                        break
        
        return mapping
    
    def convert_to_archive_format(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Приведение DataFrame к формату архива
        
        Args:
            df: Исходный DataFrame
            
        Returns:
            DataFrame в формате архива
        """
        # Создаем копию с нужными полями
        archive_df = pd.DataFrame()
        
        for field in self.archive_field_order:
            if field in df.columns:
                archive_df[field] = df[field]
            else:
                # Добавляем пустое поле с правильными типами данных
                if field in ['sentiment_score', 'positive_words_count', 'negative_words_count']:
                    archive_df[field] = 0.0
                elif field == 'sentiment':
                    archive_df[field] = 'neutral'
                elif field == 'review_type':
                    archive_df[field] = 'информационный'
                else:
                    archive_df[field] = ""
        
        return archive_df 