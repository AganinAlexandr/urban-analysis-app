"""
Модуль для улучшенной валидации данных
"""

import pandas as pd
import logging
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime
import re

logger = logging.getLogger(__name__)

class DataValidator:
    """
    Класс для валидации данных перед обработкой
    """
    
    def __init__(self):
        """Инициализация валидатора данных"""
        self.required_fields = ['name', 'address', 'review_text']
        self.optional_fields = ['rating', 'date', 'user_name', 'source']
        self.allowed_groups = [
            'hospital', 'school', 'pharmacy', 'kindergarden', 
            'polyclinic', 'university', 'shopmall', 'resident_complexes'
        ]
        
    def validate_dataframe(self, df: pd.DataFrame, file_path: str = "") -> Dict[str, Any]:
        """
        Валидация DataFrame с данными
        
        Args:
            df: DataFrame для валидации
            file_path: Путь к файлу (для логирования)
            
        Returns:
            Словарь с результатами валидации
        """
        logger.info(f"Начинаем валидацию данных из файла: {file_path}")
        
        validation_result = {
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'stats': {
                'total_rows': len(df),
                'valid_rows': 0,
                'invalid_rows': 0,
                'missing_required_fields': 0,
                'empty_reviews': 0,
                'invalid_ratings': 0,
                'duplicate_objects': 0
            }
        }
        
        # Проверяем наличие обязательных полей
        missing_fields = self._check_required_fields(df)
        if missing_fields:
            validation_result['errors'].append(f"Отсутствуют обязательные поля: {', '.join(missing_fields)}")
            validation_result['is_valid'] = False
        
        # Валидируем каждую строку
        for index, row in df.iterrows():
            row_validation = self._validate_row(row, index)
            
            if not row_validation['is_valid']:
                validation_result['stats']['invalid_rows'] += 1
                validation_result['errors'].extend(row_validation['errors'])
            else:
                validation_result['stats']['valid_rows'] += 1
            
            validation_result['warnings'].extend(row_validation['warnings'])
        
        # Проверяем дубликаты объектов
        duplicates = self._check_duplicate_objects(df)
        if duplicates:
            validation_result['stats']['duplicate_objects'] = len(duplicates)
            validation_result['warnings'].append(f"Найдено {len(duplicates)} дублирующихся объектов")
        
        # Проверяем общую статистику
        if validation_result['stats']['valid_rows'] == 0:
            validation_result['errors'].append("Нет валидных строк в данных")
            validation_result['is_valid'] = False
        
        logger.info(f"Валидация завершена: {validation_result['stats']['valid_rows']}/{validation_result['stats']['total_rows']} валидных строк")
        
        return validation_result
    
    def _check_required_fields(self, df: pd.DataFrame) -> List[str]:
        """
        Проверка наличия обязательных полей
        
        Args:
            df: DataFrame для проверки
            
        Returns:
            Список отсутствующих полей
        """
        missing_fields = []
        
        for field in self.required_fields:
            if field not in df.columns:
                missing_fields.append(field)
        
        return missing_fields
    
    def _validate_row(self, row: pd.Series, index: int) -> Dict[str, Any]:
        """
        Валидация отдельной строки данных
        
        Args:
            row: Строка данных
            index: Индекс строки
            
        Returns:
            Результат валидации строки
        """
        row_validation = {
            'is_valid': True,
            'errors': [],
            'warnings': []
        }
        
        # Проверяем обязательные поля
        for field in self.required_fields:
            if field not in row.index:
                row_validation['errors'].append(f"Строка {index}: отсутствует поле '{field}'")
                row_validation['is_valid'] = False
                continue
            
            value = row[field]
            
            # Проверяем на пустые значения
            if pd.isna(value) or str(value).strip() == '':
                row_validation['errors'].append(f"Строка {index}: пустое значение в поле '{field}'")
                row_validation['is_valid'] = False
        
        # Проверяем рейтинг
        if 'rating' in row.index and not pd.isna(row['rating']):
            try:
                rating = float(row['rating'])
                if not (1 <= rating <= 5):
                    row_validation['warnings'].append(f"Строка {index}: некорректный рейтинг {rating} (должен быть 1-5)")
            except (ValueError, TypeError):
                row_validation['warnings'].append(f"Строка {index}: некорректный формат рейтинга '{row['rating']}'")
        
        # Проверяем дату
        if 'date' in row.index and not pd.isna(row['date']):
            if not self._is_valid_date(row['date']):
                row_validation['warnings'].append(f"Строка {index}: некорректный формат даты '{row['date']}'")
        
        # Проверяем длину текста отзыва
        if 'review_text' in row.index and not pd.isna(row['review_text']):
            review_text = str(row['review_text']).strip()
            if len(review_text) < 10:
                row_validation['warnings'].append(f"Строка {index}: очень короткий отзыв ({len(review_text)} символов)")
            elif len(review_text) > 5000:
                row_validation['warnings'].append(f"Строка {index}: очень длинный отзыв ({len(review_text)} символов)")
        
        return row_validation
    
    def _is_valid_date(self, date_value: Any) -> bool:
        """
        Проверка корректности даты
        
        Args:
            date_value: Значение даты для проверки
            
        Returns:
            True если дата корректна
        """
        if pd.isna(date_value):
            return True
        
        try:
            if isinstance(date_value, str):
                # Пробуем различные форматы даты
                formats = ['%Y-%m-%d', '%d.%m.%Y', '%d/%m/%Y', '%Y-%m-%d %H:%M:%S']
                for fmt in formats:
                    try:
                        datetime.strptime(date_value, fmt)
                        return True
                    except ValueError:
                        continue
                return False
            elif isinstance(date_value, (datetime, pd.Timestamp)):
                return True
            else:
                return False
        except Exception:
            return False
    
    def _check_duplicate_objects(self, df: pd.DataFrame) -> List[Dict]:
        """
        Проверка дублирующихся объектов
        
        Args:
            df: DataFrame для проверки
            
        Returns:
            Список дублирующихся объектов
        """
        duplicates = []
        
        if 'name' in df.columns and 'address' in df.columns:
            # Создаем ключ для поиска дубликатов
            df_copy = df.copy()
            df_copy['object_key'] = df_copy['name'].astype(str) + '|' + df_copy['address'].astype(str)
            
            # Находим дубликаты
            duplicate_groups = df_copy[df_copy.duplicated(subset=['object_key'], keep=False)]
            
            if not duplicate_groups.empty:
                for key in duplicate_groups['object_key'].unique():
                    duplicate_rows = duplicate_groups[duplicate_groups['object_key'] == key]
                    duplicates.append({
                        'name': duplicate_rows.iloc[0]['name'],
                        'address': duplicate_rows.iloc[0]['address'],
                        'count': len(duplicate_rows),
                        'indices': duplicate_rows.index.tolist()
                    })
        
        return duplicates
    
    def validate_group(self, group_name: str) -> bool:
        """
        Валидация названия группы
        
        Args:
            group_name: Название группы для проверки
            
        Returns:
            True если группа допустима
        """
        return group_name.lower() in [g.lower() for g in self.allowed_groups]
    
    def clean_text_field(self, text: str) -> str:
        """
        Очистка текстового поля от проблемных символов
        
        Args:
            text: Исходный текст
            
        Returns:
            Очищенный текст
        """
        if pd.isna(text):
            return ""
        
        text = str(text)
        
        # Удаляем лишние пробелы
        text = re.sub(r'\s+', ' ', text)
        
        # Удаляем проблемные символы
        text = re.sub(r'[^\w\s\.\,\!\?\-\(\)\:;]', '', text)
        
        # Обрезаем пробелы
        text = text.strip()
        
        return text
    
    def validate_and_clean_dataframe(self, df: pd.DataFrame, file_path: str = "") -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Валидация и очистка DataFrame
        
        Args:
            df: DataFrame для обработки
            file_path: Путь к файлу
            
        Returns:
            Кортеж (очищенный DataFrame, результаты валидации)
        """
        # Сначала валидируем
        validation_result = self.validate_dataframe(df, file_path)
        
        if not validation_result['is_valid']:
            logger.error(f"Данные не прошли валидацию: {validation_result['errors']}")
            return df, validation_result
        
        # Очищаем данные
        cleaned_df = df.copy()
        
        # Очищаем текстовые поля
        text_fields = ['name', 'address', 'review_text', 'user_name']
        for field in text_fields:
            if field in cleaned_df.columns:
                cleaned_df[field] = cleaned_df[field].apply(self.clean_text_field)
        
        # Удаляем строки с пустыми обязательными полями
        for field in self.required_fields:
            if field in cleaned_df.columns:
                cleaned_df = cleaned_df[cleaned_df[field].notna() & (cleaned_df[field].str.strip() != '')]
        
        # Удаляем дубликаты объектов
        if 'name' in cleaned_df.columns and 'address' in cleaned_df.columns:
            cleaned_df = cleaned_df.drop_duplicates(subset=['name', 'address'], keep='first')
        
        logger.info(f"Очистка завершена: {len(cleaned_df)} строк из {len(df)}")
        
        return cleaned_df, validation_result
    
    def get_validation_summary(self, validation_result: Dict[str, Any]) -> str:
        """
        Получение краткого отчета о валидации
        
        Args:
            validation_result: Результат валидации
            
        Returns:
            Текстовый отчет
        """
        stats = validation_result['stats']
        
        summary = f"""
=== ОТЧЕТ О ВАЛИДАЦИИ ===
Всего строк: {stats['total_rows']}
Валидных строк: {stats['valid_rows']}
Некорректных строк: {stats['invalid_rows']}
Дублирующихся объектов: {stats['duplicate_objects']}

Ошибки: {len(validation_result['errors'])}
Предупреждения: {len(validation_result['warnings'])}

Статус: {'✅ ВАЛИДНЫ' if validation_result['is_valid'] else '❌ НЕВАЛИДНЫ'}
        """
        
        if validation_result['errors']:
            summary += "\n=== ОШИБКИ ===\n"
            for error in validation_result['errors'][:5]:  # Показываем первые 5 ошибок
                summary += f"• {error}\n"
        
        if validation_result['warnings']:
            summary += "\n=== ПРЕДУПРЕЖДЕНИЯ ===\n"
            for warning in validation_result['warnings'][:5]:  # Показываем первые 5 предупреждений
                summary += f"• {warning}\n"
        
        return summary.strip()


def main():
    """Тестовая функция для проверки валидатора"""
    import sys
    
    if len(sys.argv) < 2:
        print("Использование: python data_validator.py <путь_к_файлу>")
        sys.exit(1)
    
    file_path = sys.argv[1]
    
    try:
        # Загружаем данные
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        elif file_path.endswith('.xlsx'):
            df = pd.read_excel(file_path)
        else:
            print("Поддерживаются только CSV и Excel файлы")
            sys.exit(1)
        
        # Создаем валидатор
        validator = DataValidator()
        
        # Валидируем и очищаем данные
        cleaned_df, validation_result = validator.validate_and_clean_dataframe(df, file_path)
        
        # Выводим отчет
        print(validator.get_validation_summary(validation_result))
        
        # Сохраняем очищенные данные
        if validation_result['is_valid']:
            output_path = file_path.replace('.csv', '_cleaned.csv').replace('.xlsx', '_cleaned.csv')
            cleaned_df.to_csv(output_path, index=False)
            print(f"\nОчищенные данные сохранены в: {output_path}")
        
    except Exception as e:
        print(f"Ошибка: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 