"""
Нормализация данных при загрузке файлов
"""

import pandas as pd
from typing import Dict, Optional, Tuple
from .group_utils import normalize_group_name, get_russian_group_name

class DataNormalizer:
    """Класс для нормализации данных при загрузке файлов"""
    
    @staticmethod
    def normalize_group_fields(df: pd.DataFrame) -> pd.DataFrame:
        """
        Нормализует поля групп в DataFrame
        
        Args:
            df: DataFrame с данными
            
        Returns:
            DataFrame с нормализованными полями групп
        """
        df_normalized = df.copy()
        
        # Нормализуем поле group (группа от поставщика)
        if 'group' in df_normalized.columns:
            df_normalized['group'] = df_normalized['group'].apply(
                lambda x: normalize_group_name(x) if pd.notna(x) and str(x).strip() else ''
            )
        
        # Нормализуем поле determined_group (определяемая группа)
        if 'determined_group' in df_normalized.columns:
            df_normalized['determined_group'] = df_normalized['determined_group'].apply(
                lambda x: normalize_group_name(x) if pd.notna(x) and str(x).strip() else ''
            )
        
        # Добавляем стандартные поля
        if 'group' in df_normalized.columns:
            df_normalized['group_name'] = df_normalized['group']
            df_normalized['group_type'] = df_normalized['group'].apply(
                lambda x: get_russian_group_name(x) if x else ''
            )
        
        if 'determined_group' in df_normalized.columns:
            df_normalized['detected_group_type'] = df_normalized['determined_group'].apply(
                lambda x: get_russian_group_name(x) if x else ''
            )
        
        return df_normalized
    
    @staticmethod
    def validate_group_data(df: pd.DataFrame) -> Dict[str, any]:
        """
        Валидирует данные групп в DataFrame
        
        Args:
            df: DataFrame с данными
            
        Returns:
            Словарь с результатами валидации
        """
        validation_result = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'group_stats': {}
        }
        
        # Проверяем поле group
        if 'group' in df.columns:
            valid_groups = df['group'].apply(lambda x: bool(normalize_group_name(x)) if pd.notna(x) and str(x).strip() else True)
            invalid_groups = df[~valid_groups]
            
            if not invalid_groups.empty:
                validation_result['valid'] = False
                validation_result['errors'].append(f"Найдено {len(invalid_groups)} записей с невалидными группами в поле 'group'")
                
                # Статистика по группам
                group_counts = df['group'].value_counts()
                validation_result['group_stats']['group'] = group_counts.to_dict()
        
        # Проверяем поле determined_group
        if 'determined_group' in df.columns:
            valid_determined = df['determined_group'].apply(lambda x: bool(normalize_group_name(x)) if pd.notna(x) and str(x).strip() else True)
            invalid_determined = df[~valid_determined]
            
            if not invalid_determined.empty:
                validation_result['warnings'].append(f"Найдено {len(invalid_determined)} записей с невалидными группами в поле 'determined_group'")
                
                # Статистика по определяемым группам
                determined_counts = df['determined_group'].value_counts()
                validation_result['group_stats']['determined_group'] = determined_counts.to_dict()
        
        return validation_result
    
    @staticmethod
    def suggest_group_from_content(object_name: str, review_text: str = '') -> Tuple[str, float]:
        """
        Предлагает группу на основе содержимого
        
        Args:
            object_name: Название объекта
            review_text: Текст отзыва
            
        Returns:
            Кортеж (название группы, уверенность)
        """
        # Здесь можно использовать существующую логику определения группы
        # Пока возвращаем базовую логику
        combined_text = f"{object_name} {review_text}".lower()
        
        # Простые ключевые слова для определения группы
        keywords = {
            'school': ['школа', 'лицей', 'гимназия', 'школа-интернат'],
            'hospital': ['больница', 'госпиталь', 'клиника', 'медицинский', 'дгкб'],
            'university': ['университет', 'институт', 'академия', 'вуз'],
            'pharmacy': ['аптека', 'фармация'],
            'kindergarden': ['детский сад', 'сад'],
            'polyclinic': ['поликлиника', 'амбулатория'],
            'shopmall': ['торговый', 'молл', 'центр', 'магазин'],
            'resident_complex': ['жилой', 'комплекс', 'дом']
        }
        
        best_match = 'unknown'
        best_score = 0.0
        
        for group, group_keywords in keywords.items():
            score = sum(1 for keyword in group_keywords if keyword in combined_text)
            if score > best_score:
                best_score = score
                best_match = group
        
        confidence = min(best_score / 3.0, 1.0)  # Нормализуем уверенность
        
        return best_match, confidence
    
    @staticmethod
    def fill_missing_groups(df: pd.DataFrame, auto_detect: bool = True) -> pd.DataFrame:
        """
        Заполняет отсутствующие группы
        
        Args:
            df: DataFrame с данными
            auto_detect: Автоматически определять группы для пустых полей
            
        Returns:
            DataFrame с заполненными группами
        """
        df_filled = df.copy()
        
        # Заполняем пустые determined_group
        if 'determined_group' in df_filled.columns and auto_detect:
            empty_determined = df_filled['determined_group'].isna() | (df_filled['determined_group'] == '')
            
            for idx in df_filled[empty_determined].index:
                object_name = df_filled.at[idx, 'name'] if 'name' in df_filled.columns else ''
                review_text = df_filled.at[idx, 'review_text'] if 'review_text' in df_filled.columns else ''
                
                suggested_group, confidence = DataNormalizer.suggest_group_from_content(object_name, review_text)
                df_filled.at[idx, 'determined_group'] = suggested_group
        
        # Заполняем пустые group на основе determined_group
        if 'group' in df_filled.columns and 'determined_group' in df_filled.columns:
            empty_group = df_filled['group'].isna() | (df_filled['group'] == '')
            
            for idx in df_filled[empty_group].index:
                determined_group = df_filled.at[idx, 'determined_group']
                if determined_group and determined_group != 'unknown':
                    df_filled.at[idx, 'group'] = determined_group
        
        return df_filled


