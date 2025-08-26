"""
Утилиты для работы с группами объектов
"""

from typing import Optional, Dict, List
from .config import GROUP_MAPPING, RUSSIAN_TO_ENGLISH, RUSSIAN_PLURAL_TO_ENGLISH, GROUP_NAMES_PLURAL

def get_english_group_name(russian_name: str) -> Optional[str]:
    """
    Получает английское название группы по русскому
    
    Args:
        russian_name: Русское название группы
        
    Returns:
        Английское название группы или None если не найдено
    """
    return RUSSIAN_TO_ENGLISH.get(russian_name)

def get_russian_group_name(english_name: str) -> Optional[str]:
    """
    Получает русское название группы по английскому
    
    Args:
        english_name: Английское название группы
        
    Returns:
        Русское название группы или None если не найдено
    """
    return GROUP_MAPPING.get(english_name)

def get_russian_group_name_plural(english_name: str) -> Optional[str]:
    """
    Получает русское название группы во множественном числе
    
    Args:
        english_name: Английское название группы
        
    Returns:
        Русское название группы во множественном числе или None если не найдено
    """
    return GROUP_NAMES_PLURAL.get(english_name)

def normalize_group_name(group_name: str) -> str:
    """
    Нормализует название группы к стандартному английскому виду
    
    Args:
        group_name: Название группы (может быть на русском или английском)
        
    Returns:
        Нормализованное английское название группы
    """
    if not group_name:
        return ''
    
    group_name = str(group_name).strip().lower()
    
    # Если это уже английское название
    if group_name in GROUP_MAPPING:
        return group_name
    
    # Если это русское название (единственное число)
    english_name = get_english_group_name(group_name)
    if english_name:
        return english_name
    
    # Если это русское название во множественном числе
    english_name = RUSSIAN_PLURAL_TO_ENGLISH.get(group_name)
    if english_name:
        return english_name
    
    # Попробуем найти по частичному совпадению
    for english, russian in GROUP_MAPPING.items():
        if group_name in russian.lower() or russian.lower() in group_name:
            return english
    
    return group_name

def validate_group_name(group_name: str) -> bool:
    """
    Проверяет, является ли название группы валидным
    
    Args:
        group_name: Название группы для проверки
        
    Returns:
        True если группа валидна, False иначе
    """
    if not group_name:
        return False
    
    normalized = normalize_group_name(group_name)
    return normalized in GROUP_MAPPING

def get_all_group_variants() -> Dict[str, Dict[str, str]]:
    """
    Получает все варианты названий групп
    
    Returns:
        Словарь с вариантами названий для каждой группы
    """
    result = {}
    for english, russian in GROUP_MAPPING.items():
        result[english] = {
            'english': english,
            'russian_singular': russian,
            'russian_plural': GROUP_NAMES_PLURAL.get(english, russian)
        }
    return result
