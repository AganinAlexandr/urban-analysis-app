"""
Конфигурация приложения для анализа городской среды
"""

# Конфигурация приложения

# Единая система маппинга групп
GROUP_MAPPING = {
    # Английские ключи -> русские названия (единственное число)
    'school': 'Школа',
    'hospital': 'Больница', 
    'university': 'Университет',
    'pharmacy': 'Аптека',
    'kindergarden': 'Детский сад',
    'polyclinic': 'Поликлиника',
    'shopmall': 'Торговый центр',
    'resident_complex': 'Жилой комплекс'
}

# Обратный маппинг: русские -> английские
RUSSIAN_TO_ENGLISH = {v: k for k, v in GROUP_MAPPING.items()}

# Маппинг множественного числа русских названий на английские
RUSSIAN_PLURAL_TO_ENGLISH = {
    'школы': 'school',
    'больницы': 'hospital',
    'университеты': 'university',
    'аптеки': 'pharmacy',
    'детские сады': 'kindergarden',
    'поликлиники': 'polyclinic',
    'торговые центры': 'shopmall',
    'жилые комплексы': 'resident_complex'
}

# Русские названия во множественном числе для отображения
GROUP_NAMES_PLURAL = {
    'school': 'Школы',
    'hospital': 'Больницы',
    'university': 'Университеты', 
    'pharmacy': 'Аптеки',
    'kindergarden': 'Детские сады',
    'polyclinic': 'Поликлиники',
    'shopmall': 'Торговые центры',
    'resident_complex': 'Жилые комплексы'
}

# Настройки сентимента
SENTIMENT_CONFIG = {
    'values': ['положительный', 'нейтральный', 'отрицательный'],
    'colors': {
        'положительный': '#28a745',  # Зеленый
        'нейтральный': '#ffc107',    # Желтый
        'отрицательный': '#dc3545'   # Красный
    },
    'rating_to_sentiment': {
        5: 'положительный',
        4: 'нейтральный',
        3: 'нейтральный',
        2: 'отрицательный',
        1: 'отрицательный'
    }
}

# Настройки групп
GROUP_CONFIG = {
    'supplier_groups': ['school', 'hospital', 'pharmacy', 'kindergarden', 'polyclinic', 'university', 'shopmall', 'resident_complex'],
    'determined_groups': ['school', 'hospital', 'pharmacy', 'kindergarden', 'polyclinic', 'university', 'shopmall', 'resident_complex'],
    'colors': {
        'school': '#007bff',
        'hospital': '#dc3545', 
        'pharmacy': '#28a745',
        'kindergarden': '#ffc107',
        'polyclinic': '#17a2b8',
        'university': '#6f42c1',
        'shopmall': '#fd7e14',
        'resident_complex': '#e83e8c'
    }
}

# Методы анализа
ANALYSIS_METHODS = {
    'rating': 'Оценка пользователя',
    'classical_sentiment': 'Классический анализ',
    'openai_sentiment': 'OpenAI GPT',
    'google_gemini_sentiment': 'Google Gemini',
    'yandexgpt_sentiment': 'YandexGPT',
    'gigachat_sentiment': 'GigaChat',
    'qwen_sentiment': 'Qwen Turbo',
    'deepseek_sentiment': 'DeepSeek Chat'
} 