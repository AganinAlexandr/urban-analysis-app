"""
Конфигурация приложения для анализа городской среды
"""

# Конфигурация приложения

# Настройки сентимента
SENTIMENT_CONFIG = {
    'values': ['хорошо', 'удовлетворительно', 'плохо'],
    'colors': {
        'хорошо': '#28a745',
        'удовлетворительно': '#ffc107', 
        'плохо': '#fd7e14'
    },
    'rating_to_sentiment': {
        5: 'хорошо',
        4: 'удовлетворительно',
        3: 'удовлетворительно',
        2: 'плохо',
        1: 'плохо'
    }
}

# Настройки групп
GROUP_CONFIG = {
    'supplier_groups': ['school', 'hospital', 'pharmacy', 'kindergarden', 'polyclinic', 'university', 'shopmall', 'resident_complexes'],
    'determined_groups': ['school', 'hospital', 'pharmacy', 'kindergarden', 'polyclinic', 'university', 'shopmall', 'resident_complexes'],
    'colors': {
        'school': '#007bff',
        'schools': '#007bff',
        'hospital': '#dc3545',
        'hospitals': '#dc3545',
        'pharmacy': '#28a745',
        'pharmacies': '#28a745',
        'kindergarden': '#ffc107',
        'kindergartens': '#ffc107',
        'polyclinic': '#17a2b8',
        'polyclinics': '#17a2b8',
        'university': '#6f42c1',
        'universities': '#6f42c1',
        'shopmall': '#fd7e14',
        'shopping_malls': '#fd7e14',
        'resident_complexes': '#e83e8c'
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