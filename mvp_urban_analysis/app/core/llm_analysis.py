"""
Модуль для анализа текста с использованием больших языковых моделей
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
import logging
import json
import time

logger = logging.getLogger(__name__)

class LLMAnalyzer:
    """Класс для анализа текста с использованием LLM"""
    
    def __init__(self, api_keys: Dict[str, str] = None):
        """
        Инициализация анализатора LLM
        
        Args:
            api_keys: Словарь с API ключами для различных сервисов
        """
        self.api_keys = api_keys or {}
        self.available_methods = self._check_available_methods()
        
    def _check_available_methods(self) -> List[str]:
        """
        Проверка доступности различных методов анализа
        
        Returns:
            Список доступных методов
        """
        import os
        from dotenv import load_dotenv
        
        # Загружаем переменные окружения
        load_dotenv('env_data.env')
        
        available = ['classical']  # Классический метод всегда доступен
        
        # Проверяем наличие API ключей для различных LLM
        if self.api_keys.get('openai'):
            available.append('openai_gpt')
        if self.api_keys.get('gemini'):
            available.append('google_gemini')
        
        # Проверяем Yandex GPT через переменные окружения
        yandex_folder_id = os.getenv('YANDEX_GPT_FOLDER_ID')
        yandex_oauth_token = os.getenv('YANDEX_GPT_OAUTH_TOKEN')
        if yandex_folder_id and yandex_oauth_token:
            available.append('yandex_gpt')
            logger.info("Yandex GPT доступен")
        else:
            logger.warning("Yandex GPT недоступен: отсутствуют переменные окружения")
        
        if self.api_keys.get('gigachat'):
            available.append('gigachat')
        if self.api_keys.get('qwen'):
            available.append('qwen_turbo')
        if self.api_keys.get('deepseek'):
            available.append('deepseek_chat')
            
        return available
    
    def analyze_sentiment_classical(self, text: str) -> Dict:
        """
        Классический анализ сентимента (NLTK + VADER)
        
        Args:
            text: Текст для анализа
            
        Returns:
            Результаты анализа
        """
        # Простая реализация классического анализа
        # В реальном проекте здесь будет использоваться NLTK и VADER
        
        if not text or pd.isna(text):
            return {
                'method': 'classical',
                'sentiment': 'neutral',
                'sentiment_score': 0.0,
                'confidence': 0.5,
                'review_type': 'информационный'
            }
        
        text = str(text).lower()
        
        # Простой анализ на основе ключевых слов
        positive_words = ['хорошо', 'отлично', 'прекрасно', 'спасибо', 'благодарю']
        negative_words = ['плохо', 'ужасно', 'жалоба', 'проблема', 'недоволен']
        
        positive_count = sum(1 for word in positive_words if word in text)
        negative_count = sum(1 for word in negative_words if word in text)
        
        if positive_count > negative_count:
            sentiment = 'positive'
            sentiment_score = positive_count / (positive_count + negative_count + 1)
        elif negative_count > positive_count:
            sentiment = 'negative'
            sentiment_score = -negative_count / (positive_count + negative_count + 1)
        else:
            sentiment = 'neutral'
            sentiment_score = 0.0
        
        # Определение типа отзыва
        if any(word in text for word in ['спасибо', 'благодарю']):
            review_type = 'благодарность'
        elif any(word in text for word in ['жалоба', 'проблема']):
            review_type = 'жалоба'
        elif any(word in text for word in ['предлагаю', 'рекомендую']):
            review_type = 'предложение'
        else:
            review_type = 'информационный'
        
        return {
            'method': 'classical',
            'sentiment': sentiment,
            'sentiment_score': sentiment_score,
            'confidence': 0.7,
            'review_type': review_type
        }
    
    def analyze_sentiment_openai(self, text: str) -> Dict:
        """
        Анализ сентимента с использованием OpenAI GPT
        
        Args:
            text: Текст для анализа
            
        Returns:
            Результаты анализа
        """
        # Заглушка для OpenAI анализа
        # В реальном проекте здесь будет вызов OpenAI API
        
        logger.info(f"OpenAI анализ для текста: {text[:50]}...")
        
        return {
            'method': 'openai_gpt',
            'sentiment': 'neutral',
            'sentiment_score': 0.0,
            'confidence': 0.8,
            'review_type': 'информационный',
            'note': 'OpenAI API не настроен'
        }
    
    def analyze_sentiment_gemini(self, text: str) -> Dict:
        """
        Анализ сентимента с использованием Google Gemini
        
        Args:
            text: Текст для анализа
            
        Returns:
            Результаты анализа
        """
        # Заглушка для Gemini анализа
        
        logger.info(f"Gemini анализ для текста: {text[:50]}...")
        
        return {
            'method': 'google_gemini',
            'sentiment': 'neutral',
            'sentiment_score': 0.0,
            'confidence': 0.8,
            'review_type': 'информационный',
            'note': 'Gemini API не настроен'
        }
    
    def analyze_sentiment_yandex(self, text: str) -> Dict:
        """
        Анализ сентимента с использованием YandexGPT
        
        Args:
            text: Текст для анализа
            
        Returns:
            Результаты анализа
        """
        try:
            from app.core.yandex_gpt_analyzer import YandexGPTAnalyzer
            
            # Создаем анализатор Yandex GPT
            analyzer = YandexGPTAnalyzer()
            
            # Выполняем анализ
            result = analyzer.analyze_sentiment(text)
            
            logger.info(f"YandexGPT анализ завершен для текста: {text[:50]}...")
            
            return {
                'method': 'yandex_gpt',
                'sentiment': result.get('sentiment', 'нейтральный'),
                'sentiment_score': result.get('sentiment_score', 0.0),
                'confidence': result.get('confidence', 0.5),
                'review_type': result.get('review_type', 'информационный'),
                'raw_response': result.get('raw_response', '')
            }
            
        except Exception as e:
            logger.error(f"Ошибка при анализе YandexGPT: {e}")
            
            return {
                'method': 'yandex_gpt',
                'sentiment': 'нейтральный',
                'sentiment_score': 0.0,
                'confidence': 0.5,
                'review_type': 'информационный',
                'note': f'Ошибка API: {str(e)}'
            }
    
    def analyze_dataframe(self, df: pd.DataFrame, methods: List[str] = None) -> pd.DataFrame:
        """
        Анализ DataFrame с использованием выбранных методов
        
        Args:
            df: DataFrame с колонкой 'review_text'
            methods: Список методов для анализа
            
        Returns:
            DataFrame с результатами анализа
        """
        logger.info(f"analyze_dataframe: получены методы: {methods}")
        logger.info(f"analyze_dataframe: доступные методы: {self.available_methods}")
        
        if 'review_text' not in df.columns:
            logger.error("Колонка 'review_text' не найдена")
            return df
        
        if methods is None:
            methods = ['classical']  # По умолчанию используем классический метод
        
        # Проверяем доступность методов
        available_methods = [m for m in methods if m in self.available_methods]
        logger.info(f"analyze_dataframe: доступные из запрошенных: {available_methods}")
        
        if not available_methods:
            logger.warning("Ни один из запрошенных методов не доступен, используем классический")
            available_methods = ['classical']
        
        results = []
        
        for idx, row in df.iterrows():
            text = row.get('review_text', '')
            
            # Анализируем каждым доступным методом
            analysis_results = {}
            
            for method in available_methods:
                logger.info(f"Анализируем методом: {method}")
                if method == 'classical':
                    result = self.analyze_sentiment_classical(text)
                elif method == 'openai_gpt':
                    result = self.analyze_sentiment_openai(text)
                elif method == 'google_gemini':
                    result = self.analyze_sentiment_gemini(text)
                elif method == 'yandex_gpt':
                    result = self.analyze_sentiment_yandex(text)
                else:
                    logger.warning(f"Неизвестный метод: {method}")
                    continue
                
                analysis_results[method] = result
                logger.info(f"Результат {method}: {result}")
            
            # Создаем строку результата
            row_dict = row.to_dict()
            
            # Добавляем результаты каждого метода
            for method, result in analysis_results.items():
                for key, value in result.items():
                    row_dict[f"{method}_{key}"] = value
            
            # Добавляем информацию о методах
            row_dict['analysis_methods'] = list(analysis_results.keys())
            
            results.append(row_dict)
        
        return pd.DataFrame(results)
    
    def compare_methods(self, df: pd.DataFrame, methods: List[str] = None) -> Dict:
        """
        Сравнение результатов разных методов анализа
        
        Args:
            df: DataFrame с результатами анализа
            methods: Список методов для сравнения
            
        Returns:
            Словарь с результатами сравнения
        """
        if methods is None:
            methods = ['classical']
        
        comparison = {
            'methods_used': methods,
            'total_records': len(df),
            'agreement_rate': 0.0,
            'method_results': {}
        }
        
        # Анализируем согласованность методов
        if len(methods) > 1:
            agreement_count = 0
            for idx, row in df.iterrows():
                sentiments = []
                for method in methods:
                    sentiment_key = f"{method}_sentiment"
                    if sentiment_key in row:
                        sentiments.append(row[sentiment_key])
                
                # Проверяем согласованность
                if len(set(sentiments)) == 1:
                    agreement_count += 1
            
            comparison['agreement_rate'] = agreement_count / len(df) if df.shape[0] > 0 else 0.0
        
        # Статистика по методам
        for method in methods:
            sentiment_key = f"{method}_sentiment"
            if sentiment_key in df.columns:
                # Конвертируем числовые типы для JSON сериализации
                sentiment_counts_raw = df[sentiment_key].value_counts().to_dict()
                sentiment_counts = {str(k): int(v) for k, v in sentiment_counts_raw.items()}
                comparison['method_results'][method] = {
                    'sentiment_distribution': sentiment_counts,
                    'records_analyzed': df[sentiment_key].notna().sum()
                }
        
        return comparison 