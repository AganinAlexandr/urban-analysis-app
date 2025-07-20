"""
Модуль для анализа текста отзывов
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
import re
import logging

logger = logging.getLogger(__name__)

class TextAnalyzer:
    """Класс для анализа текста отзывов"""
    
    def __init__(self):
        """Инициализация анализатора текста"""
        # Словари для анализа сентиментов
        self.positive_words = [
            'хорошо', 'отлично', 'прекрасно', 'замечательно', 'удобно',
            'чисто', 'современно', 'профессионально', 'доброжелательно',
            'быстро', 'качественно', 'спасибо', 'благодарю', 'рекомендую'
        ]
        
        self.negative_words = [
            'плохо', 'ужасно', 'отвратительно', 'неудобно', 'грязно',
            'старо', 'непрофессионально', 'грубо', 'медленно', 'некачественно',
            'жалоба', 'проблема', 'недоволен', 'разочарован'
        ]
        
        # Типы отзывов
        self.review_types = {
            'благодарность': ['спасибо', 'благодарю', 'спасибо большое', 'благодарность'],
            'жалоба': ['жалоба', 'жалуемся', 'проблема', 'недоволен', 'разочарован'],
            'предложение': ['предлагаю', 'рекомендую', 'можно', 'стоит'],
            'информационный': ['информация', 'рассказать', 'сообщить', 'уведомить']
        }
    
    def analyze_sentiment(self, text: str) -> Dict:
        """
        Анализ сентимента текста
        
        Args:
            text: Текст для анализа
            
        Returns:
            Словарь с результатами анализа
        """
        if not text or pd.isna(text):
            return {
                'sentiment': 'neutral',
                'sentiment_score': 0.0,
                'positive_words': [],
                'negative_words': [],
                'review_type': 'информационный'
            }
        
        text = str(text).lower()
        
        # Подсчет положительных и отрицательных слов
        positive_count = sum(1 for word in self.positive_words if word in text)
        negative_count = sum(1 for word in self.negative_words if word in text)
        
        # Определение типа отзыва
        review_type = self._determine_review_type(text)
        
        # Расчет сентимента
        if positive_count > negative_count:
            sentiment = 'positive'
            sentiment_score = positive_count / (positive_count + negative_count + 1)
        elif negative_count > positive_count:
            sentiment = 'negative'
            sentiment_score = -negative_count / (positive_count + negative_count + 1)
        else:
            sentiment = 'neutral'
            sentiment_score = 0.0
        
        return {
            'sentiment': sentiment,
            'sentiment_score': sentiment_score,
            'positive_words': [word for word in self.positive_words if word in text],
            'negative_words': [word for word in self.negative_words if word in text],
            'review_type': review_type
        }
    
    def _determine_review_type(self, text: str) -> str:
        """
        Определение типа отзыва
        
        Args:
            text: Текст отзыва
            
        Returns:
            Тип отзыва
        """
        text = text.lower()
        
        for review_type, keywords in self.review_types.items():
            if any(keyword in text for keyword in keywords):
                return review_type
        
        return 'информационный'
    
    def split_complex_review(self, text: str, sentiment_analysis: Dict) -> List[Dict]:
        """
        Разделение сложного отзыва на части
        
        Args:
            text: Текст отзыва
            sentiment_analysis: Результат анализа сентимента
            
        Returns:
            Список частей отзыва
        """
        if sentiment_analysis['sentiment'] == 'neutral':
            return []
        
        # Простая логика разделения по предложениям
        sentences = re.split(r'[.!?]+', text)
        parts = []
        
        for i, sentence in enumerate(sentences):
            sentence = sentence.strip()
            if not sentence:
                continue
            
            # Анализируем каждое предложение
            sentence_analysis = self.analyze_sentiment(sentence)
            
            if sentence_analysis['sentiment'] != 'neutral':
                parts.append({
                    'text': sentence,
                    'sentiment': sentence_analysis['sentiment'],
                    'sentiment_score': sentence_analysis['sentiment_score'],
                    'review_type': sentence_analysis['review_type'],
                    'positive_words': sentence_analysis['positive_words'],
                    'negative_words': sentence_analysis['negative_words'],
                    'part_index': i
                })
        
        return parts
    
    def analyze_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Анализ DataFrame с отзывами
        
        Args:
            df: DataFrame с колонкой 'review_text'
            
        Returns:
            DataFrame с результатами анализа
        """
        if 'review_text' not in df.columns:
            logger.error("Колонка 'review_text' не найдена")
            return df
        
        # Анализируем каждый отзыв
        results = []
        complex_parts = []
        
        for idx, row in df.iterrows():
            text = row.get('review_text', '')
            analysis = self.analyze_sentiment(text)
            
            # Добавляем результаты анализа
            row_dict = row.to_dict()
            row_dict.update({
                'sentiment': analysis['sentiment'],
                'sentiment_score': analysis['sentiment_score'],
                'review_type': analysis['review_type'],
                'positive_words_count': len(analysis['positive_words']),
                'negative_words_count': len(analysis['negative_words'])
            })
            
            results.append(row_dict)
            
            # Проверяем на сложный отзыв и разделяем на части
            if analysis['sentiment'] != 'neutral':
                parts = self.split_complex_review(text, analysis)
                
                # Группируем части по сентименту
                positive_parts = [p for p in parts if p['sentiment'] == 'positive']
                negative_parts = [p for p in parts if p['sentiment'] == 'negative']
                
                # Создаем части с суффиксами согласно ТЗ
                if positive_parts:
                    for i, part in enumerate(positive_parts):
                        part_dict = row.to_dict()
                        part_dict.update({
                            'review_text': part['text'],
                            'sentiment': part['sentiment'],
                            'sentiment_score': part['sentiment_score'],
                            'review_type': part['review_type'],
                            'positive_words_count': len(part.get('positive_words', [])),
                            'negative_words_count': len(part.get('negative_words', [])),
                            'is_complex_part': True,
                            'part_type': '_p',  # Суффикс для положительной части
                            'part_index': part['part_index']
                        })
                        complex_parts.append(part_dict)
                
                if negative_parts:
                    for i, part in enumerate(negative_parts):
                        part_dict = row.to_dict()
                        part_dict.update({
                            'review_text': part['text'],
                            'sentiment': part['sentiment'],
                            'sentiment_score': part['sentiment_score'],
                            'review_type': part['review_type'],
                            'positive_words_count': len(part.get('positive_words', [])),
                            'negative_words_count': len(part.get('negative_words', [])),
                            'is_complex_part': True,
                            'part_type': '_n',  # Суффикс для отрицательной части
                            'part_index': part['part_index']
                        })
                        complex_parts.append(part_dict)
        
        # Создаем DataFrame с результатами
        result_df = pd.DataFrame(results)
        
        # Добавляем части сложных отзывов
        if complex_parts:
            complex_df = pd.DataFrame(complex_parts)
            result_df = pd.concat([result_df, complex_df], ignore_index=True)
        
        return result_df 