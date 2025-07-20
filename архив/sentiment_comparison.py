"""
Модуль сравнения различных методов анализа сентиментов

Назначение:
    Сравнение эффективности различных методов анализа сентиментов для русскоязычных текстов.
    Включает:
    - NLTK (для сравнения)
    - Простой анализ на основе ключевых слов

Зависимости:
    - nltk: для базового анализа
    - pandas: для обработки данных
    - numpy: для математических операций
    - matplotlib: для визуализации
    - seaborn: для улучшенных визуализаций
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, List, Tuple, Any
import warnings
from abc import ABC, abstractmethod
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
warnings.filterwarnings('ignore')

class SentimentAnalyzerBase(ABC):
    """Базовый класс для анализаторов сентиментов"""
    
    @abstractmethod
    def analyze(self, text: str) -> Dict[str, Any]:
        """
        Анализ сентимента текста
        
        Args:
            text (str): Текст для анализа
            
        Returns:
            Dict[str, Any]: Результаты анализа
        """
        pass

class KeywordAnalyzer(SentimentAnalyzerBase):
    """Анализатор на основе ключевых слов"""
    
    def __init__(self):
        self.positive_words = [
            "хорошо", "отлично", "прекрасно", "замечательно", "превосходно", 
            "компетентно", "профессионально", "качественно", "чисто", "быстро", 
            "аккуратно", "классно", "уютно", "доволен", "впечатляет", "современно", 
            "идеально", "круто", "рад", "доволен", "удобно", "спасибо", "благодарю",
            "рекомендую", "советую", "полезно", "интересно", "понравилось"
        ]
        
        self.negative_words = [
            "плохо", "ужасно", "отвратительно", "некомпетентно", "непрофессионально", 
            "бестолково", "неразумно", "эскулап", "бездарь", "грязно", "медленно", 
            "кошмар", "разочарование", "неудобно", "жалоба", "недоволен", "недовольны",
            "проблема", "ошибка", "неправильно", "некачественно", "неудовлетворительно"
        ]
        
    def analyze(self, text: str) -> Dict[str, Any]:
        if not isinstance(text, str) or not text.strip():
            return {'sentiment': 'neutral', 'score': 0.0}
            
        words = text.lower().split()
        positive_count = sum(1 for word in words if word in self.positive_words)
        negative_count = sum(1 for word in words if word in self.negative_words)
        
        if positive_count > negative_count:
            sentiment = 'positive'
            score = positive_count / (positive_count + negative_count) if (positive_count + negative_count) > 0 else 0.0
        elif negative_count > positive_count:
            sentiment = 'negative'
            score = negative_count / (positive_count + negative_count) if (positive_count + negative_count) > 0 else 0.0
        else:
            sentiment = 'neutral'
            score = 0.0
            
        return {
            'sentiment': sentiment,
            'score': score
        }

class NLTKAnalyzer(SentimentAnalyzerBase):
    """Анализатор на основе NLTK"""
    
    def __init__(self):
        print("Загрузка NLTK ресурсов...")
        nltk.download('vader_lexicon', quiet=True)
        self.analyzer = SentimentIntensityAnalyzer()
        print("NLTK ресурсы загружены")
        
    def analyze(self, text: str) -> Dict[str, Any]:
        if not isinstance(text, str) or not text.strip():
            return {'sentiment': 'neutral', 'score': 0.0}
            
        try:
            scores = self.analyzer.polarity_scores(text)
            compound_score = scores['compound']
            
            if compound_score >= 0.05:
                sentiment = 'positive'
            elif compound_score <= -0.05:
                sentiment = 'negative'
            else:
                sentiment = 'neutral'
                
            return {
                'sentiment': sentiment,
                'score': abs(compound_score)
            }
        except Exception as e:
            print(f"Ошибка при анализе NLTK: {str(e)}")
            return {'sentiment': 'error', 'score': 0.0}

class SentimentComparison:
    """Класс для сравнения различных методов анализа сентиментов"""
    
    def __init__(self):
        print("\nИнициализация анализаторов...")
        self.analyzers = {
            'keywords': KeywordAnalyzer(),
            'nltk': NLTKAnalyzer()
        }
        print("Анализаторы инициализированы")
        
    def analyze_text(self, text: str) -> Dict[str, Dict[str, Any]]:
        """
        Анализ текста всеми методами
        
        Args:
            text (str): Текст для анализа
            
        Returns:
            Dict[str, Dict[str, Any]]: Результаты анализа всеми методами
        """
        results = {}
        for name, analyzer in self.analyzers.items():
            try:
                results[name] = analyzer.analyze(text)
            except Exception as e:
                print(f"Ошибка при анализе методом {name}: {str(e)}")
                results[name] = {'sentiment': 'error', 'score': 0.0}
        return results
    
    def analyze_dataset(self, df: pd.DataFrame, text_column: str = 'review_text') -> pd.DataFrame:
        """
        Анализ датасета всеми методами
        
        Args:
            df (pd.DataFrame): Датасет для анализа
            text_column (str): Название колонки с текстом
            
        Returns:
            pd.DataFrame: Датасет с результатами анализа
        """
        results = []
        total = len(df)
        
        for idx, row in df.iterrows():
            if idx % 100 == 0:
                print(f"Обработка {idx}/{total} отзывов...")
                
            text = row[text_column]
            analysis_results = self.analyze_text(text)
            results.append(analysis_results)
            
        # Преобразуем результаты в DataFrame
        results_df = pd.DataFrame(results)
        
        # Добавляем результаты к исходному датасету
        for method in self.analyzers.keys():
            df[f'{method}_sentiment'] = results_df[method].apply(lambda x: x['sentiment'])
            df[f'{method}_score'] = results_df[method].apply(lambda x: x['score'])
            
        return df
    
    def plot_comparison(self, df: pd.DataFrame):
        """
        Построение графиков сравнения методов
        
        Args:
            df (pd.DataFrame): Датасет с результатами анализа
        """
        print("\nПостроение графиков...")
        
        # 1. Распределение сентиментов по методам
        plt.figure(figsize=(15, 5))
        for i, method in enumerate(self.analyzers.keys(), 1):
            plt.subplot(1, 2, i)
            sentiment_counts = df[f'{method}_sentiment'].value_counts()
            plt.pie(sentiment_counts, labels=sentiment_counts.index, autopct='%1.1f%%')
            plt.title(f'Распределение сентиментов ({method})')
        plt.tight_layout()
        plt.savefig('sentiment_distribution_comparison.png')
        plt.close()
        print("График распределения сентиментов сохранен")
        
        # 2. Корреляция между методами
        plt.figure(figsize=(8, 6))
        score_columns = [f'{method}_score' for method in self.analyzers.keys()]
        corr_matrix = df[score_columns].corr()
        sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', center=0)
        plt.title('Корреляция между методами анализа')
        plt.tight_layout()
        plt.savefig('sentiment_correlation_comparison.png')
        plt.close()
        print("График корреляции сохранен")
        
        # 3. Сравнение с оценками
        plt.figure(figsize=(15, 5))
        for i, method in enumerate(self.analyzers.keys(), 1):
            plt.subplot(1, 2, i)
            sentiment_by_rating = df.groupby('review_rating')[f'{method}_sentiment'].value_counts().unstack()
            sentiment_by_rating.plot(kind='bar', stacked=True)
            plt.title(f'Распределение сентиментов по оценкам ({method})')
            plt.xlabel('Оценка')
            plt.ylabel('Количество отзывов')
        plt.tight_layout()
        plt.savefig('sentiment_rating_comparison.png')
        plt.close()
        print("График сравнения с оценками сохранен")

def main():
    # Загрузка данных
    print("Загрузка данных...")
    df = pd.read_csv('processed_data_1_20.csv', sep=',', encoding='utf-8-sig')
    print(f"Загружено {len(df)} отзывов")
    
    # Инициализация сравнения
    comparison = SentimentComparison()
    
    # Анализ датасета
    print("\nАнализ датасета...")
    results_df = comparison.analyze_dataset(df)
    
    # Построение графиков
    comparison.plot_comparison(results_df)
    
    # Сохранение результатов
    print("\nСохранение результатов...")
    results_df.to_csv('sentiment_comparison_results.csv', 
                     index=False, 
                     encoding='utf-8-sig',
                     sep=',')
    
    print("\nАнализ завершен. Результаты сохранены в 'sentiment_comparison_results.csv'")
    print("Графики сравнения сохранены в файлы:")
    print("- sentiment_distribution_comparison.png")
    print("- sentiment_correlation_comparison.png")
    print("- sentiment_rating_comparison.png")

if __name__ == "__main__":
    main() 