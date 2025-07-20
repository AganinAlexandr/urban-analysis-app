"""
Модуль анализа сентиментов отзывов

Назначение:
    Анализ эмоциональной окраски текстов отзывов с использованием NLTK и VADER.
    Классификация отзывов по типам и визуализация результатов анализа.
    Разделение сложных отзывов на положительные и отрицательные части.

Ключевые компоненты:
    - SentimentAnalyzer: класс для анализа сентиментов
    - Функции визуализации: plot_correlation_matrix, plot_distributions, plot_sentiment_distribution
    - Функции классификации: determine_review_type, determine_response_type
    - Функции разделения отзывов: split_complex_review

Зависимости:
    - pandas: обработка данных
    - numpy: математические операции
    - matplotlib: базовые графики
    - seaborn: улучшенные визуализации
    - nltk: обработка естественного языка
    - csv: работа с CSV файлами
    - plotly: создание интерактивных визуализаций

Требования к данным:
    - Входной CSV файл должен содержать колонки:
        * review_text: текст отзыва
        * answer_text: текст ответа
        * review_rating: числовая оценка

Выходные данные:
    - analyzed_data.csv: обогащенный датасет с результатами анализа
    - correlation_matrix.png: матрица корреляций
    - sentiment_distribution.png: распределение сентиментов
    - review_type_distribution.png: распределение типов отзывов
    - response_type_distribution.png: распределение типов ответов
    - combined_distribution.png: комбинированное распределение
    - review_parts_analysis.png: анализ частей сложных отзывов
    - 3d_analysis_words_threshold.html: интерактивная 3D визуализация для анализа параметров
    - 3d_analysis_ratings.html: интерактивная 3D визуализация для анализа параметров
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import nltk
from nltk.tokenize import sent_tokenize
from nltk.sentiment import SentimentIntensityAnalyzer
import re
from typing import List, Dict, Tuple
import warnings
import csv
import plotly.graph_objects as go
from itertools import product
warnings.filterwarnings('ignore')

# Константы
GEОCODER_ON = False  # Флаг для включения/отключения геокодера

# Загрузка необходимых ресурсов NLTK
nltk.download('punkt')
nltk.download('vader_lexicon')

class SentimentAnalyzer:
    def __init__(self):
        # Инициализация анализатора
        self.sia = SentimentIntensityAnalyzer()
        
        # Списки положительных и отрицательных слов
        self.positive_words = ["хорошо", "отлично", "прекрасно", "замечательно", "превосходно", 
                             "компетентно", "профессионально", "качественно", "чисто", "быстро", 
                             "аккуратно", "классно", "уютно", "доволен", "впечатляет", "современно", 
                             "идеально", "круто", "рад", "доволен", "удобно"]
        
        self.negative_words = ["плохо", "ужасно", "отвратительно", "некомпетентно", "непрофессионально", 
                             "бестолково", "неразумно", "эскулап", "бездарь", "грязно", "медленно", 
                             "кошмар", "разочарование", "неудобно"]
        
        # Пороговые значения для определения сложных отзывов
        self.complex_threshold = 0.05  # Уменьшен с 0.3 до 0.05
        self.sentence_threshold = 0.025  # Уменьшен с 0.1 до 0.025
        self.min_words_threshold = 30  # Оставлен без изменений
        self.min_positive_words = 1  # Оставлен без изменений
        self.min_negative_words = 1  # Оставлен без изменений
        self.complex_ratings = [2, 3, 4]  # Оставлен без изменений

    def analyze_sentiment(self, text: str) -> Tuple[str, float]:
        """Анализ сентимента текста"""
        scores = self.sia.polarity_scores(text)
        compound_score = scores['compound']
        
        if compound_score >= 0.05:
            sentiment = 'positive'
        elif compound_score <= -0.05:
            sentiment = 'negative'
        else:
            sentiment = 'neutral'
            
        return sentiment, abs(compound_score)

    def get_word_sentiment_ratio(self, text: str) -> Tuple[float, float]:
        """Подсчет доли положительных и отрицательных слов"""
        words = text.lower().split()
        total_words = len(words)
        if total_words == 0:
            return 0.0, 0.0
            
        positive_count = sum(1 for word in words if word in self.positive_words)
        negative_count = sum(1 for word in words if word in self.negative_words)
        
        return positive_count/total_words, negative_count/total_words

    def analyze_text(self, text: str) -> Dict:
        """Полный анализ текста"""
        if not isinstance(text, str) or not text.strip():
            return {
                'sentiment': 'neutral',
                'confidence': 0.0,
                'sentence_sentiments': [],
                'sentiment_variance': 0.0,
                'positive_word_ratio': 0.0,
                'negative_word_ratio': 0.0
            }

        # Анализ всего текста
        sentiment, confidence = self.analyze_sentiment(text)

        # Анализ по предложениям
        sentences = sent_tokenize(text)
        sentence_sentiments = []
        for sentence in sentences:
            sentiment, _ = self.analyze_sentiment(sentence)
            sentence_sentiments.append(sentiment)

        # Расчет дисперсии сентимента
        sentiment_map = {'negative': 0, 'neutral': 1, 'positive': 2}
        sentiment_values = [sentiment_map[s] for s in sentence_sentiments]
        sentiment_variance = np.var(sentiment_values) if sentiment_values else 0.0

        # Подсчет долей слов
        positive_ratio, negative_ratio = self.get_word_sentiment_ratio(text)

        return {
            'sentiment': sentiment,
            'confidence': confidence,
            'sentence_sentiments': sentence_sentiments,
            'sentiment_variance': sentiment_variance,
            'positive_word_ratio': positive_ratio,
            'negative_word_ratio': negative_ratio
        }

    def is_complex_review(self, text: str, rating: int = None) -> bool:
        """
        Определение, является ли отзыв сложным
        
        Args:
            text (str): Текст отзыва
            rating (int, optional): Оценка отзыва
            
        Returns:
            bool: True если отзыв сложный, False иначе
        """
        if not isinstance(text, str) or not text.strip():
            return False
            
        # Проверка длины отзыва
        words = text.split()
        if len(words) < self.min_words_threshold:
            return False
            
        # Проверка оценки (если предоставлена)
        if rating is not None and rating not in self.complex_ratings:
            return False
            
        # Подсчет положительных и отрицательных слов
        positive_count, negative_count = self.count_sentiment_words(text)
        if positive_count < self.min_positive_words or negative_count < self.min_negative_words:
            return False
            
        # Отзыв считается сложным, если содержит достаточное количество
        # положительных и отрицательных слов
        is_complex = (positive_count >= self.min_positive_words and 
                     negative_count >= self.min_negative_words)
        
        # Отладочная информация
        if rating in self.complex_ratings and len(words) >= self.min_words_threshold:
            print(f"\nОтладочная информация для отзыва с оценкой {rating}:")
            print(f"Длина отзыва: {len(words)} слов")
            print(f"Положительных слов: {positive_count}")
            print(f"Отрицательных слов: {negative_count}")
            print(f"Является сложным: {is_complex}")
        
        return is_complex

    def split_complex_review(self, text: str) -> Dict[str, List[str]]:
        """
        Разделение сложного отзыва на положительные и отрицательные части
        
        Args:
            text (str): Текст отзыва
            
        Returns:
            Dict[str, List[str]]: Словарь с положительными и отрицательными частями
        """
        if not isinstance(text, str) or not text.strip():
            return {'positive': [], 'negative': []}
            
        # Разбиваем текст на предложения
        sentences = sent_tokenize(text)
        
        positive_parts = []
        negative_parts = []
        
        for sentence in sentences:
            # Подсчитываем положительные и отрицательные слова в предложении
            positive_count, negative_count = self.count_sentiment_words(sentence)
            
            # Определяем тип предложения по преобладанию слов
            if positive_count > negative_count:
                positive_parts.append(sentence)
            elif negative_count > positive_count:
                negative_parts.append(sentence)
                
        return {
            'positive': positive_parts,
            'negative': negative_parts
        }

    def count_sentiment_words(self, text: str) -> Tuple[int, int]:
        """Подсчет количества положительных и отрицательных слов в тексте"""
        words = text.lower().split()
        positive_count = sum(1 for word in words if word in self.positive_words)
        negative_count = sum(1 for word in words if word in self.negative_words)
        return positive_count, negative_count

    def analyze_complex_review(self, text: str) -> Dict:
        """
        Анализ сложного отзыва
        
        Args:
            text (str): Текст отзыва
            
        Returns:
            Dict: Результаты анализа сложного отзыва
        """
        parts = self.split_complex_review(text)
        
        # Анализируем каждую часть
        positive_sentiment = self.analyze_text(' '.join(parts['positive']))
        negative_sentiment = self.analyze_text(' '.join(parts['negative']))
        
        # Вычисляем соотношение частей с проверкой на деление на ноль
        total_parts = len(parts['positive']) + len(parts['negative'])
        if total_parts == 0:
            positive_ratio = 0.0
            negative_ratio = 0.0
        else:
            positive_ratio = len(parts['positive']) / total_parts
            negative_ratio = len(parts['negative']) / total_parts
        
        return {
            'is_complex': self.is_complex_review(text),
            'positive_parts': parts['positive'],
            'negative_parts': parts['negative'],
            'positive_sentiment': positive_sentiment,
            'negative_sentiment': negative_sentiment,
            'positive_ratio': positive_ratio,
            'negative_ratio': negative_ratio
        }

def plot_correlation_matrix(df: pd.DataFrame):
    """Построение матрицы корреляций"""
    # Преобразование категориальных переменных в числовые
    sentiment_map = {'negative': 0, 'neutral': 1, 'positive': 2}
    df['sentiment_num'] = df['sentiment'].map(sentiment_map)
    
    # Выбор числовых колонок для корреляции
    numeric_cols = ['review_rating', 'sentiment_num', 'sentiment_variance', 
                   'positive_word_ratio', 'negative_word_ratio']
    
    # Расчет корреляций
    corr_matrix = df[numeric_cols].corr()
    
    # Построение тепловой карты
    plt.figure(figsize=(10, 8))
    sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', center=0)
    plt.title('Матрица корреляций признаков')
    plt.tight_layout()
    plt.savefig('correlation_matrix.png')
    plt.close()

def plot_distributions(df: pd.DataFrame):
    """Построение распределений оценок"""
    plt.style.use('seaborn')
    
    # 1. Распределение оценок по сентиментам
    plt.figure(figsize=(10, 6))
    sentiment_counts = df.groupby(['review_rating', 'sentiment']).size().unstack(fill_value=0)
    sentiment_counts.plot(kind='bar', stacked=True)
    plt.title('Распределение оценок по сентиментам')
    plt.xlabel('Оценка')
    plt.ylabel('Количество отзывов')
    plt.legend(title='Сентимент')
    plt.tight_layout()
    plt.savefig('sentiment_distribution.png')
    plt.close()
    
    # 2. Распределение оценок по типам отзывов
    plt.figure(figsize=(10, 6))
    review_type_counts = df.groupby(['review_rating', 'review_type']).size().unstack(fill_value=0)
    review_type_counts.plot(kind='bar', stacked=True)
    plt.title('Распределение оценок по типам отзывов')
    plt.xlabel('Оценка')
    plt.ylabel('Количество отзывов')
    plt.legend(title='Тип отзыва')
    plt.tight_layout()
    plt.savefig('review_type_distribution.png')
    plt.close()
    
    # 3. Распределение оценок по типам ответов
    plt.figure(figsize=(10, 6))
    response_type_counts = df.groupby(['review_rating', 'response_type']).size().unstack(fill_value=0)
    response_type_counts.plot(kind='bar', stacked=True)
    plt.title('Распределение оценок по типам ответов')
    plt.xlabel('Оценка')
    plt.ylabel('Количество отзывов')
    plt.legend(title='Тип ответа')
    plt.tight_layout()
    plt.savefig('response_type_distribution.png')
    plt.close()
    
    # 4. Распределение оценок по типам отзывов и ответов
    plt.figure(figsize=(12, 8))
    combined_counts = df.groupby(['review_rating', 'review_type', 'response_type']).size().unstack(fill_value=0)
    combined_counts.plot(kind='bar', stacked=True)
    plt.title('Распределение оценок по типам отзывов и ответов')
    plt.xlabel('Оценка')
    plt.ylabel('Количество отзывов')
    plt.legend(title='Тип отзыва/ответа')
    plt.tight_layout()
    plt.savefig('combined_distribution.png')
    plt.close()

def determine_review_type(text: str) -> str:
    if not isinstance(text, str):
        text = ""
    text = text.lower()
    if any(word in text for word in ['спасибо', 'благодарю', 'благодарность']):
        return 'благодарность'
    elif any(word in text for word in ['жалоба', 'жалоб', 'недоволен', 'недовольны']):
        return 'жалоба'
    elif any(word in text for word in ['предложение', 'предлагаю', 'рекомендация']):
        return 'предложение'
    else:
        return 'информационный'

def determine_response_type(text: str) -> str:
    if not isinstance(text, str):
        text = ""
    text = text.lower()
    if any(word in text for word in ['спасибо', 'благодарю', 'благодарность']):
        return 'благодарность'
    elif any(word in text for word in ['приносим извинения', 'приношу извинения', 'сожалеем']):
        return 'извинение'
    elif any(word in text for word in ['будет исправлено', 'будет рассмотрено', 'примем меры']):
        return 'обещание'
    else:
        return 'информационный'

def plot_sentiment_distribution(df: pd.DataFrame):
    """Построение распределения сентиментов"""
    plt.figure(figsize=(10, 6))
    sentiment_counts = df['sentiment'].value_counts()
    plt.pie(sentiment_counts, labels=sentiment_counts.index, autopct='%1.1f%%')
    plt.title('Распределение сентиментов')
    plt.tight_layout()
    plt.savefig('sentiment_distribution.png')
    plt.close()

def plot_review_parts_analysis(df: pd.DataFrame):
    """Построение анализа частей сложных отзывов"""
    # Фильтруем сложные отзывы
    complex_reviews = df[df['is_complex'] == True]
    
    if len(complex_reviews) == 0:
        print("Сложные отзывы не найдены")
        return
        
    # 1. Распределение соотношения положительных и отрицательных частей
    plt.figure(figsize=(10, 6))
    plt.hist(complex_reviews['positive_ratio'], bins=20, alpha=0.5, label='Положительные')
    plt.hist(complex_reviews['negative_ratio'], bins=20, alpha=0.5, label='Отрицательные')
    plt.title('Распределение соотношения частей в сложных отзывах')
    plt.xlabel('Доля частей')
    plt.ylabel('Количество отзывов')
    plt.legend()
    plt.tight_layout()
    plt.savefig('review_parts_ratio.png')
    plt.close()
    
    # 2. Корреляция между оценкой и соотношением частей
    plt.figure(figsize=(10, 6))
    plt.scatter(complex_reviews['review_rating'], complex_reviews['positive_ratio'])
    plt.title('Корреляция между оценкой и долей положительных частей')
    plt.xlabel('Оценка')
    plt.ylabel('Доля положительных частей')
    plt.tight_layout()
    plt.savefig('review_parts_correlation.png')
    plt.close()

def process_geodata(address: str) -> Tuple[float, float]:
    """
    Обработка геоданных с учетом флага GEОCODER_ON
    
    Args:
        address (str): Адрес для геокодирования
        
    Returns:
        Tuple[float, float]: Координаты (широта, долгота)
    """
    if not GEОCODER_ON:
        # Возвращаем тестовые координаты при отключенном геокодере
        return 55.7558, 37.6173  # Москва
        
    try:
        # Здесь код для работы с геокодером
        # geocoder = Geocoder()
        # coordinates = geocoder.get_coordinates(address)
        # return coordinates
        pass
    except Exception as e:
        print(f"Ошибка при геокодировании адреса {address}: {str(e)}")
        return None, None

def plot_feature_analysis(df: pd.DataFrame, feature_name: str, base_value: float = None, 
                         step: float = 0.1, num_steps: int = 3):
    """
    Построение диаграммы анализа признака
    
    Args:
        df: DataFrame с данными
        feature_name: Название признака
        base_value: Базовое значение признака (опционально)
        step: Шаг изменения значения
        num_steps: Количество шагов в каждую сторону
    """
    plt.figure(figsize=(15, 8))
    
    # Создаем подграфики
    ax1 = plt.gca()
    ax2 = ax1.twinx()
    
    if feature_name == 'rating':
        # Специальная обработка для оценок
        values = sorted(df['review_rating'].unique())
        counts = []
        cumulative_percentages = []
        total_reviews = len(df)
        
        for value in values:
            count = len(df[df['review_rating'] == value])
            counts.append(count)
            cumulative_percentages.append(count / total_reviews * 100)
            
        # Построение графика для оценок
        ax1.bar(values, counts, alpha=0.6, color='blue', label='Количество отзывов')
        ax1.set_xlabel('Оценка отзыва')
        ax1.set_ylabel('Количество отзывов', color='blue')
        ax1.tick_params(axis='y', labelcolor='blue')
        
        # Добавление линии накопительного процента
        ax2.plot(values, cumulative_percentages, 'r-', label='Накопительный процент')
        ax2.set_ylabel('Накопительный процент', color='red')
        ax2.tick_params(axis='y', labelcolor='red')
        
        # Добавление вертикальных линий для границ сложных отзывов
        ax1.axvline(x=1.5, color='green', linestyle='--', label='Граница сложных отзывов')
        ax1.axvline(x=4.5, color='green', linestyle='--')
        
    else:
        # Создаем сетку значений признака
        values = [base_value + i * step for i in range(-num_steps, num_steps + 1)]
        sorted_values = sorted(values, reverse=True)
        
        # Считаем количество отзывов для каждого значения признака
        counts = []
        cumulative_percentages = []
        total_reviews = len(df)
        
        for value in sorted_values:
            if feature_name == 'complex_threshold':
                count = len(df[df['sentiment_variance'] >= value])
            elif feature_name == 'sentence_threshold':
                count = len(df[df['confidence'] >= value])
            elif feature_name == 'min_words_threshold':
                count = len(df[df['review_text'].str.split().str.len() >= value])
            elif feature_name == 'min_positive_words':
                count = len(df[df['positive_word_ratio'] * df['review_text'].str.split().str.len() >= value])
            elif feature_name == 'min_negative_words':
                count = len(df[df['negative_word_ratio'] * df['review_text'].str.split().str.len() >= value])
            
            counts.append(count)
            cumulative_percentages.append(count / total_reviews * 100)
        
        # Построение графика
        ax1.bar(sorted_values, counts, alpha=0.6, color='blue', label='Количество отзывов')
        ax1.set_xlabel(f'Значение признака {feature_name}')
        ax1.set_ylabel('Количество отзывов', color='blue')
        ax1.tick_params(axis='y', labelcolor='blue')
        
        # Добавление линии накопительного процента
        ax2.plot(sorted_values, cumulative_percentages, 'r-', label='Накопительный процент')
        ax2.set_ylabel('Накопительный процент', color='red')
        ax2.tick_params(axis='y', labelcolor='red')
        
        # Добавление вертикальной линии для базового значения
        ax1.axvline(x=base_value, color='green', linestyle='--', label='Базовое значение')
    
    plt.title(f'Анализ признака {feature_name}')
    
    # Объединение легенд
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper right')
    
    plt.tight_layout()
    plt.savefig(f'feature_analysis_{feature_name}.png')
    plt.close()

def analyze_features(df: pd.DataFrame):
    """
    Анализ всех признаков для определения сложных отзывов
    """
    # Базовые значения признаков
    features = {
        'rating': None,  # Специальный случай для оценок
        'complex_threshold': 0.3,
        'sentence_threshold': 0.1,
        'min_words_threshold': 30,
        'min_positive_words': 1,
        'min_negative_words': 1
    }
    
    # Шаги для каждого признака
    steps = {
        'rating': None,  # Не используется для оценок
        'complex_threshold': 0.1,
        'sentence_threshold': 0.05,
        'min_words_threshold': 5,
        'min_positive_words': 1,
        'min_negative_words': 1
    }
    
    # Построение диаграмм для каждого признака
    for feature, base_value in features.items():
        plot_feature_analysis(df, feature, base_value, steps[feature])

def create_3d_visualization(df: pd.DataFrame):
    """
    Создание интерактивных 3D визуализаций для анализа параметров
    """
    # Создаем сетки значений для параметров
    positive_words_range = range(1, 4)  # 1-3 слова
    negative_words_range = range(1, 4)  # 1-3 слова
    words_threshold_range = range(20, 41, 5)  # 20-40 слов с шагом 5
    ratings = [1, 2, 3, 4, 5]
    
    # 1. Визуализация (min_positive_words, min_negative_words, min_words_threshold)
    data1 = []
    for pos, neg, thresh in product(positive_words_range, negative_words_range, words_threshold_range):
        count = len(df[
            (df['review_text'].str.split().str.len() >= thresh) &
            (df['positive_word_ratio'] * df['review_text'].str.split().str.len() >= pos) &
            (df['negative_word_ratio'] * df['review_text'].str.split().str.len() >= neg)
        ])
        data1.append([pos, neg, thresh, count])
    
    data1 = np.array(data1)
    
    fig1 = go.Figure(data=[go.Scatter3d(
        x=data1[:, 0],
        y=data1[:, 1],
        z=data1[:, 2],
        mode='markers',
        marker=dict(
            size=data1[:, 3] / 2,  # Увеличили размер в 5 раз (было /10)
            color=data1[:, 3],
            colorscale='Viridis',
            showscale=True,
            colorbar=dict(title='Количество отзывов')
        ),
        text=[f'Положительных слов: {x}<br>Отрицательных слов: {y}<br>Порог длины: {z}<br>Количество: {c}'
              for x, y, z, c in data1]
    )])
    
    fig1.update_layout(
        title='Анализ параметров (min_positive_words, min_negative_words, min_words_threshold)',
        scene=dict(
            xaxis_title='min_positive_words',
            yaxis_title='min_negative_words',
            zaxis_title='min_words_threshold'
        ),
        width=1000,
        height=800
    )
    
    fig1.write_html('3d_analysis_words_threshold.html')
    
    # 2. Визуализация (min_positive_words, min_negative_words, complex_ratings)
    data2 = []
    for pos, neg, rating in product(positive_words_range, negative_words_range, ratings):
        count = len(df[
            (df['positive_word_ratio'] * df['review_text'].str.split().str.len() >= pos) &
            (df['negative_word_ratio'] * df['review_text'].str.split().str.len() >= neg) &
            (df['review_rating'] == rating)
        ])
        data2.append([pos, neg, rating, count])
    
    data2 = np.array(data2)
    
    fig2 = go.Figure(data=[go.Scatter3d(
        x=data2[:, 0],
        y=data2[:, 1],
        z=data2[:, 2],
        mode='markers',
        marker=dict(
            size=data2[:, 3] / 0.5,  # Увеличили размер в 10 раз (было /5)
            color=data2[:, 3],
            colorscale='Viridis',
            showscale=True,
            colorbar=dict(title='Количество отзывов')
        ),
        text=[f'Положительных слов: {x}<br>Отрицательных слов: {y}<br>Оценка: {z}<br>Количество: {c}'
              for x, y, z, c in data2]
    )])
    
    fig2.update_layout(
        title='Анализ параметров (min_positive_words, min_negative_words, complex_ratings)',
        scene=dict(
            xaxis_title='min_positive_words',
            yaxis_title='min_negative_words',
            zaxis_title='Оценка'
        ),
        width=1000,
        height=800
    )
    
    fig2.write_html('3d_analysis_ratings.html')

def analyze_with_different_thresholds(df: pd.DataFrame):
    """
    Анализ данных с разными пороговыми значениями
    
    Args:
        df: DataFrame с данными
    """
    # Создаем анализатор
    analyzer = SentimentAnalyzer()
    
    # Тестируем разные комбинации порогов
    thresholds = [
        (0.05, 0.025),  # Новые значения
        (0.1, 0.05),    # Промежуточные значения
        (0.3, 0.1)      # Старые значения
    ]
    
    results = []
    
    # Анализ распределения оценок
    print("\nРаспределение оценок в датасете:")
    rating_distribution = df['review_rating'].value_counts().sort_index()
    print(rating_distribution)
    
    # Анализ распределения длины отзывов
    print("\nСтатистика по длине отзывов:")
    df['word_count'] = df['review_text'].str.split().str.len()
    word_count_stats = df['word_count'].describe()
    print(word_count_stats)
    
    for complex_thresh, sentence_thresh in thresholds:
        # Устанавливаем новые пороги
        analyzer.complex_threshold = complex_thresh
        analyzer.sentence_threshold = sentence_thresh
        
        print(f"\nАнализ с порогами complex_threshold={complex_thresh}, sentence_threshold={sentence_thresh}")
        
        # Анализируем отзывы
        complex_count = 0
        total_reviews = len(df)
        
        # Счетчики для отладки
        rating_filtered = 0
        length_filtered = 0
        words_filtered = 0
        parts_filtered = 0
        
        for idx, row in df.iterrows():
            # Проверка оценки
            if row['review_rating'] not in analyzer.complex_ratings:
                rating_filtered += 1
                continue
                
            # Проверка длины
            words = row['review_text'].split()
            if len(words) < analyzer.min_words_threshold:
                length_filtered += 1
                continue
                
            # Проверка слов
            positive_count, negative_count = analyzer.count_sentiment_words(row['review_text'])
            if positive_count < analyzer.min_positive_words or negative_count < analyzer.min_negative_words:
                words_filtered += 1
                continue
                
            # Проверка частей
            parts = analyzer.split_complex_review(row['review_text'])
            if not (len(parts['positive']) > 0 and len(parts['negative']) > 0):
                parts_filtered += 1
                continue
                
            if analyzer.is_complex_review(row['review_text'], row['review_rating']):
                complex_count += 1
        
        # Собираем статистику
        result = {
            'complex_threshold': complex_thresh,
            'sentence_threshold': sentence_thresh,
            'complex_reviews_count': complex_count,
            'complex_reviews_percentage': (complex_count / total_reviews) * 100,
            'rating_filtered': rating_filtered,
            'length_filtered': length_filtered,
            'words_filtered': words_filtered,
            'parts_filtered': parts_filtered
        }
        results.append(result)
    
    # Создаем DataFrame с результатами
    results_df = pd.DataFrame(results)
    
    # Сохраняем результаты
    results_df.to_csv('threshold_analysis_results.csv', 
                     index=False, 
                     encoding='utf-8-sig',
                     sep=';',
                     quoting=csv.QUOTE_MINIMAL)
    
    # Выводим результаты
    print("\nРезультаты анализа с разными порогами:")
    print(results_df.to_string(index=False))
    
    return results_df

def main():
    # Загрузка данных
    print("Загрузка данных из файла processed_data_1_20.csv")
    df = pd.read_csv('processed_data_1_20.csv', sep=',', quoting=csv.QUOTE_MINIMAL, encoding='utf-8-sig')
    
    # Анализ с разными пороговыми значениями
    print("\nАнализ с разными пороговыми значениями...")
    threshold_results = analyze_with_different_thresholds(df)
    
    # Обработка геоданных только если включен геокодер
    if GEОCODER_ON:
        print("\nОбработка геоданных...")
        df['coordinates'] = df['address'].apply(process_geodata)
    else:
        print("\nГеокодер отключен, пропускаем обработку геоданных")
    
    # Определение типов отзывов и ответов
    print("\nОпределение типов отзывов и ответов...")
    df['review_type'] = df['review_text'].apply(determine_review_type)
    df['response_type'] = df['answer_text'].apply(determine_response_type)
    
    # Инициализация анализатора
    analyzer = SentimentAnalyzer()
    
    # Анализ текстов
    print("\nАнализ отзывов...")
    sentiment_results = []
    complex_review_results = []
    
    for idx, row in df.iterrows():
        text = row['review_text']
        rating = row['review_rating']
        
        # Базовый анализ
        results = analyzer.analyze_text(text)
        sentiment_results.append(results)
        
        # Анализ сложных отзывов с учетом оценки
        complex_results = analyzer.analyze_complex_review(text)
        complex_results['is_complex'] = analyzer.is_complex_review(text, rating)
        complex_review_results.append(complex_results)
    
    # Добавление результатов в DataFrame
    sentiment_df = pd.DataFrame(sentiment_results)
    complex_df = pd.DataFrame(complex_review_results)
    df = pd.concat([df, sentiment_df, complex_df], axis=1)
    
    # Анализ признаков
    print("\nАнализ признаков для определения сложных отзывов...")
    analyze_features(df)
    
    # Создание 3D визуализаций
    print("\nСоздание 3D визуализаций...")
    create_3d_visualization(df)
    
    # Сохранение результатов с правильной кодировкой
    print("\nСохранение результатов в файл analyzed_data.csv")
    df.to_csv('analyzed_data.csv', 
              index=False, 
              encoding='utf-8-sig',
              sep=',',
              quoting=csv.QUOTE_MINIMAL)
    
    # Построение визуализаций
    print("\nПостроение визуализаций...")
    plot_correlation_matrix(df)
    plot_distributions(df)
    plot_sentiment_distribution(df)
    plot_review_parts_analysis(df)
    
    print("\nАнализ завершен. Результаты сохранены в 'analyzed_data.csv'")
    print("Диаграммы анализа признаков сохранены в файлы feature_analysis_*.png")
    print("3D визуализации сохранены в файлы 3d_analysis_*.html")

if __name__ == "__main__":
    main() 