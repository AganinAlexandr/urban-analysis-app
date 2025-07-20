import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from typing import List, Dict, Tuple
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from gensim.models import Word2Vec
from sklearn.feature_extraction.text import TfidfVectorizer
import re
import os

class TextAnalyzer:
    def __init__(self):
        """
        Инициализация анализатора текста
        """
        # Загрузка необходимых ресурсов NLTK
        try:
            nltk.data.find('tokenizers/punkt')
        except LookupError:
            nltk.download('punkt')
        try:
            nltk.data.find('corpora/stopwords')
        except LookupError:
            nltk.download('stopwords')
        
        self.stop_words = set(stopwords.words('russian'))
        self.word2vec_model = None
        
    def preprocess_text(self, text: str) -> str:
        """
        Предобработка текста
        
        Args:
            text (str): Исходный текст
            
        Returns:
            str: Обработанный текст
        """
        if not isinstance(text, str):
            return ""
            
        # Приведение к нижнему регистру
        text = text.lower()
        
        # Удаление специальных символов
        text = re.sub(r'[^\w\s]', ' ', text)
        
        # Удаление лишних пробелов
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
        
    def extract_keywords(self, text: str, n_keywords: int = 3) -> List[str]:
        """
        Извлечение ключевых слов из текста
        
        Args:
            text (str): Исходный текст
            n_keywords (int): Количество ключевых слов
            
        Returns:
            List[str]: Список ключевых слов
        """
        text = self.preprocess_text(text)
        if not text:
            return []
            
        # Токенизация
        tokens = word_tokenize(text)
        
        # Удаление стоп-слов
        tokens = [token for token in tokens if token not in self.stop_words]
        
        # Подсчет частоты слов
        word_freq = {}
        for token in tokens:
            if token in word_freq:
                word_freq[token] += 1
            else:
                word_freq[token] = 1
                
        # Сортировка по частоте
        sorted_words = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)
        
        return [word for word, _ in sorted_words[:n_keywords]]
        
    def determine_sentiment(self, text: str) -> str:
        """
        Определение окраски сообщения
        
        Args:
            text (str): Исходный текст
            
        Returns:
            str: Окраска сообщения
        """
        text = self.preprocess_text(text)
        if not text:
            return "неизвестно"
            
        # Простой словарь для определения окраски
        sentiment_words = {
            'благодарность': ['спасибо', 'благодарю', 'благодарность', 'отличный', 'хороший'],
            'восхищение': ['восхитительно', 'прекрасно', 'замечательно', 'супер', 'отлично'],
            'недовольство': ['плохо', 'ужасно', 'кошмар', 'недоволен', 'проблема'],
            'предложение': ['предлагаю', 'можно', 'нужно', 'следует', 'рекомендую']
        }
        
        # Подсчет совпадений
        sentiment_scores = {sentiment: 0 for sentiment in sentiment_words}
        for sentiment, words in sentiment_words.items():
            for word in words:
                if word in text:
                    sentiment_scores[sentiment] += 1
                    
        # Определение преобладающей окраски
        max_score = max(sentiment_scores.values())
        if max_score == 0:
            return "нейтральный"
            
        return max(sentiment_scores.items(), key=lambda x: x[1])[0]
        
    def train_word2vec(self, texts: List[str], vector_size: int = 100, window: int = 5, min_count: int = 1):
        """
        Обучение модели Word2Vec на текстах отзывов
        
        Args:
            texts (List[str]): Список текстов отзывов
            vector_size (int): Размерность векторов
            window (int): Размер окна контекста
            min_count (int): Минимальная частота слова
        """
        print("Подготовка текстов для Word2Vec...")
        
        # Обрабатываем тексты
        processed_texts = []
        for text in texts:
            if not text:
                continue
                
            # Предобработка текста
            text = self.preprocess_text(text)
            
            # Простая токенизация по пробелам
            words = text.split()
            
            # Удаляем стоп-слова и короткие слова
            words = [word for word in words if word not in self.stop_words and len(word) > 2]
            
            if words:  # Добавляем только непустые списки слов
                processed_texts.append(words)
        
        if not processed_texts:
            print("Предупреждение: нет текстов для обучения Word2Vec")
            return
            
        print(f"Подготовлено {len(processed_texts)} текстов для обучения")
        
        # Обучаем модель
        print("Обучение модели Word2Vec...")
        self.word2vec_model = Word2Vec(
            sentences=processed_texts,
            vector_size=vector_size,
            window=window,
            min_count=min_count,
            workers=4
        )
        
        print("Модель Word2Vec обучена")
        print(f"Размер словаря: {len(self.word2vec_model.wv)}")
        
        # Сохраняем модель
        if not os.path.exists('results'):
            os.makedirs('results')
        self.word2vec_model.save('results/word2vec_model.bin')
        print("Модель сохранена в 'results/word2vec_model.bin'")
        
    def get_text_embedding(self, text: str) -> np.ndarray:
        """
        Получение embedding для текста
        
        Args:
            text (str): Исходный текст
            
        Returns:
            np.ndarray: Вектор embedding
        """
        if not self.word2vec_model or not text:
            return np.zeros(100)
            
        # Токенизация текста
        tokens = word_tokenize(self.preprocess_text(text))
        
        # Получение векторов для каждого слова
        word_vectors = []
        for token in tokens:
            try:
                word_vectors.append(self.word2vec_model.wv[token])
            except KeyError:
                continue
                
        if not word_vectors:
            return np.zeros(100)
            
        # Усреднение векторов
        return np.mean(word_vectors, axis=0)
        
    def create_visualizations(self, df: pd.DataFrame, output_dir: str = "diagrams"):
        """
        Создание визуализаций
        
        Args:
            df (pd.DataFrame): DataFrame с данными
            output_dir (str): Директория для сохранения визуализаций
        """
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        # 1. Гистограмма распределения количества объектов по группам и источникам
        plt.figure(figsize=(12, 6))
        sns.countplot(data=df, x='group', hue='source')
        plt.xticks(rotation=45)
        plt.title('Распределение количества объектов по группам и источникам')
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, 'objects_distribution.png'))
        plt.close()
        
        # 2. Гистограмма распределения количества отзывов
        plt.figure(figsize=(12, 6))
        review_counts = df.groupby(['group', 'source']).size().reset_index(name='count')
        sns.barplot(data=review_counts, x='group', y='count', hue='source')
        plt.xticks(rotation=45)
        plt.title('Распределение количества отзывов по группам и источникам')
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, 'reviews_distribution.png'))
        plt.close()
        
        # 3. Гистограмма распределения рейтингов
        plt.figure(figsize=(12, 6))
        sns.boxplot(data=df, x='group', y='review_rating', hue='source')
        plt.xticks(rotation=45)
        plt.title('Распределение рейтингов по группам и источникам')
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, 'ratings_distribution.png'))
        plt.close()
        
        # 4. Тепловая карта корреляции рейтингов и embeddings
        if self.word2vec_model:
            # Получение embeddings для текстов отзывов
            df['review_embedding'] = df['review_text'].apply(self.get_text_embedding)
            
            # Создание тепловой карты
            plt.figure(figsize=(10, 8))
            correlation_matrix = df[['review_rating'] + [f'embedding_{i}' for i in range(100)]].corr()
            sns.heatmap(correlation_matrix.iloc[0:1, 1:], cmap='coolwarm')
            plt.title('Корреляция рейтингов и embeddings')
            plt.tight_layout()
            plt.savefig(os.path.join(output_dir, 'ratings_embeddings_correlation.png'))
            plt.close()
            
        # 5. Распределение окраски сообщений
        df['sentiment'] = df['review_text'].apply(self.determine_sentiment)
        plt.figure(figsize=(12, 6))
        sns.countplot(data=df, x='group', hue='sentiment')
        plt.xticks(rotation=45)
        plt.title('Распределение окраски сообщений по группам')
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, 'sentiment_distribution.png'))
        plt.close()

if __name__ == "__main__":
    # Пример использования
    from data_processor import DataProcessor
    
    # Загрузка данных
    processor = DataProcessor("parsed")
    processor.process_all_data()
    df = processor.get_dataframe()
    
    # Анализ текста
    analyzer = TextAnalyzer()
    analyzer.train_word2vec(df['review_text'].dropna().tolist())
    
    # Создание визуализаций
    analyzer.create_visualizations(df) 