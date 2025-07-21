"""
Модуль для визуализации сравнения методов обработки
"""

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')  # Используем фоновый режим для работы в веб-приложении
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, List, Optional, Tuple
import logging

logger = logging.getLogger(__name__)

class MethodComparisonVisualizer:
    """
    Визуализация сравнения методов обработки отзывов
    """
    
    def __init__(self):
        # Цветовая схема для сентимента
        self.sentiment_colors = {
            'положительный': '#2E8B57',  # Зеленый
            'нейтральный': '#FFD700',     # Желтый  
            'отрицательный': '#FF8C00',   # Оранжевый
            'хорошо': '#2E8B57',          # Зеленый
            'нейтрально': '#FFD700',      # Желтый
            'плохо': '#FF8C00'            # Оранжевый
        }
        
        # Маппинг для унификации шкалы
        self.rating_to_sentiment = {
            'хорошо': 'положительный',
            'нейтрально': 'нейтральный', 
            'плохо': 'отрицательный',
            'positive': 'положительный',
            'neutral': 'нейтральный',
            'negative': 'отрицательный'
        }
    
    def unify_rating_scale(self, value: str) -> str:
        """
        Приведение оценки к единой шкале сентимента
        
        Args:
            value: Исходная оценка
            
        Returns:
            Унифицированная оценка
        """
        if pd.isna(value) or value == '':
            return 'нейтральный'
        
        value_str = str(value).lower().strip()
        
        # Прямое соответствие
        if value_str in self.rating_to_sentiment:
            return self.rating_to_sentiment[value_str]
        
        # Числовые рейтинги (1-5)
        try:
            rating_num = float(value_str)
            if rating_num >= 4.0:
                return 'положительный'
            elif rating_num >= 3.0:
                return 'нейтральный'
            else:
                return 'отрицательный'
        except ValueError:
            pass
        
        # По умолчанию
        return 'нейтральный'
    
    def prepare_comparison_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Подготовка данных для сравнения методов
        
        Args:
            df: DataFrame с результатами обработки
            
        Returns:
            DataFrame для визуализации
        """
        try:
            # Создаем копию для работы
            comparison_df = df.copy()
            
            # Унифицируем оценки пользователей
            if 'rating' in comparison_df.columns:
                comparison_df['rating_unified'] = comparison_df['rating'].apply(self.unify_rating_scale)
            
            # Унифицируем сентимент
            if 'sentiment' in comparison_df.columns:
                comparison_df['sentiment_unified'] = comparison_df['sentiment'].apply(self.unify_rating_scale)
            
            # Создаем поле для сравнения
            comparison_df['user_rating'] = comparison_df.get('rating_unified', 'нейтральный')
            comparison_df['sentiment_analysis'] = comparison_df.get('sentiment_unified', 'нейтральный')
            
            logger.info(f"Подготовлены данные для сравнения: {len(comparison_df)} записей")
            return comparison_df
            
        except Exception as e:
            logger.error(f"Ошибка подготовки данных для сравнения: {e}")
            return df
    
    def create_comparison_table(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Создание таблицы сравнения методов
        
        Args:
            df: DataFrame с данными
            
        Returns:
            Таблица сравнения
        """
        try:
            # Подготавливаем данные
            comparison_df = self.prepare_comparison_data(df)
            
            # Создаем таблицу сравнения
            comparison_table = comparison_df[['name', 'group', 'user_rating', 'sentiment_analysis']].copy()
            
            # Добавляем цветовые коды
            comparison_table['user_rating_color'] = comparison_table['user_rating'].map(self.sentiment_colors)
            comparison_table['sentiment_color'] = comparison_table['sentiment_analysis'].map(self.sentiment_colors)
            
            logger.info(f"Создана таблица сравнения: {len(comparison_table)} записей")
            return comparison_table
            
        except Exception as e:
            logger.error(f"Ошибка создания таблицы сравнения: {e}")
            return pd.DataFrame()
    
    def create_heatmap(self, df: pd.DataFrame, save_path: Optional[str] = None) -> str:
        """
        Создание тепловой карты сравнения методов
        
        Args:
            df: DataFrame с данными
            save_path: Путь для сохранения
            
        Returns:
            Путь к сохраненному файлу
        """
        try:
            # Подготавливаем данные
            comparison_df = self.prepare_comparison_data(df)
            
            # Создаем матрицу сравнения
            comparison_matrix = []
            labels = []
            
            # Ограничиваем количество записей для читаемости
            max_records = min(50, len(comparison_df))
            
            for idx, row in comparison_df.head(max_records).iterrows():
                name = row.get('name', f'Запись {idx}')
                user_rating = row.get('user_rating', 'нейтральный')
                sentiment = row.get('sentiment_analysis', 'нейтральный')
                
                # Кодируем оценки численно
                rating_code = {'положительный': 1, 'нейтральный': 0, 'отрицательный': -1}
                
                comparison_matrix.append([
                    rating_code.get(user_rating, 0),
                    rating_code.get(sentiment, 0)
                ])
                labels.append(name[:30] + '...' if len(name) > 30 else name)
            
            if not comparison_matrix:
                logger.warning("Нет данных для создания тепловой карты")
                return ""
            
            # Создаем тепловую карту
            plt.figure(figsize=(10, max(6, len(comparison_matrix) * 0.4)))
            
            # Создаем матрицу для seaborn
            matrix_df = pd.DataFrame(
                comparison_matrix,
                index=labels,
                columns=['Оценка пользователя', 'Анализ сентимента']
            )
            
            # Создаем цветовую карту
            colors = ['#FF8C00', '#FFD700', '#2E8B57']  # Оранжевый, желтый, зеленый
            cmap = sns.blend_palette(colors, as_cmap=True)
            
            # Рисуем тепловую карту
            sns.heatmap(
                matrix_df,
                annot=True,
                cmap=cmap,
                center=0,
                cbar_kws={'label': 'Оценка'},
                fmt='.0f',
                square=True
            )
            
            plt.title('Сравнение методов обработки отзывов', fontsize=14, pad=20)
            plt.xlabel('Методы обработки', fontsize=12)
            plt.ylabel('Записи', fontsize=12)
            plt.xticks(rotation=0)
            plt.yticks(rotation=0)
            plt.tight_layout()
            
            # Сохраняем
            if save_path is None:
                save_path = 'method_comparison_heatmap.png'
            
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            logger.info(f"Тепловая карта сохранена: {save_path}")
            return save_path
            
        except Exception as e:
            logger.error(f"Ошибка создания тепловой карты: {e}")
            return ""
    
    def create_comparison_chart(self, df: pd.DataFrame, save_path: Optional[str] = None) -> str:
        """
        Создание столбчатой диаграммы сравнения методов
        
        Args:
            df: DataFrame с данными
            save_path: Путь для сохранения
            
        Returns:
            Путь к сохраненному файлу
        """
        try:
            # Подготавливаем данные
            comparison_df = self.prepare_comparison_data(df)
            
            # Группируем по методам
            user_ratings = comparison_df['user_rating'].value_counts()
            sentiment_ratings = comparison_df['sentiment_analysis'].value_counts()
            
            # Создаем график
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
            
            # График оценок пользователей
            if len(user_ratings) > 0:
                colors_user = [self.sentiment_colors.get(rating, '#CCCCCC') for rating in user_ratings.index]
                user_ratings.plot(kind='bar', ax=ax1, color=colors_user)
                ax1.set_title('Распределение оценок пользователей', fontsize=12)
                ax1.set_xlabel('Оценка')
                ax1.set_ylabel('Количество')
                ax1.tick_params(axis='x', rotation=45)
            else:
                ax1.text(0.5, 0.5, 'Нет данных', ha='center', va='center', transform=ax1.transAxes)
                ax1.set_title('Распределение оценок пользователей', fontsize=12)
            
            # График сентимент-анализа
            if len(sentiment_ratings) > 0:
                colors_sentiment = [self.sentiment_colors.get(rating, '#CCCCCC') for rating in sentiment_ratings.index]
                sentiment_ratings.plot(kind='bar', ax=ax2, color=colors_sentiment)
                ax2.set_title('Распределение сентимент-анализа', fontsize=12)
                ax2.set_xlabel('Сентимент')
                ax2.set_ylabel('Количество')
                ax2.tick_params(axis='x', rotation=45)
            else:
                ax2.text(0.5, 0.5, 'Нет данных', ha='center', va='center', transform=ax2.transAxes)
                ax2.set_title('Распределение сентимент-анализа', fontsize=12)
            
            plt.tight_layout()
            
            # Сохраняем
            if save_path is None:
                save_path = 'method_comparison_chart.png'
            
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            logger.info(f"Диаграмма сравнения сохранена: {save_path}")
            return save_path
            
        except Exception as e:
            logger.error(f"Ошибка создания диаграммы сравнения: {e}")
            return ""
    
    def get_comparison_statistics(self, df: pd.DataFrame) -> Dict:
        """
        Получение статистики сравнения методов
        
        Args:
            df: DataFrame с данными
            
        Returns:
            Словарь со статистикой
        """
        try:
            # Подготавливаем данные
            comparison_df = self.prepare_comparison_data(df)
            
            # Статистика по методам
            user_ratings = comparison_df['user_rating'].value_counts()
            sentiment_ratings = comparison_df['sentiment_analysis'].value_counts()
            
            # Согласованность методов
            agreement = (comparison_df['user_rating'] == comparison_df['sentiment_analysis']).sum()
            total = len(comparison_df)
            agreement_percent = (agreement / total * 100) if total > 0 else 0
            
            # Детальная статистика согласованности
            agreement_matrix = pd.crosstab(
                comparison_df['user_rating'], 
                comparison_df['sentiment_analysis'],
                margins=True
            )
            
            # Конвертируем numpy типы в обычные Python типы для JSON сериализации
            stats = {
                'total_records': int(total),
                'user_ratings': {str(k): int(v) for k, v in user_ratings.to_dict().items()},
                'sentiment_ratings': {str(k): int(v) for k, v in sentiment_ratings.to_dict().items()},
                'agreement_count': int(agreement),
                'agreement_percent': round(float(agreement_percent), 2),
                'agreement_matrix': {str(k): {str(k2): int(v2) for k2, v2 in v.items()} for k, v in agreement_matrix.to_dict().items()}
            }
            
            logger.info(f"Статистика сравнения: согласованность {agreement_percent:.1f}%")
            return stats
            
        except Exception as e:
            logger.error(f"Ошибка получения статистики сравнения: {e}")
            return {} 

    def create_simple_heatmap(self, df: pd.DataFrame, save_path: Optional[str] = None) -> str:
        """
        Создание упрощенной тепловой карты для быстрой работы
        
        Args:
            df: DataFrame с данными
            save_path: Путь для сохранения
            
        Returns:
            Путь к сохраненному файлу
        """
        try:
            # Подготавливаем данные
            comparison_df = self.prepare_comparison_data(df)
            
            if len(comparison_df) == 0:
                logger.warning("Нет данных для создания тепловой карты")
                return ""
            
            # Создаем простую матрицу сравнения
            comparison_matrix = []
            labels = []
            
            # Ограничиваем количество записей
            max_records = min(20, len(comparison_df))
            
            for idx, row in comparison_df.head(max_records).iterrows():
                name = row.get('name', f'Запись {idx}')
                user_rating = row.get('user_rating', 'нейтральный')
                sentiment = row.get('sentiment_analysis', 'нейтральный')
                
                # Кодируем оценки численно
                rating_code = {'положительный': 1, 'нейтральный': 0, 'отрицательный': -1}
                
                comparison_matrix.append([
                    rating_code.get(user_rating, 0),
                    rating_code.get(sentiment, 0)
                ])
                labels.append(name[:25] + '...' if len(name) > 25 else name)
            
            if not comparison_matrix:
                logger.warning("Нет данных для создания тепловой карты")
                return ""
            
            # Создаем простую тепловую карту
            plt.figure(figsize=(8, max(4, len(comparison_matrix) * 0.3)))
            
            # Создаем матрицу для seaborn
            matrix_df = pd.DataFrame(
                comparison_matrix,
                index=labels,
                columns=['Оценка пользователя', 'Анализ сентимента']
            )
            
            # Простая цветовая карта
            colors = ['#FF8C00', '#FFD700', '#2E8B57']  # Оранжевый, желтый, зеленый
            cmap = sns.blend_palette(colors, as_cmap=True)
            
            # Рисуем тепловую карту
            sns.heatmap(
                matrix_df,
                annot=True,
                cmap=cmap,
                center=0,
                cbar_kws={'label': 'Оценка'},
                fmt='.0f',
                square=True,
                linewidths=0.5
            )
            
            plt.title('Сравнение методов обработки отзывов', fontsize=12, pad=15)
            plt.xlabel('Методы обработки', fontsize=10)
            plt.ylabel('Записи', fontsize=10)
            plt.xticks(rotation=0)
            plt.yticks(rotation=0)
            plt.tight_layout()
            
            # Сохраняем
            if save_path is None:
                save_path = 'simple_comparison_heatmap.png'
            
            plt.savefig(save_path, dpi=150, bbox_inches='tight')
            plt.close()
            
            logger.info(f"Упрощенная тепловая карта сохранена: {save_path}")
            return save_path
            
        except Exception as e:
            logger.error(f"Ошибка создания упрощенной тепловой карты: {e}")
            return "" 