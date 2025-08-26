"""
Модуль для управления эмбеддингами в базе данных.
Обеспечивает генерацию, хранение и извлечение эмбеддингов для отзывов и объектов.
"""

import sqlite3
import json
import logging
from typing import List, Dict, Optional, Tuple
from .embeddings_service import EmbeddingsService

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EmbeddingsManager:
    """
    Менеджер для работы с эмбеддингами в базе данных.
    Генерирует эмбеддинги для отзывов и объектов, управляет их хранением.
    """
    
    def __init__(self, db_path: str = "urban_analysis_fixed.db"):
        """
        Инициализация менеджера эмбеддингов.
        
        Args:
            db_path: Путь к базе данных
        """
        self.db_path = db_path
        self.embeddings_service = EmbeddingsService()
        self.init_embeddings_table()
        
        logger.info("🚀 EmbeddingsManager инициализирован")
    
    def init_embeddings_table(self):
        """Инициализирует таблицу reviews, добавляя поля для эмбеддингов."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Проверяем существование полей для эмбеддингов
                cursor.execute("PRAGMA table_info(reviews)")
                columns = [column[1] for column in cursor.fetchall()]
                
                # Добавляем поле для эмбеддинга отзыва, если его нет
                if 'review_embedding' not in columns:
                    cursor.execute("ALTER TABLE reviews ADD COLUMN review_embedding TEXT")
                    logger.info("✅ Добавлено поле review_embedding в таблицу reviews")
                
                # Добавляем поле для эмбеддинга объекта, если его нет
                if 'object_embedding' not in columns:
                    cursor.execute("ALTER TABLE reviews ADD COLUMN object_embedding TEXT")
                    logger.info("✅ Добавлено поле object_embedding в таблицу reviews")
                
                # Добавляем поле для категории сентимента, если его нет
                if 'sentiment_category' not in columns:
                    cursor.execute("ALTER TABLE reviews ADD COLUMN sentiment_category TEXT")
                    logger.info("✅ Добавлено поле sentiment_category в таблицу reviews")
                
                conn.commit()
                logger.info("✅ Таблица reviews готова для работы с эмбеддингами и сентиментами")
                
        except Exception as e:
            logger.error(f"❌ Ошибка при инициализации таблицы: {str(e)}")
    
    def generate_review_embedding(self, review_id: int) -> bool:
        """
        Генерирует эмбеддинг для конкретного отзыва.
        
        Args:
            review_id: ID отзыва
            
        Returns:
            True если эмбеддинг успешно сгенерирован и сохранен
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Проверяем, существует ли уже эмбеддинг
                cursor = conn.execute(
                    "SELECT review_text, review_embedding FROM reviews WHERE id = ?",
                    (review_id,)
                )
                row = cursor.fetchone()
                
                if not row:
                    logger.warning(f"⚠️ Отзыв с ID {review_id} не найден")
                    return False
                
                review_text, existing_embedding = row
                
                # Если эмбеддинг уже существует, пропускаем
                if existing_embedding:
                    logger.info(f"⏭️ Эмбеддинг для отзыва {review_id} уже существует")
                    return True
                
                if not review_text or not review_text.strip():
                    logger.warning(f"⚠️ Отзыв {review_id} не содержит текста")
                    return False
                
                # Генерируем эмбеддинг
                logger.info(f"🔄 Генерация эмбеддинга для отзыва {review_id}")
                embedding = self.embeddings_service.get_text_embedding(review_text)
                
                if embedding:
                    # Сохраняем эмбеддинг как JSON строку
                    embedding_json = json.dumps(embedding)
                    cursor = conn.execute(
                        "UPDATE reviews SET review_embedding = ? WHERE id = ?",
                        (embedding_json, review_id)
                    )
                    conn.commit()
                    logger.info(f"✅ Эмбеддинг для отзыва {review_id} сохранен")
                    return True
                else:
                    logger.error(f"❌ Не удалось сгенерировать эмбеддинг для отзыва {review_id}")
                    return False
                    
        except Exception as e:
            logger.error(f"❌ Ошибка при генерации эмбеддинга отзыва {review_id}: {str(e)}")
            return False
    
    def generate_object_embedding(self, object_id: int) -> bool:
        """
        Генерирует эмбеддинг для объекта на основе его названия и всех отзывов.
        
        Args:
            object_id: ID объекта
            
        Returns:
            True если эмбеддинг успешно сгенерирован и сохранен
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Получаем название объекта и все отзывы
                cursor = conn.execute("""
                    SELECT o.name, GROUP_CONCAT(r.review_text, ' | ') as all_reviews
                    FROM objects o
                    LEFT JOIN reviews r ON o.id = r.object_id
                    WHERE o.id = ?
                    GROUP BY o.id
                """, (object_id,))
                
                row = cursor.fetchone()
                if not row:
                    logger.warning(f"⚠️ Объект с ID {object_id} не найден")
                    return False
                
                object_name, all_reviews = row
                
                # Проверяем, существует ли уже эмбеддинг для этого объекта
                cursor = conn.execute("""
                    SELECT COUNT(*) FROM reviews 
                    WHERE object_id = ? AND object_embedding IS NOT NULL
                """, (object_id,))
                
                if cursor.fetchone()[0] > 0:
                    logger.info(f"⏭️ Эмбеддинг для объекта {object_id} уже существует")
                    return True
                
                # Формируем текст для эмбеддинга
                combined_text = object_name
                if all_reviews:
                    combined_text += f" | {all_reviews}"
                
                if not combined_text.strip():
                    logger.warning(f"⚠️ Объект {object_id} не содержит текста для эмбеддинга")
                    return False
                
                # Генерируем эмбеддинг
                logger.info(f"🔄 Генерация эмбеддинга для объекта {object_id}")
                embedding = self.embeddings_service.get_text_embedding(combined_text)
                
                if embedding:
                    # Сохраняем эмбеддинг для всех отзывов этого объекта
                    embedding_json = json.dumps(embedding)
                    cursor = conn.execute("""
                        UPDATE reviews 
                        SET object_embedding = ? 
                        WHERE object_id = ?
                    """, (embedding_json, object_id))
                    
                    conn.commit()
                    logger.info(f"✅ Эмбеддинг для объекта {object_id} сохранен в {cursor.rowcount} отзывах")
                    return True
                else:
                    logger.error(f"❌ Не удалось сгенерировать эмбеддинг для объекта {object_id}")
                    return False
                    
        except Exception as e:
            logger.error(f"❌ Ошибка при генерации эмбеддинга объекта {object_id}: {str(e)}")
            return False
    
    def generate_all_embeddings(self) -> Dict[str, int]:
        """
        Генерирует эмбеддинги для всех отзывов и объектов.
        
        Returns:
            Словарь с статистикой генерации
        """
        try:
            logger.info("🚀 Начинаем генерацию всех эмбеддингов...")
            
            with sqlite3.connect(self.db_path) as conn:
                # Получаем все отзывы
                cursor = conn.execute("SELECT id, object_id FROM reviews ORDER BY id")
                reviews = cursor.fetchall()
                
                total_reviews = len(reviews)
                total_objects = len(set(review[1] for review in reviews))
                
                logger.info(f"📊 Всего отзывов: {total_reviews}, уникальных объектов: {total_objects}")
                
                # Генерируем эмбеддинги для отзывов
                review_embeddings_generated = 0
                for review_id, _ in reviews:
                    if self.generate_review_embedding(review_id):
                        review_embeddings_generated += 1
                
                # Генерируем эмбеддинги для объектов
                object_embeddings_generated = 0
                unique_object_ids = set(review[1] for review in reviews)
                for object_id in unique_object_ids:
                    if self.generate_object_embedding(object_id):
                        object_embeddings_generated += 1
                
                stats = {
                    'total_reviews': total_reviews,
                    'total_objects': total_objects,
                    'review_embeddings_generated': review_embeddings_generated,
                    'object_embeddings_generated': object_embeddings_generated,
                    'review_embeddings_skipped': total_reviews - review_embeddings_generated,
                    'object_embeddings_skipped': total_objects - object_embeddings_generated
                }
                
                logger.info("✅ Генерация эмбеддингов завершена")
                logger.info(f"📊 Статистика: {stats}")
                
                return stats
                
        except Exception as e:
            logger.error(f"❌ Ошибка при генерации всех эмбеддингов: {str(e)}")
            return {}
    
    def force_regenerate_all_embeddings(self) -> Dict[str, int]:
        """
        Принудительно перегенерирует все эмбеддинги, очищая существующие.
        
        Returns:
            Словарь с статистикой генерации
        """
        try:
            logger.info("🔄 Начинаем принудительную перегенерацию эмбеддингов...")
            
            # Сначала очищаем все существующие эмбеддинги
            if not self.clear_all_embeddings():
                logger.error("❌ Не удалось очистить существующие эмбеддинги")
                return {}
            
            # Теперь генерируем заново
            return self.generate_all_embeddings()
                
        except Exception as e:
            logger.error(f"❌ Ошибка при принудительной перегенерации эмбеддингов: {str(e)}")
            return {}
    
    def get_embeddings_for_visualization(self, limit: int = 1000) -> List[Dict]:
        """
        Получает данные эмбеддингов для 3D визуализации.
        
        Args:
            limit: Максимальное количество записей
            
        Returns:
            Список словарей с данными для визуализации
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("""
                    SELECT r.id, r.review_text, r.rating, r.review_embedding, r.object_embedding,
                           o.name as object_name,
                           og.group_name,
                           dg.group_type as detected_group_type
                    FROM reviews r
                    JOIN objects o ON r.object_id = o.id
                    LEFT JOIN object_groups og ON o.group_id = og.id
                    LEFT JOIN detected_groups dg ON o.detected_group_id = dg.id
                    WHERE r.review_embedding IS NOT NULL
                       OR r.object_embedding IS NOT NULL
                    LIMIT ?
                """, (limit,))
                
                rows = cursor.fetchall()
                
                if not rows:
                    logger.warning("⚠️ Нет данных эмбеддингов для визуализации")
                    return []
                
                # Преобразуем данные в удобный формат
                visualization_data = []
                for row in rows:
                    review_id, review_text, rating, review_embedding, object_embedding, \
                    object_name, group_name, detected_group_type = row
                    
                    data_item = {
                        'review_id': review_id,
                        'review_text': review_text,
                        'rating': rating,
                        'object_name': object_name,
                        'group_name': group_name or 'unknown',
                        'detected_group_type': detected_group_type or 'unknown',
                        'has_review_embedding': review_embedding is not None,
                        'has_object_embedding': object_embedding is not None
                    }
                    
                    # Добавляем эмбеддинги, если они есть
                    if review_embedding:
                        try:
                            data_item['review_embedding'] = json.loads(review_embedding)
                        except json.JSONDecodeError:
                            logger.warning(f"⚠️ Некорректный JSON в review_embedding для отзыва {review_id}")
                            continue
                    
                    if object_embedding:
                        try:
                            data_item['object_embedding'] = json.loads(object_embedding)
                        except json.JSONDecodeError:
                            logger.warning(f"⚠️ Некорректный JSON в object_embedding для отзыва {review_id}")
                            continue
                    
                    visualization_data.append(data_item)
                
                logger.info(f"✅ Получено {len(visualization_data)} записей для визуализации")
                return visualization_data
                
        except Exception as e:
            logger.error(f"❌ Ошибка при получении данных для визуализации: {str(e)}")
            return []
    
    def get_3d_visualization_data(self, clustering_mode: str = 'groups', limit: int = 1000) -> List[Dict]:
        """
        Получает данные для 3D визуализации с тремя режимами кластеризации.
        
        Args:
            clustering_mode: Режим кластеризации ('groups', 'sentiment', 'groups_sentiment')
            limit: Максимальное количество записей
            
        Returns:
            Список словарей с данными для 3D визуализации
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Базовый запрос для получения данных с реальными сентиментами
                base_query = """
                    SELECT r.id, r.review_text, r.rating, r.review_embedding, r.object_embedding,
                           o.name as object_name, o.group_id,
                           og.group_name, og.group_type,
                           COALESCE(
                               (SELECT ar.sentiment 
                                FROM analysis_results ar 
                                JOIN processing_methods pm ON ar.method_id = pm.id 
                                WHERE ar.review_id = r.id 
                                  AND pm.method_name = 'yandex_gpt' 
                                  AND ar.sentiment IS NOT NULL 
                                LIMIT 1),
                               (SELECT ar.sentiment 
                                FROM analysis_results ar 
                                JOIN processing_methods pm ON ar.method_id = pm.id 
                                WHERE ar.review_id = r.id 
                                  AND pm.method_name = 'yandexgpt_sentiment' 
                                  AND ar.sentiment IS NOT NULL 
                                LIMIT 1),
                               r.sentiment_category, 
                               'Отсутствует'
                           ) as sentiment,
                           -- Добавляем отладочную информацию
                           (SELECT COUNT(*) FROM analysis_results ar2 WHERE ar2.review_id = r.id) as total_analysis_results,
                           (SELECT COUNT(*) FROM analysis_results ar3 
                            JOIN processing_methods pm3 ON ar3.method_id = pm3.id 
                            WHERE ar3.review_id = r.id AND pm3.method_name = 'yandex_gpt') as yandexgpt_results,
                           -- Добавляем информацию о доступных методах для диагностики
                           (SELECT GROUP_CONCAT(DISTINCT pm4.method_name) 
                            FROM analysis_results ar4 
                            JOIN processing_methods pm4 ON ar4.method_id = pm4.id 
                            WHERE ar4.review_id = r.id) as available_methods
                    FROM reviews r
                    JOIN objects o ON r.object_id = o.id
                    LEFT JOIN object_groups og ON o.group_id = og.id
                    WHERE r.review_embedding IS NOT NULL
                       OR r.object_embedding IS NOT NULL
                    LIMIT ?
                """
                
                cursor = conn.execute(base_query, (limit,))
                rows = cursor.fetchall()
                
                if not rows:
                    logger.warning("⚠️ Нет данных для 3D визуализации")
                    return []
                
                # Преобразуем данные в удобный формат
                visualization_data = []
                for row in rows:
                    review_id, review_text, rating, review_embedding, object_embedding, \
                    object_name, group_id, group_name, group_type, sentiment, \
                    total_analysis_results, yandexgpt_results, available_methods = row
                    
                    # Логируем отладочную информацию для первых нескольких записей
                    if len(visualization_data) < 5:
                        logger.info(f"🔍 Отзыв {review_id}: sentiment='{sentiment}', "
                                  f"total_analysis_results={total_analysis_results}, "
                                  f"yandexgpt_results={yandexgpt_results}, "
                                  f"available_methods='{available_methods}'")
                    
                    # Определяем кластер в зависимости от режима
                    cluster = self._determine_cluster(clustering_mode, group_name, sentiment, group_id)
                    
                    data_item = {
                        'review_id': review_id,
                        'review_text': review_text,
                        'rating': rating,
                        'object_name': object_name,
                        'group_name': group_name or 'unknown',
                        'group_type': group_type or 'unknown',
                        'sentiment': sentiment,
                        'cluster': cluster,
                        'clustering_mode': clustering_mode,
                        'has_review_embedding': review_embedding is not None,
                        'has_object_embedding': object_embedding is not None
                    }
                    
                    # Добавляем эмбеддинги, если они есть
                    if review_embedding:
                        try:
                            embedding_vector = json.loads(review_embedding)
                            # Уменьшаем размерность до 3D
                            data_item['review_embedding'] = self._reduce_embedding_dimensions(embedding_vector, 3)
                        except json.JSONDecodeError:
                            logger.warning(f"⚠️ Некорректный JSON в review_embedding для отзыва {review_id}")
                            continue
                    
                    if object_embedding:
                        try:
                            embedding_vector = json.loads(object_embedding)
                            # Уменьшаем размерность до 3D
                            data_item['object_embedding'] = self._reduce_embedding_dimensions(embedding_vector, 3)
                        except json.JSONDecodeError:
                            logger.warning(f"⚠️ Некорректный JSON в object_embedding для отзыва {review_id}")
                            continue
                    
                    visualization_data.append(data_item)
                
                logger.info(f"✅ Получено {len(visualization_data)} записей для 3D визуализации (режим: {clustering_mode})")
                return visualization_data
                
        except Exception as e:
            logger.error(f"❌ Ошибка при получении данных для 3D визуализации: {str(e)}")
            return []
    
    def _determine_cluster(self, clustering_mode: str, group_name: str, sentiment: str, group_id: int) -> str:
        """
        Определяет кластер для записи в зависимости от режима кластеризации.
        
        Args:
            clustering_mode: Режим кластеризации
            group_name: Название группы
            sentiment: Категория сентимента
            group_id: ID группы
            
        Returns:
            Название кластера
        """
        if clustering_mode == 'groups':
            return group_name or f'group_{group_id}'
        elif clustering_mode == 'sentiment':
            return sentiment
        elif clustering_mode == 'groups_sentiment':
            return f"{group_name or f'group_{group_id}'}-{sentiment}"
        else:
            return 'unknown'
    
    def _reduce_embedding_dimensions(self, embedding: List[float], target_dim: int = 3) -> List[float]:
        """
        Уменьшает размерность эмбеддинга до целевой размерности.
        
        Args:
            embedding: Вектор эмбеддинга
            target_dim: Целевая размерность (по умолчанию 3 для 3D визуализации)
            
        Returns:
            Эмбеддинг с уменьшенной размерностью
        """
        if not embedding or len(embedding) <= target_dim:
            return embedding[:target_dim] if embedding else [0, 0, 0]
        
        # Простое уменьшение размерности: берем первые target_dim компонент
        # В будущем можно заменить на PCA
        return embedding[:target_dim]
    
    def get_embeddings_stats(self) -> Dict:
        """
        Возвращает статистику по эмбеддингам в базе данных.
        
        Returns:
            Словарь со статистикой
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Общее количество отзывов
                cursor.execute("SELECT COUNT(*) FROM reviews")
                total_reviews = cursor.fetchone()[0]
                
                # Отзывы с эмбеддингами
                cursor.execute("SELECT COUNT(*) FROM reviews WHERE review_embedding IS NOT NULL")
                reviews_with_embeddings = cursor.fetchone()[0]
                
                # Отзывы с эмбеддингами объектов
                cursor.execute("SELECT COUNT(*) FROM reviews WHERE object_embedding IS NOT NULL")
                reviews_with_object_embeddings = cursor.fetchone()[0]
                
                # Уникальные объекты с эмбеддингами
                cursor.execute("""
                    SELECT COUNT(DISTINCT object_id) 
                    FROM reviews 
                    WHERE object_embedding IS NOT NULL
                """)
                objects_with_embeddings = cursor.fetchone()[0]
                
                # Общее количество уникальных объектов
                cursor.execute("SELECT COUNT(DISTINCT object_id) FROM reviews")
                total_objects = cursor.fetchone()[0]
                
                stats = {
                    'total_reviews': total_reviews,
                    'total_objects': total_objects,
                    'reviews_with_embeddings': reviews_with_embeddings,
                    'reviews_with_object_embeddings': reviews_with_object_embeddings,
                    'objects_with_embeddings': objects_with_embeddings,
                    'review_embeddings_coverage': round(reviews_with_embeddings / total_reviews * 100, 1) if total_reviews > 0 else 0,
                    'object_embeddings_coverage': round(objects_with_embeddings / total_objects * 100, 1) if total_objects > 0 else 0
                }
                
                return stats
                
        except Exception as e:
            logger.error(f"❌ Ошибка при получении статистики: {str(e)}")
            return {}
    
    def clear_all_embeddings(self) -> bool:
        """
        Очищает все эмбеддинги из базы данных.
        
        Returns:
            True если очистка прошла успешно
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("UPDATE reviews SET review_embedding = NULL, object_embedding = NULL")
                conn.commit()
                
                logger.info(f"✅ Очищены эмбеддинги из {cursor.rowcount} записей")
                return True
                
        except Exception as e:
            logger.error(f"❌ Ошибка при очистке эмбеддингов: {str(e)}")
            return False
