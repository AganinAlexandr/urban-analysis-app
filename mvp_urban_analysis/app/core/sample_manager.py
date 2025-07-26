import sqlite3
import pandas as pd
import os
from typing import Dict, Any, Optional

class SampleManager:
    """Менеджер для работы с выборками данных"""
    
    def __init__(self, db_path: str = "urban_analysis.db"):
        self.db_path = db_path
        self._ensure_sample_field_exists()
    
    def _ensure_sample_field_exists(self):
        """Убеждаемся, что поле в_Выборке существует в таблице reviews"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.execute("PRAGMA table_info(reviews)")
            columns = [column[1] for column in cursor.fetchall()]
            
            if 'в_Выборке' not in columns:
                conn.execute("ALTER TABLE reviews ADD COLUMN в_Выборке TEXT DEFAULT NULL")
                print("✅ Поле в_Выборке добавлено в таблицу reviews")
            
            conn.close()
        except Exception as e:
            print(f"❌ Ошибка при проверке поля в_Выборке: {e}")
    
    def get_connection(self):
        """Получаем соединение с базой данных"""
        return sqlite3.connect(self.db_path)
    
    def create_sample_from_filters(self, filters: Dict[str, Any]) -> int:
        """
        Создает выборку на основе переданных фильтров
        
        Args:
            filters: Словарь с фильтрами (group_type, sentiment_method, color_scheme)
            
        Returns:
            int: Количество записей в выборке
        """
        try:
            conn = self.get_connection()
            
            # Сначала очищаем предыдущую выборку
            conn.execute("UPDATE reviews SET в_Выборке = NULL")
            
            # Строим SQL запрос на основе фильтров
            query, params = self._build_sample_query(filters)
            
            print(f"🔍 SQL запрос для выборки: {query}")
            print(f"🔍 Параметры: {params}")
            
            # Выполняем запрос и обновляем поле в_Выборке
            cursor = conn.execute(query, params)
            selected_reviews = cursor.fetchall()
            
            if selected_reviews:
                # Получаем ID отзывов для обновления
                review_ids = [review[0] for review in selected_reviews]
                placeholders = ','.join(['?' for _ in review_ids])
                
                update_query = f"UPDATE reviews SET в_Выборке = 'да' WHERE id IN ({placeholders})"
                conn.execute(update_query, review_ids)
            
            conn.commit()
            conn.close()
            
            print(f"✅ Создана выборка из {len(selected_reviews)} записей")
            return len(selected_reviews)
            
        except Exception as e:
            print(f"❌ Ошибка создания выборки: {e}")
            return 0
    
    def _build_sample_query(self, filters: Dict[str, Any]) -> tuple:
        """
        Строит SQL запрос для выборки на основе фильтров
        
        Args:
            filters: Словарь с фильтрами
            
        Returns:
            tuple: (SQL запрос, параметры)
        """
        base_query = """
            SELECT DISTINCT r.id
            FROM reviews r
            JOIN objects o ON r.object_id = o.id
            LEFT JOIN object_group_mapping ogm ON o.id = ogm.object_id
            LEFT JOIN object_groups og ON ogm.group_id = og.id
            WHERE 1=1
        """
        params = []
        
        # Фильтр по группе объекта
        if 'group_type' in filters and filters['group_type']:
            base_query += " AND og.group_type = ?"
            params.append(filters['group_type'])
        
        # Фильтр по определенной группе (пока используем ту же логику)
        if 'detected_group_type' in filters and filters['detected_group_type']:
            base_query += " AND og.group_type = ?"
            params.append(filters['detected_group_type'])
        
        # Фильтр по методу анализа сентимента
        if 'sentiment_method' in filters and filters['sentiment_method']:
            base_query += " AND r.sentiment_method = ?"
            params.append(filters['sentiment_method'])
        
        # Фильтр по цветовой схеме (рейтинг)
        if 'color_scheme' in filters and filters['color_scheme'] == 'rating':
            base_query += " AND r.rating IS NOT NULL"
        
        # Фильтр по рейтингу
        if 'rating' in filters and filters['rating']:
            base_query += " AND r.rating = ?"
            params.append(filters['rating'])
        
        # Фильтр по дате
        if 'date_from' in filters and filters['date_from']:
            base_query += " AND r.review_date >= ?"
            params.append(filters['date_from'])
        
        if 'date_to' in filters and filters['date_to']:
            base_query += " AND r.review_date <= ?"
            params.append(filters['date_to'])
        
        return base_query, params
    
    def get_sample_info(self) -> Dict[str, Any]:
        """
        Получает информацию о текущей выборке
        
        Returns:
            Dict: Информация о выборке
        """
        try:
            conn = self.get_connection()
            
            # Общее количество записей в выборке
            cursor = conn.execute("SELECT COUNT(*) FROM reviews WHERE в_Выборке = 'да'")
            total_records = cursor.fetchone()[0]
            
            # Распределение по группам
            cursor = conn.execute("""
                SELECT og.group_type, COUNT(*) as count
                FROM reviews r
                JOIN objects o ON r.object_id = o.id
                LEFT JOIN object_group_mapping ogm ON o.id = ogm.object_id
                LEFT JOIN object_groups og ON ogm.group_id = og.id
                WHERE r.в_Выборке = 'да'
                GROUP BY og.group_type
            """)
            group_distribution = dict(cursor.fetchall())
            
            # Диапазон дат
            cursor = conn.execute("""
                SELECT MIN(review_date), MAX(review_date)
                FROM reviews
                WHERE в_Выборке = 'да' AND review_date IS NOT NULL
            """)
            date_range = cursor.fetchone()
            
            # Заполненность полей
            completeness = self._calculate_field_completeness_sample()
            
            conn.close()
            
            return {
                'total_records': total_records,
                'group_distribution': group_distribution,
                'date_range': {
                    'min_date': date_range[0] if date_range[0] else None,
                    'max_date': date_range[1] if date_range[1] else None
                },
                'field_completeness': completeness
            }
            
        except Exception as e:
            print(f"❌ Ошибка получения информации о выборке: {e}")
            return {}
    
    def _calculate_field_completeness_sample(self) -> Dict[str, float]:
        """Вычисляет заполненность полей в выборке"""
        try:
            conn = self.get_connection()
            
            # Общее количество записей в выборке
            cursor = conn.execute("SELECT COUNT(*) FROM reviews WHERE в_Выборке = 'да'")
            total = cursor.fetchone()[0]
            
            if total == 0:
                return {}
            
            completeness = {}
            
            # Проверяем заполненность различных полей
            fields = [
                ('review_text', 'Текст отзыва'),
                ('rating', 'Рейтинг'),
                ('review_date', 'Дата отзыва')
            ]
            
            for field, name in fields:
                cursor = conn.execute(f"""
                    SELECT COUNT(*) FROM reviews 
                    WHERE в_Выборке = 'да' AND {field} IS NOT NULL
                """)
                filled = cursor.fetchone()[0]
                percentage = (filled / total) * 100 if total > 0 else 0
                completeness[name] = round(percentage, 1)
            
            conn.close()
            return completeness
            
        except Exception as e:
            print(f"❌ Ошибка вычисления заполненности: {e}")
            return {}
    
    def download_sample(self) -> pd.DataFrame:
        """
        Скачивает текущую выборку как DataFrame
        
        Returns:
            pd.DataFrame: Данные выборки
        """
        try:
            conn = self.get_connection()
            
            query = """
                SELECT 
                    o.id as object_id,
                    o.name as object_name,
                    o.address as object_address,
                    o.latitude,
                    o.longitude,
                    o.district,
                    og.group_type as group_supplier,
                    og.group_type as group_determined,
                    r.id as review_id,
                    r.review_text,
                    r.rating,
                    r.review_date,
                    NULL as sentiment_score,
                    NULL as sentiment_method,
                    r.source,
                    r.в_Выборке
                FROM reviews r
                JOIN objects o ON r.object_id = o.id
                LEFT JOIN object_group_mapping ogm ON o.id = ogm.object_id
                LEFT JOIN object_groups og ON ogm.group_id = og.id
                WHERE r.в_Выборке = 'да'
                ORDER BY o.name, r.review_date
            """
            
            df = pd.read_sql_query(query, conn)
            conn.close()
            
            return df
            
        except Exception as e:
            print(f"❌ Ошибка скачивания выборки: {e}")
            return pd.DataFrame()
    
    def clear_sample(self) -> bool:
        """
        Очищает выборку (устанавливает в_Выборке = NULL для всех записей)
        
        Returns:
            bool: Успешность операции
        """
        try:
            conn = self.get_connection()
            conn.execute("UPDATE reviews SET в_Выборке = NULL")
            conn.commit()
            conn.close()
            
            print("✅ Выборка очищена")
            return True
            
        except Exception as e:
            print(f"❌ Ошибка очистки выборки: {e}")
            return False
    
    def get_sample_statistics(self) -> Dict[str, Any]:
        """
        Получает общую статистику по выборке
        
        Returns:
            Dict: Статистика выборки
        """
        try:
            conn = self.get_connection()
            
            # Количество записей в выборке
            cursor = conn.execute("SELECT COUNT(*) FROM reviews WHERE в_Выборке = 'да'")
            sample_count = cursor.fetchone()[0]
            
            # Общее количество записей
            cursor = conn.execute("SELECT COUNT(*) FROM reviews")
            total_count = cursor.fetchone()[0]
            
            # Процент от общего количества
            percentage = (sample_count / total_count * 100) if total_count > 0 else 0
            
            conn.close()
            
            return {
                'sample_count': sample_count,
                'total_count': total_count,
                'percentage': round(percentage, 1)
            }
            
        except Exception as e:
            print(f"❌ Ошибка получения статистики: {e}")
            return {} 