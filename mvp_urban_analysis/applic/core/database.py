"""
Модуль для работы с базой данных SQLite
"""
import sqlite3
import json
import hashlib
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import pandas as pd
from pathlib import Path


class DatabaseManager:
    """Менеджер базы данных для системы анализа городской среды"""
    
    def __init__(self, db_path: str = "urban_analysis.db"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Инициализация базы данных"""
        schema_path = Path(__file__).parent.parent.parent / "database_schema.sql"
        
        with sqlite3.connect(self.db_path) as conn:
            # Включение поддержки внешних ключей
            conn.execute("PRAGMA foreign_keys = ON")
            
            # Чтение и выполнение схемы
            if schema_path.exists():
                with open(schema_path, 'r', encoding='utf-8') as f:
                    schema = f.read()
                    conn.executescript(schema)
    
    def get_connection(self):
        """Получение соединения с базой данных"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # Для доступа к колонкам по имени
        return conn
    
    def create_object_key(self, name: str, address: str) -> str:
        """Создание уникального ключа объекта"""
        combined = f"{name}|{address}".lower().strip()
        return hashlib.md5(combined.encode()).hexdigest()
    
    def insert_object(self, name: str, address: str, latitude: float = None, 
                     longitude: float = None, district: str = None) -> int:
        """Добавление объекта в базу данных"""
        object_key = self.create_object_key(name, address)
        
        with self.get_connection() as conn:
            cursor = conn.execute("""
                INSERT OR IGNORE INTO objects (name, address, object_key, latitude, longitude, district)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (name, address, object_key, latitude, longitude, district))
            
            # Получаем ID объекта
            cursor = conn.execute("SELECT id FROM objects WHERE object_key = ?", (object_key,))
            result = cursor.fetchone()
            return result['id'] if result else None
    
    def insert_review(self, object_id: int, review_text: str, rating: int = None,
                     review_date: str = None, source: str = None, external_id: str = None) -> int:
        """Добавление отзыва в базу данных"""
        with self.get_connection() as conn:
            cursor = conn.execute("""
                INSERT INTO reviews (object_id, review_text, rating, review_date, source, external_id)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (object_id, review_text, rating, review_date, source, external_id))
            return cursor.lastrowid
    
    def insert_analysis_result(self, review_id: int, method_id: int, sentiment: str,
                             confidence: float = None, review_type: str = None,
                             keywords: List[str] = None, topics: List[str] = None) -> int:
        """Добавление результата анализа"""
        keywords_json = json.dumps(keywords) if keywords else None
        topics_json = json.dumps(topics) if topics else None
        
        with self.get_connection() as conn:
            cursor = conn.execute("""
                INSERT OR REPLACE INTO analysis_results 
                (review_id, method_id, sentiment, confidence, review_type, keywords, topics)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (review_id, method_id, sentiment, confidence, review_type, keywords_json, topics_json))
            return cursor.lastrowid
    
    def get_method_id(self, method_name: str) -> Optional[int]:
        """Получение ID метода по имени"""
        with self.get_connection() as conn:
            cursor = conn.execute("SELECT id FROM processing_methods WHERE method_name = ?", (method_name,))
            result = cursor.fetchone()
            return result['id'] if result else None
    
    def get_group_id(self, group_type: str) -> Optional[int]:
        """Получение ID группы по типу"""
        with self.get_connection() as conn:
            cursor = conn.execute("SELECT id FROM object_groups WHERE group_type = ?", (group_type,))
            result = cursor.fetchone()
            return result['id'] if result else None
    
    def link_object_to_group(self, object_id: int, group_id: int):
        """Связывание объекта с группой"""
        with self.get_connection() as conn:
            conn.execute("""
                INSERT OR IGNORE INTO object_group_mapping (object_id, group_id)
                VALUES (?, ?)
            """, (object_id, group_id))
    
    def migrate_csv_to_database(self, csv_data: pd.DataFrame, source: str = "csv") -> Dict[str, int]:
        """Миграция данных из CSV в базу данных"""
        stats = {
            'objects_created': 0,
            'reviews_created': 0,
            'analysis_results_created': 0
        }
        
        with self.get_connection() as conn:
            for _, row in csv_data.iterrows():
                # Добавляем объект
                object_id = self.insert_object(
                    name=row.get('name', ''),
                    address=row.get('address', ''),
                    latitude=row.get('latitude'),
                    longitude=row.get('longitude'),
                    district=row.get('district')
                )
                
                if object_id:
                    stats['objects_created'] += 1
                    
                    # Добавляем отзыв
                    review_id = self.insert_review(
                        object_id=object_id,
                        review_text=row.get('review_text', ''),
                        rating=row.get('rating'),
                        review_date=row.get('review_date'),
                        source=source,
                        external_id=row.get('external_id')
                    )
                    
                    if review_id:
                        stats['reviews_created'] += 1
                        
                        # Добавляем результаты анализа, если есть
                        for method_name in ['user_rating', 'nlp_vader', 'llm_yandex']:
                            sentiment_col = f'{method_name}_sentiment'
                            if sentiment_col in row and pd.notna(row[sentiment_col]):
                                method_id = self.get_method_id(method_name)
                                if method_id:
                                    self.insert_analysis_result(
                                        review_id=review_id,
                                        method_id=method_id,
                                        sentiment=row[sentiment_col]
                                    )
                                    stats['analysis_results_created'] += 1
                        
                        # Связываем с группой, если есть
                        group_type = row.get('group_type')
                        if group_type:
                            group_id = self.get_group_id(group_type)
                            if group_id:
                                self.link_object_to_group(object_id, group_id)
        
        return stats
    
    def export_to_dataframe(self, include_analysis: bool = True) -> pd.DataFrame:
        """Экспорт данных из БД в DataFrame"""
        query = """
        SELECT 
            o.id as object_id,
            o.name,
            o.address,
            o.latitude,
            o.longitude,
            o.district,
            r.id as review_id,
            r.review_text,
            r.rating,
            r.review_date,
            r.source,
            r.external_id
        FROM objects o
        LEFT JOIN reviews r ON o.id = r.object_id
        ORDER BY o.id, r.id
        """
        
        with self.get_connection() as conn:
            df = pd.read_sql_query(query, conn)
            
            if include_analysis:
                # Добавляем результаты анализа
                analysis_query = """
                SELECT 
                    ar.review_id,
                    pm.method_name,
                    ar.sentiment,
                    ar.confidence,
                    ar.review_type
                FROM analysis_results ar
                JOIN processing_methods pm ON ar.method_id = pm.id
                """
                
                analysis_df = pd.read_sql_query(analysis_query, conn)
                
                # Преобразуем в широкий формат
                if not analysis_df.empty:
                    analysis_wide = analysis_df.pivot(
                        index='review_id',
                        columns='method_name',
                        values=['sentiment', 'confidence']
                    )
                    
                    # Сглаживаем имена колонок
                    analysis_wide.columns = [f"{col[1]}_{col[0]}" for col in analysis_wide.columns]
                    analysis_wide = analysis_wide.reset_index()
                    
                    # Объединяем с основными данными
                    df = df.merge(analysis_wide, on='review_id', how='left')
        
        return df
    
    def get_statistics(self) -> Dict[str, int]:
        """Получение статистики базы данных"""
        with self.get_connection() as conn:
            stats = {}
            
            # Количество объектов
            cursor = conn.execute("SELECT COUNT(*) as count FROM objects")
            stats['objects_count'] = cursor.fetchone()['count']
            
            # Количество отзывов
            cursor = conn.execute("SELECT COUNT(*) as count FROM reviews")
            stats['reviews_count'] = cursor.fetchone()['count']
            
            # Количество результатов анализа
            cursor = conn.execute("SELECT COUNT(*) as count FROM analysis_results")
            stats['analysis_results_count'] = cursor.fetchone()['count']
            
            # Количество методов
            cursor = conn.execute("SELECT COUNT(*) as count FROM processing_methods")
            stats['methods_count'] = cursor.fetchone()['count']
            
            return stats
    
    def get_sentiment_distribution(self, method_name: str = None) -> pd.DataFrame:
        """Получение распределения сентиментов"""
        query = """
        SELECT 
            ar.sentiment,
            COUNT(*) as count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
        FROM analysis_results ar
        """
        
        params = []
        if method_name:
            query += " JOIN processing_methods pm ON ar.method_id = pm.id WHERE pm.method_name = ?"
            params.append(method_name)
        
        query += " GROUP BY ar.sentiment ORDER BY count DESC"
        
        with self.get_connection() as conn:
            return pd.read_sql_query(query, conn, params=params)


# Синглтон для глобального доступа к БД
db_manager = DatabaseManager() 