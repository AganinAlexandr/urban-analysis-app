"""
Модуль для работы с базой данных SQLite (исправленная версия)
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
    
    def __init__(self, db_path: str = "urban_analysis_fixed.db"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Инициализация базы данных"""
        schema_path = Path(__file__).parent.parent.parent / "database_schema_fixed.sql"
        
        with sqlite3.connect(self.db_path) as conn:
            # Включение поддержки внешних ключей
            conn.execute("PRAGMA foreign_keys = ON")
            
            # Чтение и выполнение схемы
            if schema_path.exists():
                with open(schema_path, 'r', encoding='utf-8') as f:
                    schema = f.read()
                    conn.executescript(schema)
            
            # Добавляем поле в_Выборке если его нет (для совместимости со старыми БД)
            try:
                cursor = conn.execute("PRAGMA table_info(reviews)")
                columns = [column[1] for column in cursor.fetchall()]
                
                if 'в_Выборке' not in columns:
                    conn.execute("ALTER TABLE reviews ADD COLUMN в_Выборке TEXT DEFAULT NULL")
                    print("Поле в_Выборке добавлено в таблицу reviews")
            except Exception as e:
                print(f"Ошибка при добавлении поля в_Выборке: {e}")
    
    def get_connection(self):
        """Получение соединения с базой данных"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # Для доступа к колонкам по имени
        return conn
    
    def create_object_key(self, name: str, address: str) -> str:
        """Создание уникального ключа объекта"""
        combined = f"{name}|{address}".lower().strip()
        return hashlib.md5(combined.encode()).hexdigest()
    
    def create_group(self, group_name: str, group_type: str) -> int:
        """Создание группы в таблице object_groups"""
        with self.get_connection() as conn:
            cursor = conn.execute("""
                INSERT INTO object_groups (group_name, group_type)
                VALUES (?, ?)
            """, (group_name, group_type))
            return cursor.lastrowid

    def insert_object(self, name: str, address: str, latitude: float = None, 
                     longitude: float = None, district: str = None,
                     group_type: str = None, detected_group_type: str = None) -> int:
        """Добавление объекта в базу данных"""
        object_key = self.create_object_key(name, address)
        
        # Получаем ID группы
        group_id = None
        if group_type:
            group_id = self.get_group_id(group_type)
            if group_id is None:
                # Создаем группу, если её нет
                group_id = self.create_group(group_type, group_type)
        
        # Получаем ID определяемой группы
        detected_group_id = None
        if detected_group_type:
            detected_group_id = self.get_detected_group_id(detected_group_type)
            if detected_group_id is None:
                # Создаем определяемую группу, если её нет
                detected_group_id = self.create_detected_group(detected_group_type, detected_group_type)
        
        with self.get_connection() as conn:
            cursor = conn.execute("""
                INSERT OR IGNORE INTO objects 
                (name, address, object_key, latitude, longitude, district, group_id, detected_group_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (name, address, object_key, latitude, longitude, district, group_id, detected_group_id))
            
            # Получаем ID объекта
            cursor = conn.execute("SELECT id FROM objects WHERE object_key = ?", (object_key,))
            result = cursor.fetchone()
            return result['id'] if result else None
    
    def insert_review(self, object_id: int, review_text: str, rating: int = None,
                     review_date: str = None, source: str = None, external_id: str = None) -> int:
        """Добавление отзыва в базу данных"""
        with self.get_connection() as conn:
            # Проверяем, существует ли уже такой отзыв
            cursor = conn.execute("""
                SELECT id FROM reviews 
                WHERE object_id = ? AND review_text = ? AND rating = ? AND review_date = ?
            """, (object_id, review_text, rating, review_date))
            
            existing_review = cursor.fetchone()
            if existing_review:
                # Отзыв уже существует, возвращаем его ID
                return existing_review['id']
            
            # Добавляем новый отзыв
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
    
    def get_detected_group_id(self, group_type: str) -> Optional[int]:
        """Получение ID определяемой группы по типу"""
        with self.get_connection() as conn:
            cursor = conn.execute("SELECT id FROM detected_groups WHERE group_type = ?", (group_type,))
            result = cursor.fetchone()
            return result['id'] if result else None
    
    def create_detected_group(self, group_name: str, group_type: str, 
                            detection_method: str = "auto", confidence: float = 1.0) -> int:
        """Создание определяемой группы"""
        with self.get_connection() as conn:
            cursor = conn.execute("""
                INSERT INTO detected_groups (group_name, group_type, detection_method, confidence)
                VALUES (?, ?, ?, ?)
            """, (group_name, group_type, detection_method, confidence))
            return cursor.lastrowid
    
    def migrate_csv_to_database(self, csv_data: pd.DataFrame, source: str = "csv") -> Dict[str, int]:
        """Миграция данных из CSV в базу данных"""
        stats = {
            'objects_created': 0,
            'reviews_created': 0,
            'analysis_results_created': 0
        }
        
        with self.get_connection() as conn:
            for _, row in csv_data.iterrows():
                # Определяем группу из данных или пути файла
                group_type = row.get('group')  # В JSON файлах поле называется 'group'
                detected_group_type = row.get('detected_group_type', group_type)
                
                # Обрабатываем координаты
                lat = row.get('latitude')
                lon = row.get('longitude')
                
                # Преобразуем строки "None" в None
                if lat is not None and str(lat).lower() == 'none':
                    lat = None
                if lon is not None and str(lon).lower() == 'none':
                    lon = None
                
                # Преобразуем в float, если возможно
                if lat is not None and lat != 0:
                    try:
                        lat = float(lat)
                    except (ValueError, TypeError):
                        lat = None
                if lon is not None and lon != 0:
                    try:
                        lon = float(lon)
                    except (ValueError, TypeError):
                        lon = None
                
                # Добавляем объект
                object_id = self.insert_object(
                    name=row.get('name', ''),
                    address=row.get('address', ''),
                    latitude=lat,
                    longitude=lon,
                    district=row.get('district'),
                    group_type=group_type,
                    detected_group_type=detected_group_type
                )
                
                if object_id:
                    # Проверяем, был ли объект создан только что или уже существовал
                    object_key = self.create_object_key(row.get('name', ''), row.get('address', ''))
                    cursor = conn.execute("""
                        SELECT COUNT(*) as count FROM objects WHERE object_key = ?
                    """, (object_key,))
                    
                    # Если объект был создан только что (INSERT OR IGNORE сработал)
                    if cursor.fetchone()['count'] > 0:
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
                    
                    # Проверяем, был ли отзыв создан только что
                    if review_id:
                        cursor = conn.execute("""
                            SELECT COUNT(*) as count FROM reviews WHERE id = ? AND created_at >= datetime('now', '-1 second')
                        """, (review_id,))
                        
                        if cursor.fetchone()['count'] > 0:
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
            og.group_name,
            og.group_type,
            dg.group_name as detected_group_name,
            dg.group_type as detected_group_type,
            r.id as review_id,
            r.review_text,
            r.rating,
            r.review_date,
            r.source,
            r.external_id
        FROM objects o
        LEFT JOIN object_groups og ON o.group_id = og.id
        LEFT JOIN detected_groups dg ON o.detected_group_id = dg.id
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
            
            # Количество групп
            cursor = conn.execute("SELECT COUNT(*) as count FROM object_groups")
            stats['groups_count'] = cursor.fetchone()['count']
            
            # Количество определяемых групп
            cursor = conn.execute("SELECT COUNT(*) as count FROM detected_groups")
            stats['detected_groups_count'] = cursor.fetchone()['count']
            
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
    
    def clear_all_data(self):
        """Полная очистка всех данных из базы данных"""
        with self.get_connection() as conn:
            # Отключаем проверку внешних ключей для очистки
            conn.execute("PRAGMA foreign_keys = OFF")
            
            # Очищаем таблицы в правильном порядке (от зависимых к независимым)
            tables_to_clear = [
                'analysis_results',
                'reviews', 
                'objects',
                'detected_groups',
                'object_groups'
            ]
            
            for table in tables_to_clear:
                try:
                    conn.execute(f"DELETE FROM {table}")
                    print(f"Очищена таблица: {table}")
                except Exception as e:
                    print(f"Ошибка при очистке таблицы {table}: {e}")
            
            # Включаем обратно проверку внешних ключей
            conn.execute("PRAGMA foreign_keys = ON")
            
            # Сбрасываем автоинкрементные счетчики
            for table in tables_to_clear:
                try:
                    conn.execute(f"DELETE FROM sqlite_sequence WHERE name = '{table}'")
                except Exception as e:
                    print(f"Ошибка при сбросе счетчика для {table}: {e}")
            
            conn.commit()
            print("✅ Все данные очищены из базы данных")


# Синглтон для глобального доступа к БД
db_manager_fixed = DatabaseManager() 