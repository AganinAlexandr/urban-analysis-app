#!/usr/bin/env python3
"""
Простое Flask приложение для работы с простой БД
"""

from flask import Flask, render_template, jsonify
import sqlite3
import pandas as pd
import json

app = Flask(__name__)

def get_map_data():
    """Получает данные для карты из простой БД"""
    try:
        with sqlite3.connect("urban_analysis_fixed.db") as conn:
            # Получаем объекты с координатами
            query = """
            SELECT DISTINCT
                o.id,
                o.name,
                o.address,
                o.latitude,
                o.longitude,
                o.district,
                o.group_type,
                o.rating,
                o.count_rating,
                o.stars
            FROM simple_objects o
            WHERE o.latitude IS NOT NULL 
              AND o.longitude IS NOT NULL
            ORDER BY o.id
            """
            
            df = pd.read_sql_query(query, conn)
            
            if df.empty:
                print("⚠️ Нет объектов с координатами в БД")
                return []
            
            # Преобразуем в формат для карты
            map_data = []
            for _, row in df.iterrows():
                map_data.append({
                    'id': row['id'],
                    'name': row['name'],
                    'address': row['address'],
                    'latitude': row['latitude'],
                    'longitude': row['longitude'],
                    'district': row['district'],
                    'group_type': row['group_type'],
                    'rating': row['rating'],
                    'count_rating': row['count_rating'],
                    'stars': row['stars']
                })
            
            print(f"✅ Загружено {len(map_data)} объектов для карты")
            print(f"Группы: {df['group_type'].value_counts().to_dict()}")
            
            return map_data
            
    except Exception as e:
        print(f"❌ Ошибка получения данных для карты: {e}")
        return []

def get_statistics():
    """Получает статистику из простой БД"""
    try:
        with sqlite3.connect("urban_analysis_fixed.db") as conn:
            # Статистика объектов
            objects_query = "SELECT COUNT(*) as total_objects FROM simple_objects"
            objects_count = conn.execute(objects_query).fetchone()[0]
            
            # Статистика отзывов
            reviews_query = "SELECT COUNT(*) as total_reviews FROM simple_reviews"
            reviews_count = conn.execute(reviews_query).fetchone()[0]
            
            # Статистика групп
            groups_query = """
            SELECT group_type, COUNT(*) as count 
            FROM simple_objects 
            GROUP BY group_type
            """
            groups_df = pd.read_sql_query(groups_query, conn)
            groups_stats = groups_df.set_index('group_type')['count'].to_dict()
            
            # Статистика объектов с координатами
            coords_query = """
            SELECT COUNT(*) as coords_count 
            FROM simple_objects 
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
            """
            coords_count = conn.execute(coords_query).fetchone()[0]
            
            return {
                'total_objects': objects_count,
                'total_reviews': reviews_count,
                'groups': groups_stats,
                'objects_with_coords': coords_count
            }
            
    except Exception as e:
        print(f"❌ Ошибка получения статистики: {e}")
        return {
            'total_objects': 0,
            'total_reviews': 0,
            'groups': {},
            'objects_with_coords': 0
        }

@app.route('/')
def index():
    """Главная страница"""
    stats = get_statistics()
    return render_template('index.html', stats=stats)

@app.route('/api/map-data')
def map_data():
    """API для получения данных карты"""
    data = get_map_data()
    return jsonify(data)

@app.route('/api/statistics')
def statistics():
    """API для получения статистики"""
    stats = get_statistics()
    return jsonify(stats)

if __name__ == '__main__':
    print("=== ПРОСТОЕ ПРИЛОЖЕНИЕ ===")
    print("Запускаем простое Flask приложение...")
    
    # Проверяем данные
    map_data = get_map_data()
    if map_data:
        print(f"✅ Найдено {len(map_data)} объектов для карты")
        for obj in map_data:
            print(f"  {obj['name']} - {obj['group_type']} ({obj['latitude']}, {obj['longitude']})")
    else:
        print("⚠️ Нет данных для карты")
    
    # Запускаем приложение
    app.run(debug=True, host='0.0.0.0', port=5000) 