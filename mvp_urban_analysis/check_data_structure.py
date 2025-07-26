import sqlite3
import pandas as pd

def check_data_structure():
    db_path = "urban_analysis.db"
    
    print(f"🔍 Проверяем структуру данных в БД: {db_path}")
    
    conn = sqlite3.connect(db_path)
    
    # Проверяем таблицы
    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    print(f"📋 Таблицы в БД: {[table[0] for table in tables]}")
    
    # Проверяем данные из БД
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
    
    df = pd.read_sql_query(query, conn)
    
    print(f"\n📊 Структура данных:")
    print(f"  Размер: {df.shape}")
    print(f"  Колонки: {list(df.columns)}")
    
    print(f"\n📋 Первые записи:")
    print(df.head())
    
    print(f"\n📊 Статистика по группам:")
    if 'group_type' in df.columns:
        print(f"  group_type: {df['group_type'].value_counts().to_dict()}")
    if 'detected_group_type' in df.columns:
        print(f"  detected_group_type: {df['detected_group_type'].value_counts().to_dict()}")
    
    print(f"\n📍 Координаты:")
    print(f"  С координатами: {df[(df['latitude'].notna()) & (df['longitude'].notna())].shape[0]}")
    print(f"  Без координат: {df[(df['latitude'].isna()) | (df['longitude'].isna())].shape[0]}")
    
    conn.close()

if __name__ == "__main__":
    check_data_structure() 