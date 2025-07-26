import sqlite3
import os

def check_coordinates():
    db_path = "urban_analysis.db"
    
    print(f"📍 Проверяем координаты в БД: {db_path}")
    
    conn = sqlite3.connect(db_path)
    
    # Проверяем объекты с координатами
    cursor = conn.execute("""
        SELECT id, name, address, latitude, longitude, district
        FROM objects
        ORDER BY id
    """)
    objects = cursor.fetchall()
    
    print(f"\n🏢 Объекты и их координаты:")
    for obj in objects:
        obj_id, name, address, lat, lon, district = obj
        print(f"  ID: {obj_id}")
        print(f"    Имя: {name}")
        print(f"    Адрес: {address}")
        print(f"    Координаты: lat={lat}, lon={lon}")
        print(f"    Район: {district}")
        print()
    
    # Проверяем объекты без координат
    cursor = conn.execute("""
        SELECT id, name, address
        FROM objects
        WHERE latitude IS NULL OR longitude IS NULL OR latitude = 0 OR longitude = 0
    """)
    objects_without_coords = cursor.fetchall()
    
    if objects_without_coords:
        print(f"⚠️ Объекты без координат:")
        for obj in objects_without_coords:
            obj_id, name, address = obj
            print(f"  ID: {obj_id}, Имя: {name}, Адрес: {address}")
    else:
        print(f"✅ Все объекты имеют координаты")
    
    conn.close()

if __name__ == "__main__":
    check_coordinates() 