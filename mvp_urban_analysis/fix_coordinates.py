import sqlite3
import os

def fix_coordinates():
    db_path = "urban_analysis.db"
    
    print(f"📍 Исправляем координаты в БД: {db_path}")
    
    conn = sqlite3.connect(db_path)
    
    # Координаты для тестовых объектов (Москва)
    test_coordinates = {
        1: (55.7558, 37.6176),  # Центр Москвы - больница
        2: (55.7517, 37.6178),  # Центр Москвы - школа  
        3: (55.7539, 37.6208)   # Центр Москвы - аптека
    }
    
    # Обновляем координаты для каждого объекта
    for obj_id, (lat, lon) in test_coordinates.items():
        cursor = conn.execute("""
            UPDATE objects 
            SET latitude = ?, longitude = ?, district = 'Центральный'
            WHERE id = ?
        """, (lat, lon, obj_id))
        
        if cursor.rowcount > 0:
            print(f"✅ Обновлены координаты для объекта ID {obj_id}: ({lat}, {lon})")
        else:
            print(f"⚠️ Объект ID {obj_id} не найден")
    
    # Проверяем результат
    cursor = conn.execute("""
        SELECT id, name, address, latitude, longitude, district
        FROM objects
        ORDER BY id
    """)
    objects = cursor.fetchall()
    
    print(f"\n🏢 Объекты после обновления:")
    for obj in objects:
        obj_id, name, address, lat, lon, district = obj
        print(f"  ID: {obj_id}")
        print(f"    Имя: {name}")
        print(f"    Адрес: {address}")
        print(f"    Координаты: lat={lat}, lon={lon}")
        print(f"    Район: {district}")
        print()
    
    conn.commit()
    conn.close()

if __name__ == "__main__":
    fix_coordinates() 