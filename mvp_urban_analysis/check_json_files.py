#!/usr/bin/env python3
"""
Проверка JSON файлов и выбор небольших файлов для тестирования
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def check_json_files():
    """Проверяет JSON файлы в папках данных"""
    print("=== ПРОВЕРКА JSON ФАЙЛОВ ===")
    
    # Папки с данными
    data_folders = [
        'mvp_urban_analysis/data/initial_data/json/schools_parse',
        'mvp_urban_analysis/data/initial_data/json/hospital_yandex',
        'mvp_urban_analysis/data/initial_data/json/kindergarden_parse',
        'mvp_urban_analysis/data/initial_data/json/university_parse'
    ]
    
    small_files = []
    
    for folder in data_folders:
        if os.path.exists(folder):
            print(f"\n=== ПАПКА: {folder} ===")
            
            # Получаем список файлов
            files = [f for f in os.listdir(folder) if f.endswith('.json')]
            
            # Разделяем на большие и маленькие файлы
            large_files = [f for f in files if not f.startswith('_')]
            small_files_in_folder = [f for f in files if f.startswith('_')]
            
            print(f"Всего JSON файлов: {len(files)}")
            print(f"Больших файлов: {len(large_files)}")
            print(f"Маленьких файлов (начинаются с '_'): {len(small_files_in_folder)}")
            
            if small_files_in_folder:
                print("Маленькие файлы:")
                for file in small_files_in_folder:
                    file_path = os.path.join(folder, file)
                    file_size = os.path.getsize(file_path)
                    print(f"  - {file} ({file_size} байт)")
                    small_files.append(file_path)
        else:
            print(f"⚠️ Папка не найдена: {folder}")
    
    print(f"\n=== ИТОГО МАЛЕНЬКИХ ФАЙЛОВ ===")
    print(f"Найдено {len(small_files)} файлов для тестирования:")
    for file in small_files:
        print(f"  {file}")
    
    return small_files

if __name__ == "__main__":
    check_json_files() 