#!/usr/bin/env python3
"""
Скрипт для исправления логики создания данных диаграммы в app.py
"""

def fix_chart_data_logic():
    """Исправляет логику создания данных для диаграммы"""
    
    # Читаем файл app.py
    with open('app.py', 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Старая логика (неправильная)
    old_logic = '''        # Заполняем данные для каждого отзыва
        for review_id, review_data in reviews_data.items():
            positive_data = [];
            neutral_data = [];
            negative_data = [];
            
            for method in methods_list:
                if method in review_data['methods']:
                    method_data = review_data['methods'][method]
                    if method_data['positive'] == 1:
                        positive_data.append(1)
                        neutral_data.append(0)
                        negative_data.append(0)
                    elif method_data['negative'] == 1:
                        positive_data.append(0)
                        neutral_data.append(0)
                        negative_data.append(1)
                    else:
                        positive_data.append(0)
                        neutral_data.append(1)
                        negative_data.append(0)
                else:
                    positive_data.append(0)
                    neutral_data.append(0)
                    negative_data.append(0)
            
            positive_series['data'].append(positive_data)
            neutral_series['data'].append(negative_data)
            negative_series['data'].append(neutral_data)'''
    
    # Новая логика (правильная)
    new_logic = '''        # Заполняем данные для каждого отзыва
        for review_id, review_data in reviews_data.items():
            # Ищем любой метод с результатом для этого отзыва
            has_positive = False
            has_negative = False
            has_neutral = False
            
            for method in methods_list:
                if method in review_data['methods']:
                    method_data = review_data['methods'][method]
                    if method_data['positive'] == 1:
                        has_positive = True
                    elif method_data['negative'] == 1:
                        has_negative = True
                    else:
                        has_neutral = True
            
            # Добавляем одно значение для каждого отзыва
            if has_positive:
                positive_series['data'].append(1)
                neutral_series['data'].append(0)
                negative_series['data'].append(0)
            elif has_negative:
                positive_series['data'].append(0)
                neutral_series['data'].append(0)
                negative_series['data'].append(1)
            else:
                positive_series['data'].append(0)
                neutral_series['data'].append(1)
                negative_series['data'].append(0)'''
    
    # Заменяем логику
    if old_logic in content:
        content = content.replace(old_logic, new_logic)
        
        # Сохраняем исправленный файл
        with open('app.py', 'w', encoding='utf-8') as f:
            f.write(content)
        
        print("✅ Логика создания данных диаграммы исправлена!")
        print("   Теперь каждая серия содержит простые массивы значений")
        print("   вместо массивов массивов")
        
    else:
        print("❌ Старая логика не найдена в файле")
        print("   Возможно, файл уже исправлен или структура изменилась")

if __name__ == "__main__":
    fix_chart_data_logic()







