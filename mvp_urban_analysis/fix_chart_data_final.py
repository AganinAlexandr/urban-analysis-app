#!/usr/bin/env python3
"""
Финальное исправление логики создания данных диаграммы в app.py
"""

def fix_chart_data_final():
    """Исправляет логику создания данных для диаграммы - убирает массивы массивов"""
    
    # Читаем файл app.py
    with open('app.py', 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Старая логика (создает массивы массивов)
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
            neutral_series['data'].append(neutral_data)
            negative_series['data'].append(negative_data)'''
    
    # Новая логика (создает простые значения)
    new_logic = '''        # Заполняем данные для каждого отзыва
        for review_id, review_data in reviews_data.items():
            # Определяем итоговый сентимент для отзыва
            final_sentiment = 'neutral'  # по умолчанию
            
            # Ищем любой метод с результатом для этого отзыва
            for method in methods_list:
                if method in review_data['methods']:
                    method_data = review_data['methods'][method]
                    if method_data['positive'] == 1:
                        final_sentiment = 'positive'
                        break  # берем первый положительный
                    elif method_data['negative'] == 1:
                        final_sentiment = 'negative'
                        break  # берем первый отрицательный
            
            # Добавляем одно значение для каждого отзыва
            if final_sentiment == 'positive':
                positive_series['data'].append(1)
                neutral_series['data'].append(0)
                negative_series['data'].append(0)
            elif final_sentiment == 'negative':
                positive_series['data'].append(0)
                neutral_series['data'].append(0)
                negative_series['data'].append(1)
            else:  # neutral
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
        print("   Каждый отзыв теперь имеет один итоговый сентимент")
        
    else:
        print("❌ Старая логика не найдена в файле")
        print("   Возможно, файл уже исправлен или структура изменилась")

if __name__ == "__main__":
    fix_chart_data_final()






