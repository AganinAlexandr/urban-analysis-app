#!/usr/bin/env python3
"""
Демонстрационный скрипт для новой функциональности диаграммы заполненности полей
"""

import requests
import json
import time

def demo_completeness_feature():
    """Демонстрация новой функциональности"""
    
    print("🎯 ДЕМОНСТРАЦИЯ НОВОЙ ФУНКЦИОНАЛЬНОСТИ")
    print("=" * 50)
    print()
    print("✅ Реализована диаграмма заполненности полей в архиве")
    print("✅ Новый макет с разделением на две колонки")
    print("✅ Цветовая индикация уровня заполненности")
    print("✅ Интерактивная диаграмма с Chart.js")
    print()
    
    try:
        # Получаем информацию об архиве
        response = requests.get('http://localhost:5000/archive/info')
        
        if response.status_code == 200:
            data = response.json()
            
            print("📊 ТЕКУЩЕЕ СОСТОЯНИЕ АРХИВА:")
            print(f"   Всего записей: {data['total_records']}")
            print(f"   Количество групп: {len(data['groups'])}")
            print()
            
            print("📈 ЗАПОЛНЕННОСТЬ ПОЛЕЙ:")
            field_names = {
                'review_text': 'Текст отзыва',
                'rating': 'Рейтинг',
                'answer_text': 'Ответ',
                'latitude': 'Широта',
                'longitude': 'Долгота',
                'district': 'Район'
            }
            
            for field, percentage in data['field_completeness'].items():
                display_name = field_names.get(field, field)
                if percentage >= 80:
                    status = "🟢 Отлично"
                elif percentage >= 50:
                    status = "🟡 Хорошо"
                else:
                    status = "🔴 Требует внимания"
                
                print(f"   {display_name}: {percentage}% {status}")
            
            print()
            print("🎨 ОСОБЕННОСТИ НОВОГО ИНТЕРФЕЙСА:")
            print("   • Левая колонка: основная статистика архива")
            print("   • Правая колонка: диаграмма заполненности полей")
            print("   • Цветовая индикация: зеленый (>80%), желтый (50-80%), красный (<50%)")
            print("   • Интерактивная диаграмма с подсказками")
            print("   • Автоматическое обновление при загрузке новых данных")
            print()
            print("🌐 ДЛЯ ПРОСМОТРА:")
            print("   1. Откройте браузер")
            print("   2. Перейдите на http://localhost:5000")
            print("   3. Нажмите кнопку 'Информация об архиве'")
            print("   4. Увидите новый макет с диаграммой")
            print()
            print("✅ Функциональность готова к использованию!")
            
            return True
        else:
            print(f"❌ Ошибка получения данных: HTTP {response.status_code}")
            return False
            
    except requests.exceptions.ConnectionError:
        print("❌ Не удалось подключиться к серверу")
        print("   Убедитесь, что приложение запущено: python app.py")
        return False
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return False

def main():
    """Основная функция"""
    
    print("Запуск демонстрации новой функциональности...")
    print("Убедитесь, что приложение запущено на http://localhost:5000")
    print()
    
    time.sleep(1)
    
    if demo_completeness_feature():
        print("\n🎉 Демонстрация завершена успешно!")
        return True
    else:
        print("\n💥 Демонстрация не удалась!")
        return False

if __name__ == "__main__":
    main() 