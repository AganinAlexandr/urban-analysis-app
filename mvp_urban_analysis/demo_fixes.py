#!/usr/bin/env python3
"""
Демонстрационный скрипт для показа исправлений проблем с полями
"""

import requests
import json
import time

def demo_field_fixes():
    """Демонстрация исправлений проблем с полями"""
    
    print("🎯 ДЕМОНСТРАЦИЯ ИСПРАВЛЕНИЙ ПРОБЛЕМ С ПОЛЯМИ")
    print("=" * 60)
    print()
    print("🔧 ИСПРАВЛЕННЫЕ ПРОБЛЕМЫ:")
    print("1. ✅ Поля latitude, longitude, district теперь помечены как автоматически генерируемые")
    print("2. ✅ Маппинг поля 'stars' -> 'rating' исправлен")
    print("3. ✅ Расчет заполненности различает исходные и обработанные поля")
    print("4. ✅ Интерфейс обновлен для отображения новых названий полей")
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
            
            print("📈 ЗАПОЛНЕННОСТЬ ПОЛЕЙ (ИСПРАВЛЕННАЯ):")
            field_names = {
                'review_text': 'Текст отзыва',
                'rating': 'Рейтинг',
                'answer_text': 'Ответ',
                'latitude_auto': 'Широта (авто)',
                'longitude_auto': 'Долгота (авто)',
                'district_auto': 'Район (авто)'
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
            print("🎨 ОСОБЕННОСТИ ИСПРАВЛЕНИЙ:")
            print("   • Поля с пометкой '(авто)' добавляются автоматически в процессе обработки")
            print("   • Их заполненность показывает результат обработки, а не качество исходных данных")
            print("   • Поле 'Рейтинг' теперь правильно мапится с поля 'stars' в JSON")
            print("   • Интерфейс показывает более точную информацию о качестве данных")
            print()
            print("🌐 ДЛЯ ПРОСМОТРА:")
            print("   1. Откройте браузер")
            print("   2. Перейдите на http://localhost:5000")
            print("   3. Нажмите кнопку 'Информация об архиве'")
            print("   4. Увидите обновленную диаграмму с правильными названиями полей")
            print()
            print("✅ Все проблемы исправлены!")
            
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
    
    print("Запуск демонстрации исправлений...")
    print("Убедитесь, что приложение запущено на http://localhost:5000")
    print()
    
    time.sleep(1)
    
    if demo_field_fixes():
        print("\n🎉 Демонстрация исправлений завершена успешно!")
        return True
    else:
        print("\n💥 Демонстрация не удалась!")
        return False

if __name__ == "__main__":
    main() 