#!/usr/bin/env python3
"""
Скрипт для очистки базы данных от дублирующихся данных
"""

import sys
import os
import logging
from datetime import datetime

# Добавляем путь к модулям
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from app.core.db_cleaner import DatabaseCleaner

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('database_cleanup.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def main():
    """Основная функция очистки базы данных"""
    
    print("=== ОЧИСТКА БАЗЫ ДАННЫХ ОТ ДУБЛИРУЮЩИХСЯ ДАННЫХ ===")
    print(f"Время запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Определяем путь к базе данных
    db_path = 'urban_analysis_fixed.db'
    
    if not os.path.exists(db_path):
        print(f"❌ Ошибка: Файл базы данных '{db_path}' не найден")
        print("Убедитесь, что вы находитесь в правильной директории")
        return False
    
    print(f"📁 База данных: {db_path}")
    print(f"📊 Размер файла: {os.path.getsize(db_path) / 1024:.1f} KB")
    print()
    
    # Создаем резервную копию
    backup_path = f"{db_path}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    try:
        import shutil
        shutil.copy2(db_path, backup_path)
        print(f"💾 Создана резервная копия: {backup_path}")
    except Exception as e:
        print(f"⚠️  Предупреждение: Не удалось создать резервную копию: {e}")
        print("Продолжаем без резервной копии...")
    print()
    
    # Создаем очиститель базы данных
    cleaner = DatabaseCleaner(db_path)
    
    try:
        # Выполняем полную очистку
        result = cleaner.clean_all_duplicates()
        
        print("✅ ОЧИСТКА ЗАВЕРШЕНА УСПЕШНО")
        print()
        
        # Выводим результаты
        print("=== РЕЗУЛЬТАТЫ ОЧИСТКИ ===")
        
        groups_cleanup = result['groups_cleanup']
        objects_cleanup = result['objects_cleanup']
        
        print(f"🗂️  Группы объектов:")
        print(f"   • Удалено дубликатов: {groups_cleanup['cleaned']}")
        print(f"   • Оставлено уникальных: {groups_cleanup['kept']}")
        
        print(f"🏢 Объекты:")
        print(f"   • Удалено дубликатов: {objects_cleanup['cleaned']}")
        print(f"   • Оставлено уникальных: {objects_cleanup['kept']}")
        
        print()
        print("=== СТАТИСТИКА БАЗЫ ДАННЫХ ===")
        
        stats_before = result['stats_before']
        stats_after = result['stats_after']
        
        print("До очистки:")
        for key, value in stats_before.items():
            if key.endswith('_count'):
                table_name = key.replace('_count', '').replace('_', ' ').title()
                print(f"   • {table_name}: {value}")
        
        print()
        print("После очистки:")
        for key, value in stats_after.items():
            if key.endswith('_count'):
                table_name = key.replace('_count', '').replace('_', ' ').title()
                print(f"   • {table_name}: {value}")
        
        # Вычисляем экономию
        total_before = sum(stats_before.values())
        total_after = sum(stats_after.values())
        saved = total_before - total_after
        
        print()
        print(f"💾 Экономия записей: {saved}")
        print(f"📉 Сжатие: {(saved / total_before * 100):.1f}%" if total_before > 0 else "📉 Сжатие: 0%")
        
        print()
        print("=== РЕКОМЕНДАЦИИ ===")
        
        if groups_cleanup['cleaned'] > 0:
            print("✅ Дублирующиеся группы объектов успешно удалены")
            print("   • Созданы уникальные индексы для предотвращения дублирования")
        
        if objects_cleanup['cleaned'] > 0:
            print("✅ Дублирующиеся объекты успешно удалены")
            print("   • Ссылки в отзывах обновлены")
        
        if groups_cleanup['cleaned'] == 0 and objects_cleanup['cleaned'] == 0:
            print("ℹ️  Дублирующихся данных не найдено")
            print("   • База данных уже оптимизирована")
        
        print()
        print("🔧 Созданы уникальные индексы для предотвращения дублирования в будущем")
        print("📊 Рекомендуется регулярно запускать очистку при добавлении новых данных")
        
        return True
        
    except Exception as e:
        print(f"❌ ОШИБКА ПРИ ОЧИСТКЕ: {e}")
        logger.error(f"Ошибка при очистке БД: {e}", exc_info=True)
        return False
    
    finally:
        print()
        print(f"⏰ Время завершения: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("📝 Подробный лог сохранен в: database_cleanup.log")


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 