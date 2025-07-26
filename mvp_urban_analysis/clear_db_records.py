#!/usr/bin/env python3
"""
Скрипт для очистки записей объектов, отзывов и анализов из БД (справочники не трогаются)
"""
from app.core.database_fixed import db_manager_fixed

if __name__ == "__main__":
    print("Очищаем записи объектов, отзывов и анализов из БД...")
    db_manager_fixed.clear_all_data()
    print("✅ Все записи очищены! Справочники (группы, методы и т.д.) сохранены.") 