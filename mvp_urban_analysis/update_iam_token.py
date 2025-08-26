#!/usr/bin/env python3
"""
Скрипт для автоматического обновления IAM токена Yandex Cloud.
Использует Yandex CLI для создания нового токена и обновляет env_data.env.
"""

import os
import sys
import subprocess
import logging
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def check_yc_cli():
    """Проверяет доступность Yandex CLI."""
    try:
        result = subprocess.run(['yc', '--version'], capture_output=True, text=True)
        if result.returncode == 0:
            logger.info(f"✅ Yandex CLI найден: {result.stdout.strip()}")
            return True
        else:
            logger.error("❌ Yandex CLI не работает корректно")
            return False
    except FileNotFoundError:
        logger.error("❌ Yandex CLI (yc) не найден")
        logger.info("💡 Установите Yandex CLI: https://cloud.yandex.ru/docs/cli/quickstart")
        return False

def check_authentication():
    """Проверяет аутентификацию в Yandex Cloud."""
    try:
        result = subprocess.run(['yc', 'config', 'list'], capture_output=True, text=True)
        if result.returncode == 0:
            logger.info("✅ Аутентификация в Yandex Cloud успешна")
            return True
        else:
            logger.error("❌ Не удалось получить конфигурацию Yandex Cloud")
            logger.info("💡 Выполните: yc init")
            return False
    except Exception as e:
        logger.error(f"❌ Ошибка при проверке аутентификации: {str(e)}")
        return False

def create_iam_token():
    """Создает новый IAM токен через Yandex CLI."""
    try:
        logger.info("🔄 Создание нового IAM токена...")
        
        result = subprocess.run(
            ['yc', 'iam', 'create-token'],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode == 0:
            new_token = result.stdout.strip()
            if new_token and len(new_token) > 10:
                logger.info("✅ Новый IAM токен получен")
                return new_token
            else:
                logger.error("❌ Получен некорректный токен")
                return None
        else:
            logger.error(f"❌ Ошибка создания токена: {result.stderr}")
            return None
            
    except subprocess.TimeoutExpired:
        logger.error("❌ Таймаут при создании токена")
        return None
    except Exception as e:
        logger.error(f"❌ Ошибка при создании токена: {str(e)}")
        return None

def update_env_file(new_token):
    """Обновляет IAM токен в файле env_data.env."""
    try:
        env_file_path = "env_data.env"
        
        if not os.path.exists(env_file_path):
            logger.warning(f"⚠️ Файл {env_file_path} не найден")
            return False
        
        # Читаем текущий файл
        with open(env_file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        # Ищем строку с YANDEX_IAM_TOKEN и обновляем её
        updated = False
        for i, line in enumerate(lines):
            if line.startswith('YANDEX_IAM_TOKEN='):
                lines[i] = f'YANDEX_IAM_TOKEN={new_token}\n'
                updated = True
                break
        
        # Если строка не найдена, добавляем её
        if not updated:
            lines.append(f'YANDEX_IAM_TOKEN={new_token}\n')
        
        # Создаем резервную копию
        backup_path = f"{env_file_path}.backup.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        with open(backup_path, 'w', encoding='utf-8') as f:
            f.writelines(lines)
        
        # Записываем обновленный файл
        with open(env_file_path, 'w', encoding='utf-8') as f:
            f.writelines(lines)
        
        logger.info(f"✅ Файл {env_file_path} обновлен")
        logger.info(f"💾 Резервная копия: {backup_path}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка при обновлении файла {env_file_path}: {str(e)}")
        return False

def update_environment_variable(new_token):
    """Обновляет переменную окружения YANDEX_IAM_TOKEN."""
    try:
        os.environ['YANDEX_IAM_TOKEN'] = new_token
        logger.info("✅ Переменная окружения YANDEX_IAM_TOKEN обновлена")
        return True
    except Exception as e:
        logger.error(f"❌ Ошибка при обновлении переменной окружения: {str(e)}")
        return False

def main():
    """Основная функция скрипта."""
    logger.info("🚀 Запуск обновления IAM токена Yandex Cloud")
    
    # Проверяем Yandex CLI
    if not check_yc_cli():
        sys.exit(1)
    
    # Проверяем аутентификацию
    if not check_authentication():
        sys.exit(1)
    
    # Создаем новый токен
    new_token = create_iam_token()
    if not new_token:
        logger.error("❌ Не удалось создать новый IAM токен")
        sys.exit(1)
    
    # Обновляем файл env_data.env
    if not update_env_file(new_token):
        logger.error("❌ Не удалось обновить файл env_data.env")
        sys.exit(1)
    
    # Обновляем переменную окружения
    if not update_environment_variable(new_token):
        logger.warning("⚠️ Не удалось обновить переменную окружения")
    
    logger.info("🎉 IAM токен успешно обновлен!")
    logger.info("💡 Токен действителен 12 часов")
    logger.info("💡 Для автоматического обновления добавьте этот скрипт в cron")

if __name__ == "__main__":
    main()
