"""
Модуль для работы с YandexGPT API для генерации эмбеддингов.
Реализует гибридную систему токенов: OAuth (основной) + IAM (fallback).
"""

import os
import json
import logging
import subprocess
import requests
from typing import Optional, List, Dict
from datetime import datetime, timedelta

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EmbeddingsService:
    """
    Сервис для работы с YandexGPT API для генерации эмбеддингов.
    Поддерживает гибридную систему токенов и автоматическое обновление IAM токенов.
    """
    
    def __init__(self):
        """Инициализация сервиса с загрузкой токенов из переменных окружения."""
        self.folder_id = os.getenv('YANDEX_FOLDER_ID')
        self.oauth_token = os.getenv('YANDEX_GPT_OAUTH_TOKEN')
        self.iam_token = os.getenv('YANDEX_IAM_TOKEN')
        
        # URL для API эмбеддингов
        self.embeddings_url = "https://llm.api.cloud.yandex.net:443/foundationModels/v1/textEmbedding"
        
        # Модель для эмбеддингов (используем text-search-doc для документов)
        self.model_uri = f"emb://{self.folder_id}/text-search-doc/latest"
        
        logger.info("🚀 EmbeddingsService инициализирован")
        logger.info(f"📁 Folder ID: {self.folder_id}")
        logger.info(f"🔑 OAuth токен: {'✅' if self.oauth_token else '❌'}")
        logger.info(f"🔑 IAM токен: {'✅' if self.iam_token else '❌'}")
    
    def _truncate_text_for_tokens(self, text: str, max_tokens: int = 2000) -> str:
        """
        Обрезает текст до безопасного лимита токенов.
        Примерно 1 токен = 4 символа для русского текста.
        
        Args:
            text: Исходный текст
            max_tokens: Максимальное количество токенов (по умолчанию 2000 для безопасности)
            
        Returns:
            Обрезанный текст
        """
        # Безопасный лимит символов (примерно 4 символа на токен)
        max_chars = max_tokens * 4
        
        if len(text) <= max_chars:
            return text
        
        # Обрезаем текст и добавляем индикатор
        truncated_text = text[:max_chars] + "\n\n[Текст обрезан для соответствия лимиту токенов]"
        logger.warning(f"⚠️ Текст обрезан с {len(text)} до {len(truncated_text)} символов (лимит: {max_tokens} токенов)")
        
        return truncated_text
    
    def _get_headers(self, use_oauth: bool = True) -> dict:
        """
        Формирует заголовки для API запроса.
        
        Args:
            use_oauth: True - использовать OAuth токен, False - IAM токен
            
        Returns:
            Словарь с заголовками для HTTP запроса
        """
        if use_oauth and self.oauth_token:
            return {
                'Authorization': f'Bearer {self.oauth_token}',
                'Content-Type': 'application/json'
            }
        elif not use_oauth and self.iam_token:
            return {
                'Authorization': f'Bearer {self.iam_token}',
                'Content-Type': 'application/json',
                'x-folder-id': self.folder_id
            }
        else:
            raise ValueError("Токен не найден для указанного типа аутентификации")
    
    def _make_api_request(self, text: str, use_oauth: bool = True) -> Optional[dict]:
        """
        Выполняет запрос к YandexGPT API для получения эмбеддинга.
        
        Args:
            text: Текст для генерации эмбеддинга
            use_oauth: True - использовать OAuth токен, False - IAM токен
            
        Returns:
            Ответ API или None в случае ошибки
        """
        try:
            headers = self._get_headers(use_oauth)
            
            payload = {
                'modelUri': self.model_uri,
                'text': text
            }
            
            logger.info(f"📤 Отправка запроса к API (токен: {'OAuth' if use_oauth else 'IAM'})")
            logger.info(f"📝 Текст длиной: {len(text)} символов")
            
            response = requests.post(
                self.embeddings_url,
                headers=headers,
                json=payload,
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                logger.info(f"✅ Успешный ответ от API")
                return result
            else:
                logger.error(f"❌ Ошибка API: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Ошибка при запросе к API: {str(e)}")
            return None
    
    def get_text_embedding(self, text: str) -> Optional[List[float]]:
        """
        Получает эмбеддинг для текста, используя гибридную систему токенов.
        
        Args:
            text: Текст для генерации эмбеддинга
            
        Returns:
            Список чисел (вектор эмбеддинга) или None в случае ошибки
        """
        if not text or not text.strip():
            logger.warning("⚠️ Пустой текст для эмбеддинга")
            return None
        
        # Обрезаем текст до безопасного лимита токенов
        truncated_text = self._truncate_text_for_tokens(text)
        
        # Сначала пробуем OAuth токен
        if self.oauth_token:
            result = self._make_api_request(truncated_text, use_oauth=True)
            if result and 'embedding' in result:
                embedding = result['embedding']
                logger.info(f"✅ Получен эмбеддинг через OAuth для текста длиной {len(text)} символов")
                return embedding
        
        # Если OAuth не сработал, пробуем IAM токен
        if self.iam_token:
            logger.info("🔄 Используем IAM токен как fallback")
            result = self._make_api_request(truncated_text, use_oauth=False)
            if result and 'embedding' in result:
                embedding = result['embedding']
                logger.info(f"✅ Получен эмбеддинг через IAM токен для текста длиной {len(text)} символов")
                return embedding
        
        logger.error("❌ Не удалось получить эмбеддинг ни с одним токеном")
        return None
    
    def update_iam_token(self) -> bool:
        """
        Автоматически обновляет IAM токен через Yandex CLI.
        
        Returns:
            True если токен успешно обновлен, False в случае ошибки
        """
        try:
            logger.info("🔄 Обновление IAM токена через Yandex CLI...")
            
            # Выполняем команду для создания нового IAM токена
            result = subprocess.run(
                ['yc', 'iam', 'create-token'],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                new_token = result.stdout.strip()
                if new_token and len(new_token) > 10:  # Проверяем, что получили валидный токен
                    self.iam_token = new_token
                    
                    # Обновляем переменную окружения
                    os.environ['YANDEX_IAM_TOKEN'] = new_token
                    
                    # Обновляем файл env_data.env
                    self._update_env_file(new_token)
                    
                    logger.info("✅ IAM токен успешно обновлен")
                    return True
                else:
                    logger.error("❌ Получен некорректный токен")
                    return False
            else:
                logger.error(f"❌ Ошибка выполнения команды yc: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            logger.error("❌ Таймаут при выполнении команды yc")
            return False
        except FileNotFoundError:
            logger.error("❌ Yandex CLI (yc) не найден. Установите Yandex CLI")
            return False
        except Exception as e:
            logger.error(f"❌ Ошибка при обновлении IAM токена: {str(e)}")
            return False
    
    def _update_env_file(self, new_token: str):
        """
        Обновляет IAM токен в файле env_data.env.
        
        Args:
            new_token: Новый IAM токен
        """
        try:
            env_file_path = "env_data.env"
            if os.path.exists(env_file_path):
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
                
                # Записываем обновленный файл
                with open(env_file_path, 'w', encoding='utf-8') as f:
                    f.writelines(lines)
                
                logger.info("✅ Файл env_data.env обновлен")
            else:
                logger.warning("⚠️ Файл env_data.env не найден")
                
        except Exception as e:
            logger.error(f"❌ Ошибка при обновлении файла env_data.env: {str(e)}")
    
    def get_token_status(self) -> Dict[str, any]:
        """
        Возвращает детальную информацию о статусе токенов.
        
        Returns:
            Словарь с информацией о токенах
        """
        status = {
            'folder_id': {
                'value': self.folder_id,
                'status': '✅' if self.folder_id else '❌',
                'description': 'Folder ID для Yandex Cloud'
            },
            'oauth_token': {
                'value': f"{self.oauth_token[:10]}..." if self.oauth_token else None,
                'status': '✅' if self.oauth_token else '❌',
                'description': 'OAuth токен (долгосрочный)'
            },
            'iam_token': {
                'value': f"{self.iam_token[:10]}..." if self.iam_token else None,
                'status': '✅' if self.iam_token else '❌',
                'description': 'IAM токен (краткосрочный, 12 часов)'
            },
            'api_url': self.embeddings_url,
            'model_uri': self.model_uri,
            'timestamp': datetime.now().isoformat()
        }
        
        return status
    
    def test_connection(self) -> Dict[str, any]:
        """
        Тестирует подключение к YandexGPT API.
        
        Returns:
            Словарь с результатами тестирования
        """
        test_text = "Тестовый текст для проверки подключения к API"
        
        logger.info("🧪 Тестирование подключения к YandexGPT API...")
        
        # Тестируем OAuth токен
        oauth_result = None
        if self.oauth_token:
            oauth_result = self._make_api_request(test_text, use_oauth=True)
        
        # Тестируем IAM токен
        iam_result = None
        if self.iam_token:
            iam_result = self._make_api_request(test_text, use_oauth=False)
        
        test_results = {
            'oauth_test': {
                'status': '✅' if oauth_result else '❌',
                'details': 'OAuth токен работает' if oauth_result else 'OAuth токен не работает'
            },
            'iam_test': {
                'status': '✅' if iam_result else '❌',
                'details': 'IAM токен работает' if iam_result else 'IAM токен не работает'
            },
            'overall_status': '✅' if (oauth_result or iam_result) else '❌',
            'test_text': test_text,
            'timestamp': datetime.now().isoformat()
        }
        
        if oauth_result or iam_result:
            logger.info("✅ Тест подключения успешен")
        else:
            logger.error("❌ Тест подключения не прошел")
        
        return test_results
    
    def reduce_dimensions(self, embeddings: List[List[float]], target_dim: int = 3) -> List[List[float]]:
        """
        Уменьшает размерность эмбеддингов с помощью PCA.
        
        Args:
            embeddings: Список эмбеддингов
            target_dim: Целевая размерность (по умолчанию 3 для 3D визуализации)
            
        Returns:
            Список эмбеддингов с уменьшенной размерностью
        """
        try:
            from sklearn.decomposition import PCA
            
            if len(embeddings) == 0:
                return []
            
            # Если размерность уже меньше или равна целевой, возвращаем как есть
            if len(embeddings[0]) <= target_dim:
                return embeddings
            
            pca = PCA(n_components=target_dim)
            reduced_embeddings = pca.fit_transform(embeddings)
            
            logger.info(f"✅ Размерность эмбеддингов уменьшена с {len(embeddings[0])} до {target_dim}")
            return reduced_embeddings.tolist()
            
        except ImportError:
            logger.warning("⚠️ scikit-learn не установлен. Установите: pip install scikit-learn")
            # Возвращаем первые target_dim компонент каждого эмбеддинга
            return [emb[:target_dim] for emb in embeddings]
        except Exception as e:
            logger.error(f"❌ Ошибка при уменьшении размерности: {str(e)}")
            return embeddings
