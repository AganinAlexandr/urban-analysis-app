"""
Модуль для анализа текста с использованием Yandex GPT API
"""

import requests
import json
import logging
import os
from typing import Dict, List, Optional
import time

logger = logging.getLogger(__name__)

class YandexGPTAnalyzer:
    """Класс для анализа текста с использованием Yandex GPT API"""
    
    def __init__(self, folder_id: str = None, oauth_token: str = None):
        """
        Инициализация анализатора Yandex GPT
        
        Args:
            folder_id: ID папки в Yandex Cloud (если не указан, берется из переменных окружения)
            oauth_token: OAuth токен для доступа к API (если не указан, берется из переменных окружения)
        """
        # Получаем настройки из переменных окружения, если не переданы
        self.folder_id = folder_id or os.getenv('YANDEX_GPT_FOLDER_ID')
        self.oauth_token = oauth_token or os.getenv('YANDEX_GPT_OAUTH_TOKEN')
        
        if not self.folder_id or not self.oauth_token:
            raise ValueError(
                "Не указаны настройки Yandex GPT API. "
                "Укажите folder_id и oauth_token или установите переменные окружения "
                "YANDEX_GPT_FOLDER_ID и YANDEX_GPT_OAUTH_TOKEN"
            )
        
        # Убираем префикс Bearer если он есть
        if self.oauth_token.startswith('Bearer '):
            self.oauth_token = self.oauth_token[7:]
        
        self.base_url = "https://llm.api.cloud.yandex.net/foundationModels/v1/completion"
        self.headers = {
            "Authorization": f"Bearer {self.oauth_token}",
            "Content-Type": "application/json"
        }
        
        logger.info(f"Yandex GPT Analyzer инициализирован с folder_id: {self.folder_id}")
    
    def _make_request(self, prompt: str, max_tokens: int = 1000) -> Optional[Dict]:
        """
        Выполнение запроса к Yandex GPT API
        
        Args:
            prompt: Текст запроса
            max_tokens: Максимальное количество токенов в ответе
            
        Returns:
            Ответ от API или None в случае ошибки
        """
        try:
            payload = {
                "modelUri": f"gpt://{self.folder_id}/yandexgpt-lite",
                "completionOptions": {
                    "maxTokens": max_tokens,
                    "temperature": 0.3
                },
                "messages": [
                    {
                        "role": "user",
                        "text": prompt
                    }
                ]
            }
            
            response = requests.post(
                self.base_url,
                headers=self.headers,
                json=payload,
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Ошибка API Yandex GPT: {response.status_code} - {response.text}")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка запроса к Yandex GPT API: {e}")
            return None
        except Exception as e:
            logger.error(f"Неожиданная ошибка при запросе к Yandex GPT: {e}")
            return None
    
    def analyze_sentiment(self, text: str) -> Dict:
        """
        Анализ сентимента текста с использованием Yandex GPT
        
        Args:
            text: Текст для анализа
            
        Returns:
            Результаты анализа сентимента
        """
        if not text or not text.strip():
            return {
                'method': 'yandex_gpt',
                'sentiment': 'neutral',  # Используем английские значения для БД
                'sentiment_score': 0.0,
                'confidence': 0.5,
                'review_type': 'информационный',
                'note': 'Пустой текст'
            }
        
        # Формируем промпт для анализа сентимента
        prompt = f"""
        Проанализируй сентимент следующего отзыва и верни результат в формате JSON:
        
        Текст отзыва: "{text}"
        
        Проанализируй эмоциональную окраску текста и определи:
        1. Сентимент: положительный, отрицательный или нейтральный
        2. Уверенность в оценке (от 0.0 до 1.0)
        3. Тип отзыва: благодарность, жалоба, предложение, информационный
        
        Верни результат строго в формате JSON:
        {{
            "sentiment": "положительный/отрицательный/нейтральный",
            "confidence": 0.8,
            "review_type": "тип_отзыва"
        }}
        """
        
        try:
            response = self._make_request(prompt)
            
            if response and 'result' in response:
                # Безопасно извлекаем текст ответа
                try:
                    alternatives = response['result'].get('alternatives', [])
                    if alternatives and len(alternatives) > 0:
                        # Проверяем структуру с message
                        if 'message' in alternatives[0]:
                            response_text = alternatives[0]['message'].get('text', '').strip()
                        else:
                            # Старая структура без message
                            response_text = alternatives[0].get('text', '').strip()
                    else:
                        # Если нет alternatives, ищем текст в других местах
                        response_text = response['result'].get('text', '').strip()
                        
                    if not response_text:
                        logger.warning("Пустой ответ от Yandex GPT API")
                        return self._get_fallback_result()
                        
                except (KeyError, IndexError) as e:
                    logger.error(f"Ошибка при извлечении текста из ответа: {e}")
                    logger.debug(f"Структура ответа: {response}")
                    return self._get_fallback_result()
                
                # Пытаемся извлечь JSON из ответа
                try:
                    # Ищем JSON в ответе
                    start_idx = response_text.find('{')
                    end_idx = response_text.rfind('}') + 1
                    
                    if start_idx != -1 and end_idx > start_idx:
                        json_text = response_text[start_idx:end_idx]
                        result = json.loads(json_text)
                        
                        # Нормализуем результат
                        sentiment = result.get('sentiment', 'нейтральный')
                        confidence = float(result.get('confidence', 0.5))
                        review_type = result.get('review_type', 'информационный')
                        
                        # Ограничиваем confidence в диапазоне [0, 1]
                        confidence = max(0.0, min(1.0, confidence))
                        
                        # Конвертируем русские значения в английские
                        sentiment = self._convert_sentiment_to_english(sentiment)
                        
                        return {
                            'method': 'yandex_gpt',
                            'sentiment': sentiment,
                            'sentiment_score': self._sentiment_to_score(sentiment),
                            'confidence': confidence,
                            'review_type': review_type,
                            'raw_response': response_text
                        }
                    else:
                        # Если JSON не найден, анализируем текстовый ответ
                        return self._parse_text_response(response_text)
                        
                except json.JSONDecodeError:
                    # Если не удалось распарсить JSON, анализируем текстовый ответ
                    return self._parse_text_response(response_text)
            else:
                logger.warning("Не удалось получить ответ от Yandex GPT API")
                return self._get_fallback_result()
                
        except Exception as e:
            logger.error(f"Ошибка при анализе сентимента: {e}")
            return self._get_fallback_result()
    
    def _convert_sentiment_to_english(self, sentiment: str) -> str:
        """
        Конвертация русских значений сентимента в английские
        
        Args:
            sentiment: Русский сентимент
            
        Returns:
            Английский сентимент
        """
        sentiment_lower = sentiment.lower()
        if 'положительный' in sentiment_lower or 'positive' in sentiment_lower:
            return 'positive'
        elif 'отрицательный' in sentiment_lower or 'negative' in sentiment_lower:
            return 'negative'
        else:
            return 'neutral'
    
    def _sentiment_to_score(self, sentiment: str) -> float:
        """
        Конвертация текстового сентимента в числовой скор
        
        Args:
            sentiment: Текстовый сентимент
            
        Returns:
            Числовой скор от -1 до 1
        """
        sentiment_lower = sentiment.lower()
        if 'положительный' in sentiment_lower or 'positive' in sentiment_lower:
            return 0.7
        elif 'отрицательный' in sentiment_lower or 'negative' in sentiment_lower:
            return -0.7
        else:
            return 0.0
    
    def _parse_text_response(self, response_text: str) -> Dict:
        """
        Парсинг текстового ответа от API
        
        Args:
            response_text: Текст ответа от API
            
        Returns:
            Результат анализа
        """
        # Убираем лишние пробелы и переносы строк
        response_text = response_text.strip()
        response_lower = response_text.lower()
        
        # Определяем сентимент по ключевым словам
        if any(word in response_lower for word in ['положительный', 'positive', 'хороший', 'отличный']):
            sentiment = 'positive'  # Используем английские значения для БД
            sentiment_score = 0.7
        elif any(word in response_lower for word in ['отрицательный', 'negative', 'плохой', 'ужасный']):
            sentiment = 'negative'  # Используем английские значения для БД
            sentiment_score = -0.7
        else:
            sentiment = 'neutral'  # Используем английские значения для БД
            sentiment_score = 0.0
        
        # Определяем тип отзыва
        if any(word in response_lower for word in ['благодарность', 'спасибо']):
            review_type = 'благодарность'
        elif any(word in response_lower for word in ['жалоба', 'проблема']):
            review_type = 'жалоба'
        elif any(word in response_lower for word in ['предложение', 'рекомендация']):
            review_type = 'предложение'
        else:
            review_type = 'информационный'
        
        return {
            'method': 'yandex_gpt',
            'sentiment': sentiment,
            'sentiment_score': sentiment_score,
            'confidence': 0.6,
            'review_type': review_type,
            'raw_response': response_text
        }
    
    def _get_fallback_result(self) -> Dict:
        """
        Возвращает результат по умолчанию при ошибках
        
        Returns:
            Результат по умолчанию
        """
        return {
            'method': 'yandex_gpt',
            'sentiment': 'neutral',  # Используем английские значения для БД
            'sentiment_score': 0.0,
            'confidence': 0.5,
            'review_type': 'информационный',
            'note': 'Ошибка API'
        }
    
    def analyze_batch(self, texts: List[str]) -> List[Dict]:
        """
        Пакетный анализ текстов
        
        Args:
            texts: Список текстов для анализа
            
        Returns:
            Список результатов анализа
        """
        results = []
        
        for i, text in enumerate(texts):
            logger.info(f"Анализируем текст {i+1}/{len(texts)}")
            
            result = self.analyze_sentiment(text)
            results.append(result)
            
            # Небольшая пауза между запросами
            if i < len(texts) - 1:
                time.sleep(0.5)
        
        return results
    
    def test_connection(self) -> bool:
        """
        Тестирование подключения к API
        
        Returns:
            True если подключение успешно, False иначе
        """
        test_prompt = "Ответь одним словом: работает ли API?"
        
        try:
            response = self._make_request(test_prompt, max_tokens=10)
            if response and 'result' in response:
                logger.info("Подключение к Yandex GPT API успешно")
                return True
            else:
                logger.error("Не удалось подключиться к Yandex GPT API")
                return False
        except Exception as e:
            logger.error(f"Ошибка при тестировании подключения: {e}")
            return False 