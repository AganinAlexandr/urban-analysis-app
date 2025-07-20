"""
Модуль анализа отзывов с использованием LLM

Назначение:
    Анализ отзывов с использованием различных LLM через API:
    - Определение тональности (сентимента)
    - Классификация категории объекта
    - Классификация замечаний
    - Отслеживание стоимости запросов

Зависимости:
    - pandas: для обработки данных
    - openai: для работы с GPT API
    - yandexcloud: для работы с YandexGPT API
    - gigachat: для работы с GigaChat API
    - google.generativeai: для работы с Gemini API
    - dashscope: для работы с Qwen API
    - deepseek: для работы с DeepSeek API
"""

import pandas as pd
import json
from typing import Dict, List, Optional, Tuple
import os
from abc import ABC, abstractmethod
import openai
import time
from yandexcloud import SDK
import requests
from gigachat import GigaChat
from gigachat.models import Chat, Messages, MessagesRole
import google.generativeai as genai
from dashscope import Generation
from deepseek import DeepSeek

class CostTracker:
    """Класс для отслеживания стоимости запросов"""
    
    def __init__(self):
        self.costs = {
            'gpt-3.5-turbo': {'input': 0.0015, 'output': 0.002},  # $ per 1K tokens
            'gpt-4': {'input': 0.03, 'output': 0.06},
            'yandexgpt-lite': {'input': 0.0005, 'output': 0.001},  # ₽ per 1K tokens
            'gigachat': {'input': 0.0005, 'output': 0.001},  # ₽ per 1K tokens
            'gemini-pro': {'input': 0.00025, 'output': 0.0005},  # $ per 1K tokens
            'qwen-turbo': {'input': 0.0002, 'output': 0.0004},  # ¥ per 1K tokens
            'deepseek-chat': {'input': 0.0003, 'output': 0.0006}  # $ per 1K tokens
        }
        self.total_costs = {model: 0.0 for model in self.costs.keys()}
        self.total_tokens = {model: {'input': 0, 'output': 0} for model in self.costs.keys()}
    
    def add_cost(self, model: str, input_tokens: int, output_tokens: int):
        """Добавление стоимости запроса"""
        if model in self.costs:
            input_cost = (input_tokens / 1000) * self.costs[model]['input']
            output_cost = (output_tokens / 1000) * self.costs[model]['output']
            self.total_costs[model] += input_cost + output_cost
            self.total_tokens[model]['input'] += input_tokens
            self.total_tokens[model]['output'] += output_tokens
    
    def get_report(self) -> str:
        """Получение отчета о стоимости"""
        report = "Отчет о стоимости запросов:\n"
        for model, cost in self.total_costs.items():
            tokens = self.total_tokens[model]
            report += f"\n{model}:\n"
            report += f"  Общая стоимость: {cost:.4f}\n"
            report += f"  Входные токены: {tokens['input']}\n"
            report += f"  Выходные токены: {tokens['output']}\n"
        return report

class LLMAnalyzer(ABC):
    """Базовый класс для анализаторов на основе LLM"""
    
    def __init__(self, cost_tracker: CostTracker):
        self.cost_tracker = cost_tracker
    
    @abstractmethod
    def analyze_text(self, text: str) -> Dict:
        """
        Анализ текста с помощью LLM
        
        Args:
            text (str): Текст для анализа
            
        Returns:
            Dict: Результаты анализа
        """
        pass

class GPTAnalyzer(LLMAnalyzer):
    """Анализатор на основе GPT"""
    
    def __init__(self, api_key: str, model: str = "gpt-3.5-turbo", cost_tracker: CostTracker = None):
        super().__init__(cost_tracker)
        self.api_key = api_key
        self.model = model
        openai.api_key = api_key
        
    def analyze_text(self, text: str) -> Dict:
        prompt = create_prompt(text)
        
        try:
            response = openai.ChatCompletion.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "Ты - эксперт по анализу отзывов. Твоя задача - определить тональность отзыва, категорию объекта и группу замечаний (если есть)."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=500
            )
            
            # Отслеживаем стоимость
            if self.cost_tracker:
                self.cost_tracker.add_cost(
                    self.model,
                    response.usage.prompt_tokens,
                    response.usage.completion_tokens
                )
            
            result_text = response.choices[0].message.content
            
            try:
                result = json.loads(result_text)
            except json.JSONDecodeError:
                print(f"Ошибка парсинга JSON для текста: {text[:100]}...")
                return {
                    "sentiment": "нейтрально",
                    "category": "неизвестно",
                    "complaint_group": "нет"
                }
            
            return result
            
        except Exception as e:
            print(f"Ошибка при анализе текста: {str(e)}")
            return {
                "sentiment": "нейтрально",
                "category": "неизвестно",
                "complaint_group": "нет"
            }
        
        finally:
            time.sleep(1)

class GeminiAnalyzer(LLMAnalyzer):
    """Анализатор на основе Gemini"""
    
    def __init__(self, api_key: str, cost_tracker: CostTracker = None):
        super().__init__(cost_tracker)
        self.api_key = api_key
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel('gemini-pro')
        
    def analyze_text(self, text: str) -> Dict:
        prompt = create_prompt(text)
        
        try:
            response = self.model.generate_content(prompt)
            
            # Отслеживаем стоимость (примерные значения)
            if self.cost_tracker:
                self.cost_tracker.add_cost(
                    'gemini-pro',
                    len(prompt.split()) * 1.3,  # Примерная оценка токенов
                    len(response.text.split()) * 1.3
                )
            
            try:
                result = json.loads(response.text)
            except json.JSONDecodeError:
                print(f"Ошибка парсинга JSON для текста: {text[:100]}...")
                return {
                    "sentiment": "нейтрально",
                    "category": "неизвестно",
                    "complaint_group": "нет"
                }
            
            return result
            
        except Exception as e:
            print(f"Ошибка при анализе текста: {str(e)}")
            return {
                "sentiment": "нейтрально",
                "category": "неизвестно",
                "complaint_group": "нет"
            }
        
        finally:
            time.sleep(1)

class QwenAnalyzer(LLMAnalyzer):
    """Анализатор на основе Qwen"""
    
    def __init__(self, api_key: str, cost_tracker: CostTracker = None):
        super().__init__(cost_tracker)
        self.api_key = api_key
        
    def analyze_text(self, text: str) -> Dict:
        prompt = create_prompt(text)
        
        try:
            response = Generation.call(
                model='qwen-turbo',
                prompt=prompt,
                api_key=self.api_key
            )
            
            # Отслеживаем стоимость
            if self.cost_tracker:
                self.cost_tracker.add_cost(
                    'qwen-turbo',
                    len(prompt.split()) * 1.3,
                    len(response.output.text.split()) * 1.3
                )
            
            try:
                result = json.loads(response.output.text)
            except json.JSONDecodeError:
                print(f"Ошибка парсинга JSON для текста: {text[:100]}...")
                return {
                    "sentiment": "нейтрально",
                    "category": "неизвестно",
                    "complaint_group": "нет"
                }
            
            return result
            
        except Exception as e:
            print(f"Ошибка при анализе текста: {str(e)}")
            return {
                "sentiment": "нейтрально",
                "category": "неизвестно",
                "complaint_group": "нет"
            }
        
        finally:
            time.sleep(1)

class DeepSeekAnalyzer(LLMAnalyzer):
    """Анализатор на основе DeepSeek"""
    
    def __init__(self, api_key: str, cost_tracker: CostTracker = None):
        super().__init__(cost_tracker)
        self.api_key = api_key
        self.client = DeepSeek(api_key=api_key)
        
    def analyze_text(self, text: str) -> Dict:
        prompt = create_prompt(text)
        
        try:
            response = self.client.chat.completions.create(
                model="deepseek-chat",
                messages=[
                    {"role": "system", "content": "Ты - эксперт по анализу отзывов. Твоя задача - определить тональность отзыва, категорию объекта и группу замечаний (если есть)."},
                    {"role": "user", "content": prompt}
                ]
            )
            
            # Отслеживаем стоимость
            if self.cost_tracker:
                self.cost_tracker.add_cost(
                    'deepseek-chat',
                    response.usage.prompt_tokens,
                    response.usage.completion_tokens
                )
            
            try:
                result = json.loads(response.choices[0].message.content)
            except json.JSONDecodeError:
                print(f"Ошибка парсинга JSON для текста: {text[:100]}...")
                return {
                    "sentiment": "нейтрально",
                    "category": "неизвестно",
                    "complaint_group": "нет"
                }
            
            return result
            
        except Exception as e:
            print(f"Ошибка при анализе текста: {str(e)}")
            return {
                "sentiment": "нейтрально",
                "category": "неизвестно",
                "complaint_group": "нет"
            }
        
        finally:
            time.sleep(1)

class YandexGPTAnalyzer(LLMAnalyzer):
    """Анализатор на основе YandexGPT"""
    
    def __init__(self, api_key: str, folder_id: str, cost_tracker: CostTracker = None):
        """
        Инициализация анализатора
        
        Args:
            api_key (str): API ключ
            folder_id (str): ID каталога в Yandex Cloud
        """
        super().__init__(cost_tracker)
        self.api_key = api_key
        self.folder_id = folder_id
        self.api_url = "https://llm.api.cloud.yandex.net/foundationModels/v1/completion"
        
    def analyze_text(self, text: str) -> Dict:
        """
        Анализ текста с помощью YandexGPT
        
        Args:
            text (str): Текст для анализа
            
        Returns:
            Dict: Результаты анализа
        """
        prompt = create_prompt(text)
        
        try:
            # Формируем заголовки запроса
            headers = {
                "Authorization": f"Api-Key {self.api_key}",
                "Content-Type": "application/json"
            }
            
            # Формируем тело запроса
            data = {
                "modelUri": f"gpt://{self.folder_id}/yandexgpt-lite",
                "completionOptions": {
                    "stream": False,
                    "temperature": 0.3,
                    "maxTokens": "500"
                },
                "messages": [
                    {
                        "role": "system",
                        "text": "Ты - эксперт по анализу отзывов. Твоя задача - определить тональность отзыва, категорию объекта и группу замечаний (если есть)."
                    },
                    {
                        "role": "user",
                        "text": prompt
                    }
                ]
            }
            
            # Отправляем запрос
            response = requests.post(self.api_url, headers=headers, json=data)
            response.raise_for_status()
            
            # Получаем ответ
            result_text = response.json()["result"]["alternatives"][0]["message"]["text"]
            
            # Парсим JSON из ответа
            try:
                result = json.loads(result_text)
            except json.JSONDecodeError:
                # Если не удалось распарсить JSON, возвращаем пустой результат
                print(f"Ошибка парсинга JSON для текста: {text[:100]}...")
                return {
                    "sentiment": "нейтрально",
                    "category": "неизвестно",
                    "complaint_group": "нет"
                }
            
            return result
            
        except Exception as e:
            print(f"Ошибка при анализе текста: {str(e)}")
            # В случае ошибки возвращаем пустой результат
            return {
                "sentiment": "нейтрально",
                "category": "неизвестно",
                "complaint_group": "нет"
            }
        
        finally:
            # Добавляем задержку между запросами
            time.sleep(1)

class GigaChatAnalyzer(LLMAnalyzer):
    """Анализатор на основе GigaChat"""
    
    def __init__(self, api_key: str, cost_tracker: CostTracker = None):
        """
        Инициализация анализатора
        
        Args:
            api_key (str): API ключ
        """
        super().__init__(cost_tracker)
        self.api_key = api_key
        self.client = GigaChat(credentials=api_key, verify_ssl_certs=False)
        
    def analyze_text(self, text: str) -> Dict:
        """
        Анализ текста с помощью GigaChat
        
        Args:
            text (str): Текст для анализа
            
        Returns:
            Dict: Результаты анализа
        """
        prompt = create_prompt(text)
        
        try:
            # Формируем сообщения для чата
            messages = Messages(
                messages=[
                    MessagesRole(role="system", content="Ты - эксперт по анализу отзывов. Твоя задача - определить тональность отзыва, категорию объекта и группу замечаний (если есть)."),
                    MessagesRole(role="user", content=prompt)
                ]
            )
            
            # Создаем чат
            chat = Chat(messages=messages)
            
            # Отправляем запрос
            response = self.client.chat(chat)
            
            # Получаем ответ
            result_text = response.choices[0].message.content
            
            # Парсим JSON из ответа
            try:
                result = json.loads(result_text)
            except json.JSONDecodeError:
                # Если не удалось распарсить JSON, возвращаем пустой результат
                print(f"Ошибка парсинга JSON для текста: {text[:100]}...")
                return {
                    "sentiment": "нейтрально",
                    "category": "неизвестно",
                    "complaint_group": "нет"
                }
            
            return result
            
        except Exception as e:
            print(f"Ошибка при анализе текста: {str(e)}")
            # В случае ошибки возвращаем пустой результат
            return {
                "sentiment": "нейтрально",
                "category": "неизвестно",
                "complaint_group": "нет"
            }
        
        finally:
            # Добавляем задержку между запросами
            time.sleep(1)

def create_prompt(text: str) -> str:
    """
    Создание промпта для LLM
    
    Args:
        text (str): Текст отзыва
        
    Returns:
        str: Промпт для LLM
    """
    return f"""
    Проанализируй следующий отзыв и предоставь информацию в формате JSON:
    1. Тональность отзыва (значения: "плохо", "нейтрально", "хорошо")
    2. Категория объекта (значения: "больница", "школа", "поликлиника", "магазин" и т.д.)
    3. Если есть замечания, укажи их группу:
       - "персонал" (относятся к профессионализму и отношению сотрудников)
       - "оснащение" (относятся к оснащению учреждения расходниками, оборудованием)
       - "ремонт" (относятся к ремонту или отсутствию такого)
       - "организация" (относятся к организации работы учреждения, в том числе и возможностью удобной записи)
       - "нет" (если замечаний нет)
    
    Отзыв: {text}
    
    Ответ должен быть в формате JSON:
    {{
        "sentiment": "значение",
        "category": "значение",
        "complaint_group": "значение"
    }}
    """

def analyze_dataset(df: pd.DataFrame, analyzer: LLMAnalyzer) -> pd.DataFrame:
    """
    Анализ датасета с помощью LLM
    
    Args:
        df (pd.DataFrame): Датасет для анализа
        analyzer (LLMAnalyzer): Анализатор
        
    Returns:
        pd.DataFrame: Датасет с результатами анализа
    """
    results = []
    
    for idx, row in df.iterrows():
        if idx % 10 == 0:
            print(f"Обработка {idx}/{len(df)} отзывов...")
            
        text = row['review_text']
        analysis_result = analyzer.analyze_text(text)
        results.append(analysis_result)
        
        # Сохраняем промежуточные результаты каждые 100 отзывов
        if (idx + 1) % 100 == 0:
            temp_df = pd.DataFrame(results)
            temp_df.to_csv(f'llm_analysis_results_temp_{idx+1}.csv', 
                          index=False, 
                          encoding='utf-8-sig',
                          sep=',')
    
    # Преобразуем результаты в DataFrame
    results_df = pd.DataFrame(results)
    
    # Добавляем результаты к исходному датасету
    df = pd.concat([df, results_df], axis=1)
    
    return df

def main():
    # Загрузка данных
    print("Загрузка данных...")
    df = pd.read_csv('processed_data_1_20.csv', sep=',', encoding='utf-8-sig')
    
    # Создаем трекер стоимости
    cost_tracker = CostTracker()
    
    # Список доступных анализаторов
    analyzers = {
        'gpt': GPTAnalyzer(api_key=os.getenv('OPENAI_API_KEY'), cost_tracker=cost_tracker),
        'yandex': YandexGPTAnalyzer(api_key=os.getenv('YANDEX_API_KEY'), 
                                   folder_id=os.getenv('YANDEX_FOLDER_ID'),
                                   cost_tracker=cost_tracker),
        'gigachat': GigaChatAnalyzer(api_key=os.getenv('GIGACHAT_API_KEY'), 
                                    cost_tracker=cost_tracker),
        'gemini': GeminiAnalyzer(api_key=os.getenv('GEMINI_API_KEY'), 
                                cost_tracker=cost_tracker),
        'qwen': QwenAnalyzer(api_key=os.getenv('QWEN_API_KEY'), 
                            cost_tracker=cost_tracker),
        'deepseek': DeepSeekAnalyzer(api_key=os.getenv('DEEPSEEK_API_KEY'), 
                                    cost_tracker=cost_tracker)
    }
    
    # Выбираем анализатор (можно изменить на нужный)
    analyzer_name = 'gpt'
    analyzer = analyzers.get(analyzer_name)
    
    if not analyzer:
        print(f"Ошибка: Анализатор {analyzer_name} не найден")
        return
    
    # Анализ датасета
    print(f"\nАнализ датасета с помощью {analyzer_name}...")
    results_df = analyze_dataset(df, analyzer)
    
    # Сохранение результатов
    print("\nСохранение результатов...")
    results_df.to_csv(f'llm_analysis_results_{analyzer_name}.csv', 
                     index=False, 
                     encoding='utf-8-sig',
                     sep=',')
    
    # Вывод отчета о стоимости
    print("\n" + cost_tracker.get_report())
    
    print(f"\nАнализ завершен. Результаты сохранены в 'llm_analysis_results_{analyzer_name}.csv'")

if __name__ == "__main__":
    main() 