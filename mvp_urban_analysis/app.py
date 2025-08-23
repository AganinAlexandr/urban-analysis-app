"""
Основное Flask приложение для анализа данных городской среды
"""

from flask import Flask, render_template, request, jsonify, send_file, make_response
import pandas as pd
import os
from datetime import datetime
import json
import logging
from datetime import datetime
import numpy as np
import sqlite3

# Загружаем переменные окружения из файла .env
from dotenv import load_dotenv
load_dotenv('env_data.env')

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Импорт наших модулей
from app.core.data_processor import DataProcessor
from app.core.text_analyzer import TextAnalyzer
from app.core.llm_analysis import LLMAnalyzer
from app.core.geocoder import MoscowGeocoder
from app.core.database_fixed import db_manager_fixed
from app.core.data_migrator import data_migrator
from app.core.sample_manager import SampleManager

def convert_dataframe_for_json(df):
    """
    Конвертирует DataFrame для JSON сериализации
    
    Args:
        df: DataFrame для конвертации
        
    Returns:
        DataFrame с конвертированными типами данных
    """
    if df is None or df.empty:
        return df
    
    df_converted = df.copy()
    
    # Обрабатываем все колонки
    for col in df_converted.columns:
        try:
            # Проверяем тип колонки
            if df_converted[col].dtype in ['int64', 'int32', 'int16', 'int8']:
                # Целочисленные типы -> int
                df_converted[col] = df_converted[col].astype('Int64')  # pandas nullable integer
            elif df_converted[col].dtype in ['float64', 'float32', 'float16']:
                # Числа с плавающей точкой -> float
                df_converted[col] = df_converted[col].astype('float64')
            elif df_converted[col].dtype == 'bool':
                # Булевы значения -> bool
                df_converted[col] = df_converted[col].astype('bool')
            elif df_converted[col].dtype == 'object':
                # Объекты -> строки, но с обработкой None/NaN
                df_converted[col] = df_converted[col].astype('string')
            elif df_converted[col].dtype == 'datetime64[ns]':
                # Даты -> строки ISO формата
                df_converted[col] = df_converted[col].dt.strftime('%Y-%m-%d %H:%M:%S')
            else:
                # Для остальных типов -> строки
                df_converted[col] = df_converted[col].astype('string')
        except Exception as e:
            # Если не удалось конвертировать, оставляем как есть
            logger.warning(f"Не удалось конвертировать колонку {col}: {e}")
            continue
    
    return df_converted

def calculate_method_correlation(group_data, all_methods):
    """
    Рассчитывает корреляцию методов с master_rating для одной группы
    
    Args:
        group_data: список словарей с данными отзывов
                   [{'method1': value1, 'method2': value2, 'master_rating': value}, ...]
        all_methods: список методов в нужном порядке для соответствия графику
    
    Returns:
        список коэффициентов корреляции для каждого метода
    """
    if not group_data or len(group_data) < 3:
        return []
    
    # Получаем список методов (исключаем master_rating) и сортируем в том же порядке, что и all_methods
    # Это важно для соответствия порядка корреляций порядку методов на графике
    available_methods = [key for key in group_data[0].keys() if key != 'master_rating']
    
    # Сортируем методы в том же порядке, что и в all_methods (который передаем извне)
    # Для этого нужно передать all_methods в функцию
    if not available_methods:
        return []
    
    logger.info(f"🔍 calculate_method_correlation: доступные методы: {available_methods}")
    logger.info(f"🔍 calculate_method_correlation: ожидаемый порядок методов: {all_methods}")
    
    correlations = []
    
    # Обрабатываем методы в том же порядке, что и в all_methods
    for expected_method in all_methods:
        if expected_method in available_methods:
            method = expected_method
            method_values = []
            master_values = []
            
            for review in group_data:
                if method in review and 'master_rating' in review:
                    method_val = review[method]
                    master_val = review['master_rating']
                    
                    # Проверяем, что оба значения не None
                    if method_val is not None and master_val is not None:
                        method_values.append(method_val)
                        master_values.append(master_val)
            
            # Рассчитываем корреляцию только если есть достаточно данных
            if len(method_values) >= 3 and len(master_values) >= 3 and len(method_values) == len(master_values):
                try:
                    correlation = np.corrcoef(method_values, master_values)[0, 1]
                    if pd.isna(correlation):
                        correlation = 0.0
                    correlations.append(max(0.0, min(1.0, abs(correlation))))
                except Exception as e:
                    logger.error(f"calculate_method_correlation: ошибка расчета корреляции для метода {method}: {e}")
                    correlations.append(0.0)
            else:
                correlations.append(0.0)
        else:
            # Если метода нет в данных, добавляем 0.0
            logger.info(f"calculate_method_correlation: метод {expected_method} отсутствует в данных, добавляем 0.0")
            correlations.append(0.0)
    
    return correlations

def make_json_safe(obj):
    """
    Рекурсивно делает объект безопасным для JSON сериализации
    """
    import numpy as np
    import pandas as pd
    
    if obj is None:
        return None
    elif isinstance(obj, dict):
        return {str(k): make_json_safe(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [make_json_safe(x) for x in obj]
    elif isinstance(obj, tuple):
        return [make_json_safe(x) for x in obj]
    elif isinstance(obj, (np.integer, np.int64, np.int32, np.int16, np.int8)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64, np.float32, np.float16)):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, pd.Series):
        return obj.tolist()
    elif isinstance(obj, pd.DataFrame):
        return obj.to_dict('records')
    elif isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    elif isinstance(obj, (str, int, float, bool)):
        return obj
    else:
        # Для неизвестных типов пытаемся преобразовать в строку
        try:
            return str(obj)
        except:
            return None

app = Flask(__name__)
app.config['SECRET_KEY'] = 'your-secret-key-here'

# Получаем API ключ из переменных окружения
import os
geocoder_api_key = os.getenv('YANDEX_GEOCODER_API_KEY')

# Инициализация компонентов
data_processor = DataProcessor(geocoder_api_key=geocoder_api_key)
text_analyzer = TextAnalyzer()

# Инициализация LLM анализатора с API ключами
api_keys = {
    'openai': os.getenv('OPENAI_API_KEY'),
    'gemini': os.getenv('GEMINI_API_KEY'),
    'yandex': os.getenv('YANDEX_GPT_OAUTH_TOKEN'),  # Добавляем Yandex GPT
    'gigachat': os.getenv('GIGACHAT_API_KEY'),
    'qwen': os.getenv('QWEN_API_KEY'),
    'deepseek': os.getenv('DEEPSEEK_API_KEY')
}
llm_analyzer = LLMAnalyzer(api_keys=api_keys)

geocoder = MoscowGeocoder(api_key=geocoder_api_key)

# Инициализируем менеджер выборок
sample_manager = SampleManager()

@app.route('/')
def index():
    """Главная страница"""
    return render_template('index.html')

@app.route('/upload/detect-group', methods=['POST'])
def detect_group_from_upload():
    """Определение группы из загруженного файла"""
    try:
        logger.info("=== ОПРЕДЕЛЕНИЕ ГРУППЫ ИЗ ФАЙЛА ===")
        
        if 'file' not in request.files:
            logger.error("Файл не выбран")
            return jsonify({'error': 'Файл не выбран'}), 400
        
        file = request.files['file']
        if file.filename == '':
            logger.error("Файл не выбран")
            return jsonify({'error': 'Файл не выбран'}), 400
        
        logger.info(f"Загружен файл для определения группы: {file.filename}")
        
        # Проверяем формат файла
        file_ext = os.path.splitext(file.filename)[1].lower()
        if file_ext != '.json':
            return jsonify({'error': 'Для определения группы поддерживаются только JSON файлы'}), 400
        
        # Сохраняем файл временно
        temp_filename = f'detect_group_{datetime.now().strftime("%Y%m%d_%H%M%S")}{file_ext}'
        temp_path = os.path.join('data', 'temp', temp_filename)
        os.makedirs(os.path.dirname(temp_path), exist_ok=True)
        file.save(temp_path)
        
        # Читаем JSON файл
        with open(temp_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Извлекаем данные для определения группы
        if isinstance(data, list) and len(data) > 0:
            # Берем первый объект и его отзывы
            first_object = data[0]
            object_name = first_object.get('name', '')
            address = first_object.get('address', '')
            
            # Объединяем все отзывы
            reviews = []
            if 'reviews' in first_object and isinstance(first_object['reviews'], list):
                reviews = [review.get('text', '') for review in first_object['reviews']]
            
            combined_text = f"{object_name} {address} {' '.join(reviews)}"
            
            # Определяем группу
            from app.core.district_detector import detect_group_from_text
            detected_group = detect_group_from_text(combined_text)
            
            logger.info(f"Определена группа: {detected_group} для объекта: {object_name}")
            
            # Удаляем временный файл
            os.remove(temp_path)
            
            return jsonify({
                'success': True,
                'detected_group': detected_group,
                'data': data
            })
        else:
            return jsonify({
                'success': False,
                'error': 'Неверная структура JSON файла'
            })
            
    except Exception as e:
        logger.error(f"Ошибка определения группы из файла: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/upload', methods=['POST'])
def upload_file():
    """Загрузка файла для обработки"""
    try:
        print("=== ЗАГРУЗКА ФАЙЛА ===")
        print(f"Получен запрос: {request.method}")
        print(f"Форма содержит поля: {list(request.form.keys())}")
        print(f"Файлы: {list(request.files.keys())}")
        
        logger.info("=== НАЧАЛО ОБРАБОТКИ ФАЙЛА ===")
        
        if 'file' not in request.files:
            print("❌ Файл не найден в запросе")
            logger.error("Файл не выбран")
            return jsonify({'error': 'Файл не выбран'}), 400
        
        file = request.files['file']
        print(f"📁 Файл найден: {file.filename}")
        
        if file.filename == '':
            print("❌ Имя файла пустое")
            logger.error("Файл не выбран")
            return jsonify({'error': 'Файл не выбран'}), 400
        
        print(f"✅ Файл валиден: {file.filename}")
        logger.info(f"Загружен файл: {file.filename}")
        
        # Проверяем поддерживаемые форматы
        allowed_extensions = ['.csv', '.json', '.xlsx', '.xls']
        file_ext = os.path.splitext(file.filename)[1].lower()
        
        print(f"🔍 Проверяем расширение файла: {file_ext}")
        print(f"🔍 Разрешенные расширения: {allowed_extensions}")
        
        if file_ext not in allowed_extensions:
            print(f"❌ Неподдерживаемый формат файла: {file_ext}")
            logger.error(f"Неподдерживаемый формат файла: {file_ext}")
            return jsonify({'error': f'Неподдерживаемый формат файла. Поддерживаются: {", ".join(allowed_extensions)}'}), 400
        
        print(f"✅ Формат файла поддерживается: {file_ext}")
        print(f"🔍 Размер файла: {len(file.read())} байт")
        file.seek(0)  # Возвращаем указатель в начало
        logger.info(f"Формат файла: {file_ext}")
        
        # Сохраняем файл временно
        temp_filename = f'upload_{datetime.now().strftime("%Y%m%d_%H%M%S")}{file_ext}'
        temp_path = os.path.join('data', 'temp', temp_filename)
        os.makedirs(os.path.dirname(temp_path), exist_ok=True)
        file.save(temp_path)
        
        print(f"💾 Файл сохранен: {temp_path}")
        logger.info(f"Файл сохранен: {temp_path}")
        
        # Определяем тип файла
        file_type = None
        if file_ext == '.csv':
            file_type = 'csv'
        elif file_ext == '.json':
            file_type = 'json'
        elif file_ext in ['.xlsx', '.xls']:
            file_type = 'excel'
        
        print(f"🔍 Тип файла определен: {file_type}")
        print(f"🔍 Расширение файла: {file_ext}")
        logger.info(f"Тип файла определен: {file_type}")
        
        # Получаем дополнительные параметры
        sheet_name = request.form.get('sheet_name')
        filters = request.form.get('filters')
        group = request.form.get('group')  # Получаем группу из формы
        detected_group = request.form.get('detected_group')  # Получаем определенную группу из формы
        
        print(f"🔍 Sheet name: {sheet_name}")
        print(f"🔍 Группа из формы: {group}")
        print(f"🔍 Определенная группа из формы: {detected_group}")
        print(f"🔍 Все данные формы: {dict(request.form)}")
        
        if filters:
            try:
                filters = json.loads(filters)
                print(f"🔍 Применены фильтры: {filters}")
                logger.info(f"Применены фильтры: {filters}")
            except:
                filters = None
                print("⚠️ Ошибка парсинга фильтров")
                logger.warning("Ошибка парсинга фильтров")
        
        # Получаем выбранные методы анализа
        analysis_methods = request.form.get('analysis_methods', 'classical')
        print(f"🔧 Методы анализа из формы: {analysis_methods} (тип: {type(analysis_methods)})")
        logger.info(f"Получены методы анализа из формы: {analysis_methods} (тип: {type(analysis_methods)})")
        
        # Отладочная информация - выводим все данные формы
        print(f"📋 Все данные формы: {dict(request.form)}")
        logger.info(f"Все данные формы: {dict(request.form)}")
        
        if isinstance(analysis_methods, str):
            try:
                # Пытаемся распарсить JSON
                import json
                analysis_methods = json.loads(analysis_methods)
                print(f"✅ Распарсенные методы из JSON: {analysis_methods}")
                logger.info(f"Распарсенные методы из JSON: {analysis_methods}")
            except:
                # Если не JSON, то это просто строка
                analysis_methods = [analysis_methods]
                print(f"📝 Методы как список строк: {analysis_methods}")
                logger.info(f"Методы как список строк: {analysis_methods}")
        elif not analysis_methods:
            analysis_methods = ['classical']
            print(f"🔄 Используем классический метод по умолчанию: {analysis_methods}")
            logger.info(f"Используем классический метод по умолчанию: {analysis_methods}")
        
        print(f"🎯 Финальные методы анализа: {analysis_methods}")
        print(f"🔍 Указанная группа: {group}")
        logger.info(f"Финальные методы анализа: {analysis_methods}")
        logger.info(f"Указанная группа: {group}")
        
        # Обрабатываем данные
        print("🔄 Начинаем загрузку данных...")
        logger.info("Начинаем загрузку данных...")
        
        try:
            df = data_processor.load_data(temp_path, file_type, sheet_name, filters)
            print(f"✅ Данные загружены: {len(df)} строк")
            logger.info(f"Данные загружены: {len(df)} строк")
        except Exception as e:
            print(f"❌ Ошибка загрузки данных: {e}")
            logger.error(f"Ошибка загрузки данных: {e}")
            return jsonify({'error': f'Ошибка загрузки данных: {str(e)}'}), 400
        
        if df.empty:
            print("❌ DataFrame пустой")
            logger.error("Ошибка загрузки файла - DataFrame пустой")
            return jsonify({'error': 'Ошибка загрузки файла'}), 400
        
        print(f"📊 Колонки в DataFrame: {list(df.columns)}")
        
        # Применяем группы, если они указаны
        if group and 'group' in df.columns:
            print(f"🔍 Применяем группу '{group}' ко всем записям")
            logger.info(f"Применяем группу '{group}' ко всем записям")
            df['group'] = group
            print(f"✅ Группа применена. Уникальные группы в данных: {df['group'].unique()}")
            logger.info(f"Группа применена. Уникальные группы в данных: {df['group'].unique()}")
        elif group:
            print(f"⚠️ Группа '{group}' указана, но поле 'group' отсутствует в данных")
            print(f"📊 Доступные колонки: {list(df.columns)}")
        
        # Создаем поля если их нет
        if 'group' not in df.columns:
            print("➕ Создаем поле 'group'")
            df['group'] = ''
        if 'determined_group' not in df.columns:
            print("➕ Создаем поле 'determined_group'")
            df['determined_group'] = ''
        
        if detected_group:
            print(f"🔍 Применяем определенную группу '{detected_group}' ко всем записям")
            logger.info(f"Применяем определенную группу '{detected_group}' ко всем записям")
            df['determined_group'] = detected_group
            print(f"✅ Определенная группа применена. Уникальные определенные группы в данных: {df['determined_group'].unique()}")
            logger.info(f"Определенная группа применена. Уникальные определенные группы в данных: {df['determined_group'].unique()}")
        
        # Если группы не указаны, анализируем данные
        if not group and not detected_group:
            print("🔍 Группы не указаны, анализируем данные...")
            # Импортируем утилиты для работы с группами
            from app.core.group_utils import normalize_group_name, get_russian_group_name
            from app.core.data_normalizer import DataNormalizer
            
            logger.info("Анализируем поля групп в данных...")
            print("🔍 Анализируем поля групп в данных...")
            print(f"📊 Колонки в DataFrame: {list(df.columns)}")
            logger.info(f"Колонки в DataFrame: {list(df.columns)}")
            
            # Проверяем наличие полей групп
            has_group_field = 'group' in df.columns
            has_detected_group_field = 'determined_group' in df.columns
            
            print(f"🔍 Поле 'group' присутствует: {has_group_field}")
            print(f"🔍 Поле 'determined_group' присутствует: {has_detected_group_field}")
            logger.info(f"Поле 'group' присутствует: {has_group_field}")
            logger.info(f"Поле 'determined_group' присутствует: {has_detected_group_field}")
            
            # Создаем поля если их нет
            if not has_group_field:
                print("➕ Создаем поле 'group'")
                df['group'] = ''
            if not has_detected_group_field:
                print("➕ Создаем поле 'determined_group'")
                df['determined_group'] = ''
            
            # Автоматически определяем detected_group_type для всех записей
            print("🤖 Автоматически определяем группы для всех записей...")
            logger.info("Автоматически определяем группы для всех записей...")
            
            # Анализируем весь JSON как единый текст для определения основной группы
            print("🔍 Анализируем весь JSON как единый текст для определения группы...")
            
            # Собираем весь текст: названия объектов + все отзывы
            all_object_names = df['name'].dropna().astype(str).tolist()
            all_reviews = df['review_text'].dropna().astype(str).tolist()
            
            # Объединяем в единый текст для анализа
            combined_text = ' '.join(all_object_names + all_reviews)
            print(f"📝 Объединенный текст для анализа: {len(combined_text)} символов")
            if len(combined_text) > 200:
                print(f"📝 Начало текста: {combined_text[:200]}...")
            else:
                print(f"📝 Полный текст: {combined_text}")
            
            # Определяем группу на основе всего текста
            try:
                from app.core.district_detector import detect_group_from_text
                
                # Отладочная информация
                print(f"🔍 ДИАГНОСТИКА: Длина объединенного текста: {len(combined_text)}")
                print(f"🔍 ДИАГНОСТИКА: Первые 300 символов: {combined_text[:300]}")
                print(f"🔍 ДИАГНОСТИКА: Содержит 'аптека': {'аптека' in combined_text.lower()}")
                print(f"🔍 ДИАГНОСТИКА: Содержит 'фармацевт': {'фармацевт' in combined_text.lower()}")
                print(f"🔍 ДИАГНОСТИКА: Содержит 'лекарство': {'лекарство' in combined_text.lower()}")
                
                main_detected_group = detect_group_from_text(combined_text)
                print(f"🎯 Определена основная группа для всего JSON: '{main_detected_group}'")
                logger.info(f"Определена основная группа для всего JSON: '{main_detected_group}'")
                
                # Присваиваем эту группу всем записям
                df['determined_group'] = main_detected_group['main_group']
                print(f"✅ Присвоена группа '{main_detected_group['main_group']}' всем {len(df)} записям")
                
                # Сохраняем дополнительную информацию о группах для модального окна
                group_detection_info = main_detected_group
            except Exception as e:
                print(f"⚠️ Не удалось определить группу для всего JSON: {e}")
                logger.warning(f"Не удалось определить группу для всего JSON: {e}")
                # В случае ошибки используем старую логику как fallback
                print("🔄 Используем fallback - определяем группы для каждой записи отдельно...")
                for idx, row in df.iterrows():
                    object_name = str(row.get('name', ''))
                    review_text = str(row.get('review_text', ''))
                    try:
                        from initial_keywords_system import detect_group_by_initial_keywords
                        detected_group, confidence = detect_group_by_initial_keywords(object_name, review_text)
                        df.at[idx, 'determined_group'] = detected_group
                    except Exception as e2:
                        print(f"⚠️ Fallback также не сработал для записи {idx}: {e2}")
                        df.at[idx, 'determined_group'] = 'unknown'
            
            # Анализируем, какие поля пустые
            print("🔍 Анализируем пустые поля групп...")
            empty_group = df['group'].isna() | (df['group'] == '') | (df['group'].astype(str).str.strip() == '')
            empty_detected_group = df['determined_group'].isna() | (df['determined_group'] == '') | (df['determined_group'].astype(str).str.strip() == '')
            
            empty_group_count = empty_group.sum()
            empty_detected_group_count = empty_detected_group.sum()
            
            print(f"📊 Записей с пустым 'group': {empty_group_count}")
            print(f"📊 Записей с пустым 'determined_group': {empty_detected_group_count}")
            logger.info(f"Записей с пустым 'group': {empty_group_count}")
            logger.info(f"Записей с пустым 'determined_group': {empty_detected_group_count}")
            
            # Определяем, нужно ли показывать модальное окно
            needs_modal = False
            modal_reason = ""
            
            if empty_group_count > 0 and empty_detected_group_count > 0:
                # Оба поля пустые - показываем модальное окно
                needs_modal = True
                modal_reason = "both_empty"
                print("🔄 Оба поля групп пустые - показываем модальное окно")
                logger.info("Оба поля групп пустые - показываем модальное окно")
            elif empty_group_count > 0 and empty_detected_group_count == 0:
                # Только object_group пустое - показываем модальное окно
                needs_modal = True
                modal_reason = "group_empty"
                print("🔄 Только поле 'group' пустое - показываем модальное окно")
                logger.info("Только поле 'group' пустое - показываем модальное окно")
            elif empty_group_count == 0 and empty_detected_group_count > 0:
                # Только detected_group пустое - определяем автоматически без модального окна
                print("✅ Только поле 'determined_group' пустое - определяем автоматически")
                logger.info("Только поле 'determined_group' пустое - определяем автоматически")
                
                # Заполняем пустые detected_group на основе group
                for idx in df[empty_detected_group].index:
                    group_value = df.at[idx, 'group']
                    if group_value and str(group_value).strip():
                        # Нормализуем название группы
                        normalized_group = normalize_group_name(group_value)
                        df.at[idx, 'determined_group'] = normalized_group
                        print(f"🔍 Заполнено detected_group для записи {idx}: '{group_value}' -> '{normalized_group}'")
                        logger.info(f"Заполнено detected_group для записи {idx}: '{group_value}' -> '{normalized_group}'")
                
                needs_modal = False
            else:
                # Все поля заполнены
                print("✅ Все поля групп заполнены")
                logger.info("Все поля групп заполнены")
                needs_modal = False
            
            if needs_modal:
                print("🔄 Показываем модальное окно для выбора групп")
                logger.info("Показываем модальное окно для выбора групп")
                
                # Подготавливаем данные для отображения
                display_columns = ['name', 'address', 'review_text', 'group', 'determined_group']
                available_columns = [col for col in display_columns if col in df.columns]
                
                # Берем первые 10 записей для отображения
                display_df = df[available_columns].head(10)
                display_data = display_df.to_dict('records')
                
                # Получаем статистику по определенным группам
                detected_groups_stats = df['determined_group'].value_counts().to_dict()
                
                # Добавляем информацию о группах с одинаковым рейтингом
                group_info = {}
                if 'group_detection_info' in locals():
                    group_info = {
                        'tied_groups': group_detection_info.get('tied_groups', []),
                        'all_groups_scores': group_detection_info.get('all_groups', {}),
                        'detection_note': f"Найдено {len(group_detection_info.get('tied_groups', []))} групп с одинаковым рейтингом" if group_detection_info.get('tied_groups') else "Одна группа с максимальным рейтингом"
                    }
                
                print(f"📊 Возвращаем ошибку group_required для {len(df)} записей")
                print(f"🔍 Статистика групп: {detected_groups_stats}")
                if group_info.get('tied_groups'):
                    print(f"🎯 Группы с одинаковым рейтингом: {group_info['tied_groups']}")
                
                return jsonify(make_json_safe({
                    'error': 'group_required',
                    'message': f'Требуется подтверждение групп для {len(df)} записей.',
                    'total_records': len(df),
                    'empty_group_records': empty_group_count,
                    'empty_detected_group_records': empty_detected_group_count,
                    'modal_reason': modal_reason,
                    'detected_groups_stats': detected_groups_stats,
                    'group_detection_info': group_info,
                    'data': display_data
                })), 400
            else:
                print("✅ Все группы определены автоматически, продолжаем обработку")
                logger.info("Все группы определены автоматически, продолжаем обработку")
        
        # Валидируем данные
        print("🔍 Начинаем валидацию данных...")
        logger.info("Начинаем валидацию данных...")
        valid_df, addressless_df = data_processor.validate_data(df)
        print(f"✅ Валидация завершена: валидных записей {len(valid_df)}, без адреса {len(addressless_df)}")
        logger.info(f"Валидация завершена: валидных записей {len(valid_df)}, без адреса {len(addressless_df)}")
        
        # Анализируем текст с выбранными методами
        if not valid_df.empty:
            print("🧠 Начинаем анализ текста...")
            logger.info("Начинаем анализ текста...")
            # Используем LLM анализатор для множественных методов
            analyzed_df = llm_analyzer.analyze_dataframe(valid_df, methods=analysis_methods)
            print(f"✅ Анализ завершен: {len(analyzed_df)} записей")
            logger.info(f"Анализ завершен: {len(analyzed_df)} записей")
            
            # Получаем информацию о методах
            available_methods = llm_analyzer.available_methods
            used_methods = [m for m in analysis_methods if m in available_methods]
            if not used_methods:
                used_methods = ['classical']
            
            print(f"🔧 Используемые методы: {used_methods}")
            logger.info(f"Используемые методы: {used_methods}")
            
            # Сравниваем результаты методов
            comparison = llm_analyzer.compare_methods(analyzed_df, methods=used_methods)
            print("✅ Сравнение методов завершено")
            logger.info("Сравнение методов завершено")
            
            # Определяем основной метод для отображения
            primary_method = used_methods[0]
            analysis_method = f"Анализ с использованием методов: {', '.join(used_methods)}"
        
            # Проверяем и получаем координаты
            print("📍 Проверяем координаты...")
            logger.info("Проверяем координаты...")
            coordinates_status = geocoder.get_coordinates_status(analyzed_df)
            if not coordinates_status['coordinates_exist']:
                print("🌍 Выполняется геокодирование адресов...")
                logger.info("Выполняется геокодирование адресов...")
                analyzed_df = geocoder.process_dataframe(analyzed_df)
            else:
                print("✅ Координаты уже присутствуют в данных")
                logger.info("Координаты уже присутствуют в данных")
            
            # Обрабатываем районы
            print("🏘️ Обрабатываем районы...")
            logger.info("Обрабатываем районы...")
            analyzed_df = data_processor.process_districts(analyzed_df)
            
            # Сохраняем в БД
            print("💾 Сохраняем в БД...")
            logger.info("Сохраняем в БД...")
            try:
                # Импортируем db_manager_fixed
                from app.core.database_fixed import db_manager_fixed
                
                # Сохраняем данные в БД
                print(f"📊 Сохраняем {len(analyzed_df)} записей в БД...")
                success = db_manager_fixed.migrate_csv_to_database(analyzed_df, source="upload")
                print(f"✅ Сохранение в БД: {'успешно' if success else 'ошибка'}")
                logger.info(f"Сохранение в БД: {'успешно' if success else 'ошибка'}")
                
                # Сохраняем данные в app.archive_data для возможности миграции
                app.archive_data = analyzed_df
                print(f"💾 Данные сохранены в app.archive_data: {len(analyzed_df)} записей")
                logger.info(f"Данные сохранены в app.archive_data: {len(analyzed_df)} записей")
                logger.info(f"Тип app.archive_data: {type(app.archive_data)}")
                logger.info(f"Колонки в app.archive_data: {list(app.archive_data.columns)}")
                
            except Exception as e:
                print(f"❌ Ошибка сохранения в БД: {e}")
                logger.error(f"Ошибка сохранения в БД: {e}")
                success = False
            
            # Подготавливаем результаты для отображения
            print("📋 Подготавливаем результаты для отображения...")
            display_columns = ['group', 'name', 'determined_group', 'address', 'review_text', 'rating']
            # Добавляем колонки для каждого метода анализа
            for method in used_methods:
                display_columns.extend([f'{method}_sentiment', f'{method}_sentiment_score', f'{method}_review_type'])
            
            available_columns = [col for col in display_columns if col in analyzed_df.columns]
            print(f"🔍 Колонки для отображения: {available_columns}")
            logger.info(f"Колонки для отображения: {available_columns}")
            
            # Берем первые 10 записей для отображения
            display_df = convert_dataframe_for_json(analyzed_df[available_columns].head(10))
            display_data = display_df.to_dict('records')
            print(f"📊 Подготовлено {len(display_data)} записей для отображения")
            logger.info(f"Подготовлено {len(display_data)} записей для отображения")
            
            logger.info("=== ЗАВЕРШЕНИЕ ОБРАБОТКИ ФАЙЛА ===")
            print("�� Обработка файла завершена успешно!")
            print(f"📊 Обработано записей: {len(analyzed_df)}")
            print(f"💾 Сохранено в БД: {'Да' if success else 'Нет'}")
            
            return jsonify(make_json_safe({
                'success': True,
                'message': f'Обработано {len(analyzed_df)} записей',
                'valid_records': len(valid_df),
                'addressless_records': len(addressless_df),
                'coordinates_processed': not coordinates_status['coordinates_exist'],
                'districts_processed': True,
                'saved_to_archive': success,
                'file_type': file_type,
                'analysis_results': {
                    'data': display_data,
                    'columns': available_columns,
                    'total_records': len(analyzed_df),
                    'analysis_method': analysis_method,
                    'methods_used': used_methods,
                    'method_comparison': comparison,
                    'sentiment_stats': {},
                    'review_types': {}
                }
            }))
        else:
            print("❌ Нет валидных записей для обработки")
            logger.error("Нет валидных записей для обработки")
            return jsonify({'error': 'Нет валидных записей для обработки'}), 400
            
    except Exception as e:
        print(f"💥 КРИТИЧЕСКАЯ ОШИБКА: {str(e)}")
        logger.error(f"Ошибка обработки: {str(e)}")
        import traceback
        print(f"🔍 Полный traceback:")
        print(traceback.format_exc())
        logger.error(f"Полный traceback: {traceback.format_exc()}")
        return jsonify({'error': f'Ошибка обработки: {str(e)}'}), 500

@app.route('/archive/info')
def get_archive_info():
    """Получение информации о данных в БД"""
    try:
        # Получаем данные из БД
        df = db_manager_fixed.export_to_dataframe(include_analysis=True)
        
        if df.empty:
            return jsonify({
                'total_records': 0,
                'groups': {},
                'determined_groups': {},
                'date_range': {'min': None, 'max': None},
                'field_completeness': {}
            })
        
        # Статистика по группам от поставщика
        groups = {}
        if 'group_type' in df.columns:
            group_counts = df['group_type'].value_counts()
            groups = {group: int(count) for group, count in group_counts.items() if pd.notna(group) and group != ''}
        
        # Статистика по определенным группам
        determined_groups = {}
        if 'detected_group_type' in df.columns:
            determined_group_counts = df['detected_group_type'].value_counts()
            determined_groups = {group: int(count) for group, count in determined_group_counts.items() if pd.notna(group) and group != ''}
        
        # Диапазон дат
        date_range = {'min': None, 'max': None}
        if 'review_date' in df.columns:
            valid_dates = pd.to_datetime(df['review_date'], errors='coerce')
            valid_dates = valid_dates.dropna()
            if len(valid_dates) > 0:
                date_range['min'] = valid_dates.min().strftime('%Y-%m-%d')
                date_range['max'] = valid_dates.max().strftime('%Y-%m-%d')
        
        # Заполненность полей
        field_completeness = {}
        important_fields = ['name', 'address', 'review_text', 'review_date', 'rating']
        
        for field in important_fields:
            if field in df.columns:
                non_empty = df[field].notna() & (df[field] != '')
                completeness = int((non_empty.sum() / len(df)) * 100) if len(df) > 0 else 0
                field_completeness[field] = completeness
        
        return jsonify(make_json_safe({
            'total_records': len(df),
            'groups': groups,  # Группы от поставщика
            'determined_groups': determined_groups,  # Определенные группы
            'date_range': date_range,
            'field_completeness': field_completeness
        }))
        
    except Exception as e:
        logger.error(f"Ошибка получения информации о данных: {e}")
        return jsonify(make_json_safe({'error': str(e)})), 500

@app.route('/archive/clear', methods=['POST'])
def clear_archive():
    """Очистка данных из БД"""
    try:
        # Очищаем все данные из БД
        with db_manager_fixed.get_connection() as conn:
            conn.execute("DELETE FROM analysis_results")
            conn.execute("DELETE FROM reviews")
            conn.execute("DELETE FROM objects")
            conn.commit()
        
        return jsonify({'success': True, 'message': 'Данные из БД очищены'})
    except Exception as e:
        return jsonify({'error': f'Ошибка очистки БД: {str(e)}'}), 500

@app.route('/archive/download')
def download_archive():
    """Скачивание данных из БД"""
    try:
        # Экспортируем данные из БД
        df = db_manager_fixed.export_to_dataframe(include_analysis=True)
        
        if df.empty:
            return jsonify({'error': 'Данные в БД отсутствуют'}), 404
        
        # Создаем временный файл
        temp_file = 'temp_export.csv'
        df.to_csv(temp_file, index=False, encoding='utf-8-sig')
        
        # Отправляем файл
        response = send_file(temp_file, as_attachment=True, download_name='database_export.csv')
        
        # Удаляем временный файл после отправки
        import atexit
        atexit.register(lambda: os.remove(temp_file) if os.path.exists(temp_file) else None)
        
        return response
    except Exception as e:
        return jsonify({'error': f'Ошибка экспорта: {str(e)}'}), 500

@app.route('/data/sample')
def get_sample_data():
    """Получение образца данных для демонстрации"""
    try:
        # Создаем тестовые данные
        sample_data = {
            'group': ['school', 'hospital', 'pharmacy'],
            'name': ['Школа №1', 'Больница №2', 'Аптека №3'],
            'address': ['ул. Ленина, 1', 'ул. Пушкина, 10', 'ул. Гагарина, 5'],
            'review_text': [
                'Отличная школа, спасибо учителям!',
                'Плохое обслуживание, долгие очереди.',
                'Удобно расположена, хороший ассортимент.'
            ],
            'date': ['2024-01-15', '2024-01-16', '2024-01-17']
        }
        
        df = pd.DataFrame(sample_data)
        df_converted = convert_dataframe_for_json(df)
        
        return jsonify(make_json_safe({
            'success': True,
            'data': df_converted.to_dict('records'),
            'columns': df_converted.columns.tolist()
        }))
    except Exception as e:
        return jsonify({'error': f'Ошибка получения данных: {str(e)}'}), 500

@app.route('/file/info', methods=['POST'])
def get_file_info():
    """Получение информации о файле"""
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'Файл не выбран'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'Файл не выбран'}), 400
        
        # Сохраняем файл временно для анализа
        temp_filename = f'info_{datetime.now().strftime("%Y%m%d_%H%M%S")}{os.path.splitext(file.filename)[1]}'
        temp_path = os.path.join('data', 'temp', temp_filename)
        os.makedirs(os.path.dirname(temp_path), exist_ok=True)
        file.save(temp_path)
        
        file_ext = os.path.splitext(file.filename)[1].lower()
        
        try:
            if file_ext == '.csv':
                # Информация о CSV файле
                df_sample = data_processor.csv_processor._try_read_csv(temp_path)
                if not df_sample.empty:
                    info = {
                        'file_type': 'csv',
                        'columns': df_sample.columns.tolist(),
                        'sample_rows': len(df_sample),
                        'supported_fields': [col for col in df_sample.columns if col in data_processor.csv_processor.supported_fields]
                    }
                else:
                    info = {'file_type': 'csv', 'error': 'Не удалось прочитать CSV файл'}
                    
            elif file_ext == '.json':
                # Информация о JSON файле
                if os.path.isfile(temp_path):
                    # Для JSON файлов просто возвращаем базовую информацию
                    info = {
                        'file_type': 'json',
                        'file_size': os.path.getsize(temp_path),
                        'description': 'JSON файл с отзывами (поддерживается структура с company_info и company_reviews)'
                    }
                else:
                    info = {'file_type': 'json', 'error': 'Не удалось прочитать JSON файл'}
                    
            elif file_ext in ['.xlsx', '.xls']:
                # Информация об Excel файле
                info = data_processor.excel_processor.get_excel_info(temp_path)
                info['file_type'] = 'excel'
                
                # Получаем доступные фильтры
                filters = data_processor.excel_processor.get_available_filters(temp_path)
                info['available_filters'] = filters
                
            else:
                info = {'error': f'Неподдерживаемый тип файла: {file_ext}'}
            
            # Удаляем временный файл
            if os.path.exists(temp_path):
                os.remove(temp_path)
            
            return jsonify(make_json_safe({'success': True, 'info': info}))
            
        except Exception as e:
            # Удаляем временный файл в случае ошибки
            if os.path.exists(temp_path):
                os.remove(temp_path)
            return jsonify({'error': f'Ошибка получения информации о файле: {str(e)}'}), 500
            
    except Exception as e:
        return jsonify({'error': f'Ошибка получения информации о файле: {str(e)}'}), 500

@app.route('/analysis/methods')
def get_analysis_methods():
    """Получение списка доступных методов анализа"""
    try:
        methods = llm_analyzer.available_methods
        method_descriptions = {
            'classical': 'Классический анализ (NLTK + VADER)',
            'openai_gpt': 'OpenAI GPT (требует API ключ)',
            'google_gemini': 'Google Gemini (требует API ключ)',
            'yandex_gpt': 'YandexGPT (требует API ключ)',
            'gigachat': 'GigaChat (требует API ключ)',
            'qwen_turbo': 'Qwen Turbo (требует API ключ)',
            'deepseek_chat': 'DeepSeek Chat (требует API ключ)'
        }
        
        available_methods = []
        for method in methods:
            available_methods.append({
                'id': method,
                'name': method_descriptions.get(method, method),
                'available': True
            })
        
        return jsonify(make_json_safe({
            'success': True,
            'methods': available_methods,
            'default_method': 'classical'
        }))
    except Exception as e:
        return jsonify({'error': f'Ошибка получения методов: {str(e)}'}), 500

@app.route('/csv/fields')
def get_csv_fields_info():
    """Получение информации о поддерживаемых полях CSV"""
    try:
        return jsonify(make_json_safe({
            'success': True,
            'required_fields_processing': ['group', 'review_text'],
            'required_fields_archive': ['group', 'name', 'address', 'review_text', 'date'],
            'optional_fields': ['rating', 'user_name', 'answer_text', 'latitude', 'longitude']
        }))
    except Exception as e:
        return jsonify({'error': f'Ошибка получения информации о полях: {str(e)}'}), 500

@app.route('/database/stats')
def get_database_stats():
    """Получение статистики базы данных"""
    try:
        stats = db_manager_fixed.get_statistics()
        return jsonify({
            'success': True,
            'stats': stats
        })
    except Exception as e:
        logger.error(f"Ошибка при получении статистики БД: {e}")
        return jsonify(make_json_safe({'error': str(e)})), 500

@app.route('/database/migrate', methods=['POST'])
def migrate_to_database():
    """Миграция данных в базу данных"""
    try:
        data_type = request.form.get('data_type', 'current')  # current, archive, file
        
        if data_type == 'current':
            # Мигрируем текущие данные
            if hasattr(app, 'current_data') and app.current_data is not None:
                stats = db_manager_fixed.migrate_csv_to_database(app.current_data, 'current_data')
                return jsonify({
                    'success': True,
                    'message': 'Данные успешно мигрированы в базу данных',
                    'stats': stats
                })
            else:
                return jsonify({
                    'error': 'Нет текущих данных для миграции. Загрузите файл для обработки.',
                    'suggestion': 'Используйте загрузку файла для создания новых данных'
                }), 400
                
        elif data_type == 'archive':
            # Мигрируем архивные данные
            logger.info(f"Проверка app.archive_data: hasattr={hasattr(app, 'archive_data')}, value={getattr(app, 'archive_data', None)}")
            if hasattr(app, 'archive_data') and app.archive_data is not None:
                logger.info(f"Начинаем миграцию архивных данных: {len(app.archive_data)} записей")
                stats = db_manager_fixed.migrate_csv_to_database(app.archive_data, 'archive_data')
                logger.info(f"Миграция завершена, статистика: {stats}")
                return jsonify({
                    'success': True,
                    'message': 'Архивные данные успешно мигрированы в базу данных',
                    'stats': stats
                })
            else:
                logger.warning("app.archive_data не найден или пуст")
                return jsonify({
                    'error': 'Нет архивных данных для миграции. Загрузите файл для обработки.',
                    'suggestion': 'Используйте загрузку файла для создания новых данных'
                }), 400
        
        else:
            return jsonify({'error': 'Неизвестный тип данных'}), 400
            
    except Exception as e:
        logger.error(f"Ошибка при миграции в БД: {e}")
        return jsonify(make_json_safe({'error': str(e)})), 500

@app.route('/database/export', methods=['POST'])
def export_from_database():
    """Экспорт данных из базы данных"""
    try:
        include_analysis = request.form.get('include_analysis', 'true').lower() == 'true'
        
        # Экспортируем данные из БД
        df = db_manager_fixed.export_to_dataframe(include_analysis)
        
        # Сохраняем во временный файл
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"database_export_{timestamp}.csv"
        filepath = os.path.join('data', 'temp', filename)
        
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        df.to_csv(filepath, index=False, encoding='utf-8')
        
        return jsonify({
            'success': True,
            'message': 'Данные успешно экспортированы из базы данных',
            'filename': filename,
            'records_count': len(df)
        })
        
    except Exception as e:
        logger.error(f"Ошибка при экспорте из БД: {e}")
        return jsonify(make_json_safe({'error': str(e)})), 500

@app.route('/database/validate')
def validate_database():
    """Валидация базы данных"""
    try:
        validation_results = data_migrator.validate_migration()
        return jsonify({
            'success': True,
            'validation': validation_results
        })
    except Exception as e:
        logger.error(f"Ошибка при валидации БД: {e}")
        return jsonify(make_json_safe({'error': str(e)})), 500

@app.route('/database/sentiment_distribution')
def get_sentiment_distribution():
    """Получение распределения сентиментов из базы данных"""
    try:
        method_name = request.args.get('method')
        df = db_manager_fixed.get_sentiment_distribution(method_name)
        
        return jsonify(make_json_safe({
            'success': True,
            'data': df.to_dict('records')
        }))
    except Exception as e:
        logger.error(f"Ошибка при получении распределения сентиментов: {e}")
        return jsonify(make_json_safe({'error': str(e)})), 500

@app.route('/map/data')
def get_map_data():
    """Получение данных для отображения на карте"""
    print("TEST ")
    try:
        # Получаем параметры
        group_type = request.args.get('group_type', 'supplier')  # По умолчанию группы от поставщика
        filters = request.args.get('filters', '')  # Фильтры групп
        sentiment_method = request.args.get('sentiment_method', 'rating')  # Метод сентимента
        color_scheme = request.args.get('color_scheme', 'group')  # Схема цветов
        data_source = request.args.get('data_source', 'database')  # Источник данных

        # Парсим фильтры
        active_filters = [f for f in filters.split(',') if f] if filters else []
        
        # Дополнительная диагностика фильтров
        print(f"🔍 ДИАГНОСТИКА ФИЛЬТРОВ:")
        print(f"🔍 Параметр filters: '{filters}' (тип: {type(filters)})")
        print(f"🔍 active_filters: {active_filters} (тип: {type(active_filters)}, длина: {len(active_filters)})")
        print(f"🔍 active_filters and len(active_filters) > 0: {bool(active_filters and len(active_filters) > 0)}")
        print(f"🔍 bool(active_filters): {bool(active_filters)}")
        print(f"🔍 len(active_filters): {len(active_filters)}")

        # Импортируем утилиты для работы с группами
        from app.core.group_utils import get_russian_group_name

        # Определяем источник данных
        if data_source == 'sample':
            df = sample_manager.download_sample()
            if df is None or df.empty:
                return jsonify({
                    'archive': [],
                    'new': [],
                    'message': 'Выборка пуста. Создайте выборку на основе текущих фильтров.'
                })
        else:
            df = db_manager_fixed.export_to_dataframe(include_analysis=True)
            if df.empty:
                return jsonify({'archive': [], 'new': []})

        df_converted = convert_dataframe_for_json(df)

        # Диагностика: выводим уникальные значения групп
        print("=== ДИАГНОСТИКА ДАННЫХ ИЗ БД ===")
        print("Уникальные group_type:", df_converted['group_type'].unique() if 'group_type' in df_converted.columns else 'нет поля')
        print("Уникальные detected_group_type:", df_converted['detected_group_type'].unique() if 'detected_group_type' in df_converted.columns else 'нет поля')
        print("Уникальные group_name:", df_converted['group_name'].unique() if 'group_name' in df_converted.columns else 'нет поля')
        print("Всего строк в данных:", len(df_converted))
        print("Колонки в данных:", list(df_converted.columns))
        
        # Дополнительная диагностика: проверяем первые несколько строк
        if not df_converted.empty:
            print("\n=== ПЕРВЫЕ 3 СТРОКИ ДАННЫХ ===")
            for i in range(min(3, len(df_converted))):
                row = df_converted.iloc[i]
                print(f"Строка {i}: object_id={row.get('object_id')}, name={row.get('name')}, group_name={row.get('group_name')}, group={row.get('group')}")

        # Выбираем поле группировки
        if group_type == 'supplier':
            group_field = 'group_name'  # Используем group_name для английских названий
        else:
            group_field = 'detected_group_type'  # Используем detected_group_type для русских названий
            
        if group_field not in df_converted.columns:
            # fallback: берем первое поле, содержащее 'group' в названии
            group_fields = [col for col in df_converted.columns if 'group' in col.lower()]
            group_field = group_fields[0] if group_fields else None
            if not group_field:
                return jsonify({'archive': [], 'new': []})
        
        print(f"🎯 Поле группировки: {group_field}")
        print(f"🎯 Значения в поле {group_field}: {df_converted[group_field].unique() if group_field in df_converted.columns else 'нет поля'}")

        # Фильтруем только объекты с валидными координатами
        coords_df = df_converted[
            df_converted['latitude'].notna() & df_converted['longitude'].notna() &
            (df_converted['latitude'] != '') & (df_converted['longitude'] != '') &
            (df_converted['latitude'] != 0) & (df_converted['longitude'] != 0)
        ]
        
        # Удаляем дубликаты по координатам, оставляя первую запись
        coords_df = coords_df.drop_duplicates(subset=['latitude', 'longitude'], keep='first')
        
        print(f"Объектов после удаления дубликатов координат: {len(coords_df)}")
        print(f"Уникальные координаты: {coords_df[['latitude', 'longitude']].drop_duplicates().shape[0]}")

        # Разделяем на объекты с группой и без группы
        has_group = coords_df[group_field].notna() & (coords_df[group_field] != '') & (coords_df[group_field] != 'None')
        with_group = coords_df[has_group]
        without_group = coords_df[~has_group]

        archive_data = []
        
        # Проверяем, есть ли активные фильтры
        if active_filters and len(active_filters) > 0:
            # Если фильтры заданы — показываем только объекты этих групп
            # Но сначала проверяем, какие из активных фильтров реально есть в данных
            
            if group_type == 'determined':
                # Для режима 'determined' преобразуем английские фильтры в русские названия
                mapped_filters = [get_russian_group_name(f) for f in active_filters]
                print(f"🔍 Фильтрация (determined): английские фильтры {active_filters} -> русские {mapped_filters}")
                available_groups = with_group[group_field].unique()
                valid_filters = [f for f in mapped_filters if f in available_groups]
            else:
                # Для режима 'supplier' используем фильтры как есть
                available_groups = with_group[group_field].unique()
                valid_filters = [f for f in active_filters if f in available_groups]
            
            print(f"🔍 Фильтрация: доступные группы {available_groups}, валидные фильтры {valid_filters}")
            
            if valid_filters:
                # Показываем только объекты из валидных фильтров
                filtered = with_group[with_group[group_field].isin(valid_filters)]
                print(f"✅ Применяем фильтры: {valid_filters}, получаем {len(filtered)} объектов")
                
                for group, group_data in filtered.groupby(group_field):
                    points = []
                    for _, row in group_data.iterrows():
                        point_color = get_point_color(row, color_scheme, sentiment_method, group_type)
                        points.append({
                            'name': row.get('object_name', row.get('name', '')),
                            'address': row.get('object_address', row.get('address', '')),
                            'latitude': float(row.get('latitude', 0)),
                            'longitude': float(row.get('longitude', 0)),
                            'district': row.get('district', 'Неизвестный район'),
                            'group': row.get('group_name', row.get('group', '')),  # Используем group_name для английских названий
                            'determined_group': row.get('detected_group_type', row.get('determined_group', '')),
                            'color': point_color,
                            'sentiment': get_sentiment_value(row, sentiment_method)
                        })
                    if points:
                        archive_data.append({'group': group, 'points': points})
                        print(f"📍 Добавлена группа '{group}' с {len(points)} точками")
            else:
                # Если ни один из активных фильтров не найден в данных, показываем пустой список
                print("⚠️ Ни один из активных фильтров не найден в данных, показываем пустой список")
                archive_data = []
        else:
            # Если фильтры не заданы — показываем пустой список (вместо всех объектов)
            print("🔓 Фильтры не заданы, показываем пустой список")
            archive_data = []

        # Диагностика: выводим количество объектов
        total_points = sum(len(group['points']) for group in archive_data)
        print(f"API карты: возвращаем {len(archive_data)} групп, всего {total_points} точек")
        
        # Дополнительная диагностика
        print(f"=== ДИАГНОСТИКА API КАРТЫ ===")
        print(f"Всего объектов в БД: {len(df_converted)}")
        print(f"Объектов с координатами: {len(coords_df)}")
        print(f"Объектов с группами: {len(with_group)}")
        print(f"Активные фильтры: {active_filters}")
        print(f"Поле группировки: {group_field}")
        print(f"Уникальные группы в данных: {with_group[group_field].unique() if not with_group.empty else 'нет данных'}")
        
        if active_filters:
            print(f"Фильтрованные группы: {[f for f in active_filters if f in with_group[group_field].values]}")
            print(f"Отсутствующие в фильтрах: {[f for f in with_group[group_field].unique() if f not in active_filters]}")
        
        # Показываем детали по каждой группе
        for group_name in with_group[group_field].unique():
            group_count = len(with_group[with_group[group_field] == group_name])
            print(f"Группа '{group_name}': {group_count} объектов")
            if group_name in active_filters:
                print(f"  ✓ В активных фильтрах")
            else:
                print(f"  ✗ НЕ в активных фильтрах")
        
        # Дополнительная диагностика фильтрации
        print(f"\n=== ДИАГНОСТИКА ФИЛЬТРАЦИИ ===")
        if active_filters:
            print(f"Применяем фильтры: {active_filters}")
            available_groups = with_group[group_field].unique()
            valid_filters = [f for f in active_filters if f in available_groups]
            print(f"Доступные группы в данных: {available_groups}")
            print(f"Валидные фильтры: {valid_filters}")
            print(f"Невалидные фильтры: {[f for f in active_filters if f not in available_groups]}")
            
            if valid_filters:
                filtered_groups = with_group[with_group[group_field].isin(valid_filters)]
                print(f"Групп после фильтрации: {filtered_groups[group_field].unique()}")
                for group in valid_filters:
                    count = len(with_group[with_group[group_field] == group])
                    print(f"  {group}: {count} объектов ✓")
            else:
                print("⚠️ Ни один из активных фильтров не найден в данных!")
        else:
            print("Фильтры не заданы, показываем пустой список")
        
        return jsonify({
            'archive': archive_data,
            'new': [],
            'group_type': group_type,
            'color_scheme': color_scheme,
            'sentiment_method': sentiment_method
        })

    except Exception as e:
        logger.error(f"Ошибка при получении данных карты: {e}")
        return jsonify(make_json_safe({'error': str(e)})), 500

def get_point_color(row, color_scheme, sentiment_method, group_type='supplier'):
    """Определяет цвет точки на карте"""
    from app.core.config import SENTIMENT_CONFIG, GROUP_CONFIG
    from app.core.group_utils import get_english_group_name
    
    # В режиме "Определенные" всегда используем цвет по группе
    if group_type == 'determined':
        group = row.get('detected_group_type', row.get('determined_group', ''))
        # Маппим русские названия на английские для получения цвета
        group_key = get_english_group_name(group) or group
        return GROUP_CONFIG['colors'].get(group_key, '#6c757d')
    
    # В режиме "От поставщика" используем выбранную схему цветов
    if color_scheme == 'sentiment':
        # Цвет по сентименту
        sentiment = get_sentiment_value(row, sentiment_method)
        if sentiment == 'нет данных':
            return '#6c757d'  # Серый цвет для объектов без отзывов
        return SENTIMENT_CONFIG['colors'].get(sentiment, '#6c757d')
    else:
        # Цвет по группе
        group_key = row.get('group_name', row.get('group', ''))
        return GROUP_CONFIG['colors'].get(group_key, '#6c757d')

def get_sentiment_value(row, sentiment_method):
    """Получает значение сентимента для строки"""
    import logging
    logger = logging.getLogger(__name__)
    
    # Проверяем, есть ли у объекта отзывы
    review_id = row.get('review_id')
    if not review_id or pd.isna(review_id):
        return 'нет данных'  # Объект без отзывов
    
    if sentiment_method == 'user_rating':
        # Используем преобразованный рейтинг
        rating = row.get('rating')
        logger.info(f"user_rating: rating={rating}, sentiment_method={sentiment_method}")
        if rating and pd.notna(rating):
            from app.core.config import SENTIMENT_CONFIG
            result = SENTIMENT_CONFIG['rating_to_sentiment'].get(int(rating), 'удовлетворительно')
            logger.info(f"user_rating result: {result}")
            return result
        return 'удовлетворительно'
    else:
        # Используем поле сентимента для конкретного метода
        sentiment_field = f'{sentiment_method}_sentiment'
        sentiment_value = row.get(sentiment_field)
        logger.info(f"{sentiment_method}: field={sentiment_field}, value={sentiment_value}")
        
        if sentiment_value and pd.notna(sentiment_value):
            # Преобразуем английские значения в русские
            sentiment_mapping = {
                'positive': 'положительный',
                'negative': 'отрицательный', 
                'neutral': 'нейтральный'
            }
            result = sentiment_mapping.get(sentiment_value, sentiment_value)
            logger.info(f"{sentiment_method} result: {result}")
            return result
        
        return 'удовлетворительно'

@app.route('/sample/create', methods=['POST'])
def create_sample():
    """Создание выборки на основе текущих фильтров"""
    try:
        filters = request.get_json()
        result = sample_manager.create_sample_from_filters(filters)
        return jsonify(result)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/sample/info')
def get_sample_info():
    """Получение информации о текущей выборке"""
    try:
        result = sample_manager.get_sample_info()
        return jsonify(result)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/sample/download')
def download_sample():
    """Скачивание выборки в CSV формате"""
    try:
        df = sample_manager.download_sample()
        if df is not None and not df.empty:
            # Создаем временный файл
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"sample_{timestamp}.csv"
            
            # Сохраняем в CSV
            csv_data = df.to_csv(index=False, encoding='utf-8-sig')
            
            # Создаем ответ
            response = make_response(csv_data)
            response.headers['Content-Type'] = 'text/csv; charset=utf-8-sig'
            response.headers['Content-Disposition'] = f'attachment; filename={filename}'
            return response
        else:
            return jsonify({'success': False, 'error': 'Выборка пуста'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/sample/clear', methods=['POST'])
def clear_sample():
    """Очистка текущей выборки"""
    try:
        success = sample_manager.clear_sample()
        if success:
            return jsonify({'success': True, 'message': 'Выборка очищена'})
        else:
            return jsonify({'success': False, 'error': 'Ошибка очистки выборки'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

# Новые маршруты для версии 1.2.0
@app.route('/database/clean', methods=['POST'])
def clean_database():
    """Очистка базы данных от дублирующихся данных"""
    try:
        from app.core.db_cleaner import DatabaseCleaner
        
        cleaner = DatabaseCleaner()
        result = cleaner.clean_all_duplicates()
        
        return jsonify({
            'success': True,
            'message': 'База данных успешно очищена',
            'result': result
        })
    except Exception as e:
        logger.error(f"Ошибка очистки БД: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/data/validate', methods=['POST'])
def validate_data():
    """Валидация загруженных данных"""
    try:
        from app.core.data_validator import DataValidator
        
        if 'file' not in request.files:
            return jsonify({'success': False, 'error': 'Файл не загружен'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'success': False, 'error': 'Файл не выбран'}), 400
        
        # Сохраняем временный файл
        temp_path = os.path.join('data', 'temp', f'temp_validation_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
        os.makedirs(os.path.dirname(temp_path), exist_ok=True)
        file.save(temp_path)
        
        # Загружаем данные
        if file.filename.endswith('.csv'):
            df = pd.read_csv(temp_path)
        elif file.filename.endswith('.xlsx'):
            df = pd.read_excel(temp_path)
        else:
            return jsonify({'success': False, 'error': 'Неподдерживаемый формат файла'}), 400
        
        # Валидируем данные
        validator = DataValidator()
        validation_result = validator.validate_dataframe(df, file.filename)
        
        # Удаляем временный файл
        try:
            os.remove(temp_path)
        except:
            pass
        
        return jsonify({
            'success': True,
            'validation_result': validation_result,
            'summary': validator.get_validation_summary(validation_result)
        })
        
    except Exception as e:
        logger.error(f"Ошибка валидации данных: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/database/optimize', methods=['POST'])
def optimize_database():
    """Оптимизация базы данных"""
    try:
        from app.core.db_cleaner import DatabaseCleaner
        
        cleaner = DatabaseCleaner()
        
        # Получаем статистику до оптимизации
        cleaner.connect()
        stats_before = cleaner.get_database_stats()
        cleaner.disconnect()
        
        # Выполняем очистку
        result = cleaner.clean_all_duplicates()
        
        # Получаем статистику после оптимизации
        cleaner.connect()
        stats_after = cleaner.get_database_stats()
        cleaner.disconnect()
        
        # Вычисляем экономию
        total_before = sum(stats_before.values())
        total_after = sum(stats_after.values())
        saved = total_before - total_after
        compression = (saved / total_before * 100) if total_before > 0 else 0
        
        return jsonify({
            'success': True,
            'message': 'База данных успешно оптимизирована',
            'optimization': {
                'records_saved': saved,
                'compression_percent': round(compression, 1),
                'groups_cleaned': result['groups_cleanup']['cleaned'],
                'objects_cleaned': result['objects_cleanup']['cleaned']
            },
            'stats_before': stats_before,
            'stats_after': stats_after
        })
        
    except Exception as e:
        logger.error(f"Ошибка оптимизации БД: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/keywords/regenerate', methods=['POST'])
def regenerate_keywords():
    """Перегенерировать ключевые слова на основе текущих данных"""
    try:
        # Импортируем функции из initial_keywords_system.py
        from initial_keywords_system import (
            create_initial_keywords_table,
            update_initial_keywords_from_data
        )
        
        # Создаем таблицу начальных ключевых слов
        if not create_initial_keywords_table():
            return jsonify({'error': 'Ошибка создания таблицы ключевых слов'}), 500
        
        # Обновляем ключевые слова на основе данных
        if update_initial_keywords_from_data():
            return jsonify({
                'success': True,
                'message': 'Ключевые слова успешно обновлены на основе данных'
            })
        else:
            return jsonify({
                'success': True,
                'message': 'Начальные ключевые слова созданы (недостаточно данных для обновления)'
            })
        
    except Exception as e:
        return jsonify({'error': f'Ошибка: {str(e)}'}), 500

@app.route('/api/keywords/update-groups', methods=['POST'])
def update_detected_groups():
    """Обновить detected_groups на основе текущих ключевых слов"""
    try:
        # Импортируем функцию из initial_keywords_system.py
        from initial_keywords_system import detect_group_by_initial_keywords
        import sqlite3
        
        db_path = 'urban_analysis_fixed.db'
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Получаем все объекты
        cursor.execute("""
            SELECT o.id, o.name, GROUP_CONCAT(r.review_text, ' | ') as all_reviews
            FROM objects o
            LEFT JOIN reviews r ON o.id = r.object_id
            GROUP BY o.id, o.name
        """)
        
        objects = cursor.fetchall()
        
        updated_count = 0
        for object_id, object_name, reviews_text in objects:
            # Определяем группу
            detected_group, confidence = detect_group_by_initial_keywords(object_name, reviews_text)
            
            # Получаем ID группы
            if detected_group != 'undetected':
                cursor.execute("SELECT id FROM object_groups WHERE group_type = ?", (detected_group,))
                group_result = cursor.fetchone()
                if group_result:
                    detected_group_id = group_result[0]
                else:
                    detected_group_id = None
            else:
                detected_group_id = None
            
            # Обновляем объект
            cursor.execute("""
                UPDATE objects 
                SET detected_group_id = ?
                WHERE id = ?
            """, (detected_group_id, object_id))
            
            updated_count += 1
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': f'Группы успешно обновлены для {updated_count} объектов'
        })
        
    except Exception as e:
        return jsonify({'error': f'Ошибка: {str(e)}'}), 500

@app.route('/api/sentiment/methods')
def get_available_sentiment_methods():
    """Получить доступные методы сентимента из БД"""
    try:
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        # Получаем только те методы, которые реально используются в analysis_results
        cursor.execute("""
            SELECT DISTINCT pm.method_name
            FROM analysis_results ar
            JOIN processing_methods pm ON ar.method_id = pm.id
            WHERE pm.is_active = 1
            ORDER BY pm.method_name
        """)
        
        used_methods = cursor.fetchall()
        available_methods = [method[0] for method in used_methods]
        
        # Всегда добавляем 'user_rating' как базовый метод, если его нет
        if 'user_rating' not in available_methods:
            available_methods.insert(0, 'user_rating')
        
        conn.close()
        
        return jsonify({
            'success': True,
            'methods': available_methods
        })
        
    except Exception as e:
        return jsonify({'error': f'Ошибка: {str(e)}'}), 500

@app.route('/api/keywords/status')
def get_keywords_status():
    """Получить статус ключевых слов"""
    try:
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        # Подсчитываем количество ключевых слов по группам
        cursor.execute("""
            SELECT group_type, is_initial, COUNT(*) as count
            FROM initial_keywords
            GROUP BY group_type, is_initial
            ORDER BY group_type, is_initial
        """)
        
        keywords_stats = cursor.fetchall()
        
        # Группируем статистику
        grouped_stats = {}
        for group_type, is_initial, count in keywords_stats:
            if group_type not in grouped_stats:
                grouped_stats[group_type] = {'initial': 0, 'extracted': 0}
            
            if is_initial:
                grouped_stats[group_type]['initial'] = count
            else:
                grouped_stats[group_type]['extracted'] = count
        
        # Подсчитываем общее количество объектов и их групп
        cursor.execute("""
            SELECT 
                COUNT(*) as total_objects,
                COUNT(CASE WHEN detected_group_id IS NOT NULL THEN 1 END) as detected_objects,
                COUNT(CASE WHEN detected_group_id IS NULL THEN 1 END) as undetected_objects
            FROM objects
        """)
        
        objects_stats = cursor.fetchone()
        
        conn.close()
        
        return jsonify({
            'keywords_stats': [
                {
                    'group': group, 
                    'initial': stats['initial'], 
                    'extracted': stats['extracted'],
                    'total': stats['initial'] + stats['extracted']
                } 
                for group, stats in grouped_stats.items()
            ],
            'objects_stats': {
                'total': objects_stats[0],
                'detected': objects_stats[1],
                'undetected': objects_stats[2]
            }
        })
        
    except Exception as e:
        return jsonify({'error': f'Ошибка: {str(e)}'}), 500

@app.route('/chart/data')
def get_chart_data():
    """Получение данных для диаграммы с учетом фильтров карты"""
    try:
        # Получаем параметры фильтров (те же, что и для карты)
        active_filters = request.args.get('filters', '').split(',') if request.args.get('filters') else []
        active_filters = [f.strip() for f in active_filters if f.strip()]
        
        logger.info(f"Запрос данных диаграммы с фильтрами: {active_filters}")
        
        # Подключаемся к БД
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        # Базовый запрос для получения объектов с учетом фильтров
        base_query = """
            SELECT DISTINCT o.id, o.name, o.address, o.latitude, o.longitude, 
                   og.group_name as group_type, dg.group_name as determined_group
            FROM objects o
            LEFT JOIN object_groups og ON o.group_id = og.id
            LEFT JOIN detected_groups dg ON o.detected_group_id = dg.id
            WHERE o.latitude IS NOT NULL AND o.longitude IS NOT NULL
        """
        
        params = []
        if active_filters and len(active_filters) > 0:
            placeholders = ','.join(['?' for _ in active_filters])
            base_query += f" AND og.group_type IN ({placeholders})"
            params.extend(active_filters)
        
        cursor.execute(base_query, params)
        filtered_objects = cursor.fetchall()
        
        logger.info(f"Найдено объектов после фильтрации: {len(filtered_objects)}")
        
        if not filtered_objects:
            return jsonify({
                'success': True,
                'data': [],
                'methods': [],
                'reviews': [],
                'message': 'Нет данных для отображения'
            })
        
        # Получаем ID объектов
        object_ids = [obj[0] for obj in filtered_objects]
        object_ids_str = ','.join(['?' for _ in object_ids])
        
        # Запрос для получения отзывов и результатов анализа
        chart_query = """
            SELECT 
                r.id as review_id,
                r.review_text,
                r.rating,
                o.name as object_name,
                og.group_name as object_group,
                pm.method_name,
                ar.sentiment,
                ar.confidence,
                ar.review_type
            FROM reviews r
            JOIN objects o ON r.object_id = o.id
            LEFT JOIN object_groups og ON o.group_id = og.id
            LEFT JOIN analysis_results ar ON r.id = ar.review_id
            LEFT JOIN processing_methods pm ON ar.method_id = pm.id
            WHERE r.object_id IN ({})
            ORDER BY r.id, pm.method_name
        """.format(object_ids_str)
        
        cursor.execute(chart_query, object_ids)
        chart_data = cursor.fetchall()
        
        # Группируем данные по отзывам и методам
        reviews_data = {}
        methods_set = set()
        
        for row in chart_data:
            review_id = row[0]
            review_text = row[1]
            rating = row[2]
            object_name = row[3]
            object_group = row[4]
            method_name = row[5]
            sentiment = row[6]
            confidence = row[7]
            review_type = row[8]
            
            if review_id not in reviews_data:
                reviews_data[review_id] = {
                    'review_id': review_id,
                    'review_text': review_text,
                    'rating': rating,
                    'object_name': object_name,
                    'object_group': object_group,
                    'methods': {}
                }
            
            if method_name:
                methods_set.add(method_name)
                if method_name not in reviews_data[review_id]['methods']:
                    reviews_data[review_id]['methods'][method_name] = {
                        'positive': 0,
                        'negative': 0,
                        'neutral': 0
                    }
                
                # Увеличиваем счетчик для соответствующего сентимента
                if sentiment == 'positive':
                    reviews_data[review_id]['methods'][method_name]['positive'] = 1
                elif sentiment == 'negative':
                    reviews_data[review_id]['methods'][method_name]['negative'] = 1
                else:  # neutral или null
                    reviews_data[review_id]['methods'][method_name]['neutral'] = 1
        
        # Преобразуем в формат для диаграммы: методы по оси Y, отзывы по оси X
        methods_list = sorted(list(methods_set))
        reviews_list = []
        
        # Создаем серии для каждого сентимента
        chart_series = []
        
        # Создаем список отзывов для оси X
        for review_id, review_data in reviews_data.items():
            reviews_list.append({
                'id': review_id,
                'object_name': review_data['object_name'],
                'rating': review_data['rating']
            })
        
        # Создаем серии для каждого сентимента
        positive_series = {
            'name': 'Положительный',
            'data': [],
            'color': '#28a745'  # Зеленый
        }
        
        neutral_series = {
            'name': 'Нейтральный',
            'data': [],
            'color': '#ffc107'  # Желтый
        }
        
        negative_series = {
            'name': 'Отрицательный',
            'data': [],
            'color': '#dc3545'  # Красный
        }
        
        # Заполняем данные для каждого отзыва
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
                negative_series['data'].append(0)
        
        chart_series = [positive_series, neutral_series, negative_series];
        
        conn.close()
        
        return jsonify({
            'success': True,
            'data': chart_series,
            'methods': methods_list,
            'reviews': reviews_list,
            'total_reviews': len(reviews_data)
        })
        
    except Exception as e:
        logger.error(f"Ошибка получения данных диаграммы: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/chart/table')
def get_chart_table():
    """Получение данных для таблицы сентиментов с учетом фильтров карты"""
    try:
        # Получаем все параметры фильтрации
        active_filters = request.args.get('filters', '').split(',') if request.args.get('filters') else []
        active_filters = [f.strip() for f in active_filters if f.strip()]
        group_type = request.args.get('group_type', 'supplier')
        color_scheme = request.args.get('color_scheme', 'group')
        sentiment_method = request.args.get('sentiment_method', 'rating')
        completeness_filter = request.args.get('completeness_filter', 'all_results')  # Новый параметр
        
        logger.info(f"Запрос данных таблицы с параметрами: filters={active_filters}, group_type={group_type}, color_scheme={color_scheme}, sentiment_method={sentiment_method}, completeness_filter={completeness_filter}")
        logger.info(f"Тип active_filters: {type(active_filters)}, содержимое: {active_filters}")
        
        # Подключаемся к БД
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        # Базовый запрос для получения объектов с учетом фильтров
        base_query = """
            SELECT DISTINCT o.id, o.name, o.address, o.latitude, o.longitude, 
                   og.group_name as group_type, dg.group_name as determined_group
            FROM objects o
            LEFT JOIN object_groups og ON o.group_id = og.id
            LEFT JOIN detected_groups dg ON o.detected_group_id = dg.id
            WHERE o.latitude IS NOT NULL AND o.longitude IS NOT NULL
        """
        
        params = []
        if active_filters and len(active_filters) > 0:
            # Определяем, какой тип фильтрации использовать
            if group_type == 'supplier':
                # Фильтруем по группам от поставщика (object_groups)
                placeholders = ','.join(['?' for _ in active_filters])
                base_query += f" AND og.group_name IN ({placeholders})"
                logger.info(f"Применяем фильтр по группам от поставщика: {active_filters}")
            elif group_type == 'determined':
                # Фильтруем по определенным группам (detected_groups)
                placeholders = ','.join(['?' for _ in active_filters])
                base_query += f" AND dg.group_name IN ({placeholders})"
                logger.info(f"Применяем фильтр по определенным группам: {active_filters}")
            else:
                # Если group_type - это конкретное название группы, 
                # то фильтруем по группам от поставщика (object_groups)
                placeholders = ','.join(['?' for _ in active_filters])
                base_query += f" AND og.group_name IN ({placeholders})"
                logger.info(f"group_type='{group_type}' - применяем фильтр по группам от поставщика: {active_filters}")
            params.extend(active_filters)
            # Выполняем запрос только если есть фильтры
            cursor.execute(base_query, params)
            filtered_objects = cursor.fetchall()
        else:
            # Если фильтры не указаны, возвращаем пустой список
            logger.info("Фильтры не указаны, возвращаем пустой список для таблицы сентиментов")
            filtered_objects = []
        
        logger.info(f"Найдено объектов после фильтрации: {len(filtered_objects)}")
        
        if not filtered_objects:
            return jsonify({
                'success': True,
                'data': {
                    'methods': [],
                    'reviews': [],
                    'sentiments': []
                },
                'total_reviews': 0,
                'message': 'Нет данных для отображения'
            })
        
        # Получаем ID объектов
        object_ids = [obj[0] for obj in filtered_objects]
        object_ids_str = ','.join(['?' for _ in object_ids])
        
        # Запрос для получения отзывов и результатов анализа
        chart_query = """
            SELECT 
                r.id as review_id,
                r.review_text,
                r.rating,
                o.name as object_name,
                og.group_name as object_group,
                pm.method_name,
                ar.sentiment,
                ar.confidence,
                ar.review_type,
                COALESCE(mr.sentiment, '') as master_sentiment
            FROM reviews r
            JOIN objects o ON r.object_id = o.id
            LEFT JOIN object_groups og ON o.group_id = og.id
            LEFT JOIN analysis_results ar ON r.id = ar.review_id
            LEFT JOIN processing_methods pm ON ar.method_id = pm.id
            LEFT JOIN master_ratings mr ON r.id = mr.review_id
            WHERE r.object_id IN ({})
            ORDER BY r.id, pm.method_name
        """.format(object_ids_str)
        
        cursor.execute(chart_query, object_ids)
        chart_data = cursor.fetchall()
        
        logger.info(f"Найдено отзывов: {len(chart_data)}")
        logger.info(f"ID объектов для поиска: {object_ids}")
        
        # Группируем данные по отзывам и методам
        reviews_data = {}
        methods_set = set()
        
        for row in chart_data:
            review_id = row[0]
            review_text = row[1]
            rating = row[2]
            object_name = row[3]
            object_group = row[4]
            method_name = row[5]
            sentiment = row[6]
            confidence = row[7]
            review_type = row[8]
            master_sentiment = row[9]
            
            if review_id not in reviews_data:
                reviews_data[review_id] = {
                    'review_id': review_id,
                    'review_text': review_text,
                    'rating': rating,
                    'object_name': object_name,
                    'object_group': object_group,
                    'master_sentiment': master_sentiment,
                    'methods': {}
                }
            
            if method_name:
                methods_set.add(method_name)
                if method_name not in reviews_data[review_id]['methods']:
                    reviews_data[review_id]['methods'][method_name] = {
                        'positive': 0,
                        'negative': 0,
                        'neutral': 0
                    }
                
                # Увеличиваем счетчик для соответствующего сентимента
                if sentiment == 'positive':
                    reviews_data[review_id]['methods'][method_name]['positive'] = 1
                elif sentiment == 'negative':
                    reviews_data[review_id]['methods'][method_name]['negative'] = 1
                else:  # neutral или null
                    reviews_data[review_id]['methods'][method_name]['neutral'] = 1
            
            # Добавляем master_rating если есть
            if master_sentiment:
                methods_set.add('master_rating')
                if 'master_rating' not in reviews_data[review_id]['methods']:
                    reviews_data[review_id]['methods']['master_rating'] = {
                        'positive': 0,
                        'negative': 0,
                        'neutral': 0
                    }
                
                # Устанавливаем master_rating
                if master_sentiment == 'positive':
                    reviews_data[review_id]['methods']['master_rating']['positive'] = 1
                elif master_sentiment == 'negative':
                    reviews_data[review_id]['methods']['master_rating']['negative'] = 1
                else:  # neutral
                    reviews_data[review_id]['methods']['master_rating']['neutral'] = 1
        
        # Создаем таблицу данных
        methods_list = sorted(list(methods_set))
        reviews_list = []
        
        # Создаем структуру таблицы
        table_data = {
            'methods': methods_list,
            'reviews': [],
            'sentiments': []
        }
        
        # Заполняем данные таблицы
        for review_id, review_data in reviews_data.items():
            # Проверяем полноту данных для этого отзыва
            has_missing_values = False
            review_sentiments = []
            
            for method in methods_list:
                if method in review_data['methods']:
                    method_data = review_data['methods'][method]
                    if method_data['positive'] == 1:
                        review_sentiments.append('positive')
                    elif method_data['negative'] == 1:
                        review_sentiments.append('negative')
                    else:
                        review_sentiments.append('neutral')
                else:
                    review_sentiments.append('none')
                    has_missing_values = True
            
            # Применяем фильтр полноты данных
            if completeness_filter == 'no_empty_values' and has_missing_values:
                continue  # Пропускаем отзывы с пустыми значениями
            
            review_info = {
                'id': review_id,
                'object_name': review_data['object_name'],
                'rating': review_data['rating']
            }
            table_data['reviews'].append(review_info)
            table_data['sentiments'].append(review_sentiments)
        
        conn.close()
        
        return jsonify({
            'success': True,
            'data': table_data,
            'total_reviews': len(reviews_data)
        })
        
    except Exception as e:
        logger.error(f"Ошибка получения данных таблицы: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/chart/correlation')
def get_correlation_data():
    """Получение данных для тепловой карты корреляции между методами обработки отзывов"""
    try:
        active_filters = request.args.get('filters', '').split(',') if request.args.get('filters') else []
        active_filters = [f.strip() for f in active_filters if f.strip()]
        group_type = request.args.get('group_type', 'supplier')
        color_scheme = request.args.get('color_scheme', 'group')
        sentiment_method = request.args.get('sentiment_method', 'rating')
        completeness_filter = request.args.get('completeness_filter', 'all_results')
        correlation_type = request.args.get('correlation_type', 'methods')  # Новый параметр: 'methods' или 'groups'
        
        logger.info(f"Запрос данных корреляции с параметрами: filters={active_filters}, group_type={group_type}, color_scheme={color_scheme}, sentiment_method={sentiment_method}, completeness_filter={completeness_filter}, correlation_type={correlation_type}")
        
        # Подключение к базе данных
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        # Фильтрация объектов
        # group_type может быть 'supplier', 'determined' или конкретной группой
        if group_type == 'supplier':
            # Используем группы от поставщика
            base_query = """
                SELECT DISTINCT o.id, o.name, o.address, o.latitude, o.longitude,
                       og.group_name as group_type, dg.group_name as determined_group
                FROM objects o
                LEFT JOIN object_groups og ON o.group_id = og.id
                LEFT JOIN detected_groups dg ON o.detected_group_id = dg.id
                WHERE o.latitude IS NOT NULL AND o.longitude IS NOT NULL
            """
            if active_filters and len(active_filters) > 0:
                placeholders = ','.join(['?' for _ in active_filters])
                base_query += f" AND og.group_name IN ({placeholders})"
                cursor.execute(base_query, active_filters)
                filtered_objects = cursor.fetchall()
            else:
                # Если фильтры не указаны, возвращаем пустой список
                logger.info("Фильтры не указаны, возвращаем пустой список для тепловой карты корреляции (supplier)")
                filtered_objects = []
        elif group_type == 'determined':
            # Используем определенные группы
            base_query = """
                SELECT DISTINCT o.id, o.name, o.address, o.latitude, o.longitude,
                       og.group_name as group_type, dg.group_name as determined_group
                FROM objects o
                LEFT JOIN object_groups og ON o.group_id = og.id
                LEFT JOIN detected_groups dg ON o.detected_group_id = dg.id
                WHERE o.latitude IS NOT NULL AND o.longitude IS NOT NULL
            """
            if active_filters and len(active_filters) > 0:
                placeholders = ','.join(['?' for _ in active_filters])
                base_query += f" AND dg.group_name IN ({placeholders})"
                cursor.execute(base_query, active_filters)
                filtered_objects = cursor.fetchall()
            else:
                # Если фильтры не указаны, возвращаем пустой список
                logger.info("Фильтры не указаны, возвращаем пустой список для тепловой карты корреляции (determined)")
                filtered_objects = []
        else:
            # group_type содержит конкретное название группы (например, 'university')
            base_query = """
                SELECT DISTINCT o.id, o.name, o.address, o.latitude, o.longitude,
                       og.group_name as group_type, dg.group_name as determined_group
                FROM objects o
                LEFT JOIN object_groups og ON o.group_id = og.id
                LEFT JOIN detected_groups dg ON o.detected_group_id = dg.id
                WHERE o.latitude IS NOT NULL AND o.longitude IS NOT NULL
                       AND og.group_name = ?
            """
            cursor.execute(base_query, [group_type])
            filtered_objects = cursor.fetchall()
        
        logger.info(f"Найдено объектов после фильтрации: {len(filtered_objects)}")
        
        if not filtered_objects:
            return jsonify({
                'success': True,
                'data': {
                    'methods': [],
                    'correlation_matrix': []
                }
            })
        
        # Получаем ID отфильтрованных объектов
        object_ids = [obj[0] for obj in filtered_objects]
        placeholders = ','.join(['?' for _ in object_ids])
        
        # Получаем все отзывы и результаты анализа для отфильтрованных объектов
        query = f"""
            SELECT r.id as review_id, r.object_id, r.review_text, r.rating,
                   pm.method_name, ar.sentiment, ar.confidence,
                   COALESCE(mr.sentiment, '') as master_sentiment
            FROM reviews r
            JOIN analysis_results ar ON r.id = ar.review_id
            JOIN processing_methods pm ON ar.method_id = pm.id
            LEFT JOIN master_ratings mr ON r.id = mr.review_id
            WHERE r.object_id IN ({placeholders})
            ORDER BY r.id, pm.method_name
        """
        
        cursor.execute(query, object_ids)
        results = cursor.fetchall()
        
        # Группируем данные по отзывам
        reviews_data = {}
        for row in results:
            review_id, object_id, review_text, rating, method_name, sentiment, confidence, master_sentiment = row
            
            if review_id not in reviews_data:
                reviews_data[review_id] = {
                    'object_id': object_id,
                    'review_text': review_text,
                    'rating': rating,
                    'master_sentiment': master_sentiment,
                    'methods': {}
                }
            
            reviews_data[review_id]['methods'][method_name] = {
                'sentiment': sentiment,
                'confidence': confidence
            }
        
        # Получаем список всех методов
        methods_query = """
            SELECT DISTINCT pm.method_name
            FROM processing_methods pm
            JOIN analysis_results ar ON pm.id = ar.method_id
            ORDER BY pm.method_name
        """
        cursor.execute(methods_query)
        all_methods = [row[0] for row in cursor.fetchall()]
        
        # Добавляем user_rating если его нет
        if 'user_rating' not in all_methods:
            all_methods.insert(0, 'user_rating')
        
        # Добавляем master_rating если есть данные
        if any(review_data.get('master_sentiment') for review_data in reviews_data.values()):
            all_methods.append('master_rating')
        
        # Создаем матрицу данных для корреляции
        # Каждая строка - отзыв, каждый столбец - метод
        correlation_data = []
        for review_id, review_data in reviews_data.items():
            row = []
            has_missing_values = False
            
            for method in all_methods:
                if method == 'user_rating':
                    # Для user_rating используем рейтинг
                    rating = review_data['rating']
                    if rating and pd.notna(rating):
                        from app.core.config import SENTIMENT_CONFIG
                        sentiment = SENTIMENT_CONFIG['rating_to_sentiment'].get(int(rating), 'удовлетворительно')
                        # Преобразуем в числовое значение
                        if sentiment == 'положительный':
                            row.append(1)
                        elif sentiment == 'отрицательный':
                            row.append(-1)
                        else:
                            row.append(0)
                    else:
                        row.append(0)
                        has_missing_values = True
                elif method == 'master_rating':
                    # Для master_rating используем данные из master_ratings
                    master_sentiment = review_data.get('master_sentiment')
                    if master_sentiment:
                        # Преобразуем в числовое значение
                        if master_sentiment == 'positive':
                            row.append(1)
                        elif master_sentiment == 'negative':
                            row.append(-1)
                        else:  # neutral
                            row.append(0)
                    else:
                        row.append(0)
                        has_missing_values = True
                else:
                    # Для других методов используем sentiment из БД
                    method_data = review_data['methods'].get(method)
                    if method_data and method_data['sentiment']:
                        sentiment = method_data['sentiment']
                        # Преобразуем в числовое значение
                        if sentiment == 'positive' or sentiment == 'положительный':
                            row.append(1)
                        elif sentiment == 'negative' or sentiment == 'отрицательный':
                            row.append(-1)
                        else:
                            row.append(0)
                    else:
                        row.append(0)
                        has_missing_values = True
            
            # Применяем фильтр полноты данных
            if completeness_filter == 'no_empty_values' and has_missing_values:
                continue  # Пропускаем отзывы с пустыми значениями
            else:
                correlation_data.append(row)
        
        # Рассчитываем корреляционную матрицу в зависимости от типа
        if correlation_type == 'methods':
            # Стандартная корреляция между методами
            if correlation_data:
                df = pd.DataFrame(correlation_data, columns=all_methods)
                correlation_matrix = df.corr().values.tolist()
            else:
                correlation_matrix = []
            
            conn.close()
            
            return jsonify({
                'success': True,
                'data': {
                    'methods': all_methods,
                    'correlation_matrix': correlation_matrix
                }
            })
        
        elif correlation_type == 'groups':
            # Корреляция методов с master_rating по группам
            # Получаем все группы объектов
            groups_query = """
                SELECT DISTINCT og.group_name
                FROM objects o
                LEFT JOIN object_groups og ON o.group_id = og.id
                WHERE o.latitude IS NOT NULL AND o.longitude IS NOT NULL
                AND og.group_name IS NOT NULL AND og.group_name != ''
                ORDER BY og.group_name
            """
            cursor.execute(groups_query)
            all_groups = [row[0] for row in cursor.fetchall()]
            
            # Фильтруем группы по активным фильтрам
            if active_filters and len(active_filters) > 0:
                all_groups = [group for group in all_groups if group in active_filters]
            
            if not all_groups:
                conn.close()
                return jsonify({
                    'success': True,
                    'data': {
                        'methods': all_methods,
                        'groups': [],
                        'correlation_matrix': []
                    }
                })
            
            # Создаем матрицу корреляции методов с master_rating по группам
            group_correlation_matrix = []
            
            for group in all_groups:
                group_row = []
                
                # Получаем объекты группы
                objects_query = """
                    SELECT o.id
                    FROM objects o
                    LEFT JOIN object_groups og ON o.group_id = og.id
                    WHERE og.group_name = ? AND o.latitude IS NOT NULL AND o.longitude IS NOT NULL
                """
                cursor.execute(objects_query, [group])
                group_objects = cursor.fetchall()
                
                if not group_objects:
                    # Если нет объектов в группе, заполняем нулями
                    group_row = [0.0] * len(all_methods)
                    group_correlation_matrix.append(group_row)
                    continue
                
                group_object_ids = [obj[0] for obj in group_objects]
                
                # Фильтруем данные только для объектов этой группы
                group_correlation_data = []
                for row in correlation_data:
                    # Проверяем, принадлежит ли отзыв объекту из этой группы
                    review_id = row[0] if len(row) > 0 else None
                    if review_id:
                        # Получаем object_id для этого отзыва
                        review_query = "SELECT object_id FROM reviews WHERE id = ?"
                        cursor.execute(review_query, [review_id])
                        review_result = cursor.fetchone()
                        if review_result and review_result[0] in group_object_ids:
                            group_correlation_data.append(row)
                
                # Рассчитываем корреляцию для каждого метода с master_rating в этой группе
                for method_index, method in enumerate(all_methods):
                    method_values = []
                    master_values = []
                    
                    for row in group_correlation_data:
                        if method_index < len(row):
                            method_value = row[method_index]
                            # Получаем master_rating для этого отзыва
                            review_id = row[0] if len(row) > 0 else None
                            if review_id:
                                master_query = "SELECT sentiment FROM master_ratings WHERE review_id = ?"
                                cursor.execute(master_query, [review_id])
                                master_result = cursor.fetchone()
                                if master_result and master_result[0]:
                                    master_sentiment = master_result[0]
                                    # Преобразуем master_sentiment в числовое значение
                                    if master_sentiment == 'positive':
                                        master_values.append(1)
                                    elif master_sentiment == 'negative':
                                        master_values.append(-1)
                                    else:  # neutral
                                        master_values.append(0)
                                    
                                    # Добавляем значение метода
                                    method_values.append(method_value)
                    
                    # Рассчитываем корреляцию
                    if len(method_values) >= 3 and len(master_values) >= 3 and len(method_values) == len(master_values):
                        try:
                            correlation = np.corrcoef(method_values, master_values)[0, 1]
                            if pd.isna(correlation):
                                correlation = 0.0
                            group_row.append(max(0.0, min(1.0, abs(correlation))))
                        except:
                            group_row.append(0.0)
                    else:
                        group_row.append(0.0)
                
                group_correlation_matrix.append(group_row)
            
            conn.close()
            
            return jsonify({
                'success': True,
                'data': {
                    'methods': all_methods,
                    'groups': all_groups,
                    'correlation_matrix': group_correlation_matrix
                }
            })
        
        else:
            # Неизвестный тип корреляции
            conn.close()
            return jsonify({
                'success': False,
                'error': f"Неизвестный тип корреляции: {correlation_type}"
            })
        
    except Exception as e:
        logger.error(f"Ошибка при получении данных корреляции: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/chart/group-correlation')
def get_group_correlation_data():
    """Получение данных для тепловой карты корреляции между группами объектов"""
    try:
        active_filters = request.args.get('filters', '').split(',') if request.args.get('filters') else []
        active_filters = [f.strip() for f in active_filters if f.strip()]
        group_type = request.args.get('group_type', 'supplier')
        color_scheme = request.args.get('color_scheme', 'group')
        sentiment_method = request.args.get('sentiment_method', 'rating')
        completeness_filter = request.args.get('completeness_filter', 'no_empty_values')
        
        logger.info(f"Запрос данных корреляции групп с параметрами: filters={active_filters}, group_type={group_type}, color_scheme={color_scheme}, sentiment_method={sentiment_method}, completeness_filter={completeness_filter}")
        
        # Подключение к базе данных
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        # Получаем все группы объектов
        groups_query = """
            SELECT DISTINCT og.group_name
            FROM objects o
            JOIN object_groups og ON o.group_id = og.id
            WHERE o.latitude IS NOT NULL AND o.longitude IS NOT NULL
            AND og.group_name IS NOT NULL AND og.group_name != ''
            ORDER BY og.group_name
        """
        cursor.execute(groups_query)
        all_groups = [row[0] for row in cursor.fetchall()]
        
        if not all_groups:
            return jsonify({
                'success': True,
                'data': {
                    'groups': [],
                    'correlation_matrix': []
                }
            })
        
        # Фильтруем группы по активным фильтрам
        if active_filters and len(active_filters) > 0:
            all_groups = [group for group in all_groups if group in active_filters]
        
        if not all_groups:
            return jsonify({
                'success': True,
                'data': {
                    'groups': [],
                    'correlation_matrix': []
                }
            })
        
        # Получаем данные для каждой группы
        group_data = {}
        for group in all_groups:
            # Получаем объекты группы
            objects_query = """
                SELECT o.id, o.name, o.address, o.latitude, o.longitude
                FROM objects o
                JOIN object_groups og ON o.group_id = og.id
                WHERE og.group_name = ? AND o.latitude IS NOT NULL AND o.longitude IS NOT NULL
            """
            cursor.execute(objects_query, [group])
            objects = cursor.fetchall()
            
            if not objects:
                continue
                
            object_ids = [obj[0] for obj in objects]
            placeholders = ','.join(['?' for _ in object_ids])
            
            # Получаем отзывы и рейтинги для объектов группы
            reviews_query = f"""
                SELECT r.rating, COALESCE(ar.sentiment, 'neutral') as sentiment
                FROM reviews r
                LEFT JOIN analysis_results ar ON r.id = ar.review_id
                WHERE r.object_id IN ({placeholders})
            """
            cursor.execute(reviews_query, object_ids)
            reviews = cursor.fetchall()
            
            if not reviews:
                continue
            
            # Рассчитываем средние характеристики группы
            ratings = []
            sentiments = []
            
            for rating, sentiment in reviews:
                if rating and pd.notna(rating):
                    ratings.append(float(rating))
                
                if sentiment:
                    # Преобразуем sentiment в числовое значение
                    if sentiment in ['positive', 'положительный']:
                        sentiments.append(1)
                    elif sentiment in ['negative', 'отрицательный']:
                        sentiments.append(-1)
                    else:
                        sentiments.append(0)
            
            # Сохраняем данные группы
            group_data[group] = {
                'avg_rating': np.mean(ratings) if ratings else 0,
                'avg_sentiment': np.mean(sentiments) if sentiments else 0,
                'object_count': len(objects),
                'review_count': len(reviews)
            }
        
        # Создаем матрицу корреляции между группами
        correlation_matrix = []
        for i, group1 in enumerate(all_groups):
            row = []
            for j, group2 in enumerate(all_groups):
                if i == j:
                    # Диагональ - всегда 1.0
                    row.append(1.0)
                else:
                    # Рассчитываем корреляцию между группами
                    data1 = group_data.get(group1, {})
                    data2 = group_data.get(group2, {})
                    
                    # Простая корреляция на основе средних значений
                    if data1 and data2:
                        # Нормализуем данные для корреляции
                        rating1 = data1['avg_rating'] / 5.0  # Нормализуем рейтинг 0-1
                        rating2 = data2['avg_rating'] / 5.0
                        sentiment1 = data1['avg_sentiment']
                        sentiment2 = data2['avg_sentiment']
                        
                        # Рассчитываем корреляцию как среднее между корреляциями рейтинга и сентимента
                        rating_corr = 1.0 - abs(rating1 - rating2)
                        sentiment_corr = 1.0 - abs(sentiment1 - sentiment2)
                        
                        correlation = (rating_corr + sentiment_corr) / 2.0
                        row.append(max(0.0, min(1.0, correlation)))  # Ограничиваем 0-1
                    else:
                        row.append(0.0)
            
            correlation_matrix.append(row)
        
        conn.close()
        
        return jsonify({
            'success': True,
            'data': {
                'groups': all_groups,
                'correlation_matrix': correlation_matrix
            }
        })
        
    except Exception as e:
        logger.error(f"Ошибка при получении данных корреляции групп: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/chart/method-correlation')
def get_method_correlation_data():
    """Получение данных для тепловой карты корреляции методов с master_rating по группам"""
    logger.info("=== ВЫЗВАН ENDPOINT /chart/method-correlation ===")
    try:
        active_filters = request.args.get('filters', '').split(',') if request.args.get('filters') else []
        active_filters = [f.strip() for f in active_filters if f.strip()]
        group_type = request.args.get('group_type', 'supplier')
        color_scheme = request.args.get('color_scheme', 'group')
        sentiment_method = request.args.get('sentiment_method', 'rating')
        completeness_filter = request.args.get('completeness_filter', 'no_empty_values')
        
        logger.info(f"Запрос данных корреляции методов с параметрами: filters={active_filters}, group_type={group_type}, color_scheme={color_scheme}, sentiment_method={sentiment_method}, completeness_filter={completeness_filter}")
        
        # Подключение к базе данных
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        # Получаем все группы объектов
        groups_query = """
            SELECT DISTINCT og.group_name
            FROM objects o
            JOIN object_groups og ON o.group_id = og.id
            WHERE o.latitude IS NOT NULL AND o.longitude IS NOT NULL
            AND og.group_name IS NOT NULL AND og.group_name != ''
            ORDER BY og.group_name
        """
        cursor.execute(groups_query)
        all_groups = [row[0] for row in cursor.fetchall()]
        
        # Фильтруем группы по активным фильтрам
        if active_filters and len(active_filters) > 0:
            all_groups = [group for group in all_groups if group in active_filters]
        
        if not all_groups:
            return jsonify({
                'success': True,
                'data': {
                    'methods': [],
                    'groups': [],
                    'correlation_matrix': []
                }
            })
        
        # Получаем все методы анализа (исключаем старые llm_* методы)
        methods_query = """
            SELECT DISTINCT pm.method_name
            FROM processing_methods pm
            JOIN analysis_results ar ON pm.id = ar.method_id
            WHERE pm.method_name NOT LIKE 'llm_%'
            ORDER BY pm.method_name
        """
        cursor.execute(methods_query)
        all_methods = [row[0] for row in cursor.fetchall()]
        
        logger.info(f"Найдены методы анализа (после фильтрации llm_*): {all_methods}")
        
        # Добавляем user_rating если его нет
        if 'user_rating' not in all_methods:
            all_methods.insert(0, 'user_rating')
            logger.info("Добавлен user_rating в методы")
        
        # Убираем master_rating из списка методов (он будет эталоном)
        if 'master_rating' in all_methods:
            all_methods.remove('master_rating')
            logger.info("Убран master_rating из методов (будет эталоном)")
        
        logger.info(f"Итоговый список методов для анализа: {all_methods}")
        logger.info(f"🔍 Порядок методов (с индексами):")
        for i, method in enumerate(all_methods):
            logger.info(f"  [{i}] {method}")
        
        if not all_methods:
            return jsonify({
                'success': True,
                'data': {
                    'methods': [],
                    'groups': [],
                    'correlation_matrix': []
                }
            })
        
        # Создаем матрицу корреляции методов с master_rating по группам
        correlation_matrix = []
        
        for group in all_groups:
            logger.info(f"=== ОБРАБОТКА ГРУППЫ: {group} ===")
            
            # Получаем объекты группы
            objects_query = """
                SELECT o.id
                FROM objects o
                JOIN object_groups og ON o.group_id = og.id
                WHERE og.group_name = ? AND o.latitude IS NOT NULL AND o.longitude IS NOT NULL
            """
            cursor.execute(objects_query, [group])
            objects = cursor.fetchall()
            
            logger.info(f"Группа {group}: найдено {len(objects)} объектов")
            
            if not objects:
                # Если нет объектов в группе, заполняем нулями
                correlation_matrix.append([0.0] * len(all_methods))
                logger.info(f"Группа {group}: нет объектов, заполняем нулями")
                continue
                
            object_ids = [obj[0] for obj in objects]
            placeholders = ','.join(['?' for _ in object_ids])
            
            # Получаем отзывы и результаты анализа для объектов группы
            reviews_query = f"""
                SELECT r.id as review_id, r.rating, 
                       COALESCE(mr.sentiment, '') as master_sentiment
                FROM reviews r
                LEFT JOIN master_ratings mr ON r.id = mr.review_id
                WHERE r.object_id IN ({placeholders}) AND mr.sentiment IS NOT NULL
            """
            cursor.execute(reviews_query, object_ids)
            reviews = cursor.fetchall()
            
            logger.info(f"Группа {group}: найдено {len(reviews)} отзывов")
            
            if not reviews:
                # Если нет отзывов в группе, заполняем нулями
                correlation_matrix.append([0.0] * len(all_methods))
                logger.info(f"Группа {group}: нет отзывов, заполняем нулями")
                continue
            
            # Группируем данные по отзывам для нашей функции
            group_data = []
            logger.info(f"Группа {group}: преобразуем данные отзывов...")
            
            for i, (review_id, rating, master_sentiment) in enumerate(reviews):
                review_data = {}
                
                logger.info(f"Группа {group}, отзыв {i+1}: rating={rating}, master={master_sentiment}")
                
                # Добавляем user_rating
                if rating and pd.notna(rating):
                    from app.core.config import SENTIMENT_CONFIG
                    sentiment_rating = SENTIMENT_CONFIG['rating_to_sentiment'].get(int(rating), 'удовлетворительно')
                    if sentiment_rating == 'положительный':
                        review_data['user_rating'] = 1
                    elif sentiment_rating == 'отрицательный':
                        review_data['user_rating'] = -1
                    else:
                        review_data['user_rating'] = 0
                    logger.info(f"  user_rating: {rating} -> {sentiment_rating} -> {review_data['user_rating']}")
                
                # Добавляем master_rating
                if master_sentiment:
                    if master_sentiment == 'positive':
                        review_data['master_rating'] = 1
                    elif master_sentiment == 'negative':
                        review_data['master_rating'] = -1
                    else:  # neutral
                        review_data['master_rating'] = 0
                    logger.info(f"  master_rating: {master_sentiment} -> {review_data['master_rating']}")
                
                # Теперь получаем ВСЕ методы для этого отзыва
                methods_query = """
                    SELECT pm.method_name, ar.sentiment, pm.id as method_id
                    FROM analysis_results ar
                    JOIN processing_methods pm ON ar.method_id = pm.id
                    WHERE ar.review_id = ?
                """
                cursor.execute(methods_query, [review_id])
                method_results = cursor.fetchall()
                
                logger.info(f"  Отзыв {review_id}: найдено {len(method_results)} методов")
                
                # Добавляем все методы для этого отзыва
                for method_name, sentiment, method_id in method_results:
                    logger.info(f"    🔍 Проверяем метод: {method_name} (ID: {method_id}) - в all_methods: {method_name in all_methods}")
                    if method_name in all_methods:  # Только те методы, которые мы анализируем
                        if sentiment in ['positive', 'положительный']:
                            review_data[method_name] = 1
                        elif sentiment in ['negative', 'отрицательный']:
                            review_data[method_name] = -1
                        else:
                            review_data[method_name] = 0
                        logger.info(f"      ✅ {method_name}: {sentiment} -> {review_data[method_name]}")
                    else:
                        logger.info(f"      ❌ {method_name} НЕ в all_methods, пропускаем")
                
                if 'master_rating' in review_data:  # Только если есть master_rating
                    group_data.append(review_data)
                    logger.info(f"  Добавлен отзыв: {review_data}")
                else:
                    logger.info(f"  Отзыв пропущен (нет master_rating)")
            
            logger.info(f"Группа {group}: итоговых отзывов для анализа: {len(group_data)}")
            
            # Используем нашу простую функцию для расчета корреляции
            if group_data:
                logger.info(f"Группа {group}: {len(group_data)} отзывов, методы: {list(group_data[0].keys()) if group_data else 'нет данных'}")
                
                # Анализируем данные для каждого метода
                logger.info(f"🔍 АНАЛИЗ ДАННЫХ ДЛЯ ГРУППЫ {group}:")
                for method in all_methods:
                    if method == 'master_rating':
                        continue
                    
                    method_values = []
                    master_values = []
                    
                    for review in group_data:
                        if method in review and 'master_rating' in review:
                            method_values.append(review[method])
                            master_values.append(review['master_rating'])
                    
                    logger.info(f"  📊 {method}: {len(method_values)} значений, master_rating: {len(master_values)} значений")
                    if method_values:
                        logger.info(f"    Значения {method}: {method_values[:10]}{'...' if len(method_values) > 10 else ''}")
                        logger.info(f"    Значения master_rating: {master_values[:10]}{'...' if len(master_values) > 10 else ''}")
                    else:
                        logger.warning(f"    ⚠️ НЕТ ДАННЫХ для метода {method}!")
                
                correlations = calculate_method_correlation(group_data, all_methods)
                logger.info(f"Результат корреляции для группы {group}: {correlations}")
                
                # Проверяем соответствие количества методов
                logger.info(f"  Ожидаем методов: {len(all_methods)}, получили корреляций: {len(correlations)}")
                
                # Дополняем нулями, если методов меньше чем ожидается
                while len(correlations) < len(all_methods):
                    correlations.append(0.0)
                    logger.warning(f"  ⚠️ Дополняем нулём для метода {all_methods[len(correlations)-1]}")
                
                correlation_matrix.append(correlations)
            else:
                logger.info(f"Группа {group}: нет данных")
                correlation_matrix.append([0.0] * len(all_methods))

        
        conn.close()
        
        return jsonify({
            'success': True,
            'data': {
                'methods': all_methods,
                'groups': all_groups,
                'correlation_matrix': correlation_matrix
            }
        })
        
    except Exception as e:
        logger.error(f"Ошибка при получении данных корреляции методов: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/database/data')
def get_database_data():
    """Получение данных из БД с учетом фильтров"""
    try:
        # Получаем параметры фильтрации
        active_filters = request.args.get('filters', '').split(',') if request.args.get('filters') else []
        active_filters = [f.strip() for f in active_filters if f.strip()]
        group_filters = request.args.get('group_filters', '').split(',') if request.args.get('group_filters') else []
        group_filters = [f.strip() for f in group_filters if f.strip()]
        group_type = request.args.get('group_type', 'supplier')
        color_scheme = request.args.get('color_scheme', 'group')
        sentiment_method = request.args.get('sentiment_method', 'rating')
        
        logger.info(f"Запрос данных БД с параметрами: filters={active_filters}, group_filters={group_filters}, group_type={group_type}, color_scheme={color_scheme}, sentiment_method={sentiment_method}")
        
        # Подключаемся к БД
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        # Строим запрос с учетом фильтров групп
        data_query = """
            SELECT 
                o.name,
                o.address,
                og.group_name as group_type,
                dg.group_name as determined_group,
                r.review_text,
                r.rating,
                COALESCE(ar.sentiment, 'neutral') as sentiment
            FROM objects o
            LEFT JOIN object_groups og ON o.group_id = og.id
            LEFT JOIN detected_groups dg ON o.detected_group_id = dg.id
            LEFT JOIN reviews r ON o.id = r.object_id
            LEFT JOIN analysis_results ar ON r.id = ar.review_id
        """
        
        # Добавляем фильтрацию по группам, если указаны фильтры
        if group_filters:
            placeholders = ','.join(['?' for _ in group_filters])
            data_query += f" WHERE og.group_name IN ({placeholders})"
            data_query += " ORDER BY o.name, r.id"
            cursor.execute(data_query, group_filters)
        else:
            # Если фильтры не указаны, возвращаем пустой список
            logger.info("Фильтры не указаны, возвращаем пустой список")
            results = []
        
        # Выполняем запрос только если есть фильтры
        if group_filters:
            results = cursor.fetchall()
        else:
            results = []
        
        logger.info(f"Найдено записей в БД: {len(results)}")
        
        # Группируем данные по объектам
        data = []
        for row in results:
            name, address, group_type, determined_group, review_text, rating, sentiment = row
            
            data.append({
                'name': name or '',
                'address': address or '',
                'group': group_type or '',
                'determined_group': determined_group or '',
                'review_text': review_text or '',
                'rating': rating or '',
                'sentiment': sentiment or ''
            })
        
        logger.info(f"Подготовлено записей для отображения: {len(data)}")
        
        conn.close()
        
        return jsonify({
            'success': True,
            'data': data
        })
        
    except Exception as e:
        logger.error(f"Ошибка получения данных БД: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/master-rating/data')
def get_master_rating_data():
    """Получение данных для интерфейса мастер-рейтинга"""
    try:
        # Получаем параметры фильтрации
        active_filters = request.args.get('filters', '').split(',') if request.args.get('filters') else []
        active_filters = [f.strip() for f in active_filters if f.strip()]
        
        logger.info(f"Запрос данных мастер-рейтинга с параметрами: filters={active_filters}")
        
        # Подключаемся к БД
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        # Запрос для получения отзывов с мастер-рейтингами
        data_query = """
            SELECT 
                r.id as review_id,
                o.name,
                o.address,
                og.group_name as group_type,
                dg.group_name as determined_group,
                r.review_text,
                COALESCE(mr.sentiment, '') as master_sentiment
            FROM reviews r
            JOIN objects o ON r.object_id = o.id
            LEFT JOIN object_groups og ON o.group_id = og.id
            LEFT JOIN detected_groups dg ON o.detected_group_id = dg.id
            LEFT JOIN master_ratings mr ON r.id = mr.review_id
        """
        
        # Добавляем фильтрацию по группам если указаны фильтры
        params = []
        if active_filters and len(active_filters) > 0:
            placeholders = ','.join(['?' for _ in active_filters])
            data_query += f" WHERE og.group_name IN ({placeholders})"
            params.extend(active_filters)
        
        data_query += " ORDER BY o.name, r.id"
        
        # Выполняем запрос только если есть фильтры
        if active_filters and len(active_filters) > 0:
            cursor.execute(data_query, params)
            results = cursor.fetchall()
        else:
            # Если фильтры не указаны, возвращаем пустой список
            logger.info("Фильтры не указаны, возвращаем пустой список для мастер-рейтинга")
            results = []
        
        logger.info(f"Найдено отзывов для мастер-рейтинга: {len(results)}")
        
        # Группируем данные
        data = []
        for row in results:
            review_id, name, address, group_type, determined_group, review_text, master_sentiment = row
            
            data.append({
                'review_id': review_id,
                'name': name or '',
                'address': address or '',
                'group': group_type or '',
                'determined_group': determined_group or '',
                'review_text': review_text or '',
                'master_sentiment': master_sentiment or ''
            })
        
        conn.close()
        
        return jsonify({
            'success': True,
            'data': data
        })
        
    except Exception as e:
        logger.error(f"Ошибка получения данных мастер-рейтинга: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/master-rating/save', methods=['POST'])
def save_master_rating():
    """Сохранение мастер-рейтинга"""
    try:
        data = request.get_json()
        review_id = data.get('review_id')
        sentiment = data.get('sentiment')
        
        if not review_id or not sentiment:
            return jsonify({
                'success': False,
                'error': 'Не указаны review_id или sentiment'
            })
        
        # Сохраняем мастер-рейтинг
        db_manager_fixed.insert_master_rating(review_id, sentiment)
        
        logger.info(f"Сохранен мастер-рейтинг: review_id={review_id}, sentiment={sentiment}")
        
        return jsonify({
            'success': True,
            'message': 'Мастер-рейтинг сохранен'
        })
        
    except Exception as e:
        logger.error(f"Ошибка сохранения мастер-рейтинга: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/api/detect-group', methods=['POST'])
def detect_group():
    """Определение группы объекта на основе отзывов"""
    try:
        data = request.get_json()
        reviews = data.get('reviews', '')
        object_name = data.get('object_name', '')
        address = data.get('address', '')
        
        logger.info(f"Определение группы для объекта: {object_name}")
        
        # Используем существующую логику определения группы
        from app.core.district_detector import detect_group_from_text
        
        # Объединяем всю информацию для лучшего определения
        combined_text = f"{object_name} {address} {reviews}"
        detected_group = detect_group_from_text(combined_text)
        
        logger.info(f"Определена группа: {detected_group}")
        
        return jsonify({
            'success': True,
            'detected_group': detected_group
        })
        
    except Exception as e:
        logger.error(f"Ошибка определения группы: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/master-rating/stats')
def get_master_rating_stats():
    """Получение статистики мастер-рейтингов"""
    try:
        # Получаем все мастер-рейтинги
        master_ratings = db_manager_fixed.get_all_master_ratings()
        
        # Подсчитываем статистику
        stats = {
            'total_rated': len(master_ratings),
            'positive': 0,
            'negative': 0,
            'neutral': 0
        }
        
        for sentiment in master_ratings.values():
            if sentiment in stats:
                stats[sentiment] += 1
        
        return jsonify({
            'success': True,
            'stats': stats
        })
        
    except Exception as e:
        logger.error(f"Ошибка получения статистики мастер-рейтингов: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/api/object-groups')
def get_object_groups():
    """Получение списка групп объектов"""
    try:
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        # Получаем все группы объектов
        cursor.execute("""
            SELECT group_name, group_type 
            FROM object_groups 
            ORDER BY group_type
        """)
        groups = cursor.fetchall()
        
        conn.close()
        
        # Формируем ответ
        groups_data = []
        for group_name, group_type in groups:
            groups_data.append({
                'name': group_name,
                'type': group_type
            })
        
        logger.info(f"Загружено {len(groups_data)} групп объектов")
        
        return jsonify({
            'success': True,
            'groups': groups_data
        })
        
    except Exception as e:
        logger.error(f"Ошибка загрузки групп объектов: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        })

def test_calculate_method_correlation():
    """Тестирование функции calculate_method_correlation"""
    logger.info("=== ТЕСТИРОВАНИЕ ФУНКЦИИ calculate_method_correlation ===")
    
    # Тест 1: идеальная корреляция (должна быть 1.0)
    test_data_1 = [
        {'method1': 1, 'method2': 1, 'master_rating': 1},
        {'method1': 2, 'method2': 2, 'master_rating': 2},
        {'method1': 3, 'method2': 3, 'master_rating': 3}
    ]
    
    # Тест 2: отсутствие корреляции (должна быть 0.0)
    test_data_2 = [
        {'method1': 1, 'method2': 0, 'method3': 0.5, 'master_rating': 1},
        {'method1': 1, 'method2': 0, 'method3': 0.5, 'master_rating': 2},
        {'method1': 0, 'method2': 1, 'method3': 0.5, 'master_rating': 3}
    ]
    
    # Тест 3: средняя корреляция (должна быть около 0.5)
    test_data_3 = [
        {'method1': 1, 'method2': 1, 'master_rating': 1},
        {'method1': 2, 'method2': 1, 'master_rating': 2},
        {'method1': 3, 'method2': 2, 'master_rating': 3}
    ]
    
    # Создаем список методов для тестов
    test_methods = ['method1', 'method2', 'method3']
    
    logger.info("Тест 1 - идеальная корреляция:")
    result_1 = calculate_method_correlation(test_data_1, test_methods)
    logger.info(f"Результат: {result_1}")
    
    logger.info("Тест 2 - отсутствие корреляции:")
    result_2 = calculate_method_correlation(test_data_2, test_methods)
    logger.info(f"Результат: {result_2}")
    
    logger.info("Тест 3 - средняя корреляция:")
    result_3 = calculate_method_correlation(test_data_3, test_methods)
    logger.info(f"Результат: {result_3}")
    
    # Тест 4: ваши точные примеры корреляции
    logger.info("Тест 4 - точные примеры корреляции:")
    
    # Тест 4.1: корреляция = 1 (идеальная положительная)
    test_data_4_1 = [
        {'method': 5, 'master_rating': 1},
        {'method': 6, 'master_rating': 2},
        {'method': 7, 'master_rating': 3}
    ]
    result_4_1 = calculate_method_correlation(test_data_4_1, ['method'])
    logger.info(f"Корреляция = 1 (должна быть 1.0): {result_4_1}")
    
    # Тест 4.2: корреляция = -1 (идеальная отрицательная)
    test_data_4_2 = [
        {'method': 3, 'master_rating': 1},
        {'method': 2, 'master_rating': 2},
        {'method': 1, 'master_rating': 3}
    ]
    result_4_2 = calculate_method_correlation(test_data_4_2, ['method'])
    logger.info(f"Корреляция = -1 (должна быть 1.0 после abs): {result_4_2}")
    
    # Тест 4.3: корреляция = 0 (отсутствие корреляции)
    test_data_4_3 = [
        {'method': 1, 'master_rating': 1},
        {'method': 1, 'master_rating': 2},
        {'method': 1, 'master_rating': 3}
    ]
    result_4_3 = calculate_method_correlation(test_data_4_3, ['method'])
    logger.info(f"Корреляция = 0 (должна быть 0.0): {result_4_2}")
    
    logger.info("=== ТЕСТИРОВАНИЕ ЗАВЕРШЕНО ===")

if __name__ == '__main__':
    # Запускаем тест перед запуском приложения
    test_calculate_method_correlation()
    app.run(debug=True, host='0.0.0.0', port='5000') 