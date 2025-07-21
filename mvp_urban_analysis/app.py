"""
Основное Flask приложение для анализа данных городской среды
"""

from flask import Flask, render_template, request, jsonify, send_file
import pandas as pd
import os
import json
import logging
from datetime import datetime
import numpy as np

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

def convert_dataframe_for_json(df):
    """
    Конвертирует DataFrame для JSON сериализации
    
    Args:
        df: DataFrame для конвертации
        
    Returns:
        DataFrame с конвертированными типами данных
    """
    df_converted = df.copy()
    for col in df_converted.columns:
        if df_converted[col].dtype in ['int64', 'float64']:
            df_converted[col] = df_converted[col].astype(float)
        elif df_converted[col].dtype == 'object':
            df_converted[col] = df_converted[col].astype(str)
    return df_converted

def make_json_safe(obj):
    if isinstance(obj, dict):
        return {make_json_safe(k): make_json_safe(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [make_json_safe(x) for x in obj]
    elif isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    else:
        return obj

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
    'gemini': os.getenv('GOOGLE_GEMINI_API_KEY'),
    'yandex': os.getenv('YANDEXGPT_API_KEY'),
    'gigachat': os.getenv('GIGACHAT_API_KEY'),
    'qwen': os.getenv('QWEN_API_KEY'),
    'deepseek': os.getenv('DEEPSEEK_API_KEY')
}
llm_analyzer = LLMAnalyzer(api_keys=api_keys)

geocoder = MoscowGeocoder(api_key=geocoder_api_key)

@app.route('/')
def index():
    """Главная страница"""
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    """Загрузка файла для обработки"""
    try:
        logger.info("=== НАЧАЛО ОБРАБОТКИ ФАЙЛА ===")
        
        if 'file' not in request.files:
            logger.error("Файл не выбран")
            return jsonify({'error': 'Файл не выбран'}), 400
        
        file = request.files['file']
        if file.filename == '':
            logger.error("Файл не выбран")
            return jsonify({'error': 'Файл не выбран'}), 400
        
        logger.info(f"Загружен файл: {file.filename}")
        
        # Проверяем поддерживаемые форматы
        allowed_extensions = ['.csv', '.json', '.xlsx', '.xls']
        file_ext = os.path.splitext(file.filename)[1].lower()
        
        if file_ext not in allowed_extensions:
            logger.error(f"Неподдерживаемый формат файла: {file_ext}")
            return jsonify({'error': f'Неподдерживаемый формат файла. Поддерживаются: {", ".join(allowed_extensions)}'}), 400
        
        logger.info(f"Формат файла: {file_ext}")
        
        # Сохраняем файл временно
        temp_filename = f'upload_{datetime.now().strftime("%Y%m%d_%H%M%S")}{file_ext}'
        temp_path = os.path.join('data', 'temp', temp_filename)
        os.makedirs(os.path.dirname(temp_path), exist_ok=True)
        file.save(temp_path)
        
        logger.info(f"Файл сохранен: {temp_path}")
        
        # Определяем тип файла
        file_type = None
        if file_ext == '.csv':
            file_type = 'csv'
        elif file_ext == '.json':
            file_type = 'json'
        elif file_ext in ['.xlsx', '.xls']:
            file_type = 'excel'
        
        logger.info(f"Тип файла определен: {file_type}")
        
        # Получаем дополнительные параметры
        sheet_name = request.form.get('sheet_name')
        filters = request.form.get('filters')
        group = request.form.get('group')  # Получаем группу из формы
        
        if filters:
            try:
                filters = json.loads(filters)
                logger.info(f"Применены фильтры: {filters}")
            except:
                filters = None
                logger.warning("Ошибка парсинга фильтров")
        
        # Получаем выбранные методы анализа
        analysis_methods = request.form.get('analysis_methods', 'classical')
        if isinstance(analysis_methods, str):
            analysis_methods = [analysis_methods]
        elif not analysis_methods:
            analysis_methods = ['classical']
        
        logger.info(f"Методы анализа: {analysis_methods}")
        logger.info(f"Указанная группа: {group}")
        
        # Обрабатываем данные
        logger.info("Начинаем загрузку данных...")
        df = data_processor.load_data(temp_path, file_type, sheet_name, filters)
        logger.info(f"Данные загружены: {len(df)} строк")
        
        if df.empty:
            logger.error("Ошибка загрузки файла - DataFrame пустой")
            return jsonify({'error': 'Ошибка загрузки файла'}), 400
        
        # Применяем группу, если она указана
        if group and 'group' in df.columns:
            logger.info(f"Применяем группу '{group}' ко всем записям")
            df['group'] = group
            logger.info(f"Группа применена. Уникальные группы в данных: {df['group'].unique()}")
        else:
            # Проверяем, есть ли записи с пустой группой или отсутствующим полем group
            needs_group_input = False
            
            logger.info(f"Проверяем поле 'group' в данных...")
            logger.info(f"Колонки в DataFrame: {list(df.columns)}")
            
            if 'group' in df.columns:
                logger.info(f"Поле 'group' присутствует в данных")
                logger.info(f"Уникальные значения в поле 'group': {df['group'].unique()}")
                
                # Проверяем пустые группы (NaN, пустые строки, пробелы)
                empty_groups = df[df['group'].isna() | (df['group'] == '') | (df['group'].astype(str).str.strip() == '')]
                logger.info(f"Найдено {len(empty_groups)} записей с пустой группой")
                
                if len(empty_groups) > 0:
                    needs_group_input = True
                    logger.warning(f"Обнаружено {len(empty_groups)} записей с пустой группой")
                else:
                    logger.info("Все записи имеют непустые группы")
            else:
                # Если поле group отсутствует вообще
                needs_group_input = True
                logger.warning(f"Поле 'group' отсутствует в данных")
            
            logger.info(f"needs_group_input = {needs_group_input}")
            
            if needs_group_input:
                logger.warning("Возвращаем ошибку group_required")
                # Возвращаем ошибку, требующую ввода группы
                return jsonify({
                    'error': 'group_required',
                    'message': f'Поле группы отсутствует или пустое. Пожалуйста, выберите группу для {len(df)} записей.',
                    'total_records': len(df),
                    'empty_group_records': len(df)
                }), 400
            else:
                logger.info("Группа определена автоматически, продолжаем обработку")
        
        # Валидируем данные
        logger.info("Начинаем валидацию данных...")
        valid_df, addressless_df = data_processor.validate_data(df)
        logger.info(f"Валидация завершена: валидных записей {len(valid_df)}, без адреса {len(addressless_df)}")
        
        # Анализируем текст с выбранными методами
        if not valid_df.empty:
            logger.info("Начинаем анализ текста...")
            # Используем LLM анализатор для множественных методов
            analyzed_df = llm_analyzer.analyze_dataframe(valid_df, methods=analysis_methods)
            logger.info(f"Анализ завершен: {len(analyzed_df)} записей")
            
            # Получаем информацию о методах
            available_methods = llm_analyzer.available_methods
            used_methods = [m for m in analysis_methods if m in available_methods]
            if not used_methods:
                used_methods = ['classical']
            
            logger.info(f"Используемые методы: {used_methods}")
            
            # Сравниваем результаты методов
            comparison = llm_analyzer.compare_methods(analyzed_df, methods=used_methods)
            logger.info("Сравнение методов завершено")
            
            # Определяем основной метод для отображения
            primary_method = used_methods[0]
            analysis_method = f"Анализ с использованием методов: {', '.join(used_methods)}"
        
            # Проверяем и получаем координаты
            logger.info("Проверяем координаты...")
            coordinates_status = geocoder.get_coordinates_status(analyzed_df)
            if not coordinates_status['coordinates_exist']:
                logger.info("Выполняется геокодирование адресов...")
                analyzed_df = geocoder.process_dataframe(analyzed_df)
            else:
                logger.info("Координаты уже присутствуют в данных")
            
            # Обрабатываем районы
            logger.info("Обрабатываем районы...")
            analyzed_df = data_processor.process_districts(analyzed_df)
            
            # Сохраняем в архив
            logger.info("Сохраняем в архив...")
            success = data_processor.save_to_archive(analyzed_df)
            logger.info(f"Сохранение в архив: {'успешно' if success else 'ошибка'}")
            
            # Подготавливаем результаты для отображения
            display_columns = ['group', 'name', 'address', 'review_text', 'rating']
            # Добавляем колонки для каждого метода анализа
            for method in used_methods:
                display_columns.extend([f'{method}_sentiment', f'{method}_sentiment_score', f'{method}_review_type'])
            
            available_columns = [col for col in display_columns if col in analyzed_df.columns]
            logger.info(f"Колонки для отображения: {available_columns}")
            
            # Берем первые 10 записей для отображения
            display_df = convert_dataframe_for_json(analyzed_df[available_columns].head(10))
            display_data = display_df.to_dict('records')
            logger.info(f"Подготовлено {len(display_data)} записей для отображения")
            
            logger.info("=== ЗАВЕРШЕНИЕ ОБРАБОТКИ ФАЙЛА ===")
            
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
            logger.error("Нет валидных записей для обработки")
            return jsonify({'error': 'Нет валидных записей для обработки'}), 400
            
    except Exception as e:
        logger.error(f"Ошибка обработки: {str(e)}")
        import traceback
        logger.error(f"Полный traceback: {traceback.format_exc()}")
        return jsonify({'error': f'Ошибка обработки: {str(e)}'}), 500

@app.route('/archive/info')
def get_archive_info():
    """Получение информации об архивном файле"""
    try:
        archive_path = os.path.join('data', 'archives', 'processed_reviews.csv')
        
        if not os.path.exists(archive_path):
            return jsonify({
                'total_records': 0,
                'groups': {},
                'determined_groups': {},
                'date_range': {'min': None, 'max': None},
                'field_completeness': {}
            })
        
        df = pd.read_csv(archive_path, encoding='utf-8-sig')
        
        # Статистика по группам от поставщика
        groups = {}
        if 'group' in df.columns:
            group_counts = df['group'].value_counts()
            groups = {group: int(count) for group, count in group_counts.items() if pd.notna(group) and group != ''}
        
        # Статистика по определенным группам
        determined_groups = {}
        if 'determined_group' in df.columns:
            determined_group_counts = df['determined_group'].value_counts()
            determined_groups = {group: int(count) for group, count in determined_group_counts.items() if pd.notna(group) and group != ''}
        
        # Диапазон дат
        date_range = {'min': None, 'max': None}
        if 'date' in df.columns:
            valid_dates = pd.to_datetime(df['date'], errors='coerce')
            valid_dates = valid_dates.dropna()
            if len(valid_dates) > 0:
                date_range['min'] = valid_dates.min().strftime('%Y-%m-%d')
                date_range['max'] = valid_dates.max().strftime('%Y-%m-%d')
        
        # Заполненность полей
        field_completeness = {}
        important_fields = ['name', 'address', 'review_text', 'date', 'user_name', 'rating', 'answer_text']
        
        for field in important_fields:
            if field in df.columns:
                non_empty = df[field].notna() & (df[field] != '')
                completeness = int((non_empty.sum() / len(df)) * 100) if len(df) > 0 else 0
                field_completeness[field] = completeness
        
        return jsonify({
            'total_records': len(df),
            'groups': groups,  # Группы от поставщика
            'determined_groups': determined_groups,  # Определенные группы
            'date_range': date_range,
            'field_completeness': field_completeness
        })
        
    except Exception as e:
        logger.error(f"Ошибка получения информации об архиве: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/archive/clear', methods=['POST'])
def clear_archive():
    """Очистка архивного файла"""
    try:
        success = data_processor.clear_archive()
        if success:
            return jsonify({'success': True, 'message': 'Архив очищен'})
        else:
            return jsonify({'error': 'Ошибка очистки архива'}), 500
    except Exception as e:
        return jsonify({'error': f'Ошибка очистки: {str(e)}'}), 500

@app.route('/archive/download')
def download_archive():
    """Скачивание архивного файла"""
    try:
        archive_path = data_processor.archive_file
        if os.path.exists(archive_path):
            return send_file(archive_path, as_attachment=True, download_name='archive.csv')
        else:
            return jsonify({'error': 'Архивный файл не найден'}), 404
    except Exception as e:
        return jsonify({'error': f'Ошибка скачивания: {str(e)}'}), 500

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

@app.route('/map/data')
def get_map_data():
    """Получение данных для отображения на карте"""
    try:
        # Получаем параметры
        group_type = request.args.get('group_type', 'supplier')  # По умолчанию группы от поставщика
        filters = request.args.get('filters', '')  # Фильтры групп
        sentiment_method = request.args.get('sentiment_method', 'rating')  # Метод сентимента
        color_scheme = request.args.get('color_scheme', 'group')  # Схема цветов
        
        # Парсим фильтры
        active_filters = []
        if filters:
            active_filters = filters.split(',')
        
        archive_path = os.path.join('data', 'archives', 'processed_reviews.csv')
        if not os.path.exists(archive_path):
            return jsonify({'archive': [], 'new': []})

        df = pd.read_csv(archive_path, encoding='utf-8-sig')

        # Конвертируем для JSON
        df_converted = convert_dataframe_for_json(df)

        # Группируем по группам объектов
        archive_data = []
        
        if group_type == 'supplier':
            # Используем группы от поставщика
            group_field = 'group'
        else:
            # Используем определенные группы
            group_field = 'determined_group'
        
        # Обрабатываем записи с пустыми группами отдельно
        empty_group_data = df_converted[df_converted[group_field].isna() | (df_converted[group_field] == '')]
        if not empty_group_data.empty:
            # Фильтруем записи с координатами для отображения на карте
            points_with_coords = []
            for _, row in empty_group_data.iterrows():
                if (pd.notna(row.get('latitude')) and pd.notna(row.get('longitude')) and 
                    row.get('latitude', 0) != 0 and row.get('longitude', 0) != 0):
                    
                    # Определяем цвет точки
                    point_color = get_point_color(row, color_scheme, sentiment_method)
                    
                    points_with_coords.append({
                        'name': row.get('name', ''),
                        'address': row.get('address', ''),
                        'latitude': float(row.get('latitude', 0)),
                        'longitude': float(row.get('longitude', 0)),
                        'district': row.get('district', 'Неизвестный район'),
                        'group': row.get('group', ''),
                        'determined_group': row.get('determined_group', ''),
                        'color': point_color,
                        'sentiment': get_sentiment_value(row, sentiment_method)
                    })

            if points_with_coords:  # Добавляем группу только если есть объекты с координатами
                archive_data.append({
                    'group': 'unknown',  # Используем 'unknown' для пустых групп
                    'points': points_with_coords
                })
        
        # Обрабатываем записи с непустыми группами
        non_empty_group_data = df_converted[df_converted[group_field].notna() & (df_converted[group_field] != '')]
        
        # Применяем фильтры
        if active_filters:
            non_empty_group_data = non_empty_group_data[non_empty_group_data[group_field].isin(active_filters)]
        
        for group in non_empty_group_data[group_field].unique():
            group_data = non_empty_group_data[non_empty_group_data[group_field] == group]

            # Фильтруем записи с координатами для отображения на карте
            points_with_coords = []
            for _, row in group_data.iterrows():
                if (pd.notna(row.get('latitude')) and pd.notna(row.get('longitude')) and 
                    row.get('latitude', 0) != 0 and row.get('longitude', 0) != 0):
                    
                    # Определяем цвет точки
                    point_color = get_point_color(row, color_scheme, sentiment_method)
                    
                    points_with_coords.append({
                        'name': row.get('name', ''),
                        'address': row.get('address', ''),
                        'latitude': float(row.get('latitude', 0)),
                        'longitude': float(row.get('longitude', 0)),
                        'district': row.get('district', 'Неизвестный район'),
                        'group': row.get('group', ''),
                        'determined_group': row.get('determined_group', ''),
                        'color': point_color,
                        'sentiment': get_sentiment_value(row, sentiment_method)
                    })

            if points_with_coords:  # Добавляем группу только если есть объекты с координатами
                archive_data.append({
                    'group': group,
                    'points': points_with_coords
                })

        return jsonify({
            'archive': archive_data,
            'new': [],  # Новые объекты будут добавляться через отдельный маршрут
            'group_type': group_type,  # Возвращаем использованный тип групп
            'color_scheme': color_scheme,
            'sentiment_method': sentiment_method
        })

    except Exception as e:
        logger.error(f"Ошибка при получении данных карты: {e}")
        return jsonify({'error': str(e)}), 500

def get_point_color(row, color_scheme, sentiment_method):
    """Определяет цвет точки на карте"""
    from app.core.config import SENTIMENT_CONFIG, GROUP_CONFIG
    
    if color_scheme == 'sentiment':
        # Цвет по сентименту
        sentiment = get_sentiment_value(row, sentiment_method)
        return SENTIMENT_CONFIG['colors'].get(sentiment, '#6c757d')
    else:
        # Цвет по группе
        group = row.get('group', '')
        return GROUP_CONFIG['colors'].get(group, '#6c757d')

def get_sentiment_value(row, sentiment_method):
    """Получает значение сентимента для строки"""
    if sentiment_method == 'rating':
        # Используем преобразованный рейтинг
        rating = row.get('rating')
        if rating and pd.notna(rating):
            from app.core.config import SENTIMENT_CONFIG
            return SENTIMENT_CONFIG['rating_to_sentiment'].get(int(rating), 'удовлетворительно')
        return 'удовлетворительно'
    else:
        # Используем поле сентимента (если есть)
        sentiment_field = f'{sentiment_method}_sentiment'
        return row.get(sentiment_field, 'удовлетворительно')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000) 