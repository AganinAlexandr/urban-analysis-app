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

# Инициализируем менеджер выборок
sample_manager = SampleManager()

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
            
            # Сохраняем в БД
            logger.info("Сохраняем в БД...")
            try:
                # Импортируем db_manager_fixed
                from app.core.database_fixed import db_manager_fixed
                
                # Сохраняем данные в БД
                success = db_manager_fixed.migrate_csv_to_database(analyzed_df, source="upload")
                logger.info(f"Сохранение в БД: {'успешно' if success else 'ошибка'}")
                
                # Архив больше не используется - все данные только в БД
                
            except Exception as e:
                logger.error(f"Ошибка сохранения в БД: {e}")
                success = False
            
            # Подготавливаем результаты для отображения
            display_columns = ['group', 'name', 'determined_group', 'address', 'review_text', 'rating']
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
        
        return jsonify({
            'total_records': len(df),
            'groups': groups,  # Группы от поставщика
            'determined_groups': determined_groups,  # Определенные группы
            'date_range': date_range,
            'field_completeness': field_completeness
        })
        
    except Exception as e:
        logger.error(f"Ошибка получения информации о данных: {e}")
        return jsonify({'error': str(e)}), 500

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
        return jsonify({'error': str(e)}), 500

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
                return jsonify({'error': 'Нет данных для миграции'}), 400
                
        elif data_type == 'archive':
            # Мигрируем архивные данные
            if hasattr(app, 'archive_data') and app.archive_data is not None:
                stats = db_manager_fixed.migrate_csv_to_database(app.archive_data, 'archive_data')
                return jsonify({
                    'success': True,
                    'message': 'Архивные данные успешно мигрированы в базу данных',
                    'stats': stats
                })
            else:
                return jsonify({'error': 'Нет архивных данных для миграции'}), 400
        
        else:
            return jsonify({'error': 'Неизвестный тип данных'}), 400
            
    except Exception as e:
        logger.error(f"Ошибка при миграции в БД: {e}")
        return jsonify({'error': str(e)}), 500

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
        return jsonify({'error': str(e)}), 500

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
        return jsonify({'error': str(e)}), 500

@app.route('/database/sentiment_distribution')
def get_sentiment_distribution():
    """Получение распределения сентиментов из базы данных"""
    try:
        method_name = request.args.get('method')
        df = db_manager_fixed.get_sentiment_distribution(method_name)
        
        return jsonify({
            'success': True,
            'data': df.to_dict('records')
        })
    except Exception as e:
        logger.error(f"Ошибка при получении распределения сентиментов: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/map/data')
def get_map_data():
    """Получение данных для отображения на карте"""
    try:
        # Получаем параметры
        group_type = request.args.get('group_type', 'supplier')  # По умолчанию группы от поставщика
        filters = request.args.get('filters', '')  # Фильтры групп
        sentiment_method = request.args.get('sentiment_method', 'rating')  # Метод сентимента
        color_scheme = request.args.get('color_scheme', 'group')  # Схема цветов
        data_source = request.args.get('data_source', 'database')  # Источник данных

        # Парсим фильтры
        active_filters = [f for f in filters.split(',') if f] if filters else []

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
        print("Уникальные group_type:", df_converted['group_type'].unique() if 'group_type' in df_converted.columns else 'нет поля')
        print("Уникальные detected_group_type:", df_converted['detected_group_type'].unique() if 'detected_group_type' in df_converted.columns else 'нет поля')
        
        # Диагностика: выводим доступные поля для сентимента
        sentiment_fields = [col for col in df_converted.columns if 'sentiment' in col.lower()]
        print("Поля сентимента в данных:", sentiment_fields)
        print("Пример данных для первого объекта:", df_converted.iloc[0].to_dict() if not df_converted.empty else "Нет данных")
        
        # Диагностика: выводим распределение сентиментов для nlp_vader
        if 'nlp_vader_sentiment' in df_converted.columns:
            vader_sentiments = df_converted['nlp_vader_sentiment'].value_counts()
            print("Распределение сентиментов nlp_vader:", vader_sentiments.to_dict())
        
        # Диагностика: выводим распределение сентиментов для user_rating
        if 'user_rating_sentiment' in df_converted.columns:
            rating_sentiments = df_converted['user_rating_sentiment'].value_counts()
            print("Распределение сентиментов user_rating:", rating_sentiments.to_dict())

        # Выбираем поле группировки
        if group_type == 'supplier':
            group_field = 'group_type'
        else:
            group_field = 'detected_group_type'
        if group_field not in df_converted.columns:
            # fallback: берем первое поле, содержащее 'group' в названии
            group_fields = [col for col in df_converted.columns if 'group' in col.lower()]
            group_field = group_fields[0] if group_fields else None
            if not group_field:
                return jsonify({'archive': [], 'new': []})

        # Фильтруем только объекты с валидными координатами
        coords_df = df_converted[
            df_converted['latitude'].notna() & df_converted['longitude'].notna() &
            (df_converted['latitude'] != '') & (df_converted['longitude'] != '') &
            (df_converted['latitude'] != 0) & (df_converted['longitude'] != 0)
        ]
        
        # Удаляем дубликаты по координатам, оставляя первую запись
        coords_df = coords_df.drop_duplicates(subset=['latitude', 'longitude'], keep='first')

        # Разделяем на объекты с группой и без группы
        # Убираем дубликаты по координатам для корректного отображения
        coords_df = coords_df.drop_duplicates(subset=['latitude', 'longitude'], keep='first')
        
        has_group = coords_df[group_field].notna() & (coords_df[group_field] != '') & (coords_df[group_field] != 'None')
        with_group = coords_df[has_group]
        without_group = coords_df[~has_group]

        archive_data = []
        
        # Проверяем, есть ли активные фильтры
        if active_filters and len(active_filters) > 0:
            # Если фильтры заданы — показываем только объекты этих групп
            filtered = with_group[with_group[group_field].isin(active_filters)]
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
                        'group': row.get('group_type', row.get('group', '')),
                        'determined_group': row.get('detected_group_type', row.get('determined_group', '')),
                        'color': point_color,
                        'sentiment': get_sentiment_value(row, sentiment_method)
                    })
                if points:
                    archive_data.append({'group': group, 'points': points})
        else:
            # Если фильтры не заданы — НЕ показываем объекты (скрываем все)
            # archive_data остается пустым списком
            pass

        return jsonify({
            'archive': archive_data,
            'new': [],
            'group_type': group_type,
            'color_scheme': color_scheme,
            'sentiment_method': sentiment_method
        })

    except Exception as e:
        logger.error(f"Ошибка при получении данных карты: {e}")
        return jsonify({'error': str(e)}), 500

def get_point_color(row, color_scheme, sentiment_method, group_type='supplier'):
    """Определяет цвет точки на карте"""
    from app.core.config import SENTIMENT_CONFIG, GROUP_CONFIG
    
    if color_scheme == 'sentiment':
        # Цвет по сентименту
        sentiment = get_sentiment_value(row, sentiment_method)
        return SENTIMENT_CONFIG['colors'].get(sentiment, '#6c757d')
    else:
        # Цвет по группе
        if group_type == 'determined':
            # В режиме "Определенные" используем detected_group_type для цвета
            group = row.get('detected_group_type', row.get('determined_group', ''))
        else:
            # В режиме "От поставщика" используем group_type для цвета
            group = row.get('group_type', row.get('group', ''))
        return GROUP_CONFIG['colors'].get(group, '#6c757d')

def get_sentiment_value(row, sentiment_method):
    """Получает значение сентимента для строки"""
    import logging
    logger = logging.getLogger(__name__)
    
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
            base_query += f" AND og.group_name IN ({placeholders})"
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
            positive_data = [];
            neutral_data = [];
            negative_data = [];
            
            for method in methods_list:
                if method in review_data['methods']:
                    method_data = review_data['methods'][method]
                    if method_data['positive'] == 1:
                        positive_data.push(1);
                        neutral_data.push(0);
                        negative_data.push(0);
                    elif method_data['negative'] == 1:
                        positive_data.push(0);
                        neutral_data.push(0);
                        negative_data.push(1);
                    else:
                        positive_data.push(0);
                        neutral_data.push(1);
                        negative_data.push(0);
                else:
                    positive_data.push(0);
                    neutral_data.push(0);
                    negative_data.push(0);
            
            positive_series['data'].push(positive_data);
            neutral_series['data'].push(neutral_data);
            negative_series['data'].push(negative_data);
        
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
        
        logger.info(f"Запрос данных таблицы с параметрами: filters={active_filters}, group_type={group_type}, color_scheme={color_scheme}, sentiment_method={sentiment_method}")
        
        # Подключаемся к БД
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        # Базовый запрос для получения объектов с учетом фильтров
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
        else:
            # Используем определенные группы
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
            if group_type == 'supplier':
                placeholders = ','.join(['?' for _ in active_filters])
                base_query += f" AND og.group_name IN ({placeholders})"
            else:
                placeholders = ','.join(['?' for _ in active_filters])
                base_query += f" AND dg.group_name IN ({placeholders})"
            params.extend(active_filters)
        
        cursor.execute(base_query, params)
        filtered_objects = cursor.fetchall()
        
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
            review_info = {
                'id': review_id,
                'object_name': review_data['object_name'],
                'rating': review_data['rating']
            }
            table_data['reviews'].append(review_info)
            
            # Создаем строку сентиментов для этого отзыва
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
        
        logger.info(f"Запрос данных корреляции с параметрами: filters={active_filters}, group_type={group_type}, color_scheme={color_scheme}, sentiment_method={sentiment_method}")
        
        # Подключение к базе данных
        conn = sqlite3.connect('urban_analysis_fixed.db')
        cursor = conn.cursor()
        
        # Фильтрация объектов
        if group_type == 'supplier':
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
        else:  # group_type == 'determined'
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
        
        # Выполняем запрос с параметрами
        if active_filters and len(active_filters) > 0:
            cursor.execute(base_query, active_filters)
        else:
            cursor.execute(base_query)
        
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
                   pm.method_name, ar.sentiment, ar.confidence
            FROM reviews r
            JOIN analysis_results ar ON r.id = ar.review_id
            JOIN processing_methods pm ON ar.method_id = pm.id
            WHERE r.object_id IN ({placeholders})
            ORDER BY r.id, pm.method_name
        """
        
        cursor.execute(query, object_ids)
        results = cursor.fetchall()
        
        # Группируем данные по отзывам
        reviews_data = {}
        for row in results:
            review_id, object_id, review_text, rating, method_name, sentiment, confidence = row
            
            if review_id not in reviews_data:
                reviews_data[review_id] = {
                    'object_id': object_id,
                    'review_text': review_text,
                    'rating': rating,
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
        
        # Создаем матрицу данных для корреляции
        # Каждая строка - отзыв, каждый столбец - метод
        correlation_data = []
        for review_id, review_data in reviews_data.items():
            row = []
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
            correlation_data.append(row)
        
        # Рассчитываем корреляционную матрицу
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
        
    except Exception as e:
        logger.error(f"Ошибка при получении данных корреляции: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        })

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000) 