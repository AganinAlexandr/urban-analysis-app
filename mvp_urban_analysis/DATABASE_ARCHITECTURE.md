# Архитектура базы данных

## Обзор

Система анализа городской среды теперь использует реляционную базу данных SQLite для хранения и управления данными. Это обеспечивает лучшую структурированность, целостность данных и производительность.

## Схема базы данных

### Основные таблицы

#### 1. `objects` - Объекты
Хранит информацию об объектах городской инфраструктуры.

```sql
CREATE TABLE objects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,                    -- Название объекта
    address TEXT NOT NULL,                 -- Адрес объекта
    object_key TEXT UNIQUE NOT NULL,       -- Уникальный ключ (хеш name+address)
    latitude REAL,                         -- Широта
    longitude REAL,                        -- Долгота
    district TEXT,                         -- Район
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### 2. `reviews` - Отзывы
Хранит отзывы пользователей об объектах.

```sql
CREATE TABLE reviews (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    object_id INTEGER NOT NULL,            -- Ссылка на объект
    review_text TEXT,                      -- Текст отзыва
    rating INTEGER CHECK (rating >= 1 AND rating <= 5), -- Рейтинг 1-5
    review_date TIMESTAMP,                 -- Дата отзыва
    source TEXT,                           -- Источник данных (yandex, 2gis, etc.)
    external_id TEXT,                      -- ID из внешней системы
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (object_id) REFERENCES objects(id)
);
```

#### 3. `processing_methods` - Методы обработки
Справочник методов анализа сентимента.

```sql
CREATE TABLE processing_methods (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    method_name TEXT UNIQUE NOT NULL,      -- Название метода
    description TEXT,                      -- Описание метода
    is_active BOOLEAN DEFAULT 1,          -- Активен ли метод
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### 4. `analysis_results` - Результаты анализа
Хранит результаты анализа отзывов различными методами.

```sql
CREATE TABLE analysis_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    review_id INTEGER NOT NULL,            -- Ссылка на отзыв
    method_id INTEGER NOT NULL,            -- Ссылка на метод
    sentiment TEXT NOT NULL CHECK (sentiment IN ('positive', 'negative', 'neutral')),
    confidence REAL CHECK (confidence >= 0 AND confidence <= 1), -- Уверенность
    review_type TEXT CHECK (review_type IN ('gratitude', 'complaint', 'suggestion', 'informational')),
    keywords TEXT,                         -- JSON массив ключевых слов
    topics TEXT,                           -- JSON массив тем
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (review_id) REFERENCES reviews(id),
    FOREIGN KEY (method_id) REFERENCES processing_methods(id),
    UNIQUE(review_id, method_id)          -- Один результат на метод для отзыва
);
```

### Таблицы групп

#### 5. `object_groups` - Группы объектов
Справочник групп объектов (больницы, школы, etc.).

```sql
CREATE TABLE object_groups (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_name TEXT NOT NULL,              -- Название группы
    group_type TEXT NOT NULL,              -- Тип группы (hospitals, schools, etc.)
    description TEXT,                      -- Описание группы
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### 6. `object_group_mapping` - Связь объектов с группами
Связывает объекты с группами (many-to-many).

```sql
CREATE TABLE object_group_mapping (
    object_id INTEGER NOT NULL,
    group_id INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (object_id) REFERENCES objects(id),
    FOREIGN KEY (group_id) REFERENCES object_groups(id),
    PRIMARY KEY (object_id, group_id)
);
```

### Таблицы для определяемых групп

#### 7. `detected_groups` - Определяемые группы
Группы, определяемые алгоритмами.

```sql
CREATE TABLE detected_groups (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_name TEXT NOT NULL,
    group_type TEXT NOT NULL,
    detection_method TEXT,                 -- Алгоритм определения группы
    confidence REAL,                       -- Уверенность определения
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### 8. `object_detected_group_mapping` - Связь с определяемыми группами
Связывает объекты с определяемыми группами.

```sql
CREATE TABLE object_detected_group_mapping (
    object_id INTEGER NOT NULL,
    detected_group_id INTEGER NOT NULL,
    confidence REAL,                       -- Уверенность связи
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (object_id) REFERENCES objects(id),
    FOREIGN KEY (detected_group_id) REFERENCES detected_groups(id),
    PRIMARY KEY (object_id, detected_group_id)
);
```

## Связи между таблицами

```
objects (1) ←→ (many) reviews
objects (many) ←→ (many) object_groups (через object_group_mapping)
objects (many) ←→ (many) detected_groups (через object_detected_group_mapping)
reviews (1) ←→ (many) analysis_results
processing_methods (1) ←→ (many) analysis_results
```

## Преимущества новой архитектуры

### 1. Нормализация данных
- Устранение дублирования данных
- Целостность связей между таблицами
- Эффективное использование памяти

### 2. Масштабируемость
- Легкое добавление новых методов анализа
- Поддержка множественных источников данных
- Гибкая система группировки объектов

### 3. Производительность
- Индексы на часто используемых полях
- Оптимизированные запросы
- Кэширование результатов

### 4. Целостность данных
- Внешние ключи обеспечивают консистентность
- Проверки ограничений
- Транзакционность операций

## Миграция данных

### Из CSV в базу данных
```python
from app.core.data_migrator import data_migrator

# Миграция CSV файла
stats = data_migrator.migrate_csv_file('data.csv', 'yandex')

# Миграция Excel файла
stats = data_migrator.migrate_excel_file('data.xlsx', source='2gis')

# Миграция всех файлов из директории
results = data_migrator.migrate_directory('data/initial_data/')
```

### Экспорт из базы данных
```python
from app.core.database import db_manager

# Экспорт с результатами анализа
df = db_manager.export_to_dataframe(include_analysis=True)

# Экспорт только основных данных
df = db_manager.export_to_dataframe(include_analysis=False)
```

## API эндпоинты

### Статистика базы данных
```
GET /database/stats
```

### Миграция данных
```
POST /database/migrate
{
    "data_type": "current" | "archive" | "file"
}
```

### Экспорт данных
```
POST /database/export
{
    "include_analysis": true | false
}
```

### Валидация базы данных
```
GET /database/validate
```

### Распределение сентиментов
```
GET /database/sentiment_distribution?method=nlp_vader
```

## Тестирование

Запуск тестов базы данных:
```bash
python test_database.py
```

Тесты проверяют:
- Создание и инициализацию БД
- Миграцию данных
- Экспорт данных
- Распределение сентиментов
- Валидацию целостности

## Совместимость

Новая архитектура полностью совместима с существующим функционалом:
- Все существующие API эндпоинты продолжают работать
- Поддержка загрузки файлов через веб-интерфейс
- Отображение данных на карте
- Анализ сентимента

Архивные данные сохраняются и доступны через кнопку "Архив". 