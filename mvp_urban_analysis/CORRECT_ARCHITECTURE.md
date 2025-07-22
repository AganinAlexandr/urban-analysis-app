# Правильная архитектура базы данных

## Обзор

Исправленная архитектура базы данных использует правильные связи **один-ко-многим** между таблицами, что соответствует вашим требованиям.

## Схема базы данных (6 таблиц)

### 1. `objects` - Объекты
```sql
CREATE TABLE objects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    address TEXT NOT NULL,
    object_key TEXT UNIQUE NOT NULL,
    latitude REAL,
    longitude REAL,
    district TEXT,
    group_id INTEGER,           -- Ссылка на ОДНУ группу
    detected_group_id INTEGER,  -- Ссылка на ОДНУ определяемую группу
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (group_id) REFERENCES object_groups(id),
    FOREIGN KEY (detected_group_id) REFERENCES detected_groups(id)
);
```

### 2. `object_groups` - Группы объектов
```sql
CREATE TABLE object_groups (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_name TEXT NOT NULL,
    group_type TEXT NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 3. `detected_groups` - Определяемые группы
```sql
CREATE TABLE detected_groups (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_name TEXT NOT NULL,
    group_type TEXT NOT NULL,
    detection_method TEXT,
    confidence REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 4. `reviews` - Отзывы
```sql
CREATE TABLE reviews (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    object_id INTEGER NOT NULL,
    review_text TEXT,
    rating INTEGER CHECK (rating >= 1 AND rating <= 5),
    review_date TIMESTAMP,
    source TEXT,
    external_id TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (object_id) REFERENCES objects(id)
);
```

### 5. `processing_methods` - Методы обработки
```sql
CREATE TABLE processing_methods (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    method_name TEXT UNIQUE NOT NULL,
    description TEXT,
    is_active BOOLEAN DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 6. `analysis_results` - Результаты анализа
```sql
CREATE TABLE analysis_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    review_id INTEGER NOT NULL,
    method_id INTEGER NOT NULL,
    sentiment TEXT NOT NULL CHECK (sentiment IN ('positive', 'negative', 'neutral')),
    confidence REAL CHECK (confidence >= 0 AND confidence <= 1),
    review_type TEXT CHECK (review_type IN ('gratitude', 'complaint', 'suggestion', 'informational')),
    keywords TEXT,
    topics TEXT,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (review_id) REFERENCES reviews(id),
    FOREIGN KEY (method_id) REFERENCES processing_methods(id),
    UNIQUE(review_id, method_id)
);
```

## Связи между таблицами

```
object_groups (1) ←→ (many) objects
detected_groups (1) ←→ (many) objects
objects (1) ←→ (many) reviews
reviews (1) ←→ (many) analysis_results
processing_methods (1) ←→ (many) analysis_results
```

## Ключевые особенности

### 1. Правильные связи один-ко-многим
- **Один объект** → **одна группа** (через `group_id`)
- **Один объект** → **одна определяемая группа** (через `detected_group_id`)
- **Один объект** → **много отзывов** (через `object_id` в `reviews`)
- **Один отзыв** → **много результатов анализа** (через `review_id` в `analysis_results`)

### 2. Уникальные ключи объектов
```python
def create_object_key(self, name: str, address: str) -> str:
    combined = f"{name}|{address}".lower().strip()
    return hashlib.md5(combined.encode()).hexdigest()
```

### 3. Автоматическое связывание с группами
```python
# При создании объекта автоматически связываем с группой
group_id = self.get_group_id(group_type)
detected_group_id = self.get_detected_group_id(detected_group_type)
```

## Преимущества исправленной архитектуры

### 1. Простота
- Нет избыточных таблиц связей
- Прямые внешние ключи
- Понятная структура данных

### 2. Производительность
- Меньше JOIN операций
- Простые запросы
- Эффективные индексы

### 3. Соответствие требованиям
- Один объект → одна группа
- Один объект → одна определяемая группа
- Много отзывов на объект
- Много результатов анализа на отзыв

## Примеры использования

### Создание объекта с группой
```python
object_id = db_manager.insert_object(
    name="Городская больница №1",
    address="ул. Ленина, 10",
    group_type="hospitals",
    detected_group_type="hospitals"
)
```

### Получение объекта с группами
```sql
SELECT 
    o.name,
    og.group_name,
    dg.group_name as detected_group_name
FROM objects o
LEFT JOIN object_groups og ON o.group_id = og.id
LEFT JOIN detected_groups dg ON o.detected_group_id = dg.id
```

### Экспорт данных с группами
```python
df = db_manager.export_to_dataframe(include_analysis=True)
# Результат содержит: group_name, group_type, detected_group_name, detected_group_type
```

## Тестирование

Запуск тестов исправленной БД:
```bash
python test_database_fixed.py
```

Результаты тестирования:
- ✅ Создание и инициализация БД
- ✅ Миграция данных с группами
- ✅ Экспорт данных с группами
- ✅ Проверка связей между таблицами
- ✅ Распределение сентиментов

## Миграция с предыдущей версии

Для перехода на исправленную архитектуру:

1. **Создать новую БД** с исправленной схемой
2. **Экспортировать данные** из старой БД
3. **Импортировать в новую БД** с правильными связями
4. **Обновить код** для использования исправленного модуля

## Заключение

Исправленная архитектура полностью соответствует вашим требованиям:
- ✅ 6 таблиц вместо 8
- ✅ Правильные связи один-ко-многим
- ✅ Один объект → одна группа
- ✅ Один объект → одна определяемая группа
- ✅ Простота и производительность
- ✅ Полная функциональность

Архитектура готова к использованию! 🚀 