-- Исправленная схема базы данных для системы анализа городской среды
-- SQLite - правильные связи один-ко-многим

-- Таблица объектов
CREATE TABLE IF NOT EXISTS objects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    address TEXT NOT NULL,
    object_key TEXT UNIQUE NOT NULL, -- хеш от name + address
    latitude REAL,
    longitude REAL,
    district TEXT,
    group_id INTEGER,           -- Ссылка на одну группу
    detected_group_id INTEGER,  -- Ссылка на одну определяемую группу
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (group_id) REFERENCES object_groups(id),
    FOREIGN KEY (detected_group_id) REFERENCES detected_groups(id)
);

-- Таблица групп объектов (один) → Объекты (многие)
CREATE TABLE IF NOT EXISTS object_groups (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_name TEXT NOT NULL,
    group_type TEXT NOT NULL, -- hospitals, schools, kindergartens, etc.
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица определяемых групп (один) → Объекты (многие)
CREATE TABLE IF NOT EXISTS detected_groups (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_name TEXT NOT NULL,
    group_type TEXT NOT NULL,
    detection_method TEXT, -- алгоритм определения группы
    confidence REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица отзывов (один) → Объекты (многие)
CREATE TABLE IF NOT EXISTS reviews (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    object_id INTEGER NOT NULL,
    review_text TEXT,
    rating INTEGER CHECK (rating >= 1 AND rating <= 5),
    review_date TIMESTAMP,
    source TEXT, -- yandex, 2gis, etc.
    external_id TEXT, -- ID из внешней системы
    в_Выборке TEXT DEFAULT NULL, -- Поле для отметки включения в выборку
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (object_id) REFERENCES objects(id)
);

-- Таблица методов обработки (один) → Результаты анализа (многие)
CREATE TABLE IF NOT EXISTS processing_methods (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    method_name TEXT UNIQUE NOT NULL,
    description TEXT,
    is_active BOOLEAN DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица результатов анализа (один) → Отзывы (многие)
CREATE TABLE IF NOT EXISTS analysis_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    review_id INTEGER NOT NULL,
    method_id INTEGER NOT NULL,
    sentiment TEXT NOT NULL CHECK (sentiment IN ('positive', 'negative', 'neutral')),
    confidence REAL CHECK (confidence >= 0 AND confidence <= 1),
    review_type TEXT CHECK (review_type IN ('gratitude', 'complaint', 'suggestion', 'informational')),
    keywords TEXT, -- JSON массив ключевых слов
    topics TEXT, -- JSON массив тем
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (review_id) REFERENCES reviews(id),
    FOREIGN KEY (method_id) REFERENCES processing_methods(id),
    UNIQUE(review_id, method_id)
);

-- Индексы для производительности
CREATE INDEX IF NOT EXISTS idx_objects_object_key ON objects(object_key);
CREATE INDEX IF NOT EXISTS idx_objects_group_id ON objects(group_id);
CREATE INDEX IF NOT EXISTS idx_objects_detected_group_id ON objects(detected_group_id);
CREATE INDEX IF NOT EXISTS idx_reviews_object_id ON reviews(object_id);
CREATE INDEX IF NOT EXISTS idx_reviews_source ON reviews(source);
CREATE INDEX IF NOT EXISTS idx_analysis_results_review_id ON analysis_results(review_id);
CREATE INDEX IF NOT EXISTS idx_analysis_results_method_id ON analysis_results(method_id);
CREATE INDEX IF NOT EXISTS idx_analysis_results_sentiment ON analysis_results(sentiment);

-- Вставка базовых методов обработки
INSERT OR IGNORE INTO processing_methods (method_name, description) VALUES
('user_rating', 'Преобразование пользовательского рейтинга в сентимент'),
('nlp_vader', 'Анализ сентимента с помощью VADER'),
('llm_yandex', 'Анализ сентимента с помощью Yandex LLM'),
('llm_openai', 'Анализ сентимента с помощью OpenAI'),
('textblob', 'Анализ сентимента с помощью TextBlob'),
('transformers', 'Анализ сентимента с помощью Transformers'),
('custom_rule_based', 'Правило-базированный анализ'),
('ensemble', 'Ансамблевый метод');

-- Вставка базовых групп объектов
INSERT OR IGNORE INTO object_groups (group_name, group_type, description) VALUES
('Больницы', 'hospitals', 'Медицинские учреждения'),
('Школы', 'schools', 'Образовательные учреждения'),
('Детские сады', 'kindergartens', 'Дошкольные учреждения'),
('Поликлиники', 'polyclinics', 'Амбулаторные медицинские учреждения'),
('Аптеки', 'pharmacies', 'Фармацевтические учреждения'),
('Торговые центры', 'shopping_malls', 'Торговые комплексы'),
('Университеты', 'universities', 'Высшие учебные заведения'); 