# Руководство по сохранению проекта на GitHub

## 📋 Предварительные требования

### 1. Установка Git
Если Git не установлен, скачайте и установите его:
- Скачайте с официального сайта: https://git-scm.com/download/win
- Или используйте winget: `winget install Git.Git`

### 2. Настройка GitHub
1. Создайте аккаунт на GitHub: https://github.com
2. Создайте новый репозиторий для проекта

## 🚀 Пошаговая инструкция

### Шаг 1: Инициализация Git репозитория
```bash
# В папке проекта
git init
```

### Шаг 2: Создание .gitignore файла
Создайте файл `.gitignore` в корне проекта:

```gitignore
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg
MANIFEST

# Virtual environments
venv/
env/
ENV/
env.bak/
venv.bak/

# IDE
.vscode/
.idea/
*.swp
*.swo
*~

# OS
.DS_Store
Thumbs.db

# Project specific
data/temp/*
data/archives/*.csv
*.log
.env
env_data.env

# Keep some data files
!data/archives/.gitkeep
!data/temp/.gitkeep
```

### Шаг 3: Добавление файлов в репозиторий
```bash
# Добавить все файлы
git add .

# Создать первый коммит
git commit -m "Initial commit: MVP Urban Analysis Application

- Flask web application for urban environment data analysis
- Support for JSON, CSV, and Excel file processing
- Dual group system: supplier groups and determined groups
- Interactive map with group type switching
- Modal dialog for manual group selection
- Archive management with completeness analysis
- Geocoding integration with Yandex Maps API"
```

### Шаг 4: Подключение к GitHub
```bash
# Добавить удаленный репозиторий (замените URL на ваш)
git remote add origin https://github.com/YOUR_USERNAME/urban-analysis-app.git

# Отправить код на GitHub
git push -u origin main
```

## 📁 Структура проекта для GitHub

```
urban-analysis-app/
├── mvp_urban_analysis/
│   ├── app/
│   │   ├── core/
│   │   │   ├── __init__.py
│   │   │   ├── csv_processor.py
│   │   │   ├── data_processor.py
│   │   │   ├── district_detector.py
│   │   │   ├── excel_processor.py
│   │   │   ├── geocoder.py
│   │   │   ├── json_processor.py
│   │   │   └── text_analyzer.py
│   │   ├── gui/
│   │   ├── utils/
│   │   └── visualization/
│   ├── data/
│   │   ├── archives/
│   │   ├── initial_data/
│   │   ├── results/
│   │   └── temp/
│   ├── static/
│   ├── templates/
│   │   └── index.html
│   ├── app.py
│   ├── requirements.txt
│   └── README.md
├── tests/
│   ├── test_complete_logic.py
│   ├── test_web_modal.py
│   └── test_map_group_types.py
├── docs/
│   ├── IMPLEMENTATION_SUMMARY.md
│   └── GITHUB_SETUP_GUIDE.md
├── .gitignore
└── README.md
```

## 📝 Создание README.md

Создайте файл `README.md` в корне проекта:

```markdown
# Urban Analysis Application

Веб-приложение для анализа данных городской среды с поддержкой обработки JSON, CSV и Excel файлов.

## 🚀 Возможности

- **Обработка файлов**: Поддержка JSON, CSV, Excel форматов
- **Двойная система групп**: Группы от поставщика и автоматически определяемые
- **Интерактивная карта**: Отображение объектов с переключением типов групп
- **Модальные окна**: Ручной выбор группы при её отсутствии
- **Управление архивом**: Сохранение, просмотр и анализ данных
- **Геокодирование**: Интеграция с Яндекс.Карты API

## 🛠 Установка

1. Клонируйте репозиторий:
```bash
git clone https://github.com/YOUR_USERNAME/urban-analysis-app.git
cd urban-analysis-app
```

2. Установите зависимости:
```bash
cd mvp_urban_analysis
pip install -r requirements.txt
```

3. Настройте переменные окружения:
```bash
cp env_example.txt .env
# Отредактируйте .env файл, добавив API ключи
```

4. Запустите приложение:
```bash
python app.py
```

5. Откройте браузер: http://localhost:5000

## 📊 Использование

1. **Загрузка файлов**: Перетащите JSON, CSV или Excel файл в область загрузки
2. **Выбор группы**: Если файл не содержит группу, выберите её в модальном окне
3. **Просмотр на карте**: Объекты отображаются на интерактивной карте
4. **Переключение групп**: Используйте переключатель для изменения типа группировки
5. **Архив**: Просматривайте статистику и управляйте сохраненными данными

## 🔧 Технические детали

- **Backend**: Flask (Python)
- **Frontend**: HTML, CSS, JavaScript, Bootstrap
- **Карты**: Яндекс.Карты API
- **Геокодирование**: Яндекс.Геокодер API
- **Анализ текста**: NLTK, VADER

## 📁 Структура проекта

- `app/core/` - Основные модули обработки данных
- `templates/` - HTML шаблоны
- `static/` - Статические файлы (CSS, JS)
- `data/` - Данные и архивы
- `tests/` - Тесты

## 🤝 Вклад в проект

1. Форкните репозиторий
2. Создайте ветку для новой функции
3. Внесите изменения
4. Создайте Pull Request

## 📄 Лицензия

MIT License
```

## 🔄 Дополнительные команды

### Создание релиза
```bash
# Создать тег для версии
git tag -a v1.0.0 -m "Version 1.0.0: MVP with dual group system"

# Отправить тег на GitHub
git push origin v1.0.0
```

### Обновление кода
```bash
# Добавить изменения
git add .

# Создать коммит
git commit -m "Описание изменений"

# Отправить на GitHub
git push origin main
```

## ⚠️ Важные моменты

1. **Не коммитьте конфиденциальные данные** (API ключи, пароли)
2. **Используйте .env файл** для хранения секретов
3. **Добавьте .gitkeep файлы** в пустые папки для их сохранения
4. **Создайте ветки** для новых функций
5. **Пишите понятные сообщения коммитов**

## 📞 Поддержка

При возникновении проблем создайте Issue в репозитории GitHub. 