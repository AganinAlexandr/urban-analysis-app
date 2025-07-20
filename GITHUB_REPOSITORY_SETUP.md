# Инструкция по созданию репозитория на GitHub

## Шаг 1: Создание репозитория на GitHub

1. Перейдите на https://github.com
2. Войдите в свой аккаунт
3. Нажмите кнопку "New" или "+" → "New repository"
4. Заполните форму:
   - **Repository name**: `urban-analysis-app`
   - **Description**: `Веб-приложение для анализа данных городской среды`
   - **Visibility**: Public (или Private по вашему выбору)
   - **НЕ ставьте галочки** на "Add a README file", "Add .gitignore", "Choose a license" (у нас уже есть эти файлы)
5. Нажмите "Create repository"

## Шаг 2: Подключение локального репозитория к GitHub

После создания репозитория GitHub покажет инструкции. Выполните следующие команды:

```bash
# Добавить удаленный репозиторий (замените YOUR_USERNAME на ваше имя пользователя)
git remote add origin https://github.com/YOUR_USERNAME/urban-analysis-app.git

# Переименовать ветку в main (современный стандарт)
git branch -M main

# Отправить код на GitHub
git push -u origin main
```

## Шаг 3: Проверка

После выполнения команд:
1. Обновите страницу репозитория на GitHub
2. Убедитесь, что все файлы загружены
3. Проверьте, что README.md отображается корректно

## Альтернативный способ (если есть проблемы с HTTPS)

Если возникают проблемы с аутентификацией, можно использовать SSH:

```bash
# Создать SSH ключ (если еще нет)
ssh-keygen -t ed25519 -C "your_email@example.com"

# Добавить ключ в SSH агент
eval "$(ssh-agent -s)"
ssh-add ~/.ssh/id_ed25519

# Скопировать публичный ключ и добавить в GitHub
cat ~/.ssh/id_ed25519.pub
# Скопируйте вывод и добавьте в GitHub Settings → SSH and GPG keys

# Использовать SSH URL вместо HTTPS
git remote add origin git@github.com:YOUR_USERNAME/urban-analysis-app.git
git push -u origin main
```

## Структура проекта на GitHub

После загрузки на GitHub структура будет выглядеть так:

```
urban-analysis-app/
├── mvp_urban_analysis/          # Основное приложение
│   ├── app/
│   │   └── core/               # Модули обработки данных
│   ├── templates/
│   │   └── index.html          # Главная страница
│   ├── data/
│   │   ├── archives/           # Архивные данные
│   │   └── temp/               # Временные файлы
│   ├── static/                 # Статические файлы
│   ├── app.py                  # Главный файл приложения
│   └── requirements.txt        # Зависимости
├── tests/                      # Тестовые файлы
├── docs/                       # Документация
├── .gitignore                  # Исключения Git
└── README.md                   # Описание проекта
```

## Дополнительные настройки

### Создание релиза
```bash
# Создать тег для версии
git tag -a v1.0.0 -m "Version 1.0.0: MVP with dual group system"

# Отправить тег на GitHub
git push origin v1.0.0
```

### Настройка GitHub Pages (опционально)
1. Перейдите в Settings репозитория
2. Найдите раздел "Pages"
3. Выберите источник "Deploy from a branch"
4. Выберите ветку "main" и папку "/ (root)"
5. Нажмите "Save"

## Полезные команды

```bash
# Проверить статус
git status

# Посмотреть историю коммитов
git log --oneline

# Добавить изменения
git add .
git commit -m "Описание изменений"

# Отправить изменения на GitHub
git push origin main

# Создать новую ветку для функции
git checkout -b feature/new-function
git push -u origin feature/new-function
```

## Решение проблем

### Если git push не работает:
1. Проверьте, что вы вошли в GitHub в браузере
2. Попробуйте использовать Personal Access Token
3. Или настройте SSH ключи

### Если файлы не загружаются:
1. Проверьте .gitignore файл
2. Убедитесь, что файлы добавлены в git add
3. Проверьте размер файлов (GitHub имеет ограничения)

### Если README не отображается:
1. Убедитесь, что файл называется именно README.md
2. Проверьте синтаксис Markdown
3. Подождите несколько минут (GitHub может обновляться с задержкой) 