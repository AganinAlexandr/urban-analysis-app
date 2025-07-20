# Быстрое сохранение на GitHub

## 🚀 Быстрые шаги

### 1. Создайте репозиторий на GitHub
- Перейдите на https://github.com
- Нажмите "New repository"
- Название: `urban-analysis-app`
- НЕ ставьте галочки на README, .gitignore, license
- Нажмите "Create repository"

### 2. Выполните команды в PowerShell

```powershell
# Добавить Git в PATH (если еще не добавлен)
$env:PATH += ";C:\Program Files\Git\bin"

# Переименовать ветку в main
git branch -M main

# Добавить удаленный репозиторий (замените YOUR_USERNAME)
git remote add origin https://github.com/YOUR_USERNAME/urban-analysis-app.git

# Отправить код на GitHub
git push -u origin main
```

### 3. Проверьте результат
- Обновите страницу репозитория на GitHub
- Убедитесь, что все файлы загружены

## 📁 Что будет загружено

✅ **Основное приложение**:
- `mvp_urban_analysis/app.py` - главный файл
- `mvp_urban_analysis/app/core/` - модули обработки
- `mvp_urban_analysis/templates/` - HTML шаблоны
- `mvp_urban_analysis/static/` - статические файлы

✅ **Документация**:
- `README.md` - описание проекта
- `docs/` - подробная документация
- `.gitignore` - исключения Git

✅ **Тесты**:
- `tests/` - тестовые файлы

❌ **НЕ будут загружены**:
- Временные файлы (`data/temp/*`)
- Архивные данные (`data/archives/*.csv`)
- Конфиденциальные данные (`.env`)

## 🔧 Если возникли проблемы

### Проблема с аутентификацией:
1. Создайте Personal Access Token на GitHub
2. Используйте токен вместо пароля при push

### Проблема с большими файлами:
1. Проверьте размер файлов
2. Убедитесь, что большие файлы в .gitignore

### Проблема с кодировкой:
1. Это нормально для Windows (LF → CRLF)
2. Git автоматически обработает

## 📞 Поддержка

Если что-то не работает, проверьте:
1. Git установлен и добавлен в PATH
2. Вы вошли в GitHub в браузере
3. Репозиторий создан на GitHub
4. URL репозитория правильный 