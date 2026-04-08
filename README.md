# Тестовое на позицию Data Engineer

Реализовать Python-скрипт, который:

- загружает данные из <https://jsonplaceholder.typicode.com> (`users`, `posts`, `comments`)
- сохраняет их в **SQLite**
- поддерживает повторный запуск без дублирования данных

## Запуск

Склонировать репозиторий

```bash
git clone https://github.com/vponomareva/interview-task.git
```

Создать виртуальную среду:

```bash
python -m venv venv
```

Активировать ее:

```bash
source venv/Scripts/activate
```

Установаить зависимости:

```bash
pip install -r requirements.txt
```

Запуск:

```bash
python save_data.py
```
