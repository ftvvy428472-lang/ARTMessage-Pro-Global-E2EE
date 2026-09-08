# Используем официальный образ Python 3.10
FROM python:3.10-slim

# Устанавливаем рабочую директорию
WORKDIR /app

# Устанавливаем системные зависимости
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Копируем файлы зависимостей
COPY requirements.txt .

# Устанавливаем Python зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код приложения
COPY server.py .

# Создаем директории для загрузок
RUN mkdir -p uploads/temp uploads/avatars

# Открываем порт
EXPOSE 8000

# Запускаем приложение
CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8000"]
