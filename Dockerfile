FROM python:3.11-slim

WORKDIR /app

# Установка системных зависимостей
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Копирование requirements.txt и установка зависимостей
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование кода приложения
COPY server.py .
COPY uploads/ /app/uploads/

# Создание директорий для загрузок
RUN mkdir -p /app/uploads/temp /app/uploads/avatars

# Открытие порта
EXPOSE 8000

# Запуск приложения
CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8000"]
