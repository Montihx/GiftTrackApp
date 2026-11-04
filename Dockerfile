# /workspace/Dockerfile — единый образ для всех python-сервисов
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Сначала манифест зависимостей — для кэширования слоёв
COPY pyproject.toml README.md ./
RUN pip install --upgrade pip && pip install -e . || pip install .

COPY . .

# Непривилегированный пользователь
RUN useradd -m app && chown -R app:app /app
USER app

CMD ["python", "-m", "bot.main"]
