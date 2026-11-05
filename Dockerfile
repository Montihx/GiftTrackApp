# Единый образ для всех python-сервисов
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Сначала зависимости (кэш слоёв), код копируется после
COPY pyproject.toml README.md ./
RUN python - <<'PY'
import tomllib, subprocess, sys
deps = tomllib.load(open("pyproject.toml", "rb"))["project"]["dependencies"]
subprocess.check_call([sys.executable, "-m", "pip", "install", *deps])
PY

COPY . .

RUN useradd -m app && chown -R app:app /app
USER app

CMD ["python", "-m", "bot.bot_main"]
