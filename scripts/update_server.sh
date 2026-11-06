#!/usr/bin/env bash
# scripts/update_server.sh — обновление GiftTrackApp на сервере одной командой
#   cd ~/gifts/GiftTrackApp && bash scripts/update_server.sh
# Что делает: бэкап .env -> git pull main -> дописывает новые настройки (ничего не удаляет) ->
# спрашивает недостающие ключи (ввод скрыт) -> сборка и запуск -> вход Telegram по QR -> проверка логов.
set -euo pipefail
cd "$(dirname "$0")/.."

say() { printf '\n\033[1;36m== %s\033[0m\n' "$*"; }
trap 'echo; echo "Скрипт остановился на ошибке (строка $LINENO). Ничего не удалено; .env сохранён в .env.bak.*"; echo "Если мешают локальные правки: git status — и пришлите вывод."' ERR

[ -f .env ] || { echo "Нет .env: скопируйте .env.example в .env и заполните BOT_TOKEN, DB_PASS и т.д."; exit 1; }

say "1/6 Резервная копия .env"
cp .env ".env.bak.$(date +%Y%m%d-%H%M%S)"   # *.bak в .gitignore

say "2/6 Обновление кода (main)"
git fetch origin main
git checkout main
git pull --ff-only origin main

say "3/6 Новые настройки в .env (существующие значения не трогаются)"
added=0
while IFS= read -r line; do
  case "$line" in ''|'#'*) continue ;; esac
  key="${line%%=*}"
  if ! grep -q "^${key}=" .env; then
    case "$key" in
      TONAPI_KEY|API_ID|API_HASH|PHONE|CRYPTOBOT_TOKEN|BOT_TOKEN|DB_PASS) continue ;;  # секреты — ниже
    esac
    printf '%s\n' "$line" >> .env; echo "  + $line"; added=$((added + 1))
  fi
done < .env.example
echo "  добавлено настроек: $added"

ask_secret() {  # ask_secret KEY "подсказка" — спросить, если пусто или нет в .env
  local key="$1" hint="$2" cur val
  cur="$(grep -E "^${key}=" .env | tail -n1 | cut -d= -f2- || true)"
  case "$cur" in ''|00000000|your_api_hash|+79990000000) ;; *) echo "  $key: уже задан"; return ;; esac
  read -r -s -p "  $key ($hint, Enter — пропустить): " val; echo
  [ -z "$val" ] && { echo "  $key: пропущен"; return; }
  if grep -q "^${key}=" .env; then
    python3 - "$key" "$val" <<'PY'
import sys, pathlib
key, val = sys.argv[1], sys.argv[2]
p = pathlib.Path(".env")
lines = [f"{key}={val}" if l.startswith(f"{key}=") else l for l in p.read_text().splitlines()]
p.write_text("\n".join(lines) + "\n")
PY
  else
    printf '%s=%s\n' "$key" "$val" >> .env
  fi
  echo "  $key: сохранён"
}
say "4/6 Ключи (ввод не отображается)"
ask_secret TONAPI_KEY "ключ tonconsole.com"
ask_secret API_ID "my.telegram.org -> API development tools -> App api_id"
ask_secret API_HASH "там же, App api_hash"
ask_secret PHONE "номер этого Telegram-аккаунта, +7..."

say "5/6 Сборка и запуск (по одному сервису — бережёт память)"
COMPOSE_PARALLEL_LIMIT=1 docker compose build
docker compose up -d
docker compose ps

say "6/6 Вход в Telegram для маркета подарков и резолвера"
if docker compose run --rm -T resolver python - <<'PY'
import asyncio, sys
from bot.resolver_main import _make_client
from core.config import get_settings
async def main():
    s = get_settings()
    if not (s.api_id and s.api_hash):
        sys.exit(2)
    c = _make_client(s); await c.connect(); ok = await c.is_user_authorized(); await c.disconnect()
    sys.exit(0 if ok else 1)
asyncio.run(main())
PY
then
  echo "  Telegram уже авторизован."
else
  code=$?
  if [ "$code" = 2 ]; then
    echo "  API_ID/API_HASH не заданы — вход пропущен (маркет Telegram и резолвер будут простаивать)."
  else
    echo "  Откройте Telegram на телефоне: Настройки -> Устройства -> Подключить устройство и отсканируйте QR."
    docker compose run --rm -it resolver python -m bot.resolver_main --login-qr
    docker compose restart resolver
  fi
fi

say "Проверка (подождите ~1 мин и посмотрите строки ниже)"
sleep 45
echo "--- listener-market (ожидается: «подключён к потоку переводов NFT»)"
docker compose logs --tail=200 listener-market | grep -E "tonapi_stream|market_listener|tonapi_sync" | tail -n 5 || true
echo "--- resolver (ожидается: «Telethon worker запущен», «[tg_market] каталог: N коллекций»)"
docker compose logs --tail=200 resolver | grep -E "Telethon|tg_market" | tail -n 5 || true
echo "--- migrate"
docker compose logs --tail=20 migrate | tail -n 3 || true
say "Готово. Пришлите эти строки (без ключей) для проверки."
