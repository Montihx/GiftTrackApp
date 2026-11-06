#!/usr/bin/env bash
# scripts/update_server.sh — обновление GiftTrackApp на сервере одной командой
#   cd ~/gifts/GiftTrackApp
#   git fetch origin main && git show origin/main:scripts/update_server.sh > /tmp/gt_update.sh && bash /tmp/gt_update.sh
# (скрипт берётся из свежего main, а копии и проверка правок выполняются ДО обновления кода)
# Что делает: бэкап .env и базы -> git pull main (остановится, если есть локальные правки) ->
# дописывает новые настройки (ничего не удаляет) -> спрашивает недостающие ключи (ввод скрыт) ->
# сборка и запуск (миграции — сервис migrate) -> вход Telegram по QR -> проверка логов.
# Подходит и для очень старой версии на сервере: данные в docker-томах сохраняются.
set -euo pipefail
cd "$(git rev-parse --show-toplevel 2>/dev/null || { echo "Запустите из папки проекта (cd ~/gifts/GiftTrackApp)"; exit 1; })"

say() { printf '\n\033[1;36m== %s\033[0m\n' "$*"; }
trap 'echo; echo "Скрипт остановился на ошибке (строка $LINENO). Ничего не удалено; .env сохранён в .env.bak.*"; echo "Если мешают локальные правки: git status — и пришлите вывод."' ERR

[ -f .env ] || { echo "Нет .env: скопируйте .env.example в .env и заполните BOT_TOKEN, DB_PASS и т.д."; exit 1; }

STAMP="$(date +%Y%m%d-%H%M%S)"
say "1/7 Резервная копия .env"
cp .env ".env.bak.$STAMP"   # .env.bak* в .gitignore
echo "  .env.bak.$STAMP"

say "2/7 Резервная копия базы (до миграций)"
mkdir -p backups
DUMP="backups/db-$STAMP.sql.gz"
pg=""
if docker compose ps --status running --services 2>/dev/null | grep -qx postgres; then
  pg="compose"
elif docker ps --format '{{.ID}} {{.Image}}' | grep -q ' postgres'; then
  pg="$(docker ps --format '{{.ID}} {{.Image}}' | grep ' postgres' | head -n1 | cut -d' ' -f1)"
fi
if [ "$pg" = "compose" ]; then
  { docker compose exec -T postgres pg_dump -U gt -d gifttrack | gzip > "$DUMP"; } || rm -f "$DUMP"
elif [ -n "$pg" ]; then
  { docker exec -i "$pg" pg_dump -U gt -d gifttrack | gzip > "$DUMP"; } || rm -f "$DUMP"
fi
if [ -s "$DUMP" ]; then
  echo "  $DUMP ($(du -h "$DUMP" | cut -f1)). Восстановление при необходимости:"
  echo "  gunzip -c $DUMP | docker compose exec -T postgres psql -U gt -d gifttrack"
else
  rm -f "$DUMP"
  read -r -p "  Postgres не найден или копия пустая. Продолжить без копии базы? [y/N] " yn
  [ "$yn" = "y" ] || { echo "  Остановлено."; exit 1; }
fi

say "3/7 Обновление кода (main)"
dirty="$(git status --porcelain --untracked-files=no)"
if [ -n "$dirty" ]; then
  echo "  На сервере изменены файлы проекта — обновление их бы перезаписало:"
  echo "$dirty" | sed 's/^/    /'
  echo "  Ничего не трогаю. Пришлите этот список — подскажу, как сохранить правки."
  exit 1
fi
git fetch origin main
git checkout main
git pull --ff-only origin main

say "4/7 Новые настройки в .env (существующие значения не трогаются)"
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
# прежнее значение по умолчанию TONAPI_RPS=1 давало частые 429 на free-тарифе — поднять до нового (своё не трогаем)
if grep -qx 'TONAPI_RPS=1' .env; then
  sed -i 's/^TONAPI_RPS=1$/TONAPI_RPS=0.8/' .env && echo "  TONAPI_RPS: 1 -> 0.8"
fi

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
say "5/7 Ключи (ввод не отображается)"
ask_secret TONAPI_KEY "ключ tonconsole.com"
ask_secret API_ID "my.telegram.org -> API development tools -> App api_id"
ask_secret API_HASH "там же, App api_hash"
ask_secret PHONE "номер этого Telegram-аккаунта, +7..."

say "6/7 Сборка и запуск (по одному сервису — бережёт память; миграции применит migrate)"
COMPOSE_PARALLEL_LIMIT=1 docker compose build
docker compose up -d --remove-orphans   # контейнеры сервисов из старой версии, которых больше нет, — убрать
docker compose ps

say "7/7 Вход в Telegram для маркета подарков и резолвера"
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
echo "--- migrate (ожидается: «Running upgrade … -> 0004» или без ошибок)"
docker compose logs --tail=40 migrate | grep -E "upgrade|Error|error" | tail -n 6 || true
say "Готово. Пришлите эти строки (без ключей) для проверки."
