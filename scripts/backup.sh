#!/usr/bin/env bash
# scripts/backup.sh — бэкап PostgreSQL и очистка старых данных (для cron)
#   cd ~/gifts/GiftTrackApp && bash scripts/backup.sh
# Cron раз в сутки (crontab -e):
#   17 4 * * * cd ~/gifts/GiftTrackApp && bash scripts/backup.sh >> backups/backup.log 2>&1
# Хранит последние KEEP копий (по умолчанию 14), чистит старые снимки флоров и историю (по умолчанию старше 90/180 дней).
set -euo pipefail
cd "$(dirname "$0")/.."
KEEP="${KEEP:-14}"
SNAPSHOT_DAYS="${SNAPSHOT_DAYS:-90}"
SALES_DAYS="${SALES_DAYS:-180}"
mkdir -p backups
STAMP="$(date +%Y%m%d-%H%M%S)"
OUT="backups/db-$STAMP.sql.gz"
docker compose exec -T postgres pg_dump -U gt -d gifttrack | gzip > "$OUT"
[ -s "$OUT" ] || { rm -f "$OUT"; echo "Пустой дамп — остановлено"; exit 1; }
echo "$(date -Is) бэкап: $OUT ($(du -h "$OUT" | cut -f1))"
# оставить последние KEEP копий
ls -1t backups/db-*.sql.gz 2>/dev/null | tail -n +"$((KEEP + 1))" | xargs -r rm -f
# очистка старых данных (после успешного бэкапа)
docker compose exec -T postgres psql -U gt -d gifttrack -v ON_ERROR_STOP=1 <<SQL
DELETE FROM floor_snapshots WHERE ts < now() - interval '${SNAPSHOT_DAYS} days';
DELETE FROM sales WHERE ts < now() - interval '${SALES_DAYS} days';
SQL
echo "$(date -Is) очистка выполнена (снимки флоров > ${SNAPSHOT_DAYS} дн., продажи > ${SALES_DAYS} дн.)"
