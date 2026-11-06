// api.js — обёртки над fetch + форматтеры для витрины
export const API = "/api";

export function tgApp() {
  return window.Telegram?.WebApp || null;
}

export function initDataHeader() {
  const tg = tgApp();
  return tg && tg.initData ? { "X-Telegram-Init-Data": tg.initData } : {};
}

export async function apiRequest(method, path, body) {
  const headers = { ...initDataHeader() };
  const init = { method, headers };
  if (body !== undefined) {
    headers["Content-Type"] = "application/json";
    init.body = JSON.stringify(body);
  }
  const res = await fetch(`${API}${path}`, init);
  if (!res.ok) {
    const err = new Error(`HTTP ${res.status}`);
    err.status = res.status;
    throw err;
  }
  return res.json();
}

export const apiGet = (path) => apiRequest("GET", path);

// Кэш GET-ответов на время сессии: повторное открытие экрана показывает данные мгновенно, а свежие
// подтягиваются в фоне (stale-while-revalidate). cached — что уже есть (или null), promise — свежий ответ.
const _cache = new Map();
export function cachedGet(path, ttlMs = 60000) {
  const hit = _cache.get(path);
  const fresh = hit && Date.now() - hit.t < ttlMs;
  const promise = fresh
    ? Promise.resolve(hit.data)
    : apiGet(path).then((data) => {
        _cache.set(path, { t: Date.now(), data });
        return data;
      });
  return { cached: hit ? hit.data : null, promise };
}

// Название нативной монеты (TonAPI и GetGems называют её GRAM; было TON) — меняется VITE_COIN при сборке
export const COIN = import.meta.env.VITE_COIN || "GRAM";

// Цена в монете; null -> «—» (источник цен не подключен или листингов нет)
export function fmtTon(v) {
  if (v == null) return "—";
  const n = Number(v);
  return `${n.toLocaleString("ru-RU", { maximumFractionDigits: n >= 100 ? 0 : 2 })} ${COIN}`;
}

export function fmtChange(v) {
  if (v == null) return "—";
  return `${v > 0 ? "+" : ""}${Number(v).toFixed(2)}%`;
}

export function changeClass(v) {
  if (v == null || v === 0) return "flat";
  return v > 0 ? "up" : "down";
}

// Сервер может отдать время без таймзоны (UTC) — нормализуем
export function parseTs(iso) {
  if (!iso) return NaN;
  const s = /[zZ]$|[+-]\d\d:?\d\d$/.test(iso) ? iso : `${iso}Z`;
  return new Date(s).getTime();
}

export function timeAgo(iso) {
  const t = parseTs(iso);
  if (Number.isNaN(t)) return "";
  const sec = Math.max(0, (Date.now() - t) / 1000);
  if (sec < 60) return "только что";
  if (sec < 3600) return `${Math.floor(sec / 60)} мин назад`;
  if (sec < 86400) return `${Math.floor(sec / 3600)} ч назад`;
  return `${Math.floor(sec / 86400)} дн назад`;
}

export function shortAddr(a) {
  return a && a.length > 12 ? `${a.slice(0, 6)}…${a.slice(-4)}` : a || "";
}

// Чем ниже процент, тем реже трейт
export function rarityClass(r) {
  if (r == null) return "none";
  if (r <= 1) return "leg";
  if (r <= 5) return "epic";
  if (r <= 15) return "rare";
  return "common";
}
