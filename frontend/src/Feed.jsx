// Feed.jsx — лента сигналов: последние алерты пользователя (апгрейды / листинги / продажи)
import { useEffect, useState } from "react";
import { apiGet, fmtTon } from "./api";

const KINDS = [
  { id: "upgrade", label: "Апгрейды" },
  { id: "listing", label: "Листинги" },
  { id: "sale", label: "Продажи" },
];
const TAG = { upgrade: "Апгрейд", listing: "Листинг", sale: "Продажа" };

function ago(ts) {
  const s = Math.max(0, Math.round((Date.now() - new Date(ts).getTime()) / 1000));
  if (s < 60) return `${s} с назад`;
  if (s < 3600) return `${Math.floor(s / 60)} мин назад`;
  if (s < 86400) return `${Math.floor(s / 3600)} ч назад`;
  return `${Math.floor(s / 86400)} дн назад`;
}

function SignalCard({ c }) {
  const owner = c.owner;
  const ownerUrl = owner ? (owner.username ? `https://t.me/${owner.username}` : `tg://user?id=${owner.tg_user_id}`) : null;
  const attrs = c.attrs || {};
  return (
    <div className={`signal ${c.kind}`}>
      <div className="signal-head">
        <div className="signal-title">{c.title}</div>
        {c.price != null && <div className="signal-price">{fmtTon(c.price)}</div>}
      </div>
      <div className="signal-attrs">
        {[attrs.model, attrs.background, attrs.symbol].filter(Boolean).join(" · ") || "—"}
      </div>
      <div className="signal-tags">
        <span className={`tag tag-${c.kind}`}>{TAG[c.kind]}</span>
        {c.market && <span className="tag">{c.market}</span>}
        <span className="tag tag-time">{ago(c.ts)}</span>
      </div>
      {(ownerUrl || c.link) && (
        <div className="signal-actions">
          {ownerUrl && <a className="btn-link primary" href={ownerUrl}>💬 Написать владельцу</a>}
          {c.link && <a className="btn-link" href={c.link}>🎁 Подарок</a>}
        </div>
      )}
    </div>
  );
}

export default function Feed() {
  const [kind, setKind] = useState("upgrade");
  const [cards, setCards] = useState(null);
  const [error, setError] = useState(false);

  useEffect(() => {
    let alive = true;
    const load = async () => {
      try {
        const res = await apiGet(`/feed?kind=${kind}`);
        if (alive) {
          setCards(res.cards || []);
          setError(false);
        }
      } catch {
        if (alive) setError(true);
      }
    };
    setCards(null);
    load();
    const t = setInterval(load, 8000); // без SSE: обновляем раз в 8 секунд
    return () => {
      alive = false;
      clearInterval(t);
    };
  }, [kind]);

  return (
    <div>
      <h1 className="page-title">Лента</h1>
      <div className="chips">
        {KINDS.map((k) => (
          <button key={k.id} className={kind === k.id ? "on" : ""} onClick={() => setKind(k.id)}>{k.label}</button>
        ))}
      </div>
      {error && <div className="hint">Нет связи с сервером. Данные могут быть устаревшими.</div>}
      {cards === null && !error && <div className="empty">Загрузка…</div>}
      {cards && cards.length === 0 && (
        <div className="empty">Сигналов пока нет. Они появятся здесь, когда придёт алерт по вашим фильтрам.</div>
      )}
      {cards && cards.map((c, i) => <SignalCard key={`${c.ts}-${c.title}-${i}`} c={c} />)}
    </div>
  );
}
