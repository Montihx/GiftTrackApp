// Subscription.jsx — экран подписки: статус, тарифы, преимущества, оплата через инвойс CryptoBot
import { useEffect, useState } from "react";
import { apiGet, apiRequest, tgApp } from "./api";
import { EmptyState, Skeleton } from "./ui";

const OWNER = import.meta.env.VITE_OWNER_USERNAME || "";

const BENEFITS = [
  { icon: "⚡", title: "Уведомления по фильтрам", text: "Листинги, подходящие под ваши фильтры, приходят в бот" },
  { icon: "🔎", title: "Точный поиск владельца", text: "Показываем ссылку на владельца подарка" },
];

function openUrl(url) {
  const tg = tgApp();
  if (tg && /^https:\/\/t\.me\//.test(url) && tg.openTelegramLink) tg.openTelegramLink(url);
  else if (tg && tg.openLink) tg.openLink(url);
  else window.open(url, "_blank");
}

export default function Subscription() {
  const [sub, setSub] = useState(null);
  const [error, setError] = useState("");
  const [plan, setPlan] = useState("month1");
  const [busy, setBusy] = useState(false);
  const [msg, setMsg] = useState("");

  useEffect(() => {
    apiGet("/subscription")
      .then(setSub)
      .catch((e) => setError(e.status === 401 ? "Откройте приложение внутри Telegram" : "Не удалось загрузить подписку"));
  }, []);

  const pay = async () => {
    setBusy(true);
    setMsg("");
    try {
      const res = await apiRequest("POST", "/subscription/invoice", { plan_id: plan });
      openUrl(res.pay_url);
    } catch (e) {
      setMsg(e.status === 503 ? "Оплата пока не подключена" : "Не удалось создать счёт");
    } finally {
      setBusy(false);
    }
  };

  if (error) return <EmptyState icon="⚠️" title={error} />;
  if (!sub) {
    return (
      <div>
        <Skeleton h={150} r={22} />
        <div style={{ height: 12 }} />
        <Skeleton h={90} r={16} />
      </div>
    );
  }

  const expires = sub.expires_at ? new Date(sub.expires_at).toLocaleDateString("ru-RU") : "";

  return (
    <div>
      <div className="sub-hero">
        <div className="sub-badge">💎</div>
        <h1>Подписка</h1>
        <p>Премиум-доступ к уведомлениям и данным о владельцах подарков.</p>
        <div className="sub-status">
          {sub.is_active ? `Premium активен до ${expires}` : "Подписка не активна"}
        </div>
      </div>

      <div className="section-title"><span>Выберите тариф</span></div>
      <div className="plans">
        {sub.plans.map((p) => (
          <button key={p.id} className={`plan ${plan === p.id ? "on" : ""}`} onClick={() => setPlan(p.id)}>
            <div className="plan-title">{p.title}</div>
            <div className="plan-price">{p.price_usd}$</div>
            <div className="plan-days">/ {p.days} дней</div>
          </button>
        ))}
      </div>

      <div className="section-title"><span>Преимущества</span></div>
      {BENEFITS.map((b) => (
        <div className="benefit" key={b.title}>
          <div className="benefit-ico">{b.icon}</div>
          <div>
            <div className="benefit-title">{b.title}</div>
            <div className="benefit-text">{b.text}</div>
          </div>
        </div>
      ))}

      {msg && <div className="toast" style={{ marginTop: 10 }}>{msg}</div>}
      <button className="btn gold block" style={{ marginTop: 12 }} onClick={pay} disabled={busy}>
        {busy ? "Создаём счёт…" : sub.is_active ? "Продлить" : "Оформить"}
      </button>

      {OWNER && (
        <div className="panel" style={{ marginTop: 14, textAlign: "center" }}>
          <div className="benefit-title">Не получается оплатить?</div>
          <div className="benefit-text" style={{ margin: "4px 0 10px" }}>Свяжитесь с владельцем для получения доступа.</div>
          <button className="btn ghost" onClick={() => openUrl(`https://t.me/${OWNER}`)}>Написать владельцу</button>
        </div>
      )}
    </div>
  );
}
