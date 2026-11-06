// ItemPage.jsx — карточка предмета: атрибуты с редкостью, владелец, цена листинга
import { useEffect, useState } from "react";
import { apiGet, apiRequest, fmtTon, shortAddr, tgApp } from "./api";
import { goBack, navigate } from "./router";
import { CollectionIcon, EmptyState, RarityBadge, Skeleton } from "./ui";

const ATTR_LABELS = { model: "Модель", background: "Фон", symbol: "Символ", pattern: "Узор" };

function openProfile(url) {
  const tg = tgApp();
  if (tg && tg.openTelegramLink) tg.openTelegramLink(url);
  else window.open(url, "_blank");
}

export default function ItemPage({ address }) {
  const [item, setItem] = useState(null);
  const [error, setError] = useState("");
  const [isFav, setIsFav] = useState(false);
  const [toast, setToast] = useState("");

  useEffect(() => {
    let alive = true;
    setItem(null);
    setError("");
    apiGet(`/items/${encodeURIComponent(address)}`)
      .then((d) => alive && setItem(d))
      .catch((e) => {
        if (!alive) return;
        if (e.status === 404) setError("Предмет не найден");
        else if (e.status === 401) setError("Откройте приложение внутри Telegram");
        else setError("Не удалось загрузить предмет");
      });
    apiGet("/favorites")
      .then((f) => alive && setIsFav((f.items || []).some((i) => i.item_address === address)))
      .catch(() => {});
    return () => {
      alive = false;
    };
  }, [address]);

  const toggleFav = async () => {
    const next = !isFav;
    setIsFav(next);
    try {
      if (next) await apiRequest("POST", "/favorites", { item_address: address });
      else await apiRequest("DELETE", `/favorites?item_address=${encodeURIComponent(address)}`);
    } catch (e) {
      setIsFav(!next);
      setToast(e.status === 401 ? "Откройте приложение внутри Telegram" : "Не удалось обновить избранное");
    }
  };

  if (error) {
    return (
      <div>
        <button className="link-btn" onClick={goBack}>‹ Назад</button>
        <EmptyState icon="⚠️" title={error} />
      </div>
    );
  }

  if (!item) {
    return (
      <div>
        <button className="link-btn" onClick={goBack}>‹ Назад</button>
        <Skeleton h={90} r={14} />
        <div style={{ height: 12 }} />
        <Skeleton h={180} r={14} />
      </div>
    );
  }

  const coll = item.collection;
  const owner = item.owner || {};

  return (
    <div>
      <div className="back-row"><button className="link-btn" onClick={goBack}>‹ Назад</button></div>
      {toast && <div className="toast" onClick={() => setToast("")}>{toast}</div>}

      <div className="hero">
        {coll ? <CollectionIcon url={coll.icon_url} name={coll.name} size={52} /> : null}
        <div className="hero-main">
          <div className="hero-name">{coll ? `${coll.name} #${item.index}` : shortAddr(item.address)}</div>
          <div className="hero-floor"><b>{fmtTon(item.listing_price)}</b></div>
          <div className="row-sub">{item.listing_price == null ? "Не выставлен на продажу" : "Цена листинга"}</div>
        </div>
        <button className={`star ${isFav ? "on" : ""}`} onClick={toggleFav} aria-label="Избранное">
          {isFav ? "★" : "☆"}
        </button>
      </div>

      {coll && (
        <button className="btn ghost block" style={{ marginBottom: 12 }} onClick={() => navigate(`c/${encodeURIComponent(coll.id)}`)}>
          К коллекции {coll.name}
        </button>
      )}

      <div className="section">
        <div className="section-title"><span>Атрибуты</span></div>
        <div className="panel">
          {item.attributes.length === 0 ? (
            <div className="spark-empty">Атрибуты ещё не загружены</div>
          ) : (
            <div className="rows">
              {item.attributes.map((a) => (
                <div className="row-item" key={a.type}>
                  <div className="row-main">
                    <div className="row-sub">{ATTR_LABELS[a.type] || a.type}</div>
                    <div>{a.value}</div>
                  </div>
                  <RarityBadge value={a.rarity} />
                </div>
              ))}
            </div>
          )}
        </div>
      </div>

      <div className="section">
        <div className="section-title"><span>Владелец</span></div>
        <div className="panel">
          <div className="row-sub" style={{ marginBottom: 10 }}>{owner.ton ? shortAddr(owner.ton) : "Неизвестен"}</div>
          {owner.profile_url ? (
            <button className="btn block" onClick={() => openProfile(owner.profile_url)}>Написать владельцу</button>
          ) : owner.locked ? (
            <button className="btn gold block" onClick={() => navigate("sub")}>Ссылка на владельца — по подписке</button>
          ) : (
            <div className="row-sub">Контакт владельца не найден</div>
          )}
        </div>
      </div>
    </div>
  );
}
