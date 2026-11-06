// Favorites.jsx — вкладка «Избранное»: коллекции и предметы
import { useCallback, useEffect, useState } from "react";
import { apiGet, shortAddr } from "./api";
import { CollectionCard } from "./Home.jsx";
import { navigate } from "./router";
import { CollectionIcon, EmptyState, Skeleton } from "./ui";

export default function Favorites() {
  const [data, setData] = useState(null);
  const [error, setError] = useState("");

  const load = useCallback(() => {
    setError("");
    setData(null);
    apiGet("/favorites")
      .then(setData)
      .catch((e) => setError(e.status === 401 ? "Откройте приложение внутри Telegram" : "Не удалось загрузить избранное"));
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  return (
    <div>
      <h1 className="page-title">Избранное</h1>

      {error && (
        <EmptyState icon="⚠️" title={error}>
          <button className="btn" onClick={load}>Повторить</button>
        </EmptyState>
      )}

      {!error && data === null && (
        <div className="grid">
          <Skeleton h={120} r={14} />
          <Skeleton h={120} r={14} />
        </div>
      )}

      {!error && data !== null && data.collections.length === 0 && data.items.length === 0 && (
        <EmptyState icon="☆" title="Пока пусто" text="Нажмите ☆ на странице коллекции или предмета, чтобы добавить сюда." />
      )}

      {!error && data !== null && data.collections.length > 0 && (
        <div className="section">
          <div className="section-title"><span>Коллекции</span></div>
          <div className="grid">
            {data.collections.map((c) => (
              <CollectionCard
                key={c.id}
                c={{ ...c, items_count: 0 }}
                onOpen={() => navigate(`c/${encodeURIComponent(c.id)}`)}
              />
            ))}
          </div>
        </div>
      )}

      {!error && data !== null && data.items.length > 0 && (
        <div className="section">
          <div className="section-title"><span>Предметы</span></div>
          <div className="panel rows">
            {data.items.map((i) => (
              <button className="row-item" key={i.item_address} onClick={() => navigate(`i/${encodeURIComponent(i.item_address)}`)}>
                <CollectionIcon url={i.icon_url} name={i.collection_name || "?"} size={32} />
                <div className="row-main">
                  <div>{i.name || shortAddr(i.item_address)}</div>
                  {i.model ? <div className="row-sub">{i.model}</div> : null}
                </div>
                <span className="row-sub">›</span>
              </button>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
