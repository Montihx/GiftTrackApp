// CollectionPage.jsx — страница коллекции: шапка, график, топ-атрибуты, лента продаж, live-флор по SSE
import { useEffect, useState } from "react";
import { API, apiGet, apiRequest, fmtTon, shortAddr, timeAgo } from "./api";
import { goBack, navigate } from "./router";
import Sparkline from "./Sparkline.jsx";
import { ChangeBadge, CollectionIcon, EmptyState, RarityBadge, Skeleton } from "./ui";

const PAGE = 20;
const RANGES = [
  { id: "7d", label: "7д" },
  { id: "30d", label: "30д" },
  { id: "all", label: "Всё" },
];
const ATTR_TABS = [
  { id: "models", label: "Модели" },
  { id: "backgrounds", label: "Фоны" },
  { id: "symbols", label: "Символы" },
  { id: "patterns", label: "Узоры" },
];

export default function CollectionPage({ id }) {
  const [detail, setDetail] = useState(null);
  const [error, setError] = useState("");
  const [range, setRange] = useState("7d");
  const [history, setHistory] = useState(null);
  const [sales, setSales] = useState(null);
  const [loadingMore, setLoadingMore] = useState(false);
  const [live, setLive] = useState(null);
  const [isFav, setIsFav] = useState(false);
  const [attrTab, setAttrTab] = useState("models");
  const [toast, setToast] = useState("");

  useEffect(() => {
    let alive = true;
    setDetail(null);
    setError("");
    setSales(null);
    setLive(null);
    apiGet(`/collections/${id}`)
      .then((d) => alive && setDetail(d))
      .catch((e) => alive && setError(e.status === 404 ? "Коллекция не найдена" : "Не удалось загрузить коллекцию"));
    apiGet(`/collections/${id}/sales?limit=${PAGE}&offset=0`)
      .then((s) => alive && setSales(s))
      .catch(() => alive && setSales({ total: 0, items: [] }));
    apiGet("/favorites")
      .then((f) => alive && setIsFav((f.collections || []).some((c) => c.id === id)))
      .catch(() => {});
    return () => {
      alive = false;
    };
  }, [id]);

  useEffect(() => {
    let alive = true;
    setHistory(null);
    apiGet(`/collections/${id}/floor-history?range=${range}`)
      .then((h) => alive && setHistory(h.points || []))
      .catch(() => alive && setHistory([]));
    return () => {
      alive = false;
    };
  }, [id, range]);

  // live-флор: SSE-стрим сервера (floor_price + floor_24h_change)
  useEffect(() => {
    if (typeof EventSource === "undefined") return undefined;
    const es = new EventSource(`${API}/collections/${id}/stream`);
    es.onmessage = (ev) => {
      try {
        setLive(JSON.parse(ev.data));
      } catch {
        /* битый пакет — игнорируем */
      }
    };
    es.onerror = () => es.close();
    return () => es.close();
  }, [id]);

  const toggleFav = async () => {
    const next = !isFav;
    setIsFav(next);
    try {
      if (next) await apiRequest("POST", "/favorites", { collection_id: id });
      else await apiRequest("DELETE", `/favorites?collection_id=${encodeURIComponent(id)}`);
    } catch (e) {
      setIsFav(!next);
      setToast(e.status === 401 ? "Откройте приложение внутри Telegram" : "Не удалось обновить избранное");
    }
  };

  const loadMore = async () => {
    if (!sales) return;
    setLoadingMore(true);
    try {
      const more = await apiGet(`/collections/${id}/sales?limit=${PAGE}&offset=${sales.items.length}`);
      setSales({ total: more.total, items: [...sales.items, ...more.items] });
    } catch {
      setToast("Не удалось загрузить продажи");
    } finally {
      setLoadingMore(false);
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

  if (!detail) {
    return (
      <div>
        <button className="link-btn" onClick={goBack}>‹ Назад</button>
        <div className="hero"><Skeleton h={52} w={52} r={26} /><div className="hero-main"><Skeleton h={18} w="60%" /><div style={{ height: 8 }} /><Skeleton h={22} w="40%" /></div></div>
        <Skeleton h={160} r={14} />
      </div>
    );
  }

  const onchainFloor = live && live.floor_price != null ? live.floor_price : detail.floor_price;
  // ончейн-флор в разы ниже маркетов — вероятно нерабочий листинг: в шапке показываем лучший достоверный флор
  const floor = detail.onchain_suspect && detail.best_floor != null ? detail.best_floor : onchainFloor;
  const change = live && live.floor_24h_change != null ? live.floor_24h_change : detail.floor_24h_change;
  const attrs = (detail.top_attributes && detail.top_attributes[attrTab]) || [];
  // у Telegram-подарков нет узоров — пустые вкладки не показываем (кроме активной/первой)
  const tabs = ATTR_TABS.filter(
    (t) => t.id === "models" || t.id === attrTab || ((detail.top_attributes || {})[t.id] || []).length > 0,
  );

  return (
    <div>
      <div className="back-row"><button className="link-btn" onClick={goBack}>‹ Назад</button></div>
      {toast && <div className="toast" onClick={() => setToast("")}>{toast}</div>}

      <div className="hero">
        <CollectionIcon url={detail.icon_url} name={detail.name} size={52} />
        <div className="hero-main">
          <div className="hero-name">{detail.name}</div>
          <div className="hero-floor">
            <b>{fmtTon(floor)}</b>
            <ChangeBadge value={change} />
          </div>
        </div>
        <button className={`star ${isFav ? "on" : ""}`} onClick={toggleFav} aria-label="Избранное">
          {isFav ? "★" : "☆"}
        </button>
      </div>

      {(detail.issued_count != null || detail.tg_floor_price != null || onchainFloor != null || Object.keys(detail.market_floors || {}).length > 0) && (
        <div className="panel rows" style={{ marginBottom: 12 }}>
          <div className="row-item">
            <div className="row-main">
              Флор в блокчейне
              {detail.onchain_suspect && <div className="row-sub">в разы ниже маркетов — похоже на нерабочий листинг</div>}
            </div>
            <div className="row-price">{fmtTon(onchainFloor)}</div>
          </div>
          {Object.entries(detail.market_floors || {}).map(([src, price]) => (
            <div className="row-item" key={src}>
              <div className="row-main">Флор {src === "mrkt" ? "MRKT" : "Portals"}</div>
              <div className="row-price">{fmtTon(price)}</div>
            </div>
          ))}
          {detail.tg_floor_price != null && (
            <div className="row-item">
              <div className="row-main">
                Флор маркета Telegram
                {detail.tg_listed != null && <div className="row-sub">{detail.tg_listed.toLocaleString("ru-RU")} в продаже</div>}
              </div>
              <div className="row-price">{fmtTon(detail.tg_floor_price)}</div>
            </div>
          )}
          {detail.issued_count != null && (
            <div className="row-item">
              <div className="row-main">
                Выпущено улучшенных
                {detail.items_count > 0 && <div className="row-sub">{detail.items_count.toLocaleString("ru-RU")} из них в блокчейне</div>}
              </div>
              <div className="row-price" style={{ color: "#fff" }}>{detail.issued_count.toLocaleString("ru-RU")}</div>
            </div>
          )}
        </div>
      )}

      <div className="section">
        <div className="section-title">
          <span>Флор</span>
          <span className="range">
            {RANGES.map((r) => (
              <button key={r.id} className={range === r.id ? "on" : ""} onClick={() => setRange(r.id)}>
                {r.label}
              </button>
            ))}
          </span>
        </div>
        <div className="panel">{history === null ? <Skeleton h={130} r={10} /> : <Sparkline points={history} />}</div>
      </div>

      <div className="section">
        <div className="section-title"><span>Топ-атрибуты</span></div>
        <div className="chips">
          {tabs.map((t) => (
            <button key={t.id} className={attrTab === t.id ? "on" : ""} onClick={() => setAttrTab(t.id)}>
              {t.label}
            </button>
          ))}
        </div>
        <div className="panel">
          {attrs.length === 0 ? (
            <div className="spark-empty">Данных пока нет</div>
          ) : (
            <div className="rows">
              {attrs.map((a) => (
                <div className="row-item" key={a.id}>
                  <div className="row-main">
                    <div>{a.value}</div>
                    <div className="row-sub">{a.count > 0 ? `${a.count.toLocaleString("ru-RU")} шт.` : "—"}</div>
                  </div>
                  <RarityBadge value={a.rarity} />
                </div>
              ))}
            </div>
          )}
        </div>
      </div>

      <div className="section">
        <div className="section-title"><span>Последние продажи</span></div>
        <div className="panel">
          {sales === null ? (
            <Skeleton h={90} r={10} />
          ) : sales.items.length === 0 ? (
            <div className="spark-empty">Продаж пока нет</div>
          ) : (
            <div className="rows">
              {sales.items.map((s) => (
                <button
                  className="row-item"
                  key={`${s.item_address}-${s.ts}`}
                  onClick={() => navigate(`i/${encodeURIComponent(s.item_address)}`)}
                >
                  <div className="row-main">
                    <div>{shortAddr(s.item_address)}</div>
                    <div className="row-sub">{timeAgo(s.ts)}</div>
                  </div>
                  <div className="row-price">{fmtTon(s.price)}</div>
                </button>
              ))}
            </div>
          )}
          {sales && sales.items.length < sales.total && (
            <button className="btn ghost block more" onClick={loadMore} disabled={loadingMore}>
              {loadingMore ? "Загрузка…" : "Показать ещё"}
            </button>
          )}
        </div>
      </div>
    </div>
  );
}
