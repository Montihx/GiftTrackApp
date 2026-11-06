// Home.jsx — главная: сетка карточек коллекций, поиск, сортировка
import { useCallback, useEffect, useMemo, useState } from "react";
import { cachedGet, fmtTon } from "./api";
import { navigate } from "./router";
import { ChangeBadge, CollectionIcon, EmptyState, Skeleton } from "./ui";

const SORTS = [
  { id: "floor", label: "Флор" },
  { id: "change", label: "Изменение 24ч" },
  { id: "items", label: "Количество" },
];

function sortValue(c, sort) {
  if (sort === "floor") return c.floor_price;
  if (sort === "change") return c.floor_24h_change;
  return c.items_count;
}

export function CollectionCard({ c, onOpen }) {
  return (
    <button className="ccard" onClick={() => onOpen(c)}>
      <CollectionIcon url={c.icon_url} name={c.name} size={48} />
      <div className="ccard-name">{c.name}</div>
      <div className="ccard-floor">{fmtTon(c.floor_price)}</div>
      <div className="ccard-meta">
        <ChangeBadge value={c.floor_24h_change} />
        <span className="ccard-count">{c.items_count > 0 ? `${c.items_count.toLocaleString("ru-RU")} шт.` : "—"}</span>
      </div>
    </button>
  );
}

function GridSkeleton() {
  return (
    <div className="grid">
      {[0, 1, 2, 3, 4, 5].map((i) => (
        <div className="ccard skel-card" key={i}>
          <Skeleton h={48} w={48} r={24} />
          <Skeleton h={14} w="70%" />
          <Skeleton h={16} w="50%" />
          <Skeleton h={12} w="80%" />
        </div>
      ))}
    </div>
  );
}

export default function Home() {
  const [data, setData] = useState(null);
  const [error, setError] = useState(false);
  const [q, setQ] = useState("");
  const [sort, setSort] = useState("floor");

  const load = useCallback(() => {
    setError(false);
    const { cached, promise } = cachedGet("/collections", 20000);
    setData(cached ? cached.collections || [] : null);
    promise
      .then((d) => setData(d.collections || []))
      .catch(() => {
        if (!cached) setError(true);
      });
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  const list = useMemo(() => {
    const needle = q.trim().toLowerCase();
    const filtered = (data || []).filter((c) => !needle || c.name.toLowerCase().includes(needle));
    return [...filtered].sort((a, b) => {
      const av = sortValue(a, sort);
      const bv = sortValue(b, sort);
      if (av == null && bv == null) return a.name.localeCompare(b.name);
      if (av == null) return 1;
      if (bv == null) return -1;
      return bv - av || a.name.localeCompare(b.name);
    });
  }, [data, q, sort]);

  const open = (c) => navigate(`c/${encodeURIComponent(c.id)}`);

  return (
    <div>
      <h1 className="page-title">Коллекции</h1>
      <input
        className="search"
        type="search"
        placeholder="Поиск коллекции"
        value={q}
        onChange={(e) => setQ(e.target.value)}
      />
      <div className="chips">
        {SORTS.map((s) => (
          <button key={s.id} className={sort === s.id ? "on" : ""} onClick={() => setSort(s.id)}>
            {s.label}
          </button>
        ))}
      </div>

      {error && (
        <EmptyState icon="⚠️" title="Не удалось загрузить" text="Проверьте соединение и попробуйте снова.">
          <button className="btn" onClick={load}>Повторить</button>
        </EmptyState>
      )}

      {!error && data === null && <GridSkeleton />}

      {!error && data !== null && data.length === 0 && (
        <EmptyState icon="🎁" title="Каталог пока пуст" text="Коллекции появятся после первого цикла обновления данных." />
      )}

      {!error && data !== null && data.length > 0 && (
        <>
          {data.every((c) => c.floor_price == null) && (
            <div className="hint">Цены пока недоступны — они появятся, когда подключится источник данных.</div>
          )}
          {list.length === 0 ? (
            <EmptyState icon="🔍" title="Ничего не найдено" text="Попробуйте другой запрос." />
          ) : (
            <div className="grid">
              {list.map((c) => (
                <CollectionCard key={c.id} c={c} onOpen={open} />
              ))}
            </div>
          )}
        </>
      )}
    </div>
  );
}
