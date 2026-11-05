import { useEffect, useState, useCallback } from "react";

const API = "/api";

function initDataHeader() {
  const tg = window.Telegram?.WebApp;
  return tg ? { "X-Telegram-Init-Data": tg.initData } : {};
}

export default function App() {
  const [tab, setTab] = useState("filters");
  const [collections, setCollections] = useState([]);
  const [filters, setFilters] = useState([]);
  const [floor, setFloor] = useState(null);
  const [msg, setMsg] = useState("");

  const [form, setForm] = useState({
    collection_id: "",
    model: "",
    background: "",
    pattern: "",
    floor_min: "",
    floor_max: "",
  });

  useEffect(() => {
    const tg = window.Telegram?.WebApp;
    if (tg) {
      tg.ready();
      tg.expand();
      tg.setHeaderColor("#17212b");
      tg.setBackgroundColor("#17212b");
    }
  }, []);

  const load = useCallback(async () => {
    try {
      const [cRes, fRes] = await Promise.all([
        fetch(`${API}/collections`),
        fetch(`${API}/filters`, { headers: initDataHeader() }),
      ]);
      const cData = await cRes.json();
      const fData = await fRes.json();
      setCollections(cData.collections || []);
      setFilters(fData.filters || []);
    } catch (e) {
      setMsg("Ошибка загрузки данных");
    }
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  useEffect(() => {
    if (!form.collection_id) {
      setFloor(null);
      return;
    }
    const es = new EventSource(`${API}/collections/${form.collection_id}/stream`);
    es.onmessage = (ev) => {
      try {
        const data = JSON.parse(ev.data);
        setFloor(data.floor_price);
      } catch {}
    };
    es.onerror = () => es.close();
    return () => es.close();
  }, [form.collection_id]);

  const selectedColl = collections.find((c) => c.id === form.collection_id);
  const setField = (k, v) => setForm((f) => ({ ...f, [k]: v }));

  const submit = async (e) => {
    e.preventDefault();
    const body = {
      collection_id: form.collection_id || null,
      model_id: form.model || null,
      background_id: form.background || null,
      pattern_id: form.pattern || null,
      floor_min: form.floor_min ? Number(form.floor_min) : null,
      floor_max: form.floor_max ? Number(form.floor_max) : null,
    };
    try {
      const res = await fetch(`${API}/filters`, {
        method: "POST",
        headers: { "Content-Type": "application/json", ...initDataHeader() },
        body: JSON.stringify(body),
      });
      if (!res.ok) throw new Error();
      setMsg("Фильтр создан ✓");
      setForm({ collection_id: "", model: "", background: "", pattern: "", floor_min: "", floor_max: "" });
      load();
      setTab("filters");
    } catch {
      setMsg("Не удалось создать фильтр");
    }
  };

  const remove = async (id) => {
    try {
      await fetch(`${API}/filters/${id}`, { method: "DELETE", headers: initDataHeader() });
      setFilters((fs) => fs.filter((f) => f.id !== id));
    } catch {
      setMsg("Не удалось удалить фильтр");
    }
  };

  const attrName = (coll, type, id) => {
    const list = coll?.attributes?.[type] || [];
    const item = list.find((a) => a.id === id);
    return item ? item.value : id;
  };

  return (
    <div className="app">
      <div className="tabs">
        <button className={tab === "filters" ? "on" : ""} onClick={() => setTab("filters")}>
          Мои фильтры
        </button>
        <button className={tab === "create" ? "on" : ""} onClick={() => setTab("create")}>
          Создать
        </button>
      </div>

      {msg && <div className="msg">{msg}</div>}

      {tab === "filters" && (
        <div className="list">
          {filters.length === 0 && <div className="empty">Нет активных фильтров</div>}
          {filters.map((f) => {
            const coll = collections.find((c) => c.id === f.collection_id);
            return (
              <div className="card" key={f.id}>
                <div className="card-title">{coll ? coll.name : "Любая коллекция"}</div>
                <div className="card-sub">
                  {f.model_id ? `Модель: ${attrName(coll, "models", f.model_id)} · ` : ""}
                  {f.background_id ? `Фон: ${attrName(coll, "backgrounds", f.background_id)} · ` : ""}
                  {f.pattern_id ? `Узор: ${attrName(coll, "patterns", f.pattern_id)} · ` : ""}
                  {f.floor_min != null ? `min ${f.floor_min} ` : ""}
                  {f.floor_max != null ? `max ${f.floor_max} ` : ""}
                  TON
                </div>
                <button className="del" onClick={() => remove(f.id)}>Удалить</button>
              </div>
            );
          })}
        </div>
      )}

      {tab === "create" && (
        <form className="form" onSubmit={submit}>
          <label>Коллекция</label>
          <select value={form.collection_id} onChange={(e) => setField("collection_id", e.target.value)}>
            <option value="">— выбрать —</option>
            {collections.map((c) => (
              <option key={c.id} value={c.id}>{c.name}</option>
            ))}
          </select>

          {selectedColl && (
            <>
              <div className="floor">Floor: {floor != null ? `${floor} TON` : "…"}</div>

              <label>Модель</label>
              <select value={form.model} onChange={(e) => setField("model", e.target.value)}>
                <option value="">— любая —</option>
                {(selectedColl.attributes.models || []).map((a) => (
                  <option key={a.id} value={a.id}>{a.value}</option>
                ))}
              </select>

              <label>Фон</label>
              <select value={form.background} onChange={(e) => setField("background", e.target.value)}>
                <option value="">— любой —</option>
                {(selectedColl.attributes.backgrounds || []).map((a) => (
                  <option key={a.id} value={a.id}>{a.value}</option>
                ))}
              </select>

              <label>Узор</label>
              <select value={form.pattern} onChange={(e) => setField("pattern", e.target.value)}>
                <option value="">— любой —</option>
                {(selectedColl.attributes.patterns || []).map((a) => (
                  <option key={a.id} value={a.id}>{a.value}</option>
                ))}
              </select>
            </>
          )}

          <div className="row">
            <div>
              <label>Floor min</label>
              <input type="number" step="0.01" value={form.floor_min} onChange={(e) => setField("floor_min", e.target.value)} placeholder="0" />
            </div>
            <div>
              <label>Floor max</label>
              <input type="number" step="0.01" value={form.floor_max} onChange={(e) => setField("floor_max", e.target.value)} placeholder="∞" />
            </div>
          </div>

          <button type="submit" className="submit">Создать фильтр</button>
        </form>
      )}
    </div>
  );
}
