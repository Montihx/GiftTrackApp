// App.jsx — фильтры уведомлений: список «Мои фильтры» и форма «Создать»
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { API, apiGet, apiRequest, cachedGet, fmtTon } from "./api";
import { CollectionIcon } from "./ui";

const EMPTY_FORM = {
  collection_id: "",
  model: "",
  background: "",
  symbol: "",
  floor_min: "",
  floor_max: "",
};

// Выпадающий список с поиском по буквам (без сторонних UI-библиотек)
function SearchSelect({ label, placeholder, anyLabel, options, value, onChange, renderIcon, loading = false }) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const boxRef = useRef(null);
  const selected = options.find((o) => o.id === value) || null;

  useEffect(() => {
    if (!open) return undefined;
    const close = (e) => {
      if (boxRef.current && !boxRef.current.contains(e.target)) setOpen(false);
    };
    document.addEventListener("pointerdown", close);
    return () => document.removeEventListener("pointerdown", close);
  }, [open]);

  const q = query.trim().toLowerCase();
  const visible = q ? options.filter((o) => o.label.toLowerCase().includes(q)) : options;

  const pick = (id) => {
    onChange(id);
    setOpen(false);
    setQuery("");
  };

  return (
    <div className="ss" ref={boxRef}>
      <label>{label}</label>
      <button type="button" className={`ss-btn ${open ? "open" : ""}`} onClick={() => setOpen((v) => !v)}>
        {selected && renderIcon ? renderIcon(selected) : null}
        <span className={`ss-val ${selected ? "" : "ph"}`}>{selected ? selected.label : anyLabel}</span>
        {selected && selected.hint ? <span className="ss-hint">{selected.hint}</span> : null}
        <span className="ss-caret">▾</span>
      </button>
      {open && (
        <div className="ss-pop">
          <input
            autoFocus
            className="ss-search"
            placeholder={placeholder}
            value={query}
            onChange={(e) => setQuery(e.target.value)}
          />
          <div className="ss-list">
            <button type="button" className={`ss-opt ${value ? "" : "on"}`} onClick={() => pick("")}>
              <span className="ss-val ph">{anyLabel}</span>
            </button>
            {visible.map((o) => (
              <button type="button" key={o.id} className={`ss-opt ${o.id === value ? "on" : ""}`} onClick={() => pick(o.id)}>
                {renderIcon ? renderIcon(o) : null}
                <span className="ss-val">{o.label}</span>
                {o.hint ? <span className="ss-hint">{o.hint}</span> : null}
              </button>
            ))}
            {visible.length === 0 && (
              <div className="ss-empty">{loading && options.length === 0 ? "Загрузка…" : "Ничего не найдено"}</div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

function attrOptions(list) {
  return (list || []).map((a) => ({
    id: a.id,
    label: a.value,
    hint: a.floor != null ? fmtTon(a.floor) : a.rarity != null ? `${a.rarity}%` : "",
  }));
}

export default function Filters() {
  const [tab, setTab] = useState("filters");
  const [collections, setCollections] = useState([]);
  const [filters, setFilters] = useState([]);
  const [floor, setFloor] = useState(null);
  const [msg, setMsg] = useState("");
  const [busy, setBusy] = useState(false);
  const [form, setForm] = useState(EMPTY_FORM);

  const [loadingList, setLoadingList] = useState(true);
  const [attrData, setAttrData] = useState({}); // collection_id -> {models, backgrounds, symbols}
  const [attrLoading, setAttrLoading] = useState(false);

  // seed-коллекции без адреса контракта в форме фильтров не нужны (отслеживать нечего)
  const trackable = (d) => (d.collections || []).filter((c) => c.trackable !== false);

  const load = useCallback(async () => {
    const { cached, promise } = cachedGet("/collections", 20000);
    if (cached) {
      setCollections(trackable(cached)); // сразу показываем то, что уже загружали
      setLoadingList(false);
    }
    try {
      const [cData, fData] = await Promise.all([promise, apiGet("/filters")]);
      setCollections(trackable(cData));
      setFilters(fData.filters || []);
    } catch (e) {
      setMsg(e.status === 401 ? "Откройте приложение внутри Telegram" : "Ошибка загрузки данных");
    } finally {
      setLoadingList(false);
    }
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  const selectedColl = collections.find((c) => c.id === form.collection_id);

  // Модели/фоны/символы грузим только для выбранного подарка (у коллекций их сотни — общий список их не содержит)
  useEffect(() => {
    const id = form.collection_id;
    if (!id) return undefined;
    let alive = true;
    const { cached, promise } = cachedGet(`/collections/${id}/attributes`, 300000);
    if (cached) setAttrData((m) => ({ ...m, [id]: cached.attributes }));
    setAttrLoading(!cached);
    promise
      .then((d) => alive && setAttrData((m) => ({ ...m, [id]: d.attributes })))
      .catch(() => alive && setMsg("Не удалось загрузить модели и фоны"))
      .finally(() => alive && setAttrLoading(false));
    return () => {
      alive = false;
    };
  }, [form.collection_id]);

  useEffect(() => {
    if (!form.collection_id) {
      setFloor(null);
      return undefined;
    }
    setFloor(selectedColl ? selectedColl.floor_price : null);
    if (typeof EventSource === "undefined") return undefined;
    const es = new EventSource(`${API}/collections/${form.collection_id}/stream`);
    es.onmessage = (ev) => {
      try {
        const data = JSON.parse(ev.data);
        if (data.floor_price != null) setFloor(data.floor_price);
      } catch {
        /* ignore */
      }
    };
    es.onerror = () => es.close();
    return () => es.close();
  }, [form.collection_id]); // eslint-disable-line react-hooks/exhaustive-deps

  const setField = (k, v) => setForm((f) => ({ ...f, [k]: v }));
  const pickCollection = (id) =>
    setForm((f) => ({ ...f, collection_id: id, model: "", background: "", symbol: "" }));

  const giftOptions = useMemo(
    () =>
      collections.map((c) => ({
        id: c.id,
        label: c.name,
        icon_url: c.icon_url,
        hint: c.floor_price != null ? fmtTon(c.floor_price) : "",
      })),
    [collections],
  );
  const attrs = attrData[form.collection_id] || {};
  const modelOpts = useMemo(() => attrOptions(attrs.models), [attrs.models]);
  const bgOpts = useMemo(() => attrOptions(attrs.backgrounds), [attrs.backgrounds]);
  const symbolOpts = useMemo(() => attrOptions(attrs.symbols), [attrs.symbols]);
  const selectedModel = (attrs.models || []).find((a) => a.id === form.model);

  const floorMin = form.floor_min === "" ? null : Number(form.floor_min);
  const floorMax = form.floor_max === "" ? null : Number(form.floor_max);
  const rangeError = floorMin != null && floorMax != null && floorMin > floorMax;

  const cancel = () => {
    setForm(EMPTY_FORM);
    setMsg("");
    setTab("filters");
  };

  const submit = async (e) => {
    e.preventDefault();
    if (rangeError) return;
    setBusy(true);
    try {
      await apiRequest("POST", "/filters", {
        collection_id: form.collection_id || null,
        model_id: form.model || null,
        background_id: form.background || null,
        symbol_id: form.symbol || null,
        floor_min: floorMin,
        floor_max: floorMax,
      });
      setMsg("Фильтр создан ✓");
      setForm(EMPTY_FORM);
      await load();
      setTab("filters");
    } catch {
      setMsg("Не удалось создать фильтр");
    } finally {
      setBusy(false);
    }
  };

  const remove = async (id) => {
    try {
      await apiRequest("DELETE", `/filters/${id}`);
      setFilters((fs) => fs.filter((f) => f.id !== id));
    } catch {
      setMsg("Не удалось удалить фильтр");
    }
  };

  const describe = (f) => {
    const parts = [];
    if (f.model_id) parts.push(`Модель: ${f.model_name || "—"}`);
    if (f.background_id) parts.push(`Фон: ${f.background_name || "—"}`);
    if (f.symbol_id) parts.push(`Символ: ${f.symbol_name || "—"}`);
    if (f.pattern_id) parts.push(`Узор: ${f.pattern_name || "—"}`);
    if (f.floor_min != null || f.floor_max != null) {
      parts.push(`Флор модели ${f.floor_min != null ? `от ${f.floor_min}` : ""} ${f.floor_max != null ? `до ${f.floor_max}` : ""} TON`.replace(/\s+/g, " "));
    }
    return parts.length ? parts.join(" · ") : "Все листинги и продажи";
  };

  return (
    <div className="app">
      <div className="page-title">Фильтры</div>
      <div className="tabs">
        <button className={tab === "filters" ? "on" : ""} onClick={() => setTab("filters")}>
          Мои фильтры{filters.length ? ` · ${filters.length}` : ""}
        </button>
        <button className={tab === "create" ? "on" : ""} onClick={() => setTab("create")}>
          Создать
        </button>
      </div>

      {msg && <div className="msg">{msg}</div>}

      {tab === "filters" && (
        <div className="list">
          {filters.length === 0 && (
            <div className="empty">
              Нет активных фильтров
              <div style={{ marginTop: 12 }}>
                <button className="btn" onClick={() => setTab("create")}>Создать фильтр</button>
              </div>
            </div>
          )}
          {filters.map((f) => {
            const coll = collections.find((c) => c.id === f.collection_id);
            return (
              <div className="card fcard" key={f.id}>
                <CollectionIcon url={coll?.icon_url} name={coll?.name || "★"} size={40} />
                <div className="fcard-main">
                  <div className="card-title">{coll ? coll.name : "Любой подарок"}</div>
                  <div className="card-sub">{describe(f)}</div>
                </div>
                <button className="del" onClick={() => remove(f.id)} aria-label="Удалить фильтр">Удалить</button>
              </div>
            );
          })}
        </div>
      )}

      {tab === "create" && (
        <form className="form" onSubmit={submit}>
          <SearchSelect
            label="Подарок"
            placeholder="Поиск подарка…"
            anyLabel="Любой подарок"
            options={giftOptions}
            loading={loadingList}
            value={form.collection_id}
            onChange={pickCollection}
            renderIcon={(o) => <CollectionIcon url={o.icon_url} name={o.label} size={24} />}
          />

          {selectedColl && (
            <>
              <div className="floor">Флор подарка: {floor != null ? fmtTon(floor) : "—"}</div>
              <SearchSelect
                label="Модель"
                placeholder="Поиск модели…"
                anyLabel="Любая модель"
                options={modelOpts}
                loading={attrLoading}
                value={form.model}
                onChange={(v) => setField("model", v)}
              />
              <SearchSelect
                label="Фон"
                placeholder="Поиск фона…"
                anyLabel="Любой фон"
                options={bgOpts}
                loading={attrLoading}
                value={form.background}
                onChange={(v) => setField("background", v)}
              />
              <SearchSelect
                label="Символ"
                placeholder="Поиск символа…"
                anyLabel="Любой символ"
                options={symbolOpts}
                loading={attrLoading}
                value={form.symbol}
                onChange={(v) => setField("symbol", v)}
              />
              {attrLoading && <div className="hint">Загружаю модели, фоны и символы…</div>}
              {!attrLoading && modelOpts.length === 0 && (
                <div className="hint">Модели, фоны и символы появятся после первой синхронизации коллекции.</div>
              )}
            </>
          )}

          <div className="fblock">
            <div className="fblock-title">Флор модели (TON)</div>
            <div className="row">
              <div>
                <label>от</label>
                <input type="number" inputMode="decimal" min="0" step="0.01" value={form.floor_min}
                  onChange={(e) => setField("floor_min", e.target.value)} placeholder="0" />
              </div>
              <div>
                <label>до</label>
                <input type="number" inputMode="decimal" min="0" step="0.01" value={form.floor_max}
                  onChange={(e) => setField("floor_max", e.target.value)} placeholder="∞" />
              </div>
            </div>
            {selectedModel && selectedModel.floor != null && (
              <div className="fblock-hint">Сейчас флор модели {selectedModel.value}: {fmtTon(selectedModel.floor)}</div>
            )}
            {rangeError && <div className="fblock-err">«от» больше, чем «до»</div>}
          </div>

          <div className="form-actions">
            <button type="button" className="btn ghost" onClick={cancel}>Отмена</button>
            <button type="submit" className="btn" disabled={busy || rangeError}>
              {busy ? "Сохраняем…" : "Создать фильтр"}
            </button>
          </div>
        </form>
      )}
    </div>
  );
}
