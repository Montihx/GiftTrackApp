// ui.jsx — общие UI-блоки витрины
import { changeClass, fmtChange, rarityClass } from "./api";

export function CollectionIcon({ url, name, size = 44 }) {
  const initial = (name || "?").trim().charAt(0).toUpperCase();
  return (
    <div className="ico" style={{ width: size, height: size, fontSize: Math.round(size * 0.42) }}>
      <span>{initial}</span>
      {url ? (
        <img
          src={url}
          alt=""
          loading="lazy"
          onError={(e) => {
            e.currentTarget.style.display = "none";
          }}
        />
      ) : null}
    </div>
  );
}

export function ChangeBadge({ value }) {
  return <span className={`chg ${changeClass(value)}`}>{fmtChange(value)}</span>;
}

export function RarityBadge({ value }) {
  if (value == null) return <span className="rar none">—</span>;
  return <span className={`rar ${rarityClass(value)}`}>{value}%</span>;
}

export function Skeleton({ h = 16, w = "100%", r = 8 }) {
  return <div className="skel" style={{ height: h, width: w, borderRadius: r }} />;
}

export function EmptyState({ icon, title, text, children }) {
  return (
    <div className="empty-state">
      <div className="empty-ico">{icon}</div>
      <div className="empty-title">{title}</div>
      {text ? <div className="empty-text">{text}</div> : null}
      {children}
    </div>
  );
}

export function TabBar({ tabs, active, onSelect }) {
  return (
    <nav className="tabbar">
      {tabs.map((t) => (
        <button key={t.id} className={t.id === active ? "on" : ""} onClick={() => onSelect(t)}>
          <span className="tb-ico">{t.icon}</span>
          <span className="tb-lbl">{t.label}</span>
        </button>
      ))}
    </nav>
  );
}
