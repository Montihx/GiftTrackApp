// Sparkline.jsx — график флора на чистом SVG (polyline), без chart-библиотек
import { fmtTon, parseTs } from "./api";

const W = 320;
const H = 130;
const PAD = 8;

export default function Sparkline({ points }) {
  if (!points || points.length < 2) {
    return <div className="spark-empty">Недостаточно данных для графика</div>;
  }
  const prices = points.map((p) => p.price);
  const times = points.map((p) => parseTs(p.ts));
  const minP = Math.min(...prices);
  const maxP = Math.max(...prices);
  const minT = Math.min(...times);
  const maxT = Math.max(...times);
  const spanP = maxP - minP || 1;
  const spanT = maxT - minT || 1;

  const xy = points.map((p, i) => {
    const x = PAD + ((times[i] - minT) / spanT) * (W - 2 * PAD);
    const y = H - PAD - ((prices[i] - minP) / spanP) * (H - 2 * PAD);
    return [x, y];
  });
  const line = xy.map(([x, y]) => `${x.toFixed(1)},${y.toFixed(1)}`).join(" ");
  const area = `${PAD},${H - PAD} ${line} ${W - PAD},${H - PAD}`;
  const last = xy[xy.length - 1];

  return (
    <div>
      <svg className="spark" viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none" role="img" aria-label="График флора">
        <defs>
          <linearGradient id="sparkfill" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="#6ab7f5" stopOpacity="0.35" />
            <stop offset="100%" stopColor="#6ab7f5" stopOpacity="0" />
          </linearGradient>
        </defs>
        <polygon points={area} fill="url(#sparkfill)" />
        <polyline points={line} fill="none" stroke="#6ab7f5" strokeWidth="2" strokeLinejoin="round" strokeLinecap="round" />
        <circle cx={last[0]} cy={last[1]} r="3.5" fill="#6ab7f5" />
      </svg>
      <div className="spark-axis">
        <span>мин {fmtTon(minP)}</span>
        <span>макс {fmtTon(maxP)}</span>
      </div>
    </div>
  );
}
