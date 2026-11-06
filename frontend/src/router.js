// router.js — минимальный хеш-роутер (без react-router)
import { useEffect, useState } from "react";

export function parseHash(hash) {
  const parts = (hash || "")
    .replace(/^#\/?/, "")
    .split("/")
    .filter(Boolean)
    .map((p) => {
      try {
        return decodeURIComponent(p);
      } catch {
        return p;
      }
    });
  const head = parts[0];
  const arg = parts[1];
  if (head === "c" && arg) return { name: "collection", id: arg };
  if (head === "i" && arg) return { name: "item", address: arg };
  if (head === "feed") return { name: "feed" };
  if (head === "fav") return { name: "fav" };
  if (head === "filters") return { name: "filters" };
  if (head === "sub") return { name: "sub" };
  return { name: "home" };
}

export function navigate(path) {
  window.location.hash = `#/${path}`;
}

export function goBack() {
  if (window.history.length > 1) window.history.back();
  else navigate("");
}

export function useHashRoute() {
  const [route, setRoute] = useState(() => parseHash(window.location.hash));
  useEffect(() => {
    const onChange = () => setRoute(parseHash(window.location.hash));
    window.addEventListener("hashchange", onChange);
    return () => window.removeEventListener("hashchange", onChange);
  }, []);
  return route;
}
