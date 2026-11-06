// Shell.jsx — корень TWA: хеш-роутинг, нижние табы, интеграция с Telegram WebApp
import { useEffect } from "react";
import CollectionPage from "./CollectionPage.jsx";
import Favorites from "./Favorites.jsx";
import Feed from "./Feed.jsx";
import Filters from "./App.jsx";
import Home from "./Home.jsx";
import ItemPage from "./ItemPage.jsx";
import Subscription from "./Subscription.jsx";
import { tgApp } from "./api";
import { goBack, navigate, useHashRoute } from "./router";
import { TabBar } from "./ui";

const TABS = [
  { id: "feed", label: "Лента", icon: "⚡", path: "feed" },
  { id: "home", label: "Витрина", icon: "💎", path: "" },
  { id: "fav", label: "Избранное", icon: "★", path: "fav" },
  { id: "filters", label: "Фильтры", icon: "🔔", path: "filters" },
  { id: "sub", label: "Подписка", icon: "👑", path: "sub" },
];

function activeTab(route) {
  return ["feed", "fav", "filters", "sub"].includes(route.name) ? route.name : "home";
}

export default function Shell() {
  const route = useHashRoute();

  useEffect(() => {
    const tg = tgApp();
    if (tg) {
      tg.ready();
      tg.expand();
      tg.setHeaderColor("#17212b");
      tg.setBackgroundColor("#17212b");
    }
  }, []);

  // Кнопка «Назад» Telegram — на вложенных экранах (коллекция / предмет)
  const nested = route.name === "collection" || route.name === "item";
  useEffect(() => {
    const tg = tgApp();
    if (!tg || !tg.BackButton) return undefined;
    if (!nested) {
      tg.BackButton.hide();
      return undefined;
    }
    tg.BackButton.show();
    tg.BackButton.onClick(goBack);
    return () => tg.BackButton.offClick(goBack);
  }, [nested]);

  let screen = <Home />;
  if (route.name === "filters") screen = <Filters />;
  if (route.name === "collection") screen = <CollectionPage key={route.id} id={route.id} />;
  if (route.name === "item") screen = <ItemPage key={route.address} address={route.address} />;
  if (route.name === "feed") screen = <Feed />;
  if (route.name === "fav") screen = <Favorites />;
  if (route.name === "sub") screen = <Subscription />;

  return (
    <>
      <div className="page">{screen}</div>
      <TabBar tabs={TABS} active={activeTab(route)} onSelect={(t) => navigate(t.path)} />
    </>
  );
}
