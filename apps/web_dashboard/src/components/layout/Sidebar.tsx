import { t } from "@/i18n/pt-BR";
import { Link } from "@tanstack/react-router";
import { Activity, Building2, FileBarChart, Home, KeyRound, type LucideIcon } from "lucide-react";

type Item = {
  label: string;
  to?: string;
  icon: LucideIcon;
  future?: boolean;
};

const _ITEMS: Item[] = [
  { label: t.nav.agentes, to: "/agentes", icon: Activity },
  { label: t.nav.activate, to: "/activate", icon: KeyRound },
  { label: t.nav.overview, icon: Home, future: true },
  { label: t.nav.faturamento, icon: FileBarChart, future: true },
  { label: t.nav.estabelecimentos, icon: Building2, future: true },
];

export function Sidebar({ activePath }: { activePath: string }) {
  return (
    <nav
      className="w-56 border-r bg-muted/30 p-4 flex flex-col gap-1"
      aria-label="navegação principal"
    >
      <div className="font-semibold text-sm mb-3">{t.app.name}</div>
      {_ITEMS.map((item) =>
        item.future ? (
          <span
            key={item.label}
            className="flex items-center gap-2 px-2 py-1.5 text-xs text-muted-foreground italic"
          >
            <item.icon size={14} /> {item.label}
            <span className="ml-auto text-[10px]">{t.futureBadge}</span>
          </span>
        ) : (
          <Link
            key={item.label}
            to={item.to}
            className="flex items-center gap-2 px-2 py-1.5 rounded text-sm hover:bg-muted"
            aria-current={activePath === item.to ? "page" : undefined}
            data-active={activePath === item.to}
          >
            <item.icon size={14} /> {item.label}
          </Link>
        ),
      )}
    </nav>
  );
}
