import {
  BarChart3,
  Building2,
  ClipboardList,
  FileText,
  Home,
  Info,
  Settings,
  Users,
} from "lucide-react";
import type { LucideIcon } from "lucide-react";

import { Logo } from "@/components/brand/Logo";
import { landing } from "@/i18n/landing";
import { cn } from "@/lib/utils";

const _ICONS: LucideIcon[] = [Home, Building2, Users, Users, BarChart3, FileText];
const _FOOTER_ICONS: LucideIcon[] = [Settings, Info];

function Item({
  icon: Icon,
  label,
  active,
}: {
  icon: LucideIcon;
  label: string;
  active?: boolean;
}) {
  return (
    <li
      className={cn(
        "flex items-center gap-2 rounded-md px-2.5 py-1.5 text-[11px] text-slate-300",
        active && "bg-white/10 text-white",
      )}
    >
      <Icon aria-hidden="true" className="size-3.5" />
      {label}
    </li>
  );
}

export function PreviewSidebar() {
  return (
    <aside className="flex flex-col justify-between bg-navy-card px-3 py-4">
      <div>
        <Logo size="sm" className="px-2 text-white" />
        <ul className="mt-5 space-y-0.5">
          {landing.preview.nav.map((label, i) => (
            <Item key={label} icon={_ICONS[i] ?? ClipboardList} label={label} active={i === 0} />
          ))}
        </ul>
      </div>
      <ul className="space-y-0.5">
        {landing.preview.navFooter.map((label, i) => (
          <Item key={label} icon={_FOOTER_ICONS[i] ?? Info} label={label} />
        ))}
      </ul>
    </aside>
  );
}
