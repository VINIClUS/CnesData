import type { Interesse } from "@/components/contact/contactInterest";
import { marketing } from "@/i18n/marketing";

export type PublicPath = "/" | "/recursos" | "/sobre" | "/contato";
export type PublicNavItem = { label: string; to: PublicPath };

export const PUBLIC_NAV: readonly PublicNavItem[] = [
  { label: marketing.nav.inicio, to: "/" },
  { label: marketing.nav.recursos, to: "/recursos" },
  { label: marketing.nav.sobre, to: "/sobre" },
  { label: marketing.nav.contato, to: "/contato" },
];

export const ACCESS_SEARCH: { interesse: Interesse } = { interesse: "acesso-antecipado" };
export const PILOT_SEARCH: { interesse: Interesse } = { interesse: "piloto" };

export const NAV_LINK_CLASS =
  "relative py-1 text-sm text-foreground/80 transition-colors hover:text-foreground after:absolute after:inset-x-0 after:-bottom-1 after:h-0.5 after:rounded-full after:bg-primary after:opacity-0 after:transition-opacity";
