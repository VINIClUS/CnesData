import { Link } from "@tanstack/react-router";

import { ThemeIconButton } from "./ThemeIconButton";

import { Logo } from "@/components/brand/Logo";
import { Button } from "@/components/ui/button";
import { marketing } from "@/i18n/marketing";

const _LINKS: { label: string; to: "/" | "/precos"; hash?: string }[] = [
  { label: marketing.nav.inicio, to: "/" },
  { label: marketing.nav.recursos, to: "/", hash: "recursos" },
  { label: marketing.nav.precos, to: "/precos" },
  { label: marketing.nav.sobre, to: "/", hash: "sobre" },
];

const _LINK =
  "relative py-1 text-sm text-foreground/80 transition-colors hover:text-foreground after:absolute after:inset-x-0 after:-bottom-1 after:h-0.5 after:rounded-full after:bg-primary after:opacity-0 after:transition-opacity";

export function MarketingNavbar() {
  return (
    <header className="mx-auto flex w-full max-w-[1320px] items-center justify-between px-6 py-5">
      <div className="flex items-center gap-12">
        <Link to="/" aria-label="CnesData">
          <Logo />
        </Link>
        <nav aria-label="Principal" className="hidden items-center gap-8 md:flex">
          {_LINKS.map((l) => (
            <Link
              key={l.label}
              to={l.to}
              hash={l.hash}
              activeOptions={{ exact: true, includeHash: true }}
              activeProps={{ className: "text-foreground after:opacity-100" }}
              className={_LINK}
            >
              {l.label}
            </Link>
          ))}
          <a href={`mailto:${marketing.contactEmail}`} className={_LINK}>
            {marketing.nav.contato}
          </a>
        </nav>
      </div>
      <div className="flex items-center gap-3">
        <ThemeIconButton />
        <Button variant="outline" className="border-white/15 bg-transparent" asChild>
          <Link to="/login">{marketing.nav.entrar}</Link>
        </Button>
        <Button asChild>
          <Link to="/login">{marketing.nav.criarConta}</Link>
        </Button>
      </div>
    </header>
  );
}
