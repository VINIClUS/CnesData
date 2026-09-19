import { Link } from "@tanstack/react-router";

import { MobileMenuButton, MobileMenuPanel } from "./MarketingMobileNav";
import { ThemeIconButton } from "./ThemeIconButton";
import { ACCESS_SEARCH, NAV_LINK_CLASS, PUBLIC_NAV } from "./marketingNavigation";
import { useMobileMenu } from "./useMobileMenu";

import { Logo } from "@/components/brand/Logo";
import { Button } from "@/components/ui/button";
import { marketing } from "@/i18n/marketing";

export function MarketingNavbar() {
  const menu = useMobileMenu();
  return (
    <header className="mx-auto flex w-full max-w-[1320px] flex-wrap items-center justify-between px-6 py-5">
      <div className="flex items-center gap-12">
        <Link to="/" aria-label="CnesData">
          <Logo />
        </Link>
        <nav aria-label="Principal" className="hidden items-center gap-8 md:flex">
          {PUBLIC_NAV.map((l) => (
            <Link
              key={l.to}
              to={l.to}
              activeOptions={{ exact: true, includeSearch: false }}
              activeProps={{ className: "text-foreground after:opacity-100" }}
              className={NAV_LINK_CLASS}
            >
              {l.label}
            </Link>
          ))}
        </nav>
      </div>
      <div className="flex items-center gap-3">
        <ThemeIconButton />
        <Button
          variant="outline"
          className="hidden border-white/15 bg-transparent md:inline-flex"
          asChild
        >
          <Link to="/login">{marketing.nav.entrar}</Link>
        </Button>
        <Button className="hidden md:inline-flex" asChild>
          <Link to="/contato" search={ACCESS_SEARCH}>
            {marketing.nav.solicitarAcesso}
          </Link>
        </Button>
        <MobileMenuButton {...menu} />
      </div>
      <MobileMenuPanel {...menu} />
    </header>
  );
}
