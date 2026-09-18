import { Link } from "@tanstack/react-router";
import { Menu, X } from "lucide-react";

import { ACCESS_SEARCH, PUBLIC_NAV } from "./marketingNavigation";
import type { MobileMenu } from "./useMobileMenu";

import { Button } from "@/components/ui/button";
import { marketing } from "@/i18n/marketing";

export function MobileMenuButton({ open, id, buttonRef, toggle }: MobileMenu) {
  return (
    <Button
      ref={buttonRef}
      type="button"
      variant="ghost"
      size="icon"
      className="md:hidden"
      aria-label={marketing.nav.menu}
      aria-expanded={open}
      aria-controls={id}
      onClick={toggle}
    >
      {open ? <X aria-hidden="true" /> : <Menu aria-hidden="true" />}
    </Button>
  );
}

export function MobileMenuPanel({ open, id, close }: MobileMenu) {
  return (
    <nav
      id={id}
      aria-label={marketing.nav.menuMobile}
      hidden={!open}
      className="mt-4 w-full basis-full border-t border-white/10 pt-4 md:hidden"
    >
      <ul className="flex flex-col gap-1">
        {PUBLIC_NAV.map((l) => (
          <li key={l.to}>
            <Link
              to={l.to}
              activeOptions={{ exact: true, includeSearch: false }}
              activeProps={{ className: "text-foreground" }}
              className="block py-2 text-base text-foreground/80"
              onClick={close}
            >
              {l.label}
            </Link>
          </li>
        ))}
        <li className="mt-3 flex flex-col gap-2">
          <Button variant="outline" className="border-white/15 bg-transparent" asChild>
            <Link to="/login" onClick={close}>
              {marketing.nav.entrar}
            </Link>
          </Button>
          <Button asChild>
            <Link to="/contato" search={ACCESS_SEARCH} onClick={close}>
              {marketing.nav.solicitarAcesso}
            </Link>
          </Button>
        </li>
      </ul>
    </nav>
  );
}
