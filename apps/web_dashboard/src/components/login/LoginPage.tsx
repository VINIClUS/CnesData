import { Link, useNavigate } from "@tanstack/react-router";
import { useEffect } from "react";

import { LoginCard } from "./LoginCard";
import { LoginHero } from "./LoginHero";

import { useAuth } from "@/auth/useAuth";
import { Logo } from "@/components/brand/Logo";
import { HeroSurface } from "@/components/marketing/HeroSurface";
import { LanguageSelector } from "@/components/marketing/LanguageSelector";
import { MarketingFooter } from "@/components/marketing/MarketingFooter";
import { ThemeIconButton } from "@/components/marketing/ThemeIconButton";
import { env } from "@/lib/env";

export function LoginPage() {
  const { status } = useAuth();
  const navigate = useNavigate();

  useEffect(() => {
    if (status === "authenticated") {
      const destination = env.VITE_AUTH_MODE === "local" ? "/overview" : "/agentes";
      void navigate({ to: destination });
    }
  }, [status, navigate]);

  return (
    <HeroSurface className="flex min-h-screen flex-col">
      <header className="mx-auto flex w-full max-w-[1320px] items-center justify-between px-6 py-6">
        <Link to="/" aria-label="CnesData">
          <Logo size="lg" />
        </Link>
        <div className="flex items-center gap-2">
          <ThemeIconButton />
          <LanguageSelector />
        </div>
      </header>
      <main className="mx-auto grid w-full max-w-[1320px] flex-1 grid-cols-1 items-center gap-12 px-6 py-6 lg:grid-cols-[1fr_1fr]">
        <LoginHero />
        <div className="flex justify-end">
          <LoginCard />
        </div>
      </main>
      <MarketingFooter variant="compact" />
    </HeroSurface>
  );
}
