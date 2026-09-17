import { LoginForm } from "./LoginForm";

import { Logo } from "@/components/brand/Logo";
import { LanguageSelector } from "@/components/marketing/LanguageSelector";
import { login } from "@/i18n/login";

export function LoginCard() {
  return (
    <div className="w-full max-w-[580px] rounded-2xl border border-white/10 bg-navy-card/80 p-10 shadow-2xl shadow-black/40 backdrop-blur">
      <div className="flex items-center justify-between">
        <Logo />
        <LanguageSelector />
      </div>
      <h2 className="mt-8 text-3xl font-semibold tracking-tight">{login.card.title}</h2>
      <p className="mt-2 max-w-md text-base leading-relaxed text-muted-foreground">
        {login.card.subtitle}
      </p>
      <div className="mt-8">
        <LoginForm />
      </div>
    </div>
  );
}
