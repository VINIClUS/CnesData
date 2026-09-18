import { ArrowRight, UserRound } from "lucide-react";
import { type FormEvent, useState } from "react";

import { LoginField } from "./LoginField";
import { PasswordField } from "./PasswordField";

import { startLogin } from "@/auth/oidc";
import { Button } from "@/components/ui/button";
import { login } from "@/i18n/login";

export function LoginForm() {
  const f = login.form;
  const [identifier, setIdentifier] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);

  const begin = (withHint: boolean) => {
    setError(null);
    const hint = identifier.trim();
    startLogin(withHint && hint ? { loginHint: hint } : {}).catch(() => setError(f.error));
  };

  const onSubmit = (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    begin(true);
  };

  return (
    <form onSubmit={onSubmit} className="space-y-5" noValidate>
      <LoginField
        id="login-identifier"
        label={f.identifierLabel}
        icon={UserRound}
        placeholder={f.identifierPlaceholder}
        autoComplete="username"
        value={identifier}
        onChange={(e) => setIdentifier(e.target.value)}
      />
      <PasswordField
        id="login-password"
        label={f.passwordLabel}
        placeholder={f.passwordPlaceholder}
        value={password}
        onChange={setPassword}
      />
      <div className="flex justify-end">
        <button type="button" onClick={() => begin(true)} className="text-sm text-primary">
          {f.forgot}
        </button>
      </div>
      {error && (
        <p role="alert" className="text-sm text-destructive">
          {error}
        </p>
      )}
      <Button type="submit" size="lg" className="w-full">
        {f.submit}
        <ArrowRight aria-hidden="true" />
      </Button>
      <div className="flex items-center gap-3 text-xs text-muted-foreground">
        <span className="h-px flex-1 bg-border" />
        {f.or}
        <span className="h-px flex-1 bg-border" />
      </div>
      <Button
        type="button"
        size="lg"
        variant="outline"
        className="w-full border-primary/50 bg-transparent text-primary"
        onClick={() => begin(false)}
      >
        {f.register}
      </Button>
    </form>
  );
}
