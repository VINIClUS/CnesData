import { Eye, EyeOff, Lock } from "lucide-react";
import { useState } from "react";

import { LoginField } from "./LoginField";

import { login } from "@/i18n/login";

type Props = {
  id: string;
  label: string;
  value: string;
  onChange: (value: string) => void;
  placeholder: string;
};

export function PasswordField({ id, label, value, onChange, placeholder }: Props) {
  const [visible, setVisible] = useState(false);
  const Icon = visible ? EyeOff : Eye;
  return (
    <LoginField
      id={id}
      label={label}
      icon={Lock}
      type={visible ? "text" : "password"}
      value={value}
      onChange={(e) => onChange(e.target.value)}
      placeholder={placeholder}
      autoComplete="current-password"
      trailing={
        <button
          type="button"
          aria-label={visible ? login.form.hidePassword : login.form.showPassword}
          onClick={() => setVisible((v) => !v)}
          className="flex size-8 items-center justify-center rounded text-muted-foreground hover:text-foreground"
        >
          <Icon aria-hidden="true" className="size-4" />
        </button>
      }
    />
  );
}
