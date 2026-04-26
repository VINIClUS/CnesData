import { Monitor, Moon, Sun } from "lucide-react";

import { useTheme, type Theme } from "@/theme/useTheme";

const _OPTIONS: { value: Theme; icon: typeof Sun; label: string }[] = [
  { value: "light", icon: Sun, label: "Claro" },
  { value: "dark", icon: Moon, label: "Escuro" },
  { value: "system", icon: Monitor, label: "Sistema" },
];

export function ThemeToggle() {
  const { theme, setTheme } = useTheme();
  return (
    <div className="inline-flex rounded-lg border bg-muted/30 p-0.5">
      {_OPTIONS.map(({ value, icon: Icon, label }) => (
        <button
          key={value}
          type="button"
          onClick={() => setTheme(value)}
          aria-label={`Tema ${label}`}
          aria-pressed={theme === value}
          className={
            theme === value
              ? "rounded bg-background px-2 py-1"
              : "rounded px-2 py-1 opacity-60 hover:opacity-100"
          }
        >
          <Icon size={14} />
        </button>
      ))}
    </div>
  );
}
