import { Moon, Sun } from "lucide-react";

import { cn } from "@/lib/utils";
import { useTheme } from "@/theme/useTheme";

export function ThemeIconButton({ className }: { className?: string }) {
  const { effectiveTheme, setTheme } = useTheme();
  const isDark = effectiveTheme === "dark";
  const Icon = isDark ? Moon : Sun;
  return (
    <button
      type="button"
      aria-label="Alternar tema"
      aria-pressed={isDark}
      onClick={() => setTheme(isDark ? "light" : "dark")}
      className={cn(
        "inline-flex size-9 items-center justify-center rounded-md text-foreground/80 transition-colors hover:bg-accent hover:text-foreground",
        className,
      )}
    >
      <Icon aria-hidden="true" className="size-5" />
    </button>
  );
}
