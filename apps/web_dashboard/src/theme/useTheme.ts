import { useContext } from "react";

import { ThemeContext, type Theme } from "@/theme/ThemeProvider";

export type { Theme };

export function useTheme() {
  const ctx = useContext(ThemeContext);
  if (!ctx) throw new Error("useTheme must be inside ThemeProvider");
  return ctx;
}
