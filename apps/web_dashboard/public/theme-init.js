/*
 * Pre-hydration theme bootstrap. Loaded as a same-origin classic script from
 * index.html so it runs before first paint without needing CSP 'unsafe-inline'.
 * Keep the resolution rules identical to src/theme/ThemeProvider.tsx.
 */
(function () {
  try {
    var stored = window.localStorage.getItem("cnesdata-theme");
    var theme = stored === "light" || stored === "dark" || stored === "system" ? stored : "system";
    var prefersDark =
      typeof window.matchMedia === "function" &&
      window.matchMedia("(prefers-color-scheme: dark)").matches;
    var dark = theme === "dark" || (theme === "system" && prefersDark);
    document.documentElement.classList.toggle("dark", dark);
  } catch {
    /* storage blocked (private mode, cookies off): fall back to the light theme */
  }
})();
