/// <reference types="vitest" />
import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";
import path from "node:path";

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: { "@": path.resolve(__dirname, "src") },
  },
  server: {
    port: 5173,
    proxy: {
      "/api":      { target: "http://localhost:8000", changeOrigin: true },
      "/oauth":    { target: "http://localhost:8000", changeOrigin: true },
      "/activate": { target: "http://localhost:8000", changeOrigin: true },
    },
  },
  build: {
    target: "esnext",
  },
  test: {
    environment: "jsdom",
    globals: true,
    setupFiles: ["./tests/setup.ts"],
    env: {
      VITE_API_BASE_URL: "/api/v1",
    },
    coverage: {
      provider: "v8",
      reporter: ["text", "html", "json-summary"],
      thresholds: { lines: 80, branches: 70 },
      exclude: [
        "src/main.tsx",
        "src/App.tsx",
        "src/components/ui/**",
        "src/api/generated.ts",
        "**/*.config.*",
        "tests/**",
      ],
    },
  },
});
