/// <reference types="vitest" />
import path from "node:path";
import { tanstackRouter } from "@tanstack/router-plugin/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [tanstackRouter({ routesDirectory: "src/routes", target: "react" }), react()],
  resolve: {
    alias: { "@": path.resolve(__dirname, "src") },
  },
  server: {
    port: 5173,
    proxy: {
      "/api": { target: "http://localhost:8000", changeOrigin: true },
      "/oauth": { target: "http://localhost:8000", changeOrigin: true },
      "/activate": { target: "http://localhost:8000", changeOrigin: true },
    },
  },
  build: {
    target: "esnext",
    rollupOptions: {
      output: {
        manualChunks(id) {
          if (id.includes("node_modules/react/") || id.includes("node_modules/react-dom/")) {
            return "react";
          }
          if (id.includes("node_modules/@tanstack/")) return "tanstack";
          if (id.includes("node_modules/oidc-client-ts/")) return "oidc";
          if (id.includes("node_modules/recharts/")) return "recharts";
          if (id.includes("node_modules/d3-")) return "d3";
          if (id.includes("node_modules/react-day-picker/")) return "datepicker";
          if (id.includes("node_modules/@tremor/")) return "tremor";
          return undefined;
        },
      },
    },
  },
  test: {
    environment: "jsdom",
    globals: true,
    setupFiles: ["./tests/setup.ts"],
    exclude: ["**/node_modules/**", "**/dist/**", "tests/e2e/**"],
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
        "src/auth/oidc.ts",
        "src/routeTree.gen.ts",
        "**/*.config.*",
        "tests/**",
      ],
    },
  },
});
