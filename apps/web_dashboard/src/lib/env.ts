import { z } from "zod";

const emptyToUndefined = (v: unknown) => (v === "" ? undefined : v);

const schema = z.object({
  VITE_API_BASE_URL: z.string().min(1).default("/api/v1"),
  VITE_AUTH_MODE: z.enum(["oidc", "local"]).default("oidc"),
  VITE_OIDC_AUTHORITY: z.preprocess(emptyToUndefined, z.string().url().optional()),
  VITE_OIDC_CLIENT_ID: z.preprocess(emptyToUndefined, z.string().min(1).optional()),
  VITE_OIDC_REDIRECT_URI: z.preprocess(emptyToUndefined, z.string().url().optional()),
  VITE_PRECOS_NOINDEX: z
    .enum(["true", "false"])
    .default("true")
    .transform((v) => v === "true"),
});

export type Env = z.infer<typeof schema>;

export function parseEnv(raw: Record<string, string | undefined>): Env {
  const result = schema.safeParse(raw);
  if (!result.success) {
    const issues = result.error.issues.map((i) => `${i.path.join(".")}: ${i.message}`).join("; ");
    throw new Error(`invalid env: ${issues}`);
  }
  return result.data;
}

export const env = parseEnv(import.meta.env);
