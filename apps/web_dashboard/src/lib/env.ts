import { z } from "zod";

const schema = z.object({
  VITE_API_BASE_URL: z.string().min(1).default("/api/v1"),
  VITE_OIDC_AUTHORITY: z.string().url().optional(),
  VITE_OIDC_CLIENT_ID: z.string().min(1).optional(),
  VITE_OIDC_REDIRECT_URI: z.string().url().optional(),
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
