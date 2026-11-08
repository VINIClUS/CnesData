import { z } from "zod";

const PRODUCTION_API_BASE_URL = "https://api.cnesdata.com.br/api/v1";
const PRODUCTION_OIDC_DOMAIN_SHAPE = "https://<prefix>.auth.us-east-2.amazoncognito.com";
const PRODUCTION_OIDC_DOMAIN =
  /^https:\/\/[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.auth\.us-east-2\.amazoncognito\.com$/;

const emptyToUndefined = (v: unknown) => (v === "" ? undefined : v);

export const stripTrailingSlash = (value: string): string => value.replace(/\/$/, "");

type ProductionOidc = { VITE_OIDC_AUTHORITY?: string; VITE_OIDC_DOMAIN?: string };

function productionOidcDomainValid({ VITE_OIDC_AUTHORITY, VITE_OIDC_DOMAIN }: ProductionOidc) {
  if (VITE_OIDC_DOMAIN === undefined) return VITE_OIDC_AUTHORITY === undefined;
  return PRODUCTION_OIDC_DOMAIN.test(VITE_OIDC_DOMAIN);
}

const schema = z
  .object({
    VITE_RUNTIME_POLICY: z.preprocess(
      emptyToUndefined,
      z.enum(["generic", "production"]).default("generic"),
    ),
    VITE_API_BASE_URL: z.string().min(1).default("/api/v1"),
    VITE_AUTH_MODE: z.enum(["oidc", "local"]).default("oidc"),
    VITE_OIDC_AUTHORITY: z.preprocess(emptyToUndefined, z.string().url().optional()),
    VITE_OIDC_CLIENT_ID: z.preprocess(emptyToUndefined, z.string().min(1).optional()),
    VITE_OIDC_REDIRECT_URI: z.preprocess(emptyToUndefined, z.string().url().optional()),
    VITE_OIDC_DOMAIN: z.preprocess(emptyToUndefined, z.string().optional()),
    VITE_PRECOS_NOINDEX: z
      .enum(["true", "false"])
      .default("true")
      .transform((v) => v === "true"),
  })
  .superRefine((value, ctx) => {
    if (value.VITE_RUNTIME_POLICY !== "production") return;
    if (stripTrailingSlash(value.VITE_API_BASE_URL) !== PRODUCTION_API_BASE_URL) {
      ctx.addIssue({
        code: "custom",
        path: ["VITE_API_BASE_URL"],
        message: `production_requires=${PRODUCTION_API_BASE_URL}`,
      });
    }
    if (value.VITE_AUTH_MODE !== "oidc") {
      ctx.addIssue({
        code: "custom",
        path: ["VITE_AUTH_MODE"],
        message: "production_requires=oidc",
      });
    }
    if (!productionOidcDomainValid(value)) {
      ctx.addIssue({
        code: "custom",
        path: ["VITE_OIDC_DOMAIN"],
        message: `production_requires=${PRODUCTION_OIDC_DOMAIN_SHAPE}`,
      });
    }
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
