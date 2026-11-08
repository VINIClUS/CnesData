import type { OidcMetadata, SignoutRedirectArgs, UserManagerSettings } from "oidc-client-ts";

import { stripTrailingSlash } from "@/lib/env";
import type { Env } from "@/lib/env";

export type OidcEnv = Pick<
  Env,
  | "VITE_RUNTIME_POLICY"
  | "VITE_OIDC_AUTHORITY"
  | "VITE_OIDC_CLIENT_ID"
  | "VITE_OIDC_REDIRECT_URI"
  | "VITE_OIDC_DOMAIN"
>;

type OidcClientIdentity = Pick<UserManagerSettings, "authority" | "client_id" | "redirect_uri">;

const NOT_CONFIGURED = "OIDC env not configured";
const OIDC_SCOPE = "openid profile email";
const API_RESOURCE = "https://api.cnesdata.com.br";
const API_SCOPE = `${API_RESOURCE}/api.access`;

function clientIdentity(env: OidcEnv): OidcClientIdentity {
  const { VITE_OIDC_AUTHORITY, VITE_OIDC_CLIENT_ID, VITE_OIDC_REDIRECT_URI } = env;
  if (!VITE_OIDC_AUTHORITY || !VITE_OIDC_CLIENT_ID || !VITE_OIDC_REDIRECT_URI) {
    throw new Error(NOT_CONFIGURED);
  }
  return {
    authority: VITE_OIDC_AUTHORITY,
    client_id: VITE_OIDC_CLIENT_ID,
    redirect_uri: VITE_OIDC_REDIRECT_URI,
  };
}

function cognitoMetadata(authority: string, domain: string | undefined): Partial<OidcMetadata> {
  if (!domain) throw new Error(NOT_CONFIGURED);
  const issuer = stripTrailingSlash(authority);
  return {
    issuer,
    authorization_endpoint: `${domain}/oauth2/authorize`,
    token_endpoint: `${domain}/oauth2/token`,
    userinfo_endpoint: `${domain}/oauth2/userInfo`,
    jwks_uri: `${issuer}/.well-known/jwks.json`,
    end_session_endpoint: `${domain}/logout`,
  };
}

export function buildOidcSettings(env: OidcEnv, origin: string): UserManagerSettings {
  const identity = clientIdentity(env);
  const common = {
    ...identity,
    response_type: "code",
    loadUserInfo: false,
    automaticSilentRenew: true,
  };
  if (env.VITE_RUNTIME_POLICY !== "production") {
    return { ...common, scope: OIDC_SCOPE, post_logout_redirect_uri: origin };
  }
  return {
    ...common,
    scope: `${OIDC_SCOPE} ${API_SCOPE}`,
    extraQueryParams: { resource: API_RESOURCE },
    metadata: cognitoMetadata(identity.authority, env.VITE_OIDC_DOMAIN),
  };
}

export function buildSignoutArgs(env: OidcEnv, origin: string): SignoutRedirectArgs {
  if (env.VITE_RUNTIME_POLICY !== "production") return {};
  const { client_id } = clientIdentity(env);
  return { extraQueryParams: { client_id, logout_uri: origin } };
}
