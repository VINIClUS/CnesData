import { UserManager, WebStorageStateStore } from "oidc-client-ts";

import { env } from "@/lib/env";

function _config() {
  if (!env.VITE_OIDC_AUTHORITY || !env.VITE_OIDC_CLIENT_ID || !env.VITE_OIDC_REDIRECT_URI) {
    throw new Error("OIDC env not configured");
  }
  return {
    authority: env.VITE_OIDC_AUTHORITY,
    client_id: env.VITE_OIDC_CLIENT_ID,
    redirect_uri: env.VITE_OIDC_REDIRECT_URI,
    response_type: "code",
    scope: "openid profile email",
    post_logout_redirect_uri: window.location.origin,
    userStore: new WebStorageStateStore({ store: window.sessionStorage }),
    loadUserInfo: false,
    automaticSilentRenew: true,
  };
}

let _userManager: UserManager | null = null;

function _manager(): UserManager {
  if (_userManager === null) {
    _userManager = new UserManager(_config());
  }
  return _userManager;
}

export async function startLogin(): Promise<void> {
  await _manager().signinRedirect();
}

export async function completeLogin(): Promise<void> {
  await _manager().signinRedirectCallback();
}

export async function logout(): Promise<void> {
  await _manager().signoutRedirect();
}

export async function getAccessToken(): Promise<string | null> {
  const u = await _manager().getUser();
  if (!u || u.expired) return null;
  return u.access_token;
}
