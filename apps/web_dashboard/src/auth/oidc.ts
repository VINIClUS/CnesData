import { UserManager, WebStorageStateStore } from "oidc-client-ts";

import { buildOidcSettings, buildSignoutArgs } from "@/auth/oidcSettings";
import { notifySessionChange } from "@/auth/sessionEvents";
import { env } from "@/lib/env";

let _userManager: UserManager | null = null;

function _manager(): UserManager {
  if (_userManager === null) {
    _userManager = new UserManager({
      ...buildOidcSettings(env, window.location.origin),
      userStore: new WebStorageStateStore({ store: window.sessionStorage }),
    });
    _userManager.events.addUserLoaded(notifySessionChange);
    _userManager.events.addUserUnloaded(notifySessionChange);
    _userManager.events.addAccessTokenExpired(notifySessionChange);
  }
  return _userManager;
}

export type StartLoginOptions = { loginHint?: string };

export async function startLogin(opts: StartLoginOptions = {}): Promise<void> {
  const args = opts.loginHint ? { login_hint: opts.loginHint } : {};
  await _manager().signinRedirect(args);
}

export async function completeLogin(): Promise<void> {
  await _manager().signinRedirectCallback();
}

export async function logout(): Promise<void> {
  await _manager().signoutRedirect(buildSignoutArgs(env, window.location.origin));
}

export async function getAccessToken(): Promise<string | null> {
  if (!env.VITE_OIDC_AUTHORITY || !env.VITE_OIDC_CLIENT_ID || !env.VITE_OIDC_REDIRECT_URI) {
    return null;
  }
  const u = await _manager().getUser();
  if (!u || u.expired) return null;
  return u.access_token;
}
