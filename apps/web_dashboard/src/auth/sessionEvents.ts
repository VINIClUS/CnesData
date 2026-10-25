export const SESSION_CHANGED = "cnesdata-session-changed";

export function notifySessionChange(): void {
  window.dispatchEvent(new Event(SESSION_CHANGED));
}
