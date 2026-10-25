export const SESSION_CHANGED = "cnesdata-session-changed";

let generation = 0;

export function sessionGeneration(): number {
  return generation;
}

export function notifySessionChange(): void {
  generation++;
  window.dispatchEvent(new Event(SESSION_CHANGED));
}
