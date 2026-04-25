const _MS_PER_MONTH = 1000 * 60 * 60 * 24 * 30;

export function lagInMonths(extracaoTs: Date, now: Date = new Date()): number {
  const diffMs = now.getTime() - extracaoTs.getTime();
  return Math.floor(diffMs / _MS_PER_MONTH);
}
