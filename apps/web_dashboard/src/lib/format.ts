import { format, formatDistanceToNow } from "date-fns";
import { ptBR } from "date-fns/locale";

const _MES_PT: Record<number, string> = {
  1: "jan",
  2: "fev",
  3: "mar",
  4: "abr",
  5: "mai",
  6: "jun",
  7: "jul",
  8: "ago",
  9: "set",
  10: "out",
  11: "nov",
  12: "dez",
};

const _BRL = new Intl.NumberFormat("pt-BR", {
  style: "currency",
  currency: "BRL",
  minimumFractionDigits: 2,
});

export function formatBRL(cents: number): string {
  return _BRL.format(cents / 100);
}

export function formatCompetenciaBR(yyyymm: number): string {
  const ano = Math.floor(yyyymm / 100);
  const mes = yyyymm % 100;
  return `${_MES_PT[mes]}/${ano}`;
}

export function formatLagMeses(meses: number): string {
  if (meses === 0) return "em dia";
  if (meses === 1) return "1 mês atrás";
  return `${meses} meses atrás`;
}

export function formatRelativeTime(d: Date, _now: Date = new Date()): string {
  const out = formatDistanceToNow(d, {
    addSuffix: true,
    locale: ptBR,
  });
  return out.replace(/^aproximadamente /, "");
}

export function formatDateBR(d: Date): string {
  return format(d, "dd/MM/yyyy", { locale: ptBR });
}
