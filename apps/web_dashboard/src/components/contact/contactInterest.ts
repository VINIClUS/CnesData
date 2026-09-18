import { z } from "zod";

export const INTERESSES = ["acesso-antecipado", "piloto", "contato"] as const;
export type Interesse = (typeof INTERESSES)[number];

export const contatoSearchSchema = z.object({
  interesse: z.enum(INTERESSES).default("contato").catch("contato"),
});

export const API_INTERESTS = ["early_access", "pilot", "contact"] as const;
export type ApiInterest = (typeof API_INTERESTS)[number];

export const INTEREST_BY_INTERESSE: Record<Interesse, ApiInterest> = {
  "acesso-antecipado": "early_access",
  piloto: "pilot",
  contato: "contact",
};
