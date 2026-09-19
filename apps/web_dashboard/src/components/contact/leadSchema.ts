import { z } from "zod";

import { API_INTERESTS } from "./contactInterest";

import { contact } from "@/i18n/contact";

const _E = contact.errors;

export const leadSchema = z.object({
  name: z.string().trim().min(1, _E.nameRequired).max(120, _E.nameMax),
  email: z
    .string()
    .trim()
    .min(1, _E.emailRequired)
    .max(254, _E.emailMax)
    .pipe(z.email(_E.emailInvalid)),
  interest: z.enum(API_INTERESTS),
  organization: z.string().trim().max(200, _E.organizationMax),
  role: z.string().trim().max(120, _E.roleMax),
  message: z.string().trim().max(2000, _E.messageMax),
});

export type LeadValues = z.input<typeof leadSchema>;
export type Lead = z.output<typeof leadSchema>;
export type LeadField = keyof Lead;
export type LeadFieldErrors = Partial<Record<LeadField, string>>;

export const LEAD_FIELD_ORDER: readonly LeadField[] = [
  "name",
  "email",
  "interest",
  "organization",
  "role",
  "message",
];

export type LeadValidation = { ok: true; lead: Lead } | { ok: false; errors: LeadFieldErrors };

export function validateLead(values: LeadValues): LeadValidation {
  const result = leadSchema.safeParse(values);
  if (result.success) return { ok: true, lead: result.data };
  const errors: LeadFieldErrors = {};
  for (const issue of result.error.issues) {
    const key = issue.path[0] as LeadField | undefined;
    if (key && !errors[key]) errors[key] = issue.message;
  }
  return { ok: false, errors };
}
