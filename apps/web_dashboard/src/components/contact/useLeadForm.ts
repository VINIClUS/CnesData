import { useCallback, useRef, useState } from "react";

import { INTEREST_BY_INTERESSE } from "./contactInterest";
import type { Interesse } from "./contactInterest";
import { LEAD_FIELD_ORDER, validateLead } from "./leadSchema";
import type { LeadField, LeadFieldErrors, LeadValues } from "./leadSchema";

import { submitLead } from "@/api/marketingLeads";
import type { LeadFailure } from "@/api/marketingLeads";

export type LeadStatus = "idle" | "submitting" | "received" | "failed";

export const LEAD_FIELD_ID: Record<LeadField, string> = {
  name: "lead-name",
  email: "lead-email",
  interest: "lead-interest",
  organization: "lead-organization",
  role: "lead-role",
  message: "lead-message",
};

function initialValues(interesse: Interesse): LeadValues {
  return {
    name: "",
    email: "",
    interest: INTEREST_BY_INTERESSE[interesse],
    organization: "",
    role: "",
    message: "",
  };
}

function focusFirstError(errors: LeadFieldErrors) {
  const first = LEAD_FIELD_ORDER.find((f) => errors[f]);
  if (first) document.getElementById(LEAD_FIELD_ID[first])?.focus();
}

export function useLeadForm(interesse: Interesse) {
  const [values, setValues] = useState<LeadValues>(() => initialValues(interesse));
  const [errors, setErrors] = useState<LeadFieldErrors>({});
  const [status, setStatus] = useState<LeadStatus>("idle");
  const [failure, setFailure] = useState<LeadFailure | null>(null);
  const inFlight = useRef(false);

  const setField = useCallback((field: LeadField, value: string) => {
    setValues((v) => ({ ...v, [field]: value }));
    setErrors((e) => (e[field] ? { ...e, [field]: undefined } : e));
  }, []);

  const submit = useCallback(async () => {
    if (inFlight.current) return;
    const result = validateLead(values);
    if (!result.ok) {
      setErrors(result.errors);
      focusFirstError(result.errors);
      return;
    }
    inFlight.current = true;
    setStatus("submitting");
    const outcome = await submitLead({
      ...result.lead,
      municipality: "",
      source_path: "/contato",
      source_cta: interesse,
      privacy_notice_version: "1",
      newsletter_opt_in: false,
    });
    inFlight.current = false;
    setFailure(outcome.ok ? null : outcome.reason);
    setStatus(outcome.ok ? "received" : "failed");
  }, [values, interesse]);

  return { values, errors, status, failure, setField, submit };
}
