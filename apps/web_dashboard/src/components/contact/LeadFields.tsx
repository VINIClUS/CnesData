import { LeadField } from "./LeadField";
import { API_INTERESTS } from "./contactInterest";
import type { LeadField as LeadFieldName, LeadFieldErrors, LeadValues } from "./leadSchema";
import { LEAD_FIELD_ID } from "./useLeadForm";

import { Label } from "@/components/ui/label";
import { contact } from "@/i18n/contact";

const _SELECT =
  "flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-base ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50 md:text-sm";

type Props = {
  values: LeadValues;
  errors: LeadFieldErrors;
  disabled: boolean;
  setField: (field: LeadFieldName, value: string) => void;
};

export function LeadFields({ values, errors, disabled, setField }: Props) {
  const l = contact.form.labels;
  const id = LEAD_FIELD_ID;
  return (
    <div className="space-y-4">
      <LeadField
        id={id.name}
        label={l.name}
        value={values.name}
        error={errors.name}
        disabled={disabled}
        maxLength={120}
        onChange={(v) => setField("name", v)}
      />
      <LeadField
        id={id.email}
        label={l.email}
        value={values.email}
        error={errors.email}
        disabled={disabled}
        type="email"
        maxLength={254}
        onChange={(v) => setField("email", v)}
      />
      <div className="space-y-1.5">
        <Label htmlFor={id.interest}>{l.interest}</Label>
        <select
          id={id.interest}
          value={values.interest}
          disabled={disabled}
          className={_SELECT}
          onChange={(e) => setField("interest", e.target.value)}
        >
          {API_INTERESTS.map((k) => (
            <option key={k} value={k}>
              {contact.form.interests[k]}
            </option>
          ))}
        </select>
      </div>
      <LeadField
        id={id.organization}
        label={l.organization}
        value={values.organization}
        error={errors.organization}
        optional
        disabled={disabled}
        maxLength={200}
        onChange={(v) => setField("organization", v)}
      />
      <LeadField
        id={id.role}
        label={l.role}
        value={values.role}
        error={errors.role}
        optional
        disabled={disabled}
        maxLength={120}
        onChange={(v) => setField("role", v)}
      />
      <LeadField
        id={id.message}
        label={l.message}
        value={values.message}
        error={errors.message}
        hint={contact.form.messageHint}
        optional
        multiline
        disabled={disabled}
        maxLength={2000}
        onChange={(v) => setField("message", v)}
      />
    </div>
  );
}
