import type { ChangeEvent } from "react";

import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { contact } from "@/i18n/contact";

type Props = {
  id: string;
  label: string;
  value: string;
  onChange: (value: string) => void;
  error?: string;
  hint?: string;
  optional?: boolean;
  multiline?: boolean;
  disabled?: boolean;
  type?: string;
  maxLength?: number;
};

export function LeadField({ id, label, value, onChange, error, hint, ...opts }: Props) {
  const describedBy =
    [error && `${id}-erro`, hint && `${id}-dica`].filter(Boolean).join(" ") || undefined;
  const common = {
    id,
    value,
    disabled: opts.disabled,
    maxLength: opts.maxLength,
    "aria-invalid": error ? true : undefined,
    "aria-describedby": describedBy,
    onChange: (e: ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) => onChange(e.target.value),
  };
  return (
    <div className="space-y-1.5">
      <Label htmlFor={id}>
        {label}
        {opts.optional && (
          <span className="ml-1 font-normal text-muted-foreground">{contact.form.optional}</span>
        )}
      </Label>
      {opts.multiline ? <Textarea {...common} /> : <Input type={opts.type ?? "text"} {...common} />}
      {hint && (
        <p id={`${id}-dica`} className="text-xs text-muted-foreground">
          {hint}
        </p>
      )}
      {error && (
        <p id={`${id}-erro`} className="text-sm text-destructive">
          {error}
        </p>
      )}
    </div>
  );
}
