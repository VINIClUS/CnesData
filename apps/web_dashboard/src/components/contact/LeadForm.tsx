import { LoaderCircle } from "lucide-react";
import type { FormEvent } from "react";

import { LeadFields } from "./LeadFields";
import { LeadFormStatus } from "./LeadFormStatus";
import type { Interesse } from "./contactInterest";
import { useLeadForm } from "./useLeadForm";

import { Button } from "@/components/ui/button";
import { contact } from "@/i18n/contact";

export function LeadForm({ interesse }: { interesse: Interesse }) {
  const form = useLeadForm(interesse);
  const submitting = form.status === "submitting";
  const onSubmit = (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    void form.submit();
  };
  return (
    <div className="space-y-6">
      <h2 className="text-xl font-semibold">{contact.form.title}</h2>
      {form.status !== "received" && (
        <form noValidate onSubmit={onSubmit} className="space-y-5">
          <LeadFields
            values={form.values}
            errors={form.errors}
            disabled={submitting}
            setField={form.setField}
          />
          <p className="text-xs leading-relaxed text-muted-foreground">{contact.form.privacy}</p>
          <Button type="submit" size="lg" disabled={submitting}>
            {submitting && <LoaderCircle aria-hidden="true" className="animate-spin" />}
            {submitting ? contact.form.submitting : contact.form.submit}
          </Button>
        </form>
      )}
      <LeadFormStatus
        status={form.status}
        failure={form.failure}
        interest={form.values.interest}
        onRetry={() => void form.submit()}
      />
    </div>
  );
}
