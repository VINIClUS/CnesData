import { CircleAlert, CircleCheck } from "lucide-react";

import type { ApiInterest } from "./contactInterest";
import type { LeadStatus } from "./useLeadForm";

import type { LeadFailure } from "@/api/marketingLeads";
import { RETRYABLE_FAILURES } from "@/api/marketingLeads";
import { Button } from "@/components/ui/button";
import { contact } from "@/i18n/contact";
import { marketing } from "@/i18n/marketing";

type Props = {
  status: LeadStatus;
  failure: LeadFailure | null;
  interest: ApiInterest;
  onRetry: () => void;
};

function mailHref(interest: ApiInterest) {
  const subject = encodeURIComponent(contact.mailSubject[interest]);
  return `mailto:${marketing.contactEmail}?subject=${subject}`;
}

export function LeadFormStatus({ status, failure, interest, onRetry }: Props) {
  return (
    <div role="status" aria-live="polite" className="text-sm">
      {status === "received" && (
        <div className="flex gap-3 rounded-lg border border-emerald-600/40 bg-emerald-600/10 p-4">
          <CircleCheck aria-hidden="true" className="mt-0.5 size-5 shrink-0 text-emerald-600" />
          <p>{contact.status.received}</p>
        </div>
      )}
      {status === "failed" && failure && (
        <div className="space-y-3 rounded-lg border border-destructive/40 bg-destructive/10 p-4">
          <div className="flex gap-3">
            <CircleAlert aria-hidden="true" className="mt-0.5 size-5 shrink-0 text-destructive" />
            <p>{contact.status.failed[failure]}</p>
          </div>
          <div className="flex flex-wrap gap-2">
            {RETRYABLE_FAILURES.includes(failure) && (
              <Button type="button" variant="outline" size="sm" onClick={onRetry}>
                {contact.status.retry}
              </Button>
            )}
            <Button type="button" variant="ghost" size="sm" asChild>
              <a href={mailHref(interest)}>{contact.status.sendEmail}</a>
            </Button>
          </div>
        </div>
      )}
    </div>
  );
}
