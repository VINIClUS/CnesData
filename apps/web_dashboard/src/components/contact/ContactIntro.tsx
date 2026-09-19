import { ShieldAlert } from "lucide-react";

import { CheckBullet } from "@/components/marketing/CheckBullet";
import { SectionHeading } from "@/components/marketing/SectionHeading";
import { StageBadge } from "@/components/marketing/StageBadge";
import { contact } from "@/i18n/contact";

export function ContactIntro() {
  const i = contact.intro;
  return (
    <div className="space-y-6">
      <SectionHeading eyebrow={i.eyebrow} title={i.title} description={i.description} />
      <StageBadge />
      <ul className="space-y-3">
        {i.bullets.map((b) => (
          <CheckBullet key={b}>{b}</CheckBullet>
        ))}
      </ul>
      <p className="flex items-start gap-2 rounded-lg border bg-muted/40 p-4 text-sm text-muted-foreground">
        <ShieldAlert aria-hidden="true" className="mt-0.5 size-4 shrink-0 text-primary" />
        <span>{i.sensitive}</span>
      </p>
    </div>
  );
}
