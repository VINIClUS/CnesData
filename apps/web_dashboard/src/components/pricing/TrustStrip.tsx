import { FileText, Headset, Settings, ShieldCheck } from "lucide-react";
import type { LucideIcon } from "lucide-react";

import { FeatureItem } from "@/components/marketing/FeatureItem";
import { pricing } from "@/i18n/pricing";

const _ICONS: LucideIcon[] = [ShieldCheck, FileText, Settings, Headset];

export function TrustStrip() {
  return (
    <section className="bg-background">
      <div className="mx-auto w-full max-w-[1320px] px-6 pb-10">
        <div className="grid grid-cols-1 gap-6 rounded-2xl bg-muted/50 px-8 py-6 md:grid-cols-2 lg:grid-cols-4">
          {pricing.trust.map((t, i) => (
            <FeatureItem key={t.title} icon={_ICONS[i] ?? ShieldCheck} badgeSize="sm" {...t} />
          ))}
        </div>
      </div>
    </section>
  );
}
