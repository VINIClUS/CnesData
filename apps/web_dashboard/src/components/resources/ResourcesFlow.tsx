import { Database, Search, Workflow } from "lucide-react";
import type { LucideIcon } from "lucide-react";

import { IconBadge } from "@/components/marketing/IconBadge";
import { SectionHeading } from "@/components/marketing/SectionHeading";
import { StageBadge } from "@/components/marketing/StageBadge";
import { resources } from "@/i18n/resources";

const _ICONS: LucideIcon[] = [Database, Workflow, Search];

export function ResourcesFlow() {
  const f = resources.flow;
  return (
    <section id="fluxo" className="border-t">
      <div className="mx-auto w-full max-w-[1320px] space-y-10 px-6 py-14">
        <SectionHeading
          eyebrow={f.eyebrow}
          title={f.title}
          description={f.description}
          className="max-w-2xl"
        />
        <ol className="grid grid-cols-1 gap-5 md:grid-cols-3">
          {f.steps.map((s, i) => (
            <li key={s.title} className="rounded-xl border bg-card p-6 shadow-sm">
              <IconBadge icon={_ICONS[i] ?? Database} />
              <h3 className="mt-5 text-base font-semibold">
                {i + 1}. {s.title}
              </h3>
              <p className="mt-2 text-sm leading-relaxed text-muted-foreground">{s.description}</p>
              <StageBadge stage={s.stage} className="mt-4" />
            </li>
          ))}
        </ol>
      </div>
    </section>
  );
}
