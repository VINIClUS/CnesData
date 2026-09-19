import { Lightbulb, Scale } from "lucide-react";
import type { LucideIcon } from "lucide-react";

import { StageBadge } from "@/components/marketing/StageBadge";
import { about } from "@/i18n/about";

type Section = { id: string; title: string; paragraphs: readonly string[] };
type Highlight = { title: string; text: string };

function EditorialSection({ section }: { section: Section }) {
  return (
    <section id={section.id} className="space-y-3">
      <h2 className="text-2xl font-semibold leading-tight tracking-tight">{section.title}</h2>
      {section.paragraphs.map((p) => (
        <p key={p} className="text-base leading-relaxed text-muted-foreground">
          {p}
        </p>
      ))}
      {section.id === "estagio" && <StageBadge />}
    </section>
  );
}

function AboutHighlight({ icon: Icon, item }: { icon: LucideIcon; item: Highlight }) {
  return (
    <aside className="flex gap-4 rounded-r-xl border-l-4 border-primary bg-muted/40 p-5">
      <Icon aria-hidden="true" className="mt-0.5 size-5 shrink-0 text-primary" />
      <div>
        <p className="text-sm font-semibold">{item.title}</p>
        <p className="mt-1 text-sm leading-relaxed text-muted-foreground">{item.text}</p>
      </div>
    </aside>
  );
}

export function AboutBody() {
  const [problema, publico, abordagem, estagio, participar] = about.sections;
  return (
    <div className="mx-auto w-full max-w-[1320px] px-6 py-14">
      <article className="mx-auto max-w-[720px] space-y-10">
        <EditorialSection section={problema} />
        <AboutHighlight icon={Lightbulb} item={about.highlights.motivation} />
        <EditorialSection section={publico} />
        <EditorialSection section={abordagem} />
        <AboutHighlight icon={Scale} item={about.highlights.independence} />
        <EditorialSection section={estagio} />
        <EditorialSection section={participar} />
      </article>
    </div>
  );
}
