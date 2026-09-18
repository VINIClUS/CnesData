import type { ReactNode } from "react";

import { Eyebrow } from "./Eyebrow";
import { HeroTitle } from "./HeroTitle";
import { StageBadge } from "./StageBadge";
import type { Stage } from "./StageBadge";

type Props = {
  eyebrow: string;
  lead: string;
  highlight: string;
  description: string;
  stage?: Stage;
  children?: ReactNode;
};

export function PageHero({ eyebrow, lead, highlight, description, stage, children }: Props) {
  return (
    <section className="mx-auto w-full max-w-[1320px] px-6 pb-12 pt-4">
      <div className="max-w-3xl space-y-5">
        <div className="flex flex-wrap items-center gap-3">
          <Eyebrow>{eyebrow}</Eyebrow>
          {stage && <StageBadge stage={stage} />}
        </div>
        <HeroTitle lead={lead} highlight={highlight} />
        <p className="max-w-2xl text-base leading-relaxed text-muted-foreground">{description}</p>
        {children}
      </div>
    </section>
  );
}
