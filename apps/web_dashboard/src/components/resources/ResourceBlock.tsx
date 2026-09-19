import type { ReactNode } from "react";

import { CheckBullet } from "@/components/marketing/CheckBullet";
import { SectionHeading } from "@/components/marketing/SectionHeading";
import { StageBadge } from "@/components/marketing/StageBadge";
import type { Stage } from "@/components/marketing/StageBadge";
import { cn } from "@/lib/utils";

type Block = {
  id: string;
  eyebrow: string;
  title: string;
  description: string;
  bullets: readonly string[];
  stage: Stage;
};

type Props = { block: Block; reversed?: boolean; visual: ReactNode };

export function ResourceBlock({ block, reversed = false, visual }: Props) {
  return (
    <section id={block.id} className={cn("border-t", reversed && "bg-muted/40")}>
      <div className="mx-auto grid w-full max-w-[1320px] grid-cols-1 items-center gap-10 px-6 py-14 lg:grid-cols-2">
        <div className={cn("space-y-6", reversed && "lg:order-last")}>
          <SectionHeading
            eyebrow={block.eyebrow}
            title={block.title}
            description={block.description}
          />
          <StageBadge stage={block.stage} />
          <ul className="space-y-2">
            {block.bullets.map((b) => (
              <CheckBullet key={b}>{b}</CheckBullet>
            ))}
          </ul>
        </div>
        {visual}
      </div>
    </section>
  );
}
