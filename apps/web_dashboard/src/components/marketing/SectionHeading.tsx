import { Eyebrow } from "./Eyebrow";

import { cn } from "@/lib/utils";

type Props = { eyebrow: string; title: string; description: string; className?: string };

export function SectionHeading({ eyebrow, title, description, className }: Props) {
  return (
    <div className={cn("space-y-3", className)}>
      <Eyebrow>{eyebrow}</Eyebrow>
      <h2 className="text-2xl font-semibold leading-tight tracking-tight lg:text-[28px]">
        {title}
      </h2>
      <p className="text-sm leading-relaxed text-muted-foreground">{description}</p>
    </div>
  );
}
