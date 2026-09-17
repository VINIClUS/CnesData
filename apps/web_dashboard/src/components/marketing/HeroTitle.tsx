import { cn } from "@/lib/utils";

type Props = { lead: string; highlight: string; className?: string };

export function HeroTitle({ lead, highlight, className }: Props) {
  return (
    <h1
      className={cn(
        "text-4xl font-semibold leading-[1.15] tracking-tight lg:text-[40px]",
        className,
      )}
    >
      {lead} <span className="block text-primary">{highlight}</span>
    </h1>
  );
}
