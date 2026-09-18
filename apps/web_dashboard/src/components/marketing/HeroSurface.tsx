import type { ReactNode } from "react";

import { HeroBackground } from "./HeroBackground";

import { cn } from "@/lib/utils";

export function HeroSurface({ children, className }: { children: ReactNode; className?: string }) {
  return (
    <div className={cn("bg-hero-navy dark relative overflow-hidden text-foreground", className)}>
      <HeroBackground />
      <div className="relative flex flex-1 flex-col">{children}</div>
    </div>
  );
}
