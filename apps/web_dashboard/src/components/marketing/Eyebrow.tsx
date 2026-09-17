import type { ReactNode } from "react";

import { cn } from "@/lib/utils";

export function Eyebrow({ children, className }: { children: ReactNode; className?: string }) {
  return (
    <p
      className={cn(
        "text-[11px] font-medium uppercase tracking-[0.2em] text-muted-foreground",
        className,
      )}
    >
      {children}
    </p>
  );
}
