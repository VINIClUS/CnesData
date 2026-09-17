import { Check } from "lucide-react";
import type { ReactNode } from "react";

import { cn } from "@/lib/utils";

export function CheckBullet({ children, className }: { children: ReactNode; className?: string }) {
  return (
    <li className={cn("flex items-start gap-2 text-sm", className)}>
      <Check aria-hidden="true" className="mt-0.5 size-4 shrink-0 text-primary" />
      <span>{children}</span>
    </li>
  );
}
