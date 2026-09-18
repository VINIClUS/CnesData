import { Wrench } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { marketing } from "@/i18n/marketing";
import { cn } from "@/lib/utils";

export type Stage = keyof typeof marketing.stage;

export function StageBadge({
  stage = "desenvolvimento",
  className,
}: {
  stage?: Stage;
  className?: string;
}) {
  return (
    <Badge
      variant="outline"
      className={cn("gap-1.5 border-primary/40 font-medium text-primary", className)}
    >
      <Wrench aria-hidden="true" className="size-3" />
      {marketing.stage[stage]}
    </Badge>
  );
}
