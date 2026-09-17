import type { LucideIcon } from "lucide-react";

import { cn } from "@/lib/utils";

export type IconBadgeSize = "sm" | "md" | "lg";

const _SIZE: Record<IconBadgeSize, string> = {
  sm: "size-10 [&_svg]:size-4",
  md: "size-14 [&_svg]:size-6",
  lg: "size-20 [&_svg]:size-8",
};

type Props = { icon: LucideIcon; size?: IconBadgeSize; className?: string };

export function IconBadge({ icon: Icon, size = "md", className }: Props) {
  return (
    <span
      className={cn(
        "inline-flex shrink-0 items-center justify-center rounded-full bg-primary/10 text-primary dark:bg-primary/15",
        _SIZE[size],
        className,
      )}
    >
      <Icon aria-hidden="true" />
    </span>
  );
}
