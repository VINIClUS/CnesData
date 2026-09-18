import type { LucideIcon } from "lucide-react";

import { IconBadge, type IconBadgeSize } from "./IconBadge";

export type FeatureItemProps = {
  icon: LucideIcon;
  title: string;
  description: string;
  layout?: "row" | "card";
  badgeSize?: IconBadgeSize;
};

export function FeatureItem({
  icon,
  title,
  description,
  layout = "row",
  badgeSize = "md",
}: FeatureItemProps) {
  if (layout === "card") {
    return (
      <article className="rounded-xl border bg-card p-6 shadow-sm">
        <IconBadge icon={icon} size={badgeSize} />
        <h3 className="mt-5 text-base font-semibold">{title}</h3>
        <p className="mt-2 text-sm leading-relaxed text-muted-foreground">{description}</p>
      </article>
    );
  }
  return (
    <div className="flex items-start gap-4">
      <IconBadge icon={icon} size={badgeSize} />
      <div>
        <h3 className="text-base font-semibold">{title}</h3>
        <p className="mt-1 text-sm leading-relaxed text-muted-foreground">{description}</p>
      </div>
    </div>
  );
}
