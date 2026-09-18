import type { LucideIcon } from "lucide-react";
import type { ComponentProps, ReactNode } from "react";

import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { cn } from "@/lib/utils";

export type LoginFieldProps = {
  id: string;
  label: string;
  icon: LucideIcon;
  trailing?: ReactNode;
} & Omit<ComponentProps<"input">, "id">;

export function LoginField({
  id,
  label,
  icon: Icon,
  trailing,
  className,
  ...rest
}: LoginFieldProps) {
  return (
    <div className="space-y-2">
      <Label htmlFor={id} className="text-sm">
        {label}
      </Label>
      <div className="relative">
        <Icon
          aria-hidden="true"
          className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-muted-foreground"
        />
        <Input
          id={id}
          className={cn("h-12 bg-white/[0.03] pl-10", trailing && "pr-11", className)}
          {...rest}
        />
        {trailing && <div className="absolute right-2 top-1/2 -translate-y-1/2">{trailing}</div>}
      </div>
    </div>
  );
}
