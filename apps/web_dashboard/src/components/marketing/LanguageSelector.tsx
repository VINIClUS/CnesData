import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { marketing } from "@/i18n/marketing";
import { cn } from "@/lib/utils";

const _LOCALE = "pt-BR";

export function LanguageSelector({ className }: { className?: string }) {
  return (
    <Select value={_LOCALE}>
      <SelectTrigger
        aria-label="Idioma"
        className={cn(
          "h-8 w-auto gap-1 border-0 bg-transparent px-2 text-sm shadow-none focus:ring-0 focus:ring-offset-0",
          className,
        )}
      >
        <SelectValue />
      </SelectTrigger>
      <SelectContent className="dark">
        <SelectItem value={_LOCALE}>{marketing.language}</SelectItem>
      </SelectContent>
    </Select>
  );
}
