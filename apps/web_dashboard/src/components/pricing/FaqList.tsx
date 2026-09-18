import { Minus, Plus } from "lucide-react";
import { useState } from "react";

type Item = { question: string; answer: string };

export function FaqList({ items }: { items: readonly Item[] }) {
  const [open, setOpen] = useState<number | null>(null);
  return (
    <ul className="grid grid-cols-1 gap-3 md:grid-cols-2">
      {items.map((item, i) => {
        const isOpen = open === i;
        const Icon = isOpen ? Minus : Plus;
        const panelId = `faq-panel-${i}`;
        return (
          <li key={item.question} className="rounded-lg border bg-card">
            <button
              type="button"
              aria-expanded={isOpen}
              aria-controls={panelId}
              onClick={() => setOpen(isOpen ? null : i)}
              className="flex w-full items-center justify-between gap-4 px-4 py-3 text-left text-sm font-medium"
            >
              {item.question}
              <Icon aria-hidden="true" className="size-4 shrink-0 text-muted-foreground" />
            </button>
            <div id={panelId} hidden={!isOpen} className="px-4 pb-4 text-sm text-muted-foreground">
              {item.answer}
            </div>
          </li>
        );
      })}
    </ul>
  );
}
