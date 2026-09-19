import { useCallback, useEffect, useId, useRef, useState } from "react";

export type MobileMenu = {
  open: boolean;
  id: string;
  buttonRef: React.RefObject<HTMLButtonElement>;
  toggle: () => void;
  close: () => void;
};

export function useMobileMenu(): MobileMenu {
  const [open, setOpen] = useState(false);
  const id = useId();
  const buttonRef = useRef<HTMLButtonElement>(null);
  const close = useCallback(() => setOpen(false), []);
  const toggle = useCallback(() => setOpen((o) => !o), []);

  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key !== "Escape") return;
      setOpen(false);
      buttonRef.current?.focus();
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [open]);

  return { open, id, buttonRef, toggle, close };
}
