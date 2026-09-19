import { createFileRoute } from "@tanstack/react-router";

import { LegalPage } from "@/components/legal/LegalPage";
import { legal } from "@/i18n/legal";

const _DOC = legal.termos;

export const Route = createFileRoute("/termos")({
  head: () => ({
    meta: [{ title: _DOC.meta.title }, { name: "description", content: _DOC.meta.description }],
  }),
  component: () => <LegalPage doc={_DOC} />,
});
