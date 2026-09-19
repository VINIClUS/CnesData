import { createFileRoute } from "@tanstack/react-router";

import { ContactPage } from "@/components/contact/ContactPage";
import { contatoSearchSchema } from "@/components/contact/contactInterest";
import { contact } from "@/i18n/contact";

function ContatoRoute() {
  const { interesse } = Route.useSearch();
  return <ContactPage interesse={interesse} />;
}

export const Route = createFileRoute("/contato")({
  validateSearch: contatoSearchSchema,
  head: () => ({
    meta: [
      { title: contact.meta.title },
      { name: "description", content: contact.meta.description },
    ],
  }),
  component: ContatoRoute,
});
