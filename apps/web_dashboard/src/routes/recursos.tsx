import { createFileRoute } from "@tanstack/react-router";

import { ResourcesPage } from "@/components/resources/ResourcesPage";
import { resources } from "@/i18n/resources";

export const Route = createFileRoute("/recursos")({
  head: () => ({
    meta: [
      { title: resources.meta.title },
      { name: "description", content: resources.meta.description },
    ],
  }),
  component: ResourcesPage,
});
