import { createFileRoute } from "@tanstack/react-router";

import { AboutPage } from "@/components/about/AboutPage";
import { about } from "@/i18n/about";

export const Route = createFileRoute("/sobre")({
  head: () => ({
    meta: [{ title: about.meta.title }, { name: "description", content: about.meta.description }],
  }),
  component: AboutPage,
});
