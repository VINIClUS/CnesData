import { createFileRoute } from "@tanstack/react-router";

import { LandingPage } from "@/components/landing/LandingPage";
import { landing } from "@/i18n/landing";

export const Route = createFileRoute("/")({
  head: () => ({
    meta: [
      { title: landing.meta.title },
      { name: "description", content: landing.meta.description },
    ],
  }),
  component: LandingPage,
});
