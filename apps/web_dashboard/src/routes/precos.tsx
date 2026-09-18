import { createFileRoute } from "@tanstack/react-router";

import { PricingPage } from "@/components/pricing/PricingPage";

export const Route = createFileRoute("/precos")({
  head: () => ({
    meta: [{ title: "Planos e preços | CnesData" }, { name: "robots", content: "noindex" }],
  }),
  component: PricingPage,
});
