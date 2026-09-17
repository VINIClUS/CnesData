import { createFileRoute } from "@tanstack/react-router";

import { PricingPage } from "@/components/pricing/PricingPage";

export const Route = createFileRoute("/precos")({
  component: PricingPage,
});
