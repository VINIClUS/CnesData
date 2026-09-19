import { createFileRoute } from "@tanstack/react-router";

import { PricingPage } from "@/components/pricing/PricingPage";
import { env } from "@/lib/env";

const _ROBOTS = env.VITE_PRECOS_NOINDEX ? [{ name: "robots", content: "noindex" }] : [];

export const Route = createFileRoute("/precos")({
  head: () => ({ meta: [{ title: "Planos e preços | CnesData" }, ..._ROBOTS] }),
  component: PricingPage,
});
