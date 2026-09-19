import { HeadContent, Outlet, createRootRoute } from "@tanstack/react-router";
import { createPortal } from "react-dom";

import { marketing } from "@/i18n/marketing";

function RootLayout() {
  return (
    <>
      {createPortal(<HeadContent />, document.head)}
      <Outlet />
    </>
  );
}

export const Route = createRootRoute({
  head: () => ({ meta: [{ title: marketing.meta.defaultTitle }] }),
  component: RootLayout,
});
