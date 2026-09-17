import {
  RouterProvider,
  createMemoryHistory,
  createRootRoute,
  createRouter,
} from "@tanstack/react-router";
import { render } from "@testing-library/react";
import type { ReactNode } from "react";

import { ThemeProvider } from "@/theme/ThemeProvider";

export function renderWithRouter(ui: ReactNode, initialPath = "/") {
  const root = createRootRoute({ component: () => <ThemeProvider>{ui}</ThemeProvider> });
  const router = createRouter({
    routeTree: root,
    history: createMemoryHistory({ initialEntries: [initialPath] }),
  });
  const result = render(<RouterProvider router={router} />);
  return { ...result, router };
}
