export type Me = {
  user_id: string;
  email: string;
  display_name: string | null;
  role: "gestor" | "admin";
  tenant_ids: string[];
  has_pending_request: boolean;
};

export type LocalPrincipal = {
  user_id: string;
  email: string;
  tenant_id: string;
  role: "gestor" | "admin";
};
