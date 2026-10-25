export type LegacyMe = {
  user_id: string;
  email: string;
  display_name: string | null;
  role: "gestor" | "admin";
  tenant_ids: string[];
  has_pending_request: boolean;
};

export type AwsMe = {
  subject: string;
  issuer: string;
  email: string | null;
  display_name: string | null;
};

export type Me = LegacyMe | AwsMe;

export function isAwsMe(user: Me): user is AwsMe {
  return "subject" in user && "issuer" in user;
}

export function principalKey(user: Me | null): readonly string[] {
  if (!user) return ["anonymous"];
  return isAwsMe(user) ? ["aws", user.issuer, user.subject] : ["legacy", user.user_id];
}

export type LocalPrincipal = {
  user_id: string;
  email: string;
  tenant_id: string;
  role: "gestor" | "admin";
};
