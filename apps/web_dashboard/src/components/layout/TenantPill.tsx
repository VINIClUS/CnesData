type Props = { nome: string; uf: string; ibge6: string };

export function TenantPill({ nome, uf, ibge6 }: Props) {
  return (
    <span className="inline-flex items-center gap-2 rounded-full border bg-primary/5 px-3 py-1 text-xs">
      {nome} / {uf} · IBGE {ibge6}
    </span>
  );
}
