# dumpagent Python — Deprecation Timeline

## Status

- **Iniciado:** `<data Fase D>`
- **Modo:** Manutenção (bug-fixes críticos apenas)
- **Remoção do repo:** `<data Fase D + 3 meses>`
- **Tag final:** `dumpagent-py-v<N.M.P>-final`

## Bug-fixes críticos aceitos

| Categoria | Exemplo |
|---|---|
| Segurança | CVE em dependência ativa |
| Data integrity | Extrator retorna rows corrompidas silenciosamente |
| Regressão pós-cutover | Tenant ainda em Python impactado por mudança incompatível no central |

## Não aceitos

- Features novas
- Refactor
- Dep updates sem motivação de segurança
- Melhorias de performance
- Mudança de schema

## Processo para tenant ainda em Python

Se algum tenant não migrou:

1. Investigar motivo (técnico, político, treinamento)
2. Escalar para gerente ops antes do prazo de 3 meses
3. Se razão técnica (hw muito antigo p/ Go): documentar + extender prazo
   case-by-case
4. Se razão política (município não autoriza): escalar para diretoria

## Remoção

Antes de deletar:

- [ ] Zero tenants reportando uso via `/agents/status?agent_version=py*`
- [ ] Tag `dumpagent-py-v<N.M.P>-final` empurrada para origin
- [ ] Bucket releases ainda tem ZIP Python (para rollback de emergência
  caso-limite, retido por 12 meses após remoção)
- [ ] CLAUDE.md global atualizado (remoção do `dump_agent/` Python)

Comando:

```bash
git checkout -b chore/remove-dumpagent-python
git rm -r apps/dump_agent/
git commit -m "chore: remove apps/dump_agent (Python) após cutover completo
(veja tag dumpagent-py-v<N.M.P>-final para referência histórica)"
git push origin chore/remove-dumpagent-python
gh pr create --title "chore: remove legacy Python dumpagent"
```

Merge apenas com aprovação de 2 reviewers sêniores.
