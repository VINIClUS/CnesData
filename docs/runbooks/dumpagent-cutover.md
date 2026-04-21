# dumpagent Cutover — Python → Go

## Visão Geral

Processo em 4 fases. Cada fase deve ser concluída e validada antes da próxima.

```
Fase A (1 sem)   Fase B (1-2 sem)   Fase C (3-4 sem)   Fase D (3 meses)
───────────────  ────────────────   ─────────────────  ──────────────
Shadow mode      Go ativo PE        Rollout tenants    Python deprecation
(PE apenas)      Python parado      manuais            manutenção → remove
```

## Checklist pré-cutover (aplicar a cada tenant)

- [ ] NTP sync verificado (`w32tm /query /status`, skew < 1min). Ver `edge-ntp-setup.md`.
- [ ] Versão Go baixada do bucket + SHA256 validado
- [ ] AV whitelist aplicado se ambiente tem AV restritivo
- [ ] `CnesDumpAgent_Backup\` populado com última versão Python (para rollback)
- [ ] Runbook `dumpagent-install-windows.md` revisado pelo operador local
- [ ] Runbook `dumpagent-rollback.md` disponível e impresso/salvo offline
- [ ] Janela de observação de 1h combinada com central

---

## Fase A — Shadow mode em PE (1 semana)

### A.1 — Deploy shadow

Na máquina do piloto PE (já com Python rodando normalmente):

```powershell
# baixar dumpagent-shadow do bucket (mesma versão stable)
Expand-Archive .\dumpagent-v0.1.0-shadow.zip "C:\Program Files\CnesAgentShadow"
```

### A.2 — Configurar shadow

`C:\Program Files\CnesAgentShadow\config.env`:

```env
# copiar da config Python + adicionar:
DUMP_SHADOW_MODE=true
DUMP_SHADOW_DIR=C:\ProgramData\CnesAgent\shadow
```

### A.3 — Executar shadow em foreground

```powershell
cd "C:\Program Files\CnesAgentShadow"
.\dumpagent.exe run --verbose
```

Deixar rodando em parallel ao Python (lock não conflita porque são serviços
distintos — considerar renomear para `CnesDumpAgentShadow` se conflitar).

**Importante:** shadow NÃO chama CompleteJob — portanto o Python completa
o mesmo job, fila não bloqueia.

### A.4 — Comparar

Diariamente:

```powershell
# para cada parquet shadow gerado:
Get-ChildItem "C:\ProgramData\CnesAgent\shadow\*.parquet.gz" | ForEach-Object {
    $job_id = $_.BaseName -replace '\.parquet$', ''
    $python_baseline = "C:\CnesAgent\production_output\$job_id.parquet"
    python scripts\shadow_diff.py --python $python_baseline --go $_.FullName
}
```

Registrar resultados em planilha/doc:
- jobs comparados
- diffs identificados (row count, byte count, field-level)
- encoding issues (§10.1.1 spec)

### A.5 — Critério de saída Fase A

- [ ] 100 jobs consecutivos com `shadow_diff` retornando `identical=True`
- [ ] Fixtures sujas de encoding testadas (ao menos 3 rows com char WIN1252
      corrompido preservadas via `SanitizeString`)
- [ ] Zero crashes do shadow por 7 dias
- [ ] Aprovação da equipe central para avançar

## Fase B — Go ativo em PE (1-2 semanas)

### B.1 — Parar Python

```powershell
Stop-Service CnesDumpAgent
```

### B.2 — Desinstalar Python

Opcional — manter instalação como fallback local para rollback rápido:

```powershell
# backup antes de desinstalar
Copy-Item "C:\Program Files\CnesAgentPy" "C:\CnesAgent_Backup\" -Recurse
pip uninstall dump-agent -y
# ou remove pasta se PyInstaller
```

### B.3 — Instalar Go

Seguir `dumpagent-install-windows.md`.

### B.4 — Observação 14 dias

Operador central monitora diariamente via `/agents/status`:

```bash
curl -H "X-Tenant-Id: 354130" "https://api.cnesdata.gov.br/api/v1/agents/status?tenant_id=354130"
```

Métricas-alvo:
- `jobs_completed_7d` ≥ histórico Python (±10%)
- `jobs_failed_7d` ≤ 2 × histórico Python
- `last_seen` < 15min atrás
- `agent_version` inicia com `v0.1`

### B.5 — Critério de saída Fase B

- [ ] 14 dias sem rollback
- [ ] Fail rate estável
- [ ] Zero encoding issues reportados
- [ ] Zero clock skew incidents

---

## Fase C — Rollout multi-tenant (3-4 semanas)

Replicar Fase B para os demais tenants, 1-2 por semana:

1. Operador do tenant recebe link presigned + runbook install
2. Executar shadow local 48h (mini-Fase A)
3. Cutover com janela pré-agendada
4. 7 dias de observação antes de marcar "estável"

Taxa-alvo: 80% dos tenants migrados em 4 semanas.

### C.1 — Critério de saída Fase C

- [ ] ≥80% tenants migrados
- [ ] PE completou 30+ dias em Go
- [ ] Fail rate global Go ≤ fail rate global Python (baseline)

---

## Fase D — Deprecation Python (3 meses)

### D.1 — Banner DEPRECATED

Editar `apps/dump_agent/CLAUDE.md` adicionando no topo:

```markdown
> **⚠️ DEPRECATED — Migração Go em andamento**
>
> `dump_agent` Python está em modo manutenção desde `<data>`. Nova feature
> development acontece em `apps/dump_agent_go/`. Este app será removido em
> `<data + 3 meses>`. Bug-fixes críticos ainda aceitos.
```

### D.2 — Travar PRs não-críticos

Adicionar label `dumpagent-py-legacy` + GitHub CODEOWNERS para exigir
aprovação sênior em mudanças.

### D.3 — Tag final Python

Após 3 meses:

```bash
git tag dumpagent-py-v<N.M.P>-final
git push origin dumpagent-py-v<N.M.P>-final
```

### D.4 — Remoção

PR final:

```bash
git rm -r apps/dump_agent/
git commit -m "chore: remove apps/dump_agent (Python) após cutover completo"
```

Preservar `dumpagent-py-v<N.M.P>-final` para referência arqueológica.

---

## Contatos

- Operação central: `<nome>` — <email>
- Escalation: `<nome sênior>` — <phone>
- Grupo Telegram: `<link>`
