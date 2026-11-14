# Capability check: CloudFront Free ACTIVE

Status: executado em 2026-11-14. FREE `ACTIVE`, replay `pass` e plan sem diff estão na #476:
<https://github.com/VINIClUS/CnesData/issues/476#issuecomment-6882664275>.
A remoção do bloqueio segue a emenda de 2026-11-14 da spec de custo. O plano Free do
CloudFront não tem recurso no provider
AWS: a assinatura fica fora do state do OpenTofu (`docs/superpowers/specs/
2026-08-29-cnesdata-production-deployment-design.md`, seção 6.1). O gate só vê os sinais de
plano pago no plan (certificado `vip`, Origin Shield, monitoring subscription e real-time
logs, regras do `policies/cnesdata.rego`) e o bloqueio de ativação
`cloudfront_free_plan_active`, que fica pendente até esta evidência.

## Aprovação

Aprovação explícita do dono na #476, citando a distribuição e a WAF dedicada (escopo
CloudFront, `us-east-1`, não compartilhada com LimnoPulse). A assinatura deve estar
FREE `ACTIVE` antes do cutover de DNS; falha em qualquer passo mantém a ativação bloqueada.

## Retenção mínima

- Nenhum recurso novo: a prova liga a distribuição e a WAF existentes ao plano Free e verifica
  `ACTIVE`.
- Sem upgrade automático nem pay-as-you-go: se o plano não ficar `ACTIVE`, parar sem
  cancelar, substituir ou desfazer associação.
- A entrada do binding é a saída `free_subscription_binding_input` do módulo static-web
  (distribuição e WAF exatas).

## Procedimento

1. Conferir a elegibilidade e a cota Free da conta antes do cutover.
2. Chamada manual e idempotente da API do Pricing Plan Manager em `us-east-1`, ligando a
   distribuição e a WAF exatas ao plano FREE.
3. Ler o estado da assinatura e exigir `ACTIVE`; repetir a chamada e exigir o mesmo resultado
   (idempotência).
4. Conferir que a distribuição não tem Origin Shield, certificado `vip`, monitoring
   subscription nem real-time logs, e que `tofu plan` continua sem diff.

## Evidência redigida

Registrar só: plano (`FREE`), estado (`ACTIVE`), resultado da repetição e a confirmação de que o
plan não tem diff. Trocar conta, ID da distribuição e ARN da WAF por `<conta>`, `<dist>`,
`<waf>`. Anexar a evidência assinada de deploy; nunca credenciais.

## Limpeza

- Nada a apagar. Se o plano não ficou `ACTIVE`, manter a ativação bloqueada e abrir issue.
  Nunca cancelar/substituir a assinatura ou desfazer a associação automaticamente.
- Só depois da evidência, um humano remove `cloudfront_free_plan_active` de
  `activation_blocking_preconditions` em PR próprio.


## Ferramenta #476 e elegibilidade real

Código não significa Ready nem Produção. `scripts/promotion/reconcile_cloudfront_free.py`
consome exatamente `free_subscription_binding_input:{distribution_arn,web_acl_arn}` do
receipt bootstrap #468. Esse binding é **input**, não prova de assinatura ativa.

O cliente Pricing Plan Manager roda em `us-east-1`; modelo botocore `2025-08-05`.
A API usa `list_subscriptions` (`subscriptionSummaries`/`nextToken`),
`create_subscription` (`subscription`/`eTag`) e `get_subscription(arn=...)`.
Não há campos `eligible`/`free_slots`. A quota documentada é três FREE e cem planos
por conta, sem aumento; o inventário percorre páginas, recusa duplicidade/ciclos e
associações conflitantes. O readback de CloudFront confere ARN/distribuição Deployed,
WAF global dedicada, nenhuma segunda distribuição com a WAF, `vip`, Origin Shield,
real-time logs ou monitoring subscription pago.

`freetier:GetAccountPlanState` comprova a conta `PAID`/`ACTIVE`, requisito de
**elegibilidade**: contas no plano AWS Free Tier não podem assinar flat-rate plans.
Isso não é assinatura paga do CloudFront. A assinatura solicitada é exclusivamente
`planFamily=CloudFront`, `planTier=FREE`, `approvalMode=IMMEDIATE`. Histórico de uso e
restrições adicionais são validados pelo serviço no CreateSubscription; sucesso local
não antecipa essa decisão. Recusar quota/elegibilidade, região errada, AccessDenied,
conflito, tier pago, mudança agendada ou estado distinto de ACTIVE. Nunca aprovar plano
pago, fazer upgrade, cancelar, substituir ou associar recursos adicionais como fallback.

O `clientToken` deriva do binding; retries SDK são desabilitados. Antes do Create,
o journal é criado exclusivamente e sincronizado em disco. Preservar o mesmo caminho
privado e durável por binding entre invocações. Timeout/resposta incerta mantém o journal:
o próximo run só pode reconhecer assinatura ACTIVE existente por List/Get; se ainda
não aparecer, falha sem novo Create. Não apagar journal para "tentar novamente".
Uma decisão humana posterior exige readback conclusivo e novo plano de ação revisado.

```bash
uv run --frozen python -m scripts.promotion.reconcile_cloudfront_free \
  --binding /private/free-subscription-binding.json \
  --attempt-journal /private/free-create-attempt.json \
  --release-hash "$RELEASE_SHA256"
```

Executar de novo com os mesmos inputs: deve somente ler e confirmar a mesma assinatura.
A saída contém só `assertion=cloudfront_free_active:pass|fail`, timestamp UTC e hash da
release. O código não roda tofu, altera HSTS, remove bloqueio ou troca DNS. Anexar também
a confirmação independente de `tofu plan` sem diff; não alegar que a saída do script
comprova ausência de diff ou elegibilidade futura. Não publicar binding/ARN/ID/journal.

## Forma elegível do static-web (#476)

Em 2026-11-14 o `CreateSubscription` FREE foi recusado com `ValidationException`. O modelo
`pricing-plan-manager` 2025-08-05 (botocore 1.43.82) descreve esse erro como "The request
failed a business rule validation". A mensagem no CloudTrail (#476, comentário 6876244915)
nomeia só a distribuição, não a feature.

Base documental: a tabela "Pricing plan features" e a seção "Unsupported features" de
<https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/flat-rate-pricing-plan.html>.
- No Free, cache e headers de resposta ficam só nas regras padrão (policies gerenciadas);
  policies próprias de cache, headers e origem são Business/Premium.
- CloudFront Functions entram em todos os tiers. KeyValueStore e logs de acesso são Pro ou
  acima.
- Real-time logs, field-level encryption, deploy contínuo e ForwardedValues não são
  suportados.
- A web ACL é obrigatória e cabe em 5 regras ("custom rules and AWS Managed Rules").

Forma atual:
- Cache policy gerenciada CachingOptimized (`658327ea-f89d-4fab-a63d-7e88639e58f6`: TTL
  mínimo 1 s, padrão 24 h, máximo 1 ano).
- A Function viewer-response `cnesdata-prod-static-web-headers` emite os valores da antiga
  response headers policy: `nosniff`, `DENY`, `strict-origin-when-cross-origin`,
  `Permissions-Policy` e a CSP report-only. HSTS só sai com `certificate_domain_validated`, e
  a CSP enforce só com `csp_mode = "enforce"`.
- O `Cache-Control` por objeto já vem da publicação (`static_release`), conferido no readback
  do `PutObject`: `no-cache` nos entrypoints e `public,max-age=31536000,immutable` nos
  assets. Com o TTL mínimo de 1 s, os entrypoints ficam até 1 s em cache, e a publicação os
  invalida.

Diferença aceita: o CloudFront não executa Functions viewer-response quando a origem
responde 400 ou mais ("HTTP status codes" em
<https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/edge-function-restrictions-all.html>).
Respostas 403/404 do S3 saem sem os headers de segurança.

Risco residual: a web ACL mantém os grupos gerenciados AWS `AWSManagedRulesCommonRuleSet` e
`AWSManagedRulesAmazonIpReputationList`. Pela decisão do coordenador da EPIC #465, a linha
"Rule Groups" da tabela é lida como rule group próprio. Partner Managed Rules, Bot Control,
ATP e ACFP seguem fora.

Guardas:
- `policies/cnesdata.rego` recusa como `free_plan_ineligible`:
  - policies próprias, KeyValueStore, deploy contínuo e field-level encryption;
  - rule group próprio de escopo CloudFront;
  - distribuição com mais de 5 behaviors, logs, ForwardedValues ou behavior fora da
    CachingOptimized;
  - web ACL CloudFront com mais de 5 regras, `rule_json`, rule group próprio ou de parceiro,
    Bot Control, ATP ou ACFP.
- `test_static_web.py` fixa a forma do HCL.
- `test_static_web_composition.py` executa a Function renderizada no plano mockado e confere
  a borda exclusiva: uma distribuição, sem associação de web ACL.

Uma nova tentativa usa journal novo, e o anterior fica preservado; a decisão é do coordenador
da EPIC #465, por delegação do dono. Antes dela, conferir:
- conta fora do Free Tier;
- sem Firewall Manager;
- sem Shield Advanced;
- no máximo 3 planos Free.

## Ordem operacional P1/P2/P3

1. #468 fase 1 gera receipt ACM. EXT-4 confere os registros exatos
   `{domain,name,type:CNAME,value}` em state/DescribeCertificate e os publica no DNS.
   #476 P1 lê DescribeCertificate do **mesmo ARN** e exige `ISSUED`; presença do
   ResourceRecord ou DNS propagado não basta. Nenhum outro certificado é substituto.
2. #468 fase 2 bootstrap cria distribuição/WAF e entrega binding conferido em state/APIs.
   PRE vale só para ACM/bootstrap inicial: checkpoint monotônico autenticado, target
   bloqueado para ativação/DNS/workloads, custos **brutos** de CF/WAF unbound incluídos
   no limite USD8 produto / agregado inferior a USD15 / Actions pagos USD0. Sem créditos.
3. #476 P2 reconcilia imediatamente após bootstrap para reduzir a janela WAF unbound;
   exige FREE ACTIVE real, replay e plan sem diff **antes** de DNS/cutover e Phase A.
   A evidência atual não autoriza fallback pay-as-you-go. EXT-4 executa o DNS aprovado.
4. PR humano próprio decide remoção de `cloudfront_free_plan_active`, incluindo amendment
   nominal dos paths de custo/OPA/fixtures necessários; outra mudança própria ativa
   `certificate_domain_validated=true` depois da validação real. HSTS tem efeito
   persistente no navegador. Nenhuma dessas mudanças está no escopo Código #476.
5. Seguir #469 Produção (após #472 Código), #473 P1 → #474 → #473 P2 → #475 → #476 P3.
   O smoke e seed de [demo-seed.md](demo-seed.md) mais D5 fecham a evidência posterior;
   não removem gates de Phase B/C, rollback, LimnoPulse antes/depois ou EXT-1/5/6.

## D5 — execução real sob #476 P3

Seguir [infrastructure-plan.md](infrastructure-plan.md), com sessão temporária,
principal/recurso/contexto exatos do handoff e aprovação do dono nesta issue. Ausência
ou recusa por credencial errada nunca é aprovação do check. Inputs/respostas completas
ficam só no ambiente privado; evidência pública mantém nome/timestamp/hash da release.

- **Chave ausente:** assumir separadamente as três roles reais `unit`, `recovery`, `API`;
  conferir GetCallerIdentity da sessão e pedir GetObject de chave inexistente sob o
  prefixo permitido `reconciliation/` ou `serving/`, sem criar objeto. Exigir 404 para
  cada uma; 403 é falha e não prova ausência. Não reutilizar sessão de uma role nas três.
  Evidências fechadas: `missing_key_unit:pass|fail`, `missing_key_recovery:pass|fail`,
  `missing_key_api:pass|fail`.
- **TriggeredRules:** conferir ListTargetsByRule vazio para a regra exata, registrar janela
  UTC e emitir um evento sintético que case seu padrão real. Não assumir que um evento
  customizado com source AWS reservado será aceito: para regra de serviço, disparar a
  operação governada que produz o evento de serviço. Conferir FailedEntryCount quando
  PutEvents for aplicável. Consultar CloudWatch `AWS/Events`, métrica `TriggeredRules`,
  dimensão RuleName exata, estatística Sum, período 60s, janela posterior ao evento.
  Exigir datapoint positivo; vazio/zero não passa. Não adicionar target como correção
  automática. Evidência `triggered_rules_without_target:pass|fail`.
- **IAM Step Functions:** usar execução real SUCCEEDED da máquina governada e histórico
  que identifique somente a revisão ativa exata; conferir RunTask e PassRole pelo
  principal/contexto real do handoff. Exigir negações reais para revisão não aprovada e
  role não aprovada, sem criar trust/máquina/rota ou usar simulação como prova runtime.
  Se o principal de serviço não puder ser reproduzido com segurança pelo procedimento
  autorizado, o check fica pendente, nunca passa por policy readback sozinho. Não editar
  state machine ou assumir a role de serviço com operador para fabricar negação.
  Registrar `sfn_service_active_task:pass|fail`, `sfn_wrong_task_denied:pass|fail`,
  `sfn_wrong_passrole_denied:pass|fail`. Encerrar execução pendente conforme runbook;
  AWS não oferece apagar histórico de uma execução encerrada.

O runner automático cobre readbacks/trust/task e smoke HTTP/COMPLIANCE/RA; os três D5
acima exigem procedimentos operacionais próprios e evidência adicional. O exit 0 do
runner não declara que D5 foi executado. #476 continua aberta até o aceite de Produção.

Fontes atuais: [planos CloudFront][plans], [Pricing Plan Manager][ppm] e [API Boto3][sdk].

[plans]: https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/flat-rate-pricing-plan.html
[ppm]: https://docs.aws.amazon.com/PricingPlanManager/latest/UserGuide/plans.html
[sdk]: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/pricing-plan-manager.html
