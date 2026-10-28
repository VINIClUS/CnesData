# Aceite de runtime de produção

O workflow `.github/workflows/production-runtime.yml` publica o check
`production-runtime-acceptance` para todo pull request, para pushes em `develop` e
`main`, e para execução manual. O workflow usa somente runners hospedados e
`contents: read`; não acessa credenciais de produção, não aplica infraestrutura e
não coleta `tests/production/infra`.

## Topologia

Três produtores executam contratos finitos e um job final fail-closed agrega os
resultados:

- `runtime-python` executa os nove validadores de fonte, `tests/production` com
  `--ignore=tests/production/infra`, 978 contratos Python e 51 contratos CI.
- `runtime-dashboard` executa 133 contratos Vitest e grava
  `apps/web_dashboard/runtime-dashboard.xml`.
- `runtime-emulators` executa a suíte AWS com `dynamodb-local` e LocalStack e a
  suíte Floci em projetos Compose separados. A execução oficial selecionou 28
  casos AWS e 122 casos Floci, sem skips; cada relatório é validado depois da
  suíte e cada emulador tem limpeza independente com `if: always()`.
- `production-runtime-acceptance` exige que os três resultados literais sejam
  `success`. Falha, cancelamento, skip, valor vazio e resultado desconhecido
  falham no shell final.

Os quatro relatórios JUnit precisam ser parseáveis, conter ao menos um testcase e
ter contadores e elementos `skipped`, `failure` e `error` zerados. Relatório
ausente, vazio, incompleto ou malformado falha fechado.

## Validação local

Use a mesma expressão de markers e os mesmos comandos do workflow. O gate de fonte
é stdlib-only e pode ser chamado sem pytest:

```bash
uv run pytest -q tests/production/test_runtime_acceptance_validators.py \
  tests/production/test_runtime_acceptance_regressions.py tests/scripts/test_ci_aws_runtime.py
uv run pytest -q tests/production --ignore=tests/production/infra
uv run pytest \
  -m "not integration and not postgres and not bigquery and not e2e \
and not stress and not soak and not spike and not windows_only \
and not s3_integration and not dynamodb_local and not floci" \
  tests/production --ignore=tests/production/infra
```

Para os emuladores, mantenha nomes de projeto isolados e finalize cada perfil:

```bash
docker compose -p runtime377-aws-local --profile aws-test up -d --wait dynamodb-local localstack
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
AWS_ENDPOINT_URL=http://127.0.0.1:4566 DYNAMODB_ENDPOINT_URL=http://127.0.0.1:18000 \
  uv run --no-build pytest -rs -m "dynamodb_local and s3_integration" \
  tests/integration/aws --junitxml=runtime-emulators.xml
docker compose -p runtime377-aws-local --profile aws-test down -v --remove-orphans

docker compose -p runtime377-floci-local --profile floci up -d --wait floci
FLOCI_ENDPOINT_URL=http://127.0.0.1:4577 \
  uv run --no-build pytest -rs -m floci tests/integration/floci --junitxml=runtime-floci.xml
docker compose -p runtime377-floci-local --profile floci down -v --remove-orphans
```

Não use `docker compose down` sem projeto explícito em uma máquina com outras
stacks. Não execute os testes de infraestrutura como substituto do runtime gate.

## Triagem

Uma falha de fonte deve indicar `rule`, caminho POSIX relativo e linha. Corrija o
comportamento real ou registre uma alteração de allowlist revisada; não silencie a
violação com um padrão amplo. O caminho legado
`apps/central_api/src/central_api/raw_aws_runtime.py` continua fora do inventário
de chaves estáticas até #280. Relativo genérico `/api/v1`, `request.url.path`,
`ScanIndexForward`, `ScanAttributes`, comentários e métricas sem decisão são
permitidos.

Falha de JUnit indica uma suíte não executada, interrompida ou parcialmente
coletada. Verifique a etapa de teste e a etapa `validate-*` correspondente antes
de investigar o shell final. Falha de teardown permanece falha do produtor; nunca
remova `--remove-orphans` para obter verde.

## Evidência e limites

A execução oficial candidata foi feita na branch `codex/prod-012-runtime-acceptance`,
no SHA `7c382d44f07afdff7680eaae5ce09f9342d66ce4`, com base
`709394c09969001fa41df5d885f10c9e821b06c9`. O workflow dedicado terminou com
sucesso no run [40604640818](https://github.com/VINIClUS/CnesData/actions/runs/40604640818),
com os três produtores e o agregador final concluídos; os checks auxiliares
[CI 40604640649](https://github.com/VINIClUS/CnesData/actions/runs/40604640649),
[quality 40604640646](https://github.com/VINIClUS/CnesData/actions/runs/40604640646),
[Sonar 40604640618](https://github.com/VINIClUS/CnesData/actions/runs/40604640618) e
[Trivy 40604640635](https://github.com/VINIClUS/CnesData/actions/runs/40604640635)
também terminaram com sucesso.

Uma verificação hospedada separada, baseada no head docs-only
`c69292d955234c37337965a36e612ce5fb7bf715`, terminou com sucesso no run
[40605328299](https://github.com/VINIClUS/CnesData/actions/runs/40605328299) e
reproduziu as mesmas contagens selecionadas. Ela está registrada apenas como
prova independente: a [PR #427](https://github.com/VINIClUS/CnesData/pull/427)
permanece não integrada (`merged=false`). O ensaio de falha por alteração
proibida, no head `b06e26989486bfffd32b12b0ec59a9483ae58fe3`, terminou com falha no
run [40703634080](https://github.com/VINIClUS/CnesData/actions/runs/40703634080),
como esperado para essa proteção.

O run [40705270173](https://github.com/VINIClUS/CnesData/actions/runs/40705270173)
teve uma segunda tentativa concluída com sucesso no SHA restaurado
`13cd90836967f043d05141087e882e42198b4268`. Nessa tentativa, os produtores
`122879699949`, `122879700394` e `122879700625` e o agregador final
`122881118504` terminaram com sucesso; os validadores confirmaram 978 contratos
Python, 51 CI, 133 dashboard, 28 AWS e 122 Floci, sem skips selecionados. A
tentativa 1 deixou Floci sem iniciar; na tentativa 2 os containers e as redes
AWS e Floci foram explicitamente removidos.

O ruleset `production-runtime-acceptance-develop` (`26047506`) está ativo para
`develop` e exige o check `production-runtime-acceptance` da GitHub Actions
(`integration_id=15368`). O pacote de evidências registra que os rulesets
anteriores permaneceram inalterados e que `main` permaneceu sem essa exigência.

O candidato ainda não foi integrado. Esta documentação não afirma readiness AWS,
emissão de certificado, deploy ou fechamento da issue #377; #398, #381, #280 e
#60 continuam limitações do estado operacional.
