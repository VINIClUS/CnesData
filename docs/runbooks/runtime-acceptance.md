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
  `--ignore=tests/production/infra` e os 28 arquivos Python aprovados.
- `runtime-dashboard` executa os oito arquivos Vitest aprovados e grava
  `apps/web_dashboard/runtime-dashboard.xml`.
- `runtime-emulators` executa a suíte AWS com `dynamodb-local` e LocalStack e a
  suíte Floci em projetos Compose separados. Cada relatório é validado depois da
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

A implementação parte de #375 integrado no SHA `709394c09969001fa41df5d885f10c9e821b06c9`,
com CI integrado `40592568935` e Deploy develop `40592568914`. O pacote de testes
congelado foi `d6d09143a9d85c91f0183c4dae564400e788fc1ccbc13dd25d9b11e7cf828eca`.

O check ainda precisa de execução oficial no SHA integrado e de decisão posterior
do dono sobre ruleset. Esta documentação não afirma readiness AWS, emissão de
certificado, deploy ou fechamento da issue #377.

Ensaio temporário da issue #377: esta alteração exclusivamente documental verifica
o disparo do check em pull request. A PR de ensaio será encerrada sem merge.
