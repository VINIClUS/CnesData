# Testes de Performance — guia operacional

## Tiers

| Tier | Alvo | Runtime típico | CI |
|---|---|---|---|
| micro | função isolada | <10ms/iter | todo PR |
| macro | pipeline end-to-end | 10-60s/cenário | nightly |
| stress | rampa até break-point | ~15 min | nightly |
| soak | 30 min carga constante | 30-60 min | nightly |
| spike | baseline → burst → recovery | ~20 min | nightly |

## Rodar local

```bash
docker compose -f docker-compose.perf.yml up -d
pytest tests/perf/micro -m perf_micro --benchmark-only
pytest tests/perf/macro -m perf_macro --benchmark-only
pytest tests/perf/stress -m stress -v
pytest tests/perf/soak -m soak -v
pytest tests/perf/spike -m spike -v
docker compose -f docker-compose.perf.yml down -v
```

## Atualizar baseline (após regressão intencional)

```bash
pytest tests/perf/micro tests/perf/macro --benchmark-only --benchmark-json=new_baseline.json
cp new_baseline.json .benchmarks/baselines.json
git add .benchmarks/baselines.json
git commit -m "perf: novo baseline — <motivo>"
```

## Métricas coletadas

- **Micro/Macro:** mean, median, p99 (via pytest-benchmark)
- **Stress:** RPS de break-point (primeira rampa com p99 > 1s OU error_rate > 1%)
- **Soak:** slope linear de RSS em MB/min; FD count delta; contagem de zombie connections
- **Spike:** recovery_p99 / pre_spike_p99 (alvo ≤ 1.1)

## Gate

- Regressão >20% em qualquer métrica micro/macro bloqueia PR (script `scripts/perf_compare.py`)
- Soak falha se RSS slope > 1 MB/min em 30 min
- Spike falha se recovery_p99 > 1.1 × pre_spike_p99

## Rebuild de fixture Firebird

```bash
docker compose -f docker-compose.perf.yml down -v firebird_perf
docker compose -f docker-compose.perf.yml up -d firebird_perf
python scripts/seed_firebird_fixture.py --n-profs 100000
```
