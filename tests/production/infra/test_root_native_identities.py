"""Preflight computado e observação nativa do root de produção."""

import json
import subprocess
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

from tests.production.infra._root_native_identities import (
    _override_hcl,
    assert_root_identity_producers,
    merge_identity_records,
    root_identity_seeds,
)
from tests.production.infra._root_native_observation import observe_root_result

type Node = dict[str, Any]

PROVIDER = "registry.opentofu.org/hashicorp/aws"
CONTRACT = {"account_id": "123456789012", "name_prefix": "cnesdata-prod", "region": "us-east-2"}
TABLE = "module.control_plane.aws_dynamodb_table.control_plane"
DATA = "module.data_buckets.aws_s3_bucket.data"
AUDIT = "module.data_buckets.aws_s3_bucket.audit"
EXECUTION = "module.runtime_iam.aws_iam_role.execution"
UNIT = 'module.runtime_iam.aws_iam_role.task["unit"]'
RECOVERY = 'module.runtime_iam.aws_iam_role.task["recovery"]'
AUDITOR = 'module.runtime_iam.aws_iam_role.task["audit"]'
SFN = "module.runtime_iam.aws_iam_role.step_functions[0]"
TABLE_ARN = "arn:aws:dynamodb:us-east-2:123456789012:table/cnesdata-prod-control-plane"
EXPECTED = {
    TABLE: {"arn": TABLE_ARN},
    DATA: {"arn": "arn:aws:s3:::cnesdata-prod-data", "id": "cnesdata-prod-data"},
    AUDIT: {"arn": "arn:aws:s3:::cnesdata-prod-audit", "id": "cnesdata-prod-audit"},
    EXECUTION: {"arn": "arn:aws:iam::123456789012:role/cnesdata-prod-processor-execution",
           "id": "cnesdata-prod-processor-execution",},
    UNIT: {"arn": "arn:aws:iam::123456789012:role/cnesdata-prod-processor-unit",
           "id": "cnesdata-prod-processor-unit",},
    RECOVERY: {"arn": "arn:aws:iam::123456789012:role/cnesdata-prod-processor-recovery",
           "id": "cnesdata-prod-processor-recovery",},
    AUDITOR: {"arn": "arn:aws:iam::123456789012:role/cnesdata-prod-processor-audit",
           "id": "cnesdata-prod-processor-audit",},
    SFN: {"arn": "arn:aws:iam::123456789012:role/cnesdata-prod-step-functions",
           "id": "cnesdata-prod-step-functions",},
}
FILE = "tests/root.tftest.hcl"
RUN = "raiz_positiva"
SUMMARY = {"status": "pass", "passed": 1, "failed": 0, "errored": 0, "skipped": 0}
FIXTURE_SOURCE = "tests/production/infra/conftest.py::VALID_SHARED_OUTPUTS"
PRODUCER_SOURCES = {
    TABLE: ["infra/opentofu/env/prod/main.tf", "infra/opentofu/modules/control-plane/main.tf"],
    DATA: ["infra/opentofu/env/prod/main.tf", "infra/opentofu/modules/data-buckets/main.tf"],
    AUDIT: ["infra/opentofu/env/prod/main.tf", "infra/opentofu/modules/data-buckets/main.tf"],
    EXECUTION: ["infra/opentofu/modules/runtime-iam/task-roles.tf"],
    UNIT: ["infra/opentofu/modules/runtime-iam/task-roles.tf"],
    RECOVERY: ["infra/opentofu/modules/runtime-iam/task-roles.tf"],
    AUDITOR: ["infra/opentofu/modules/runtime-iam/task-roles.tf"],
    SFN: ["infra/opentofu/env/prod/main.tf",
          "infra/opentofu/modules/runtime-iam/step-functions.tf"],
}


def _configuracao() -> Node:
    calls: Node = {}
    resources = [
        ("control_plane", "aws_dynamodb_table.control_plane", "name", "var.table_name"),
        ("data_buckets", "aws_s3_bucket.data", "bucket", "var.bucket_names.data"),
        ("data_buckets", "aws_s3_bucket.audit", "bucket", "var.bucket_names.audit"),
        ("runtime_iam", "aws_iam_role.execution", "name", "var.name_prefix"),
        ("runtime_iam", "aws_iam_role.task", "name", "var.name_prefix"),
        ("runtime_iam", "aws_iam_role.step_functions", "name", "var.name_prefix"),
    ]
    for module, address, field, reference in resources:
        call = calls.setdefault(module, {"source": "../../modules/" + module.replace("_", "-"),
                                         "module": {"resources": []}})
        kind, name = address.split(".")
        resource = {"address": address, "mode": "managed", "type": kind, "name": name,
                    "provider_config_key": "aws",
                    "expressions": {field: {"references": [reference]}}}
        if name == "step_functions":
            resource["count_expression"] = {"references": ["var.state_machine_arn"]}
        call["module"]["resources"].append(resource)
    return {"provider_config": {"aws": {"name": "aws", "full_name": PROVIDER,
            "expressions": {"region": {"constant_value": "us-east-2"}}}},
            "root_module": {"module_calls": calls, "variables": {"shared_outputs": {}}}}


def _schema() -> Node:
    attributes = {"arn": {"type": "string", "computed": True},
                  "id": {"type": "string", "computed": True, "optional": True}}
    resources = {kind: {"block": {"attributes": deepcopy(attributes)}} for kind in
                 ("aws_dynamodb_table", "aws_s3_bucket", "aws_iam_role")}
    return {"provider_schemas": {PROVIDER: {"resource_schemas": resources}}}


def _seeds() -> list[Node]:
    return root_identity_seeds(_configuracao(), _schema(), deepcopy(CONTRACT))


def _plano() -> Node:
    resources = []
    for address, fields in EXPECTED.items():
        kind = address.split(".")[2]
        values = dict(fields)
        if kind == "aws_iam_role":
            values |= {"name": fields["id"], "path": None}
        elif kind == "aws_s3_bucket":
            values["bucket"] = fields["id"]
        else:
            values |= {
                "name": "cnesdata-prod-control-plane",
                "global_secondary_index": [{"name": f"gsi{number}"} for number in range(1, 7)],
            }
        resources.append({"address": address, "mode": "managed", "type": kind,
                          "provider_name": PROVIDER, "change": {"after": values}})
    return {"resource_changes": resources,
            "variables": {"shared_outputs": {"value": {"account_id": "123456789012"}}},
            "configuration": _configuracao()}


def _recurso(plan: Node, address: str) -> Node:
    return next(resource for resource in plan["resource_changes"] if resource["address"] == address)


def _evento(kind: str, payload: Node, run: str = RUN, file: str = FILE) -> Node:
    event = {"@level": "error" if kind == "diagnostic" else "info", "@module": "tofu.ui",
             "@message": "native event", "type": kind, kind: payload}
    if kind != "test_summary":
        event |= {"@testfile": file, "@testrun": run}
    return event


def _eventos(plan: Node | None) -> list[Node]:
    events = [_evento("test_run", {"path": FILE, "run": RUN, "status": "pass"})]
    if plan is not None:
        events.append(_evento("test_plan", plan))
    return events + [_evento("test_summary", deepcopy(SUMMARY))]


def _resultado(events: list[Node], rc: int = 0) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(["/usr/bin/tofu", "test", "-json", "-verbose"], rc,
                                       "".join(json.dumps(event) + "\n" for event in events),
                                       "native stderr\n")


def _contexto(directory: Path, label: str = "observacao") -> Node:
    return {"directory": directory, "label": label, "suite": b'run "raiz_positiva" {}\n',
            "metadata": {"cwd": "/fixture/root", "tool": "OpenTofu 1.13.2"}}


def _observar(result: subprocess.CompletedProcess[str], seeds: list[Node], context: Node,
              positive: frozenset[str] = frozenset({RUN})) -> Any:
    return observe_root_result(result, seeds, {"test_file": FILE, "positive_roots": positive},
                               context)


def _valores(value: Any) -> list[Any]:
    values = [value]
    children = (value.values() if isinstance(value, dict)
                else value if isinstance(value, list) else [])
    for child in children:
        values.extend(_valores(child))
    return values


def test_semeia_exatamente_oito_produtores_quinze_campos_com_proveniencia_preplan() -> None:
    configuration, schema = _configuracao(), _schema()
    originals = deepcopy((configuration, schema))
    seeds = root_identity_seeds(configuration, schema, deepcopy(CONTRACT))
    actual = {
        node["address"]: {key: field["value"] for key, field in node["fields"].items()}
        for node in seeds
    }
    assert actual == EXPECTED
    assert len(seeds) == 8
    assert sum(len(node["fields"]) for node in seeds) == 15
    for node in seeds:
        assert node["provider"] == PROVIDER
        module, address = node["address"].split(".", maxsplit=2)[1:]
        resources = configuration["root_module"]["module_calls"][module]["module"]["resources"]
        configured = next(item for item in resources if item["address"] == address.split("[")[0])
        assert node["configured_expressions"] == configured["expressions"]
        for name, field in node["fields"].items():
            source = field["source"]
            assert source["schema_attribute"]["computed"] is True
            assert source["schema_attribute"]["type"] == "string"
            attributes = schema["provider_schemas"][PROVIDER]["resource_schemas"][node["type"]][
                "block"]["attributes"]
            assert source["schema_attribute"] == attributes[name]
            assert source["provider_config_key"] == "aws"
            assert source["fixture_contract"] == CONTRACT
            assert source["fixture_source"] == FIXTURE_SOURCE
            assert source["producer_address"] == node["address"]
            assert source["producer_sources"] == PRODUCER_SOURCES[node["address"]]
            assert "original_computed_value" not in json.dumps(source)
    assert (configuration, schema) == originals


@pytest.mark.parametrize("mutation", [
    "ausente", "arn_configurado", "id_configurado", "data", "tipo", "provider", "alias",
    "regiao", "escopo", "source",
])
def test_rejeita_configuracao_que_nao_autoriza_seed_computado(mutation: str) -> None:
    configuration = _configuracao()
    resource = configuration["root_module"]["module_calls"]["runtime_iam"]["module"]["resources"][0]
    if mutation == "ausente":
        configuration["root_module"]["module_calls"].pop("control_plane")
    elif mutation.endswith("_configurado"):
        field = mutation.split("_", maxsplit=1)[0]
        resource["expressions"][field] = {"constant_value": "configurado"}
    elif mutation in {"data", "tipo", "provider"}:
        key, value = {"data": ("mode", "data"), "tipo": ("type", "aws_s3_bucket"),
                      "provider": ("provider_config_key", "aws.other")}[mutation]
        resource[key] = value
    elif mutation == "source":
        configuration["provider_config"]["aws"]["full_name"] = "registry.example/aws"
    elif mutation in {"alias", "regiao"}:
        provider = configuration["provider_config"]["aws"]
        provider.update({"alias": "other"} if mutation == "alias" else
                        {"expressions": {"region": {"constant_value": "us-east-1"}}})
    else:
        configuration["root_module"] = {"resources": [resource]}
    with pytest.raises(pytest.fail.Exception, match="setup_error"):
        root_identity_seeds(configuration, _schema(), deepcopy(CONTRACT))


@pytest.mark.parametrize("mutation", ["ausente", "nao_computado", "tipo", "source"])
def test_rejeita_schema_sem_atributo_aws_computado_string(mutation: str) -> None:
    schema = _schema()
    role = schema["provider_schemas"][PROVIDER]["resource_schemas"]["aws_iam_role"]
    attributes = role["block"]["attributes"]
    if mutation == "ausente":
        attributes.pop("arn")
    elif mutation == "source":
        schema["provider_schemas"]["registry.example/aws"] = schema["provider_schemas"].pop(
            PROVIDER)
    else:
        attributes["arn"].update({"computed": False} if mutation == "nao_computado" else
                                 {"type": "number"})
    with pytest.raises(pytest.fail.Exception, match="setup_error"):
        root_identity_seeds(_configuracao(), schema, deepcopy(CONTRACT))


@pytest.mark.parametrize("malformed", [None, {"account_id": "invalido"}, {}])
def test_seed_nao_repara_input_root_malformado(malformed: Any) -> None:
    configuration = _configuracao()
    configuration["root_module"]["variables"]["shared_outputs"]["default"] = malformed
    before = deepcopy(configuration)
    seeds = root_identity_seeds(configuration, _schema(), deepcopy(CONTRACT))
    assert next(node for node in seeds if node["address"] == TABLE)["fields"]["arn"][
        "value"] == TABLE_ARN
    assert configuration == before


def test_merge_agrega_fontes_concordantes_e_preserva_hostname_static_independente() -> None:
    seeds = _seeds()
    observed = deepcopy(seeds[0])
    observed["fields"]["arn"]["source"] = {"observation": "discovery-native"}
    static = {"address": DATA, "type": "aws_s3_bucket", "provider": PROVIDER,
              "configured_expressions": {"bucket": {"references": ["var.bucket_names.data"]}},
              "fields": {
                  "arn": {"value": "arn:aws:s3:::cnesdata-prod-data", "source": {"boundary": "A"}},
                  "id": {"value": "cnesdata-prod-data", "source": {"boundary": "A"}},
                  "bucket_regional_domain_name": {"value": "boundary-a.s3.us-east-2.amazonaws.com",
                                                   "source": {"boundary": "A"}}}}
    boundary_b = deepcopy(static)
    for field in boundary_b["fields"].values():
        field["source"] = {"boundary": "B"}
    boundary_b["fields"]["bucket_regional_domain_name"]["value"] = "boundary-b.s3.amazonaws.com"
    merged_a = merge_identity_records(seeds, [observed, static])
    merged_b = merge_identity_records(_seeds(), [boundary_b])
    table = next(node for node in merged_a if node["address"] == seeds[0]["address"])
    provenance = json.dumps(table["fields"]["arn"])
    assert "fixture_contract" in provenance
    assert "discovery-native" in provenance
    for merged, host, other in [(merged_a, "boundary-a.s3.us-east-2.amazonaws.com", "boundary-b"),
                                 (merged_b, "boundary-b.s3.amazonaws.com", "boundary-a")]:
        data = next(node for node in merged if node["address"] == DATA)
        assert {key: field["value"] for key, field in data["fields"].items()} == EXPECTED[DATA] | {
            "bucket_regional_domain_name": host}
        assert data["fields"]["bucket_regional_domain_name"]["source"] == {
            "boundary": "A" if merged is merged_a else "B"}
        rendered = _override_hcl(merged)
        assert len(merged) == 8
        assert rendered.count("override_resource {") == 8
        assert rendered.count(f"target = {DATA}") == 1
        assert rendered.count(f"target = {TABLE}") == 1
        assert host in rendered
        assert other not in rendered


def test_merge_rejeita_valores_conflitantes_em_vez_de_sobrescrever() -> None:
    seeds = _seeds()
    conflict = deepcopy(seeds[0])
    conflict["fields"]["arn"]["value"] = "arn:aws:iam::123456789012:role/outro"
    with pytest.raises(pytest.fail.Exception, match="setup_error"):
        merge_identity_records(seeds, [conflict])


def test_aceita_produtores_reais_e_path_default_sem_mudar_plano() -> None:
    plan = _plano()
    before = deepcopy(plan)
    assert_root_identity_producers(plan, _seeds())
    assert plan == before


@pytest.mark.parametrize("mutation", [
    "tabela", "bucket", "role", "path", "account", "arn", "id", "modo", "extra", "ausente",
    "indice", "indice_ausente",
])
def test_rejeita_drift_real_apesar_de_arn_e_id_semeados(mutation: str) -> None:
    plan = _plano()
    changes = {"tabela": (TABLE, "name", "outro"), "bucket": (DATA, "bucket", "outro"),
               "role": (UNIT, "name", "outro"), "path": (UNIT, "path", "/outro/"),
               "arn": (TABLE, "arn", "mock-invalido"), "id": (UNIT, "id", "mock-invalido")}
    if mutation in changes:
        address, field, value = changes[mutation]
        _recurso(plan, address)["change"]["after"][field] = value
    elif mutation == "account":
        plan["variables"]["shared_outputs"]["value"]["account_id"] = "999999999999"
    elif mutation == "modo":
        _recurso(plan, UNIT)["mode"] = "data"
    elif mutation == "extra":
        extra = deepcopy(_recurso(plan, UNIT))
        extra["address"] = 'module.runtime_iam.aws_iam_role.task["extra"]'
        plan["resource_changes"].append(extra)
    elif mutation == "ausente":
        plan["resource_changes"].remove(_recurso(plan, SFN))
    else:
        indexes = _recurso(plan, TABLE)["change"]["after"]["global_secondary_index"]
        indexes.pop() if mutation == "indice_ausente" else indexes[0].update({"name": "outro"})
    with pytest.raises(pytest.fail.Exception, match="setup_error"):
        assert_root_identity_producers(plan, _seeds())


def test_observa_plano_positivo_original_com_resumo_real(tmp_path: Path) -> None:
    plan = _plano()
    events = _eventos(plan)
    result = _resultado(events)
    report = _observar(result, _seeds(), _contexto(tmp_path))
    assert report.result is result
    assert report.statuses == {RUN: "pass"}
    assert report.observations["plans"] == {(FILE, RUN): plan}
    assert report.observations["summary"] == events[-1]
    assert report.observations["diagnostics"] == []


def _eventos_invalidos(mutation: str) -> list[Node]:
    events = _eventos(_plano())
    changes = {"vazio": ("test_plan", {}), "outro_run": ("@testrun", "outro"),
               "outro_file": ("@testfile", "outro")}
    if mutation in changes:
        field, value = changes[mutation]
        events[1][field] = value
    elif mutation in {"ausente", "sem_resumo", "sem_status"}:
        events.pop({"ausente": 1, "sem_resumo": -1, "sem_status": 0}[mutation])
    elif mutation in {"duplicado", "status_duplicado"}:
        events.append(deepcopy(events[1 if mutation == "duplicado" else 0]))
    elif mutation == "resumo_falho":
        events[-1]["test_summary"].update({"status": "error", "errored": 1})
    elif mutation == "diagnostico":
        events.append(_evento("diagnostic", {"severity": "error", "summary": "unexpected",
                                             "detail": "native failure"}))
    elif mutation == "produtor":
        _recurso(events[1]["test_plan"], UNIT)["change"]["after"]["name"] = "outro"
    elif mutation.startswith("status_"):
        events[0]["test_run"]["status"] = mutation.removeprefix("status_")
    elif mutation in {"failed", "errored", "skipped"}:
        events[-1]["test_summary"][mutation] = 1
    return events


@pytest.mark.parametrize("mutation", [
    "ausente", "vazio", "duplicado", "outro_run", "outro_file", "sem_resumo", "resumo_falho",
    "status_duplicado", "json", "diagnostico", "rc", "produtor", "sem_status",
    "status_fail", "status_error", "status_skip", "failed", "errored", "skipped",
])
def test_observacao_positiva_rejeita_evidencia_incompleta_ou_drift(
    tmp_path: Path, mutation: str,
) -> None:
    events = _eventos_invalidos(mutation)
    result = _resultado(events, 19 if mutation == "rc" else 0)
    if mutation == "json":
        result.stdout += "{invalid-json\n"
    original = (result.stdout, result.stderr, deepcopy(result.args), result.returncode)
    seeds, context = _seeds(), _contexto(tmp_path)
    with pytest.raises(pytest.fail.Exception, match="setup_error"):
        _observar(result, seeds, context)
    assert (result.stdout, result.stderr, result.args, result.returncode) == original
    contents = [path.read_bytes() for path in tmp_path.rglob("*") if path.is_file()]
    assert result.stdout.encode() in contents
    assert result.stderr.encode() in contents
    assert context["suite"] in contents
    values = []
    for content in contents:
        try:
            values.extend(_valores(json.loads(content)))
        except (ValueError, UnicodeDecodeError):
            continue
    assert result.args in values
    assert result.returncode in values
    assert seeds in values


def test_falha_esperada_sem_plano_so_e_permitida_fora_do_escopo_positivo(tmp_path: Path) -> None:
    result = _resultado(_eventos(None))
    report = _observar(result, _seeds(), _contexto(tmp_path, "negativo"), frozenset())
    assert report.result is result
    assert report.statuses == {RUN: "pass"}
    assert report.observations["plans"] == {}
    with pytest.raises(pytest.fail.Exception, match="setup_error"):
        _observar(result, _seeds(), _contexto(tmp_path, "positivo"))


@pytest.mark.parametrize("structural", [False, True])
def test_negativos_preservam_diagnosticos_nativos_e_bytes(tmp_path: Path, structural: bool) -> None:
    message = ('attribute "account_baseline" is required' if structural else
               "required variable may not be set to null")
    diagnostic = _evento("diagnostic", {"severity": "error",
                                       "summary": ("Invalid value for input variable" if structural
                                                   else "Required variable not set"),
                                       "detail": message})
    events = [_evento("test_run", {"path": FILE, "run": RUN, "status": "error"}), diagnostic,
              _evento("test_summary", {"status": "error", "passed": 0, "failed": 0,
                                       "errored": 1, "skipped": 0})]
    result = _resultado(events, 1)
    original = (result.stdout, result.stderr)
    report = _observar(result, _seeds(), _contexto(tmp_path), frozenset())
    assert report.result is result
    assert result.returncode == 1
    assert (result.stdout, result.stderr) == original
    assert report.observations["diagnostics"] == [diagnostic]
    assert message in report.observations["diagnostics"][0]["diagnostic"]["detail"]
    assert report.observations["summary"] == events[-1]


def test_rejeita_associacao_ambigua_de_run_entre_arquivos(tmp_path: Path) -> None:
    events = _eventos(_plano())
    events.append(_evento("test_run", {"path": "tests/outro.tftest.hcl", "run": RUN,
                                      "status": "pass"}, file="tests/outro.tftest.hcl"))
    with pytest.raises(pytest.fail.Exception, match="setup_error"):
        _observar(_resultado(events), _seeds(), _contexto(tmp_path), frozenset())


def test_negativo_pre_run_sem_resumo_permanece_sem_resumo(tmp_path: Path) -> None:
    diagnostic = _evento("diagnostic", {"severity": "error", "summary": 'Invalid "command" keyword',
                                       "detail": "apply or plan required"})
    diagnostic.pop("@testfile")
    diagnostic.pop("@testrun")
    result = _resultado([diagnostic], 1)
    report = _observar(result, _seeds(), _contexto(tmp_path), frozenset())
    assert report.result is result
    assert report.statuses == {}
    assert report.observations["summary"] is None
    assert report.observations["diagnostics"] == [diagnostic]


def test_rotulos_distintos_preservam_evidencia_de_cada_chamada(tmp_path: Path) -> None:
    first = _resultado(_eventos(None))
    seeds = _seeds()
    _observar(first, seeds, _contexto(tmp_path, "primeiro"), frozenset())
    second = _resultado([])
    second.stdout = "{invalid-json\n"
    with pytest.raises(pytest.fail.Exception, match="setup_error"):
        _observar(second, seeds, _contexto(tmp_path, "segundo"), frozenset())
    saved = [path.read_bytes() for path in tmp_path.rglob("*") if path.is_file()]
    assert first.stdout.encode() in saved
    assert second.stdout.encode() in saved


@pytest.mark.parametrize("mutation", [
    "nenhuma", "segundo_produtor", "segundo_plano", "modulo_extra"])
def test_observa_cada_root_positivo_e_aceita_contagem_de_suite_mista(
    tmp_path: Path, mutation: str,
) -> None:
    second = "raiz_secundaria"
    events = _eventos(_plano())[:-1] + [
        _evento("test_run", {"path": FILE, "run": second, "status": "pass"}, run=second),
        _evento("test_plan", _plano(), run=second),
        _evento("test_summary", SUMMARY | {"passed": 2})]
    if mutation == "segundo_produtor":
        _recurso(events[3]["test_plan"], UNIT)["change"]["after"]["name"] = "outro"
    elif mutation == "segundo_plano":
        events.pop(3)
    elif mutation == "modulo_extra":
        events.insert(4, _evento("test_run", {"path": FILE, "run": "modulo", "status": "pass"},
                                 run="modulo"))
        events[-1]["test_summary"]["passed"] = 3
    result, seeds, context = _resultado(events), _seeds(), _contexto(tmp_path)
    if mutation in {"segundo_produtor", "segundo_plano"}:
        with pytest.raises(pytest.fail.Exception, match="setup_error"):
            _observar(result, seeds, context, frozenset({RUN, second}))
    else:
        report = _observar(result, seeds, context, frozenset({RUN, second}))
        assert report.observations["plans"] == {(FILE, RUN): _plano(), (FILE, second): _plano()}
        assert report.observations["summary"] == events[-1]
        assert report.statuses == {event["test_run"]["run"]: "pass" for event in events
                                   if event["type"] == "test_run"}
