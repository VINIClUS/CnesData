"""Preservação dos defaults computados sem ampliar os seeds da raiz."""

from copy import deepcopy

import pytest

from tests.production.infra._root_native_identities import (
    _override_hcl,
    merge_identity_records,
    preserve_root_mock_defaults,
    root_identity_seeds,
)
from tests.production.infra.test_root_native_identities import (
    AUDIT,
    CONTRACT,
    DATA,
    EXPECTED,
    PROVIDER,
    Node,
    _configuracao,
    _schema,
)

HOST = "bucket_regional_domain_name"
DEFAULT = {
    "value": "fixture-data.s3.us-east-2.amazonaws.com",
    "source": "tests/production/infra/_aws_mock_defaults.py::BUCKET_REGIONAL_DOMAIN_NAME",
}
CUSTOM = {
    "value": "fixture-other.s3.us-east-2.amazonaws.com",
    "source": "tests/production/infra/test_static_web_composition.py::STATIC_BUCKET_DEFAULTS",
}


def _contexto() -> tuple[list[Node], Node, Node]:
    configuration, schema = _configuracao(), _schema()
    attributes = schema["provider_schemas"][PROVIDER]["resource_schemas"]["aws_s3_bucket"]
    attributes["block"]["attributes"][HOST] = {
        "type": "string", "computed": True, "optional": True,
    }
    return root_identity_seeds(configuration, schema, CONTRACT), configuration, schema


def _campos(records: list[Node]) -> dict[str, Node]:
    return {record["address"]: record["fields"] for record in records}


def _seeds_permanecem(records: list[Node]) -> None:
    observed = {
        address: {name: field["value"] for name, field in fields.items() if name != HOST}
        for address, fields in _campos(records).items()
    }
    assert observed == EXPECTED


def test_preserva_defaults_sem_ampliar_seeds() -> None:
    seeds, configuration, schema = _contexto()
    original = deepcopy(seeds)
    records = preserve_root_mock_defaults(seeds, configuration, schema, DEFAULT)
    assert len(records) == 8
    assert sum(len(record["fields"]) for record in records) == 17
    assert sum(len(seed["fields"]) for seed in seeds) == 15
    assert seeds == original
    _seeds_permanecem(records)
    attributes = schema["provider_schemas"][PROVIDER]["resource_schemas"]["aws_s3_bucket"]
    for address in (DATA, AUDIT):
        field = _campos(records)[address][HOST]
        assert field["value"] == "fixture-data.s3.us-east-2.amazonaws.com"
        assert field["source"]["mock_default_source"] == DEFAULT["source"]
        assert field["source"]["producer_address"] == address
        assert field["source"]["provider_config_key"] == "aws"
        assert field["source"]["schema_attribute"] == attributes["block"]["attributes"][HOST]
    for record, seed in zip(records, seeds, strict=True):
        assert {key: value for key, value in record.items() if key != "fields"} == {
            key: value for key, value in seed.items() if key != "fields"
        }
    rendered = _override_hcl(records)
    assert rendered.count("override_resource {") == 8
    assert rendered.count(f"target = {DATA}\n") == 1
    assert rendered.count(f"target = {AUDIT}\n") == 1


def test_default_explicito_alterado_e_usado_sem_literal_antigo() -> None:
    seeds, configuration, schema = _contexto()
    changed = {"value": "changed.s3.us-east-2.amazonaws.com", "source": "fixture::CHANGED_HOST"}
    records = preserve_root_mock_defaults(seeds, configuration, schema, changed)
    for address in (DATA, AUDIT):
        assert HOST in _campos(records)[address]
        assert _campos(records)[address][HOST]["value"] == "changed.s3.us-east-2.amazonaws.com"
        assert _campos(records)[address][HOST]["source"]["mock_default_source"] == changed["source"]
    assert "fixture-data.s3.us-east-2.amazonaws.com" not in _override_hcl(records)
    _seeds_permanecem(records)


@pytest.mark.parametrize("boundary", ["a", "b"])
def test_preserva_hosts_static_a_b_e_default_custom_do_audit(boundary: str) -> None:
    seeds, configuration, schema = _contexto()
    explicit = deepcopy(next(seed for seed in seeds if seed["address"] == DATA))
    field = {"value": f"fixture-{boundary}.s3.us-east-2.amazonaws.com",
             "source": {"boundary": boundary, "origin": "static-explicit"}}
    explicit["fields"] = {HOST: deepcopy(field)}
    merged = merge_identity_records(seeds, [explicit])
    records = preserve_root_mock_defaults(merged, configuration, schema, CUSTOM)
    assert sum(len(record["fields"]) for record in records) == 17
    assert _campos(records)[DATA][HOST] == field
    assert _campos(records)[AUDIT][HOST]["value"] == "fixture-other.s3.us-east-2.amazonaws.com"
    assert _campos(records)[AUDIT][HOST]["source"]["mock_default_source"] == CUSTOM["source"]
    _seeds_permanecem(records)
    assert sum(len(seed["fields"]) for seed in seeds) == 15
    rendered = _override_hcl(records)
    other = "b" if boundary == "a" else "a"
    assert rendered.count("override_resource {") == 8
    assert rendered.count(f"target = {DATA}\n") == 1
    assert field["value"] in rendered
    assert f"fixture-{other}.s3.us-east-2.amazonaws.com" not in rendered


@pytest.mark.parametrize("bucket", ["data", "audit"])
@pytest.mark.parametrize("explicit", [False, True], ids=("sem_explicito", "com_explicito"))
@pytest.mark.parametrize("invalid", ["configured", "missing", "noncomputed", "type"])
def test_rejeita_hostname_configurado_ou_schema_invalido(
    bucket: str, explicit: bool, invalid: str
) -> None:
    records, configuration, schema = _contexto()
    address = DATA if bucket == "data" else AUDIT
    if explicit:
        record = next(record for record in records if record["address"] == address)
        record["fields"][HOST] = {"value": DEFAULT["value"], "source": {"explicit": True}}
    attributes = schema["provider_schemas"][PROVIDER]["resource_schemas"]["aws_s3_bucket"]
    fields = attributes["block"]["attributes"]
    if invalid == "configured":
        resources = configuration["root_module"]["module_calls"]["data_buckets"]["module"]
        resource = next(r for r in resources["resources"] if r["address"].endswith("." + bucket))
        resource["expressions"][HOST] = {"constant_value": "configured.s3.example.com"}
    elif invalid == "missing":
        del fields[HOST]
    elif invalid == "noncomputed":
        fields[HOST]["computed"] = False
    else:
        fields[HOST]["type"] = "number"
    before = deepcopy((records, configuration, schema, DEFAULT))
    with pytest.raises(pytest.fail.Exception, match="setup_error"):
        preserve_root_mock_defaults(records, configuration, schema, DEFAULT)
    assert (records, configuration, schema, DEFAULT) == before


def test_preservacao_nao_muta_entradas() -> None:
    records, configuration, schema = _contexto()
    context = deepcopy(DEFAULT)
    before = deepcopy((records, configuration, schema, context))
    result = preserve_root_mock_defaults(records, configuration, schema, context)
    assert result is not records
    assert (records, configuration, schema, context) == before
    result[0]["fields"]["arn"]["value"] = "changed-output-only"
    assert (records, configuration, schema, context) == before
