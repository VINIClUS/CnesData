"""Rollback, drain e poda das revisões exatas do processor."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from scripts.promotion import prune_revisions as prune

ACCOUNT, NOW = "123456789012", datetime(2026, 11, 14, 12, tzinfo=UTC)
URI = f"{ACCOUNT}.dkr.ecr.us-east-2.amazonaws.com/cnesdata-prod-processor"
MODES = ("unit", "recovery", "audit")
DIGESTS = {n: "sha256:" + str(n) * 64 for n in range(1, 7)}
AGES = {1: 40, 2: 35, 3: 10, 4: 1, 5: 3, 6: 2}


def _arn(mode, number):
    family = f"cnesdata-prod-processor-{mode}"
    return f"arn:aws:ecs:us-east-2:{ACCOUNT}:task-definition/{family}:{number}"


def _tag(number):
    return f"candidate-{'a' * 40}-{number}-1"


class Registry:
    def __init__(self, releases=(1, 2, 3, 4), page=2):
        self.images = {DIGESTS[n]: [NOW - timedelta(days=AGES[n]), [_tag(n)]] for n in releases}
        self.revisions = {_arn(m, n): DIGESTS[n] for n in releases for m in MODES}
        self.page, self.calls, self.reads, self.failures, self.after_read = page, [], 0, [], {}

    def _paged(self, items, key, request):
        start = int(request.get("nextToken") or 0)
        body = {key: items[start:start + self.page]}
        more = start + self.page < len(items)
        return body | ({"nextToken": str(start + self.page)} if more else {})

    def describe_images(self, **request):
        assert request["repositoryName"] == "cnesdata-prod-processor"
        self.reads += "nextToken" not in request
        self.after_read.pop(self.reads, lambda: None)()
        details = [{"imageDigest": d, "imagePushedAt": at, **({"imageTags": tags} if tags else {})}
                   for d, (at, tags) in sorted(self.images.items())]
        return self._paged(details, "imageDetails", request)

    def list_task_definitions(self, **request):
        assert request["status"] == "ACTIVE"
        family = request["familyPrefix"]
        arns = sorted(a for a in self.revisions if a.split("/")[1].split(":")[0] == family)
        return self._paged(arns, "taskDefinitionArns", request)

    def describe_task_definition(self, **request):
        arn = request["taskDefinition"]
        image = f"{URI}@{self.revisions[arn]}"
        return {"taskDefinition": {"taskDefinitionArn": arn, "status": "ACTIVE",
                                   "containerDefinitions": [{"image": image}]}}

    def deregister_task_definition(self, **request):
        self.calls.append(("deregister", request["taskDefinition"]))
        del self.revisions[request["taskDefinition"]]

    def batch_delete_image(self, **request):
        self.calls.append(("delete", request["repositoryName"], request["imageIds"]))
        for item in request["imageIds"]:
            self.images.pop(item["imageDigest"])
        return {"imageIds": request["imageIds"], "failures": self.failures}


def _refs(current=4, prior=3, in_flight=(), retained=None, drained=True):
    units = retained if retained is not None else (prior, current)
    return prune.PruneReferences(
        current=frozenset(_arn(m, current) for m in MODES),
        prior=frozenset(_arn(m, prior) for m in MODES),
        in_flight=frozenset(in_flight), retained=frozenset(_arn("unit", n) for n in units),
        drained=drained)


def _prune(registry, refs=None):
    return prune.execute_prune({"ecr": registry, "ecs": registry}, refs or _refs(), NOW)


def _deleted(registry):
    return [item[2][0]["imageDigest"] for item in registry.calls if item[0] == "delete"]


def _deregistered(registry):
    return sorted(item[1] for item in registry.calls if item[0] == "deregister")


def test_protege_atual_anterior_e_referenciada_por_execucao():
    registry = Registry()
    evidence = _prune(registry, _refs(in_flight={_arn("unit", 2)}))
    assert evidence["outcome"] == "PLANNED"
    assert _deleted(registry) == [DIGESTS[1]] == evidence["prune"]
    assert _deregistered(registry) == sorted(_arn(m, 1) for m in MODES)
    assert all(_arn(m, n) in registry.revisions for m in MODES for n in (2, 3, 4))
    assert set(registry.images) == {DIGESTS[n] for n in (2, 3, 4)}


def test_so_revisao_antiga_sem_referencia_e_elegivel():
    registry = Registry()
    registry.revisions[_arn("recovery", 5)] = DIGESTS[2]
    evidence = _prune(registry, _refs(retained=(1, 3, 4)))
    assert _deleted(registry) == []
    assert evidence["prune"] == []
    assert evidence["outcome"] == "PLANNED"
    assert _deregistered(registry) == []
    assert evidence["retained_after"] == sorted(_arn("unit", n) for n in (3, 4))
    registry = Registry()
    evidence = _prune(registry)
    assert _deleted(registry) == [DIGESTS[1], DIGESTS[2]]
    assert _deregistered(registry) == sorted(_arn(m, n) for m in MODES for n in (1, 2))
    assert evidence["deregistered"] == _deregistered(registry)


def test_mapeia_revisao_para_digest_e_consome_processor_retention(monkeypatch):
    seen = []
    real = prune.plan_processor_retention
    monkeypatch.setattr(prune, "plan_processor_retention",
                        lambda *args: seen.append(args) or real(*args))
    registry = Registry(releases=(1, 2, 3, 4, 5, 6))
    evidence = _prune(registry, _refs(current=6, prior=5, in_flight={_arn("audit", 4)},
                                      retained=(3, 5, 6)))
    snapshot, candidate, now = seen[0]
    assert (candidate.digest, now) == (DIGESTS[6], NOW)
    assert candidate.first_admitted_at == NOW - timedelta(days=2)
    references = snapshot.references
    assert (references.current, references.prior) == ({DIGESTS[6]}, {DIGESTS[5]})
    assert references.in_flight == {DIGESTS[4]}
    assert references.retained_revisions == {DIGESTS[n] for n in (3, 5, 6)}
    assert {image.digest for image in snapshot.history} == set(DIGESTS.values())
    assert snapshot.complete is True
    assert evidence["prune"] == [DIGESTS[1], DIGESTS[2]] == _deleted(registry)


def _untag(registry, number):
    registry.images[DIGESTS[number]][1] = []


INCOMPLETE = {
    "digest_referenciado_sem_tag": lambda r: _untag(r, 3),
    "revisao_fora_do_repositorio": lambda r: r.revisions.update({_arn("unit", 1): "x@" + "y"}),
    "revisao_referenciada_ausente": lambda r: r.revisions.pop(_arn("audit", 3)),
    "paginas_sem_fim": lambda r: setattr(r, "page", 0),
}


@pytest.mark.parametrize("case_name", sorted(INCOMPLETE))
def test_inventario_incompleto_nao_poda(case_name):
    registry = Registry()
    INCOMPLETE[case_name](registry)
    evidence = _prune(registry)
    assert evidence["outcome"] == "REJECTED_INCOMPLETE"
    assert registry.calls == []
    assert evidence["retained_after"] == sorted(_refs().retained)


def test_digest_com_retencao_vencida_e_podado():
    registry = Registry(releases=(1, 3, 4))
    evidence = _prune(registry)
    assert registry.calls[-1] == ("delete", "cnesdata-prod-processor",
                                  [{"imageDigest": DIGESTS[1]}])
    assert [call[0] for call in registry.calls] == ["deregister"] * 3 + ["delete"]
    assert DIGESTS[1] not in registry.images
    assert _tag(1) not in str(registry.images)
    assert evidence["readbacks"][0] == evidence["readbacks"][1] != evidence["readbacks"][2]
    registry = Registry(releases=(1, 2, 3, 4, 5, 6))
    for number in (1, 2):
        registry.images[DIGESTS[number]][0] = NOW - timedelta(days=5 + number)
    _prune(registry, _refs(current=6, prior=5, retained=(5, 6)))
    assert _deleted(registry) == [DIGESTS[3]]


def _changed(registry):
    registry.after_read[2] = lambda: registry.images.update({DIGESTS[5]: [NOW, [_tag(5)]]})


UNCERTAIN = {
    "inventario_muda_entre_leituras": (_changed, "prune=readback_changed"),
    "falha_no_delete": (lambda r: setattr(r, "failures", [{"failureCode": "x"}]),
                        "prune=delete_failed"),
    "imagem_volta_no_readback": (lambda r: r.after_read.update(
        {3: lambda: r.images.setdefault(DIGESTS[1], [NOW, [_tag(1)]])}), "prune=post_readback"),
}


@pytest.mark.parametrize("case_name", sorted(UNCERTAIN))
def test_incerteza_no_readback_mantem_recursos(case_name):
    registry = Registry(releases=(1, 3, 4))
    edit, reason = UNCERTAIN[case_name]
    edit(registry)
    with pytest.raises(ValueError, match=reason):
        _prune(registry)
    if case_name == "inventario_muda_entre_leituras":
        assert registry.calls == []


def test_sem_drain_mantem_revisoes_digests_e_grants():
    registry = Registry()
    refs = replace(_refs(retained=(1, 2, 3, 4)), drained=False)
    evidence = _prune(registry, refs)
    assert evidence["outcome"] == "NOT_DRAINED"
    assert registry.calls == []
    assert registry.reads == 0
    assert evidence["retained_after"] == sorted(refs.retained)
