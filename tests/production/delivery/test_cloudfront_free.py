"""Reconciliação FREE usa o modelo AWS real e recusa deriva."""

from copy import deepcopy
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import Mock

import boto3
import pytest
from botocore.stub import Stubber

from scripts.promotion.reconcile_cloudfront_free import FreeRefused, reconcile

ACCOUNT = "123456789012"
BINDING = {
    "distribution_arn": f"arn:aws:cloudfront::{ACCOUNT}:distribution/EXAMPLE",
    "web_acl_arn": f"arn:aws:wafv2:us-east-1:{ACCOUNT}:global/webacl/demo/1234",
}
SUB = {
    "arn": f"arn:aws:pricingplanmanager::{ACCOUNT}:subscription/example",
    "planFamily": "CloudFront",
    "planTier": "FREE",
    "status": "ACTIVE",
    "resourceArns": list(BINDING.values()),
    "createdAt": datetime(2026, 11, 9, tzinfo=UTC),
    "updatedAt": datetime(2026, 11, 9, tzinfo=UTC),
}


def ports():
    ppm = Mock(meta=SimpleNamespace(region_name="us-east-1"))
    ppm.list_subscriptions.return_value = {"subscriptionSummaries": []}
    ppm.create_subscription.return_value = {"subscription": SUB, "eTag": "1"}
    ppm.get_subscription.return_value = {"subscription": SUB, "eTag": "1"}
    cf = Mock()
    cf.get_distribution.return_value = {
        "Distribution": {
            "ARN": BINDING["distribution_arn"],
            "Status": "Deployed",
            "DistributionConfig": {
                "WebACLId": BINDING["web_acl_arn"],
                "ViewerCertificate": {"SSLSupportMethod": "sni-only"},
                "Origins": {"Items": [{"Id": "private"}]},
                "DefaultCacheBehavior": {},
                "CacheBehaviors": {"Items": []},
            },
        }
    }
    cf.list_distributions_by_web_acl_id.return_value = {
        "DistributionList": {"Items": [{"ARN": BINDING["distribution_arn"]}], "IsTruncated": False}
    }
    cf.get_monitoring_subscription.return_value = {"MonitoringSubscription": {}}
    ft = Mock()
    ft.get_account_plan_state.return_value = {
        "accountId": ACCOUNT,
        "accountPlanType": "PAID",
        "accountPlanStatus": "ACTIVE",
    }
    return SimpleNamespace(ppm=ppm, cloudfront=cf, freetier=ft)


def test_repeticao_e_idempotente():
    api = ports()
    reconcile(BINDING, api)
    api.ppm.list_subscriptions.return_value = {"subscriptionSummaries": [SUB | {"eTag": "1"}]}
    reconcile(BINDING, api)
    assert api.ppm.create_subscription.call_count == 1
    request = api.ppm.create_subscription.call_args.kwargs
    assert request["planTier"] == "FREE"
    assert request["resourceArns"] == list(BINDING.values())
    assert len(request["clientToken"]) == 64


@pytest.mark.parametrize(
    "change",
    [
        {"planTier": "PRO"},
        {"status": "FAILED"},
        {"resourceArns": [BINDING["distribution_arn"]]},
        {"scheduledChange": {"changeType": "CANCELLATION"}},
    ],
)
def test_recusa_tier_pago_e_associacao_conflitante(change):
    api = ports()
    api.ppm.list_subscriptions.return_value = {"subscriptionSummaries": [SUB | change]}
    with pytest.raises(FreeRefused):
        reconcile(BINDING, api)
    api.ppm.create_subscription.assert_not_called()


def test_recusa_associacao_ambigua_e_paginacao_ciclica():
    api = ports()
    api.ppm.list_subscriptions.side_effect = [
        {"subscriptionSummaries": [SUB], "nextToken": "next"},
        {"subscriptionSummaries": [SUB | {"arn": "duplicate"}]},
    ]
    with pytest.raises(FreeRefused):
        reconcile(BINDING, api)
    api.ppm.create_subscription.assert_not_called()
    api = ports()
    api.ppm.list_subscriptions.return_value = {"subscriptionSummaries": [], "nextToken": "x"}
    with pytest.raises(FreeRefused):
        reconcile(BINDING, api)


@pytest.mark.parametrize("problem", ["free_account", "quota", "shared_waf", "region"])
def test_exige_elegibilidade_e_vaga_na_cota(problem):
    api = ports()
    if problem == "free_account":
        api.freetier.get_account_plan_state.return_value["accountPlanType"] = "FREE"
    elif problem == "quota":
        api.ppm.list_subscriptions.return_value = {
            "subscriptionSummaries": [
                SUB | {"arn": f"other-{i}", "resourceArns": [f"other-{i}"]} for i in range(3)
            ]
        }
    elif problem == "shared_waf":
        api.cloudfront.list_distributions_by_web_acl_id.return_value["DistributionList"][
            "Items"
        ].append({"ARN": "other"})
    else:
        api.ppm.meta.region_name = "us-east-2"
    with pytest.raises(FreeRefused):
        reconcile(BINDING, api)
    api.ppm.create_subscription.assert_not_called()


@pytest.mark.parametrize("error", [TimeoutError("secret"), ValueError("secret")])
def test_mutacao_incerta_nao_e_repetida(error):
    api = ports()
    api.ppm.create_subscription.side_effect = error
    with pytest.raises(FreeRefused, match="reason=uncertain_create"):
        reconcile(BINDING, api)
    assert api.ppm.create_subscription.call_count == 1


def test_readback_inativo_ou_com_arn_trocado_recusa():
    for change in ({"status": "SYNC_IN_PROGRESS"}, {"arn": "changed"}):
        api = ports()
        api.ppm.get_subscription.return_value = {"subscription": SUB | change}
        with pytest.raises(FreeRefused):
            reconcile(BINDING, api)


def test_pedido_e_resposta_validam_no_modelo_botocore_real():
    api = ports()
    api.ppm = boto3.client(
        "pricing-plan-manager",
        region_name="us-east-1",
        aws_access_key_id="synthetic",
        aws_secret_access_key="synthetic",  # noqa: S106
    )
    with Stubber(api.ppm) as stub:
        stub.add_response("list_subscriptions", {"subscriptionSummaries": [SUB | {"eTag": "1"}]})
        stub.add_response(
            "get_subscription", {"subscription": deepcopy(SUB), "eTag": "1"}, {"arn": SUB["arn"]}
        )
        reconcile(BINDING, api)
        stub.assert_no_pending_responses()


def test_journal_bloqueia_nova_mutacao_apos_resposta_incerta(tmp_path):
    api = ports()
    journal = tmp_path / "attempt.json"
    api.ppm.create_subscription.side_effect = TimeoutError("private")
    with pytest.raises(FreeRefused):
        reconcile(BINDING, api, journal)
    api.ppm.create_subscription.side_effect = None
    with pytest.raises(FreeRefused, match="prior_attempt"):
        reconcile(BINDING, api, journal)
    assert api.ppm.create_subscription.call_count == 1
    api.ppm.list_subscriptions.return_value = {"subscriptionSummaries": [SUB]}
    reconcile(BINDING, api, journal)
    assert api.ppm.create_subscription.call_count == 1


def test_create_usa_modelo_aws_e_nao_aprova_assinatura_paga():
    from botocore.stub import ANY

    api = ports()
    api.ppm = boto3.client(
        "pricing-plan-manager",
        region_name="us-east-1",
        aws_access_key_id="synthetic",
        aws_secret_access_key="synthetic",  # noqa: S106
    )
    with Stubber(api.ppm) as stub:
        stub.add_response("list_subscriptions", {"subscriptionSummaries": []})
        stub.add_response(
            "create_subscription",
            {"subscription": SUB, "eTag": "1"},
            {
                "planFamily": "CloudFront",
                "planTier": "FREE",
                "approvalMode": "IMMEDIATE",
                "resourceArns": list(BINDING.values()),
                "clientToken": ANY,
            },
        )
        stub.add_response(
            "get_subscription", {"subscription": SUB, "eTag": "1"}, {"arn": SUB["arn"]}
        )
        reconcile(BINDING, api)
        stub.assert_no_pending_responses()


@pytest.mark.parametrize("change", ["certificate", "shield", "logs", "monitoring"])
def test_recusa_recursos_pagos_sem_mutacao(change):
    api = ports()
    config = api.cloudfront.get_distribution.return_value["Distribution"]["DistributionConfig"]
    if change == "certificate":
        config["ViewerCertificate"]["SSLSupportMethod"] = "vip"
    elif change == "shield":
        config["Origins"]["Items"][0]["OriginShield"] = {"Enabled": True}
    elif change == "logs":
        config["DefaultCacheBehavior"]["RealtimeLogConfigArn"] = "private"
    else:
        api.cloudfront.get_monitoring_subscription.return_value = {
            "MonitoringSubscription": {
                "RealtimeMetricsSubscriptionConfig": {
                    "RealtimeMetricsSubscriptionStatus": "Enabled"
                }
            }
        }
    with pytest.raises(FreeRefused):
        reconcile(BINDING, api)
    api.ppm.create_subscription.assert_not_called()
