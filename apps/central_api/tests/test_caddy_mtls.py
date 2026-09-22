"""Caddyfile: api.* vhosts verificam o cert do agente e sobrescrevem o header (#253)."""
import re
from pathlib import Path

_CADDYFILE = Path(__file__).resolve().parents[3] / "deploy/prod/caddy/Caddyfile"


def _site_block(config: str, host: str) -> str:
    match = re.search(rf"^{re.escape(host)} \{{\n(.*?)^\}}", config, re.S | re.M)
    assert match, f"vhost ausente: {host}"
    return match.group(1)


def test_snippet_mtls_verifica_cert_quando_apresentado():
    config = _CADDYFILE.read_text(encoding="utf-8")
    assert "mode verify_if_given" in config
    assert "trust_pool file {args[0]}" in config


def test_snippet_upstream_sobrescreve_header_do_certificado():
    config = _CADDYFILE.read_text(encoding="utf-8")
    assert (
        'header_up X-SSL-Client-Cert "{http.request.tls.client.certificate_der_base64}"'
        in config
    )
    assert "header_up -X-SSL-Client-Verify" in config


def test_vhosts_api_usam_ca_do_proprio_ambiente_e_upstream_com_header():
    config = _CADDYFILE.read_text(encoding="utf-8")
    for host, ca, upstream in (
        ("api.cnesdata.com.br", "prod-ca.crt", "central-api:8000"),
        ("api.dev.cnesdata.com.br", "dev-ca.crt", "dev-central-api:8000"),
    ):
        block = _site_block(config, host)
        assert f"import agent_mtls /etc/caddy/ca/{ca}" in block
        assert f"import agent_upstream {upstream}" in block
        assert "reverse_proxy" not in block


def test_vhosts_do_dashboard_removem_header_de_certificado():
    config = _CADDYFILE.read_text(encoding="utf-8")
    for host in ("cnesdata.com.br", "dev.cnesdata.com.br"):
        assert "request_header -X-SSL-Client-Cert" in _site_block(config, host)
