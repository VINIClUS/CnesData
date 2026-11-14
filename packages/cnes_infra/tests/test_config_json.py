"""Loader de /run/config/config.json do host gate sob o perfil AWS da imagem (#473)."""

import importlib
import io
import json
import os
import pathlib
import re
from pathlib import Path

import dotenv
import pytest

CHAVE = "_TEST_CONFIG_JSON_CHAVE"
AWS_SRC = Path(__file__).resolve().parents[1] / "src/cnes_infra/aws"


def _config(tmp_path: Path, valores: object) -> Path:
    arquivo = tmp_path / "config.json"
    arquivo.write_text(json.dumps(valores), encoding="utf-8")
    return arquivo


def _credenciais(tmp_path: Path) -> dict[str, str]:
    return {
        "AWS_CONFIG_FILE": "/etc/cnesdata/aws-config",
        "AWS_SHARED_CREDENTIALS_FILE": os.devnull,
        "AWS_EC2_METADATA_DISABLED": "true",
    }


def test_arquivo_ausente_nao_altera_o_ambiente(tmp_path: Path) -> None:
    from cnes_infra.config import carregar_config_json

    ambiente = {"DB_URL": "postgresql://u:p@h/db", "PROFILE": "aws"}
    carregar_config_json(tmp_path / "ausente.json", ambiente)
    assert ambiente == {"DB_URL": "postgresql://u:p@h/db", "PROFILE": "aws"}


def test_arquivo_preenche_chaves_ausentes_do_ambiente(tmp_path: Path) -> None:
    from cnes_infra.config import carregar_config_json

    ambiente = _credenciais(tmp_path)
    valores = {"AWS_REGION": "us-east-2", "DB_URL": "postgresql://u:p@h/db"}
    carregar_config_json(_config(tmp_path, valores), ambiente)
    assert ambiente == _credenciais(tmp_path) | valores


def test_mesmo_valor_no_ambiente_e_no_arquivo_e_aceito(tmp_path: Path) -> None:
    from cnes_infra.config import carregar_config_json

    ambiente = _credenciais(tmp_path) | {"PROFILE": "aws"}
    carregar_config_json(_config(tmp_path, {"PROFILE": "aws", "AWS_REGION": "x"}), ambiente)
    assert ambiente == _credenciais(tmp_path) | {"PROFILE": "aws", "AWS_REGION": "x"}


def test_conflito_com_o_ambiente_e_recusado_sem_alterar_nada(tmp_path: Path) -> None:
    from cnes_infra.config import carregar_config_json

    ambiente = _credenciais(tmp_path) | {"PROFILE": "aws"}
    antes = dict(ambiente)
    arquivo = _config(tmp_path, {"AWS_REGION": "us-east-2", "PROFILE": "local"})
    with pytest.raises(OSError, match="config_json=conflict key=PROFILE"):
        carregar_config_json(arquivo, ambiente)
    assert ambiente == antes


@pytest.mark.parametrize(
    ("conteudo", "erro"),
    [
        (b"{nao json", "invalid_json"),
        (b"\xff\xfe", "invalid_json"),
        (b"[]", "not_object"),
        (b'{"minuscula": "x"}', "invalid_key key=minuscula"),
        (b'{"1A": "x"}', "invalid_key"),
        (b'{"A": 1}', "invalid_value key=A"),
        (b'{"A": null}', "invalid_value key=A"),
        (b'{"A": "a\\u0000b"}', "invalid_value key=A"),
        (b'{"A": "ok", "B": "\\ud800"}', "invalid_value key=B"),
        (b'{"A": "x", "A": "y"}', "duplicate_key key=A"),
        (b'{"A": "' + b"x" * 65536 + b'"}', "too_large"),
    ],
)
def test_conteudo_fora_do_contrato_e_recusado_sem_alterar_nada(
    tmp_path: Path, conteudo: bytes, erro: str,
) -> None:
    from cnes_infra.config import carregar_config_json

    arquivo = tmp_path / "config.json"
    arquivo.write_bytes(conteudo)
    ambiente = _credenciais(tmp_path)
    with pytest.raises(OSError, match=f"config_json={erro}"):
        carregar_config_json(arquivo, ambiente)
    assert ambiente == _credenciais(tmp_path)


@pytest.mark.parametrize(
    "chave",
    [
        "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_SECURITY_TOKEN",
        "AWS_PROFILE", "AWS_DEFAULT_PROFILE", "AWS_CONFIG_FILE", "AWS_SHARED_CREDENTIALS_FILE",
        "AWS_ROLE_ARN", "AWS_ROLE_SESSION_NAME", "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_CA_BUNDLE",
        "AWS_DATA_PATH", "AWS_CONTAINER_CREDENTIALS_FULL_URI",
        "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", "AWS_EC2_METADATA_SERVICE_ENDPOINT",
        "AWS_EC2_METADATA_DISABLED", "AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_STS",
        "DYNAMODB_ENDPOINT_URL", "LD_PRELOAD", "PYTHONPATH", "DOCKER_HOST", "SSH_AUTH_SOCK",
        "AWS_USE_FIPS_ENDPOINT", "AWS_USE_DUALSTACK_ENDPOINT", "AWS_STS_REGIONAL_ENDPOINTS",
        "AWS_S3_US_EAST_1_REGIONAL_ENDPOINT", "AWS_IGNORE_CONFIGURED_ENDPOINT_URLS",
        "AWS_METADATA_SERVICE_TIMEOUT", "AWS_ACCOUNT_ID", "AWS_CREDENTIAL_FILE", "AWS_CHAVE_NOVA",
        "BOTO_CONFIG", "HOME", "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY",
        "SSL_CERT_FILE", "SSL_CERT_DIR", "SSLKEYLOGFILE", "REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE",
        "OPENSSL_CONF", "S3_ENDPOINT_URL", "S3_PUBLIC_ENDPOINT_URL",
    ],
)
def test_arquivo_nao_define_fonte_de_credencial_nem_endpoint(tmp_path: Path, chave: str) -> None:
    from cnes_infra.config import carregar_config_json

    ambiente = _credenciais(tmp_path)
    arquivo = _config(tmp_path, {"AWS_REGION": "us-east-2", chave: "x"})
    with pytest.raises(OSError, match=f"config_json=credential_authority key={chave}"):
        carregar_config_json(arquivo, ambiente)
    assert ambiente == _credenciais(tmp_path)


def test_chaves_aws_lidas_pelo_produto_sao_carregaveis(tmp_path: Path) -> None:
    from cnes_infra.config import carregar_config_json

    literais = {
        chave for fonte in sorted(AWS_SRC.glob("*.py"))
        for chave in re.findall(r"[\"'](AWS_[A-Z0-9_]+)[\"']", fonte.read_text(encoding="utf-8"))
    }
    produto = literais - {"AWS_ENDPOINT_URL"}
    assert {"AWS_REGION", "AWS_CONTROL_PLANE_TABLE", "AWS_PRODUCTION_POLICY"} <= produto
    ambiente = _credenciais(tmp_path)
    carregar_config_json(_config(tmp_path, dict.fromkeys(sorted(produto), "v")), ambiente)
    assert ambiente == _credenciais(tmp_path) | dict.fromkeys(produto, "v")


def test_config_sob_arquivo_regular_equivale_a_ausente(tmp_path: Path) -> None:
    from cnes_infra.config import carregar_config_json

    (tmp_path / "run").write_text("", encoding="utf-8")
    ambiente = {"PROFILE": "aws"}
    carregar_config_json(tmp_path / "run" / "config.json", ambiente)
    assert ambiente == {"PROFILE": "aws"}


def test_config_ilegivel_e_recusado(tmp_path: Path) -> None:
    from cnes_infra.config import carregar_config_json

    diretorio = tmp_path / "config.json"
    diretorio.mkdir()
    with pytest.raises(OSError, match="config_json=unreadable"):
        carregar_config_json(diretorio, _credenciais(tmp_path))


@pytest.mark.parametrize(
    "caminho", [None, "", "relativo/credentials", "/nao/existe/credentials", os.devnull],
)
def test_config_presente_nao_exige_arquivo_de_credencial(
    tmp_path: Path, caminho: str | None,
) -> None:
    from cnes_infra.config import carregar_config_json

    ambiente = {} if caminho is None else {"AWS_SHARED_CREDENTIALS_FILE": caminho}
    antes = dict(ambiente)
    carregar_config_json(_config(tmp_path, {"AWS_REGION": "us-east-2"}), ambiente)
    assert ambiente == antes | {"AWS_REGION": "us-east-2"}


def test_config_presente_aplica_sob_o_perfil_aws_da_imagem(tmp_path: Path) -> None:
    from cnes_infra.config import carregar_config_json

    ambiente = _credenciais(tmp_path) | {"PROFILE": "aws", "AWS_PRODUCTION_POLICY": "true"}
    antes = dict(ambiente)
    carregar_config_json(_config(tmp_path, {"AWS_REGION": "us-east-2", "PROFILE": "aws"}), ambiente)
    assert ambiente == antes | {"AWS_REGION": "us-east-2"}


@pytest.fixture
def recarregar_config(monkeypatch: pytest.MonkeyPatch):
    yield
    monkeypatch.undo()
    os.environ.pop(CHAVE, None)
    from cnes_infra import config

    importlib.reload(config)


def test_modulo_aplica_o_config_fixo_antes_do_dotenv(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, recarregar_config: None,
) -> None:
    for chave, valor in _credenciais(tmp_path).items():
        monkeypatch.setenv(chave, valor)
    monkeypatch.delenv(CHAVE, raising=False)
    bruto = json.dumps({CHAVE: "do-arquivo"}).encode()
    abrir = pathlib.Path.open

    def abrir_fixo(self: Path, *args: object, **kwargs: object):
        if self == Path("/run/config/config.json"):
            return io.BytesIO(bruto)
        return abrir(self, *args, **kwargs)  # type: ignore[arg-type]

    vistos: list[str | None] = []
    monkeypatch.setattr(pathlib.Path, "open", abrir_fixo)
    monkeypatch.setattr(dotenv, "load_dotenv", lambda *a, **k: vistos.append(os.getenv(CHAVE)))
    from cnes_infra import config

    importlib.reload(config)
    assert Path("/run/config/config.json") == config.CONFIG_JSON
    assert vistos == ["do-arquivo"]
    assert os.environ[CHAVE] == "do-arquivo"


def test_modulo_sem_config_mantem_o_caminho_por_env(
    monkeypatch: pytest.MonkeyPatch, recarregar_config: None,
) -> None:
    monkeypatch.setattr(dotenv, "load_dotenv", lambda *a, **k: True)
    monkeypatch.setenv("API_PORT", "8123")
    antes = dict(os.environ)
    from cnes_infra import config

    importlib.reload(config)
    assert dict(os.environ) == antes
    assert config.API_PORT == 8123
