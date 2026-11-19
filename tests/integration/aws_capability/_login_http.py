"""Porta HTTP do login: toda chamada consome orçamento e vira status mais corpo JSON."""

from collections.abc import Mapping
from dataclasses import dataclass, field

import httpx

from tests.integration.aws_capability.contracts import RequestContext


@dataclass(frozen=True)
class HttpReply:
    status: int
    body: Mapping[str, object] = field(default_factory=dict, repr=False)


@dataclass(frozen=True)
class Http:
    client: httpx.Client = field(repr=False)
    context: RequestContext

    def get(self, url: str) -> HttpReply:
        self.context.budget.acquire(self.context.lane)
        try:
            return _reply(self.client.get(url, follow_redirects=True))
        except httpx.HTTPError:
            return HttpReply(0)

    def post_form(self, url: str, form: Mapping[str, str]) -> HttpReply:
        self.context.budget.acquire(self.context.lane)
        try:
            return _reply(self.client.post(url, data=dict(form)))
        except httpx.HTTPError:
            return HttpReply(0)


def _reply(response: httpx.Response) -> HttpReply:
    try:
        body = response.json()
    except ValueError:
        body = {}
    return HttpReply(response.status_code, body if isinstance(body, dict) else {})
