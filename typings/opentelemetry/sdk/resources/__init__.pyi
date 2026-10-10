from collections.abc import Mapping

class Resource:
    @classmethod
    def create(
        cls, attributes: Mapping[str, object] | None = None, schema_url: str | None = None,
    ) -> Resource: ...
