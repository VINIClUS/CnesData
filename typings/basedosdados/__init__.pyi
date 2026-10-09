from typing import Any

from pandas import DataFrame

def read_sql(
    query: str,
    billing_project_id: str | None = None,
    from_file: bool = False,
    reauth: bool = False,
    use_bqstorage_api: bool = False,
) -> DataFrame: ...
def __getattr__(name: str) -> Any: ...
