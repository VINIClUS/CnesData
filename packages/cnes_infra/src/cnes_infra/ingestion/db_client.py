import logging

import pandas as pd
from sqlalchemy import create_engine


def load_from_sql(query: str, connection_string: str) -> pd.DataFrame:
    """Executa consulta SQL e retorna DataFrame."""
    try:
        engine = create_engine(connection_string)
        df = pd.read_sql(query, engine)
        logging.getLogger(__name__).info(
            "sql_ok rows=%d", len(df),
        )
        return df
    except Exception:
        logging.getLogger(__name__).exception("sql_error")
        return pd.DataFrame()
