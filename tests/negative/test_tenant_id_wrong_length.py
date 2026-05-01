"""tenant_id diferente de 6 chars ou nao-digitos rejeita."""

from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st

from cnes_domain.tenant import InvalidTenantError, validate_tenant_id


@pytest.mark.negative
@given(
    bad=st.one_of(
        st.text(min_size=0, max_size=5),
        st.text(min_size=7, max_size=20),
        st.text(alphabet="abcdef", min_size=6, max_size=6),
    )
)
def test_tenant_invalido_rejeita(bad):
    with pytest.raises(InvalidTenantError):
        validate_tenant_id(bad)
