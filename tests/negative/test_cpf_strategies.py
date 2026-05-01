"""Invalid CPFs must raise InvalidCPFError, never accept."""

from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st

from cnes_domain.validators import InvalidCPFError, validate_cpf


@pytest.mark.negative
@given(
    bad=st.one_of(
        st.text(min_size=0, max_size=10),
        st.text(min_size=12, max_size=20),
        st.text(alphabet="abcdefghij", min_size=11, max_size=11),
        st.just("00000000000"),
        st.just("11111111111"),
    )
)
def test_cpf_invalido_rejeita(bad):
    with pytest.raises(InvalidCPFError):
        validate_cpf(bad)
