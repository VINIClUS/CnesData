"""Invalid CNS must raise InvalidCNSError."""

from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st

from cnes_domain.validators import InvalidCNSError, validate_cns


@pytest.mark.negative
@given(
    bad=st.one_of(
        st.text(min_size=0, max_size=14),
        st.text(min_size=16, max_size=25),
        st.text(alphabet="abcdefghij", min_size=15, max_size=15),
    )
)
def test_cns_invalido_rejeita(bad):
    with pytest.raises(InvalidCNSError):
        validate_cns(bad)
