"""Competencia fora de range ou mes invalido e rejeitada."""

from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st

from cnes_domain.validators import InvalidCompetenciaError, validate_competencia


@pytest.mark.negative
@given(
    bad=st.one_of(
        st.integers(min_value=-1000, max_value=200000),
        st.integers(min_value=210000, max_value=999999),
        st.sampled_from([202013, 202014, 202015, 202020, 202099]),
        st.sampled_from([202000]),
    )
)
def test_competencia_invalida_rejeita(bad):
    with pytest.raises(InvalidCompetenciaError):
        validate_competencia(bad)
