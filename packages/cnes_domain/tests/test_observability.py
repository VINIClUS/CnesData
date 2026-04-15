from cnes_domain.observability import tracer


def test_tracer_nao_none():
    assert tracer is not None


def test_tracer_callable():
    assert callable(getattr(tracer, "start_as_current_span", None))
