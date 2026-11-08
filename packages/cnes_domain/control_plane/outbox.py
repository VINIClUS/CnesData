"""Página imutável e geração durável da outbox."""

from dataclasses import dataclass

from cnes_domain.control_plane.entities import OutboxEvent


@dataclass(frozen=True, slots=True)
class OutboxCursor:
    """Posição opaca de uma geração da varredura."""

    version: int
    position: str | None

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version < 0:
            raise ValueError("version=invalid")
        if self.position is not None and type(self.position) is not str:
            raise ValueError("position=invalid")


@dataclass(frozen=True, slots=True)
class OutboxPage:
    """Eventos, token esperado e próxima posição candidata."""

    events: tuple[OutboxEvent, ...]
    expected: OutboxCursor | None
    next_cursor: OutboxCursor


def next_outbox_cursor(expected: OutboxCursor | None, position: str | None) -> OutboxCursor:
    """Args: Token esperado e posição opaca ou início da varredura.
    Returns: Cursor da geração seguinte.
    Raises: ValueError para geração ou posição inválida.
    """
    return OutboxCursor(1 if expected is None else expected.version + 1, position)


def validate_outbox_advance(
    expected: OutboxCursor | None, proposed: OutboxCursor | None,
) -> OutboxCursor:
    """Args: Token esperado e avanço proposto ou volta ao início.
    Returns: Cursor validado da geração seguinte.
    Raises: ValueError para transição de geração inválida.
    """
    following = next_outbox_cursor(expected, None) if proposed is None else proposed
    if following.version != (1 if expected is None else expected.version + 1):
        raise ValueError("version_transition=invalid")
    return following
