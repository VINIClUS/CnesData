"""Control-plane domain errors."""


class Conflict(RuntimeError):
    pass


class InvalidTransition(Conflict):
    pass


class LeaseLost(Conflict):
    pass


class FenceRejected(Conflict):
    pass


class NotFound(LookupError):
    pass
