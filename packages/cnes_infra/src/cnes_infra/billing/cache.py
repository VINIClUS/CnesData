"""Cache local de entitlements com TTL curto e invalidação por versão."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from threading import Lock

from cnes_domain.billing.models import EntitlementSnapshot
from cnes_domain.billing.ports import ClockPort
from cnes_domain.billing.validation import require_id, require_positive

MAX_CACHE_TTL_SECONDS = 60


@dataclass(frozen=True, slots=True)
class CacheKey:
    billing_account_id: str
    entitlement_version: int


@dataclass(frozen=True, slots=True)
class EntitlementChange:
    billing_account_id: str
    new_entitlement_version: int

    def __post_init__(self) -> None:
        require_id(self.billing_account_id, "billing_account_id")
        require_positive(self.new_entitlement_version, "new_entitlement_version")


@dataclass(frozen=True, slots=True)
class _Entry:
    snapshot: EntitlementSnapshot
    expires_at: datetime


class LocalEntitlementCache:
    """Cache local de leitura para UI e emissão de cookie de URL.

    Nunca é usado por gates críticos, que leem a projeção com consistência forte.
    """

    def __init__(self, max_ttl_seconds: int, clock: ClockPort) -> None:
        _validate_ttl(max_ttl_seconds)
        self._ttl = timedelta(seconds=max_ttl_seconds)
        self._clock = clock
        self._lock = Lock()
        self._entries: dict[CacheKey, _Entry] = {}
        self._latest: dict[str, int] = {}
        self._floors: dict[str, int] = {}

    def put(self, snapshot: EntitlementSnapshot) -> None:
        """Armazena o snapshot, ignorando versões abaixo do piso de invalidação."""
        account = snapshot.billing_account_id
        version = snapshot.entitlement_version
        with self._lock:
            self._evict_expired()
            if version < self._floors.get(account, 0):
                return
            self._entries[CacheKey(account, version)] = _Entry(snapshot, self._clock() + self._ttl)
            self._latest[account] = max(version, self._latest.get(account, 0))

    def get(self, key: CacheKey) -> EntitlementSnapshot | None:
        """Retorna o snapshot vigente da chave ou None se ausente ou expirado."""
        with self._lock:
            return self._lookup(key)

    def get_latest(self, billing_account_id: str) -> EntitlementSnapshot | None:
        """Retorna a maior versão local da conta ou None se ausente ou expirada."""
        with self._lock:
            version = self._latest.get(billing_account_id)
            if version is None:
                return None
            return self._lookup(CacheKey(billing_account_id, version))

    def invalidate_before(self, billing_account_id: str, entitlement_version: int) -> None:
        """Remove versões anteriores da conta e eleva o piso de invalidação."""
        with self._lock:
            floor = max(self._floors.get(billing_account_id, 0), entitlement_version)
            self._floors[billing_account_id] = floor
            stale = [
                key
                for key in self._entries
                if key.billing_account_id == billing_account_id and key.entitlement_version < floor
            ]
            for key in stale:
                del self._entries[key]
            self._drop_stale_latest(billing_account_id, floor)

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)

    def _evict_expired(self) -> None:
        now = self._clock()
        expired = [key for key, entry in self._entries.items() if now >= entry.expires_at]
        for key in expired:
            del self._entries[key]

    def _lookup(self, key: CacheKey) -> EntitlementSnapshot | None:
        entry = self._entries.get(key)
        if entry is None:
            return None
        if self._clock() >= entry.expires_at:
            del self._entries[key]
            return None
        return entry.snapshot

    def _drop_stale_latest(self, account: str, floor: int) -> None:
        if self._latest.get(account, floor) < floor:
            del self._latest[account]


def handle_entitlement_changed(change: EntitlementChange, cache: LocalEntitlementCache) -> None:
    """Invalida versões anteriores da conta; entrega perdida é limitada pelo TTL."""
    cache.invalidate_before(change.billing_account_id, change.new_entitlement_version)


def _validate_ttl(max_ttl_seconds: int) -> None:
    if isinstance(max_ttl_seconds, bool) or not isinstance(max_ttl_seconds, int):
        raise ValueError("cache_ttl_invalid")
    if max_ttl_seconds <= 0:
        raise ValueError("cache_ttl_invalid")
    if max_ttl_seconds > MAX_CACHE_TTL_SECONDS:
        raise ValueError("cache_ttl_gt_60")
