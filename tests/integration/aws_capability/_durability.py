"""Observação independente dos syscalls de persistência do journal."""

import os
import stat
from dataclasses import dataclass, field
from pathlib import Path

import pytest


@dataclass
class DurabilityTrace:
    events: list[tuple] = field(default_factory=list)

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        original_sync = os.fsync
        original_replace = os.replace

        def synced(fd: int) -> None:
            info = os.fstat(fd)
            path = Path(os.readlink(f"/proc/self/fd/{fd}"))
            original_sync(fd)
            if stat.S_ISDIR(info.st_mode):
                self.events.append(("directory", info.st_ino, path))
            else:
                self.events.append(("file", info.st_ino, path, path.read_bytes()))

        def replaced(source: str | Path, target: str | Path, **kwargs: int | None) -> None:
            inode = Path(source).stat().st_ino
            original_replace(source, target, **kwargs)
            self.events.append(("replace", inode, Path(source), Path(target)))

        monkeypatch.setattr(os, "fsync", synced)
        monkeypatch.setattr(os, "replace", replaced)


def assert_durable(trace: DurabilityTrace, journal: Path) -> None:
    assert journal.exists()
    inode = journal.stat().st_ino
    replacements = [(i, e) for i, e in enumerate(trace.events) if e[0] == "replace"]
    assert replacements, "oracle=missing_atomic_replace"
    position, replaced = replacements[-1]
    assert replaced[1] == inode
    assert replaced[3] == journal
    assert replaced[2].parent == journal.parent
    synced = [event for event in trace.events[:position] if event[0] == "file"]
    assert synced, "oracle=missing_file_sync"
    assert synced[-1][1:3] == replaced[1:3], "oracle=wrong_inode_or_path"
    assert synced[-1][3] == journal.read_bytes(), "oracle=unsynced_contents"
    parents = [event for event in trace.events[position + 1 :] if event[0] == "directory"]
    assert parents, "oracle=missing_parent_sync_after_replace"
    assert parents[-1][1:] == (journal.parent.stat().st_ino, journal.parent)
