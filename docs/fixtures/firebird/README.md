# Firebird 1.5.6 Embed Fixture

## Purpose

Local + CI test fixture for reading Firebird 1.5 format databases
(primarily `BPAMAG.GDB` from BPA-Mag). Embedded DLL variant — no
server install required; clients link `fbclient.dll` directly.

## Version pinned

- **Version:** 1.5.6.5026-0 (Win32 embedded)
- **Source:** Official Firebird project release
- **File:** `Firebird-1.5.6.5026-0_embed_win32.zip` (1,630,011 bytes)
- **SHA256:** `540f6ede8ee625afdb547bb6515ed7328a3fa87cb6769a4ecbfec41c54032be7`

## Setup

```bash
git lfs pull              # hydrate the zip after clone
python scripts/fb156_setup.py
```

Produces `.cache/firebird-1.5.6/fbembed.dll` plus a `fbclient.dll`
symlink/copy for client library compatibility.

## Consumer examples

### Go (nakagami/firebirdsql)

```go
// DSN with explicit library path
dsn := "user=SYSDBA;password=masterkey;sqldialect=3;library=.cache/firebird-1.5.6/fbclient.dll"
db, err := sql.Open("firebirdsql", dsn + ";database=localhost:/path/to/BPAMAG.GDB")
```

### Python (fdb)

```python
import fdb

fdb.load_api(fb_library_name=".cache/firebird-1.5.6/fbclient.dll")
con = fdb.connect(
    dsn="localhost:/path/to/BPAMAG.GDB",
    user="SYSDBA",
    password="masterkey",
)
```

## Architecture constraint — x86 only

Firebird 1.5.6 was released pre-x64; only 32-bit Windows builds exist.
The embedded `fbclient.dll` is **x86** — loadable only by a 32-bit
process. Loading from 64-bit Python/Go will fail with:

```
OSError: [WinError 193] %1 is not a valid Win32 application
```

Consumption strategies:

- **Go BPA intent** → build with `GOARCH=386` (or a 32-bit subprocess bridge).
- **Python BPA reader** → use 32-bit Python (`py -3.13-32`), OR spawn a 32-bit sidecar that streams rows back.
- **One-shot extraction** (recommended for analysis-only): run `.cache/firebird-1.5.6/isql.exe` once to dump `BPAMAG.GDB` → Parquet/CSV, then process with 64-bit tooling.

BPA-Mag itself is a 32-bit application, so its native `BPAMAG.GDB` is
naturally paired with a 32-bit FB client. This fixture matches the
original application's architecture.

## Troubleshooting

### `git lfs pull` required after clone

If the zip is ~130 bytes (LFS pointer), you forgot to pull:
```bash
git lfs pull
```

### Windows symlink OSError 1314

`scripts/fb156_setup.py` falls back to copying `fbembed.dll → fbclient.dll`
when `os.symlink` fails (common when running as non-admin user). Expected
behaviour; no action required.

## License

Firebird is distributed under a dual-license:
- **Initial Developer's Public License (IDPL)**
- **InterBase Public License (IPL)**

Full text in `LICENSE-IPL.txt`. Redistribution of the unmodified zip
requires attribution — satisfied by this README + `LICENSE-IPL.txt`.

Upstream: https://firebirdsql.org/en/firebird-1-5/
