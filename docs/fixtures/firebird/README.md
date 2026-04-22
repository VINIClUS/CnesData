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
