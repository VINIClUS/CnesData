# NTP Sync em Edge Machines

## Por quê

Clock drift > 5min invalida presigned URLs (HTTP 403 `RequestTimeTooSkewed`).
Skew > 60min trava o dumpagent (`CLOCK_FATAL.txt` escrito em boot).

## Windows

### Verificar status atual

```powershell
w32tm /query /status
```

Campos importantes:
- `Last Successful Sync Time:` — deve ser < 24h
- `Source:` — servidor NTP atualmente em uso

### Resync manual

```powershell
Stop-Service w32time
w32tm /unregister; w32tm /register
Start-Service w32time
w32tm /resync /force
```

### Configurar time server

Para domínios internos:

```powershell
w32tm /config /manualpeerlist:"ntp.cnesdata.gov.br,0x8 br.pool.ntp.org,0x8" `
  /syncfromflags:manual /reliable:yes /update
Restart-Service w32time
```

Verificar skew após resync:

```powershell
w32tm /monitor /computers:br.pool.ntp.org
```

## Linux (systemd)

```bash
sudo timedatectl set-ntp true
sudo systemctl restart systemd-timesyncd
timedatectl status
```

Ou com chrony:

```bash
sudo apt install chrony
sudo systemctl enable --now chrony
chronyc sources
```

## Verificação pós-setup

```powershell
# Windows
w32tm /stripchart /computer:br.pool.ntp.org /samples:3 /dataonly
# Esperado: offset < 1s
```

```bash
# Linux
chronyc tracking
```

## Troubleshooting

| Sintoma | Causa | Ação |
|---|---|---|
| `CLOCK_FATAL.txt` presente | Skew > 60min | `w32tm /resync /force`, reiniciar dumpagent |
| Upload falha 403 silencioso | Skew 5-60min | Resync NTP + retry automático |
| NTP resync falha | Firewall bloqueia UDP 123 | Liberar porta 123 para NTP server |
