package platform

import (
	"context"
	"fmt"
	"time"

	"github.com/beevik/ntp"
)

// SkewLevel categoriza clock drift detectado.
type SkewLevel int

const (
	SkewOK    SkewLevel = iota // |skew| < 1min
	SkewWarn                   // 1min <= |skew| < 5min
	SkewError                  // 5min <= |skew| < 60min
	SkewFatal                  // |skew| >= 60min
)

func (s SkewLevel) String() string {
	switch s {
	case SkewOK:
		return "ok"
	case SkewWarn:
		return "warn"
	case SkewError:
		return "error"
	case SkewFatal:
		return "fatal"
	}
	return "unknown"
}

// ClassifySkew aplica thresholds a skew (pode ser negativo).
func ClassifySkew(skew time.Duration) SkewLevel {
	abs := skew
	if abs < 0 {
		abs = -abs
	}
	switch {
	case abs < time.Minute:
		return SkewOK
	case abs < 5*time.Minute:
		return SkewWarn
	case abs < 60*time.Minute:
		return SkewError
	default:
		return SkewFatal
	}
}

// CheckClockSkew consulta servidores NTP e retorna skew local vs NTP.
// Usa primeiro servidor que responder dentro do timeout.
func CheckClockSkew(ctx context.Context, servers []string, timeout time.Duration) (time.Duration, error) {
	if len(servers) == 0 {
		servers = []string{"pool.ntp.org"}
	}
	var lastErr error
	for _, srv := range servers {
		subCtx, cancel := context.WithTimeout(ctx, timeout)
		resp, err := ntpQuery(subCtx, srv)
		cancel()
		if err == nil {
			return resp.ClockOffset, nil
		}
		lastErr = err
	}
	return 0, fmt.Errorf("all_ntp_failed: %w", lastErr)
}

// indireção para permitir mock em testes futuros
var ntpQuery = func(ctx context.Context, srv string) (*ntp.Response, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	return ntp.Query(srv)
}
