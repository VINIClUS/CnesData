package obs

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
)

// FailureKind classifica erros para decisão de retry / dead-letter.
type FailureKind int

const (
	FailureTransient FailureKind = iota // retryable=true
	FailurePermanent                    // retryable=false, dead_letter
	FailureRateLimit                    // retryable com backoff longo (Retry-After)
)

func (k FailureKind) String() string {
	switch k {
	case FailureTransient:
		return "transient"
	case FailurePermanent:
		return "permanent"
	case FailureRateLimit:
		return "rate_limit"
	}
	return "unknown"
}

// HTTPError representa resposta HTTP não-2xx do central_api ou MinIO.
type HTTPError struct {
	StatusCode int
	Body       string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("http_status=%d body=%q", e.StatusCode, e.Body)
}

// Classify mapeia err para FailureKind. Default = Transient (conservador).
func Classify(err error) FailureKind {
	if errors.Is(err, context.Canceled) {
		return FailureTransient
	}

	var httpErr *HTTPError
	if errors.As(err, &httpErr) {
		return classifyHTTP(httpErr)
	}

	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return FailureTransient
	}

	return FailureTransient
}

func classifyHTTP(e *HTTPError) FailureKind {
	if e.StatusCode == 429 {
		return FailureRateLimit
	}
	if e.StatusCode == 403 {
		if strings.Contains(e.Body, "RequestTimeTooSkewed") ||
			strings.Contains(e.Body, "RequestExpired") {
			return FailureTransient
		}
		return FailurePermanent
	}
	if e.StatusCode >= 500 {
		return FailureTransient
	}
	if e.StatusCode >= 400 {
		return FailurePermanent
	}
	return FailureTransient
}
