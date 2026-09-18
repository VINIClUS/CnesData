package queue

import (
	"net/http"
	"strconv"
	"time"
)

// Classification of HTTP response outcome.
type Classification int

const (
	ClassSuccess Classification = iota
	ClassTerminalDrop
	ClassTransient
	ClassRateLimit
)

const defaultRetryAfter = 60 * time.Second

// Classify maps (resp, err) pair to outcome + sleep duration (for RateLimit).
// Network err → Transient. 2xx → Success. 4xx (not 429) → TerminalDrop.
// 429 → RateLimit (parse Retry-After header). 5xx → Transient.
func Classify(resp *http.Response, err error) (Classification, time.Duration) {
	if err != nil {
		return ClassTransient, 0
	}
	if resp == nil {
		return ClassTransient, 0
	}
	code := resp.StatusCode
	switch {
	case code >= 200 && code < 300:
		return ClassSuccess, 0
	case code == http.StatusTooManyRequests:
		return ClassRateLimit, parseRetryAfter(resp)
	case code >= 400 && code < 500:
		return ClassTerminalDrop, 0
	case code >= 500:
		return ClassTransient, 0
	}
	return ClassTransient, 0
}

func parseRetryAfter(resp *http.Response) time.Duration {
	h := resp.Header.Get("Retry-After")
	if h == "" {
		return defaultRetryAfter
	}
	if secs, err := strconv.Atoi(h); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(h); err == nil {
		d := time.Until(t)
		if d > 0 {
			return d
		}
	}
	return defaultRetryAfter
}
