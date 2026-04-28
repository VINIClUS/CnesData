package auth

import "time"

// SetClockSleep is a test-only helper to inject deterministic clock + sleep.
// Same package as Client; available only during `go test`.
func (c *Client) SetClockSleep(now func() time.Time, sleep func(time.Duration)) {
	c.clock = now
	c.sleep = sleep
}
