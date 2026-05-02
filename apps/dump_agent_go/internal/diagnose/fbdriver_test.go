package diagnose

// Blank import enables the firebirdsql driver in this package's test binary so
// probeFirebird can reach the post-Open code paths (query_error / success) when
// FB_TEST_DSN is unset. With the driver registered, sql.Open succeeds; the
// Query exercises the network/timeout path against an unreachable host.
import _ "github.com/nakagami/firebirdsql"
