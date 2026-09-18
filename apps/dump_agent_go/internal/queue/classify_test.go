package queue

import (
	"errors"
	"net/http"
	"testing"
	"time"
)

func TestClassify_Success(t *testing.T) {
	for _, code := range []int{200, 201, 204, 299} {
		resp := &http.Response{StatusCode: code}
		cls, sleep := Classify(resp, nil)
		if cls != ClassSuccess || sleep != 0 {
			t.Errorf("code %d: got cls=%v sleep=%v", code, cls, sleep)
		}
	}
}

func TestClassify_TerminalDrop(t *testing.T) {
	for _, code := range []int{400, 401, 403, 404, 409} {
		resp := &http.Response{StatusCode: code}
		cls, _ := Classify(resp, nil)
		if cls != ClassTerminalDrop {
			t.Errorf("code %d: got cls=%v want TerminalDrop", code, cls)
		}
	}
}

func TestClassify_Transient5xx(t *testing.T) {
	for _, code := range []int{500, 502, 503, 504} {
		resp := &http.Response{StatusCode: code}
		cls, _ := Classify(resp, nil)
		if cls != ClassTransient {
			t.Errorf("code %d: got cls=%v want Transient", code, cls)
		}
	}
}

func TestClassify_TransientNetworkErr(t *testing.T) {
	cls, _ := Classify(nil, errors.New("connection refused"))
	if cls != ClassTransient {
		t.Fatalf("network err: got cls=%v want Transient", cls)
	}
}

func TestClassify_RateLimitNumeric(t *testing.T) {
	resp := &http.Response{
		StatusCode: 429,
		Header:     http.Header{"Retry-After": []string{"30"}},
	}
	cls, sleep := Classify(resp, nil)
	if cls != ClassRateLimit {
		t.Fatalf("got cls=%v want RateLimit", cls)
	}
	if sleep != 30*time.Second {
		t.Fatalf("got sleep=%v want 30s", sleep)
	}
}

func TestClassify_RateLimitMissingHeader(t *testing.T) {
	resp := &http.Response{StatusCode: 429, Header: http.Header{}}
	cls, sleep := Classify(resp, nil)
	if cls != ClassRateLimit || sleep != defaultRetryAfter {
		t.Fatalf("got cls=%v sleep=%v want RateLimit+%v", cls, sleep, defaultRetryAfter)
	}
}

func TestClassify_RateLimitHTTPDate(t *testing.T) {
	future := time.Now().Add(45 * time.Second).UTC().Format(http.TimeFormat)
	resp := &http.Response{
		StatusCode: 429,
		Header:     http.Header{"Retry-After": []string{future}},
	}
	cls, sleep := Classify(resp, nil)
	if cls != ClassRateLimit {
		t.Fatalf("got cls=%v want RateLimit", cls)
	}
	if sleep < 30*time.Second || sleep > 60*time.Second {
		t.Fatalf("got sleep=%v want ~45s", sleep)
	}
}
