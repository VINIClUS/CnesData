package auth

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func newMockServer(t *testing.T, handlers map[string]http.HandlerFunc) (*httptest.Server, *http.Client) {
	t.Helper()
	mux := http.NewServeMux()
	for path, h := range handlers {
		mux.HandleFunc(path, h)
	}
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, srv.Client()
}

func TestAuthorize_Success_ReturnsDeviceFlow(t *testing.T) {
	srv, client := newMockServer(t, map[string]http.HandlerFunc{
		"/oauth/device_authorization": func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				t.Fatalf("want POST got %s", r.Method)
			}
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode body: %v", err)
			}
			if body["client_id"] != "agent" || body["scope"] != "agent.provision" {
				t.Fatalf("unexpected body %+v", body)
			}
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"device_code":               "dc-43-chars-padding-padding-padding-padding",
				"user_code":                 "WDJB-MJHT",
				"verification_uri":          "https://central/activate",
				"verification_uri_complete": "https://central/activate?code=WDJB-MJHT",
				"expires_in":                600,
				"interval":                  5,
			})
		},
	})

	c := NewClient(srv.URL, client)
	flow, err := c.Authorize(context.Background(), "agent.provision")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if flow.UserCode != "WDJB-MJHT" {
		t.Errorf("want user_code WDJB-MJHT got %s", flow.UserCode)
	}
	if flow.VerificationURI != "https://central/activate" {
		t.Errorf("want verification_uri https://central/activate got %s", flow.VerificationURI)
	}
	if !strings.HasSuffix(flow.VerificationURIComplete, "WDJB-MJHT") {
		t.Errorf("want suffix WDJB-MJHT got %s", flow.VerificationURIComplete)
	}
}

func TestAuthorize_Non200_ReturnsError(t *testing.T) {
	srv, client := newMockServer(t, map[string]http.HandlerFunc{
		"/oauth/device_authorization": func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"invalid_client"}`))
		},
	})
	c := NewClient(srv.URL, client)
	_, err := c.Authorize(context.Background(), "agent.provision")
	if err == nil {
		t.Fatal("expected error got nil")
	}
	if !strings.Contains(err.Error(), "status=400") {
		t.Errorf("error should mention status=400, got: %v", err)
	}
}

func TestAuthorize_NetworkError_ReturnsWrappedError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	client := srv.Client()
	srv.Close()
	c := NewClient(srv.URL, client)
	_, err := c.Authorize(context.Background(), "agent.provision")
	if err == nil {
		t.Fatal("expected error got nil")
	}
	if !strings.Contains(err.Error(), "network") {
		t.Errorf("error should mention network, got: %v", err)
	}
}

func TestAuthorize_MalformedJSON_ReturnsError(t *testing.T) {
	srv, client := newMockServer(t, map[string]http.HandlerFunc{
		"/oauth/device_authorization": func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("not json"))
		},
	})
	c := NewClient(srv.URL, client)
	_, err := c.Authorize(context.Background(), "agent.provision")
	if err == nil {
		t.Fatal("expected error got nil")
	}
	if !strings.Contains(err.Error(), "malformed") && !strings.Contains(err.Error(), "decode") {
		t.Errorf("error should mention malformed/decode, got: %v", err)
	}
}

func pollHandler(t *testing.T, script []func(http.ResponseWriter, *http.Request)) http.HandlerFunc {
	t.Helper()
	calls := 0
	return func(w http.ResponseWriter, r *http.Request) {
		if calls >= len(script) {
			t.Fatalf("token endpoint called more times than scripted (%d)", calls+1)
		}
		script[calls](w, r)
		calls++
	}
}

func devAuthHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"device_code":               "dc-test-43-chars-padding-padding-padding-padding",
			"user_code":                 "WDJB-MJHT",
			"verification_uri":          "https://x/activate",
			"verification_uri_complete": "https://x/activate?code=WDJB-MJHT",
			"expires_in":                600,
			"interval":                  5,
		})
	}
}

func errorBody(code string, extra map[string]any) []byte {
	body := map[string]any{"error": code}
	for k, v := range extra {
		body[k] = v
	}
	out, _ := json.Marshal(body)
	return out
}

func nullSleep(time.Duration) {}

func TestPoll_AuthorizationPending_ThenAuthorized(t *testing.T) {
	srv, client := newMockServer(t, map[string]http.HandlerFunc{
		"/oauth/device_authorization": devAuthHandler(),
		"/oauth/token": pollHandler(t, []func(http.ResponseWriter, *http.Request){
			func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write(errorBody("authorization_pending", nil))
			},
			func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"access_token": "at-43-chars-padding-padding-padding-padding",
					"token_type":   "Bearer",
					"expires_in":   300,
				})
			},
		}),
	})
	c := NewClient(srv.URL, client)
	c.SetClockSleep(time.Now, nullSleep)
	flow, err := c.Authorize(context.Background(), "agent.provision")
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}
	tok, err := flow.Poll(context.Background())
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if tok.AccessToken == "" || tok.TokenType != "Bearer" {
		t.Errorf("unexpected token: %+v", tok)
	}
	if tok.ExpiresIn != 300*time.Second {
		t.Errorf("want 300s expires_in got %v", tok.ExpiresIn)
	}
}

func TestPoll_SlowDown_DoublesInterval(t *testing.T) {
	srv, client := newMockServer(t, map[string]http.HandlerFunc{
		"/oauth/device_authorization": devAuthHandler(),
		"/oauth/token": pollHandler(t, []func(http.ResponseWriter, *http.Request){
			func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write(errorBody("slow_down", nil))
			},
			func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"access_token": "at-x", "token_type": "Bearer", "expires_in": 300,
				})
			},
		}),
	})
	c := NewClient(srv.URL, client)
	var sleeps []time.Duration
	recorder := func(d time.Duration) { sleeps = append(sleeps, d) }
	c.SetClockSleep(time.Now, recorder)
	flow, err := c.Authorize(context.Background(), "agent.provision")
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}
	_, err = flow.Poll(context.Background())
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if len(sleeps) < 2 {
		t.Fatalf("want >=2 sleeps got %d", len(sleeps))
	}
	if sleeps[1] != 10*time.Second {
		t.Errorf("want 10s after slow_down, got %v", sleeps[1])
	}
}

func TestPoll_SlowDown_HonorsServerInterval(t *testing.T) {
	srv, client := newMockServer(t, map[string]http.HandlerFunc{
		"/oauth/device_authorization": devAuthHandler(),
		"/oauth/token": pollHandler(t, []func(http.ResponseWriter, *http.Request){
			func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write(errorBody("slow_down", map[string]any{"interval": 30}))
			},
			func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"access_token": "at-x", "token_type": "Bearer", "expires_in": 300,
				})
			},
		}),
	})
	c := NewClient(srv.URL, client)
	var sleeps []time.Duration
	c.SetClockSleep(time.Now, func(d time.Duration) { sleeps = append(sleeps, d) })
	flow, _ := c.Authorize(context.Background(), "agent.provision")
	_, err := flow.Poll(context.Background())
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if sleeps[1] != 30*time.Second {
		t.Errorf("want server-supplied 30s, got %v", sleeps[1])
	}
}

func TestPoll_SlowDown_CapsAt60Seconds(t *testing.T) {
	script := []func(http.ResponseWriter, *http.Request){}
	for i := 0; i < 5; i++ {
		script = append(script, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write(errorBody("slow_down", nil))
		})
	}
	script = append(script, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": "at-x", "token_type": "Bearer", "expires_in": 300,
		})
	})
	srv, client := newMockServer(t, map[string]http.HandlerFunc{
		"/oauth/device_authorization": devAuthHandler(),
		"/oauth/token":                pollHandler(t, script),
	})
	c := NewClient(srv.URL, client)
	var sleeps []time.Duration
	c.SetClockSleep(time.Now, func(d time.Duration) { sleeps = append(sleeps, d) })
	flow, _ := c.Authorize(context.Background(), "agent.provision")
	_, err := flow.Poll(context.Background())
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	want := []time.Duration{5, 10, 20, 40, 60, 60}
	for i, w := range want {
		if sleeps[i] != w*time.Second {
			t.Errorf("sleeps[%d] want %ds got %v", i, w, sleeps[i])
		}
	}
}

func TestPoll_ExpiredToken_ReturnsErrExpiredToken(t *testing.T) {
	srv, client := newMockServer(t, map[string]http.HandlerFunc{
		"/oauth/device_authorization": devAuthHandler(),
		"/oauth/token": pollHandler(t, []func(http.ResponseWriter, *http.Request){
			func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write(errorBody("expired_token", nil))
			},
		}),
	})
	c := NewClient(srv.URL, client)
	c.SetClockSleep(time.Now, nullSleep)
	flow, _ := c.Authorize(context.Background(), "agent.provision")
	_, err := flow.Poll(context.Background())
	if !errors.Is(err, ErrExpiredToken) {
		t.Errorf("want ErrExpiredToken got %v", err)
	}
}

func TestPoll_AccessDenied_ReturnsErrAccessDenied(t *testing.T) {
	srv, client := newMockServer(t, map[string]http.HandlerFunc{
		"/oauth/device_authorization": devAuthHandler(),
		"/oauth/token": pollHandler(t, []func(http.ResponseWriter, *http.Request){
			func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write(errorBody("access_denied", nil))
			},
		}),
	})
	c := NewClient(srv.URL, client)
	c.SetClockSleep(time.Now, nullSleep)
	flow, _ := c.Authorize(context.Background(), "agent.provision")
	_, err := flow.Poll(context.Background())
	if !errors.Is(err, ErrAccessDenied) {
		t.Errorf("want ErrAccessDenied got %v", err)
	}
}

func TestPoll_InvalidGrant_ReturnsErrInvalidGrant(t *testing.T) {
	srv, client := newMockServer(t, map[string]http.HandlerFunc{
		"/oauth/device_authorization": devAuthHandler(),
		"/oauth/token": pollHandler(t, []func(http.ResponseWriter, *http.Request){
			func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write(errorBody("invalid_grant", nil))
			},
		}),
	})
	c := NewClient(srv.URL, client)
	c.SetClockSleep(time.Now, nullSleep)
	flow, _ := c.Authorize(context.Background(), "agent.provision")
	_, err := flow.Poll(context.Background())
	if !errors.Is(err, ErrInvalidGrant) {
		t.Errorf("want ErrInvalidGrant got %v", err)
	}
}

func TestPoll_UnsupportedGrant_ReturnsErrUnsupportedGrant(t *testing.T) {
	srv, client := newMockServer(t, map[string]http.HandlerFunc{
		"/oauth/device_authorization": devAuthHandler(),
		"/oauth/token": pollHandler(t, []func(http.ResponseWriter, *http.Request){
			func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write(errorBody("unsupported_grant_type", nil))
			},
		}),
	})
	c := NewClient(srv.URL, client)
	c.SetClockSleep(time.Now, nullSleep)
	flow, _ := c.Authorize(context.Background(), "agent.provision")
	_, err := flow.Poll(context.Background())
	if !errors.Is(err, ErrUnsupportedGrant) {
		t.Errorf("want ErrUnsupportedGrant got %v", err)
	}
}

func TestPoll_InvalidClient_ReturnsErrInvalidClient(t *testing.T) {
	srv, client := newMockServer(t, map[string]http.HandlerFunc{
		"/oauth/device_authorization": devAuthHandler(),
		"/oauth/token": pollHandler(t, []func(http.ResponseWriter, *http.Request){
			func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write(errorBody("invalid_client", nil))
			},
		}),
	})
	c := NewClient(srv.URL, client)
	c.SetClockSleep(time.Now, nullSleep)
	flow, _ := c.Authorize(context.Background(), "agent.provision")
	_, err := flow.Poll(context.Background())
	if !errors.Is(err, ErrInvalidClient) {
		t.Errorf("want ErrInvalidClient got %v", err)
	}
}

func TestPoll_ContextCancellation_ReturnsCtxErr(t *testing.T) {
	srv, client := newMockServer(t, map[string]http.HandlerFunc{
		"/oauth/device_authorization": devAuthHandler(),
		"/oauth/token": func(w http.ResponseWriter, r *http.Request) {
			t.Fatal("token endpoint should NOT be called after context cancel")
		},
	})
	c := NewClient(srv.URL, client)
	cancelOnce := make(chan struct{}, 1)
	c.SetClockSleep(time.Now, func(time.Duration) {
		select {
		case cancelOnce <- struct{}{}:
		default:
		}
	})
	flow, _ := c.Authorize(context.Background(), "agent.provision")
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-cancelOnce
		cancel()
	}()
	_, err := flow.Poll(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("want context.Canceled got %v", err)
	}
}

func TestPoll_ExpiresAtElapsed_ReturnsErrExpiredToken(t *testing.T) {
	srv, client := newMockServer(t, map[string]http.HandlerFunc{
		"/oauth/device_authorization": devAuthHandler(),
		"/oauth/token": func(w http.ResponseWriter, r *http.Request) {
			t.Fatal("token endpoint should NOT be called when ExpiresAt elapsed")
		},
	})
	c := NewClient(srv.URL, client)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	c.SetClockSleep(func() time.Time { return now }, nullSleep)
	flow, _ := c.Authorize(context.Background(), "agent.provision")
	now = flow.ExpiresAt.Add(time.Second)
	c.SetClockSleep(func() time.Time { return now }, nullSleep)
	_, err := flow.Poll(context.Background())
	if !errors.Is(err, ErrExpiredToken) {
		t.Errorf("want ErrExpiredToken got %v", err)
	}
}

func TestPoll_UnknownErrorCode_ReturnsGenericError(t *testing.T) {
	srv, client := newMockServer(t, map[string]http.HandlerFunc{
		"/oauth/device_authorization": devAuthHandler(),
		"/oauth/token": pollHandler(t, []func(http.ResponseWriter, *http.Request){
			func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write(errorBody("totally_made_up", nil))
			},
		}),
	})
	c := NewClient(srv.URL, client)
	c.SetClockSleep(time.Now, nullSleep)
	flow, _ := c.Authorize(context.Background(), "agent.provision")
	_, err := flow.Poll(context.Background())
	if err == nil {
		t.Fatal("expected error got nil")
	}
	if errors.Is(err, ErrExpiredToken) || errors.Is(err, ErrAccessDenied) ||
		errors.Is(err, ErrInvalidGrant) || errors.Is(err, ErrUnsupportedGrant) ||
		errors.Is(err, ErrInvalidClient) {
		t.Errorf("unknown error should NOT match any sentinel, got %v", err)
	}
	if !strings.Contains(err.Error(), "totally_made_up") {
		t.Errorf("error should mention unknown code, got: %v", err)
	}
}


func TestNewClient_NilHTTPClient_UsesDefault(t *testing.T) {
	c := NewClient("https://x", nil)
	if c == nil {
		t.Fatal("NewClient returned nil")
	}
	if c.httpClient == nil {
		t.Error("expected default httpClient to be set")
	}
	if c.httpClient != http.DefaultClient {
		t.Error("expected http.DefaultClient")
	}
}

func TestPoll_TokenResponseMissingAccessToken_ReturnsError(t *testing.T) {
	srv, client := newMockServer(t, map[string]http.HandlerFunc{
		"/oauth/device_authorization": devAuthHandler(),
		"/oauth/token": pollHandler(t, []func(http.ResponseWriter, *http.Request){
			func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"token_type": "Bearer",
					"expires_in": 300,
				})
			},
		}),
	})
	c := NewClient(srv.URL, client)
	c.SetClockSleep(time.Now, nullSleep)
	flow, _ := c.Authorize(context.Background(), "agent.provision")
	_, err := flow.Poll(context.Background())
	if err == nil {
		t.Fatal("expected error got nil")
	}
	if !strings.Contains(err.Error(), "missing access_token") {
		t.Errorf("error should mention missing access_token, got: %v", err)
	}
}
