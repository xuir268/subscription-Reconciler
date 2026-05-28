package api

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

type MiddlewareConfig struct {
	RateLimit          int
	RateLimitWindow    time.Duration
	RequestCacheTTL    time.Duration
	MaxCacheableBody   int64
	EnablePostgresGate bool
}

func DefaultMiddlewareConfig() MiddlewareConfig {
	return MiddlewareConfig{
		RateLimit:          120,
		RateLimitWindow:    time.Minute,
		RequestCacheTTL:    10 * time.Minute,
		MaxCacheableBody:   1 << 20,
		EnablePostgresGate: true,
	}
}

func (h *Handler) withMiddleware(next http.Handler) http.Handler {
	return h.instrument(h.rateLimit(h.postgresRequestCache(next)))
}

func (h *Handler) rateLimit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if skipGatewayMiddleware(r) || h.repo == nil {
			next.ServeHTTP(w, r)
			return
		}

		limit := h.repo.GetConfigInt(r.Context(), "api_rate_limit_per_minute", h.middleware.RateLimit)
		if limit <= 0 {
			next.ServeHTTP(w, r)
			return
		}

		rateKey := clientIP(r) + ":" + r.Method + ":" + r.URL.Path
		allowed, err := h.repo.CheckRateLimit(r.Context(), rateKey, limit, h.middleware.RateLimitWindow)
		if err != nil {
			h.metrics.ObserveRateLimit("error")
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if !allowed {
			h.metrics.ObserveRateLimit("blocked")
			writeError(w, http.StatusTooManyRequests, "rate limit exceeded")
			return
		}

		h.metrics.ObserveRateLimit("allowed")
		next.ServeHTTP(w, r)
	})
}

func (h *Handler) postgresRequestCache(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gatewayEnabled := h.middleware.EnablePostgresGate
		if h.repo != nil {
			gatewayEnabled = h.repo.GetConfigBool(r.Context(), "api_gateway_enabled", gatewayEnabled)
		}
		if skipGatewayMiddleware(r) || h.repo == nil || !gatewayEnabled || !isCacheableMethod(r.Method) {
			next.ServeHTTP(w, r)
			return
		}

		body, err := io.ReadAll(io.LimitReader(r.Body, h.middleware.MaxCacheableBody+1))
		if err != nil {
			h.metrics.ObserveRequestCache("read_error")
			writeError(w, http.StatusBadRequest, "cannot read request body")
			return
		}
		_ = r.Body.Close()
		if int64(len(body)) > h.middleware.MaxCacheableBody {
			h.metrics.ObserveRequestCache("body_too_large")
			writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))

		bodyHash := hashBytes(body)
		requestKey := requestCacheKey(r, bodyHash)
		ttl := time.Duration(h.repo.GetConfigInt(r.Context(), "api_request_cache_ttl_seconds", int(h.middleware.RequestCacheTTL.Seconds()))) * time.Second
		claim, err := h.repo.ClaimAPIRequest(r.Context(), requestKey, r.Method, r.URL.Path, bodyHash, ttl)
		if err != nil {
			h.metrics.ObserveRequestCache("error")
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if !claim.Claimed {
			if claim.BodyMismatch {
				h.metrics.ObserveRequestCache("body_mismatch")
				writeError(w, http.StatusConflict, "idempotency key reused with different request body")
				return
			}
			if claim.InProgress {
				h.metrics.ObserveRequestCache("in_progress")
				writeJSON(w, http.StatusAccepted, map[string]string{"status": "duplicate_in_progress"})
				return
			}
			h.metrics.ObserveRequestCache("replay")
			if claim.ContentType != "" {
				w.Header().Set("Content-Type", claim.ContentType)
			}
			w.WriteHeader(claim.StatusCode)
			_, _ = w.Write(claim.ResponseBody)
			return
		}

		recorder := newResponseRecorder(w)
		h.metrics.ObserveRequestCache("claimed")
		next.ServeHTTP(recorder, r)

		if recorder.statusCode >= 500 {
			h.metrics.ObserveRequestCache("forgotten_after_error")
			_ = h.repo.ForgetAPIRequest(r.Context(), requestKey)
			return
		}
		_ = h.repo.CompleteAPIRequest(r.Context(), requestKey, recorder.statusCode, recorder.Header().Get("Content-Type"), recorder.body.Bytes())
	})
}

func (h *Handler) instrument(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		recorder := newResponseRecorder(w)
		next.ServeHTTP(recorder, r)
		h.metrics.ObserveRequest(r.Method, routeLabel(r), recorder.statusCode, time.Since(start))
	})
}

func skipGatewayMiddleware(r *http.Request) bool {
	return r.URL.Path == "/metrics" || r.URL.Path == "/healthz"
}

func routeLabel(r *http.Request) string {
	if strings.HasPrefix(r.URL.Path, "/users/") && strings.HasSuffix(r.URL.Path, "/entitlement") {
		return "/users/{id}/entitlement"
	}
	return r.URL.Path
}

func isCacheableMethod(method string) bool {
	return method == http.MethodPost || method == http.MethodPut || method == http.MethodPatch || method == http.MethodDelete
}

func requestCacheKey(r *http.Request, bodyHash string) string {
	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if key != "" {
		return "idempotency:" + r.Method + ":" + r.URL.Path + ":" + key
	}
	return "body:" + r.Method + ":" + r.URL.Path + ":" + bodyHash
}

func hashBytes(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func clientIP(r *http.Request) string {
	forwarded := r.Header.Get("X-Forwarded-For")
	if forwarded != "" {
		parts := strings.Split(forwarded, ",")
		return strings.TrimSpace(parts[0])
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

type responseRecorder struct {
	http.ResponseWriter
	statusCode int
	body       bytes.Buffer
	wrote      bool
}

func newResponseRecorder(w http.ResponseWriter) *responseRecorder {
	return &responseRecorder{
		ResponseWriter: w,
		statusCode:     http.StatusOK,
	}
}

func (r *responseRecorder) WriteHeader(statusCode int) {
	if r.wrote {
		return
	}
	r.wrote = true
	r.statusCode = statusCode
	r.ResponseWriter.WriteHeader(statusCode)
}

func (r *responseRecorder) Write(body []byte) (int, error) {
	if !r.wrote {
		r.WriteHeader(r.statusCode)
	}
	r.body.Write(body)
	return r.ResponseWriter.Write(body)
}
