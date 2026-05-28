package api

import (
	"encoding/json"
	"net/http"
	"strings"

	"subscription-reconciler/internal/metrics"
	"subscription-reconciler/internal/models"
	"subscription-reconciler/internal/repository"
	"subscription-reconciler/internal/service"
)

type Handler struct {
	entitlements *service.EntitlementService
	repo         *repository.Repository
	middleware   MiddlewareConfig
	metrics      *metrics.Registry
	seenEvents   *seenCache
	carrierMock  *carrierMock
}

func NewHandler(entitlements *service.EntitlementService, repo *repository.Repository, middleware MiddlewareConfig, metricsRegistry *metrics.Registry) *Handler {
	return &Handler{
		entitlements: entitlements,
		repo:         repo,
		middleware:   middleware,
		metrics:      metricsRegistry,
		seenEvents:   newSeenCache(10000),
		carrierMock:  newCarrierMock(),
	}
}

func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", h.Health)
	mux.HandleFunc("GET /metrics", h.Metrics)
	mux.HandleFunc("GET /admin/config", h.ListConfig)
	mux.HandleFunc("POST /admin/config", h.SetConfig)
	mux.HandleFunc("POST /webhooks/store", h.StoreWebhook)
	mux.HandleFunc("POST /webhooks/marketplace/revoke", h.MarketplaceRevoke)
	mux.HandleFunc("GET /users/{id}/entitlement", h.GetEntitlement)
	mux.HandleFunc("GET /mock/carrier/plan", h.MockCarrierPlan)
	return h.withMiddleware(mux)
}

func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) Metrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = w.Write([]byte(h.metrics.WritePrometheus()))
}

func (h *Handler) ListConfig(w http.ResponseWriter, r *http.Request) {
	entries, err := h.repo.ListConfig(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"config": entries})
}

func (h *Handler) SetConfig(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	req.Key = strings.TrimSpace(req.Key)
	req.Value = strings.TrimSpace(req.Value)
	if req.Key == "" || req.Value == "" {
		writeError(w, http.StatusBadRequest, "key and value are required")
		return
	}
	if err := h.repo.SetConfig(r.Context(), req.Key, req.Value); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) StoreWebhook(w http.ResponseWriter, r *http.Request) {
	var event models.StoreEvent
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	if event.EventID == "" || event.UserID == "" || event.Type == "" || event.EventTimeMs == 0 || event.ProductID == "" {
		writeError(w, http.StatusBadRequest, "missing required field")
		return
	}

	if h.seenEvents.seenOrAdd(event.EventID) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "duplicate"})
		return
	}

	applied, err := h.entitlements.IngestStoreWebhook(r.Context(), event)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "applied": applied})
}

func (h *Handler) MarketplaceRevoke(w http.ResponseWriter, r *http.Request) {
	var req models.MarketplaceRevokeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	if err := h.entitlements.RevokeMarketplace(r.Context(), req.UserIDs); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "revoked": len(req.UserIDs)})
}

func (h *Handler) GetEntitlement(w http.ResponseWriter, r *http.Request) {
	entitlement, err := h.entitlements.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, entitlement)
}

func (h *Handler) MockCarrierPlan(w http.ResponseWriter, r *http.Request) {
	if r.URL.Query().Get("userId") == "" {
		writeError(w, http.StatusBadRequest, "missing userId")
		return
	}

	writeJSON(w, http.StatusOK, models.CarrierPlanResponse{Status: h.carrierMock.nextStatus()})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": strings.TrimSpace(message)})
}
