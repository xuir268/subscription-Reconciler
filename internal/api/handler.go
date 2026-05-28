package api

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
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

// WarmSeenCache loads recently seen event IDs from the DB into the in-memory
// cache. Call once after startup so the fast-path cache is useful immediately,
// even after a restart.
func (h *Handler) WarmSeenCache(ctx context.Context) error {
	ids, err := h.repo.GetRecentEventIDs(ctx, 10000)
	if err != nil {
		return err
	}
	for _, id := range ids {
		h.seenEvents.add(id)
	}
	return nil
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
	mux.HandleFunc("GET /users/{id}/timeline", h.GetTimeline)
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

	applied, duplicate, err := h.entitlements.IngestStoreWebhook(r.Context(), event)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if duplicate {
		// DB caught it (cross-replica or post-eviction) — backfill cache so
		// the next retry for this event_id is handled in memory.
		h.seenEvents.add(event.EventID)
		writeJSON(w, http.StatusOK, map[string]any{"status": "duplicate"})
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

func (h *Handler) GetTimeline(w http.ResponseWriter, r *http.Request) {
	userID := r.PathValue("id")
	entries, err := h.repo.GetAuditLog(r.Context(), userID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if entries == nil {
		entries = []models.AuditEntry{}
	}

	if r.URL.Query().Get("format") == "csv" {
		w.Header().Set("Content-Type", "text/csv")
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"timeline_%s.csv\"", userID))
		cw := csv.NewWriter(w)
		_ = cw.Write([]string{"id", "source", "eventId", "prevActive", "prevReason", "prevExpiresAt", "nextActive", "nextReason", "nextExpiresAt", "changedAt"})
		for _, e := range entries {
			eventID := ""
			if e.EventID != nil {
				eventID = *e.EventID
			}
			prevActive, prevReason, prevExpiresAt := "", "", ""
			if e.Prev != nil {
				prevActive = fmt.Sprintf("%t", e.Prev.Active)
				prevReason = e.Prev.Reason
				if e.Prev.ExpiresAt != nil {
					prevExpiresAt = e.Prev.ExpiresAt.UTC().Format("2006-01-02T15:04:05Z")
				}
			}
			nextExpiresAt := ""
			if e.Next.ExpiresAt != nil {
				nextExpiresAt = e.Next.ExpiresAt.UTC().Format("2006-01-02T15:04:05Z")
			}
			_ = cw.Write([]string{
				fmt.Sprintf("%d", e.ID),
				string(e.Source),
				eventID,
				prevActive,
				prevReason,
				prevExpiresAt,
				fmt.Sprintf("%t", e.Next.Active),
				e.Next.Reason,
				nextExpiresAt,
				e.ChangedAt.UTC().Format("2006-01-02T15:04:05Z"),
			})
		}
		cw.Flush()
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"userId": userID, "timeline": entries})
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
