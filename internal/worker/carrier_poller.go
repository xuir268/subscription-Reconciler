package worker

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"subscription-reconciler/internal/metrics"
	"subscription-reconciler/internal/models"
	"subscription-reconciler/internal/repository"
	"subscription-reconciler/internal/service"
)

type CarrierPoller struct {
	repo         *repository.Repository
	entitlements *service.EntitlementService
	httpClient   *http.Client
	baseURL      string
	poolSize     int
	batchSize    int
	metrics      *metrics.Registry
}

func NewCarrierPoller(repo *repository.Repository, entitlements *service.EntitlementService, baseURL string, poolSize int, metricsRegistry *metrics.Registry) *CarrierPoller {
	if poolSize < 1 {
		poolSize = 1
	}

	return &CarrierPoller{
		repo:         repo,
		entitlements: entitlements,
		httpClient:   &http.Client{Timeout: 5 * time.Second},
		baseURL:      baseURL,
		poolSize:     poolSize,
		batchSize:    100,
		metrics:      metricsRegistry,
	}
}

func (p *CarrierPoller) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	p.pollOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.pollOnce(ctx)
		}
	}
}

func (p *CarrierPoller) pollOnce(ctx context.Context) {
	start := time.Now()
	workerID := "worker-" + time.Now().UTC().Format("20060102150405.000000000")
	userIDs, err := p.repo.ClaimCarrierPollJobs(ctx, workerID, p.batchSize, 2*time.Minute)
	if err != nil {
		log.Printf("claim carrier jobs: %v", err)
		p.metrics.ObserveCarrierPoll("claim_error")
		return
	}
	if len(userIDs) == 0 {
		p.metrics.ObserveCarrierBatch(0, time.Since(start))
		return
	}

	p.processUsers(ctx, userIDs)
	p.metrics.ObserveCarrierBatch(len(userIDs), time.Since(start))
}

func (p *CarrierPoller) processUsers(ctx context.Context, userIDs []string) {
	jobs := make(chan string)
	var wg sync.WaitGroup

	for i := 0; i < p.poolSize; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for userID := range jobs {
				status := p.fetchCarrierStatus(ctx, userID)
				p.metrics.ObserveCarrierPoll(status)
				if err := p.entitlements.ApplyCarrierStatus(ctx, userID, status); err != nil {
					log.Printf("apply carrier status for %s: %v", userID, err)
					p.metrics.ObserveCarrierPoll("apply_error")
				}
			}
		}()
	}

	for _, userID := range userIDs {
		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return
		case jobs <- userID:
		}
	}

	close(jobs)
	wg.Wait()
}

func (p *CarrierPoller) fetchCarrierStatus(ctx context.Context, userID string) string {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.baseURL+"/mock/carrier/plan?userId="+userID, nil)
	if err != nil {
		return "api_error"
	}
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return "api_error"
	}
	defer resp.Body.Close()

	var body models.CarrierPlanResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return "api_error"
	}
	return body.Status
}
