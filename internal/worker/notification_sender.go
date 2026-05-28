package worker

import (
	"context"
	"log"
	"time"

	"subscription-reconciler/internal/metrics"
	"subscription-reconciler/internal/repository"
)

type NotificationSender struct {
	repo    *repository.Repository
	metrics *metrics.Registry
}

func NewNotificationSender(repo *repository.Repository, metricsRegistry *metrics.Registry) *NotificationSender {
	return &NotificationSender{repo: repo, metrics: metricsRegistry}
}

func (s *NotificationSender) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	s.sendOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.sendOnce(ctx)
		}
	}
}

func (s *NotificationSender) sendOnce(ctx context.Context) {
	count, err := s.repo.SendDueNotifications(ctx, 100)
	if err != nil {
		log.Printf("send due notifications: %v", err)
		return
	}
	s.metrics.ObserveNotificationsSent(count)
}
