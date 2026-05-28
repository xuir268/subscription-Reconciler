package service

import (
	"context"
	"database/sql"
	"time"

	"subscription-reconciler/internal/models"
	"subscription-reconciler/internal/repository"
)

type EntitlementService struct {
	repo *repository.Repository
}

func NewEntitlementService(repo *repository.Repository) *EntitlementService {
	return &EntitlementService{repo: repo}
}

func (s *EntitlementService) Get(ctx context.Context, userID string) (*models.Entitlement, error) {
	entitlement, err := s.repo.GetEntitlement(ctx, userID)
	if err != nil {
		return nil, err
	}

	if entitlement == nil {
		return &models.Entitlement{
			UserID:        userID,
			Active:        false,
			IsPremium:     false,
			Source:        models.SourceNone,
			Reason:        "no active entitlement",
			LastChangedAt: time.Now().UTC(),
			UpdatedAt:     time.Now().UTC(),
		}, nil
	}

	return entitlement, nil
}

func (s *EntitlementService) IngestStoreWebhook(ctx context.Context, event models.StoreEvent) (bool, error) {
	tx, err := s.repo.DB.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()

	inserted, err := repository.InsertStoreEventTx(ctx, tx, event)
	if err != nil {
		return false, err
	}
	if !inserted {
		return false, tx.Commit()
	}

	lastEventTimeMs, err := repository.GetSourceLastEventTimeTx(ctx, tx, event.UserID, models.SourceStore)
	if err != nil {
		return false, err
	}
	if lastEventTimeMs > event.EventTimeMs {
		return true, tx.Commit()
	}

	active, expiresAt := storeTransition(event)
	if err := repository.UpsertSourceEntitlementTx(ctx, tx, models.SourceEntitlement{
		UserID:          event.UserID,
		Source:          models.SourceStore,
		Active:          active,
		Reason:          string(event.Type),
		ProductID:       event.ProductID,
		ExpiresAt:       expiresAt,
		LastEventTimeMs: event.EventTimeMs,
	}); err != nil {
		return false, err
	}
	if active && expiresAt != nil {
		if err := repository.ScheduleExpiringSoonTx(ctx, tx, event.UserID, models.SourceStore, *expiresAt); err != nil {
			return false, err
		}
	}

	return true, tx.Commit()
}

func (s *EntitlementService) RevokeMarketplace(ctx context.Context, userIDs []string) error {
	tx, err := s.repo.DB.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	nowMs := time.Now().UnixMilli()
	for _, userID := range userIDs {
		if userID == "" {
			continue
		}
		if err := repository.UpsertSourceEntitlementTx(ctx, tx, models.SourceEntitlement{
			UserID:          userID,
			Source:          models.SourceMarketplace,
			Active:          false,
			Reason:          "MARKETPLACE_BULK_REVOKE",
			LastEventTimeMs: nowMs,
		}); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (s *EntitlementService) ApplyCarrierStatus(ctx context.Context, userID string, status string) error {
	tx, err := s.repo.DB.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().UTC()
	nowMs := now.UnixMilli()
	switch status {
	case "active":
		if err := repository.UpsertSourceEntitlementTx(ctx, tx, models.SourceEntitlement{
			UserID:          userID,
			Source:          models.SourceCarrier,
			Active:          true,
			Reason:          "CARRIER_ACTIVE",
			LastEventTimeMs: nowMs,
		}); err != nil {
			return err
		}
		if err := repository.ScheduleCarrierPollTx(ctx, tx, userID, now.Add(5*time.Minute), ""); err != nil {
			return err
		}
	case "inactive":
		if err := repository.UpsertSourceEntitlementTx(ctx, tx, models.SourceEntitlement{
			UserID:          userID,
			Source:          models.SourceCarrier,
			Active:          false,
			Reason:          "CARRIER_INACTIVE",
			LastEventTimeMs: nowMs,
		}); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM carrier_poll_jobs WHERE user_id = $1`, userID); err != nil {
			return err
		}
	case "api_error":
		if err := repository.ScheduleCarrierPollTx(ctx, tx, userID, now.Add(5*time.Minute), "api_error"); err != nil {
			return err
		}
	default:
		return nil
	}

	return tx.Commit()
}

func storeTransition(event models.StoreEvent) (bool, *time.Time) {
	eventTime := time.UnixMilli(event.EventTimeMs).UTC()
	switch event.Type {
	case models.InitialPurchase, models.Renewal, models.UnCancellation:
		expiresAt := eventTime.Add(30 * 24 * time.Hour)
		return true, &expiresAt
	case models.Cancellation, models.BillingIssue:
		expiresAt := eventTime.Add(24 * time.Hour)
		return true, &expiresAt
	case models.Expiration:
		return false, nil
	default:
		return false, nil
	}
}
