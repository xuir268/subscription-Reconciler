package repository

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"subscription-reconciler/internal/models"
)

func (r *Repository) GetEntitlement(ctx context.Context, userID string) (*models.Entitlement, error) {
	rows, err := r.DB.QueryContext(ctx, `
		SELECT source, reason, expires_at, last_event_time_ms, updated_at
		FROM source_entitlements
		WHERE user_id = $1 AND active = true
		ORDER BY CASE source
			WHEN 'STORE' THEN 1
			WHEN 'CARRIER' THEN 2
			WHEN 'MARKETPLACE' THEN 3
			ELSE 4
		END
	`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	entitlement := &models.Entitlement{
		UserID:        userID,
		Source:        models.SourceNone,
		Reason:        "no active entitlement",
		LastChangedAt: time.Now().UTC(),
		UpdatedAt:     time.Now().UTC(),
	}

	for rows.Next() {
		var sourceValue string
		var reason string
		var expiresAt sql.NullTime
		var lastEventTimeMs int64
		var updatedAt time.Time

		if err := rows.Scan(&sourceValue, &reason, &expiresAt, &lastEventTimeMs, &updatedAt); err != nil {
			return nil, err
		}

		source := models.Source(sourceValue)
		entitlement.ActiveSources = append(entitlement.ActiveSources, source)
		if entitlement.PrimaryReason == "" {
			entitlement.PrimaryReason = source
			entitlement.Source = source
			entitlement.Reason = reason
			entitlement.LastChangedAt = updatedAt
			if expiresAt.Valid {
				value := expiresAt.Time
				entitlement.ExpiresAt = &value
			}
		}
		if entitlement.LastEventTimeMs == nil || lastEventTimeMs > *entitlement.LastEventTimeMs {
			value := lastEventTimeMs
			entitlement.LastEventTimeMs = &value
		}
		if updatedAt.After(entitlement.UpdatedAt) {
			entitlement.UpdatedAt = updatedAt
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	entitlement.IsPremium = len(entitlement.ActiveSources) > 0
	entitlement.Active = entitlement.IsPremium
	if !entitlement.IsPremium {
		entitlement.Source = models.SourceNone
		entitlement.Reason = "no active entitlement"
		entitlement.LastChangedAt = entitlement.UpdatedAt
	}

	return entitlement, nil
}

func GetSourceLastEventTimeTx(ctx context.Context, tx *sql.Tx, userID string, source models.Source) (int64, error) {
	var lastEventTimeMs int64
	err := tx.QueryRowContext(ctx, `
		SELECT last_event_time_ms
		FROM source_entitlements
		WHERE user_id = $1 AND source = $2
	`, userID, source).Scan(&lastEventTimeMs)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return lastEventTimeMs, nil
}

func UpsertSourceEntitlementTx(ctx context.Context, tx *sql.Tx, e models.SourceEntitlement) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO source_entitlements (
			user_id, source, active, reason, product_id, expires_at, last_event_time_ms, updated_at
		)
		VALUES ($1, $2, $3, $4, NULLIF($5, ''), $6, $7, now())
		ON CONFLICT (user_id, source)
		DO UPDATE SET
			active = EXCLUDED.active,
			reason = EXCLUDED.reason,
			product_id = EXCLUDED.product_id,
			expires_at = EXCLUDED.expires_at,
			last_event_time_ms = EXCLUDED.last_event_time_ms,
			updated_at = now()
	`, e.UserID, e.Source, e.Active, e.Reason, e.ProductID, e.ExpiresAt, e.LastEventTimeMs)
	return err
}

func ScheduleCarrierPollTx(ctx context.Context, tx *sql.Tx, userID string, nextPollAt time.Time, lastError string) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO carrier_poll_jobs (
			user_id, next_poll_at, locked_until, worker_id, last_error
		)
		VALUES ($1, $2, NULL, NULL, NULLIF($3, ''))
		ON CONFLICT (user_id)
		DO UPDATE SET
			next_poll_at = EXCLUDED.next_poll_at,
			locked_until = NULL,
			worker_id = NULL,
			last_error = EXCLUDED.last_error
	`, userID, nextPollAt, lastError)
	return err
}
