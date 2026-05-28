package repository

import (
	"context"
	"database/sql"
	"time"

	"subscription-reconciler/internal/models"
)

func ScheduleExpiringSoonTx(ctx context.Context, tx *sql.Tx, userID string, source models.Source, expiresAt time.Time) error {
	if time.Until(expiresAt) <= 0 || time.Until(expiresAt) > 24*time.Hour {
		return nil
	}

	_, err := tx.ExecContext(ctx, `
		INSERT INTO notifications (
			user_id, type, entitlement_source, expires_at, scheduled_for, message
		)
		VALUES ($1, 'PREMIUM_EXPIRES_SOON', $2, $3, now(), $4)
		ON CONFLICT (user_id, entitlement_source, expires_at) DO NOTHING
	`, userID, source, expiresAt, models.ExpiringSoonMessage)
	return err
}
