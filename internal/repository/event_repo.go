package repository

import (
	"context"
	"database/sql"

	"subscription-reconciler/internal/models"
)

func InsertStoreEventTx(ctx context.Context, tx *sql.Tx, e models.StoreEvent) (bool, error) {
	res, err := tx.ExecContext(ctx, `
		INSERT INTO store_events (
			event_id, user_id, type, event_time_ms, product_id
		)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (event_id) DO NOTHING
	`, e.EventID, e.UserID, e.Type, e.EventTimeMs, e.ProductID)
	if err != nil {
		return false, err
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return false, err
	}

	return rows == 1, nil
}
