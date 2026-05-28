package repository

import (
	"context"
	"database/sql"

	"subscription-reconciler/internal/models"
)

// GetRecentEventIDs returns the most recently received event IDs, newest first.
// Used to pre-warm the in-memory seen cache on startup.
func (r *Repository) GetRecentEventIDs(ctx context.Context, limit int) ([]string, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT event_id FROM store_events ORDER BY received_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	ids := make([]string, 0, limit)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

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
