package repository

import (
	"context"
	"time"
)

func (r *Repository) ClaimCarrierPollJobs(ctx context.Context, workerID string, limit int, lease time.Duration) ([]string, error) {
	rows, err := r.DB.QueryContext(ctx, `
		WITH claimed AS (
			SELECT user_id
			FROM carrier_poll_jobs
			WHERE next_poll_at <= now()
			  AND (locked_until IS NULL OR locked_until < now())
			ORDER BY next_poll_at
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE carrier_poll_jobs jobs
		SET locked_until = now() + ($2 * interval '1 second'),
			worker_id = $3
		FROM claimed
		WHERE jobs.user_id = claimed.user_id
		RETURNING jobs.user_id
	`, limit, int64(lease.Seconds()), workerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var userIDs []string
	for rows.Next() {
		var userID string
		if err := rows.Scan(&userID); err != nil {
			return nil, err
		}
		userIDs = append(userIDs, userID)
	}
	return userIDs, rows.Err()
}

func (r *Repository) DeleteCarrierPollJob(ctx context.Context, userID string) error {
	_, err := r.DB.ExecContext(ctx, `DELETE FROM carrier_poll_jobs WHERE user_id = $1`, userID)
	return err
}

func (r *Repository) SendDueNotifications(ctx context.Context, limit int) (int64, error) {
	res, err := r.DB.ExecContext(ctx, `
		WITH due AS (
			SELECT id
			FROM notifications
			WHERE sent_at IS NULL AND scheduled_for <= now()
			ORDER BY scheduled_for
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE notifications notifications
		SET sent_at = now()
		FROM due
		WHERE notifications.id = due.id
	`, limit)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
