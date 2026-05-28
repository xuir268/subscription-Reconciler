package repository

import (
	"context"
	"database/sql"
	"time"

	"subscription-reconciler/internal/models"
)

func (r *Repository) GetAuditLog(ctx context.Context, userID string) ([]models.AuditEntry, error) {
	rows, err := r.DB.QueryContext(ctx, `
		SELECT id, user_id, source, event_id,
		       prev_active, prev_reason, prev_expires_at,
		       next_active, next_reason, next_expires_at,
		       created_at
		FROM entitlement_audit_log
		WHERE user_id = $1
		ORDER BY created_at DESC
	`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []models.AuditEntry
	for rows.Next() {
		var e models.AuditEntry
		var eventID sql.NullString
		var prevActive sql.NullBool
		var prevReason sql.NullString
		var prevExpiresAt sql.NullTime
		var nextExpiresAt sql.NullTime
		var createdAt time.Time

		if err := rows.Scan(
			&e.ID, &e.UserID, &e.Source, &eventID,
			&prevActive, &prevReason, &prevExpiresAt,
			&e.Next.Active, &e.Next.Reason, &nextExpiresAt,
			&createdAt,
		); err != nil {
			return nil, err
		}

		e.ChangedAt = createdAt
		if eventID.Valid {
			e.EventID = &eventID.String
		}
		if nextExpiresAt.Valid {
			t := nextExpiresAt.Time
			e.Next.ExpiresAt = &t
		}
		if prevActive.Valid {
			prev := models.AuditState{
				Active: prevActive.Bool,
				Reason: prevReason.String,
			}
			if prevExpiresAt.Valid {
				t := prevExpiresAt.Time
				prev.ExpiresAt = &t
			}
			e.Prev = &prev
		}

		entries = append(entries, e)
	}
	return entries, rows.Err()
}
