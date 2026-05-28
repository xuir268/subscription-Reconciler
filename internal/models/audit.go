package models

import "time"

type AuditState struct {
	Active    bool       `json:"active"`
	Reason    string     `json:"reason"`
	ExpiresAt *time.Time `json:"expiresAt"`
}

type AuditEntry struct {
	ID        int64       `json:"id"`
	UserID    string      `json:"userId"`
	Source    Source      `json:"source"`
	EventID   *string     `json:"eventId"`
	Prev      *AuditState `json:"prev"`
	Next      AuditState  `json:"next"`
	ChangedAt time.Time   `json:"changedAt"`
}
