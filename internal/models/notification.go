package models

import "time"

type Notification struct {
	ID                int64      `json:"id"`
	UserID            string     `json:"userId"`
	Type              string     `json:"type"`
	EntitlementSource Source     `json:"entitlementSource"`
	ExpiresAt         time.Time  `json:"expiresAt"`
	ScheduledFor      time.Time  `json:"scheduledFor"`
	Message           string     `json:"message"`
	SentAt            *time.Time `json:"sentAt"`
	CreatedAt         time.Time  `json:"createdAt"`
}

const ExpiringSoonMessage = "your premium expires soon"
