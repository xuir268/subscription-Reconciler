package models

import "time"

type Source string

const (
	SourceStore       Source = "STORE"
	SourceCarrier     Source = "CARRIER"
	SourceMarketplace Source = "MARKETPLACE"
	SourceNone        Source = "NONE"
)

type Entitlement struct {
	UserID string `json:"-"`

	Active          bool       `json:"active"`
	Source          Source     `json:"source"`
	ExpiresAt       *time.Time `json:"expiresAt,omitempty"`
	LastChangedAt   time.Time  `json:"lastChangedAt"`
	Reason          string     `json:"reason"`
	IsPremium       bool       `json:"-"`
	ActiveSources   []Source   `json:"-"`
	PrimaryReason   Source     `json:"-"`
	LastEventTimeMs *int64     `json:"-"`
	UpdatedAt       time.Time  `json:"-"`
}

type SourceEntitlement struct {
	UserID          string
	Source          Source
	Active          bool
	Reason          string
	ProductID       string
	ExpiresAt       *time.Time
	LastEventTimeMs int64
	UpdatedAt       time.Time
}
