package models

type StoreEventType string

const (
	InitialPurchase StoreEventType = "INITIAL_PURCHASE"
	Renewal         StoreEventType = "RENEWAL"
	Cancellation    StoreEventType = "CANCELLATION"
	BillingIssue    StoreEventType = "BILLING_ISSUE"
	Expiration      StoreEventType = "EXPIRATION"
	UnCancellation  StoreEventType = "UN_CANCELLATION"
)

type StoreEvent struct {
	EventID     string         `json:"eventId"`
	UserID      string         `json:"userId"`
	Type        StoreEventType `json:"type"`
	EventTimeMs int64          `json:"eventTimeMs"`
	ProductID   string         `json:"productId"`
}
