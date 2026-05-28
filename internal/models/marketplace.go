package models

type MarketplaceRevokeRequest struct {
	UserIDs []string `json:"userIds"`
}

type CarrierPlanResponse struct {
	Status string `json:"status"`
}
