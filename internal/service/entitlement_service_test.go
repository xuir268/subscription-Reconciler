package service

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"subscription-reconciler/internal/db"
	"subscription-reconciler/internal/models"
	"subscription-reconciler/internal/repository"
)

func TestDuplicateStoreWebhookIsIdempotent(t *testing.T) {
	svc, repo := testService(t)
	ctx := context.Background()
	event := models.StoreEvent{
		EventID:     "evt_duplicate",
		UserID:      "u_duplicate",
		Type:        models.InitialPurchase,
		EventTimeMs: time.Now().UnixMilli(),
		ProductID:   "premium_monthly",
	}

	applied, err := svc.IngestStoreWebhook(ctx, event)
	if err != nil {
		t.Fatal(err)
	}
	if !applied {
		t.Fatal("first event should be applied")
	}

	applied, err = svc.IngestStoreWebhook(ctx, event)
	if err != nil {
		t.Fatal(err)
	}
	if applied {
		t.Fatal("duplicate event should not be applied")
	}

	var count int
	if err := repo.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM store_events WHERE event_id = $1`, event.EventID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("expected one stored event, got %d", count)
	}
}

func TestOutOfOrderStoreWebhookDoesNotOverrideNewerEvent(t *testing.T) {
	svc, _ := testService(t)
	ctx := context.Background()

	if _, err := svc.IngestStoreWebhook(ctx, models.StoreEvent{
		EventID:     "evt_expiration",
		UserID:      "u_order",
		Type:        models.Expiration,
		EventTimeMs: 2000,
		ProductID:   "premium_monthly",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.IngestStoreWebhook(ctx, models.StoreEvent{
		EventID:     "evt_old_purchase",
		UserID:      "u_order",
		Type:        models.InitialPurchase,
		EventTimeMs: 1000,
		ProductID:   "premium_monthly",
	}); err != nil {
		t.Fatal(err)
	}

	entitlement, err := svc.Get(ctx, "u_order")
	if err != nil {
		t.Fatal(err)
	}
	if entitlement.IsPremium {
		t.Fatalf("older late purchase should not reactivate entitlement: %+v", entitlement)
	}
}

func TestLateArrivingNewerStoreWebhookApplies(t *testing.T) {
	svc, _ := testService(t)
	ctx := context.Background()

	if _, err := svc.IngestStoreWebhook(ctx, models.StoreEvent{
		EventID:     "evt_purchase_first",
		UserID:      "u_late",
		Type:        models.InitialPurchase,
		EventTimeMs: 1000,
		ProductID:   "premium_monthly",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.IngestStoreWebhook(ctx, models.StoreEvent{
		EventID:     "evt_expiration_late",
		UserID:      "u_late",
		Type:        models.Expiration,
		EventTimeMs: 2000,
		ProductID:   "premium_monthly",
	}); err != nil {
		t.Fatal(err)
	}

	entitlement, err := svc.Get(ctx, "u_late")
	if err != nil {
		t.Fatal(err)
	}
	if entitlement.IsPremium {
		t.Fatalf("newer late expiration should revoke entitlement: %+v", entitlement)
	}
}

func TestMarketplaceRevokeOnlyRevokesMarketplaceSource(t *testing.T) {
	svc, repo := testService(t)
	ctx := context.Background()

	tx, err := repo.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.UpsertSourceEntitlementTx(ctx, tx, models.SourceEntitlement{
		UserID:          "u_marketplace",
		Source:          models.SourceStore,
		Active:          true,
		Reason:          "TEST",
		ProductID:       "premium_monthly",
		LastEventTimeMs: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := repository.UpsertSourceEntitlementTx(ctx, tx, models.SourceEntitlement{
		UserID:          "u_marketplace",
		Source:          models.SourceMarketplace,
		Active:          true,
		Reason:          "TEST",
		LastEventTimeMs: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	if err := svc.RevokeMarketplace(ctx, []string{"u_marketplace"}); err != nil {
		t.Fatal(err)
	}

	entitlement, err := svc.Get(ctx, "u_marketplace")
	if err != nil {
		t.Fatal(err)
	}
	if !entitlement.IsPremium || len(entitlement.ActiveSources) != 1 || entitlement.ActiveSources[0] != models.SourceStore {
		t.Fatalf("store entitlement should remain active: %+v", entitlement)
	}
}

func TestCarrierInactiveRevokesOnlyCarrierSource(t *testing.T) {
	svc, repo := testService(t)
	ctx := context.Background()

	tx, err := repo.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.UpsertSourceEntitlementTx(ctx, tx, models.SourceEntitlement{
		UserID:          "u_carrier_inactive",
		Source:          models.SourceStore,
		Active:          true,
		Reason:          "TEST",
		ProductID:       "premium_monthly",
		LastEventTimeMs: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := repository.UpsertSourceEntitlementTx(ctx, tx, models.SourceEntitlement{
		UserID:          "u_carrier_inactive",
		Source:          models.SourceCarrier,
		Active:          true,
		Reason:          "TEST",
		LastEventTimeMs: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := repository.ScheduleCarrierPollTx(ctx, tx, "u_carrier_inactive", time.Now().Add(-time.Minute), ""); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	if err := svc.ApplyCarrierStatus(ctx, "u_carrier_inactive", "inactive"); err != nil {
		t.Fatal(err)
	}

	entitlement, err := svc.Get(ctx, "u_carrier_inactive")
	if err != nil {
		t.Fatal(err)
	}
	if !entitlement.IsPremium || len(entitlement.ActiveSources) != 1 || entitlement.ActiveSources[0] != models.SourceStore {
		t.Fatalf("inactive carrier status should only revoke carrier source: %+v", entitlement)
	}

	var jobCount int
	if err := repo.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM carrier_poll_jobs WHERE user_id = $1`, "u_carrier_inactive").Scan(&jobCount); err != nil {
		t.Fatal(err)
	}
	if jobCount != 0 {
		t.Fatalf("inactive carrier status should remove carrier poll job, got %d jobs", jobCount)
	}
}

func TestCarrierAPIErrorDoesNotRevoke(t *testing.T) {
	svc, repo := testService(t)
	ctx := context.Background()

	tx, err := repo.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.UpsertSourceEntitlementTx(ctx, tx, models.SourceEntitlement{
		UserID:          "u_carrier_error",
		Source:          models.SourceCarrier,
		Active:          true,
		Reason:          "TEST",
		LastEventTimeMs: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	if err := svc.ApplyCarrierStatus(ctx, "u_carrier_error", "api_error"); err != nil {
		t.Fatal(err)
	}

	entitlement, err := svc.Get(ctx, "u_carrier_error")
	if err != nil {
		t.Fatal(err)
	}
	if !entitlement.IsPremium || entitlement.PrimaryReason != models.SourceCarrier {
		t.Fatalf("api_error should leave carrier entitlement active: %+v", entitlement)
	}
}

func TestEntitlementReadReturnsCanonicalPublicShape(t *testing.T) {
	svc, repo := testService(t)
	ctx := context.Background()

	expiresAt := time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC)
	tx, err := repo.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, entitlement := range []models.SourceEntitlement{
		{
			UserID:          "u_read",
			Source:          models.SourceMarketplace,
			Active:          true,
			Reason:          "MARKETPLACE_GRANT",
			LastEventTimeMs: 1,
		},
		{
			UserID:          "u_read",
			Source:          models.SourceCarrier,
			Active:          true,
			Reason:          "CARRIER_ACTIVE",
			LastEventTimeMs: 2,
		},
		{
			UserID:          "u_read",
			Source:          models.SourceStore,
			Active:          true,
			Reason:          "RENEWAL",
			ProductID:       "premium_monthly",
			ExpiresAt:       &expiresAt,
			LastEventTimeMs: 3,
		},
	} {
		if err := repository.UpsertSourceEntitlementTx(ctx, tx, entitlement); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	entitlement, err := svc.Get(ctx, "u_read")
	if err != nil {
		t.Fatal(err)
	}
	if !entitlement.Active || entitlement.Source != models.SourceStore || entitlement.Reason != "RENEWAL" {
		t.Fatalf("expected canonical store entitlement, got %+v", entitlement)
	}

	body, err := json.Marshal(entitlement)
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	for _, internalField := range []string{"userId", "isPremium", "activeSources", "primaryReason", "lastEventTimeMs", "updatedAt"} {
		if _, ok := payload[internalField]; ok {
			t.Fatalf("internal field %q leaked into response: %s", internalField, string(body))
		}
	}
	for _, publicField := range []string{"active", "source", "expiresAt", "lastChangedAt", "reason"} {
		if _, ok := payload[publicField]; !ok {
			t.Fatalf("public field %q missing from response: %s", publicField, string(body))
		}
	}

	missing, err := svc.Get(ctx, "u_missing")
	if err != nil {
		t.Fatal(err)
	}
	if missing.Active || missing.Source != models.SourceNone {
		t.Fatalf("expected inactive NONE entitlement, got %+v", missing)
	}
}

func TestExpiringSoonNotificationIsScheduledOnce(t *testing.T) {
	svc, repo := testService(t)
	ctx := context.Background()
	eventTimeMs := time.Now().Add(-23 * time.Hour).UnixMilli()

	for _, eventID := range []string{"evt_cancel_1", "evt_cancel_2"} {
		if _, err := svc.IngestStoreWebhook(ctx, models.StoreEvent{
			EventID:     eventID,
			UserID:      "u_notification",
			Type:        models.Cancellation,
			EventTimeMs: eventTimeMs,
			ProductID:   "premium_monthly",
		}); err != nil {
			t.Fatal(err)
		}
	}

	var count int
	if err := repo.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM notifications WHERE user_id = $1`, "u_notification").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("expected one notification, got %d", count)
	}
}

func TestNotificationNotScheduledWhenExpiryBeyond24h(t *testing.T) {
	svc, repo := testService(t)
	ctx := context.Background()

	// event time is now → expiresAt = now+30d, well outside 24h window
	if _, err := svc.IngestStoreWebhook(ctx, models.StoreEvent{
		EventID:     "evt_notif_far",
		UserID:      "u_notif_far",
		Type:        models.InitialPurchase,
		EventTimeMs: time.Now().UnixMilli(),
		ProductID:   "premium_monthly",
	}); err != nil {
		t.Fatal(err)
	}

	var count int
	if err := repo.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM notifications WHERE user_id = $1`, "u_notif_far").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("expected no notification for expiry >24h away, got %d", count)
	}
}

func TestNotificationNotScheduledWhenAlreadyExpired(t *testing.T) {
	svc, repo := testService(t)
	ctx := context.Background()

	// event time is 2 days ago → expiresAt = yesterday, already past
	eventTimeMs := time.Now().Add(-48 * time.Hour).UnixMilli()
	if _, err := svc.IngestStoreWebhook(ctx, models.StoreEvent{
		EventID:     "evt_notif_past",
		UserID:      "u_notif_past",
		Type:        models.Cancellation,
		EventTimeMs: eventTimeMs,
		ProductID:   "premium_monthly",
	}); err != nil {
		t.Fatal(err)
	}

	var count int
	if err := repo.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM notifications WHERE user_id = $1`, "u_notif_past").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("expected no notification for already-expired entitlement, got %d", count)
	}
}

func TestNotificationRowShape(t *testing.T) {
	svc, repo := testService(t)
	ctx := context.Background()

	eventTimeMs := time.Now().Add(-23 * time.Hour).UnixMilli()
	if _, err := svc.IngestStoreWebhook(ctx, models.StoreEvent{
		EventID:     "evt_notif_shape",
		UserID:      "u_notif_shape",
		Type:        models.Cancellation,
		EventTimeMs: eventTimeMs,
		ProductID:   "premium_monthly",
	}); err != nil {
		t.Fatal(err)
	}

	var notifType, userID, message string
	var sentAt *time.Time
	var scheduledFor time.Time
	if err := repo.DB.QueryRowContext(ctx, `
		SELECT user_id, type, scheduled_for, message, sent_at
		FROM notifications WHERE user_id = $1
	`, "u_notif_shape").Scan(&userID, &notifType, &scheduledFor, &message, &sentAt); err != nil {
		t.Fatal(err)
	}

	if userID != "u_notif_shape" {
		t.Fatalf("unexpected userId %q", userID)
	}
	if notifType != "PREMIUM_EXPIRES_SOON" {
		t.Fatalf("expected type PREMIUM_EXPIRES_SOON, got %q", notifType)
	}
	if message != "your premium expires soon" {
		t.Fatalf("unexpected message %q", message)
	}
	if sentAt != nil {
		t.Fatalf("sentAt should be null before sending, got %v", sentAt)
	}
	if scheduledFor.IsZero() {
		t.Fatal("scheduledFor should be set")
	}
}

func TestNotificationSenderMarksSentAt(t *testing.T) {
	_, repo := testService(t)
	ctx := context.Background()

	// insert a notification that is already due (scheduled_for = now)
	if _, err := repo.DB.ExecContext(ctx, `
		INSERT INTO notifications (user_id, type, entitlement_source, expires_at, scheduled_for, message)
		VALUES ('u_sender', 'PREMIUM_EXPIRES_SOON', 'STORE', now() + interval '1 hour', now() - interval '1 second', 'your premium expires soon')
	`); err != nil {
		t.Fatal(err)
	}

	sent, err := repo.SendDueNotifications(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if sent != 1 {
		t.Fatalf("expected 1 notification sent, got %d", sent)
	}

	var sentAt *time.Time
	if err := repo.DB.QueryRowContext(ctx, `SELECT sent_at FROM notifications WHERE user_id = $1`, "u_sender").Scan(&sentAt); err != nil {
		t.Fatal(err)
	}
	if sentAt == nil {
		t.Fatal("sent_at should be stamped after sending")
	}
}

func TestNotificationSenderDoesNotResendAlreadySent(t *testing.T) {
	_, repo := testService(t)
	ctx := context.Background()

	if _, err := repo.DB.ExecContext(ctx, `
		INSERT INTO notifications (user_id, type, entitlement_source, expires_at, scheduled_for, message, sent_at)
		VALUES ('u_resend', 'PREMIUM_EXPIRES_SOON', 'STORE', now() + interval '1 hour', now() - interval '1 second', 'your premium expires soon', now())
	`); err != nil {
		t.Fatal(err)
	}

	sent, err := repo.SendDueNotifications(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if sent != 0 {
		t.Fatalf("already-sent notification should not be resent, got %d", sent)
	}
}

func TestNotificationNotScheduledForNonExpiringEvents(t *testing.T) {
	svc, repo := testService(t)
	ctx := context.Background()

	// Expiration event → active=false, no expiresAt → no notification
	eventTimeMs := time.Now().Add(-23 * time.Hour).UnixMilli()
	if _, err := svc.IngestStoreWebhook(ctx, models.StoreEvent{
		EventID:     "evt_notif_expiration",
		UserID:      "u_notif_expiration",
		Type:        models.Expiration,
		EventTimeMs: eventTimeMs,
		ProductID:   "premium_monthly",
	}); err != nil {
		t.Fatal(err)
	}

	var count int
	if err := repo.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM notifications WHERE user_id = $1`, "u_notif_expiration").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("expiration event should not schedule notification, got %d", count)
	}
}

func TestConcurrentCarrierClaimsDoNotDoubleClaim(t *testing.T) {
	_, repo := testService(t)
	ctx := context.Background()

	tx, err := repo.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.ScheduleCarrierPollTx(ctx, tx, "u_claim", time.Now().Add(-time.Minute), ""); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	first, err := repo.ClaimCarrierPollJobs(ctx, "worker-1", 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	second, err := repo.ClaimCarrierPollJobs(ctx, "worker-2", 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	if len(first) != 1 || len(second) != 0 {
		t.Fatalf("expected one claim then no duplicate claim, got %v and %v", first, second)
	}
}

func TestGetEntitlementNone(t *testing.T) {
	svc, _ := testService(t)
	ctx := context.Background()

	got, err := svc.Get(ctx, "u_get_none")
	if err != nil {
		t.Fatal(err)
	}
	if got.Active {
		t.Fatalf("expected inactive, got %+v", got)
	}
	if got.Source != models.SourceNone {
		t.Fatalf("expected source NONE, got %q", got.Source)
	}
	if got.ExpiresAt != nil {
		t.Fatalf("expected nil expiresAt, got %v", got.ExpiresAt)
	}
}

func TestGetEntitlementStoreActive(t *testing.T) {
	svc, repo := testService(t)
	ctx := context.Background()

	expiry := time.Now().UTC().Add(30 * 24 * time.Hour).Truncate(time.Second)
	tx, err := repo.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.UpsertSourceEntitlementTx(ctx, tx, models.SourceEntitlement{
		UserID:          "u_get_store",
		Source:          models.SourceStore,
		Active:          true,
		Reason:          "INITIAL_PURCHASE",
		ProductID:       "premium_monthly",
		ExpiresAt:       &expiry,
		LastEventTimeMs: time.Now().UnixMilli(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	got, err := svc.Get(ctx, "u_get_store")
	if err != nil {
		t.Fatal(err)
	}
	if !got.Active {
		t.Fatalf("expected active, got %+v", got)
	}
	if got.Source != models.SourceStore {
		t.Fatalf("expected source STORE, got %q", got.Source)
	}
	if got.Reason != "INITIAL_PURCHASE" {
		t.Fatalf("expected reason INITIAL_PURCHASE, got %q", got.Reason)
	}
	if got.ExpiresAt == nil || !got.ExpiresAt.Equal(expiry) {
		t.Fatalf("expected expiresAt %v, got %v", expiry, got.ExpiresAt)
	}
}

func TestGetEntitlementCarrierActive(t *testing.T) {
	svc, repo := testService(t)
	ctx := context.Background()

	tx, err := repo.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.UpsertSourceEntitlementTx(ctx, tx, models.SourceEntitlement{
		UserID:          "u_get_carrier",
		Source:          models.SourceCarrier,
		Active:          true,
		Reason:          "CARRIER_ACTIVE",
		LastEventTimeMs: time.Now().UnixMilli(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	got, err := svc.Get(ctx, "u_get_carrier")
	if err != nil {
		t.Fatal(err)
	}
	if !got.Active {
		t.Fatalf("expected active, got %+v", got)
	}
	if got.Source != models.SourceCarrier {
		t.Fatalf("expected source CARRIER, got %q", got.Source)
	}
}

func TestGetEntitlementMarketplaceActive(t *testing.T) {
	svc, repo := testService(t)
	ctx := context.Background()

	tx, err := repo.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.UpsertSourceEntitlementTx(ctx, tx, models.SourceEntitlement{
		UserID:          "u_get_marketplace",
		Source:          models.SourceMarketplace,
		Active:          true,
		Reason:          "MARKETPLACE_GRANT",
		LastEventTimeMs: time.Now().UnixMilli(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	got, err := svc.Get(ctx, "u_get_marketplace")
	if err != nil {
		t.Fatal(err)
	}
	if !got.Active {
		t.Fatalf("expected active, got %+v", got)
	}
	if got.Source != models.SourceMarketplace {
		t.Fatalf("expected source MARKETPLACE, got %q", got.Source)
	}
}

func TestGetEntitlementSourcePriority(t *testing.T) {
	svc, repo := testService(t)
	ctx := context.Background()

	tx, err := repo.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, src := range []models.Source{models.SourceCarrier, models.SourceMarketplace, models.SourceStore} {
		if err := repository.UpsertSourceEntitlementTx(ctx, tx, models.SourceEntitlement{
			UserID:          "u_get_priority",
			Source:          src,
			Active:          true,
			Reason:          "TEST",
			LastEventTimeMs: time.Now().UnixMilli(),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	got, err := svc.Get(ctx, "u_get_priority")
	if err != nil {
		t.Fatal(err)
	}
	if got.Source != models.SourceStore {
		t.Fatalf("STORE should win priority, got source %q", got.Source)
	}
	if len(got.ActiveSources) != 3 {
		t.Fatalf("expected 3 active sources, got %d", len(got.ActiveSources))
	}
}

func TestGetEntitlementInactiveSourceNotReturned(t *testing.T) {
	svc, repo := testService(t)
	ctx := context.Background()

	tx, err := repo.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.UpsertSourceEntitlementTx(ctx, tx, models.SourceEntitlement{
		UserID:          "u_get_inactive",
		Source:          models.SourceStore,
		Active:          false,
		Reason:          "EXPIRATION",
		LastEventTimeMs: time.Now().UnixMilli(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := repository.UpsertSourceEntitlementTx(ctx, tx, models.SourceEntitlement{
		UserID:          "u_get_inactive",
		Source:          models.SourceCarrier,
		Active:          true,
		Reason:          "CARRIER_ACTIVE",
		LastEventTimeMs: time.Now().UnixMilli(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	got, err := svc.Get(ctx, "u_get_inactive")
	if err != nil {
		t.Fatal(err)
	}
	if got.Source != models.SourceCarrier {
		t.Fatalf("inactive STORE should not suppress active CARRIER, got source %q", got.Source)
	}
	if len(got.ActiveSources) != 1 {
		t.Fatalf("expected 1 active source, got %d: %v", len(got.ActiveSources), got.ActiveSources)
	}
}

func testService(t *testing.T) (*EntitlementService, *repository.Repository) {
	t.Helper()

	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set TEST_DATABASE_URL to run Postgres integration tests")
	}

	database, err := db.Connect(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })

	if err := db.Migrate(database); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`
		TRUNCATE store_events, source_entitlements, carrier_poll_jobs, notifications RESTART IDENTITY
	`); err != nil {
		t.Fatal(err)
	}

	repo := repository.New(database)
	return NewEntitlementService(repo), repo
}
