package repository

import (
	"context"
	"os"
	"testing"
	"time"

	"subscription-reconciler/internal/db"
)

func TestCheckRateLimit(t *testing.T) {
	repo := testRepo(t)
	ctx := context.Background()

	allowed, err := repo.CheckRateLimit(ctx, "test-rate-key", 2, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !allowed {
		t.Fatal("first request should be allowed")
	}

	allowed, err = repo.CheckRateLimit(ctx, "test-rate-key", 2, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !allowed {
		t.Fatal("second request should be allowed")
	}

	allowed, err = repo.CheckRateLimit(ctx, "test-rate-key", 2, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if allowed {
		t.Fatal("third request should be rate limited")
	}
}

func TestClaimAPIRequestCachesCompletedResponse(t *testing.T) {
	repo := testRepo(t)
	ctx := context.Background()

	claim, err := repo.ClaimAPIRequest(ctx, "req-1", "POST", "/webhooks/store", "hash-1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !claim.Claimed {
		t.Fatal("first request should be claimed")
	}

	claim, err = repo.ClaimAPIRequest(ctx, "req-1", "POST", "/webhooks/store", "hash-1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !claim.InProgress {
		t.Fatal("duplicate should be suppressed while original is in progress")
	}

	if err := repo.CompleteAPIRequest(ctx, "req-1", 200, "application/json", []byte(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}

	claim, err = repo.ClaimAPIRequest(ctx, "req-1", "POST", "/webhooks/store", "hash-1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if claim.Claimed || claim.InProgress || claim.StatusCode != 200 || string(claim.ResponseBody) != `{"ok":true}` {
		t.Fatalf("expected cached response, got %+v", claim)
	}
}

func TestClaimAPIRequestRejectsBodyMismatch(t *testing.T) {
	repo := testRepo(t)
	ctx := context.Background()

	claim, err := repo.ClaimAPIRequest(ctx, "req-2", "POST", "/webhooks/store", "hash-1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !claim.Claimed {
		t.Fatal("first request should be claimed")
	}

	claim, err = repo.ClaimAPIRequest(ctx, "req-2", "POST", "/webhooks/store", "hash-2", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !claim.BodyMismatch {
		t.Fatalf("expected body mismatch, got %+v", claim)
	}
}

func testRepo(t *testing.T) *Repository {
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
		TRUNCATE store_events, source_entitlements, carrier_poll_jobs, notifications, api_request_cache, api_rate_limits RESTART IDENTITY
	`); err != nil {
		t.Fatal(err)
	}

	return New(database)
}
