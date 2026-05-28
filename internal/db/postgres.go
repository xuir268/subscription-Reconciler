package db

import (
	"database/sql"
	"time"

	_ "github.com/lib/pq"
)

func Connect(databaseURL string) (*sql.DB, error) {
	database, err := sql.Open("postgres", databaseURL)
	if err != nil {
		return nil, err
	}

	database.SetMaxOpenConns(10)
	database.SetMaxIdleConns(5)
	database.SetConnMaxLifetime(30 * time.Minute)

	if err := database.Ping(); err != nil {
		return nil, err
	}

	return database, nil
}

func Migrate(database *sql.DB) error {
	_, err := database.Exec(`
CREATE TABLE IF NOT EXISTS store_events (
	event_id TEXT PRIMARY KEY,
	user_id TEXT NOT NULL,
	type TEXT NOT NULL,
	event_time_ms BIGINT NOT NULL,
	product_id TEXT NOT NULL,
	received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS source_entitlements (
	user_id TEXT NOT NULL,
	source TEXT NOT NULL,
	active BOOLEAN NOT NULL,
	reason TEXT NOT NULL,
	product_id TEXT,
	expires_at TIMESTAMPTZ,
	last_event_time_ms BIGINT NOT NULL DEFAULT 0,
	updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	PRIMARY KEY (user_id, source)
);

CREATE TABLE IF NOT EXISTS carrier_poll_jobs (
	user_id TEXT PRIMARY KEY,
	next_poll_at TIMESTAMPTZ NOT NULL,
	locked_until TIMESTAMPTZ,
	worker_id TEXT,
	last_error TEXT
);

CREATE TABLE IF NOT EXISTS notifications (
	id BIGSERIAL PRIMARY KEY,
	user_id TEXT NOT NULL,
	type TEXT NOT NULL DEFAULT 'PREMIUM_EXPIRES_SOON',
	entitlement_source TEXT NOT NULL,
	expires_at TIMESTAMPTZ NOT NULL,
	scheduled_for TIMESTAMPTZ NOT NULL,
	message TEXT NOT NULL,
	sent_at TIMESTAMPTZ,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	UNIQUE(user_id, entitlement_source, expires_at)
);

CREATE TABLE IF NOT EXISTS api_request_cache (
	request_key TEXT PRIMARY KEY,
	method TEXT NOT NULL,
	path TEXT NOT NULL,
	body_hash TEXT NOT NULL,
	state TEXT NOT NULL,
	status_code INTEGER,
	content_type TEXT,
	response_body BYTEA,
	expires_at TIMESTAMPTZ NOT NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS api_rate_limits (
	rate_key TEXT PRIMARY KEY,
	count INTEGER NOT NULL,
	reset_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS app_config (
	key TEXT PRIMARY KEY,
	value TEXT NOT NULL,
	updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO app_config(key, value)
VALUES
	('api_rate_limit_per_minute', '120'),
	('api_request_cache_ttl_seconds', '600'),
	('api_gateway_enabled', 'true')
ON CONFLICT(key) DO NOTHING;

CREATE INDEX IF NOT EXISTS carrier_poll_jobs_due_idx
	ON carrier_poll_jobs(next_poll_at, locked_until);

CREATE INDEX IF NOT EXISTS notifications_due_idx
	ON notifications(scheduled_for)
	WHERE sent_at IS NULL;

CREATE TABLE IF NOT EXISTS entitlement_audit_log (
	id BIGSERIAL PRIMARY KEY,
	user_id TEXT NOT NULL,
	source TEXT NOT NULL,
	event_id TEXT,
	prev_active BOOLEAN,
	prev_reason TEXT,
	prev_expires_at TIMESTAMPTZ,
	next_active BOOLEAN NOT NULL,
	next_reason TEXT NOT NULL,
	next_expires_at TIMESTAMPTZ,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS audit_log_user_created_idx
	ON entitlement_audit_log(user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS api_request_cache_expires_idx
	ON api_request_cache(expires_at);
`)
	return err
}
