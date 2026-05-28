package repository

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

type APIRequestClaim struct {
	Claimed      bool
	InProgress   bool
	BodyMismatch bool
	StatusCode   int
	ContentType  string
	ResponseBody []byte
}

func (r *Repository) CheckRateLimit(ctx context.Context, rateKey string, limit int, window time.Duration) (bool, error) {
	var count int
	err := r.DB.QueryRowContext(ctx, `
		INSERT INTO api_rate_limits(rate_key, count, reset_at)
		VALUES ($1, 1, now() + ($2 * interval '1 second'))
		ON CONFLICT(rate_key)
		DO UPDATE SET
			count = CASE
				WHEN api_rate_limits.reset_at <= now() THEN 1
				ELSE api_rate_limits.count + 1
			END,
			reset_at = CASE
				WHEN api_rate_limits.reset_at <= now() THEN now() + ($2 * interval '1 second')
				ELSE api_rate_limits.reset_at
			END
		RETURNING count
	`, rateKey, int64(window.Seconds())).Scan(&count)
	if err != nil {
		return false, err
	}
	return count <= limit, nil
}

func (r *Repository) ClaimAPIRequest(ctx context.Context, requestKey, method, path, bodyHash string, ttl time.Duration) (APIRequestClaim, error) {
	_, _ = r.DB.ExecContext(ctx, `DELETE FROM api_request_cache WHERE request_key = $1 AND expires_at <= now()`, requestKey)

	res, err := r.DB.ExecContext(ctx, `
		INSERT INTO api_request_cache(request_key, method, path, body_hash, state, expires_at)
		VALUES ($1, $2, $3, $4, 'PROCESSING', now() + ($5 * interval '1 second'))
		ON CONFLICT(request_key) DO NOTHING
	`, requestKey, method, path, bodyHash, int64(ttl.Seconds()))
	if err != nil {
		return APIRequestClaim{}, err
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return APIRequestClaim{}, err
	}
	if rows == 1 {
		return APIRequestClaim{Claimed: true}, nil
	}

	var state, storedBodyHash string
	var statusCode sql.NullInt64
	var contentType sql.NullString
	var responseBody []byte
	err = r.DB.QueryRowContext(ctx, `
		SELECT state, body_hash, status_code, content_type, response_body
		FROM api_request_cache
		WHERE request_key = $1 AND expires_at > now()
	`, requestKey).Scan(&state, &storedBodyHash, &statusCode, &contentType, &responseBody)
	if errors.Is(err, sql.ErrNoRows) {
		return APIRequestClaim{InProgress: true}, nil
	}
	if err != nil {
		return APIRequestClaim{}, err
	}
	if storedBodyHash != bodyHash {
		return APIRequestClaim{BodyMismatch: true}, nil
	}
	if state != "COMPLETED" {
		return APIRequestClaim{InProgress: true}, nil
	}

	return APIRequestClaim{
		StatusCode:   int(statusCode.Int64),
		ContentType:  contentType.String,
		ResponseBody: responseBody,
	}, nil
}

func (r *Repository) CompleteAPIRequest(ctx context.Context, requestKey string, statusCode int, contentType string, responseBody []byte) error {
	_, err := r.DB.ExecContext(ctx, `
		UPDATE api_request_cache
		SET state = 'COMPLETED',
			status_code = $2,
			content_type = NULLIF($3, ''),
			response_body = $4,
			updated_at = now()
		WHERE request_key = $1
	`, requestKey, statusCode, contentType, responseBody)
	return err
}

func (r *Repository) ForgetAPIRequest(ctx context.Context, requestKey string) error {
	_, err := r.DB.ExecContext(ctx, `DELETE FROM api_request_cache WHERE request_key = $1`, requestKey)
	return err
}
