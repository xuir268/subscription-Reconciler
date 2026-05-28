package repository

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
)

type ConfigEntry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (r *Repository) GetConfigInt(ctx context.Context, key string, fallback int) int {
	var value string
	err := r.DB.QueryRowContext(ctx, `SELECT value FROM app_config WHERE key = $1`, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) || err != nil {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func (r *Repository) GetConfigBool(ctx context.Context, key string, fallback bool) bool {
	var value string
	err := r.DB.QueryRowContext(ctx, `SELECT value FROM app_config WHERE key = $1`, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) || err != nil {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func (r *Repository) ListConfig(ctx context.Context) ([]ConfigEntry, error) {
	rows, err := r.DB.QueryContext(ctx, `SELECT key, value FROM app_config ORDER BY key`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []ConfigEntry
	for rows.Next() {
		var entry ConfigEntry
		if err := rows.Scan(&entry.Key, &entry.Value); err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, rows.Err()
}

func (r *Repository) SetConfig(ctx context.Context, key, value string) error {
	_, err := r.DB.ExecContext(ctx, `
		INSERT INTO app_config(key, value, updated_at)
		VALUES ($1, $2, now())
		ON CONFLICT(key) DO UPDATE SET
			value = EXCLUDED.value,
			updated_at = now()
	`, key, value)
	return err
}
