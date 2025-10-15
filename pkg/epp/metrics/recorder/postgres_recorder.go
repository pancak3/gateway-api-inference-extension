/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package recorder

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

type postgresRecorder struct {
	pool      *pgxpool.Pool
	upsertSQL string
}

// NewRecorder establishes a pooled PostgreSQL connection using the provided config.
func NewRecorder(ctx context.Context, cfg Config) (Recorder, error) {
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	connString, err := buildConnString(cfg)
	if err != nil {
		return nil, err
	}

	poolConfig, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse recorder connection config: %w", err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to recorder database: %w", err)
	}

	return &postgresRecorder{
		pool:      pool,
		upsertSQL: buildUpsertSQL(cfg.Schema, cfg.Table),
	}, nil
}

func (r *postgresRecorder) UpsertSchedulerRecord(ctx context.Context, record SchedulerRecord) error {
	if r == nil || r.pool == nil {
		return errors.New("recorder not initialized")
	}

	if strings.TrimSpace(record.RequestID) == "" {
		return errors.New("recorder: request id must not be empty")
	}
	if record.ReceiveRequestAt == 0 {
		return errors.New("recorder: receive timestamp must not be zero")
	}
	if record.SchedulerStartAt == 0 {
		return errors.New("recorder: scheduler start timestamp must not be zero")
	}
	if record.ServiceSelectedAt == 0 {
		return errors.New("recorder: service selected timestamp must not be zero")
	}
	if strings.TrimSpace(record.SelectedActor) == "" {
		return errors.New("recorder: selected actor must not be empty")
	}

	_, err := r.pool.Exec(ctx, r.upsertSQL,
		record.RequestID,
		record.ReceiveRequestAt,
		record.SchedulerStartAt,
		record.ServiceSelectedAt,
		record.SelectedActor,
	)
	if err != nil {
		return fmt.Errorf("recorder upsert failed: %w", err)
	}

	return nil
}

func (r *postgresRecorder) Close(context.Context) error {
	if r == nil || r.pool == nil {
		return nil
	}
	r.pool.Close()
	return nil
}

func buildConnString(cfg Config) (string, error) {
	if strings.Contains(cfg.URL, "://") {
		return cfg.URL, nil
	}

	host := cfg.URL
	if cfg.Port != "" {
		if _, _, err := net.SplitHostPort(cfg.URL); err != nil {
			host = net.JoinHostPort(cfg.URL, cfg.Port)
		}
	}

	u := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(cfg.Username, cfg.Password),
		Host:   host,
		Path:   "/" + cfg.Name,
	}

	return u.String(), nil
}

func buildUpsertSQL(schema, table string) string {
	qualified := quoteIdentifier(table)
	if strings.TrimSpace(schema) != "" {
		qualified = quoteIdentifier(schema) + "." + qualified
	}

	return fmt.Sprintf(`INSERT INTO %s ("RequestID", "ReceiveRequestAt", "SchedulerStartAt", "ServiceSelectedAt", "SelectedActor") VALUES ($1, $2, $3, $4, $5) ON CONFLICT ("RequestID") DO UPDATE SET "ReceiveRequestAt" = EXCLUDED."ReceiveRequestAt", "SchedulerStartAt" = EXCLUDED."SchedulerStartAt", "ServiceSelectedAt" = EXCLUDED."ServiceSelectedAt", "SelectedActor" = EXCLUDED."SelectedActor"`, qualified)
}

func quoteIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

// SelectedActor is persisted as TEXT. Validation above already ensures it is non-empty.
