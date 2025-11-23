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
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"k8s.io/klog/v2"
)

var (
	// ErrRecorderQueueFull indicates the recorder's buffer is saturated and cannot accept more records.
	ErrRecorderQueueFull = errors.New("metrics recorder queue full")
	// ErrRecorderStopped indicates the recorder has been shut down and no longer accepts records.
	ErrRecorderStopped = errors.New("metrics recorder stopped")
)

type postgresRecorder struct {
	pool          *pgxpool.Pool
	upsertSQL     string
	queue         chan SchedulerRecord
	batchSize     int
	flushInterval time.Duration
	flushTimeout  time.Duration
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	stopped       atomic.Bool
}

// NewRecorder establishes a pooled PostgreSQL connection using the provided config.
func NewRecorder(ctx context.Context, cfg Config) (Recorder, error) {
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	connString := buildConnString(cfg)

	poolConfig, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse recorder connection config: %w", err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to recorder database: %w", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	rec := &postgresRecorder{
		pool:          pool,
		upsertSQL:     buildUpsertSQL(cfg.Schema, cfg.Table),
		queue:         make(chan SchedulerRecord, cfg.QueueCapacity),
		batchSize:     cfg.BatchSize,
		flushInterval: cfg.FlushInterval,
		flushTimeout:  cfg.FlushTimeout,
		cancel:        cancel,
	}
	rec.wg.Add(1)
	go rec.run(runCtx)

	return rec, nil
}

func (r *postgresRecorder) UpsertSchedulerRecord(ctx context.Context, record SchedulerRecord) error {
	if r == nil || r.pool == nil {
		return errors.New("recorder not initialized")
	}
	if r.stopped.Load() {
		return ErrRecorderStopped
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

	select {
	case <-ctx.Done():
		return ctx.Err()
	case r.queue <- record:
		return nil
	default:
		return ErrRecorderQueueFull
	}
}

func (r *postgresRecorder) Close(ctx context.Context) error {
	if r == nil {
		return nil
	}
	if r.stopped.Swap(true) {
		return nil
	}
	if r.cancel != nil {
		r.cancel()
	}
	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}
	if r.pool != nil {
		r.pool.Close()
	}
	return nil
}

func (r *postgresRecorder) run(ctx context.Context) {
	defer r.wg.Done()
	ticker := time.NewTicker(r.flushInterval)
	defer ticker.Stop()

	batch := make([]SchedulerRecord, 0, r.batchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		flushCtx, cancel := context.WithTimeout(context.Background(), r.flushTimeout)
		if err := r.flushBatch(flushCtx, batch); err != nil {
			klog.Background().Error(err, "scheduler recorder flush failed", "batchSize", len(batch))
		}
		cancel()
		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			for {
				select {
				case rec, ok := <-r.queue:
					if !ok {
						flush()
						return
					}
					batch = append(batch, rec)
					if len(batch) >= r.batchSize {
						flush()
					}
				default:
					flush()
					return
				}
			}
		case rec, ok := <-r.queue:
			if !ok {
				flush()
				return
			}
			batch = append(batch, rec)
			if len(batch) >= r.batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (r *postgresRecorder) flushBatch(ctx context.Context, batch []SchedulerRecord) error {
	pgBatch := &pgx.Batch{}
	for _, record := range batch {
		pgBatch.Queue(r.upsertSQL,
			record.RequestID,
			record.ReceiveRequestAt,
			record.SchedulerStartAt,
			record.ServiceSelectedAt,
			record.SelectedActor,
		)
	}

	results := r.pool.SendBatch(ctx, pgBatch)

	for range batch {
		if _, err := results.Exec(); err != nil {
			results.Close()
			return fmt.Errorf("recorder batch upsert failed: %w", err)
		}
	}

	if err := results.Close(); err != nil {
		return fmt.Errorf("recorder batch close failed: %w", err)
	}

	return nil
}

func buildConnString(cfg Config) string {
	if strings.Contains(cfg.URL, "://") {
		return cfg.URL
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

	return u.String()
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
