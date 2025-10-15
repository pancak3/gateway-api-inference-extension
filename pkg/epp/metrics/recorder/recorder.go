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

import "context"

const (
	// EnvDBURL is the environment variable containing the database host or URL.
	EnvDBURL = "INFERENCE_DB_URL"
	// EnvDBUsername is the environment variable containing the database user name.
	EnvDBUsername = "INFERENCE_DB_USERNAME"
	// EnvDBPassword is the environment variable containing the database password.
	EnvDBPassword = "INFERENCE_DB_PASSWORD"
	// EnvDBName is the environment variable containing the database name.
	EnvDBName = "INFERENCE_DB_NAME"
	// EnvDBSchema is the environment variable containing the database schema name.
	EnvDBSchema = "INFERENCE_DB_SCHEMA"
	// EnvDBTable is the environment variable containing the fully qualified table name to store scheduler records.
	EnvDBTable = "INFERENCE_DB_TABLE"
	// EnvDBPort is the environment variable containing the port that exposes the PostgreSQL service.
	EnvDBPort = "INFERENCE_DB_PORT"
)

// Config aggregates the connection parameters required to establish a reusable database connection.
type Config struct {
	URL      string
	Username string
	Password string
	Name     string
	Schema   string
	Table    string
	Port     string
}

// ConfigProvider extracts recorder configuration from a backing store such as the process environment.
type ConfigProvider interface {
	Config(ctx context.Context) (Config, error)
}

// SchedulerRecord describes a scheduler request lifecycle entry persisted to metrics."scheduler".
type SchedulerRecord struct {
	RequestID         string
	ReceiveRequestAt  int64
	SchedulerStartAt  int64
	ServiceSelectedAt int64
	SelectedActor     string
}

// Repository represents the PostgreSQL storage abstraction responsible for persisting scheduler records.
type Repository interface {
	UpsertSchedulerRecord(ctx context.Context, record SchedulerRecord) error
}

// Recorder persists scheduler records while reusing a live database connection until explicitly closed.
type Recorder interface {
	Repository
	Close(ctx context.Context) error
}
