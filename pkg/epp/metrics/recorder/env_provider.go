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
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// NewEnvConfigProvider returns a ConfigProvider that reads values from process environment variables.
func NewEnvConfigProvider() ConfigProvider {
	return envConfigProvider{}
}

type envConfigProvider struct{}

func (envConfigProvider) Config(_ context.Context) (Config, error) {
	cfg := Config{}
	anySet := false

	if value, ok := lookupEnvTrimmed(EnvDBURL); ok {
		cfg.URL = value
		anySet = true
	}
	if value, ok := lookupEnvTrimmed(EnvDBUsername); ok {
		cfg.Username = value
		anySet = true
	}
	if value, ok := lookupEnvTrimmed(EnvDBPassword); ok {
		cfg.Password = value
		anySet = true
	}
	if value, ok := lookupEnvTrimmed(EnvDBName); ok {
		cfg.Name = value
		anySet = true
	}
	if value, ok := lookupEnvTrimmed(EnvDBSchema); ok {
		cfg.Schema = value
		anySet = true
	}
	if value, ok := lookupEnvTrimmed(EnvDBTable); ok {
		cfg.Table = value
		anySet = true
	}
	if value, ok := lookupEnvTrimmed(EnvDBPort); ok {
		cfg.Port = value
		anySet = true
	}
	if value, ok := lookupEnvTrimmed(EnvQueueCapacity); ok {
		if parsed, err := strconv.Atoi(value); err == nil && parsed > 0 {
			cfg.QueueCapacity = parsed
		}
	}
	if value, ok := lookupEnvTrimmed(EnvBatchSize); ok {
		if parsed, err := strconv.Atoi(value); err == nil && parsed > 0 {
			cfg.BatchSize = parsed
		}
	}
	if value, ok := lookupEnvTrimmed(EnvFlushInterval); ok {
		if duration, err := parseDuration(value); err == nil {
			cfg.FlushInterval = duration
		}
	}
	if value, ok := lookupEnvTrimmed(EnvFlushTimeout); ok {
		if duration, err := parseDuration(value); err == nil {
			cfg.FlushTimeout = duration
		}
	}

	if !anySet {
		return Config{}, ErrConfigNotFound
	}

	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func lookupEnvTrimmed(key string) (string, bool) {
	value, ok := os.LookupEnv(key)
	if !ok {
		return "", false
	}

	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", false
	}

	return trimmed, true
}

func parseDuration(value string) (time.Duration, error) {
	if duration, err := time.ParseDuration(value); err == nil {
		return duration, nil
	}
	if millis, err := strconv.Atoi(value); err == nil {
		return time.Duration(millis) * time.Millisecond, nil
	}
	return 0, fmt.Errorf("invalid duration %q", value)
}
