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
	"errors"
	"fmt"
	"strings"
)

const (
	defaultSchema = "metrics"
	defaultTable  = "scheduler"
	defaultPort   = "5432"
)

var (
	// ErrConfigNotFound indicates that no recorder configuration was present in the backing provider.
	ErrConfigNotFound = errors.New("metrics recorder configuration not found")
)

func (c *Config) applyDefaults() {
	if c.Schema == "" {
		c.Schema = defaultSchema
	}
	if c.Table == "" {
		c.Table = defaultTable
	}
	if c.Port == "" {
		c.Port = defaultPort
	}
}

// Validate ensures the recorder configuration contains all required fields after defaults are applied.
func (c Config) Validate() error {
	missing := make([]string, 0, 4)

	if strings.TrimSpace(c.URL) == "" {
		missing = append(missing, "url")
	}
	if strings.TrimSpace(c.Username) == "" {
		missing = append(missing, "username")
	}
	if strings.TrimSpace(c.Password) == "" {
		missing = append(missing, "password")
	}
	if strings.TrimSpace(c.Name) == "" {
		missing = append(missing, "name")
	}
	if strings.TrimSpace(c.Table) == "" {
		missing = append(missing, "table")
	}

	if len(missing) > 0 {
		return fmt.Errorf("missing recorder configuration values: %s", strings.Join(missing, ", "))
	}

	return nil
}
