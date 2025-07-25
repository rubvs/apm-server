// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package beater

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/elastic/apm-server/internal/beater/config"
	"github.com/elastic/elastic-agent-libs/monitoring"
)

func TestRecordConfigs(t *testing.T) {
	stateRegistry := monitoring.NewRegistry()

	apmCfg := config.DefaultConfig()
	apmCfg.AgentAuth.APIKey.Enabled = true
	apmCfg.Kibana.Enabled = true
	recordAPMServerConfig(apmCfg, stateRegistry)

	fs := monitoring.CollectStructSnapshot(stateRegistry, monitoring.Full, false)

	assert.Equal(t, map[string]any{
		"apm-server": map[string]any{
			"rum": map[string]any{
				"enabled": false,
			},
			"api_key": map[string]any{
				"enabled": true,
			},
			"kibana": map[string]any{
				"enabled": true,
			},
			"ssl": map[string]any{
				"enabled": false,
			},
			"sampling": map[string]any{
				"tail": map[string]any{
					"enabled":  false,
					"policies": int64(0),
				},
			},
		},
	}, fs)
}
