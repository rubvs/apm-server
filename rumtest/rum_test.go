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

package rumtest

import (
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/elastic/apm-server/rumtest/estest"
	"github.com/elastic/apm-tools/pkg/approvaltest"
	"github.com/elastic/apm-tools/pkg/espoll"
)

func TestRUMXForwardedFor(t *testing.T) {
	t.Run("rum", func(t *testing.T) {
		f := SetupFixture(t)
		defer f.TeardownFixture(t)

		serverURL, err := url.Parse(f.Server.URL)
		require.NoError(t, err)
		serverURL.Path = "/intake/v2/rum/events"

		const body = `{"metadata":{"service":{"name":"rum-js-test","agent":{"name":"rum-js","version":"5.5.0"}}}}
{"transaction":{"trace_id":"611f4fa950f04631aaaaaaaaaaaaaaaa","id":"611f4fa950f04631","type":"page-load","duration":643,"span_count":{"started":0}}}
{"metricset":{"samples":{"transaction.breakdown.count":{"value":12},"transaction.duration.sum.us":{"value":12},"transaction.duration.count":{"value":2},"transaction.self_time.sum.us":{"value":10},"transaction.self_time.count":{"value":2},"span.self_time.count":{"value":1},"span.self_time.sum.us":{"value":633.288}},"transaction":{"type":"request","name":"GET /"},"span":{"type":"external","subtype":"http"},"timestamp": 1496170422281000}}
`

		req, _ := http.NewRequest("POST", serverURL.String(), strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-ndjson")
		ipAddress := "220.244.41.16"
		req.Header.Set("X-Forwarded-For", ipAddress)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, resp.StatusCode)
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		result := estest.ExpectMinDocs(t,
			Elasticsearch,
			2,
			"traces-apm*,metrics-apm*",
			espoll.TermsQuery{
				Field:  "processor.event",
				Values: []any{"transaction", "metric"},
			},
			espoll.WithTimeout(60*time.Second),
		)

		for _, hit := range result.Hits.Hits {
			// The "tags" field should only be populated if enrichment failed.
			_, hasTags := hit.Source["tags"]
			if hasTags {
				t.Errorf("did not expecte tags field to be present")
			}
			// The "client.geo" field should only be populated if enrichment succeeded.
			if hit.Source["client"] != nil {
				_, hasClientGeo := hit.Source["client"].(map[string]any)["geo"]
				if !hasClientGeo {
					t.Errorf("expected client.geo field to be present when GeoIP database is mounted")
				}
			}
		}

		approvaltest.ApproveFields(
			t, t.Name(), result.Hits.Hits,
			"@timestamp", "timestamp.us",
			"source.port",
			"client.geo.city_name",
			"client.geo.location",
			"client.geo.region_iso_code",
			"client.geo.region_name",
		)
	})
}
