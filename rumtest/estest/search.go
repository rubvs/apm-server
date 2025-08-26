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

package estest

import (
	"context"
	"strings"
	"testing"

	"github.com/elastic/apm-tools/pkg/espoll"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

func ExpectMinDocs(
	t testing.TB,
	es *espoll.Client,
	min int,
	index string,
	query interface{},
	opts ...espoll.RequestOption,
) espoll.SearchResult {
	t.Helper()

	var result espoll.SearchResult
	req := es.NewSearchRequest(index)
	req.ExpandWildcards = "open,hidden"

	if min > 10 {
		// Size defaults to 10. If the caller expects more than 10,
		// return it in the search so we don't have to search again.
		req = req.WithSize(min)
	}
	if query != nil {
		req = req.WithQuery(query)
	}

	opts = append(opts, espoll.WithCondition(espoll.AllCondition(
		result.Hits.MinHitsCondition(min),
		result.Hits.TotalHitsCondition(req),
	)))

	// Refresh the indices before issuing the search request.
	refreshReq := esapi.IndicesRefreshRequest{
		Index:           strings.Split(",", index),
		ExpandWildcards: "all",
	}
	rsp, err := refreshReq.Do(context.Background(), es.Transport)
	if err != nil {
		t.Fatalf("failed refreshing indices: %s: %s", index, err.Error())
	}

	rsp.Body.Close()

	if _, err := req.Do(context.Background(), &result, opts...); err != nil {
		t.Fatal(err)
	}

	return result
}
