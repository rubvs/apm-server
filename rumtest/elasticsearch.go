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
	"net/url"
	"time"

	"github.com/elastic/apm-tools/pkg/espoll"
	"github.com/elastic/go-elasticsearch/v8"

	"github.com/elastic/apm-server/rumtest/apmservertest"
)

const (
	adminElasticsearchUser  = "admin"
	adminElasticsearchPass  = "changeme"
	maxElasticsearchBackoff = 10 * time.Second
)

var (
	Elasticsearch *espoll.Client
)

func initElasticSearch() {
	cfg := newElasticsearchConfig()
	cfg.Username = adminElasticsearchUser
	cfg.Password = adminElasticsearchPass
	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		panic(err)
	}
	Elasticsearch = &espoll.Client{Client: client}
}

func newElasticsearchConfig() elasticsearch.Config {
	var addresses []string
	for _, host := range apmservertest.DefaultConfig().Output.Elasticsearch.Hosts {
		u := url.URL{Scheme: "http", Host: host}
		addresses = append(addresses, u.String())
	}
	return elasticsearch.Config{
		Addresses:  addresses,
		MaxRetries: 15,
		RetryBackoff: func(attempt int) time.Duration {
			backoff := (500 * time.Millisecond) * (1 << (attempt - 1))
			if backoff > maxElasticsearchBackoff {
				backoff = maxElasticsearchBackoff
			}
			return backoff
		},
	}
}
