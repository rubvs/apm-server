package rumtest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"testing"
	"time"

	"github.com/elastic/apm-server/rumtest/apmservertest"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/stretchr/testify/require"
)

type TestFixture struct {
	Server *apmservertest.Server
}

func SetupFixture(t *testing.T) *TestFixture {

	// Setup ES
	initContainers()
	if err := StartStackContainers(); err != nil {
		t.Fatalf("failed to start stack containers: %v", err)
	}
	initElasticSearch()

//	err := ToggleGeoIpMount(context.Background(), true)
//	require.NoError(t, err)
//	initElasticSearch()

//	err := waitGeoIPDatabase(context.Background())
//	require.NoError(t, err)

	// Setup APM Server
	srv := apmservertest.NewUnstartedServerTB(t)
	srv.Config.RUM = &apmservertest.RUMConfig{
		Enabled: true,
	}
	err := srv.Start()
	require.NoError(t, err)

	return &TestFixture{
		Server: srv,
	}
}

func (f *TestFixture) TeardownFixture(t *testing.T) {
	if f.Server != nil {
		defer func() {
			if closer, ok := f.Server.Log.(io.Closer); ok {
				closer.Close()
			}
		}()

		// Call the server's Close method in a background goroutine,
		// and wait for up to 10 seconds for it to complete.
		errc := make(chan error)
		go func() { errc <- f.Server.Close() }()
		select {
		case err := <-errc:
			if err != nil {
				t.Error(err)
			}
			close(errc)
		case <-time.After(10 * time.Second):
			go func() { <-errc; close(errc) }()
		}
	}

//	err := StopStackContainer()
//	require.NoError(t, err)
}

func waitGeoIPDatabase(ctx context.Context) error {
	b, err := json.Marshal(map[string]any{
		"persistent": map[string]any{
			"ingest.geoip.downloader.enabled":        true,
			"ingest.geoip.downloader.eager.download": true,
		},
	})
	r := esapi.ClusterPutSettingsRequest{
		Body: bytes.NewReader(b),
	}
	res, err := r.Do(ctx, Elasticsearch)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.IsError() {
		return fmt.Errorf("response err: %d", res.StatusCode)
	}

	var geoIpResponse struct {
		Databases []struct {
			Id       string `json:"id"`
			Database struct {
				Name string `json:"name"`
			} `json:"database"`
		} `json:"databases"`
	}

	first := true
	for {
		gr, err := Elasticsearch.Ingest.GetGeoipDatabase()
		if err != nil {
			return err
		}
		if err := json.NewDecoder(gr.Body).Decode(&geoIpResponse); err != nil {
			return err
		}
		if len(geoIpResponse.Databases) > 0 {
			return nil
		}
		if first {
			log.Printf("Waiting for geoIP database to be downloaded")
			first = false
		}
		time.Sleep(20 * time.Second)
	}
}
