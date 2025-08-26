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

package apmservertest

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"go.elastic.co/apm/v2"
	"go.uber.org/zap/zapcore"
)

// Server is an APM server listening on a system-chosen port on the local
// loopback interface, for use in end-to-end tests.
type Server struct {
	// Config holds configuration for apm-server, which will be passed
	// to the apm-server command.
	//
	// Config will be initialised with DefaultConfig, and may be changed
	// any time until Start is called.
	Config Config

	// Dir is the working directory of the server.
	//
	// If Dir is empty when Start is called on an unstarted server,
	// then it will be set to a temporary directory, in which an
	// empty apm-server.yml, and the pipeline definition, will be created.
	// The temporary directory will be removed when the server is closed.
	Dir string

	// CertDir is the directory of the TLS certificates.
	//
	// If Dir is empty the current directory is used
	CertDir string

	// Log holds an optional io.Writer to which the process's Stderr will
	// be written, in addition to being available through the Server.Logs
	// field.
	//
	// Log is set by NewServerTB and NewUnstartedServerTB, and will be nil
	// for calls to NewUnstartedServer. Callers of NewUnstartedServer may
	// set Log prior to calling Start.
	Log io.Writer

	// BeatUUID will be populated with the server's Beat UUID after Start
	// returns successfully. This can be used to search for documents
	// corresponding to this test server instance.
	BeatUUID string

	// Version will be populated with the servers' version number after
	// Start returns successfully.
	Version string

	// Logs provides access to the apm-server log entries.
	Logs LogEntries

	// Stderr holds the stderr for apm-server, excluding logging.
	Stderr io.ReadCloser

	// URL holds the base URL for Elastic APM agents, in the form
	// http[s]://ipaddr:port with no trailing slash.
	URL string

	// TLS is optional TLS client configuration, populated with a new config
	// after TLS is started.
	TLS *tls.Config

	// EventMetadataFilter holds an optional EventMetadataFilter, which
	// can modify event metadata before it is sent to the server.
	//
	// New(Unstarted)Server sets a default filter which removes or
	// replaces environment-specific properties such as host name,
	// container ID, etc., to enable repeatable tests across different
	// test environments.
	EventMetadataFilter EventMetadataFilter

	args []string
	cmd  *ServerCmd

	mu      sync.Mutex
	tracers []*apm.Tracer
	closeCh chan struct{}
}

// NewUnstartedServerTB returns an unstarted Server, passing args to the apm-server
// command. The server's Close method will be called when the test ends, and logs
// will be written under apm-server/systemtest/logs/<test-name>/.
func NewUnstartedServerTB(tb testing.TB, args ...string) *Server {
	s := &Server{
		Config:              DefaultConfig(),
		EventMetadataFilter: DefaultMetadataFilter{},
		args:                args,
		closeCh:             make(chan struct{}),
	}
	s.CertDir = tb.TempDir()
	s.Log = createLogfile(tb, "apm-server")
	return s
}

func (s *Server) Start() error {
	if s.URL != "" {
		panic("Server already started")
	}
	s.Logs.init()

	extra := map[string]any{
		"logging.level":             "debug",
		"logging.to_stderr":         true,
		"apm-server.expvar.enabled": true,
		"apm-server.host":           "127.0.0.1:0",
	}
	cfgargs, err := configArgs(s.Config, extra)
	if err != nil {
		return err
	}
	args := append(cfgargs, s.args...)
	args = append(args, "--path.home", ".") // working directory, s.Dir

	s.cmd = ServerCommand(context.Background(), "run", args...)
	s.cmd.Dir = s.Dir

	// This speeds up tests by forcing the self-instrumentation
	// event streams to be closed after 100ms. This is only necessary
	// because processor/stream waits for the stream to be closed
	// before the last batch is processed.
	//
	// TODO(axw) remove this once the server processes batches without
	// waiting for the stream to be closed.
	s.cmd.Env = append(os.Environ(), "ELASTIC_APM_API_REQUEST_TIME=1000ms")

	fmt.Println("cmd:", s.cmd)

	stderr, err := s.cmd.StderrPipe()
	if err != nil {
		return err
	}
	if err := s.cmd.Start(); err != nil {
		stderr.Close()
		return err
	}
	s.Dir = s.cmd.Dir

	// Consume the process's stderr.
	var stderrReader io.Reader = stderr
	if s.Log != nil {
		// Write the apm-server command line to the top of the log.
		s.printCmdline(s.Log, args)
		stderrReader = io.TeeReader(stderrReader, s.Log)
	}

	stderrPipeReader, stderrPipeWriter := io.Pipe()
	s.Stderr = stderrPipeReader

	go s.consumeStderr(stderrReader, stderrPipeWriter)

	logs := s.Logs.Iterator()
	defer logs.Close()
	if err := s.waitUntilListening(false, logs); err != nil {
		return err
	}

	time.Sleep(10 * time.Second)

	return nil
}

func (s *Server) printCmdline(w io.Writer, args []string) {
	var buf bytes.Buffer
	fmt.Fprint(&buf, "# Running apm-server\n")
	for i := 0; i < len(args); i += 2 {
		fmt.Fprintf(&buf, "# \t")
		if args[i] == "-E" && i+1 < len(args) {
			fmt.Fprintf(&buf, "%s %s\n", args[i], args[i+1])
		} else {
			fmt.Fprintf(&buf, "%s\n", strings.Join(args[i:], " "))
			break
		}
	}
	if _, err := buf.WriteTo(w); err != nil {
		panic(err)
	}
}

func (s *Server) waitUntilListening(tls bool, logs *LogEntryIterator) error {
	// First wait for the Beat UUID and server version to be logged.
	for entry := range logs.C() {
		if entry.Level != zapcore.InfoLevel || (entry.Message != "Beat info" && entry.Message != "Build info") {
			continue
		}
		systemInfo, ok := entry.Fields["system_info"].(map[string]interface{})
		if !ok {
			continue
		}
		for k, info := range systemInfo {
			switch k {
			case "beat":
				beatInfo := info.(map[string]interface{})
				s.BeatUUID = beatInfo["uuid"].(string)
			case "build":
				buildInfo := info.(map[string]interface{})
				s.Version = buildInfo["version"].(string)
			}
		}
		if s.BeatUUID != "" && s.Version != "" {
			break
		}
	}

	var elasticHTTPListeningAddr string
	for entry := range logs.C() {
		if entry.Level != zapcore.InfoLevel {
			continue
		}
		sep := strings.LastIndex(entry.Message, ": ")
		if sep == -1 {
			continue
		}
		prefix, addr := entry.Message[:sep], strings.TrimSpace(entry.Message[sep+1:])
		if prefix != "Listening on" {
			continue
		}
		if _, _, err := net.SplitHostPort(addr); err != nil {
			return fmt.Errorf("invalid listening address %q: %w", addr, err)
		}
		elasticHTTPListeningAddr = addr
		break
	}

	if elasticHTTPListeningAddr != "" {
		urlScheme := "http"
		s.URL = (&url.URL{Scheme: urlScheme, Host: elasticHTTPListeningAddr}).String()
		return nil
	}

	// Didn't find message, server probably exited...
	if err := s.Close(); err != nil {
		if err, ok := err.(*exec.ExitError); ok && err != nil {
			stderr, _ := io.ReadAll(s.Stderr)
			err.Stderr = stderr
		}
		return err
	}
	return errors.New("server exited cleanly without logging expected startup message")
}

// consumeStderr consumes the apm-server process's stderr, recording
// log entries. After any errors occur decoding log entries, remaining
// stderr is available through s.Stderr.
func (s *Server) consumeStderr(procStderr io.Reader, out *io.PipeWriter) {
	type logEntry struct {
		Timestamp logpTimestamp `json:"@timestamp"`
		Message   string        `json:"message"`
		Level     zapcore.Level `json:"log.level"`
		Logger    string        `json:"log.logger"`
		Origin    struct {
			File string `json:"file.name"`
			Line int    `json:"file.line"`
		} `json:"log.origin"`
	}

	decoder := json.NewDecoder(procStderr)
	for {
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			break
		}
		var entry logEntry
		if err := json.Unmarshal(raw, &entry); err != nil {
			break
		}
		var fields map[string]interface{}
		if err := json.Unmarshal(raw, &fields); err != nil {
			break
		}
		delete(fields, "@timestamp")
		delete(fields, "log.level")
		delete(fields, "log.logger")
		delete(fields, "log.origin")
		delete(fields, "message")
		s.Logs.add(LogEntry{
			Timestamp: time.Time(entry.Timestamp),
			Logger:    entry.Logger,
			Level:     entry.Level,
			File:      entry.Origin.File,
			Line:      entry.Origin.Line,
			Message:   entry.Message,
			Fields:    fields,
		})
	}
	s.Logs.close()

	// Send the remaining stderr to s.Stderr.
	procStderr = io.MultiReader(decoder.Buffered(), procStderr)
	_, err := io.Copy(out, procStderr)
	out.CloseWithError(err)
}

// Close shuts down the server gracefully if possible, and forcefully otherwise.
//
// Close must be called in order to clean up any resources created for running
// the server. Calling Close on an unstarted server is a no-op.
func (s *Server) Close() error {
	select {
	case <-s.closeCh:
		return nil
	default:
		close(s.closeCh)
	}

	if s.cmd == nil {
		return nil
	}
	s.closeTracers()
	if s.cmd.Process == nil {
		return errors.New("apm server process not started")
	}
	if err := interruptProcess(s.cmd.Process); err != nil {
		s.cmd.Process.Kill()
	}
	if err := s.Wait(); err != nil {
		return err
	}
	// close stderr so that the consumeStderr goroutine
	// exits
	s.Stderr.Close()
	return nil
}

func (s *Server) closeTracers() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, tracer := range s.tracers {
		tracer.Close()
	}
	s.tracers = nil
}

// Kill forcefully shuts down the server.
func (s *Server) Kill() error {
	if s.cmd != nil {
		s.cmd.Process.Kill()
	}
	return s.Wait()
}

// Wait waits for the server to exit.
//
// Wait waits up to 10 seconds for the process's stderr to be closed,
// and then waits for the process to exit.
func (s *Server) Wait() error {
	if s.cmd == nil {
		return errors.New("apm-server not started")
	}

	logs := s.Logs.Iterator()
	defer logs.Close()
	deadline := time.After(10 * time.Second)
	for {
		select {
		case _, ok := <-logs.C():
			if !ok {
				return s.cmd.Wait()
			}
		case <-deadline:
			return s.cmd.Wait()
		}
	}
}
