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
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"time"

	"github.com/docker/docker/client"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/api/types/network"
)

const (
	startContainersTimeout = 5 * time.Minute
)

var (
	systemtestDir string
)

func initContainers() {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		panic("could not locate systemtest directory")
	}
	systemtestDir = filepath.Dir(filename)
}

func StartStackContainers() error {
	cmd := exec.Command(
		"docker", "compose", "-f", "../docker-compose.yml",
		"up", "-d", "elasticsearch", "kibana",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), startContainersTimeout)
	defer cancel()

	err := waitContainerHealthy(ctx, "elasticsearch")
	if err != nil {
		return  err
	}

	err = waitContainerHealthy(ctx, "kibana")
	if err != nil {
		return  err
	}

	time.Sleep(30 * time.Second)

	return nil
}

func StopStackContainer() error {
	if err := stopContainer("elasticsearch"); err != nil {
		return nil
	}
	if err := stopContainer("kibana"); err != nil {
		return nil
	}
	return nil
}

func stopContainer(name string) error {
	ctx := context.Background()

	cli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		return err
	}
	defer cli.Close()
	cli.NegotiateAPIVersion(ctx)

	c, err := stackContainerInfo(ctx, cli, name)
	if err != nil {
		return err
	}

	if err := cli.ContainerStop(ctx, c.ID, container.StopOptions{}); err != nil {
		return fmt.Errorf("stopping container: %w", err)
	}

	if err := cli.ContainerRemove(ctx, c.ID, container.RemoveOptions{Force: true}); err != nil {
		return fmt.Errorf("stopping container: %w", err)
	}

	return nil
}

func waitContainerHealthy(ctx context.Context, serviceName string) error {
	docker, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		return err
	}
	defer docker.Close()
	docker.NegotiateAPIVersion(ctx)

	container, err := stackContainerInfo(ctx, docker, serviceName)
	if err != nil {
		return err
	}

	t := time.NewTicker(15000 * time.Millisecond)
	defer t.Stop()

	first := true
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			containerJSON, err := docker.ContainerInspect(ctx, container.ID)
			if err != nil {
				return err
			}
			if containerJSON.State.Health.Status == "healthy" {
				log.Printf("Container %s  Healthy", serviceName)
				return nil
			}

			if first {
				log.Printf("Waiting for %s container (%s) to become healthy", serviceName, container.ID)
				first = false
			}
		}
	}
}

func stackContainerInfo(ctx context.Context, docker *client.Client, name string) (*types.Container, error) {
	containers, err := docker.ContainerList(ctx, container.ListOptions{
		Filters: filters.NewArgs(
			filters.Arg("label", "com.docker.compose.project=apm-server"),
			filters.Arg("label", "com.docker.compose.service="+name),
		),
	})
	if err != nil {
		return nil, err
	}
	if n := len(containers); n != 1 {
		return nil, fmt.Errorf("expected 1 %s container, got %d", name, n)
	}
	return &containers[0], nil
}

func ToggleGeoIpMount(ctx context.Context, enable bool) error {

	src := filepath.Join(
		systemtestDir,
		"..",
		"testing",
		"docker",
		"elasticsearch",
		"ingest-geoip",
	)
	dest := "/usr/share/elasticsearch/config/ingest-geoip"

	cli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		return err
	}
	defer cli.Close()
	cli.NegotiateAPIVersion(ctx)

	c, err := stackContainerInfo(ctx, cli, "elasticsearch")
	if err != nil {
		return err
	}

	inspect, err := cli.ContainerInspect(ctx, c.ID)
	if err != nil {
		return err
	}

	var newMounts []mount.Mount
	mntExists := false
	for _, m := range inspect.Mounts {
		if m.Type == "bind" && m.Destination == dest {
			mntExists = true
			if enable {
				newMounts = append(newMounts, mount.Mount{
					Type:     "bind",
					Source:   src,
					Target:   dest,
					ReadOnly: !m.RW,
				})
			}
			continue
		}
		newMounts = append(newMounts, mount.Mount{
			Type:     mount.Type(m.Type),
			Source:   m.Source,
			Target:   m.Destination,
			ReadOnly: !m.RW,
		})
	}

	if enable && !mntExists {
		newMounts = append(newMounts, mount.Mount{
			Type:   "bind",
			Source: src,
			Target: dest,
		})
	}

	if err := cli.ContainerStop(ctx, c.ID, container.StopOptions{}); err != nil {
		return fmt.Errorf("stopping container: %w", err)
	}

	if err := cli.ContainerRemove(ctx, c.ID, container.RemoveOptions{Force: true}); err != nil {
		return fmt.Errorf("stopping container: %w", err)
	}

	netCfg := &network.NetworkingConfig{
		EndpointsConfig: inspect.NetworkSettings.Networks,
	}

	//type HostConfig struct {
	//	// Applicable to all platforms
	//	ContainerIDFile string            // File (path) where the containerId is written
	//	AutoRemove      bool              // Automatically remove container when it exits
	//	VolumeDriver    string            // Name of the volume driver used to mount volumes
	//	ConsoleSize     [2]uint           // Initial console size (height,width)
	//	Annotations     map[string]string `json:",omitempty"` // Arbitrary non-identifying metadata attached to container and provided to the runtime
	//
	//	// Applicable to UNIX platforms
	//	CgroupnsMode    CgroupnsMode      // Cgroup namespace mode to use for the container
	//	DNS             []string          `json:"Dns"`        // List of DNS server to lookup
	//	DNSOptions      []string          `json:"DnsOptions"` // List of DNSOption to look for
	//	DNSSearch       []string          `json:"DnsSearch"`  // List of DNSSearch to look for
	//	ExtraHosts      []string          // List of extra hosts
	//	IpcMode         IpcMode           // IPC namespace to use for the container
	//	Cgroup          CgroupSpec        // Cgroup to use for the container
	//	Links           []string          // List of links (in the name:alias form)
	//	OomScoreAdj     int               // Container preference for OOM-killing
	//	PidMode         PidMode           // PID namespace to use for the container
	//	PublishAllPorts bool              // Should docker publish all exposed port for the container
	//	ReadonlyRootfs  bool              // Is the container root filesystem in read-only
	//	SecurityOpt     []string          // List of string values to customize labels for MLS systems, such as SELinux.
	//	StorageOpt      map[string]string `json:",omitempty"` // Storage driver options per container.
	//	Tmpfs           map[string]string `json:",omitempty"` // List of tmpfs (mounts) used for the container
	//	UTSMode         UTSMode           // UTS namespace to use for the container
	//	Runtime         string            `json:",omitempty"` // Runtime to use with this container
	//
	//	// Applicable to Windows
	//	Isolation Isolation // Isolation technology of the container (e.g. default, hyperv)
	//
	//	// Contains container's resources (cgroups, ulimits)
	//
	//	// Mounts specs used by the container
	//
	//	// MaskedPaths is the list of paths to be masked inside the container (this overrides the default set of paths)
	//	MaskedPaths []string
	//
	//	// ReadonlyPaths is the list of paths to be set as read-only inside the container (this overrides the default set of paths)
	//	ReadonlyPaths []string
	//
	//	// Run a custom init inside the container, if null, use the daemon's configured settings
	//	Init *bool `json:",omitempty"`
	//}

	createResp, err := cli.ContainerCreate(ctx, &container.Config{
		Image:        inspect.Config.Image,
		Cmd:          inspect.Config.Cmd,
		Entrypoint:   inspect.Config.Entrypoint,
		Healthcheck:  inspect.Config.Healthcheck,
		Env:          inspect.Config.Env,
		Labels:       inspect.Config.Labels,
		ExposedPorts: inspect.Config.ExposedPorts,
		User:         inspect.Config.User,
		StopTimeout:  inspect.Config.StopTimeout,
		Hostname:     inspect.Config.Hostname,
		Domainname:   inspect.Config.Domainname,
		Shell:        inspect.Config.Shell,
		Volumes:      inspect.Config.Volumes,
		WorkingDir:   inspect.Config.WorkingDir,
		OnBuild:      inspect.Config.OnBuild,
		Tty:          inspect.Config.Tty,
	}, &container.HostConfig{
		StorageOpt:      inspect.HostConfig.StorageOpt,
		ContainerIDFile: inspect.HostConfig.ContainerIDFile,
		Binds:           inspect.HostConfig.Binds,
		Mounts:          newMounts,
		PortBindings:    inspect.HostConfig.PortBindings,
		Resources:       inspect.HostConfig.Resources,
		NetworkMode:     inspect.HostConfig.NetworkMode,
		RestartPolicy:   inspect.HostConfig.RestartPolicy,
		CapAdd:          inspect.HostConfig.CapAdd,
		CapDrop:         inspect.HostConfig.CapDrop,
		Privileged:      inspect.HostConfig.Privileged,
		GroupAdd:        inspect.HostConfig.GroupAdd,
		UsernsMode:      inspect.HostConfig.UsernsMode,
		ShmSize:         inspect.HostConfig.ShmSize,
		Sysctls:         inspect.HostConfig.Sysctls,
		LogConfig:       inspect.HostConfig.LogConfig,
		VolumesFrom:     inspect.HostConfig.VolumesFrom,
	}, netCfg, nil, inspect.Name)
	if err != nil {
		return fmt.Errorf("creating new container: %w", err)
	}

	if err := cli.ContainerStart(ctx, createResp.ID, container.StartOptions{}); err != nil {
		return fmt.Errorf("starting new container: %w", err)
	}

	kb, err := stackContainerInfo(ctx, cli, "kibana")
	if err != nil {
		return err
	}

	if err := cli.ContainerRestart(ctx, kb.ID, container.StopOptions{Timeout: nil}); err != nil {
		return fmt.Errorf("starting kibana container: %w", err)
	}

	fmt.Printf("Container recreated (Id: %s) with GeoIp mount enabled. Kibana restarted: %v\n", createResp.ID[:5], enable)

	return waitContainerHealthy(ctx, "kibana")
}
