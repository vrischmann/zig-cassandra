// A generated module for ZigCassandra functions
//
// This module has been generated via dagger init and serves as a reference to
// basic module structure as you get started with Dagger.
//
// Two functions have been pre-created. You can modify, delete, or add to them,
// as needed. They demonstrate usage of arguments and return types using simple
// echo and grep commands. The functions can be called from the dagger CLI or
// from one of the SDKs.
//
// The first line in this comment block is a short description line and the
// rest is a long description with more detail on the module's purpose or usage,
// if appropriate. All modules should have a short description.

package main

import (
	"context"
	"dagger/zig-cassandra/internal/dagger"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type ZigCassandra struct{}

// Returns a Cassandra service
func (m *ZigCassandra) Cassandra(ctx context.Context,
	// +optional
	// +default="4.1"
	version string,
) *dagger.Service {
	ctr := dag.Container().
		From("cassandra:"+version).
		WithMountedCache("/var/lib/cassandra", dag.CacheVolume("cassandra-data")).
		WithExposedPort(9042)

	return ctr.AsService()
}

type ZigMaster struct {
	FileName string
	Tarball  string
}

func getZigMaster(ctx context.Context, platform string) (ZigMaster, error) {
	resp, err := http.Get("https://ziglang.org/download/index.json")
	if err != nil {
		return ZigMaster{}, fmt.Errorf("unable to download zig download index, err: %w", err)
	}
	defer resp.Body.Close()

	jsonData, err := io.ReadAll(resp.Body)
	if err != nil {
		return ZigMaster{}, fmt.Errorf("unable to read zig download index, err: %w", err)
	}

	data := make(map[string]any)
	if err := json.Unmarshal(jsonData, &data); err != nil {
		return ZigMaster{}, fmt.Errorf("invalid download index JSON data, err: %w", err)
	}

	masterData := data["master"].(map[string]any)
	tarball := masterData[platform].(map[string]any)["tarball"].(string)

	fileName := tarball[len("https://ziglang.org/builds/") : len(tarball)-len(".tar.xz")]

	return ZigMaster{
		FileName: fileName,
		Tarball:  tarball,
	}, nil
}

func platformToZigPlatform(platform dagger.Platform) (string, error) {
	switch platform {
	case "linux/amd64":
		return "x86_64-linux", nil
	default:
		return "", fmt.Errorf("invalid platform %q", platform)
	}
}

func (m *ZigCassandra) Zig(ctx context.Context,
	// +optional
	// +default="linux/amd64"
	platform dagger.Platform,
) (*dagger.Container, error) {
	zigPlatform, err := platformToZigPlatform(platform)
	if err != nil {
		return nil, err
	}

	zigMaster, err := getZigMaster(ctx, zigPlatform)
	if err != nil {
		return nil, err
	}

	//

	ctr := dag.Container().
		From("debian:bookworm-slim").
		WithWorkdir("/app").
		WithMountedCache("/root/.cache/zig", dag.CacheVolume("root-zig-cache")).
		WithExec([]string{"apt-get", "update"}).
		WithExec([]string{"apt-get", "install", "-y", "curl", "xz-utils"}).
		WithExec([]string{"curl", "-J", "-o", "zig.tar.xz", zigMaster.Tarball}).
		WithExec([]string{"tar", "xJf", "zig.tar.xz"}).
		WithExec([]string{"mv", zigMaster.FileName, "zig-master"})

	return ctr, nil
}

func (m *ZigCassandra) Test(ctx context.Context,
	src *dagger.Directory,
	// +optional
	// +default="linux/amd64"
	platform dagger.Platform,
) (string, error) {
	cassandraSvc := m.Cassandra(ctx, "4.1")

	zigCtr, err := m.Zig(ctx, platform)
	if err != nil {
		return "", err
	}

	return zigCtr.
		WithServiceBinding("cassandra", cassandraSvc).
		WithWorkdir("/src").
		WithDirectory("/src", src).
		WithMountedCache("/src/zig-cache", dag.CacheVolume("src-zig-cache")).
		WithExec([]string{"/app/zig-master/zig", "build", "test", "--summary", "all"}).
		Stdout(ctx)
}
