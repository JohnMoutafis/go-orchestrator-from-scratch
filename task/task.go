package task

import (
	"io"
	"log"
	"math"
	"os"
	"time"

	"context"

	"slices"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/google/uuid"
	"github.com/moby/moby/pkg/stdcopy"
)

/**
* Task State and State Machine
 */
// State Definition
type State int

const (
	Pending State = iota
	Scheduled
	Running
	Completed
	Stopped
	Failed
)

func (s State) String() []string {
	return []string{"Pending", "Scheduled", "Running", "Completed", "Failed"}
}

// State Machine
var stateTransitionMap = map[State][]State{
	Pending:   {Scheduled},
	Scheduled: {Scheduled, Running, Failed},
	Running:   {Running, Completed, Failed},
	Completed: {},
	Failed:    {},
}

func ValidStateTransition(src State, dst State) bool {
	return slices.Contains(stateTransitionMap[src], dst)
}

/**
* Task
 */
// Task definition
type Task struct {
	ID          uuid.UUID
	ContainerID string
	Name        string
	State       State
	Image       string
	// Resources
	Cpu    float64
	Memory int64
	Disk   int64
	// Networking for Docker images
	ExposedPorts nat.PortSet
	PortBindings map[string]string
	HostPorts    nat.PortMap
	// Define retry policy on failure
	RestartPolicy container.RestartPolicy
	// Running time monitoring
	StartTime  time.Time
	FinishTime time.Time
	// Health checks and restarts
	HealthCheck  string
	RestartCount int
}

// Task Event definition
type TaskEvent struct {
	ID        uuid.UUID
	Timestamp time.Time
	State     State
	Task      Task
}

/**
* Dockerize the Task.
* Allows running the Task as a Docker image, utilizing Docker Go SDK
 */
// Docker config
type Config struct {
	Name  string
	Image string
	// Attach std in/out/error
	AttachStdin  bool
	AttachStdout bool
	AttachStderr bool
	// Set of exposed ports
	ExposedPorts nat.PortSet
	// Custom command
	Cmd []string
	// Resources
	Cpu    float64
	Memory int64
	Disk   int64
	// Env vars
	Env []string
	// Restart container policy
	RestartPolicy container.RestartPolicy
}

func NewConfig(t *Task) *Config {
	return &Config{
		Name:          t.Name,
		ExposedPorts:  t.ExposedPorts,
		Image:         t.Image,
		Cpu:           t.Cpu,
		Memory:        t.Memory,
		Disk:          t.Disk,
		RestartPolicy: t.RestartPolicy,
	}
}

// Docker encapsulation
type Docker struct {
	// Docker SDK client
	Client *client.Client
	// Config instance
	Config Config
}

func NewDocker(c *Config) *Docker {
	// Fix "Error response from daemon: client version 1.48 is too new. Maximum supported API version is 1.47"
	dc, _ := client.NewClientWithOpts(client.WithVersion("1.47"))
	return &Docker{
		Client: dc,
		Config: *c,
	}
}

// Docker Task result
type DockerResult struct {
	Error       error
	Action      string
	ContainerID string
	Result      string
}

// --------------------------------
// Container administration methods
// --------------------------------

// Create and Start container
func (d *Docker) Run() DockerResult {
	ctx := context.Background()
	reader, err := d.Client.ImagePull(ctx, d.Config.Image, image.PullOptions{})
	if err != nil {
		log.Printf("Error pulling image %s: %v\n", d.Config.Image, err)
		return DockerResult{Error: err}
	}
	io.Copy(os.Stdout, reader)

	r := container.Resources{
		Memory:   d.Config.Memory,
		NanoCPUs: int64(d.Config.Cpu * math.Pow(10, 9)),
	}
	cc := container.Config{
		Image:        d.Config.Image,
		Tty:          false,
		Env:          d.Config.Env,
		ExposedPorts: d.Config.ExposedPorts,
	}
	hc := container.HostConfig{
		RestartPolicy:   d.Config.RestartPolicy,
		Resources:       r,
		PublishAllPorts: true,
	}

	// Attempt to create the container
	resp, err := d.Client.ContainerCreate(ctx, &cc, &hc, nil, nil, d.Config.Name)
	if err != nil {
		log.Printf("Error creating container using image %s: %v\n", d.Config.Image, err)
		return DockerResult{Error: err}
	}
	// Attempt to start the container
	err = d.Client.ContainerStart(ctx, resp.ID, container.StartOptions{})
	if err != nil {
		log.Printf("Error starting container %s: %v\n", resp.ID, err)
		return DockerResult{Error: err}
	}
	// Attempt to fetch the Container logs
	out, err := d.Client.ContainerLogs(ctx, resp.ID, container.LogsOptions{ShowStdout: true, ShowStderr: true})
	if err != nil {
		log.Printf("Error getting logs for container %s: %v\n", resp.ID, err)
		return DockerResult{Error: err}
	}

	stdcopy.StdCopy(os.Stdout, os.Stderr, out)

	return DockerResult{ContainerID: resp.ID, Action: "start", Result: "success"}
}

// Stop and Remove container
func (d *Docker) Stop(id string) DockerResult {
	log.Printf("Attempting to stop container %v", id)
	ctx := context.Background()
	err := d.Client.ContainerStop(ctx, id, container.StopOptions{})
	if err != nil {
		log.Printf("Error stopping container %s: %v\n", id, err)
		return DockerResult{Error: err}
	}
	// Attempt to Remove the container
	err = d.Client.ContainerRemove(ctx, id, container.RemoveOptions{
		RemoveVolumes: true,
		RemoveLinks:   false,
		Force:         false,
	})
	if err != nil {
		log.Printf("Error removing container %s: %v\n", id, err)
		return DockerResult{Error: err}
	}
	return DockerResult{Action: "stop", Result: "success", Error: nil}
}

// Inspect a container
type DockerInspectResponse struct {
	Error     error
	Container *container.InspectResponse
}

func (d *Docker) Inspect(containerID string) DockerInspectResponse {
	dc, _ := client.NewClientWithOpts(client.WithVersion("1.47"))
	ctx := context.Background()
	resp, err := dc.ContainerInspect(ctx, containerID)
	if err != nil {
		log.Printf("Error inspecting container: %s\n", err)
		return DockerInspectResponse{Error: err}
	}

	return DockerInspectResponse{Container: &resp}
}
