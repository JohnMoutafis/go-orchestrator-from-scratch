package task

import (
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/google/uuid"
)

// Task State
type State int

const (
	Pending State = iota
	Scheduled
	Running
	Completed
	Stopped
	Failed
)

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
	// Define retry policy on failure
	RestartPolicy string
	// Running time monitoring
	StartTime  time.Time
	FinishTime time.Time
}

// Task Event definition
type TaskEvent struct {
	ID        uuid.UUID
	Timestamp time.Time
	State     State
	Task      Task
}
