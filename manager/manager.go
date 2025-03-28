package manager

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"

	"cube/logging"
	"cube/node"
	"cube/scheduler"
	"cube/store"
	"cube/task"
	workerApi "cube/worker/api"
)

type Manager struct {
	Pending       queue.Queue
	TaskDb        store.Store
	EventDb       store.Store
	Workers       []string
	WorkerTaskMap map[string][]uuid.UUID
	TaskWorkerMap map[uuid.UUID]string
	LastWorker    int
	WorkerNodes   []*node.Node
	Scheduler     scheduler.Scheduler
}

func New(workers []string, schedulerType string, dbType string) *Manager {
	// Constructor
	workerTaskMap := make(map[string][]uuid.UUID)
	taskWorkerMap := make(map[uuid.UUID]string)

	var nodes []*node.Node
	for worker := range workers {
		workerTaskMap[workers[worker]] = []uuid.UUID{}

		nAPI := fmt.Sprintf("http://%v", workers[worker])
		n := node.NewNode(workers[worker], nAPI, "worker")
		nodes = append(nodes, n)
	}

	var s scheduler.Scheduler
	switch schedulerType {
	case "epvm":
		s = &scheduler.Epvm{Name: "epvm"}
	case "greedy":
		s = &scheduler.Greedy{Name: "greedy"}
	default:
		s = &scheduler.RoundRobin{Name: "round-robin"}
	}

	var ts store.Store
	var es store.Store
	var err error
	switch dbType {
	case "memory":
		ts = store.NewInMemoryTaskStore()
		es = store.NewInMemoryTaskEventStore()
	case "persistent":
		ts, err = store.NewTaskStore("tasks.db", 0600, "tasks")
		if err != nil {
			logging.Error.Printf("Unable to create task store: %v", err)
		}

		es, err = store.NewTaskStore("events.db", 0600, "events")
		if err != nil {
			logging.Error.Printf("Unable to create task event store: %v", err)
		}
	}

	return &Manager{
		Pending:       *queue.New(),
		Workers:       workers,
		TaskDb:        ts,
		EventDb:       es,
		WorkerTaskMap: workerTaskMap,
		TaskWorkerMap: taskWorkerMap,
		WorkerNodes:   nodes,
		Scheduler:     s,
	}
}

func (m *Manager) SelectWorker(t task.Task) (*node.Node, error) {
	candidates := m.Scheduler.SelectCandidateNodes(t, m.WorkerNodes)
	if candidates == nil {
		msg := fmt.Sprintf("No available candidates match resource request for task %v", t.ID)
		err := errors.New(msg)
		return nil, err
	}

	scores := m.Scheduler.Score(t, candidates)
	if scores == nil {
		return nil, fmt.Errorf("no scores returned to task %v", t)
	}
	selectedNode := m.Scheduler.Pick(scores, candidates)

	return selectedNode, nil
}

func (m *Manager) AddTask(te task.TaskEvent) {
	m.Pending.Enqueue(te)
}

func (m *Manager) GetTasks() []*task.Task {
	tasks, err := m.TaskDb.List()
	if err != nil {
		logging.Error.Printf("Error getting list of tasks: %v\n", err)
		return nil
	}
	return tasks.([]*task.Task)
}

func (m *Manager) UpdateTasks() {
	for {
		logging.Info.Println("Checking for task updates from workers")
		for _, worker := range m.Workers {
			logging.Info.Printf("Checking worker %v for task updates", worker)
			url := fmt.Sprintf("http://%s/tasks", worker)
			resp, err := http.Get(url)
			if err != nil {
				logging.Error.Printf("Error connecting to %v: %v", worker, err)
				continue
			}

			if resp.StatusCode != http.StatusOK {
				logging.Error.Printf("Error sending request: %v", err)
				continue
			}

			d := json.NewDecoder(resp.Body)
			var tasks []*task.Task
			err = d.Decode(&tasks)
			if err != nil {
				logging.Error.Printf("Error unmarshalling tasks: %s", err.Error())
				continue
			}

			for _, t := range tasks {
				logging.Info.Printf("Attempting to update task %v", t.ID)

				res, err := m.TaskDb.Get(t.ID.String())
				if err != nil {
					log.Printf("%s\n", err)
					continue
				}
				taskPersisted, ok := res.(*task.Task)
				if !ok {
					logging.Error.Printf("Cannot convert result %v to task.Task type\n", res)
					continue
				}

				if taskPersisted.State != t.State {
					taskPersisted.State = t.State
				}

				taskPersisted.StartTime = t.StartTime
				taskPersisted.FinishTime = t.FinishTime
				taskPersisted.ContainerID = t.ContainerID
				taskPersisted.HostPorts = t.HostPorts
				m.TaskDb.Put(taskPersisted.ID.String(), taskPersisted)
			}
		}
		logging.Info.Println("Task updates completed")
		logging.Info.Println("Sleeping for 15 seconds")
		time.Sleep(15 * time.Second)
	}
}

func (m *Manager) ProcessTasks() {
	for {
		logging.Info.Printf("Processing any tasks in the queue")
		m.SendWork()
		logging.Info.Printf("Sleeping for 10 seconds")
		time.Sleep(10 * time.Second)
	}
}

func (m *Manager) stopTask(worker string, taskID string) {
	client := &http.Client{}
	url := fmt.Sprintf("http://%s/tasks/%s", worker, taskID)
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		logging.Error.Printf("Error creating request to delete task %s: %v", taskID, err)
		return
	}

	resp, err := client.Do(req)
	if err != nil {
		logging.Error.Printf("Error connecting to worker at %s: %v", url, err)
		return
	}

	if resp.StatusCode != 204 {
		logging.Error.Printf("Error sending request: %v", err)
		return
	}

	logging.Info.Printf("Task %s has been scheduled to be stopped", taskID)
}

func (m *Manager) SendWork() {
	if m.Pending.Len() > 0 {
		e := m.Pending.Dequeue()
		te := e.(task.TaskEvent)
		err := m.EventDb.Put(te.ID.String(), &te)
		if err != nil {
			logging.Error.Printf("Error attempting to store task event %s: %s\n", te.ID.String(), err)
			return
		}
		logging.Info.Printf("Pulled %v off pending queue", te)

		taskWorker, ok := m.TaskWorkerMap[te.Task.ID]
		if ok {
			res, err := m.TaskDb.Get(te.Task.ID.String())
			if err != nil {
				logging.Error.Printf("Unable to schedule task: %s", err)
				return
			}

			persistedTask, ok := res.(*task.Task)
			if !ok {
				logging.Error.Println("Unable to convert task to task.Task type")
				return
			}

			if te.State == task.Completed && task.ValidStateTransition(persistedTask.State, te.State) {
				m.stopTask(taskWorker, te.Task.ID.String())
				return
			}

			logging.Warning.Printf(
				"Invalid request: existing task %s is in state %v and cannot transition to the completed state",
				persistedTask.ID.String(), persistedTask.State,
			)
			return
		}

		t := te.Task
		w, err := m.SelectWorker(t)
		if err != nil {
			logging.Error.Printf("Error selecting worker for task %s: %v", t.ID, err)
			return
		}

		logging.Info.Printf("Selected worker %s for task %s", w.Name, t.ID)

		m.WorkerTaskMap[w.Name] = append(m.WorkerTaskMap[w.Name], te.Task.ID)
		m.TaskWorkerMap[t.ID] = w.Name

		t.State = task.Scheduled
		m.TaskDb.Put(t.ID.String(), &t)

		data, err := json.Marshal(te)
		if err != nil {
			logging.Warning.Printf("Unable to marshal task object: %v.", t)
		}

		url := fmt.Sprintf("http://%s/tasks", w.Name)
		resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
		if err != nil {
			logging.Error.Printf("Error connecting to %v: %v", w, err)
			m.Pending.Enqueue(t)
			return
		}

		d := json.NewDecoder(resp.Body)
		if resp.StatusCode != http.StatusCreated {
			e := workerApi.ErrResponse{}
			err := d.Decode(&e)
			if err != nil {
				logging.Error.Printf("Error decoding response: %s\n", err.Error())
				return
			}
			logging.Error.Printf("Response error (%d): %s", e.HTTPStatusCode, e.Message)
			return
		}

		t = task.Task{}
		err = d.Decode(&t)
		if err != nil {
			logging.Error.Printf("Error decoding response: %s\n", err.Error())
			return
		}
		w.TaskCount++
		logging.Info.Printf("Received response from worker: %#v\n", t)
	} else {
		logging.Info.Printf("No work in the queue")
	}
}

// Task HealthChecks and Restarts (Chapter 09)
// 1. Individual Task Health Check
func getHostPort(ports nat.PortMap) *string {
	for k, _ := range ports {
		return &ports[k][0].HostPort
	}
	return nil
}

func (m *Manager) checkTaskHealth(t task.Task) error {
	logging.Info.Printf("Calling health check for task %s: %s\n", t.ID, t.HealthCheck)

	w := m.TaskWorkerMap[t.ID]
	hostPort := getHostPort(t.HostPorts)
	worker := strings.Split(w, ":")
	if hostPort == nil {
		logging.Warning.Printf("Have not collected task %s host port yet. Skipping.\n", t.ID)
		return nil
	}

	url := fmt.Sprintf("http://%s:%s%s", worker[0], *hostPort, t.HealthCheck)
	logging.Info.Printf("Calling health check for task %s: %s\n", t.ID, url)
	resp, err := http.Get(url)
	if err != nil {
		msg := fmt.Sprintf("Error connecting to health check %s", url)
		logging.Error.Println(msg)
		return errors.New(msg)
	}

	if resp.StatusCode != http.StatusOK {
		msg := fmt.Sprintf("Error health check for task %s did not return 200\n", t.ID)
		logging.Error.Println(msg)
		return errors.New(msg)
	}

	logging.Info.Printf("Task %s health check response: %v\n", t.ID, resp.StatusCode)
	return nil
}

// 2. Health Check all the Tasks
func (m *Manager) DoHealthChecks() {
	for {
		logging.Info.Println("Performing task health check")
		m.doHealthChecks()
		logging.Info.Println("Task health checks completed")
		logging.Info.Println("Sleeping for 60 seconds")
		time.Sleep(60 * time.Second)
	}
}

func (m *Manager) doHealthChecks() {
	for _, t := range m.GetTasks() {
		if t.State == task.Running && t.RestartCount < 3 {
			err := m.checkTaskHealth(*t)
			if err != nil {
				if t.RestartCount < 3 {
					m.restartTask(t)
				}
			}
		} else if t.State == task.Failed && t.RestartCount < 3 {
			m.restartTask(t)
		}
	}
}

// 3. Restart unhealthy Tasks
func (m *Manager) restartTask(t *task.Task) {
	// Get the worker where the task was running
	w := m.TaskWorkerMap[t.ID]
	t.State = task.Scheduled
	t.RestartCount++
	// We need to overwrite the existing task to ensure it has
	// the current state
	m.TaskDb.Put(t.ID.String(), t)

	te := task.TaskEvent{
		ID:        uuid.New(),
		State:     task.Running,
		Timestamp: time.Now(),
		Task:      *t,
	}
	data, err := json.Marshal(te)
	if err != nil {
		logging.Error.Printf("Unable to marshal task object: %v.\n", t)
		return
	}

	url := fmt.Sprintf("http://%s/tasks", w)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		logging.Error.Printf("Error connecting to %v: %v\n", w, err)
		m.Pending.Enqueue(t)
		return
	}

	d := json.NewDecoder(resp.Body)
	if resp.StatusCode != http.StatusCreated {
		e := workerApi.ErrResponse{}
		err := d.Decode(&e)
		if err != nil {
			logging.Error.Printf("Error decoding response: %s\n", err.Error())
			return
		}
		logging.Error.Printf("Response error (%d): %s\n", e.HTTPStatusCode, e.Message)
		return
	}

	newTask := task.Task{}
	err = d.Decode(&newTask)
	if err != nil {
		logging.Error.Printf("Error decoding response: %s\n", err.Error())
		return
	}
	logging.Info.Printf("%#v\n", t)
}

func (m *Manager) UpdateNodeStats() {
	for {
		for _, node := range m.WorkerNodes {
			logging.Info.Printf("Collecting stats for node %v", node.Name)
			_, err := node.GetStats()
			if err != nil {
				logging.Error.Printf("Error updating node stats: %v", err)
			}
		}
		time.Sleep(15 * time.Second)
	}
}
