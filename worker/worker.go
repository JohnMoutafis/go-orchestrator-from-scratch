package worker

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/golang-collections/collections/queue"

	"cube/stats"
	"cube/store"
	"cube/task"
)

type Worker struct {
	Name      string
	Queue     queue.Queue
	Db        store.Store
	TaskCount int
	Stats     *stats.Stats
}

func New(name string, taskDbType string) *Worker {
	w := Worker{
		Name:  name,
		Queue: *queue.New(),
	}

	var s store.Store
	var err error
	switch taskDbType {
	case "memory":
		s = store.NewInMemoryTaskStore()
	case "persistent":
		filename := fmt.Sprintf("%s_tasks.db", name)
		s, err = store.NewTaskStore(filename, 0600, "tasks")
	}

	if err != nil {
		log.Printf("Unable to create new task store: %v", err)
	}
	w.Db = s
	return &w
}

func (w *Worker) CollectStats() {
	for {
		log.Println("Collecting stats")
		w.Stats = stats.GetStats()
		w.Stats.TaskCount = w.TaskCount
		time.Sleep(15 * time.Second)
	}
}

func (w *Worker) GetTasks() []*task.Task {
	tasks, err := w.Db.List()
	if err != nil {
		log.Printf("Error getting list of tasks: %v\n", err)
		return nil
	}
	return tasks.([]*task.Task)
}

func (w *Worker) AddTask(t task.Task) {
	w.Queue.Enqueue(t)
}

func (w *Worker) RunTasks() {
	for {
		if w.Queue.Len() != 0 {
			result := w.RunTask()
			if result.Error != nil {
				log.Printf("Error running task: %v\n", result.Error)
			}
		} else {
			log.Printf("No tasks to process currently.\n")
		}
		log.Println("Sleeping for 10 seconds.")
		time.Sleep(10 * time.Second)
	}

}

func (w *Worker) RunTask() task.DockerResult {
	t := w.Queue.Dequeue()
	if t == nil {
		log.Println("No tasks in the queue")
		return task.DockerResult{Error: nil}
	}

	taskQueued := t.(task.Task)
	fmt.Printf("Found task in queue: %v:\n", taskQueued)

	err := w.Db.Put(taskQueued.ID.String(), &taskQueued)
	if err != nil {
		msg := fmt.Errorf("error storing task '%s': %v", taskQueued.ID.String(), err)
		log.Println(msg)
		return task.DockerResult{Error: err}
	}

	res, err := w.Db.Get(taskQueued.ID.String())
	if err != nil {
		msg := fmt.Errorf("error getting task '%s': %v", taskQueued.ID.String(), err)
		log.Println(msg)
		return task.DockerResult{Error: err}
	}

	taskPersisted := *res.(*task.Task)
	if taskPersisted.State == task.Completed {
		return w.StopTask(taskPersisted)
	}

	var result task.DockerResult
	if task.ValidStateTransition(taskPersisted.State, taskQueued.State) {
		switch taskQueued.State {
		case task.Scheduled:
			result = w.StartTask(taskQueued)
		case task.Completed:
			result = w.StopTask(taskQueued)
		default:
			fmt.Printf("This is a mistake. taskPersisted: %v, taskQueued: %v\n", taskPersisted, taskQueued)
			result.Error = errors.New("we should not get here")
		}
	} else {
		err := fmt.Errorf("invalid transition from %v to %v", taskPersisted.State, taskQueued.State)
		result.Error = err
		return result
	}
	return result
}

func (w *Worker) StartTask(t task.Task) task.DockerResult {
	t.StartTime = time.Now().UTC()
	config := task.NewConfig(&t)
	d := task.NewDocker(config)

	result := d.Run()
	if result.Error != nil {
		log.Printf("Error running task %v: %v\n", t.ID, result.Error)
		t.State = task.Failed
	} else {
		t.ContainerID = result.ContainerID
		t.State = task.Running
	}
	w.Db.Put(t.ID.String(), &t)
	return result
}

func (w *Worker) StopTask(t task.Task) task.DockerResult {
	config := task.NewConfig(&t)
	d := task.NewDocker(config)

	result := d.Stop(t.ContainerID)
	if result.Error != nil {
		log.Printf("Error stopping container %v: %v\n", t.ContainerID, result.Error)
	}
	t.FinishTime = time.Now().UTC()
	t.State = task.Completed
	w.Db.Put(t.ID.String(), &t)
	log.Printf("Stopped and removed container %v for task %v\n", t.ContainerID, t.ID)
	return result
}

func (w *Worker) InspectTask(t task.Task) task.DockerInspectResponse {
	config := task.NewConfig(&t)
	d := task.NewDocker(config)
	return d.Inspect(t.ContainerID)
}

func (w *Worker) UpdateTasks() {
	for {
		log.Println("Checking status of tasks")
		w.updateTasks()
		log.Println("Task updates completed")
		log.Println("Sleeping for 15 seconds")
		time.Sleep(15 * time.Second)
	}
}

/**
* For each task in the worker's datastore:
* 1. Call InspectTask method
* 2. Verify task is in running state
* 3. If a task is in any other state than running, mark it as failed
 */
func (w *Worker) updateTasks() {
	tasks, err := w.Db.List()
	if err != nil {
		log.Printf("Error getting list of Tasks: %v\n", err)
		return
	}

	for _, t := range tasks.([]*task.Task) {
		if t.State == task.Running {
			resp := w.InspectTask(*t)
			if resp.Error != nil {
				fmt.Printf("ERROR: %v\n", resp.Error)
			}

			if resp.Container == nil {
				log.Printf("No container for running task %s\n", t.ID)
				t.State = task.Failed
				w.Db.Put(t.ID.String(), t)
			}

			if resp.Container.State.Status == "exited" {
				log.Printf(
					"Container for task %s in non-running state %s",
					t.ID, resp.Container.State.Status,
				)
				t.State = task.Failed
				w.Db.Put(t.ID.String(), t)
			}

			t.HostPorts = resp.Container.NetworkSettings.NetworkSettingsBase.Ports
			w.Db.Put(t.ID.String(), t)
		}
	}
}
