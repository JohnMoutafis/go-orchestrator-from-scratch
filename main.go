package main

import (
	"fmt"
	"os"
	"strconv"

	"cube/manager"
	managerApi "cube/manager/api"
	"cube/worker"
	workerApi "cube/worker/api"
)

func main() {
	whost := os.Getenv("CUBE_WORKER_HOST")
	wport, _ := strconv.Atoi(os.Getenv("CUBE_WORKER_PORT"))

	mhost := os.Getenv("CUBE_MANAGER_HOST")
	mport, _ := strconv.Atoi(os.Getenv("CUBE_MANAGER_PORT"))

	fmt.Println("Starting Cube worker")

	const storage_type = "persistent"
	w1 := worker.New("worker-1", storage_type)
	wapi1 := workerApi.Api{Address: whost, Port: wport, Worker: w1}

	w2 := worker.New("worker-2", storage_type)
	wapi2 := workerApi.Api{Address: whost, Port: wport + 1, Worker: w2}

	w3 := worker.New("worker-3", storage_type)
	wapi3 := workerApi.Api{Address: whost, Port: wport + 2, Worker: w3}

	go w1.RunTasks()
	go w1.UpdateTasks()
	go wapi1.Start()

	go w2.RunTasks()
	go w2.UpdateTasks()
	go wapi2.Start()

	go w3.RunTasks()
	go w3.UpdateTasks()
	go wapi3.Start()

	fmt.Println("Starting Cube manager")

	workers := []string{
		fmt.Sprintf("%s:%d", whost, wport),
		fmt.Sprintf("%s:%d", whost, wport+1),
		fmt.Sprintf("%s:%d", whost, wport+2),
	}
	//m := manager.New(workers, "greedy")
	m := manager.NewManager(workers, "epvm", storage_type)
	mapi := managerApi.Api{Address: mhost, Port: mport, Manager: m}

	go m.ProcessTasks()
	go m.UpdateTasks()
	go m.DoHealthChecks()
	//go m.UpdateNodeStats()
	mapi.Start()
}
