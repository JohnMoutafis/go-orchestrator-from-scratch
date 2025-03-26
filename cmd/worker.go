/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"
	"log"

	"github.com/google/uuid"
	"github.com/spf13/cobra"

	"cube/worker"
	workerApi "cube/worker/api"
)

func init() {
	rootCmd.AddCommand(workerCmd)
	workerCmd.Flags().StringP("host", "H", "0.0.0.0", "Hostname or IP address")
	workerCmd.Flags().IntP("port", "p", 5556, "Port on which to listen")
	workerCmd.Flags().StringP("name", "n", fmt.Sprintf("worker-%s", uuid.New().String()), "Name of the worker")
	workerCmd.Flags().StringP("dbtype", "d", "memory", "Type of datastore to use for tasks (\"memory\" or \"persistent\")")
}

// workerCmd represents the worker command
var workerCmd = &cobra.Command{
	Use:   "worker",
	Short: "Cube Worker node CLI",
	Long:  `The Cube Worker is responsible for running Cube Tasks and inform a Cube Manager about Task state.`,
	Run: func(cmd *cobra.Command, args []string) {
		host, _ := cmd.Flags().GetString("host")
		port, _ := cmd.Flags().GetInt("port")
		name, _ := cmd.Flags().GetString("name")
		dbType, _ := cmd.Flags().GetString("dbtype")

		log.Println("Starting worker.")
		w := worker.New(name, dbType)
		api := workerApi.Api{Address: host, Port: port, Worker: w}
		go w.RunTasks()
		go w.CollectStats()
		go w.UpdateTasks()
		log.Printf("Starting worker API on http://%s:%d", host, port)
		api.Start()
	},
}
