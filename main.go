package main

import (
	"fmt"
	"time"
)

func main() {
	conn, _ := NewConnection()

	go conn.Worker()

	for i := 0; i < 5; i++ {
		err := conn.EnqueueWorker(fmt.Sprintf("task %d", i))
		if err != nil {
			return
		}
		time.Sleep(2 * time.Second)
	}

	select {}

}
