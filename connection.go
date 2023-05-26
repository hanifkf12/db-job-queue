package main

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"time"
)

type Connection struct {
	db *sql.DB
}

func NewConnection() (*Connection, error) {
	host := ""
	dbName := ""
	pass := ""
	username := ""
	connectionPattern := fmt.Sprintf("postgres://%s/%s?password=%s&port=5432&sslmode=disable&user=%s", host, dbName, pass, username)
	db, err := sql.Open("postgres", connectionPattern)
	if err != nil {
		// This will not be a connection error, but a DSN parse error or
		// another initialization error.
		return nil, err
	}
	db.SetConnMaxLifetime(0)
	db.SetMaxIdleConns(50)
	db.SetMaxOpenConns(50)
	return &Connection{
		db: db,
	}, nil
}

func (c Connection) EnqueueWorker(data string) error {
	query := `INSERT INTO queue_worker(value, is_done, created_at, updated_at) VALUES ($1, false, now(), now());`
	_, err := c.db.Exec(query, data)
	if err != nil {
		return err
	}
	return nil
}

func (c Connection) Worker() {
	query := `SELECT id,value FROM queue_worker WHERE is_done=false ORDER BY id ASC LIMIT 1;`
	for {
		var value string
		var id int
		row := c.db.QueryRow(query)
		err := row.Scan(&id, &value)
		if err != nil {
			//fmt.Println(err)
			continue
		}

		fmt.Println("runing task ")
		fmt.Println(value)

		queryDone := `UPDATE queue_worker SET is_done = true WHERE id = $1`

		_, err = c.db.Exec(queryDone, id)
		if err != nil {
			//fmt.Println(err)
			continue
		}
		time.Sleep(10 * time.Second)
	}

}
