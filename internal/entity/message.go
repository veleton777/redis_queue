package entity

import "time"

type User struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Age  int    `json:"age"`
}

type Message struct {
	ID      string `json:"id"`
	Payload string `json:"payload"`
}

type GetMessagesDTO struct {
	ConsumerID string
	BlockTime  time.Duration
	Queue      string
	Group      string
	Limit      int
}

type GetFailedMessagesDTO struct {
	ConsumerID         string
	Queue              string
	Group              string
	IdleTimeForMessage time.Duration
	Limit              int
}
