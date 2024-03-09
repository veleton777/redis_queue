package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const stream = "userRegistered"

type App struct {
	redisClient *redis.Client
}

type Message struct {
	ID      uuid.UUID
	Content string
}

type User struct {
	ID     uuid.UUID
	Name   string
	Number int
}

func New(client *redis.Client) *App {
	return &App{
		redisClient: client,
	}
}

func main() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:63791",
		Password: "",
		DB:       0,
	})

	app := New(rdb)

	for i := 1; i <= 100; i++ {
		user := User{
			ID:     uuid.New(),
			Name:   fmt.Sprintf("User №%d", i),
			Number: i,
		}
		err := app.produceUser(ctx, user)
		if err != nil {
			panic(err)
		}
	}
}

func (a *App) produceUser(ctx context.Context, user User) error {
	b, err := json.Marshal(user)
	if err != nil {
		return fmt.Errorf("json marshal: %w", err)
	}

	var res map[string]any
	err = json.Unmarshal(b, &res)
	if err != nil {
		return fmt.Errorf("json unmarshal to map[string]any: %w", err)
	}

	err = a.redisClient.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: res,
	}).Err()
	if err != nil {
		return fmt.Errorf("redis XAdd res: %w", err)
	}

	return nil
}
