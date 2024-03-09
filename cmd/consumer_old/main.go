package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const stream = "userRegistered"

func main() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:63791",
		Password: "",
		DB:       0,
	})

	err := consumeMessages(ctx, rdb)
	if err != nil {
		panic(err)
	}
}

func consumeMessages(ctx context.Context, rdb *redis.Client) error {
	f, err := os.Create("consumer:" + uuid.New().String())
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}

	defer f.Close()

	id := "0"
	for {

		data, err := rdb.XRead(ctx, &redis.XReadArgs{
			Streams: []string{stream, id},
			Count:   1,
			Block:   0,
		}).Result()
		if err != nil {
			return fmt.Errorf("redis XRead: %w", err)
		}
		/*rdb.XAck()
		data, err := rdb.XRangeN(ctx, stream, "-", "+", 5).Result()
		if err != nil {
			return fmt.Errorf("redis xRangeN: %w", err)
		}*/

		/*for _, message := range data {
			b, err := json.Marshal(message.Values)
			if err != nil {
				return fmt.Errorf("json marshal: %w", err)
			}

			fmt.Println(string(b))

			id = message.ID

			time.Sleep(1 * time.Second)

			rdb.XDel(ctx, stream, id)
			_, err = f.WriteString(fmt.Sprintf("%s, %s\n", id, time.Now().Format("2006-02-01 15:04:05")))
			if err != nil {
				return fmt.Errorf("write to file: %w", err)
			}
		}*/

		for _, result := range data {
			for _, message := range result.Messages {
				b, err := json.Marshal(message.Values)
				if err != nil {
					return fmt.Errorf("json marshal: %w", err)
				}

				fmt.Println(string(b))

				id = message.ID

				time.Sleep(1 * time.Second)

				rdb.XDel(ctx, stream, id)
				_, err = f.WriteString(fmt.Sprintf("%s, %s\n", id, time.Now().Format("2006-02-01 15:04:05")))
				if err != nil {
					return fmt.Errorf("write to file: %w", err)
				}
			}
		}
	}
}
