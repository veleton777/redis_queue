package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/rueidis"
	"github.com/veleton777/redis_queue/internal/app"
	"github.com/veleton777/redis_queue/internal/config"
	"github.com/veleton777/redis_queue/internal/entity"
)

func main() {
	cfg, err := config.LoadFromEnv()
	if err != nil {
		log.Fatal(err)
	}

	redisClient, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{cfg.Redis.Addr},
		SelectDB:    cfg.Redis.DB,
		Password:    cfg.Redis.Password,
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("test 1")
	now := time.Now()
	exec(cfg, redisClient, 1)
	fmt.Println(time.Since(now)) // 1m 45s

	fmt.Println("test 3")
	now = time.Now()
	exec(cfg, redisClient, 3) // 37s
	fmt.Println(time.Since(now))

	fmt.Println("test 5")
	now = time.Now()
	exec(cfg, redisClient, 5) // 21s
	fmt.Println(time.Since(now))

	fmt.Println("test 10")
	now = time.Now()
	exec(cfg, redisClient, 10) // 12s
	fmt.Println(time.Since(now))
}

func exec(cfg config.Config, rdb rueidis.Client, countWorkers int) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		time.Sleep(3 * time.Second)

		for {
			count, err := rdb.Do(
				ctx,
				rdb.B().Xlen().Key(cfg.Redis.Consumer.Queue).Build(),
			).AsInt64()
			if err != nil {
				log.Fatal(err)
			}

			if count == 0 {
				break
			}

			time.Sleep(1 * time.Second)
		}

		cancel()
	}()

	l := NewLogger()

	apps := make([]*app.App, 0, countWorkers)

	cfg.Redis.Consumer.Group = fmt.Sprintf("gr_%s", uuid.New().String())
	cfg.Redis.Consumer.Queue = fmt.Sprintf("stream_%s", uuid.New().String())

	for i := 1; i <= countWorkers; i++ {
		appl, err := app.New(cfg, l, fmt.Sprintf("consumer_%s_%d", cfg.Redis.Consumer.Group, i))
		if err != nil {
			log.Fatal(err)
		}

		apps = append(apps, appl)
	}

	go func() {
		time.Sleep(500 * time.Millisecond)
		for i := 1; i <= 1000; i++ {
			err := apps[0].ProduceMsg(ctx, entity.Message{
				ID:      "1",
				Payload: `{"id": "1", "name": "user_1", "age": 1}`,
			})
			if err != nil {
				log.Fatal(err)
			}
		}
	}()

	wg := sync.WaitGroup{}
	wg.Add(countWorkers)

	for i := 0; i < countWorkers; i++ {
		go func(i int) {
			defer wg.Done()
			err := apps[i].RunConsumer(ctx)
			if err != nil {
				log.Fatal(err)
			}
		}(i)
	}

	wg.Wait()

	fmt.Printf("success: %d\n", l.qtySuccess)
}
