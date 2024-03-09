package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/google/uuid"
	"github.com/veleton777/redis_queue/internal/app"
	"github.com/veleton777/redis_queue/internal/config"
	"github.com/veleton777/redis_queue/internal/logger"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg, err := config.LoadFromEnv()
	if err != nil {
		log.Fatal(err)
	}

	cID := fmt.Sprintf("consumer_%s_%s", cfg.Redis.Consumer.Group, uuid.New().String())

	f, err := os.Create(fmt.Sprintf("%s.log", cID))
	if err != nil {
		log.Fatal(err)
	}

	defer f.Close()

	l := logger.NewFileLogger(f)

	appl, err := app.New(cfg, l, cID)
	if err != nil {
		log.Fatal(err)
	}

	waitCh := make(chan os.Signal, 1)

	signal.Notify(waitCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-waitCh
		cancel()
	}()

	/*for i := 1; i <= 100; i++ {
		err = appl.ProduceMsg(ctx, entity.Message{
			ID:      uuid.New().String(),
			Payload: fmt.Sprintf(`{"id": "%d", "name": "user_%d", "age": %d}`, i, i, i+7),
		})
		if err != nil {
			log.Fatal(err)
		}
	}

	return*/

	err = appl.RunConsumer(ctx)
	if err != nil {
		log.Fatal(err)
	}

	if err = appl.WaitShutdown(ctx); err != nil {
		log.Fatal(err)
	}

	log.Println("app shutdown")
}
