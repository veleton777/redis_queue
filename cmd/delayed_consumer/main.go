package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/redis/rueidis"
)

const (
	delayedQueue = "delayed_queue"
	defaultQueue = "default_queue"
)

type app struct {
	rdb rueidis.Client
}

type task struct {
	Message string
	Score   int
}

type item struct {
}

func main() {
	ctx := context.Background()

	rdb, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{"localhost:63791"},
	})
	if err != nil {
		panic(err)
	}
	defer rdb.Close()

	a := &app{rdb: rdb}

	for i := 1; i <= 1000; i++ {
		// randNumber := int64(rand.Intn(100_000_000_000))
		randNumber := int64(rand.Intn(100_000))

		cmd := rdb.B().Zadd().Key(delayedQueue).ScoreMember().
			ScoreMember(
				float64(time.Now().UTC().UnixNano()+randNumber),
				fmt.Sprintf(`{"item": "task #%d"}`, i),
			).Build()

		err := rdb.Do(ctx, cmd).Error()
		if err != nil {
			panic(err)
		}
	}

	// return

	err = a.consumeMessages(ctx)
	if err != nil {
		panic(err)
	}
}

func (a *app) consumeMessages(ctx context.Context) error {
	f, err := os.Create("delayed_consumer:" + uuid.New().String())
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}

	defer f.Close()

	for {
		tasks, err := a.lastTasks(ctx)
		if err != nil {
			fmt.Printf("err get last tasks: %v\n", err)
			continue
		}

		for _, t := range tasks {
			err := a.moveTaskToQueue(ctx, t)
			if err != nil {
				fmt.Printf("execute task: %v\n", err)
				continue
			}

			_, err = f.WriteString(fmt.Sprintf("moved task: %v\n", t.Message))
			if err != nil {
				fmt.Printf("write to file: %v\n", err)
			}
		}

		time.Sleep(2 * time.Second)
		fmt.Println("-----------")
	}
}

func (a *app) lastTasks(ctx context.Context) ([]task, error) {
	cmd := a.rdb.B().Zrangebyscore().
		Key(delayedQueue).
		Min("0").
		Max(fmt.Sprintf("%d", time.Now().UTC().UnixNano())).
		Withscores().
		Limit(0, 10).Build()

	cmdRes := a.rdb.Do(ctx, cmd)
	if cmdRes.Error() != nil {
		return nil, fmt.Errorf("redis ZRangeByScore: %w", cmdRes.Error())
	}

	res, err := cmdRes.AsZScores()
	if err != nil {
		return nil, fmt.Errorf("redis AsZScores: %w", err)
	}

	tasks := make([]task, 0, len(res))

	for _, r := range res {
		tasks = append(tasks, task{
			Message: r.Member,
			Score:   int(r.Score),
		})
	}

	return tasks, nil
}

func (a *app) moveTaskToQueue(ctx context.Context, t task) error {
	err := a.rdb.Do(
		ctx,
		a.rdb.B().Xadd().Key(defaultQueue).
			Id("*").FieldValue().FieldValue("data", t.Message).Build(),
	).Error()
	if err != nil {
		return fmt.Errorf("redis xAdd: %w", err)
	}

	err = a.rdb.Do(
		ctx,
		a.rdb.B().Zrem().Key(delayedQueue).Member(t.Message).Build(),
	).Error()
	if err != nil {
		return fmt.Errorf("redis zRem: %w", err)
	}

	return nil
}
