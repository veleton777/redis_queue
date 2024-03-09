package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const (
	queue = "delayed_tasks"

	lockTimeout = 10 * time.Second
)

type app struct {
	rdb *redis.Client
}

type task struct {
	message string
	score   int
}

func main() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:63791",
		Password: "",
		DB:       0,
	})

	a := &app{rdb: rdb}

	/*for i := 1; i <= 100; i++ {
		randNumber := int64(rand.Intn(100_000_000_000))

		rdb.ZAdd(ctx, fmt.Sprintf(queue), redis.Z{
			Score:  float64(time.Now().UTC().UnixNano() + randNumber),
			Member: fmt.Sprintf(`{"item": "task #%d"}`, i),
		})
	}

	return*/

	/*err := a.consumeMessages(ctx)
	if err != nil {
		panic(err)
	}*/

	err := a.manager(ctx)
	if err != nil {
		panic(err)
	}
}

func (a *app) manager(ctx context.Context) error {
	f, err := os.Create("consumer:" + uuid.New().String())
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}

	defer f.Close()

	/*var offset int64

	for {
		tasks, err := a.tasks(ctx, offset)
		if err != nil {
			// log error
			// continue
			return fmt.Errorf("get tasks: %w", err)
		}

		for _, t := range tasks {
			err := a.executeTask(ctx, t)
			if err != nil {
				return fmt.Errorf("execute task: %w", err)
			}
		}

		time.Sleep(2 * time.Second)
		fmt.Println("-----------")

		// todo что делать, если было много блокировок
		fmt.Println(offset)
	}*/

	return nil
}

func (a *app) consumeMessages(ctx context.Context) error {
	f, err := os.Create("consumer:" + uuid.New().String())
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}

	defer f.Close()

	var offset int64

	for {
		tasks, quantityBlocks, err := a.tasks(ctx, offset)
		if err != nil {
			// log error
			// continue
			return fmt.Errorf("get tasks: %w", err)
		}

		for _, t := range tasks {
			err := a.executeTask(ctx, t)
			if err != nil {
				return fmt.Errorf("execute task: %w", err)
			}
		}

		if len(tasks) > 0 {
			offset = 0
			err = a.ackTasks(ctx, tasks)
			if err != nil {
				return fmt.Errorf("ack tasks: %w", err)
			}
		}
		if len(tasks) == 0 && quantityBlocks > 0 {
			offset += 10
		}

		// взяли 100 записей, берем блокировки

		// глобальная очередь
		// |              |
		// локальные очереди
		// взятые в работу переносить в sorted set

		// offset дорого
		// ремонт

		// блокировка тасок
		// воркер, который ищет готовые записи и переносит в общую очередь, которая может работать с несколькими консьюмерами

		// manager консьюмеров
		// менеджер знает о всех консьюмерах, создает под них очереди
		// закидывает каждому воркеру задачи
		// если консьюмер упал, то

		time.Sleep(2 * time.Second)
		fmt.Println("-----------")

		// todo что делать, если было много блокировок
		fmt.Println(offset)
	}
}

func (a *app) tasksByOffset(ctx context.Context, offset int64) ([]task, error) {
	redisResp, err := a.rdb.ZRangeByScoreWithScores(ctx, queue, &redis.ZRangeBy{
		Max:    fmt.Sprintf("%d", time.Now().UTC().UnixNano()),
		Offset: offset,
		Count:  10,
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("redis ZRangeByScore: %w", err)
	}

	tasks := make([]task, 0, len(redisResp))

	for _, r := range redisResp {
		v, ok := r.Member.(string)
		if !ok {
			return nil, fmt.Errorf("cast redis resp to string: %w", err)
		}

		tasks = append(tasks, task{
			message: v,
			score:   int(r.Score),
		})
	}

	return tasks, nil
}

func (a *app) tasks(ctx context.Context, offset int64) ([]task, int, error) {
	tasks, err := a.tasksByOffset(ctx, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("get tasks from redis: %w", err)
	}

	cmds, err := a.rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, t := range tasks {
			key := a.lockKey(queue, t.score)
			pipe.SetNX(ctx, key, true, lockTimeout)
		}

		return nil
	})
	if err != nil {
		return nil, 0, fmt.Errorf("redis pipeline: %w", err)
	}

	unblockedTasks := make([]task, 0, len(tasks))
	var quantityBlocks int

	for k, cmd := range cmds {
		v := cmd.(*redis.BoolCmd).Val()
		if !v {
			quantityBlocks++
			continue
		}

		unblockedTasks = append(unblockedTasks, tasks[k])
	}

	return unblockedTasks, quantityBlocks, nil
}

func (a *app) executeTask(ctx context.Context, t task) error {
	fmt.Println(t)

	return nil
}

func (a *app) ackTasks(ctx context.Context, tasks []task) error {
	_, err := a.rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		lockKeysForRemove := make([]string, 0, len(tasks))
		taskKeysForRemove := make([]string, 0, len(tasks))

		for _, t := range tasks {
			lockKeysForRemove = append(lockKeysForRemove, a.lockKey(queue, t.score))
			taskKeysForRemove = append(taskKeysForRemove, t.message)
		}

		pipe.Del(ctx, lockKeysForRemove...)
		pipe.ZRem(ctx, queue, taskKeysForRemove)

		return nil
	})
	if err != nil {
		return fmt.Errorf("redis pipeline: %w", err)
	}

	return nil
}

func (a *app) lockKey(queue string, score int) string {
	return fmt.Sprintf("locks_%s_%d", queue, score)
}
