package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/rueidis"
)

const (
	queue                = "default_queue"
	defaultConsumerGroup = "default_group"

	idleTimeForFailedTask = "60000" // in milliseconds

	emptyQueueBlockTime = 1000 // in milliseconds

	limitTasksForOneIteration = 10

	checkFailedMessagesTime = time.Second * 3
)

type consumer struct {
	id  string
	rdb rueidis.Client

	logFile *os.File
}

type message struct {
	Item string `json:"item"`

	id string
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

	cID := uuid.New().String()

	f, err := os.Create("consumer:" + cID)
	if err != nil {
		err = fmt.Errorf("create file: %w", err)
		panic(err)
	}

	defer f.Close()

	a := &consumer{
		rdb:     rdb,
		id:      cID,
		logFile: f,
	}

	resp, err := a.rdb.Do(
		ctx,
		a.rdb.B().XinfoGroups().Key(queue).Build(),
	).ToMessage()
	if err != nil {
		panic(err)
	}

	if strings.Contains(resp.String(), queue) {
		err = a.xGroupCreate(ctx)
		if err != nil {
			panic(err)
		}
	}

	err = a.xGroupCreateConsumer(ctx)
	if err != nil {
		panic(err)
	}

	err = a.consumeMessages(ctx)
	if err != nil {
		panic(err)
	}
}

func (c *consumer) consumeMessages(ctx context.Context) error {
	ticker := time.NewTicker(checkFailedMessagesTime)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.executeFailedMessages(ctx)
		default:
			c.executeMessages(ctx)
		}

		time.Sleep(1 * time.Second)
		fmt.Println("-----")
	}
}

func (c *consumer) executeMessages(ctx context.Context) {
	messages, err := c.getMessages(ctx)
	if err != nil {
		fmt.Printf("get messages: %v\n", err)
		return
	}

	c.execute(ctx, messages)
}

func (c *consumer) executeFailedMessages(ctx context.Context) {
	messages, err := c.getFailedMessages(ctx)
	if err != nil {
		fmt.Printf("get failed messages: %v\n", err)
		return
	}

	c.execute(ctx, messages)
}

func (c *consumer) execute(ctx context.Context, messages []message) {
	ids := make([]string, 0, len(messages))

	for _, m := range messages {
		err := c.executeMessage(ctx, m)
		if err != nil {
			fmt.Printf("execute message: %v\n", err)
			continue
		}
		ids = append(ids, m.id)
	}

	if len(ids) == 0 {
		return
	}

	err := c.ackMessages(ctx, ids)
	if err != nil {
		fmt.Printf("ack messages: %v\n", err)
	}
}

func (c *consumer) getMessages(ctx context.Context) ([]message, error) {
	resp := c.rdb.Do(
		ctx,
		c.rdb.B().Xreadgroup().Group(defaultConsumerGroup, c.id).Count(limitTasksForOneIteration).Block(emptyQueueBlockTime).Streams().Key(queue).Id(">").Build(),
	)

	tasks, err := resp.AsXRead()
	if err != nil && resp.NonRedisError() != nil {
		return nil, fmt.Errorf("get tasks: %w", err)
	}

	messages := make([]message, 0, len(tasks))

	for _, t := range tasks[queue] {
		data, ok := t.FieldValues["data"]
		if !ok {
			return nil, fmt.Errorf("not found data in task: %v", t.ID)
		}

		var m message
		err := json.Unmarshal([]byte(data), &m)
		if err != nil {
			return nil, fmt.Errorf("json unmarshal: %w", err)
		}

		m.id = t.ID

		messages = append(messages, m)
	}

	return messages, nil
}

func (c *consumer) getFailedMessages(ctx context.Context) ([]message, error) {
	resp, err := c.rdb.Do(
		ctx,
		c.rdb.B().Xautoclaim().Key(queue).Group(defaultConsumerGroup).Consumer(c.id).MinIdleTime(idleTimeForFailedTask).Start("0-0").Count(limitTasksForOneIteration).Build(),
	).ToArray()
	if err != nil {
		return nil, fmt.Errorf("get failed tasks: %w", err)
	}

	if len(resp) != 3 {
		return nil, nil
	}

	tasks, err := resp[1].AsXRange()

	messages := make([]message, 0, len(tasks))

	for _, t := range tasks {
		data, ok := t.FieldValues["data"]
		if !ok {
			return nil, fmt.Errorf("not found data in task: %v", t.ID)
		}

		var m message
		err := json.Unmarshal([]byte(data), &m)
		if err != nil {
			return nil, fmt.Errorf("json unmarshal: %w", err)
		}

		m.id = t.ID

		messages = append(messages, m)
	}

	return messages, nil
}

func (c *consumer) executeMessage(ctx context.Context, m message) error {
	fmt.Printf("execute message: %v\n", m)

	_, err := c.logFile.WriteString(fmt.Sprintf("execute message: %v\n", m))
	if err != nil {
		fmt.Printf("write to file: %v\n", err)
	}

	time.Sleep(1 * time.Second)

	return nil
}

func (c *consumer) ackMessages(ctx context.Context, ids []string) error {
	err := c.rdb.Do(
		ctx,
		c.rdb.B().Xdel().Key(queue).Id(ids...).Build(),
	).Error()
	if err != nil {
		return fmt.Errorf("redis Xdel: %w", err)
	}

	return nil
}

func (c *consumer) xGroupCreateConsumer(ctx context.Context) error {
	err := c.rdb.Do(
		ctx,
		c.rdb.B().XgroupCreateconsumer().Key(queue).
			Group(defaultConsumerGroup).Consumer(c.id).Build(),
	).Error()
	if err != nil {
		return fmt.Errorf("redis xGroupCreateConsumer: %w", err)
	}

	return nil
}

func (c *consumer) xGroupCreate(ctx context.Context) error {
	err := c.rdb.Do(
		ctx,
		c.rdb.B().XgroupCreate().Key(queue).
			Group(defaultConsumerGroup).Id("$").Mkstream().Build(),
	).Error()
	if err != nil {
		return fmt.Errorf("redis xGroupCreate: %w", err)
	}

	return nil
}
