package consumer

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/veleton777/redis_queue/internal/consumer/handler"
	"github.com/veleton777/redis_queue/internal/entity"
	"github.com/veleton777/redis_queue/internal/logger"
)

type repo interface {
	Messages(ctx context.Context, dto entity.GetMessagesDTO) ([]entity.Message, error)
	FailedMessages(ctx context.Context, dto entity.GetFailedMessagesDTO) ([]entity.Message, error)
	AckMessages(ctx context.Context, queue string, ids []string) error
	RegisterConsumer(ctx context.Context, queue, group, consumerID string) error
}

type handlerSrv interface {
	Handle(ctx context.Context, evt handler.EventType, m entity.Message) error
}

type Consumer struct {
	logger  logger.Logger
	repo    repo
	handler handlerSrv
	opts    Opts
}

type Params struct {
	Logger  logger.Logger
	Repo    repo
	Handler handlerSrv

	Opts Opts
}

type Opts struct {
	ID                      string
	TasksForIteration       int
	Queue                   string
	Group                   string
	CheckFailedMessagesTime time.Duration
	IdleTimeForNewTask      time.Duration
}

func New(params Params) *Consumer {
	return &Consumer{
		logger:  params.Logger,
		repo:    params.Repo,
		handler: params.Handler,
		opts:    params.Opts,
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	err := c.repo.RegisterConsumer(ctx, c.opts.Queue, c.opts.Group, c.opts.ID)
	if err != nil {
		return errors.Wrap(err, "register consumer")
	}

	if err = c.consumeMessages(ctx); err != nil {
		return errors.Wrap(err, "consume messages")
	}

	return nil
}

func (c *Consumer) consumeMessages(ctx context.Context) error {
	ticker := time.NewTicker(c.opts.CheckFailedMessagesTime)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			c.executeFailedMessages(ctx)
		default:
			c.executeMessages(ctx)
		}

		time.Sleep(1 * time.Second)
	}
}

func (c *Consumer) executeMessages(ctx context.Context) {
	messages, err := c.repo.Messages(ctx, entity.GetMessagesDTO{
		ConsumerID: c.opts.ID,
		BlockTime:  c.opts.IdleTimeForNewTask,
		Queue:      c.opts.Queue,
		Group:      c.opts.Group,
		Limit:      c.opts.TasksForIteration,
	})
	if err != nil {
		c.logger.Err(fmt.Sprintf("get messages: %v\n", err))
		return
	}

	c.execute(ctx, messages)
}

func (c *Consumer) execute(ctx context.Context, messages []entity.Message) {
	ids := make([]string, 0, len(messages))

	for _, m := range messages {
		if err := c.handler.Handle(ctx, 1, m); err != nil { // todo edit
			c.logger.Err(fmt.Sprintf("handle message: %v\n", err))
			return
		}
		ids = append(ids, m.ID)
	}

	if len(ids) == 0 {
		return
	}

	if err := c.repo.AckMessages(ctx, c.opts.Queue, ids); err != nil {
		c.logger.Err(fmt.Sprintf("ack messages: %v\n", err))
	}

	for _, id := range ids {
		c.logger.Success(fmt.Sprintf("msg #: %s", id))
	}
}

func (c *Consumer) executeFailedMessages(ctx context.Context) {
	messages, err := c.repo.FailedMessages(ctx, entity.GetFailedMessagesDTO{
		ConsumerID:         c.opts.ID,
		Queue:              c.opts.Queue,
		Group:              c.opts.Group,
		Limit:              c.opts.TasksForIteration,
		IdleTimeForMessage: c.opts.CheckFailedMessagesTime,
	})
	if err != nil {
		c.logger.Err(fmt.Sprintf("get failed messages: %v\n", err))
		return
	}

	c.execute(ctx, messages)
}
