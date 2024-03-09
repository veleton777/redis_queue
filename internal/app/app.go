package app

import (
	"context"

	"github.com/pkg/errors"
	"github.com/redis/rueidis"
	"github.com/veleton777/redis_queue/internal/config"
	"github.com/veleton777/redis_queue/internal/consumer"
	"github.com/veleton777/redis_queue/internal/consumer/handler"
	"github.com/veleton777/redis_queue/internal/entity"
	"github.com/veleton777/redis_queue/internal/logger"
	"github.com/veleton777/redis_queue/internal/producer"
	"github.com/veleton777/redis_queue/internal/repository/redis"
	"golang.org/x/sync/errgroup"
)

type App struct {
	redisClient rueidis.Client
	consumer    *consumer.Consumer
	producer    *producer.Producer
	config      config.Config
	logger      logger.Logger
	consumerID  string

	queueShutDownFuncs []func() error
}

func New(config config.Config, logger logger.Logger, consumerID string) (*App, error) {
	app := &App{
		config:     config,
		logger:     logger,
		consumerID: consumerID,
	}

	if err := app.buildDeps(); err != nil {
		return nil, errors.Wrap(err, "build deps for app")
	}

	return app, nil
}

func (a *App) buildDeps() error {
	redisClient, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{a.config.Redis.Addr},
		SelectDB:    a.config.Redis.DB,
		Password:    a.config.Redis.Password,
	})
	if err != nil {
		return errors.Wrap(err, "create redis client")
	}

	a.redisClient = redisClient
	a.registerShutdown(func() error {
		a.redisClient.Close()

		return nil
	})

	repo := redis.NewRepo(a.redisClient)
	handlerSrv := handler.NewHandler()

	a.consumer = consumer.New(consumer.Params{
		Logger:  a.logger,
		Repo:    repo,
		Handler: handlerSrv,
		Opts: consumer.Opts{
			ID:                      a.consumerID,
			TasksForIteration:       10,
			Queue:                   a.config.Redis.Consumer.Queue,
			Group:                   a.config.Redis.Consumer.Group,
			CheckFailedMessagesTime: a.config.Redis.Consumer.IdleTimeForFailedTask,
			IdleTimeForNewTask:      a.config.Redis.Consumer.IdleTimeForNewTask,
		},
	})

	a.producer = producer.New(producer.Params{
		Logger: a.logger,
		Repo:   repo,
		Opts: producer.Opts{
			Queue: a.config.Redis.Consumer.Queue,
		},
	})

	return nil
}

func (a *App) RunConsumer(ctx context.Context) error {
	err := a.consumer.Run(ctx)
	if err != nil {
		return errors.Wrap(err, "run consumer")
	}

	return nil
}

func (a *App) ProduceMsg(ctx context.Context, message entity.Message) error {
	err := a.producer.Produce(ctx, message)
	if err != nil {
		return errors.Wrap(err, "produce message")
	}

	return nil
}

func (a *App) WaitShutdown(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	for _, f := range a.queueShutDownFuncs {
		f := f

		g.Go(func() error {
			if err := f(); err != nil {
				return errors.Wrap(err, "shutdown func")
			}

			return nil
		})
	}

	err := g.Wait()
	if err != nil {
		return errors.Wrap(err, "wait shutdown funcs")
	}

	return nil
}

func (a *App) registerShutdown(f func() error) {
	a.queueShutDownFuncs = append(a.queueShutDownFuncs, f)
}
