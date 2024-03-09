package producer

import (
	"context"

	"github.com/pkg/errors"
	"github.com/veleton777/redis_queue/internal/entity"
	"github.com/veleton777/redis_queue/internal/logger"
)

type repo interface {
	ProduceMsg(ctx context.Context, queue string, msg entity.Message) error
}

type Producer struct {
	logger logger.Logger
	repo   repo
	opts   Opts
}

type Params struct {
	Logger logger.Logger
	Repo   repo

	Opts Opts
}

type Opts struct {
	Queue string
}

func New(params Params) *Producer {
	return &Producer{
		logger: params.Logger,
		repo:   params.Repo,
		opts:   params.Opts,
	}
}

func (p *Producer) Produce(ctx context.Context, message entity.Message) error {
	err := p.repo.ProduceMsg(ctx, p.opts.Queue, message)
	if err != nil {
		return errors.Wrap(err, "produce message")
	}

	return nil
}
