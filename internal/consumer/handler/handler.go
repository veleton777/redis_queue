package handler

import (
	"context"

	"github.com/pkg/errors"
	"github.com/veleton777/redis_queue/internal/entity"
)

type Handler struct {
}

type EventType int

const (
	EventTypeUser EventType = 1
)

func NewHandler() *Handler {
	return &Handler{}
}

func (h *Handler) Handle(ctx context.Context, evt EventType, m entity.Message) error {
	var err error

	switch evt {
	case EventTypeUser:
		if err = h.User(ctx, m); err != nil {
			return errors.Wrap(err, "handle user")
		}
	default:
		return errors.Errorf("unknown event type: %d", evt)
	}

	return nil
}
