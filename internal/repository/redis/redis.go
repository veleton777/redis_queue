package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/redis/rueidis"
	"github.com/veleton777/redis_queue/internal/entity"
)

var errRegisGroupAlreadyExists = errors.New("BUSYGROUP Consumer Group name already exists")

const dataField = "data"

type Repo struct {
	rdb rueidis.Client
}

func NewRepo(redisClient rueidis.Client) *Repo {
	return &Repo{
		rdb: redisClient,
	}
}

func (r *Repo) Messages(ctx context.Context, dto entity.GetMessagesDTO) ([]entity.Message, error) {
	resp := r.rdb.Do(
		ctx,
		r.rdb.B().Xreadgroup().Group(dto.Group, dto.ConsumerID).Count(int64(dto.Limit)).Block(dto.BlockTime.Milliseconds()).Streams().Key(dto.Queue).Id(">").Build(),
	)

	tasks, err := resp.AsXRead()
	if err != nil && resp.NonRedisError() != nil {
		return nil, errors.Wrap(err, "get tasks")
	}

	messages := make([]entity.Message, 0, len(tasks))

	for _, t := range tasks[dto.Queue] {
		data, ok := t.FieldValues[dataField]
		if !ok {
			return nil, fmt.Errorf("not found data in task: %v", t.ID)
		}

		var m entity.Message
		err := json.Unmarshal([]byte(data), &m)
		if err != nil {
			return nil, errors.Wrap(err, "json unmarshal")
		}

		m.ID = t.ID

		messages = append(messages, m)
	}

	return messages, nil
}

func (r *Repo) FailedMessages(ctx context.Context, dto entity.GetFailedMessagesDTO) ([]entity.Message, error) {
	resp, err := r.rdb.Do(
		ctx,
		r.rdb.B().Xautoclaim().Key(dto.Queue).Group(dto.Group).Consumer(dto.ConsumerID).MinIdleTime(strconv.FormatInt(dto.IdleTimeForMessage.Milliseconds(), 10)).Start("0-0").Count(int64(dto.Limit)).Build(),
	).ToArray()
	if err != nil {
		return nil, errors.Wrap(err, "get failed tasks")
	}

	if len(resp) != 3 {
		return nil, nil
	}

	tasks, err := resp[1].AsXRange()

	messages := make([]entity.Message, 0, len(tasks))

	for _, t := range tasks {
		data, ok := t.FieldValues[dataField]
		if !ok {
			return nil, fmt.Errorf("not found data in task: %v", t.ID)
		}

		var m entity.Message
		err = json.Unmarshal([]byte(data), &m)
		if err != nil {
			return nil, errors.Wrap(err, "json unmarshal")
		}

		m.ID = t.ID

		messages = append(messages, m)
	}

	return messages, nil
}

func (r *Repo) AckMessages(ctx context.Context, queue string, ids []string) error {
	err := r.rdb.Do(
		ctx,
		r.rdb.B().Xdel().Key(queue).Id(ids...).Build(),
	).Error()
	if err != nil {
		return errors.Wrap(err, "ack messages")
	}

	return nil
}

func (r *Repo) ProduceMsg(ctx context.Context, queue string, msg entity.Message) error {
	b, err := json.Marshal(msg)
	if err != nil {
		return errors.Wrap(err, "json marshal")
	}

	err = r.rdb.Do(
		ctx,
		r.rdb.B().Xadd().Key(queue).
			Id("*").FieldValue().FieldValue(dataField, string(b)).Build(),
	).Error()
	if err != nil {
		return fmt.Errorf("redis xAdd: %w", err)
	}

	return nil
}

func (r *Repo) RegisterConsumer(ctx context.Context, queue, group, consumerID string) error {
	stream, err := r.rdb.Do(
		ctx,
		r.rdb.B().Exists().Key(queue).Build(),
	).AsInt64()

	var isExistGroup bool
	if stream != 0 {
		groupInfo, err := r.rdb.Do(
			ctx,
			r.rdb.B().XinfoGroups().Key(queue).Build(),
		).ToMessage()
		if err != nil {
			return errors.Wrap(err, "redis XinfoGroups")
		}

		isExistGroup = strings.Contains(groupInfo.String(), group)
	}

	if !isExistGroup {
		err = r.rdb.Do(
			ctx,
			r.rdb.B().XgroupCreate().Key(queue).
				Group(group).Id("$").Mkstream().Build(),
		).Error()
		if err != nil && !strings.Contains(err.Error(), errRegisGroupAlreadyExists.Error()) {
			return errors.Wrap(err, "redis xGroupCreate")
		}
	}

	err = r.rdb.Do(
		ctx,
		r.rdb.B().XgroupCreateconsumer().Key(queue).
			Group(group).Consumer(consumerID).Build(),
	).Error()
	if err != nil {
		return errors.Wrap(err, "redis xGroupCreateConsumer")
	}

	return nil
}
