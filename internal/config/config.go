package config

import (
	"time"

	"github.com/pkg/errors"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	Redis Redis
}

type Redis struct {
	Addr     string `env:"REDIS_ADDR" env-default:"localhost:63791"`
	Password string `env:"REDIS_PASSWORD"`
	DB       int    `env:"REDIS_DB" env-default:"0"`

	Consumer RedisConsumer
}

type RedisConsumer struct {
	Queue                 string        `env:"REDIS_CONSUMER_QUEUE" env-default:"default_queue"`
	Group                 string        `env:"REDIS_CONSUMER_GROUP" env-default:"default_group"`
	IdleTimeForFailedTask time.Duration `env:"REDIS_CONSUMER_IDLE_TIME_FOR_FAILED_TASK" env-default:"30s"`
	IdleTimeForNewTask    time.Duration `env:"REDIS_CONSUMER_IDLE_TIME_FOR_NEW_TASK" env-default:"10s"`
}

func LoadFromEnv() (Config, error) {
	var config Config

	if err := cleanenv.ReadEnv(&config); err != nil {
		return Config{}, errors.Wrap(err, "read env")
	}

	return config, nil
}
