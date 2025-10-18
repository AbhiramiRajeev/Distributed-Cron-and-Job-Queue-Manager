package redis

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"github.com/AbhiramiRajeev/Distributed-Cron-and-Job-Queue-Manager/config"
	"github.com/redis/go-redis/v9"
)

type RedisClient struct {
	Client *redis.Client
}

func ConnectTLS(cfg config.Redis) (*RedisClient, error) {
	caCert, err := os.ReadFile(cfg.CAPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read caCert: %w", err)
	}

	caCertPool := x509.NewCertPool()

	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to append caCert to caCertPool: %w", err)
	}
	fmt.Println("Connected to redis")

	cert, err := tls.LoadX509KeyPair(cfg.CertPath, cfg.KeyPath)
	if err != nil {
		panic(err)
	}
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
		MinVersion:   tls.VersionTLS12,
	}
	addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	rdb := redis.NewClient(&redis.Options{
		Addr:      addr,
		Password:  cfg.Password,
		DB:        cfg.DB,
		TLSConfig: tlsConfig,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = rdb.Ping(ctx).Err()
	if err != nil {
		return nil, fmt.Errorf("failed to ping redis: %w", err)
	}

	return &RedisClient{Client: rdb}, nil

}

func (r *RedisClient) SetValue(ctx context.Context, key, value string, ttl time.Duration) error {
	return r.Client.Set(ctx, key, value, ttl).Err()
}

func (r *RedisClient) GetValue(ctx context.Context, key string) (string, error) {
	val, err := r.Client.Get(ctx, key).Result()
	if err == redis.Nil {
		return "", nil
	}
	return val, err
}

func (r *RedisClient) DeleteKey(ctx context.Context, key string) error {
	return r.Client.Del(ctx, key).Err()
}

func (r *RedisClient) Increment(ctx context.Context, key string) (int64, error) {
	return r.Client.Incr(ctx, key).Result()
}

func (r *RedisClient) GetCounter(ctx context.Context, key string) (int64, error) {
	val, err := r.Client.Get(ctx, key).Int64()
	if err == redis.Nil {
		return 0, nil
	}
	return val, err

}

func (r *RedisClient) Close() error {
	return r.Client.Close()
}

func (r *RedisClient) SetNX(ctx context.Context, key string, value string, ttl time.Duration) (bool, error) {
	return r.Client.SetNX(ctx, key, value, ttl).Result()
}
