package analyzer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	db "github.com/AbhiramiRajeev/Distributed-Cron-and-Job-Queue-Manager/Postgres"
	"github.com/AbhiramiRajeev/Distributed-Cron-and-Job-Queue-Manager/config"
	"github.com/AbhiramiRajeev/Distributed-Cron-and-Job-Queue-Manager/kafka"
	"github.com/AbhiramiRajeev/Distributed-Cron-and-Job-Queue-Manager/redis"
	"github.com/IBM/sarama"
	"github.com/google/uuid"
)

type Analyzer struct {
	DB          *db.DBClient
	Config      config.Config
	WorkerCount int
	WorkerID    string
	RedisClient *redis.RedisClient
}

// NewAnalyzer creates a new Analyzer instance.
func NewAnalyzer(cfg config.Config, dbClient *db.DBClient) *Analyzer {
	workerCount := cfg.WorkerCount
	if workerCount <= 0 {
		workerCount = 3 // default fallback
	}

	return &Analyzer{
		DB:          dbClient,
		Config:      cfg,
		WorkerCount: workerCount,
		WorkerID:    fmt.Sprintf("analyzer-%d", time.Now().UnixNano()),
	}
}

// Start launches multiple workers to consume from Kafka concurrently.
func (a *Analyzer) Start(ctx context.Context) error {
	log.Printf("🚀 Starting Analyzer with %d workers...", a.WorkerCount)

	for i := 0; i < a.WorkerCount; i++ {
		go a.startWorker(ctx, i, a.Config)
	}

	// Wait for OS interrupt (Ctrl+C)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	select {
	case <-ctx.Done():
		log.Println("🛑 Context cancelled, shutting down analyzer...")
	case sig := <-sigCh:
		log.Printf("🛑 Received signal: %v, shutting down analyzer...", sig)
	}

	return nil
}

// startWorker creates a Kafka consumer per worker.
func (a *Analyzer) startWorker(ctx context.Context, id int, cfg config.Config) {
	workerID := fmt.Sprintf("%s-worker-%d", a.WorkerID, id)

	handler := func(msg *sarama.ConsumerMessage) error {
		return a.processJob(ctx, workerID, msg.Value)
	}

	consumer, err := kafka.NewConsumer(cfg, handler)
	if err != nil {
		log.Printf("Worker %d failed to start consumer: %v", id, err)
		return
	}

	log.Printf("Worker %d (%s) started consuming...", id, workerID)
	consumer.Start(ctx, cfg)
}

// processJob executes a single job message.
func (a *Analyzer) processJob(ctx context.Context, workerID string, payload []byte) error {
	var job db.Job
	if err := json.Unmarshal(payload, &job); err != nil {
		return fmt.Errorf("invalid job payload: %w", err)
	}

	lockKey := "job-lock:" + fmt.Sprint(job.ID)
	ok, err := a.RedisClient.SetNX(ctx, lockKey, workerID, 5*time.Minute)
	if err != nil || !ok {
		log.Printf("Job %d is already being processed", job.ID)
		return nil
	}

	// Heartbeat update
	a.RedisClient.SetValue(ctx, "worker:"+workerID+":heartbeat", time.Now().String(), 10*time.Second)

	// Insert execution to Postgres
	exec := db.Execution{
		ID:        uuid.NewString(),
		JobID:     fmt.Sprint(job.ID),
		Scheduled: time.Now(),
		Status:    "running",
		WorkerID:  workerID,
		Attempt:   1,
	}
	if err := a.DB.InsertExecution(ctx, exec); err != nil {
		return err
	}

	// Simulate job work
	time.Sleep(2 * time.Second)
	result := "success"

	// Update execution status
	if err := a.DB.UpdateExecutionStatus(ctx, exec.ID, "completed", result); err != nil {
		return err
	}

	log.Printf("Job completed: %s", job.Name)
	return nil
}
