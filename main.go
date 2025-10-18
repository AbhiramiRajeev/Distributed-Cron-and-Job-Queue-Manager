package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"time"

	db "github.com/AbhiramiRajeev/Distributed-Cron-and-Job-Queue-Manager/Postgres"
	"github.com/AbhiramiRajeev/Distributed-Cron-and-Job-Queue-Manager/analyzer"
	"github.com/AbhiramiRajeev/Distributed-Cron-and-Job-Queue-Manager/config"
	"github.com/AbhiramiRajeev/Distributed-Cron-and-Job-Queue-Manager/kafka"
	"github.com/AbhiramiRajeev/Distributed-Cron-and-Job-Queue-Manager/redis"
	"github.com/AbhiramiRajeev/Distributed-Cron-and-Job-Queue-Manager/scheduler"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Load config
	config.InitConfig()
	cfg := config.NewConfig()

	// Connect to PostgreSQL
	dbClient, err := db.Connect(ctx, *cfg)
	if err != nil {
		log.Fatalf("Failed to connect to Postgres: %v", err)
	}
	defer dbClient.Close()
	log.Println("Connected to Postgres")

	// Connect to Redis
	redisClient, err := redis.ConnectTLS(cfg.Redis)
	if err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	defer redisClient.Close()
	log.Println("Connected to Redis")

	// Initialize Kafka producer
	producer, err := kafka.NewProducer(*cfg)
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}
	defer producer.Close()
	log.Println("Kafka producer ready")

	// Initialize Scheduler
	scheduler := scheduler.NewScheduler(*dbClient, *producer, 10*time.Second) // poll interval
	go scheduler.Start(ctx, *cfg)
	log.Println("Scheduler started")

	// Initialize Analyzer
	analyzerInstance := analyzer.NewAnalyzer(*cfg, dbClient)
	analyzerInstance.RedisClient = redisClient
	go func() {
		if err := analyzerInstance.Start(ctx); err != nil {
			log.Printf("Analyzer stopped: %v", err)
		}
	}()
	log.Println("Analyzer started")

	// Wait for interrupt signal to gracefully shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, os.Kill)
	<-sigCh
	log.Println("Shutting down gracefully...")
	cancel()
	time.Sleep(2 * time.Second) // allow workers to finish
}
