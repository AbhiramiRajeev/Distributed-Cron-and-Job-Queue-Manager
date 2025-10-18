package kafka

import (
	"context"
	"log"
	"time"

	"github.com/AbhiramiRajeev/Distributed-Cron-and-Job-Queue-Manager/config"
	"github.com/IBM/sarama"
)

type Consumer struct {
	Group          sarama.ConsumerGroup
	MessageHandler sarama.ConsumerGroupHandler
}

func NewConsumer(cfg config.Config, Processfunc func(msg *sarama.ConsumerMessage) error) (*Consumer, error) {
	tlsConfig, err := NewtlsConfig(cfg.Kafka)
	if err != nil {
		return nil, err
	}

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Net.TLS.Enable = true
	config.Net.TLS.Config = tlsConfig
	config.Net.DialTimeout = 5 * time.Second

	consumer, err := sarama.NewConsumerGroup(cfg.Kafka.Brokers, cfg.Kafka.Group, config)
	if err != nil {
		return nil, err
	}

	handler := &MessageHandler{
		ProcessFunc: Processfunc,
	}
	return &Consumer{
		Group:          consumer,
		MessageHandler: handler,
	}, nil
}

func (c *Consumer) Start(ctx context.Context, cfg config.Config) {
	for {
		if err := c.Group.Consume(ctx, []string{cfg.Kafka.Topic}, c.MessageHandler); err != nil {
			log.Printf("Error from consumer: %v", err)
			time.Sleep(10 * time.Second)
		}

		if ctx.Err() != nil {
			log.Println("Context cancelled, stopping consumer...")
			return
		}
	}
}

type MessageHandler struct {
	ProcessFunc func(*sarama.ConsumerMessage) error
}

func (h *MessageHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *MessageHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *MessageHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		if err := h.ProcessFunc(msg); err != nil {
			log.Printf("Error processing message: %v", err)
		}
		session.MarkMessage(msg, "")
	}
	return nil
}
