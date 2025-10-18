package kafka

import (
	"fmt"

	"github.com/AbhiramiRajeev/Distributed-Cron-and-Job-Queue-Manager/config"
	"github.com/IBM/sarama"
)

type Producer struct {
	producer sarama.AsyncProducer
}

func NewProducer(cfg config.Config) (*Producer, error) {

	tlsConfig, err := NewtlsConfig(cfg.Kafka)
	if err != nil {
		return nil, err
	}
	saramaConfig := sarama.NewConfig()
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Return.Errors = true
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.Net.TLS.Enable = true
	saramaConfig.Net.TLS.Config = tlsConfig

	producer, err := sarama.NewAsyncProducer(cfg.Kafka.Brokers, saramaConfig)
	if err != nil {
		return nil, err
	}
	go func() {
		for msg := range producer.Successes() {
			fmt.Printf("Message sent to topic %s, partition %d, offset %d\n", msg.Topic, msg.Partition, msg.Offset)
		}
	}()

	go func() {
		for err := range producer.Errors() {
			fmt.Printf("Failed to send message: %s\n", err.Error())
		}
	}()

	return &Producer{producer: producer}, nil

}

func (p *Producer) Publish(topic string, key string, value []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.StringEncoder(value),
	}

	p.producer.Input() <- msg
	return nil
}

func (p *Producer) Close() error {
	return p.producer.Close()
}
