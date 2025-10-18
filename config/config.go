package config

import (
	"fmt"

	"github.com/spf13/viper"
)

type Config struct {
	Postgres Postgres
	Redis    Redis
	Kafka    Kafka
	WorkerCount int
}

type Postgres struct {
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
	SSLMode  string
	CertPath string
	KeyPath  string
	CAPath   string
}

type Redis struct {
	Host     string
	Port     int
	Password string
	DB       int
	CertPath string
	KeyPath  string
	CAPath   string
}

type Kafka struct {
	Brokers    []string
	CertPath   string
	KeyPath    string
	CACertPath string
	Group      string
	Topic 	   string
}

func InitConfig() {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	err := viper.ReadInConfig()
	if err != nil {
		fmt.Printf("fatal error config file: %s", err)
	}
	viper.AutomaticEnv()

}

func NewConfig() *Config {
	return &Config{
		Postgres: Postgres{
			Host:     viper.GetString("POSTGRES_HOST"),
			Port:     viper.GetInt("POSTGRES_PORT"),
			User:     viper.GetString("POSTGRES_USER"),
			Password: viper.GetString("POSTGRES_PASSWORD"),
			DBName:   viper.GetString("POSTGRES_DBNAME"),
			SSLMode:  viper.GetString("POSTGRES_SSLMODE"),
			CertPath: viper.GetString("POSTGRES_CERTPATH"),
			KeyPath:  viper.GetString("POSTGRES_KEYPATH"),
			CAPath:   viper.GetString("POSTGRES_CAPATH"),
		},
		Redis: Redis{
			Host:     viper.GetString("REDIS_HOST"),
			Port:     viper.GetInt("REDIS_PORT"),
			Password: viper.GetString("REDIS_PASSWORD"),
			DB:       viper.GetInt("REDIS_DB"),
			CertPath: viper.GetString("REDIS_CERTPATH"),
			KeyPath:  viper.GetString("REDIS_KEYPATH"),
			CAPath:   viper.GetString("REDIS_CAPATH"),
		},
		Kafka: Kafka{
			Brokers:    viper.GetStringSlice("KAFKA_BROKERS"),
			CertPath:   viper.GetString("KAFKA_CERT"),
			KeyPath:    viper.GetString("KAFKA_KEY"),
			CACertPath: viper.GetString("KAFKA_CA"),
			Group:      viper.GetString("KAFKA_GROUP"),
			Topic:		viper.GetString("KAFKA_TOPIC"),
		},
	}
}
