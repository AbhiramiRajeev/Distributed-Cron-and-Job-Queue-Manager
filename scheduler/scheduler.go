package scheduler

import (
	"context"
	"encoding/json"
	"time"

	db "github.com/AbhiramiRajeev/Distributed-Cron-and-Job-Queue-Manager/Postgres"
	"github.com/AbhiramiRajeev/Distributed-Cron-and-Job-Queue-Manager/config"
	"github.com/AbhiramiRajeev/Distributed-Cron-and-Job-Queue-Manager/kafka"
	"github.com/robfig/cron/v3"
)

type Scheduler struct {
	DBClient *db.DBClient
	Producer *kafka.Producer
	Interval time.Duration
	config   *config.Config
}

func (s *Scheduler) Start(ctx context.Context, cfg config.Config) {

	ticker := time.NewTicker(s.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.CheckAndScheduleJobs(ctx, cfg)
		case <-ctx.Done():
			return
		}
	}
}

func NewScheduler(DBClient db.DBClient, producer kafka.Producer, interval time.Duration) *Scheduler {
	return &Scheduler{
		DBClient: &DBClient,
		Producer: &producer,
		Interval: interval,
	}
}

func (s *Scheduler) CheckAndScheduleJobs(ctx context.Context, cfg config.Config) error {
	jobs, err := s.DBClient.GetDueJobs(ctx)
	if err != nil {
		return err
	}

	for _, job := range jobs {
		jobJson, _ := json.Marshal(job)
		err := s.Producer.Publish(cfg.Kafka.Topic, job.Name, jobJson)
		if err != nil {
			return err
		}
		job.NextRun, err = s.CalculateNextRun(job.CronExpr, time.Now())
		if err != nil {
			return err
		}
		err = s.DBClient.UpdateNextRun(ctx, &job)
		if err != nil {
			return err
		}

	}
	return nil

}

func (s *Scheduler) CalculateNextRun(cronExpr string, from time.Time) (time.Time, error) {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	schedule, err := parser.Parse(cronExpr)
	if err != nil {
		return time.Time{}, err
	}

	return schedule.Next(from), nil

}
