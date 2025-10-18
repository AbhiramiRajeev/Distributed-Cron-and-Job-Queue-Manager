package db

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/AbhiramiRajeev/Distributed-Cron-and-Job-Queue-Manager/config"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/robfig/cron/v3"
)

// DBClient manages TLS-secured Postgres operations
type DBClient struct {
	Pool *pgxpool.Pool
}

// Job represents a scheduled task definition
type Job struct {
	ID        int64     `json:"id" db:"id"`
	Name      string    `json:"name" db:"name"`
	CronExpr  string    `json:"cron_expr" db:"cron_expr"`
	Payload   string    `json:"payload" db:"payload"`
	Owner     string    `json:"owner" db:"owner"`
	Enabled   bool      `json:"enabled" db:"enabled"`
	LastRun   time.Time `json:"last_run" db:"last_run"`
	NextRun   time.Time `json:"next_run" db:"next_run"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
	UpdatedAt time.Time `json:"updated_at" db:"updated_at"`
}

// Execution tracks the lifecycle of an executed job
type Execution struct {
	ID        string
	JobID     string
	Scheduled time.Time
	Status    string
	WorkerID  string
	Attempt   int
	Result    string
}

// Connect creates a TLS-secured connection pool to PostgreSQL
func Connect(ctx context.Context, cfg config.Config) (*DBClient, error) {
	caCert, err := os.ReadFile(cfg.Postgres.CAPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA file: %w", err)
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, errors.New("failed to append CA certificate")
	}

	cert, err := tls.LoadX509KeyPair(cfg.Postgres.CertPath, cfg.Postgres.KeyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load client certificate: %w", err)
	}

	tlsConfig := &tls.Config{
		RootCAs:      caCertPool,
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	connStr := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=%s",
		cfg.Postgres.User,
		cfg.Postgres.Password,
		cfg.Postgres.Host,
		cfg.Postgres.Port,
		cfg.Postgres.DBName,
		cfg.Postgres.SSLMode,
	)

	pgxCfg, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}
	pgxCfg.ConnConfig.TLSConfig = tlsConfig

	pool, err := pgxpool.NewWithConfig(ctx, pgxCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres: %w", err)
	}

	return &DBClient{Pool: pool}, nil
}

// InsertJob inserts a new job definition into the database
func (db *DBClient) InsertJob(ctx context.Context, job *Job) error {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	schedule, err := parser.Parse(job.CronExpr)
	if err != nil {
		return fmt.Errorf("invalid cron expression: %w", err)
	}

	job.NextRun = schedule.Next(time.Now())

	query := `
	INSERT INTO jobs (name, cron_expr, payload, owner, enabled, next_run, created_at, updated_at)
	VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())
	`
	_, err = db.Pool.Exec(ctx, query,
		job.Name, job.CronExpr, job.Payload, job.Owner, job.Enabled, job.NextRun)
	if err != nil {
		return fmt.Errorf("failed to insert job: %w", err)
	}
	return nil
}

// GetDueJobs returns all jobs that are ready to run (next_run <= now)
func (db *DBClient) GetDueJobs(ctx context.Context) ([]Job, error) {
	query := `
	SELECT id, name, cron_expr, payload, owner, enabled, next_run
	FROM jobs
	WHERE next_run <= NOW() AND enabled = TRUE
	`
	rows, err := db.Pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get due jobs: %w", err)
	}
	defer rows.Close()

	var jobs []Job
	for rows.Next() {
		var job Job
		if err := rows.Scan(&job.ID, &job.Name, &job.CronExpr, &job.Payload, &job.Owner, &job.Enabled, &job.NextRun); err != nil {
			return nil, fmt.Errorf("failed to scan job row: %w", err)
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}

// UpdateNextRun computes and updates the next scheduled run for a job
func (db *DBClient) UpdateNextRun(ctx context.Context, job *Job) error {
	query := `
		UPDATE jobs
		SET next_run = $1, updated_at = NOW()
		WHERE id = $2
	`
	_, err := db.Pool.Exec(ctx, query, job.NextRun, job.ID)
	return err
}

// InsertExecution logs a job execution start event
func (db *DBClient) InsertExecution(ctx context.Context, exec Execution) error {
	query := `
	INSERT INTO executions (id, job_id, scheduled_at, status, worker_id, attempt, result, created_at)
	VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
	`
	_, err := db.Pool.Exec(ctx, query,
		exec.ID, exec.JobID, exec.Scheduled, exec.Status, exec.WorkerID, exec.Attempt, exec.Result)
	if err != nil {
		return fmt.Errorf("failed to insert execution: %w", err)
	}
	return nil
}

// UpdateExecutionStatus updates the status and result of a running job
func (db *DBClient) UpdateExecutionStatus(ctx context.Context, execID, status, result string) error {
	query := `
	UPDATE executions
	SET status = $1, result = $2, finished_at = NOW()
	WHERE id = $3
	`
	_, err := db.Pool.Exec(ctx, query, status, result, execID)
	if err != nil {
		return fmt.Errorf("failed to update execution status: %w", err)
	}
	return nil
}

// Close releases all database connections
func (db *DBClient) Close() {
	if db.Pool != nil {
		db.Pool.Close()
	}
}
