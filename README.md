#  Distributed Cron and Job Queue Manager

A **cloud-native, fault-tolerant, and distributed job scheduling system** built in **Golang** — designed to handle **time-based (cron) jobs** and **queued workloads** across multiple workers in a scalable manner.

This project demonstrates how to design a **production-grade distributed scheduler** using **Kafka, PostgreSQL, Redis**, and **Go concurrency patterns** (pipelines, fan-out/fan-in, workers).

---

##  System Overview

This system is divided into **three main services**, each running independently but working together to form a distributed job platform:

| Component     | Description |
|----------------|-------------|
| **Scheduler** | Reads jobs from DB, calculates next run using cron expressions, and dispatches them to Kafka. |
| **Analyzer**  | Listens to Kafka for scheduled jobs, executes them, tracks their progress, and updates DB/Redis. |
| **PostgreSQL** | Stores job definitions, schedules, and execution history. |
| **Redis** | Used for caching and fast access to recent execution states. |
| **Kafka** | Provides reliable event-driven messaging between scheduler and analyzers. |

---

## Architecture

```plaintext
 ┌─────────────────────┐
 │     Scheduler       │
 │  (Cron Dispatcher)  │
 └──────┬──────────────┘
        │ Publishes due jobs
        ▼
 ┌─────────────────────┐
 │       Kafka         │
 │ (Event Queue Topic) │
 └──────┬──────────────┘
        │ Consumes jobs
        ▼
 ┌─────────────────────┐
 │     Analyzer(s)     │
 │ (N Workers/Instance)│
 └──────┬──────────────┘
        │ Update DB/Redis
        ▼
 ┌─────────────────────┐
 │  PostgreSQL & Redis │
 └─────────────────────┘
