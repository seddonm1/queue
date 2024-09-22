CREATE EXTENSION pg_cron;

CREATE TYPE task_status AS ENUM (
    'QUEUED',
    'RUNNING',
    'SUCCEEDED',
    'FAILED',
    'CANCELLED'
);

CREATE TABLE task_queues (
    queue TEXT NOT NULL,
    scheduled_for TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY(queue)
);

CREATE TABLE tasks (
    queue TEXT NOT NULL,
    id UUID NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    scheduled_for TIMESTAMP WITH TIME ZONE NOT NULL,
    status task_status DEFAULT 'QUEUED',
    args JSONB NOT NULL,
    max_retries SMALLINT NOT NULL,
    errors JSONB[] NOT NULL DEFAULT '{}',
    PRIMARY KEY (queue, id),
    CHECK (max_retries > 0)
);

CREATE INDEX ix_tasks_queue_scheduled_for ON tasks(queue, scheduled_for);
