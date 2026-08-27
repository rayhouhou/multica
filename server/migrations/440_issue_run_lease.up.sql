CREATE TABLE IF NOT EXISTS issue_run_lease (
    issue_id UUID NOT NULL,
    role_key TEXT NOT NULL,
    task_id UUID NOT NULL,
    generation BIGINT NOT NULL CHECK (generation > 0),
    expires_at TIMESTAMPTZ NOT NULL,
    acquired_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    renewed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS issue_run_lease_audit (
    id UUID NOT NULL DEFAULT gen_random_uuid(),
    issue_id UUID NOT NULL,
    role_key TEXT NOT NULL,
    task_id UUID NOT NULL,
    generation BIGINT NOT NULL,
    event TEXT NOT NULL CHECK (event IN ('acquired', 'takeover', 'renewed', 'released', 'fenced_write')),
    detail TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE agent_task_queue
    ADD COLUMN IF NOT EXISTS issue_lease_generation BIGINT,
    ADD COLUMN IF NOT EXISTS dispatch_event_key TEXT;

CREATE OR REPLACE FUNCTION task_holds_current_issue_run_lease(p_task_id UUID)
RETURNS BOOLEAN
LANGUAGE sql
STABLE
AS $$
    SELECT COALESCE((
        SELECT CASE
            WHEN task.issue_id IS NULL THEN TRUE
            WHEN task.issue_lease_generation IS NULL THEN TRUE
            ELSE EXISTS (
                SELECT 1
                FROM issue_run_lease lease
                WHERE lease.issue_id = task.issue_id
                  AND lease.role_key = task.agent_id::text
                  AND lease.task_id = task.id
                  AND lease.generation = task.issue_lease_generation
            )
        END
        FROM agent_task_queue task
        WHERE task.id = p_task_id
    ), FALSE);
$$;
