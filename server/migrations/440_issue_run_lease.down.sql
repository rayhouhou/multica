DROP FUNCTION IF EXISTS task_holds_current_issue_run_lease(UUID);

ALTER TABLE agent_task_queue
    DROP COLUMN IF EXISTS dispatch_event_key,
    DROP COLUMN IF EXISTS issue_lease_generation;

DROP TABLE IF EXISTS issue_run_lease_audit;
DROP TABLE IF EXISTS issue_run_lease;
