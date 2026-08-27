CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_agent_task_dispatch_event_key
    ON agent_task_queue (dispatch_event_key)
    WHERE dispatch_event_key IS NOT NULL;
