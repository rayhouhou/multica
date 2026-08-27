CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_issue_run_lease_key
    ON issue_run_lease (issue_id, role_key);
