CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_issue_run_lease_audit_id
    ON issue_run_lease_audit (id);
