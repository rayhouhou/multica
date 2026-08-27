package service

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

func TestIssueRunLeaseSuppressesConcurrentWriterAndFencesStaleTakeover(t *testing.T) {
	pool := newResolveOriginatorPool(t)
	ctx := context.Background()
	_, _, agentID, issueID := seedAttributionFixture(t, pool)
	var runtimeID string
	if err := pool.QueryRow(ctx, `SELECT runtime_id::text FROM agent WHERE id = $1`, agentID).Scan(&runtimeID); err != nil {
		t.Fatalf("read runtime: %v", err)
	}
	t.Cleanup(func() {
		pool.Exec(context.Background(), `DELETE FROM issue_run_lease_audit WHERE issue_id = $1`, issueID)
		pool.Exec(context.Background(), `DELETE FROM issue_run_lease WHERE issue_id = $1`, issueID)
		pool.Exec(context.Background(), `DELETE FROM agent_task_queue WHERE issue_id = $1`, issueID)
	})

	insertDispatched := func() pgtype.UUID {
		t.Helper()
		var id pgtype.UUID
		if err := pool.QueryRow(ctx, `
			INSERT INTO agent_task_queue (agent_id, runtime_id, issue_id, status, priority)
			VALUES ($1, $2, $3, 'dispatched', 0)
			RETURNING id
		`, agentID, runtimeID, issueID).Scan(&id); err != nil {
			t.Fatalf("insert dispatched task: %v", err)
		}
		return id
	}

	svc := &TaskService{Queries: db.New(pool), TxStarter: pool, Bus: events.New()}
	firstID := insertDispatched()
	first, err := svc.StartTask(ctx, firstID)
	if err != nil {
		t.Fatalf("start first task: %v", err)
	}
	if !first.IssueLeaseGeneration.Valid || first.IssueLeaseGeneration.Int64 != 1 {
		t.Fatalf("first generation = %v, want 1", first.IssueLeaseGeneration)
	}

	secondID := insertDispatched()
	if _, err := svc.StartTask(ctx, secondID); !errors.Is(err, ErrIssueRunLeaseHeld) {
		t.Fatalf("concurrent start error = %v, want ErrIssueRunLeaseHeld", err)
	}
	if got := taskStatus(t, pool, secondID); got != "dispatched" {
		t.Fatalf("suppressed task status = %q, want dispatched", got)
	}

	// Simulate a crashed daemon: the task remains running but its heartbeat no
	// longer renews the lease. Expiry permits an explicit successor takeover.
	if _, err := pool.Exec(ctx, `UPDATE issue_run_lease SET expires_at = now() - interval '1 second' WHERE issue_id = $1`, issueID); err != nil {
		t.Fatalf("expire lease: %v", err)
	}
	second, err := svc.StartTask(ctx, secondID)
	if err != nil {
		t.Fatalf("start takeover task: %v", err)
	}
	if !second.IssueLeaseGeneration.Valid || second.IssueLeaseGeneration.Int64 != 2 {
		t.Fatalf("takeover generation = %v, want 2", second.IssueLeaseGeneration)
	}

	current, err := svc.Queries.TaskHoldsCurrentIssueRunLease(ctx, firstID)
	if err != nil {
		t.Fatalf("check stale generation: %v", err)
	}
	if current {
		t.Fatal("first task still owns the lease after takeover")
	}
	if _, err := svc.CompleteTask(ctx, firstID, []byte(`{"output":"stale"}`), "", "", "stale-branch", false, "", ""); !errors.Is(err, ErrIssueRunLeaseFenced) {
		t.Fatalf("stale completion error = %v, want ErrIssueRunLeaseFenced", err)
	}
	if got := taskStatus(t, pool, firstID); got != "running" {
		t.Fatalf("stale completion changed status to %q", got)
	}

	var takeoverAudit, fencedAudit int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE event = 'takeover'),
		       count(*) FILTER (WHERE event = 'fenced_write')
		FROM issue_run_lease_audit WHERE issue_id = $1
	`, issueID).Scan(&takeoverAudit, &fencedAudit); err != nil {
		t.Fatalf("read lease audit: %v", err)
	}
	if takeoverAudit != 1 || fencedAudit != 1 {
		t.Fatalf("audit takeover=%d fenced=%d, want 1/1", takeoverAudit, fencedAudit)
	}
}

func TestIssueRunLeaseIsIndependentPerRole(t *testing.T) {
	pool := newResolveOriginatorPool(t)
	ctx := context.Background()
	workspaceID, userID, writerID, issueID := seedAttributionFixture(t, pool)
	var runtimeID string
	if err := pool.QueryRow(ctx, `SELECT runtime_id::text FROM agent WHERE id = $1`, writerID).Scan(&runtimeID); err != nil {
		t.Fatalf("read runtime: %v", err)
	}
	var reviewerID string
	if err := pool.QueryRow(ctx, `
		INSERT INTO agent (workspace_id, name, runtime_mode, runtime_config, runtime_id, visibility,
			max_concurrent_tasks, owner_id, instructions, custom_env, custom_args)
		VALUES ($1, $2, 'cloud', '{}'::jsonb, $3, 'workspace', 1, $4, '', '{}'::jsonb, '[]'::jsonb)
		RETURNING id
	`, workspaceID, fmt.Sprintf("reviewer-%d", time.Now().UnixNano()), runtimeID, userID).Scan(&reviewerID); err != nil {
		t.Fatalf("seed reviewer: %v", err)
	}
	t.Cleanup(func() {
		pool.Exec(context.Background(), `DELETE FROM issue_run_lease_audit WHERE issue_id = $1`, issueID)
		pool.Exec(context.Background(), `DELETE FROM issue_run_lease WHERE issue_id = $1`, issueID)
		pool.Exec(context.Background(), `DELETE FROM agent_task_queue WHERE issue_id = $1`, issueID)
		pool.Exec(context.Background(), `DELETE FROM agent WHERE id = $1`, reviewerID)
	})

	insert := func(agentID string) pgtype.UUID {
		t.Helper()
		var id pgtype.UUID
		if err := pool.QueryRow(ctx, `
			INSERT INTO agent_task_queue (agent_id, runtime_id, issue_id, status, priority)
			VALUES ($1, $2, $3, 'dispatched', 0) RETURNING id
		`, agentID, runtimeID, issueID).Scan(&id); err != nil {
			t.Fatalf("insert task for %s: %v", agentID, err)
		}
		return id
	}

	svc := &TaskService{Queries: db.New(pool), TxStarter: pool, Bus: events.New()}
	writer, err := svc.StartTask(ctx, insert(writerID))
	if err != nil {
		t.Fatalf("start writer: %v", err)
	}
	reviewer, err := svc.StartTask(ctx, insert(reviewerID))
	if err != nil {
		t.Fatalf("start reviewer: %v", err)
	}
	if writer.IssueLeaseGeneration.Int64 != 1 || reviewer.IssueLeaseGeneration.Int64 != 1 {
		t.Fatalf("independent generations = writer %d reviewer %d, want 1/1", writer.IssueLeaseGeneration.Int64, reviewer.IssueLeaseGeneration.Int64)
	}
	var leases int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM issue_run_lease WHERE issue_id = $1`, issueID).Scan(&leases); err != nil {
		t.Fatalf("count role leases: %v", err)
	}
	if leases != 2 {
		t.Fatalf("role lease count = %d, want 2", leases)
	}
}

func TestIssueRunLeaseAllowsImmediateTakeoverAfterCancellation(t *testing.T) {
	pool := newResolveOriginatorPool(t)
	ctx := context.Background()
	_, _, agentID, issueID := seedAttributionFixture(t, pool)
	var runtimeID string
	if err := pool.QueryRow(ctx, `SELECT runtime_id::text FROM agent WHERE id = $1`, agentID).Scan(&runtimeID); err != nil {
		t.Fatalf("read runtime: %v", err)
	}
	t.Cleanup(func() {
		pool.Exec(context.Background(), `DELETE FROM issue_run_lease_audit WHERE issue_id = $1`, issueID)
		pool.Exec(context.Background(), `DELETE FROM issue_run_lease WHERE issue_id = $1`, issueID)
		pool.Exec(context.Background(), `DELETE FROM agent_task_queue WHERE issue_id = $1`, issueID)
	})

	insert := func(status string) pgtype.UUID {
		var id pgtype.UUID
		if err := pool.QueryRow(ctx, `INSERT INTO agent_task_queue (agent_id, runtime_id, issue_id, status, priority) VALUES ($1,$2,$3,$4,0) RETURNING id`, agentID, runtimeID, issueID, status).Scan(&id); err != nil {
			t.Fatalf("insert %s task: %v", status, err)
		}
		return id
	}
	svc := &TaskService{Queries: db.New(pool), TxStarter: pool, Bus: events.New()}
	firstID := insert("dispatched")
	if _, err := svc.StartTask(ctx, firstID); err != nil {
		t.Fatalf("start first: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE agent_task_queue SET status = 'cancelled' WHERE id = $1`, firstID); err != nil {
		t.Fatalf("cancel first: %v", err)
	}
	secondID := insert("dispatched")
	second, err := svc.StartTask(ctx, secondID)
	if err != nil {
		t.Fatalf("start after cancellation: %v", err)
	}
	if second.IssueLeaseGeneration.Int64 != 2 {
		t.Fatalf("generation after cancellation = %d, want 2", second.IssueLeaseGeneration.Int64)
	}
	if util.UUIDToString(second.ID) != util.UUIDToString(secondID) {
		t.Fatal("started the wrong successor task")
	}
}
