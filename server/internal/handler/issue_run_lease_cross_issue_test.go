package handler

import (
	"net/http"
	"testing"

	"github.com/multica-ai/multica/server/internal/testutil"
)

// FIN-290: the issue run lease must fence only a task's OWN issue. An orchestrator
// run (the chief-of-staff agent) routinely writes to OTHER issues — assigning
// writers, flipping status, promoting stage barriers — and those writes are
// governed by workspace membership authz plus the optimistic revision check, not
// by any run lease the orchestrator holds on its own issue.
//
// FIN-265's requireCurrentIssueRunLease rejected every task-token write whose
// target differed from the task's issue with 403 "task does not own this issue",
// which broke cross-issue orchestration wholesale (a chief-of-staff run bound to
// issue X could no longer assign or status issue Y). The fix returns the guard to
// its stated purpose — serialising canonical writers on ONE issue — by letting a
// cross-issue write fall through to the ordinary authz.
//
// Reaching the guard requires X-Actor-Source: task_token, which the daemon stamps
// on every agent run. The existing cross-issue tests set X-Task-ID but not that
// header, so the lease guard is a no-op there; this test sets it, which is the
// only way in.
func TestIssueRunLease_CrossIssueOrchestrationWritePassesGuard(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}

	coordinator := dbfx.Agent(t, "FIN-290 coordinator", handlerTestRuntimeID(t))
	ownIssue := dbfx.Issue(t, "FIN-290 the coordinator's own issue")
	otherIssue := dbfx.Issue(t, "FIN-290 an issue the coordinator orchestrates")

	// The coordinator's live run is bound to ownIssue — so it holds (at most) a
	// lease on ownIssue, and none on otherIssue.
	taskOnOwnIssue := dbfx.Task(t, coordinator, testutil.Cols{
		"runtime_id":          handlerTestRuntimeID(t),
		"issue_id":            ownIssue,
		"status":              "running",
		"originator_user_id":  testUserID,
		"accountable_user_id": testUserID,
	})

	// A task-token write to otherIssue. Priority is a plain, membership-authorized
	// field, so it isolates the run-lease guard from assign/invoke authority.
	req := asRun(newRequest(http.MethodPatch, "/api/issues/"+otherIssue, map[string]any{
		"priority": "high",
	}), coordinator, taskOnOwnIssue)
	req.Header.Set("X-Actor-Source", "task_token")

	// Before the fix this is 403 "task does not own this issue". After it, the
	// guard defers to membership authz and the orchestration write succeeds.
	testutil.Call(t, testHandler.UpdateIssue,
		testutil.WithURLParams(req, "id", otherIssue)).
		Want(http.StatusOK)
}
