package handler

import (
	"log/slog"
	"net/http"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/logger"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// requireCurrentIssueRunLease fences task-token writes to an issue's own run
// state. Human callers remain governed by membership/revision checks. A machine
// caller writing to the issue its task is bound to must additionally prove it
// owns the current lease generation. A write to any OTHER issue is orchestration
// (assigning writers, promoting stage barriers) and passes through to the
// ordinary membership/revision checks — the run lease exists to serialize
// canonical writers on one issue, and was never scoped to restrict cross-issue
// orchestration.
func (h *Handler) requireCurrentIssueRunLease(w http.ResponseWriter, r *http.Request, issue db.Issue) bool {
	if r.Header.Get("X-Actor-Source") != "task_token" {
		return true
	}

	taskID, err := util.ParseUUID(r.Header.Get("X-Task-ID"))
	if err != nil {
		writeError(w, http.StatusForbidden, "task identity is required for issue writes")
		return false
	}
	task, err := h.Queries.GetAgentTask(r.Context(), taskID)
	if err != nil {
		writeError(w, http.StatusForbidden, "task does not own this issue")
		return false
	}
	// A task holds a run lease only on its own issue. A write to any other issue
	// is orchestration and stays governed by membership authz plus the caller's
	// optimistic revision check, not this lease.
	if !task.IssueID.Valid || task.IssueID != issue.ID {
		return true
	}

	current, err := h.Queries.TaskHoldsCurrentIssueRunLease(r.Context(), taskID)
	if err != nil {
		slog.Warn("check issue run lease failed", append(logger.RequestAttrs(r), "task_id", util.UUIDToString(taskID), "error", err)...)
		writeError(w, http.StatusInternalServerError, "failed to verify issue run lease")
		return false
	}
	if current {
		return true
	}

	if task.IssueLeaseGeneration.Valid {
		if auditErr := h.Queries.RecordIssueRunLeaseAudit(r.Context(), db.RecordIssueRunLeaseAuditParams{
			IssueID:    issue.ID,
			RoleKey:    util.UUIDToString(task.AgentID),
			TaskID:     task.ID,
			Generation: task.IssueLeaseGeneration.Int64,
			Event:      "fenced_write",
			Detail:     pgtype.Text{String: r.Method + " " + r.URL.Path, Valid: true},
		}); auditErr != nil {
			slog.Warn("record fenced issue write failed", append(logger.RequestAttrs(r), "task_id", util.UUIDToString(taskID), "error", auditErr)...)
		}
	}
	slog.Warn("stale issue run write fenced", append(logger.RequestAttrs(r),
		"task_id", util.UUIDToString(taskID),
		"issue_id", util.UUIDToString(issue.ID),
		"role_key", util.UUIDToString(task.AgentID),
	)...)
	writeJSON(w, http.StatusConflict, map[string]any{
		"code":  "stale_issue_run",
		"error": "this task no longer owns the current issue run lease",
	})
	return false
}
