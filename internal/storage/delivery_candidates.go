package storage

import (
	"context"
	"fmt"
)

// DeliveryCandidateCursor is the exclusive (seq, id) position of the last loop
// returned by ListDeliveryCandidates. Seq alone is not unique for seeded or
// imported rows, so id breaks ties.
type DeliveryCandidateCursor struct {
	Seq int64
	ID  string
}

// ListDeliveryCandidates returns at most limit loops of one project and type
// that carry a non-null top-level metadata value under evidenceKey (a fixed
// identifier chosen by the caller, never user input), have a pull request number,
// and are not yet marked deliveryReconcile.done for their current pull request
// and delivery head (a loop that later records a different PR, or re-publishes
// a different implementation head, becomes a candidate again). Rows are ordered by (seq, id)
// after the cursor so callers can page fairly through a large history without
// loading every loop. Rows with invalid metadata JSON are skipped, not fatal.
func (r *LoopsRepository) ListDeliveryCandidates(ctx context.Context, projectID, loopType, evidenceKey string, after DeliveryCandidateCursor, limit int64) ([]LoopRecord, error) {
	if limit <= 0 {
		return []LoopRecord{}, nil
	}
	rows, err := r.q.QueryContext(ctx, `
		SELECT * FROM loops
		WHERE project_id = ? AND type = ? AND pr_number IS NOT NULL
			AND (seq > ? OR (seq = ? AND id > ?))
			AND COALESCE(CASE WHEN json_valid(metadata_json) THEN json_type(metadata_json, '$.' || ?) END, 'null') <> 'null'
			AND NOT COALESCE(CASE WHEN json_valid(metadata_json) THEN
				json_extract(metadata_json, '$.deliveryReconcile.done') = 1
				AND json_extract(metadata_json, '$.deliveryReconcile.prNumber') = pr_number
				AND COALESCE(json_extract(metadata_json, '$.deliveryReconcile.recordHeadSha'), '') = COALESCE(json_extract(metadata_json, '$.implementationDelivery.headSha'), '')
			END, 0)
		ORDER BY seq ASC, id ASC
		LIMIT ?`, projectID, loopType, after.Seq, after.Seq, after.ID, evidenceKey, limit)
	if err != nil {
		return nil, fmt.Errorf("list delivery candidates: %w", err)
	}
	defer rows.Close()
	return scanLoops(rows)
}

// ListWorkerLoopsForIssue returns one project's worker loops that target the
// issue directly or whose persisted work input or delivery record names the
// issue number. Callers still match the repository: the SQL filter only narrows
// rows so ownership checks do not load the whole loop history.
func (r *LoopsRepository) ListWorkerLoopsForIssue(ctx context.Context, projectID, repo string, issueNumber int64) ([]LoopRecord, error) {
	rows, err := r.q.QueryContext(ctx, `
		SELECT * FROM loops
		WHERE project_id = ? AND type = 'worker' AND (
			(target_type = 'issue' AND target_id = ? COLLATE NOCASE)
			OR COALESCE(CASE WHEN json_valid(metadata_json) THEN
				json_extract(metadata_json, '$.worker.issueNumber') = ?
				OR json_extract(metadata_json, '$.implementationDelivery.issueNumber') = ?
			END, 0)
		)
		ORDER BY seq ASC, id ASC`, projectID, fmt.Sprintf("issue:%s:%d", repo, issueNumber), issueNumber, issueNumber)
	if err != nil {
		return nil, fmt.Errorf("list worker loops for issue: %w", err)
	}
	defer rows.Close()
	return scanLoops(rows)
}

// ListPlannerLoopsForIssue returns one project's Planner loops that target the
// issue exactly. Callers still compare the repository column.
func (r *LoopsRepository) ListPlannerLoopsForIssue(ctx context.Context, projectID, repo string, issueNumber int64) ([]LoopRecord, error) {
	rows, err := r.q.QueryContext(ctx, `
		SELECT * FROM loops
		WHERE project_id = ? AND type = 'planner' AND target_type = 'issue' AND target_id = ? COLLATE NOCASE
		ORDER BY seq ASC, id ASC`, projectID, fmt.Sprintf("issue:%s:%d", repo, issueNumber))
	if err != nil {
		return nil, fmt.Errorf("list planner loops for issue: %w", err)
	}
	defer rows.Close()
	return scanLoops(rows)
}
