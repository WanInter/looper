package storage

import (
	"context"
	"fmt"
)

// ListWorkerDiscoveryIssueLoops loads only the Planner/Worker history that can
// own this issue. PR-retargeted Workers retain the issue binding in metadata.
func (r *LoopsRepository) ListWorkerDiscoveryIssueLoops(ctx context.Context, projectID, repo string, issue int64) ([]LoopRecord, error) {
	rows, err := r.q.QueryContext(ctx, `
		SELECT * FROM loops WHERE project_id = ? AND type IN ('planner', 'worker')
		AND target_type = 'issue' AND target_id = ? COLLATE NOCASE
		UNION ALL
		SELECT * FROM loops WHERE project_id = ? AND type = 'worker'
		AND NOT (target_type = 'issue' AND COALESCE(target_id, '') = ? COLLATE NOCASE)
		AND CASE WHEN json_valid(metadata_json) THEN (
			(json_extract(metadata_json, '$.worker.issueNumber') = ?
			AND COALESCE(NULLIF(trim(json_extract(metadata_json, '$.worker.issueRepo')), ''),
				NULLIF(trim(json_extract(metadata_json, '$.worker.repo')), ''), repo) = ? COLLATE NOCASE)
			OR (json_extract(metadata_json, '$.implementationDelivery.issueNumber') = ?
			AND json_extract(metadata_json, '$.implementationDelivery.repo') = ? COLLATE NOCASE)
		) ELSE 0 END
		ORDER BY updated_at DESC, seq DESC
	`, projectID, fmt.Sprintf("issue:%s:%d", repo, issue), projectID, fmt.Sprintf("issue:%s:%d", repo, issue), issue, repo, issue, repo)
	if err != nil {
		return nil, fmt.Errorf("list worker discovery issue loops: %w", err)
	}
	defer rows.Close()
	return scanLoops(rows)
}

// ListWorkerDiscoveryReviewFixLoops returns reviewer/fixer rows on the exact
// project and PR. Older rows can identify the PR via target_id rather than
// repo/pr_number, so check both persisted representations.
func (r *LoopsRepository) ListWorkerDiscoveryReviewFixLoops(ctx context.Context, projectID, repo string, pr int64) ([]LoopRecord, error) {
	rows, err := r.q.QueryContext(ctx, `
		SELECT * FROM loops WHERE project_id = ? AND type IN ('reviewer', 'fixer') AND target_type = 'pull_request'
		AND (target_id = ? COLLATE NOCASE OR (pr_number = ? AND repo = ? COLLATE NOCASE))
	`, projectID, fmt.Sprintf("pr:%s:%d", repo, pr), pr, repo)
	if err != nil {
		return nil, fmt.Errorf("list worker discovery review/fix loops: %w", err)
	}
	defer rows.Close()
	return scanLoops(rows)
}
