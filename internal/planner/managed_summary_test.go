package planner

import (
	"strings"
	"testing"

	"github.com/nexu-io/looper/internal/infra/specpr"
)

func TestPlanLinksSurviveRemovalOfManagedSummary(t *testing.T) {
	issue := checkpointIssue{Repo: "acme/app", IssueNumber: 21, SpecPath: "specs/21.md", URL: "https://github.com/acme/app/issues/21"}
	body := buildPullRequestBody(issue, checkpointWorktree{Branch: "plan/21"}, &checkpointWriteSpec{Summary: "Planning only."})
	_, after, ok := strings.Cut(body, specpr.ManagedPlanEnd)
	if !strings.HasPrefix(body, specpr.ManagedPlanStart) || !ok || strings.Contains(after, "Planning only.") || specpr.ParseSpecPathFromPullRequestBody(after) != issue.SpecPath || !strings.Contains(after, "Issue: acme/app#21") {
		t.Fatalf("worker cannot replace plan summary without losing links: %s", body)
	}
}
