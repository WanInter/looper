package planner

import (
	"testing"

	"github.com/nexu-io/looper/internal/config"
)

// Unassigned issues are treated as assigned to the current user; only issues
// explicitly assigned to someone else are skipped.
func TestShouldClaimIssueUnassignedCountsAsCurrentUser(t *testing.T) {
	policy := DiscoveryPolicy{RequireAssigneeCurrentUser: true, Labels: []string{"looper:plan"}, LabelMode: config.LabelModeAll}
	cases := []struct {
		name      string
		assignees []string
		want      bool
	}{
		{"unassigned_is_claimed", nil, true},
		{"assigned_to_current_user_is_claimed", []string{"mageia"}, true},
		{"assigned_to_other_is_skipped", []string{"someone-else"}, false},
	}
	for _, tc := range cases {
		issue := IssueSummary{Assignees: tc.assignees, Labels: []string{"looper:plan"}}
		if got := shouldClaimIssue(issue, "mageia", policy); got != tc.want {
			t.Errorf("%s: shouldClaimIssue = %v, want %v", tc.name, got, tc.want)
		}
	}
}
