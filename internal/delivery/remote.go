package delivery

// Remote pull request states reported by hosting gateways.
const (
	PullRequestOpen   = "OPEN"
	PullRequestClosed = "CLOSED"
	PullRequestMerged = "MERGED"
)

// PullRequest is the hosting-side delivery evidence for one pull request.
// Repo is the verified owner/name of both the base and head repositories.
// MergeSHA is the canonical commit GitHub produced on the base branch (merge,
// squash, or rebase) and is empty unless State is MERGED. BaseContainsMerge
// reports whether the current base branch tip contains MergeSHA.
type PullRequest struct {
	Number            int64
	State             string
	BaseBranch        string
	HeadSHA           string
	MergeSHA          string
	Repo              string
	HeadBranch        string
	Labels            []string
	BaseContainsMerge bool
}
