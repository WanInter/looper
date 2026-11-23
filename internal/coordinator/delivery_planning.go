package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/nexu-io/looper/internal/delivery"
	"github.com/nexu-io/looper/internal/domain"
	githubinfra "github.com/nexu-io/looper/internal/infra/github"
	"github.com/nexu-io/looper/internal/storage"
)

// planningEvidence is the persisted, non-prose proof that a Planner published a
// spec pull request for one issue. It is built only from loop rows: the planner
// loop's target, its pull request number, and its recorded spec path.
type planningEvidence struct {
	IssueNumber int64
	SpecPath    string
	PRNumber    int64
	HeadBranch  string
}

// reconcilePlanning advances one Planner loop's spec pull request.
//
// The spec is only treated as delivered when the PR itself is the source of
// that spec (DeliverySpecInPullRequest) and the spec is on the project base
// after the verified merge commit (DeliverySpecOnBase). A persisted spec path
// alone is a template path, not delivery evidence.
func (d *deliveryReconciler) reconcilePlanning(ctx context.Context, loop storage.LoopRecord) error {
	loopPR := deliveryDerefInt64(loop.PRNumber)
	evidence, ok := planningEvidenceFor(loop, d.projectID, d.repo)
	if !ok {
		return d.markDone(ctx, loop, deliveryReconcileState{Kind: "planning", Outcome: "ignored", Reason: "unverified-planner-evidence", PRNumber: loopPR}, nil)
	}
	if domain.IsActiveLoopStatus(domain.LoopStatus(loop.Status)) {
		// The Planner can still push to this PR; its publish state is not final.
		return nil
	}
	confirmation, err := d.plannerPublication(ctx, loop.ID, evidence)
	if err != nil {
		return err
	}
	switch confirmation {
	case plannerPublicationConflict:
		return d.markDone(ctx, loop, deliveryReconcileState{Kind: "planning", Outcome: "ignored", Reason: "conflicting-planner-checkpoint", PRNumber: loopPR, IssueNumber: evidence.IssueNumber}, nil)
	case plannerPublicationMissing:
		// No run recorded publishing this PR. Do not act on metadata alone, and
		// keep the row eligible in case a run is persisted later.
		return nil
	}
	pr, err := d.gateway.DeliveryPullRequest(ctx, d.repo, evidence.PRNumber, d.cwd)
	if err != nil {
		return fmt.Errorf("read spec pull request #%d: %w", evidence.PRNumber, err)
	}
	if pr.Number != evidence.PRNumber || !strings.EqualFold(strings.TrimSpace(pr.Repo), d.repo) || (evidence.HeadBranch != "" && pr.HeadBranch != evidence.HeadBranch) {
		return d.markDone(ctx, loop, deliveryReconcileState{Kind: "planning", Outcome: "ignored", Reason: "misassociated-spec-pr", PRNumber: loopPR, IssueNumber: evidence.IssueNumber}, nil)
	}
	switch strings.ToUpper(strings.TrimSpace(pr.State)) {
	case delivery.PullRequestMerged:
	case delivery.PullRequestClosed:
		// Closed unmerged: nothing was delivered. A replacement PR recorded on
		// this loop makes it a candidate again because done is per PR number.
		return d.markDone(ctx, loop, deliveryReconcileState{Kind: "planning", Outcome: "ignored", Reason: "spec-pr-closed-unmerged", PRNumber: loopPR, IssueNumber: evidence.IssueNumber}, nil)
	default:
		return nil
	}
	if !pr.BaseContainsMerge || strings.TrimSpace(pr.MergeSHA) == "" {
		// Not (yet) reachable from the target branch; nothing landed there.
		return nil
	}
	// Git branch names are case-sensitive, so only the exact base matches.
	if strings.TrimSpace(pr.BaseBranch) != d.baseBranch || (confirmation.baseBranch != "" && confirmation.baseBranch != pr.BaseBranch) {
		return d.markDone(ctx, loop, deliveryReconcileState{Kind: "planning", Outcome: "ignored", Reason: "base-mismatch", PRNumber: loopPR, IssueNumber: evidence.IssueNumber, MergeSHA: pr.MergeSHA}, nil)
	}
	inPullRequest, err := d.gateway.DeliverySpecInPullRequest(ctx, d.repo, evidence.PRNumber, evidence.SpecPath, d.cwd)
	if err != nil {
		return fmt.Errorf("verify spec %q in pull request #%d: %w", evidence.SpecPath, evidence.PRNumber, err)
	}
	if !inPullRequest {
		return d.markDone(ctx, loop, deliveryReconcileState{Kind: "planning", Outcome: "ignored", Reason: "spec-not-in-pull-request", PRNumber: loopPR, IssueNumber: evidence.IssueNumber, MergeSHA: pr.MergeSHA}, nil)
	}
	onBase, err := d.gateway.DeliverySpecOnBase(ctx, d.repo, pr.BaseBranch, evidence.SpecPath, pr.MergeSHA, d.cwd)
	if err != nil {
		return fmt.Errorf("verify spec %q on %s: %w", evidence.SpecPath, pr.BaseBranch, err)
	}
	if !onBase {
		return d.markDone(ctx, loop, deliveryReconcileState{Kind: "planning", Outcome: "ignored", Reason: "spec-not-on-base", PRNumber: loopPR, IssueNumber: evidence.IssueNumber, MergeSHA: pr.MergeSHA}, nil)
	}
	evidence.PRNumber = loopPR
	return d.handOffSpec(ctx, loop, evidence, pr)
}

type plannerPublicationResult struct {
	kind       string
	baseBranch string
}

var (
	plannerPublicationMissing  = plannerPublicationResult{kind: "missing"}
	plannerPublicationConflict = plannerPublicationResult{kind: "conflict"}
)

// plannerCheckpointEvidence is the subset of the Planner checkpoint that
// records what was actually published. It mirrors planner.plannerCheckpoint's
// JSON names; the planner package is not imported to avoid a dependency cycle.
type plannerCheckpointEvidence struct {
	Issue *struct {
		Repo        string `json:"repo"`
		IssueNumber int64  `json:"issueNumber"`
		SpecPath    string `json:"specPath"`
	} `json:"issue"`
	Worktree *struct {
		Branch     string `json:"branch"`
		BaseBranch string `json:"baseBranch"`
		SpecPath   string `json:"specPath"`
	} `json:"worktree"`
	Publish *struct {
		Pushed      bool `json:"pushed"`
		PullRequest *struct {
			Number int64 `json:"number"`
		} `json:"pullRequest"`
	} `json:"publish"`
}

// plannerPublication confirms from the loop's own runs that the latest Planner
// run that published a pull request published evidence.PRNumber for
// evidence.IssueNumber and evidence.SpecPath. Earlier publishing runs are
// superseded (a Planner may replace a closed spec PR), but the latest one must
// agree on PR, issue, repository, spec and branch, or the association is not
// trusted at all.
func (d *deliveryReconciler) plannerPublication(ctx context.Context, loopID string, evidence planningEvidence) (plannerPublicationResult, error) {
	if d.runner.repos.Runs == nil {
		return plannerPublicationMissing, nil
	}
	// ListByLoop returns newest first.
	runs, err := d.runner.repos.Runs.ListByLoop(ctx, loopID)
	if err != nil {
		return plannerPublicationResult{}, err
	}
	for _, run := range runs {
		if run.LoopID != loopID || run.CheckpointJSON == nil {
			continue
		}
		var checkpoint plannerCheckpointEvidence
		if json.Unmarshal([]byte(*run.CheckpointJSON), &checkpoint) != nil {
			continue
		}
		if checkpoint.Publish == nil || checkpoint.Publish.PullRequest == nil || checkpoint.Publish.PullRequest.Number <= 0 {
			continue
		}
		if checkpoint.Issue == nil || checkpoint.Publish.PullRequest.Number != evidence.PRNumber || checkpoint.Issue.IssueNumber != evidence.IssueNumber || strings.TrimSpace(checkpoint.Issue.SpecPath) != evidence.SpecPath || (checkpoint.Issue.Repo != "" && !strings.EqualFold(checkpoint.Issue.Repo, d.repo)) {
			return plannerPublicationConflict, nil
		}
		baseBranch := ""
		if checkpoint.Worktree != nil {
			if (checkpoint.Worktree.SpecPath != "" && checkpoint.Worktree.SpecPath != evidence.SpecPath) || (evidence.HeadBranch != "" && checkpoint.Worktree.Branch != "" && checkpoint.Worktree.Branch != evidence.HeadBranch) {
				return plannerPublicationConflict, nil
			}
			baseBranch = strings.TrimSpace(checkpoint.Worktree.BaseBranch)
		}
		return plannerPublicationResult{kind: "confirmed", baseBranch: baseBranch}, nil
	}
	return plannerPublicationMissing, nil
}

// handOffSpec moves the issue from planning to implementation once the spec is
// verified on base. Every guard here is a reason not to take the issue away from
// someone already working on it.
//
// Label work is unconditional and finishes before the reconciled marker:
// handoff labels are added only when no Worker already owns the issue, stale
// planning labels are always removed, and a partial label failure leaves the
// candidate eligible so the next tick completes it. A Worker that appears
// between the two writes therefore cannot freeze the issue with stale planning
// labels.
func (d *deliveryReconciler) handOffSpec(ctx context.Context, loop storage.LoopRecord, evidence planningEvidence, pr delivery.PullRequest) error {
	state := deliveryReconcileState{Kind: "planning", PRNumber: evidence.PRNumber, IssueNumber: evidence.IssueNumber, MergeSHA: pr.MergeSHA, HeadSHA: pr.HeadSHA}
	if !d.roles.Worker.AutoDiscovery || !d.roles.Reviewer.Behavior.ReviewEvents.AutoImplement {
		// Operator opted out of automatic implementation. Leave the candidate
		// eligible so enabling autoImplement later still hands off.
		return nil
	}
	if d.issueHeld(pr.Labels) {
		// A hold on the spec PR itself is a human control on the handoff.
		return nil
	}
	issue, err := d.github.ViewIssue(ctx, githubinfra.ViewIssueInput{Repo: d.repo, IssueNumber: evidence.IssueNumber, CWD: d.cwd})
	if err != nil {
		return fmt.Errorf("read issue #%d: %w", evidence.IssueNumber, err)
	}
	// Association first, and fail closed on a live read that lost its identity.
	if issue.Number == 0 {
		return nil
	}
	if issue.IsPullRequest || issue.Number != evidence.IssueNumber {
		state.Outcome, state.Reason = "ignored", "misassociated-issue"
		return d.markDone(ctx, loop, state, nil)
	}
	switch strings.ToLower(strings.TrimSpace(issue.State)) {
	case "open":
	case "closed":
		state.Outcome, state.Reason = "ignored", "issue-not-open"
		return d.markDone(ctx, loop, state, nil)
	default:
		// Unknown live state: wait rather than act on it.
		return nil
	}
	if d.issueHeld(issue.Labels) {
		// A hold is temporary and human-controlled: retry after it is lifted.
		return nil
	}
	inFlight, err := d.reviewFixInFlight(ctx, evidence.PRNumber)
	if err != nil {
		return err
	}
	if inFlight {
		// Reviewer/Fixer still owns the spec PR or holds its scope.
		return nil
	}
	delivered, active, err := d.workerOwnership(ctx, evidence.IssueNumber)
	if err != nil {
		return err
	}

	if !delivered && !active {
		add := deliveryMissingLabels(issue.Labels, d.implementationHandoffLabels())
		if len(add) > 0 {
			if err := d.github.AddIssueLabels(ctx, githubinfra.IssueLabelsInput{Repo: d.repo, IssueNumber: evidence.IssueNumber, Labels: add, CWD: d.cwd}); err != nil {
				return fmt.Errorf("add implementation handoff labels to issue #%d: %w", evidence.IssueNumber, err)
			}
		}
	}
	// Stale planning labels are removed even when a Worker already owns the
	// issue: leaving them behind would re-dispatch planning on the next reopen.
	if remove := d.planningStaleLabels(issue.Labels); len(remove) > 0 {
		if err := d.github.RemoveIssueLabels(ctx, githubinfra.IssueLabelsInput{Repo: d.repo, IssueNumber: evidence.IssueNumber, Labels: remove, CWD: d.cwd}); err != nil {
			return fmt.Errorf("remove planning labels from issue #%d: %w", evidence.IssueNumber, err)
		}
	}
	state.Outcome = "handed-off"
	state.LabelsReconciled = true
	switch {
	case delivered:
		state.Reason = "worker-delivered"
	case active:
		state.Reason = "worker-active"
	}
	return d.markDone(ctx, loop, state, nil)
}

// workerOwnership separates the two states that make a handoff unnecessary from
// a Worker loop that merely exists. A loop with a delivery record for this issue
// has delivered; a loop in flight is working. Any other historical Worker loop
// (failed, stopped, never delivered) carries no authority and does not count.
func (d *deliveryReconciler) workerOwnership(ctx context.Context, issueNumber int64) (delivered bool, active bool, err error) {
	list, err := d.runner.repos.Loops.ListWorkerLoopsForIssue(ctx, d.projectID, d.repo, issueNumber)
	if err != nil {
		return false, false, err
	}
	for _, loop := range list {
		if loop.Type != "worker" || loop.ProjectID != d.projectID {
			continue
		}
		record, recordErr := delivery.FromMetadata(loop.MetadataJSON)
		if recordErr != nil {
			record = nil
		}
		ownsRecord := record != nil && record.IssueNumber == issueNumber && strings.EqualFold(record.Repo, d.repo) && record.ProjectID == d.projectID && record.WorkerLoopID == loop.ID
		if !ownsRecord && !deliveryWorkerTracksIssue(loop, d.repo, issueNumber) {
			continue
		}
		switch {
		case ownsRecord && record.VerifiedFor(d.projectID, d.repo, issueNumber, record.PRNumber) && record.PRNumber == deliveryDerefInt64(loop.PRNumber):
			delivered = true
		case ownsRecord && record.MetadataPending:
			// Publication is being written right now: the Worker is still working.
			active = true
		}
		if deliveryLoopInFlight(loop.Status) {
			active = true
		}
	}
	return delivered, active, nil
}

// planningEvidenceFor validates that a planner loop row is durable evidence of a
// published spec PR for this issue and repository. A conflicting pull request
// number between the row and its metadata is treated as untrusted, and prose in
// the issue or PR body is never consulted.
func planningEvidenceFor(loop storage.LoopRecord, projectID, repo string) (planningEvidence, bool) {
	if loop.Type != "planner" || loop.ProjectID != projectID {
		return planningEvidence{}, false
	}
	if !strings.EqualFold(deliveryDerefString(loop.Repo), repo) {
		return planningEvidence{}, false
	}
	metadata := deliveryMetadataObject(loop.MetadataJSON)
	issueNumber := deliveryInt64(metadata["issueNumber"])
	if issueNumber <= 0 {
		return planningEvidence{}, false
	}
	if loop.TargetType != "issue" || !strings.EqualFold(deliveryDerefString(loop.TargetID), fmt.Sprintf("issue:%s:%d", repo, issueNumber)) {
		return planningEvidence{}, false
	}
	specPath := strings.TrimSpace(asString(metadata[plannerSpecPathKey]))
	if specPath == "" {
		return planningEvidence{}, false
	}
	prNumber := deliveryDerefInt64(loop.PRNumber)
	metadataPR := deliveryInt64(metadata["prNumber"])
	switch {
	case prNumber > 0 && metadataPR > 0 && prNumber != metadataPR:
		return planningEvidence{}, false
	case prNumber <= 0:
		prNumber = metadataPR
	}
	if prNumber <= 0 {
		return planningEvidence{}, false
	}
	return planningEvidence{IssueNumber: issueNumber, SpecPath: specPath, PRNumber: prNumber, HeadBranch: strings.TrimSpace(asString(metadata["branch"]))}, true
}
