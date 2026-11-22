package reviewer

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/nexu-io/looper/internal/domain"
	githubinfra "github.com/nexu-io/looper/internal/infra/github"
	"github.com/nexu-io/looper/internal/loops"
	"github.com/nexu-io/looper/internal/reviewer/automerge"
	"github.com/nexu-io/looper/internal/reviewer/criteria"
	"github.com/nexu-io/looper/internal/storage"
)

const (
	metadataMergeProof      = "lastPublishedAutoMergeProof"
	metadataMergeReconciled = "lastAutoMergeReconciledReviewId"
	mergeProofRejected      = "rejected"
	mergeProofNoop          = "clean_noop"
	mergeProofNative        = "clean_native"
)

func autoMergePRCandidate(head, state string, draft, conflicted bool, labels []string) bool {
	return head != "" && normalizePRState(state) == "open" && !draft && !conflicted &&
		!domain.IsAutoLaneHeld(domain.LoopTypeReviewer, labels) && resolvePullRequestPhase(labels) != "spec" && automerge.HasLooperLabel(labels)
}

func mergeLoopHeld(loop storage.LoopRecord) bool {
	return terminalReviewerLoopReason(loop) != "" || loops.IsReviewFixPairHold(loop) || loops.HasPendingReviewScopeHuman(loop)
}

func mergePublicationID(loop storage.LoopRecord, head string) string {
	if head == "" || mergeLoopHeld(loop) {
		return ""
	}
	meta := parseJSONObject(loop.MetadataJSON)
	if meta["lastPublishedHeadSha"] != head {
		return ""
	}
	if proof, ok := meta[metadataMergeProof]; ok && proof != mergeProofNoop && proof != mergeProofNative {
		return ""
	}
	id, _ := stringFromAny(meta[metadataLastPublishedReviewIDKey])
	return id
}

func mergeCleanCompletion(pending pendingReviewCheckpoint) bool {
	if pending.Outcome != "clean" {
		return false
	}
	if pending.ReviewerSummaryJSON == "" {
		return true
	}
	var completion reviewerCommentOnlyCompletion
	return json.Unmarshal([]byte(pending.ReviewerSummaryJSON), &completion) == nil &&
		(completion.Outcome == "" || completion.Outcome == "clean") &&
		len(publishableCommentOnlyFindings(completion.Findings)) == 0 && !commentOnlyCompletionHasNeedsHuman(completion)
}

func mergeCleanMarker(marker ReviewMarkerResult) bool {
	return marker.Found && marker.Outcome == "clean" && len(marker.InlineCommentBodies) == 0 &&
		(marker.Event == ReviewEventApprove || marker.Event == ReviewEventComment)
}

// Publication metadata identifies the review; the completed publish checkpoint
// supplies the original phase and findings, even after subsequent skipped runs.
func (r *Runner) sameHeadMergePublication(ctx context.Context, loop storage.LoopRecord, head, id string) (*reviewerCheckpoint, time.Time, error) {
	runs, err := r.repos.Runs.ListByLoop(ctx, loop.ID)
	if err != nil {
		return nil, time.Time{}, err
	}
	for _, run := range runs {
		checkpoint := parseCheckpoint(run.CheckpointJSON)
		pending := checkpoint.PendingReview
		if pending == nil || pending.HeadSHA != head {
			continue
		}
		// Do not let an older publication override later same-head blocking evidence.
		if !mergeCleanCompletion(*pending) {
			return nil, time.Time{}, nil
		}
		if pending.IdempotencyKey != id || run.Status != "success" || derefString(run.LastCompletedStep) != string(stepPublish) || checkpoint.SkipReason != "" {
			continue
		}
		// The legacy native non-noop path overwrote Detail during publish;
		// it cannot establish that the agent reviewed the implementation phase.
		if _, hasProof := parseJSONObject(loop.MetadataJSON)[metadataMergeProof]; !hasProof && !pending.CleanNoop {
			return nil, time.Time{}, nil
		}
		if checkpoint.Detail == nil || checkpoint.Detail.HeadSHA != head || checkpoint.Detail.IsDraft || resolvePullRequestPhase(checkpoint.Detail.Labels) == "spec" {
			return nil, time.Time{}, nil
		}
		return &checkpoint, parseRFC3339OrZero(run.StartedAt), nil
	}
	return nil, time.Time{}, nil
}

func (r *Runner) discoverSameHeadAutoMerge(ctx context.Context, projectID string, loop storage.LoopRecord, pr PullRequestSummary) (bool, error) {
	if !r.reviewerAutoMergeConfigForProject(projectID).Enabled || !autoMergePRCandidate(pr.HeadSHA, pr.State, pr.IsDraft, pr.HasConflicts, pr.Labels) || strings.EqualFold(pr.ReviewDecision, "CHANGES_REQUESTED") {
		return false, nil
	}
	id := mergePublicationID(loop, pr.HeadSHA)
	if id == "" || parseJSONObject(loop.MetadataJSON)[metadataMergeReconciled] == id {
		return false, nil
	}
	prior, _, err := r.sameHeadMergePublication(ctx, loop, pr.HeadSHA, id)
	return prior != nil && !automerge.HasLooperLabel(prior.Detail.Labels) && (prior.Detail.BaseSHA == "" || prior.Detail.BaseSHA == pr.BaseSHA), err
}

// A new or edited substantive COMMENT review has no provider-level blocking
// vote. Old clean evidence cannot judge its intent. An empty review body does
// not block by itself: inline findings are governed by live resolution state.
func hasFreshMergeReviewFeedback(reviews, reviewed []map[string]any, marker ReviewMarkerResult, login string, since time.Time) bool {
	type reviewText struct{ id, author, body string }
	text := func(review map[string]any) reviewText {
		author, _ := review["author"].(map[string]any)
		if author == nil {
			author, _ = review["user"].(map[string]any)
		}
		name, _ := author["login"].(string)
		body, _ := review["body"].(string)
		id, _ := json.Marshal(review["id"])
		return reviewText{string(id), normalizeLogin(name), body}
	}
	known := make(map[reviewText]struct{}, len(reviewed))
	for _, review := range reviewed {
		if review["id"] != nil {
			known[text(review)] = struct{}{}
		}
	}
	for _, review := range reviews {
		state, _ := review["state"].(string)
		dismissed, _ := review["dismissed"].(bool)
		feedback := text(review)
		if dismissed || !(strings.EqualFold(state, "COMMENTED") || strings.EqualFold(state, "COMMENT")) || strings.TrimSpace(feedback.body) == "" {
			continue
		}
		if marker.Found && feedback.body == marker.Body && sameReviewAuthorLogin(feedback.author, login) {
			continue
		}
		for _, field := range []string{"submittedAt", "updatedAt"} {
			if at, _ := review[field].(string); at != "" && !parseRFC3339OrZero(at).Before(since) {
				return true
			}
		}
		// Forgejo reviews do not expose submission time in the adapter.
		if _, ok := known[feedback]; !ok {
			return true
		}
	}
	return false
}

// Reconcile only the merge side effect. Do not publish, react, run criteria
// verification, or count a second review. The provider still enforces branch
// protection and the merge request is pinned to the reviewed commit.
func (r *Runner) reconcileSameHeadAutoMerge(ctx context.Context, input stepInput, head string) error {
	cfg := r.reviewerAutoMergeConfigForProject(input.Project.ID)
	if !cfg.Enabled {
		return nil
	}
	live, err := r.repos.Loops.GetByID(ctx, input.Loop.ID)
	if err != nil || live == nil {
		return err
	}
	id := mergePublicationID(*live, head)
	if id == "" || parseJSONObject(live.MetadataJSON)[metadataMergeReconciled] == id {
		return nil
	}
	prior, reviewedSince, err := r.sameHeadMergePublication(ctx, *live, head, id)
	if err != nil || prior == nil {
		return err
	}
	view := func() (PullRequestDetail, error) {
		return r.github.ViewPullRequest(ctx, ViewPullRequestInput{Repo: input.Repo, PRNumber: input.PRNumber, CWD: input.Project.RepoPath})
	}
	eligible := func(detail PullRequestDetail) bool {
		return detail.HeadSHA == head && autoMergePRCandidate(detail.HeadSHA, detail.State, detail.IsDraft, detail.HasConflicts, detail.Labels) &&
			!strings.EqualFold(detail.ReviewDecision, "CHANGES_REQUESTED") &&
			reviewerPublishDriftReason(input, *prior, detail) == ""
	}
	detail, err := view()
	if githubinfra.IsPullRequestNotFoundError(err) {
		return nil
	}
	if err != nil || !eligible(detail) {
		return err
	}
	login, err := r.requireCurrentUserLogin(ctx, input.Project.RepoPath)
	if err != nil {
		return err
	}
	proof, _ := stringFromAny(parseJSONObject(live.MetadataJSON)[metadataMergeProof])
	marker := ReviewMarkerResult{}
	if proof != mergeProofNoop {
		// Use only the exact publication marker, never an arbitrary COMMENT or
		// an older loop-level marker. This also handles pre-fix native reviews.
		marker, err = r.github.FindReviewMarker(ctx, VerifyReviewMarkerInput{Repo: input.Repo, PRNumber: input.PRNumber, CWD: input.Project.RepoPath,
			Marker: agentNativeReviewMarker(live.ID, head, id), AuthorLogin: login, AllowedReviewEvents: []ReviewEvent{ReviewEventApprove, ReviewEventComment}, AllowCleanComment: true})
		if err != nil {
			return err
		}
		if !mergeCleanMarker(marker) {
			return nil
		}
	}
	issueRef, issue, ok, err := r.resolveLinkedIssueForCriteria(ctx, input, detail)
	if err != nil {
		return err
	}
	if !ok || domain.IsAutoLaneHeld(domain.LoopTypeReviewer, issue.Labels) {
		return nil
	}
	for _, criterion := range criteria.Extract(issue.Body) {
		// Only reuse explicit PASS rows from the previously verified review.
		if !strings.Contains(marker.Body, criteriaVerificationHeading) || !strings.Contains("\n"+marker.Body, fmt.Sprintf("\n- **%s** — PASS", criterion)) {
			return nil
		}
	}
	if prior.Snapshot != nil {
		original, ok := parseClosingReference(input.Repo, prior.Snapshot.Body)
		if !ok || !strings.EqualFold(original.Repo, issueRef.Repo) || original.Number != issueRef.Number {
			return nil
		}
	}
	decision, err := r.decideAutoMerge(ctx, input, detail, cfg, issueRef)
	if err != nil {
		return err
	}
	if decision.Reason != "" {
		return nil
	}
	if r.hasNativeThreadCapabilities(input.Project.ID) {
		threads, err := r.listReviewThreadsForSignal(ctx, input.Project.RepoPath, input.Repo, input.PRNumber)
		if err != nil {
			return err
		}
		for _, thread := range threads {
			if !thread.IsResolved {
				return nil
			}
		}
	}
	// The detail view may include only automation comments; read the full PR
	// conversation here. New nonempty comments without a blocking vote are
	// ambiguous, so leave them for human review instead of guessing intent.
	comments, err := r.github.ListIssueComments(ctx, ViewPullRequestInput{Repo: input.Repo, PRNumber: input.PRNumber, CWD: input.Project.RepoPath})
	if err != nil {
		return err
	}
	for _, comment := range comments {
		if strings.TrimSpace(comment.Body) == "" || (marker.Found && comment.Body == marker.Body && sameReviewAuthorLogin(comment.Author, login)) {
			continue
		}
		at := parseRFC3339OrZero(comment.UpdatedAt)
		if at.IsZero() || !at.Before(reviewedSince) {
			return nil
		}
	}
	// Refresh after policy and feedback lookups so label, draft, state and
	// head drift cannot authorize a mutation. Reject link and base changes too.
	fresh, err := view()
	if githubinfra.IsPullRequestNotFoundError(err) {
		return nil
	}
	if err != nil || !eligible(fresh) || fresh.Body != detail.Body || fresh.BaseRefName != detail.BaseRefName {
		return err
	}
	if hasFreshMergeReviewFeedback(fresh.Reviews, prior.Detail.Reviews, marker, login, reviewedSince) {
		return nil
	}
	// Also check the refreshed inline state. For Forgejo this is the only
	// inline feedback source, since it does not use the GitHub thread API.
	for _, comment := range fresh.Comments {
		if resolved, _ := comment["isResolved"].(bool); !resolved {
			return nil
		}
	}
	all, err := r.repos.Loops.List(ctx)
	if err != nil {
		return err
	}
	for _, sibling := range loops.FindSiblingReviewFixLoops(all, *live) {
		if loops.IsReviewFixPairHold(sibling) || loops.HasPendingReviewScopeHuman(sibling) {
			return nil
		}
	}
	live, err = r.repos.Loops.GetByID(ctx, input.Loop.ID)
	if err != nil || live == nil || mergePublicationID(*live, head) != id {
		return err
	}
	if cfg.DirectMerge {
		for _, status := range strings.Split(fresh.ChecksSummary, ",") {
			switch strings.ToUpper(strings.TrimSpace(status)) {
			case "", "SUCCESS", "NEUTRAL", "SKIPPED":
			default:
				return &loopError{message: fmt.Sprintf("Auto-merge reconciliation is waiting for checks on head %s", head), kind: FailureRetryableAfterResume}
			}
		}
	}
	if cfg.DirectMerge && !r.forgejoProject(input.Project.ID) {
		err = r.github.MergePullRequest(ctx, githubinfra.MergePullRequestInput{Repo: input.Repo, PRNumber: input.PRNumber, CWD: input.Project.RepoPath, Strategy: decision.Strategy, HeadSHA: head})
	} else {
		// Forgejo's enable operation is already an immediate, head-bound merge.
		err = r.github.EnableAutoMerge(ctx, githubinfra.EnableAutoMergeInput{Repo: input.Repo, PRNumber: input.PRNumber, CWD: input.Project.RepoPath, Strategy: decision.Strategy, HeadSHA: head})
		if !r.forgejoProject(input.Project.ID) && isAlreadyEnabledAutoMergeError(err) {
			err = nil
		}
	}
	if err != nil {
		return &loopError{message: err.Error(), kind: FailureRetryableAfterResume}
	}
	// Temporary refusals never consume the publication. Only a successful
	// merge/enable (including already enabled) suppresses subsequent attempts.
	_, err = r.updateLoop(ctx, *live, func(updated *storage.LoopRecord) {
		if mergePublicationID(*updated, head) == id {
			if metadata, err := mergeLoopMetadataJSON(updated.MetadataJSON, map[string]any{metadataMergeReconciled: id}); err == nil {
				updated.MetadataJSON = &metadata
			}
		}
	})
	return err
}
