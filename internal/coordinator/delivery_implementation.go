package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nexu-io/looper/internal/delivery"
	githubinfra "github.com/nexu-io/looper/internal/infra/github"
	"github.com/nexu-io/looper/internal/storage"
)

// reconcileImplementation advances one Worker loop whose published
// implementationDelivery record is the authority for what that loop delivered.
//
// Ordering here is deliberate:
//  1. prove the record verifies against project, repository, issue and PR;
//  2. prove the validation reference resolves to a real passing validation of
//     the published head in this loop's own runs (a resumed run may own the
//     publication while an earlier run owns the validation);
//  3. read remote state and trust only verified merge, head, base and source;
//  4. record implementationMerged, independently of whether the issue may be
//     closed;
//  5. reconcile labels so nothing can re-dispatch, then close only a fully
//     resolved issue, then persist the final reconciled marker.
//
// A failure at any step leaves the candidate eligible, so a partial failure is
// retried instead of being lost or reported as complete.
func (d *deliveryReconciler) reconcileImplementation(ctx context.Context, loop storage.LoopRecord) error {
	loopPR := deliveryDerefInt64(loop.PRNumber)
	if loop.Type != "worker" || loop.ProjectID != d.projectID {
		return d.markDone(ctx, loop, deliveryReconcileState{Kind: "implementation", Outcome: "ignored", Reason: "not-a-worker-loop", PRNumber: loopPR}, nil)
	}
	record, err := delivery.FromMetadata(loop.MetadataJSON)
	if err != nil {
		return d.markDone(ctx, loop, deliveryReconcileState{Kind: "implementation", Outcome: "ignored", Reason: "invalid-delivery-metadata", PRNumber: loopPR}, nil)
	}
	if record == nil {
		return d.markDone(ctx, loop, deliveryReconcileState{Kind: "implementation", Outcome: "ignored", Reason: "no-delivery-record", PRNumber: loopPR}, nil)
	}
	state := deliveryReconcileState{Kind: "implementation", PRNumber: loopPR, IssueNumber: record.IssueNumber, HeadSHA: record.HeadSHA, RecordHeadSHA: record.HeadSHA}
	if !record.Published || record.MetadataPending {
		// Evidence is not sufficient yet: publication is still being written.
		// Wait without writing anything, so nothing is claimed or finished early.
		return nil
	}
	// Association is decided before any remote read and before any effect.
	if record.WorkerLoopID != loop.ID || record.PRNumber != loopPR || !record.VerifiedFor(d.projectID, d.repo, record.IssueNumber, record.PRNumber) {
		state.Outcome, state.Reason = "ignored", "unverified-delivery-binding"
		return d.markDone(ctx, loop, state, nil)
	}
	validated, err := d.validationResolves(ctx, loop.ID, *record)
	if err != nil {
		return err
	}
	if !validated {
		state.Outcome, state.Reason = "ignored", "unverified-validation-reference"
		// The marker below is terminal for this head, so this line is the only
		// operator-visible signal that the issue will not close itself. Name the
		// cause: a project that runs no validation commands records no reference
		// at all, while a recorded reference that no longer resolves is an anomaly.
		detail := "the recorded validation reference does not resolve to a passing checkpoint on this head"
		if strings.TrimSpace(record.ValidationRef) == "" {
			detail = "no validation reference was recorded for this delivery"
		}
		if d.runner.logger != nil {
			d.runner.logger.Warn("worker delivery cannot close its issue automatically", map[string]any{
				"loopId":      loop.ID,
				"repo":        d.repo,
				"issueNumber": record.IssueNumber,
				"prNumber":    record.PRNumber,
				"headSha":     record.HeadSHA,
				"detail":      detail,
			})
		}
		return d.markDone(ctx, loop, state, nil)
	}

	pr, err := d.gateway.DeliveryPullRequest(ctx, d.repo, record.PRNumber, d.cwd)
	if err != nil {
		return fmt.Errorf("read implementation pull request #%d: %w", record.PRNumber, err)
	}
	if pr.Number != record.PRNumber || !strings.EqualFold(strings.TrimSpace(pr.Repo), d.repo) {
		state.Outcome, state.Reason = "ignored", "misassociated-pull-request"
		return d.markDone(ctx, loop, state, nil)
	}
	// Git branch names are case-sensitive: only the exact recorded base counts.
	if base := strings.TrimSpace(record.BaseBranch); base == "" || strings.TrimSpace(pr.BaseBranch) != base {
		state.Outcome, state.Reason = "ignored", "base-mismatch"
		return d.markDone(ctx, loop, state, nil)
	}
	switch strings.ToUpper(strings.TrimSpace(pr.State)) {
	case delivery.PullRequestMerged:
	case delivery.PullRequestClosed:
		state.Outcome, state.Reason = "ignored", "pull-request-closed-unmerged"
		return d.markDone(ctx, loop, state, nil)
	default:
		// Open (or unknown): publication alone closes nothing.
		return nil
	}
	if strings.TrimSpace(pr.MergeSHA) == "" || !pr.BaseContainsMerge {
		// Merged somewhere the target branch does not contain: not delivered yet.
		return nil
	}

	// The merge is real from here on. Record it independently of closability.
	merged := &implementationMergedState{
		PRNumber:    record.PRNumber,
		MergeSHA:    pr.MergeSHA,
		HeadSHA:     pr.HeadSHA,
		BaseBranch:  pr.BaseBranch,
		IssueNumber: record.IssueNumber,
		MergedAt:    d.runner.nowISO(),
	}
	state.MergeSHA = pr.MergeSHA
	state.HeadSHA = pr.HeadSHA
	previous := readDeliveryReconcileState(deliveryMetadataObject(loop.MetadataJSON))
	if previous.PRNumber == state.PRNumber {
		state.CloseRequestedAt = previous.CloseRequestedAt
	}

	if !record.Complete {
		// A partial delivery may be merged, and that fact is durable, but it never
		// authorises closing: only a fully resolved, validated delivery does.
		merged.Outcome = "partial-delivery"
		state.Outcome, state.Reason = "merged", "partial-delivery"
		return d.writeDeliveryState(ctx, loop.ID, state, merged)
	}

	issue, err := d.github.ViewIssue(ctx, githubinfra.ViewIssueInput{Repo: d.repo, IssueNumber: record.IssueNumber, CWD: d.cwd})
	if err != nil {
		// The merge fact stands on its own; keep it even though closability is
		// undecided this tick.
		merged.Outcome = "issue-read-failed"
		state.Outcome, state.Reason = "merged", "issue-read-failed"
		readErr := fmt.Errorf("read issue #%d: %w", record.IssueNumber, err)
		if writeErr := d.writeDeliveryState(ctx, loop.ID, state, merged); writeErr != nil {
			return errors.Join(readErr, writeErr)
		}
		return readErr
	}
	decision, err := d.closeDecision(ctx, loop, *record, pr, issue, state.CloseRequestedAt)
	if err != nil {
		return err
	}
	switch decision {
	case deliveryCloseIssue:
	case deliveryCloseMismatch:
		// Wrong association: record the merge fact, touch nothing else.
		merged.Outcome = string(decision)
		state.Outcome, state.Reason = "merged", string(decision)
		return d.markDone(ctx, loop, state, merged)
	case deliveryCloseDeferred:
		merged.Outcome = "deferred"
		state.Outcome, state.Reason = "merged", decision.reason()
		// Keep the merge fact durable but stay eligible: a hold or in-flight role
		// may end, and then the close becomes possible.
		return d.writeDeliveryState(ctx, loop.ID, state, merged)
	case deliveryCloseHeadChanged:
		// The merged revision is not the verified one: record the fact only.
		merged.Outcome = string(decision)
		state.Outcome, state.Reason = "merged", string(decision)
		return d.markDone(ctx, loop, state, merged)
	default:
		// The issue is closed (by us, externally, or not planned) or a human
		// reopened it after our close. Never reopen and never close again. When
		// the verified head is what merged and nothing is held, clean stale
		// stage labels so a later manual reopen cannot silently re-dispatch.
		merged.Outcome = string(decision)
		state.Outcome, state.Reason = "merged", string(decision)
		state.IssueClosed = decision == deliveryCloseClosedByUs
		if !strings.EqualFold(strings.TrimSpace(pr.HeadSHA), record.HeadSHA) {
			return d.markDone(ctx, loop, state, merged)
		}
		if decision != deliveryCloseReopened {
			held, err := d.closeHeld(ctx, loop.ID, *record, pr, issue.Labels)
			if err != nil {
				return err
			}
			if held {
				// Hold labels are human controls: keep the fact, retry cleanup later.
				return d.writeDeliveryState(ctx, loop.ID, state, merged)
			}
			if err := d.cleanDeliveredLabels(ctx, issue.Labels, record.IssueNumber); err != nil {
				return err
			}
			state.LabelsReconciled = true
		}
		return d.markDone(ctx, loop, state, merged)
	}

	// Labels first: removing dispatch and trigger labels stops any new work from
	// starting on an issue that is about to close, and a human who reopens it
	// later does not find it silently re-dispatched.
	if err := d.cleanDeliveredLabels(ctx, issue.Labels, record.IssueNumber); err != nil {
		return err
	}
	state.LabelsReconciled = true
	// Persist the close intent before the close. If the process dies after the
	// close but before the final marker, the next tick finds the intent and the
	// closed issue and finishes; if a human reopened it in between, the intent
	// plus the reopen event stops us from closing it again.
	if state.CloseRequestedAt == "" {
		state.CloseRequestedAt = d.runner.nowISO()
		state.Outcome, state.Reason = "merged", "close-requested"
		merged.Outcome = "close-requested"
		if err := d.writeDeliveryState(ctx, loop.ID, state, merged); err != nil {
			return err
		}
		if !d.closeIntentPersisted(ctx, loop.ID, state.PRNumber, state.CloseRequestedAt) {
			// Lost a CAS race with another writer; retry on a later tick.
			return nil
		}
	}
	if err := d.gateway.CloseDeliveredIssue(ctx, d.repo, record.IssueNumber, d.cwd); err != nil {
		return fmt.Errorf("close delivered issue #%d: %w", record.IssueNumber, err)
	}
	merged.Outcome = "closed"
	state.Outcome, state.Reason = "closed", ""
	state.IssueClosed = true
	return d.markDone(ctx, loop, state, merged)
}

// cleanDeliveredLabels removes dispatch and trigger labels from a delivered
// issue. It runs for closed and to-be-closed issues alike: labels on a finished
// issue are stale, and a manual reopen must not silently re-dispatch.
func (d *deliveryReconciler) cleanDeliveredLabels(ctx context.Context, labels []string, issueNumber int64) error {
	remove := d.closedIssueStaleLabels(labels)
	if len(remove) == 0 {
		return nil
	}
	if err := d.github.RemoveIssueLabels(ctx, githubinfra.IssueLabelsInput{Repo: d.repo, IssueNumber: issueNumber, Labels: remove, CWD: d.cwd}); err != nil {
		return fmt.Errorf("remove dispatch labels from issue #%d: %w", issueNumber, err)
	}
	return nil
}

func (d *deliveryReconciler) closeIntentPersisted(ctx context.Context, loopID string, prNumber int64, requestedAt string) bool {
	current, err := d.runner.repos.Loops.GetByID(ctx, loopID)
	if err != nil || current == nil {
		return false
	}
	persisted := readDeliveryReconcileState(deliveryMetadataObject(current.MetadataJSON))
	return persisted.PRNumber == prNumber && persisted.CloseRequestedAt == requestedAt
}

// closeDecisionKind is the outcome of deciding whether one verified merged
// delivery fully resolves its issue.
type closeDecisionKind string

const (
	deliveryCloseIssue       closeDecisionKind = "close"
	deliveryCloseClosedByUs  closeDecisionKind = "closed"
	deliveryCloseDeferred    closeDecisionKind = "deferred"
	deliveryCloseHeadChanged closeDecisionKind = "head-changed"
	deliveryCloseIssueClosed closeDecisionKind = "issue-already-closed"
	deliveryCloseNotPlanned  closeDecisionKind = "issue-closed-not-planned"
	deliveryCloseReopened    closeDecisionKind = "issue-reopened-by-human"
	deliveryCloseMismatch    closeDecisionKind = "issue-mismatch"
	// deliveryCloseClosedUnknown is a closed issue whose reason is missing or
	// unrecognised: neither ours nor a completed delivery.
	deliveryCloseClosedUnknown closeDecisionKind = "issue-closed-unknown-reason"
)

func (d closeDecisionKind) reason() string {
	if d == deliveryCloseDeferred {
		return "held-or-in-flight"
	}
	return string(d)
}

// closeDecision decides closability from remote evidence only. Anything that
// cannot be proven to be a complete, single, verified delivery of this issue
// stays open; a live issue read that lacks its number or state fails closed.
func (d *deliveryReconciler) closeDecision(ctx context.Context, loop storage.LoopRecord, record delivery.Record, pr delivery.PullRequest, issue githubinfra.IssueDetail, closeRequestedAt string) (closeDecisionKind, error) {
	if issue.IsPullRequest || (issue.Number != 0 && issue.Number != record.IssueNumber) {
		return deliveryCloseMismatch, nil
	}
	if issue.Number == 0 {
		return deliveryCloseDeferred, nil
	}
	switch strings.ToLower(strings.TrimSpace(issue.State)) {
	case "closed":
		// Never touch a closed issue's state or reason. A not_planned close is a
		// human decision; recording it as a completed delivery misstates reality.
		// Only a completed close that follows our durable intent is ours.
		switch strings.ToLower(strings.TrimSpace(issue.StateReason)) {
		case "not_planned":
			return deliveryCloseNotPlanned, nil
		case "completed":
			if closeRequestedAt != "" {
				return deliveryCloseClosedByUs, nil
			}
			return deliveryCloseIssueClosed, nil
		default:
			return deliveryCloseClosedUnknown, nil
		}
	case "open":
	default:
		// Unknown live state: do not act on it.
		return deliveryCloseDeferred, nil
	}
	if !strings.EqualFold(strings.TrimSpace(pr.HeadSHA), record.HeadSHA) {
		// The merged revision is not the published, validated revision.
		return deliveryCloseHeadChanged, nil
	}
	if deliveryLoopInFlight(loop.Status) {
		// The delivering Worker itself is running again (for example pushing a
		// follow-up); its published record is not final.
		return deliveryCloseDeferred, nil
	}
	if closeRequestedAt != "" {
		reopened, err := d.reopenedSince(ctx, record.IssueNumber, closeRequestedAt)
		if err != nil {
			return "", err
		}
		if reopened {
			return deliveryCloseReopened, nil
		}
	}
	held, err := d.closeHeld(ctx, loop.ID, record, pr, issue.Labels)
	if err != nil {
		return "", err
	}
	if held {
		return deliveryCloseDeferred, nil
	}
	return deliveryCloseIssue, nil
}

// closeHeld reports every human or role control that must keep the issue open:
// hold labels on the issue or on the implementation PR, reviewer/fixer work or
// scope/budget holds on the implementation PR and on every spec PR the issue's
// Planner loops published, a hold label on such a spec PR, and any other Worker
// still running for the issue.
func (d *deliveryReconciler) closeHeld(ctx context.Context, loopID string, record delivery.Record, pr delivery.PullRequest, issueLabels []string) (bool, error) {
	if d.issueHeld(issueLabels) || d.issueHeld(pr.Labels) {
		return true, nil
	}
	inFlight, err := d.reviewFixInFlight(ctx, record.PRNumber)
	if err != nil || inFlight {
		return inFlight, err
	}
	specPRs, err := d.plannerPullRequestsForIssue(ctx, record.IssueNumber)
	if err != nil {
		return false, err
	}
	for _, specPR := range specPRs {
		if specPR == record.PRNumber {
			continue
		}
		inFlight, err := d.reviewFixInFlight(ctx, specPR)
		if err != nil || inFlight {
			return inFlight, err
		}
		remote, err := d.gateway.DeliveryPullRequest(ctx, d.repo, specPR, d.cwd)
		if err != nil {
			return false, fmt.Errorf("read spec pull request #%d for hold check: %w", specPR, err)
		}
		if d.issueHeld(remote.Labels) {
			return true, nil
		}
	}
	active, err := d.workerInFlight(ctx, loopID, record.IssueNumber)
	if err != nil {
		return false, err
	}
	return active, nil
}

// plannerPullRequestsForIssue lists the spec pull requests recorded by this
// project's Planner loops for the issue. Only exact issue targets count.
func (d *deliveryReconciler) plannerPullRequestsForIssue(ctx context.Context, issueNumber int64) ([]int64, error) {
	list, err := d.runner.repos.Loops.ListPlannerLoopsForIssue(ctx, d.projectID, d.repo, issueNumber)
	if err != nil {
		return nil, err
	}
	seen := map[int64]struct{}{}
	out := []int64{}
	for _, loop := range list {
		pr := deliveryDerefInt64(loop.PRNumber)
		if pr <= 0 || !strings.EqualFold(deliveryDerefString(loop.Repo), d.repo) {
			continue
		}
		if _, ok := seen[pr]; ok {
			continue
		}
		seen[pr] = struct{}{}
		out = append(out, pr)
	}
	return out, nil
}

// reopenedSince reports whether the issue has a reopened timeline event at or
// after our persisted close intent. It is consulted only when a close intent is
// already durable, so ordinary reconciliation does not read timelines.
func (d *deliveryReconciler) reopenedSince(ctx context.Context, issueNumber int64, since string) (bool, error) {
	requestedAt, ok := parseCoordinatorTime(since)
	if !ok {
		return true, nil
	}
	events, err := d.github.ListIssueTimeline(ctx, githubinfra.IssueTimelineInput{Repo: d.repo, IssueNumber: issueNumber, CWD: d.cwd})
	if err != nil {
		return false, fmt.Errorf("read issue #%d timeline: %w", issueNumber, err)
	}
	for _, event := range events {
		if !strings.EqualFold(asString(event["event"]), "reopened") {
			continue
		}
		at, ok := parseCoordinatorTime(asString(event["created_at"]))
		if !ok || !at.Before(requestedAt.Truncate(time.Second)) {
			return true, nil
		}
	}
	return false, nil
}

// closedIssueStaleLabels drops the labels that would dispatch more work after an
// issue is delivered: dispatch state and every role trigger we know about.
func (d *deliveryReconciler) closedIssueStaleLabels(labels []string) []string {
	patterns := []string{"dispatch/*", "looper:worker-ready", "looper:spec-ready"}
	patterns = append(patterns, requiredTriggerLabels(d.roles.Planner.Triggers)...)
	patterns = append(patterns, requiredTriggerLabels(d.roles.Worker.Triggers)...)
	patterns = append(patterns, requiredPRTriggerLabels(d.roles.Fixer.Triggers)...)
	return matchingLabels(labels, patterns)
}

// workerCheckpointValidation is the subset of the Worker checkpoint that proves
// a validation pass. It mirrors worker.ValidationResult's JSON names without
// importing the worker package.
type workerCheckpointValidation struct {
	Validation *struct {
		RunID         string   `json:"runId"`
		Commands      []string `json:"commands"`
		Passed        bool     `json:"passed"`
		ValidatedHead string   `json:"validatedHead"`
	} `json:"validation"`
}

// validationResolves checks that record.ValidationRef names a validation of
// record.HeadSHA that actually happened in this loop.
//
// The publication step can resume in a later run while the validation evidence
// belongs to an earlier run, so the reference is authoritative only when the
// referenced run of this loop, or a run of this loop holding that validation,
// proves a passing validation of the published head with real commands.
func (d *deliveryReconciler) validationResolves(ctx context.Context, loopID string, record delivery.Record) (bool, error) {
	reference := strings.TrimSpace(record.ValidationRef)
	refRun, ok := strings.CutSuffix(reference, ":validate")
	if !ok || strings.TrimSpace(refRun) == "" {
		return false, nil
	}
	if d.runner.repos.Runs == nil {
		return false, nil
	}
	runs, err := d.runner.repos.Runs.ListByLoop(ctx, loopID)
	if err != nil {
		return false, err
	}
	refRunInLoop, recordRunInLoop := false, false
	for _, run := range runs {
		if run.LoopID != loopID {
			continue
		}
		refRunInLoop = refRunInLoop || run.ID == refRun
		recordRunInLoop = recordRunInLoop || run.ID == record.RunID
	}
	if !refRunInLoop || !recordRunInLoop {
		return false, nil
	}
	for _, run := range runs {
		if run.LoopID != loopID || run.CheckpointJSON == nil {
			continue
		}
		var checkpoint workerCheckpointValidation
		if json.Unmarshal([]byte(*run.CheckpointJSON), &checkpoint) != nil || checkpoint.Validation == nil {
			continue
		}
		validation := checkpoint.Validation
		if !validation.Passed || len(validation.Commands) == 0 || !strings.EqualFold(strings.TrimSpace(validation.ValidatedHead), record.HeadSHA) {
			continue
		}
		owner := strings.TrimSpace(validation.RunID)
		if owner == refRun || (owner == "" && run.ID == refRun) {
			return true, nil
		}
	}
	return false, nil
}
