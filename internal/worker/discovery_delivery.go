package worker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/nexu-io/looper/internal/delivery"
	"github.com/nexu-io/looper/internal/domain"
	"github.com/nexu-io/looper/internal/eventlog"
	"github.com/nexu-io/looper/internal/loops"
	"github.com/nexu-io/looper/internal/storage"
)

// Stored on the replacement loop, not on its source: if the transaction rolls
// back, the recovery allowance has not been spent. One automatic replacement
// per issue prevents a succession of abandoned PRs (or no-op runs).
const discoveryRecoverySource = "workerDiscoveryRecoverySource"

type discoveryPRLookup int64

func (pr discoveryPRLookup) Error() string { return fmt.Sprintf("need current state of PR #%d", pr) }

// undecidedDeliveryHistory reports that one historical worker loop can be
// classified neither as delivery nor as non-delivery from local evidence and
// live remote state. It is not a fault: no stored fact is wrong and no remote
// call failed. Discovery has no authority to start new work on such an issue,
// so discoverIssueDelivery turns this into a skip. Reporting it as an error
// instead would fail the whole scheduler tick on unchanged historical data.
type undecidedDeliveryHistory struct {
	LoopID string
	Reason string
}

func (e undecidedDeliveryHistory) Error() string {
	return fmt.Sprintf("cannot determine delivery from worker loop %s: %s", e.LoopID, e.Reason)
}

// ctx must carry the worker hosting identity but NOT a discovery snapshot.
// Remote observations are fetched outside the SQLite writer transaction; on
// reentry all local decisions are made again against current rows.
func (r *Runner) discoverIssueDelivery(ctx context.Context, project storage.ProjectRecord, repo string, issue IssueSummary, fingerprint string) (DiscoveryResult, error) {
	if r.db == nil {
		return DiscoveryResult{}, fmt.Errorf("worker discovery requires a SQLite transaction")
	}
	states := map[int64]PullRequestDetail{}
	for {
		var enqueued bool
		var result DiscoveryResult
		err := storage.WithTransaction(ctx, r.db, nil, func(tx *sql.Tx) error {
			repos := storage.NewRepositories(tx)
			current, err := repos.Projects.GetByID(ctx, project.ID)
			if err != nil {
				return err
			}
			if current == nil || current.Archived {
				result.Skipped = 1
				return nil
			}
			if current.RepoPath != project.RepoPath {
				return fmt.Errorf("project repository path changed during discovery")
			}
			result, enqueued, err = r.discoverIssueDeliveryTx(ctx, repos, *current, repo, issue, fingerprint, states)
			return err
		})
		if err == nil {
			if enqueued {
				r.wakeSchedulerAfterEnqueue()
			}
			return result, nil
		}
		var undecided undecidedDeliveryHistory
		if errors.As(err, &undecided) {
			// Refuse to start new work, but do not report a scheduler fault: the
			// stored history is unchanged, so retrying reaches the same conclusion.
			// Deciding how such an issue is closed stays with an operator.
			if r.logger != nil {
				r.logger.Info("worker discovery skipped an issue whose delivery history cannot be decided", map[string]any{
					"repo":        repo,
					"issueNumber": issue.Number,
					"loopId":      undecided.LoopID,
					"reason":      undecided.Reason,
				})
			}
			return DiscoveryResult{Skipped: 1}, nil
		}
		var lookup discoveryPRLookup
		if !errors.As(err, &lookup) {
			return DiscoveryResult{}, fmt.Errorf("worker discovery for %s#%d: %w", repo, issue.Number, err)
		}
		pr := int64(lookup)
		// Bound the number of live lookups on inconsistent or extensive history.
		if len(states) >= 8 {
			return DiscoveryResult{}, fmt.Errorf("worker discovery for %s#%d: too many historical PRs to verify", repo, issue.Number)
		}
		detail, err := r.github.ViewPullRequest(ctx, ViewPullRequestInput{Repo: repo, CWD: project.RepoPath, PRNumber: pr})
		if err != nil {
			return DiscoveryResult{}, fmt.Errorf("worker discovery for %s#%d: verify PR #%d: %w", repo, issue.Number, pr, err)
		}
		state := strings.ToLower(strings.TrimSpace(detail.State))
		if detail.Number != pr || (state != "open" && state != "closed" && state != "merged") {
			return DiscoveryResult{}, fmt.Errorf("worker discovery for %s#%d: unknown remote state for PR #%d (number=%d state=%q)", repo, issue.Number, pr, detail.Number, detail.State)
		}
		detail.State = state
		states[pr] = detail
	}
}

func (r *Runner) discoverIssueDeliveryTx(ctx context.Context, repos *storage.Repositories, project storage.ProjectRecord, repo string, issue IssueSummary, fingerprint string, states map[int64]PullRequestDetail) (DiscoveryResult, bool, error) {
	skipped := DiscoveryResult{Skipped: 1}
	target := buildIssueTargetID(repo, issue.Number)
	// The old key collides with PRs of the same number. New discovery always
	// dedupes by source issue even when the executable target is a PR.
	dedupeKey := "worker:" + storage.IssueLockKey(project.ID, strings.ToLower(repo), issue.Number)
	base := firstNonEmpty(derefString(project.BaseBranch), "main")
	work := workerInput{Title: firstNonEmpty(issue.Title, buildDefaultIssueWorkerTitle(repo, issue.Number)), Repo: repo, BaseBranch: base, ExecutionMode: "create-pr", IssueNumber: issue.Number, IssueURL: issue.URL, TriggerLogin: issue.Author, AutoDiscovered: true}
	records, err := repos.Loops.ListWorkerDiscoveryIssueLoops(ctx, project.ID, repo, issue.Number)
	if err != nil {
		return DiscoveryResult{}, false, err
	}
	var planner, reusable *storage.LoopRecord
	var history []storage.LoopRecord
	var pending *storage.QueueItemRecord
	for i := range records {
		loop := &records[i]
		if loop.Type == "planner" {
			if planner == nil {
				planner = loop
			}
			continue
		}
		switch loop.Status {
		case "running", "paused", "held", "awaiting_human", "human_takeover", "waiting", "interrupted", "stopped", "terminated":
			return skipped, false, nil
		}
		active, err := repos.Queue.FindActiveByLoopID(ctx, loop.ID)
		if err != nil {
			return DiscoveryResult{}, false, err
		}
		if active != nil {
			if active.Status == "running" {
				return skipped, false, nil
			}
			pending = active
			if loop.Status == "queued" && loop.TargetType == "issue" {
				reusable = loop // Legacy discovery refreshes issue metadata on a pending loop.
			}
			continue
		}
		latestQueue, err := repos.Queue.GetLatestByLoopID(ctx, loop.ID)
		if err != nil {
			return DiscoveryResult{}, false, err
		}
		if latestQueue != nil && (latestQueue.Status == "manual_intervention" || latestQueue.Status == "cancelled") {
			return skipped, false, nil
		}
		if (loop.Status == "queued" || loop.Status == "idle") && latestQueue == nil {
			run, err := repos.Runs.GetLatestByLoopID(ctx, loop.ID)
			if err != nil {
				return DiscoveryResult{}, false, err
			}
			if run == nil {
				if reusable == nil {
					reusable = loop
				}
				continue
			}
		}
		history = append(history, *loop)
	}
	var plannerPR int64
	var plannerSpec string
	if planner != nil {
		meta := parseJSONObject(planner.MetadataJSON)
		plannerSpec = stringFromAnyDefault(meta["specPath"])
		plannerPR = firstNonZero(derefInt64(planner.PRNumber), int64FromAny(meta["prNumber"]))
	}
	if pending != nil {
		if reusable != nil && derefString(pending.LoopID) == reusable.ID {
			work.SpecPath = plannerSpec
			if err := r.persistDiscoveredWorkerLoop(ctx, repos, reusable, work, ""); err != nil {
				return DiscoveryResult{}, false, err
			}
		}
		return DiscoveryResult{QueueItems: []storage.QueueItemRecord{*pending}}, false, nil
	}
	// Also protect active rows created by an older discovery path without a
	// matching loop. Do not confuse a legacy PR key with a source issue.
	for _, key := range []string{dedupeKey, buildWorkerIssueDedupeKey(project.ID, repo, issue.Number)} {
		active, err := repos.Queue.FindActiveByDedupe(ctx, key)
		if err != nil {
			return DiscoveryResult{}, false, err
		}
		if active != nil && (key == dedupeKey || (active.TargetType == "issue" && strings.EqualFold(active.TargetID, target))) {
			return skipped, false, nil
		}
	}
	var plannerDetail PullRequestDetail
	if plannerPR > 0 {
		var held bool
		var err error
		plannerDetail, held, err = discoveryPRDisposition(ctx, repos, project.ID, repo, plannerPR, states)
		if err != nil || held {
			return skipped, false, err
		}
	}

	var source string
	var recoverySpent, closedPR bool
	for _, loop := range history {
		meta := parseJSONObject(loop.MetadataJSON)
		recoverySpent = recoverySpent || stringFromAnyDefault(meta[discoveryRecoverySource]) != ""
	}
	for _, loop := range history {
		resolved, closed, legacyRevive, err := discoveryHistory(ctx, repos, project, repo, issue.Number, loop, fingerprint, states)
		if err != nil {
			return DiscoveryResult{}, false, err
		}
		if resolved {
			return skipped, false, nil
		}
		if legacyRevive && reusable == nil {
			reusable = &loop
		}
		if !legacyRevive && source == "" {
			source = loop.ID
		}
		closedPR = closedPR || closed
		meta, _ := parseJSONObject(loop.MetadataJSON)["worker"].(map[string]any)
		work.SpecPath = firstNonEmpty(work.SpecPath, stringFromAnyDefault(meta["specPath"]))
	}
	if source != "" && recoverySpent {
		// The single automatic recovery this issue was allowed has already run.
		// Whether that replacement delivered is not decidable from local evidence,
		// so this belongs to the same class as an undecided history: refuse new
		// work and leave the decision to an operator.
		return DiscoveryResult{}, false, undecidedDeliveryHistory{LoopID: source, Reason: "automatic historical recovery was already attempted; manual review required"}
	}

	work.SpecPath = firstNonEmpty(plannerSpec, work.SpecPath)
	selectedPR := plannerPR
	if reusable != nil {
		// Keep the target of an existing issue loop. For a PR-targeted orphan
		// the live observation decides whether that target is still usable.
		selectedPR = derefInt64(reusable.PRNumber)
	}
	if selectedPR > 0 {
		detail := plannerDetail
		if selectedPR != plannerPR {
			var held bool
			var err error
			detail, held, err = discoveryPRDisposition(ctx, repos, project.ID, repo, selectedPR, states)
			if err != nil || held {
				return skipped, false, err
			}
		}
		if !closedPR && detail.State == "open" {
			active, err := repos.Queue.FindActiveByDedupe(ctx, fmt.Sprintf("worker:%s:%s:%d", project.ID, repo, selectedPR))
			if err != nil {
				return DiscoveryResult{}, false, err
			}
			if active != nil && active.TargetType == "pull_request" {
				return skipped, false, nil
			}
			work.PRNumber, work.ExecutionMode = selectedPR, "push-existing"
		}
	}
	now := r.nowISO()
	result := DiscoveryResult{}
	if reusable == nil {
		seq, err := repos.Loops.AllocateSeq(ctx)
		if err != nil {
			return DiscoveryResult{}, false, err
		}
		reusable = &storage.LoopRecord{ID: eventlog.NewEventID("loop"), Seq: seq, ProjectID: project.ID, Type: "worker", Status: "queued", CreatedAt: now}
		result.CreatedLoopIDs = append(result.CreatedLoopIDs, reusable.ID)
	}
	reusable.Status, reusable.NextRunAt = "queued", &now
	reusable.TargetType, reusable.TargetID, reusable.PRNumber = "issue", &target, nil
	lock := storage.IssueLockKey(project.ID, repo, issue.Number)
	if work.PRNumber > 0 {
		reusable.TargetType = "pull_request"
		reusable.TargetID = stringPtr(fmt.Sprintf("pr:%s:%d", repo, work.PRNumber))
		reusable.PRNumber = &work.PRNumber
		lock = storage.PullRequestLockKey(project.ID, repo, work.PRNumber)
	}
	if err := r.persistDiscoveredWorkerLoop(ctx, repos, reusable, work, source); err != nil {
		return DiscoveryResult{}, false, err
	}
	fields := parseJSONObject(stringPtr(mustMarshalJSON(work)))
	fields["discoveryFingerprint"] = fingerprint
	queue := storage.QueueItemRecord{ID: eventlog.NewEventID("queue"), ProjectID: &project.ID, LoopID: &reusable.ID, Type: "worker", TargetType: reusable.TargetType, TargetID: derefString(reusable.TargetID), Repo: &repo, PRNumber: reusable.PRNumber, DedupeKey: dedupeKey, Priority: storage.QueuePriorityWorker, Status: "queued", AvailableAt: now, MaxAttempts: r.retryMaxAttempts, LockKey: &lock, PayloadJSON: stringPtr(mustMarshalJSON(fields)), CreatedAt: now, UpdatedAt: now}
	if err := repos.Queue.Upsert(ctx, queue); err != nil {
		return DiscoveryResult{}, false, err
	}
	result.QueueItems = []storage.QueueItemRecord{queue}
	return result, true, nil
}

func discoveryPRDisposition(ctx context.Context, repos *storage.Repositories, projectID, repo string, pr int64, states map[int64]PullRequestDetail) (PullRequestDetail, bool, error) {
	reviewLoops, err := repos.Loops.ListWorkerDiscoveryReviewFixLoops(ctx, projectID, repo, pr)
	if err != nil {
		return PullRequestDetail{}, false, err
	}
	for _, loop := range reviewLoops {
		if loops.IsReviewFixPairHold(loop) || loops.HasPendingReviewScopeHuman(loop) {
			return PullRequestDetail{}, true, nil
		}
	}
	detail, ok := states[pr]
	if !ok {
		return PullRequestDetail{}, false, discoveryPRLookup(pr)
	}
	for _, lane := range []domain.LoopType{domain.LoopTypeWorker, domain.LoopTypeReviewer, domain.LoopTypeFixer} {
		if domain.IsAutoLaneHeld(lane, detail.Labels) {
			return detail, true, nil
		}
	}
	return detail, false, nil
}

func (r *Runner) persistDiscoveredWorkerLoop(ctx context.Context, repos *storage.Repositories, loop *storage.LoopRecord, work workerInput, source string) error {
	meta := parseJSONObject(loop.MetadataJSON)
	worker := mergeWorkerMetadata(meta, work)
	if work.PRNumber <= 0 {
		if int64FromAny(worker["prNumber"]) > 0 || stringFromAnyDefault(meta["prUrl"]) != "" {
			delete(worker, "branch")
			delete(worker, "headSha")
		}
		delete(worker, "prNumber")
		if loop.TargetType == "issue" {
			delete(meta, "prNumber")
			delete(meta, "prUrl")
		}
	}
	meta["worker"] = worker
	if source != "" {
		meta[discoveryRecoverySource] = source
	}
	loop.Repo, loop.UpdatedAt, loop.MetadataJSON = &work.Repo, r.nowISO(), stringPtr(mustMarshalJSON(meta))
	return repos.Loops.Upsert(ctx, *loop)
}

// A run/loop status by itself is not publication evidence. An open or merged
// historical PR without an exact delivery record needs human reconciliation.
func discoveryHistory(ctx context.Context, repos *storage.Repositories, project storage.ProjectRecord, repo string, issue int64, loop storage.LoopRecord, fingerprint string, states map[int64]PullRequestDetail) (resolved, closed, legacyRevive bool, err error) {
	unknown := func(reason string) error {
		return undecidedDeliveryHistory{LoopID: loop.ID, Reason: reason}
	}
	record, err := delivery.FromMetadata(loop.MetadataJSON)
	if err != nil {
		return false, false, false, unknown(err.Error())
	}
	meta := parseJSONObject(loop.MetadataJSON)
	worker, _ := meta["worker"].(map[string]any)
	prNumbers := []int64{derefInt64(loop.PRNumber), int64FromAny(meta["prNumber"]), int64FromAny(worker["prNumber"])}
	run, err := repos.Runs.GetLatestByLoopID(ctx, loop.ID)
	if err != nil {
		return false, false, false, err
	}
	var checkpoint workerCheckpoint
	if run != nil {
		if run.Status == "running" {
			return true, false, false, nil
		}
		checkpoint, err = parseCheckpoint(run.CheckpointJSON)
		if err != nil {
			return false, false, false, unknown(err.Error())
		}
		if checkpoint.PullRequest != nil {
			prNumbers = append(prNumbers, checkpoint.PullRequest.Number)
		}
		if checkpoint.Lifecycle != nil {
			prNumbers = append(prNumbers, checkpoint.Lifecycle.PRNumber)
		}
	}
	if record != nil {
		if record.ProjectID != project.ID || !strings.EqualFold(record.Repo, repo) || record.WorkerLoopID != loop.ID || record.IssueNumber != issue || record.RunID == "" {
			return false, false, false, unknown("delivery record does not match loop/issue/PR")
		}
		prNumbers = append(prNumbers, record.PRNumber)
	}
	pr := firstNonZero(prNumbers...)
	for _, candidate := range prNumbers {
		if candidate != 0 && candidate != pr {
			return false, false, false, unknown("conflicting historical PR numbers")
		}
	}
	if pr > 0 {
		detail, held, err := discoveryPRDisposition(ctx, repos, project.ID, repo, pr, states)
		if err != nil || held {
			return held, false, false, err
		}
		if detail.State == "closed" {
			return false, true, false, nil
		}
		if record != nil && record.Published && record.HeadSHA != "" && !strings.EqualFold(detail.HeadSHA, record.HeadSHA) {
			return false, false, false, unknown(fmt.Sprintf("PR #%d is %s but its live head does not match the delivered head", pr, detail.State))
		}
		if record != nil && record.Complete && record.VerifiedFor(project.ID, repo, issue, pr) {
			return true, false, false, nil
		}
		return false, false, false, unknown(fmt.Sprintf("PR #%d is %s but lacks completed implementation evidence", pr, detail.State))
	}
	if loop.TargetType == "pull_request" {
		return false, false, false, unknown("publication may already have occurred without a known PR")
	}
	// The legacy open-PR step persisted this exact no-op only after checking
	// that the branch had no commits ahead of base. A branch push in this
	// specific case is not implementation delivery.
	if record == nil && run != nil && checkpoint.Work != nil && checkpoint.Worktree != nil && checkpoint.Worktree.Branch != "" && checkpoint.Work.BaseBranch != "" && checkpoint.SkipReason == fmt.Sprintf("Worker stopped because branch %s has no commits ahead of %s", checkpoint.Worktree.Branch, checkpoint.Work.BaseBranch) {
		return false, false, false, nil
	}
	if checkpoint.Lifecycle != nil && checkpoint.Lifecycle.Pushed {
		return false, false, false, unknown("publication may already have occurred without a known PR")
	}
	if record != nil {
		if !record.Published && !record.MetadataPending && !record.Complete {
			return false, false, false, nil
		}
		return false, false, false, unknown("delivery record lacks a verifiable PR")
	}
	stored := loops.LastFailedDiscoveryFingerprint(loop.MetadataJSON)
	if loops.ShouldSuppressFailedRediscovery(loop.Status, stored, fingerprint) {
		return true, false, false, nil
	}
	if loop.Status == "failed" && stored != "" && run == nil {
		return false, false, true, nil // Existing failed-discovery fingerprint contract.
	}
	if run != nil {
		switch WorkerStep(derefString(run.CurrentStep)) {
		case stepPrepareWork, stepPrepareWorktree, stepPlan:
			if checkpoint.Execution == nil {
				return false, false, false, nil
			}
		}
	}
	return false, false, false, unknown("no affirmative evidence of delivery or non-delivery")
}
