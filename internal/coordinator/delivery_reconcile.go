package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/nexu-io/looper/internal/config"
	"github.com/nexu-io/looper/internal/coordinator/dispatch"
	"github.com/nexu-io/looper/internal/delivery"
	"github.com/nexu-io/looper/internal/domain"
	"github.com/nexu-io/looper/internal/hostingidentity"
	"github.com/nexu-io/looper/internal/loops"
	"github.com/nexu-io/looper/internal/storage"
)

const (
	deliveryReconcileMetadataKey = "deliveryReconcile"
	implementationDeliveryKey    = "implementationDelivery"
	implementationMergedKey      = "implementationMerged"
	plannerSpecPathKey           = "specPath"

	// deliveryCandidatePageSize bounds the loops read per scan pass. Candidates
	// are paged by (seq, id) so a large history is scanned fairly over ticks
	// instead of being loaded or blocked in one pass.
	deliveryCandidatePageSize = 20
	// deliveryPlanningCandidatesPerPass and deliveryImplementationCandidates-
	// PerPass bound remote calls per tick: each candidate costs a small, fixed
	// number of gateway calls.
	deliveryPlanningCandidatesPerPass       = 3
	deliveryImplementationCandidatesPerPass = 3
)

// DeliveryGateway is the hosting surface that delivery reconciliation needs
// beyond GitHubGateway. It is optional so fakes and non-GitHub providers can
// omit it; ReconcileDeliveries reports a configuration error rather than
// guessing when it is missing.
type DeliveryGateway interface {
	// DeliveryPullRequest returns verified delivery evidence for one pull
	// request: remote state, target and source repositories, base branch, head
	// SHA, and the canonical merge SHA with base-membership already checked so
	// squash and rebase merges are recognised.
	DeliveryPullRequest(ctx context.Context, repo string, pr int64, cwd string) (delivery.PullRequest, error)
	// DeliverySpecOnBase reports whether path exists on base at mergedCommit.
	DeliverySpecOnBase(ctx context.Context, repo, base, path, mergedCommit, cwd string) (bool, error)
	// DeliverySpecInPullRequest reports whether pr itself added or modified
	// path, so a persisted template path is not mistaken for this PR's work.
	DeliverySpecInPullRequest(ctx context.Context, repo string, pr int64, path, cwd string) (bool, error)
	// CloseDeliveredIssue closes issue as completed after a verified delivery.
	CloseDeliveredIssue(ctx context.Context, repo string, issue int64, cwd string) error
}

// ReconcileDeliveries advances coordinator-owned planning and implementation
// delivery facts for one project.
//
// Authority for every action here is persisted loop evidence, never issue or PR
// prose: a planner loop that recorded a published spec PR, or a worker loop
// whose implementationDelivery record verifies against the project, repository,
// issue and pull request. Remote state is then re-read because merge status,
// head, base and source cannot be inferred from local rows.
//
// This runs independently of coordinator triage so projects that never enable
// it still hand off a merged spec and close a fully delivered issue. It does
// not merge, requeue, or change restart/failure criteria.
func (r *Runner) ReconcileDeliveries(ctx context.Context, input DiscoveryInput) error {
	if r == nil {
		return nil
	}
	if r.repos == nil || r.repos.Projects == nil || r.repos.Loops == nil {
		return fmt.Errorf("coordinator repositories are not configured")
	}
	if r.config == nil {
		return fmt.Errorf("coordinator config is not configured")
	}
	projectID := strings.TrimSpace(input.ProjectID)
	repo := strings.TrimSpace(input.Repo)
	if projectID == "" || repo == "" {
		return fmt.Errorf("delivery reconciliation requires a project and repository")
	}
	if r.github == nil {
		return nil
	}
	gateway, ok := r.github.(DeliveryGateway)
	if !ok {
		return fmt.Errorf("github gateway does not support delivery reconciliation")
	}
	if bound, err := hostingidentity.Bind(ctx, *r.config, projectID, "coordinator"); err != nil {
		return err
	} else {
		ctx = bound
	}

	// Re-check the project and its configuration immediately before any effect
	// or remote read: the scheduler's coarse gate is not authority for this write.
	project, err := r.repos.Projects.GetByID(ctx, projectID)
	if err != nil {
		return err
	}
	if project == nil {
		return fmt.Errorf("project %q not found", projectID)
	}
	if project.Archived {
		return nil
	}
	if r.projectNetworkMode(projectID) == config.NetworkModeRouted {
		return nil
	}

	reconciler := &deliveryReconciler{
		runner:     r,
		github:     r.github,
		gateway:    gateway,
		projectID:  projectID,
		repo:       repo,
		cwd:        project.RepoPath,
		baseBranch: deliveryBaseBranch(project),
		roles:      config.ProjectRoleConfigs(*r.config, projectID),
	}
	return reconciler.run(ctx)
}

type deliveryReconciler struct {
	runner     *Runner
	github     GitHubGateway
	gateway    DeliveryGateway
	projectID  string
	repo       string
	cwd        string
	baseBranch string
	roles      config.RoleConfigs
}

func (d *deliveryReconciler) run(ctx context.Context) error {
	// Planning and implementation passes are independent; one failing must not
	// stop the other, and neither stops the rest of the scheduler tick.
	var errs []error
	if err := d.scan(ctx, "planner", plannerSpecPathKey, deliveryPlanningCandidatesPerPass, d.reconcilePlanning); err != nil {
		errs = append(errs, err)
	}
	if err := d.scan(ctx, "worker", implementationDeliveryKey, deliveryImplementationCandidatesPerPass, d.reconcileImplementation); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// scan walks at most budget candidates for one loop type, continuing from a
// persisted per-project cursor so long histories are visited fairly across
// ticks. A candidate error is recorded and skipped: it must not starve the rest
// of the page, and its cursor still advances so it cannot block this pass.
func (d *deliveryReconciler) scan(ctx context.Context, loopType, evidenceKey string, budget int, handle func(context.Context, storage.LoopRecord) error) error {
	if budget <= 0 {
		return nil
	}
	cursorKey := d.projectID + "\x00" + d.repo + "\x00" + loopType
	cursor := d.runner.deliveryCandidateCursor(cursorKey)
	rows, err := d.runner.repos.Loops.ListDeliveryCandidates(ctx, d.projectID, loopType, evidenceKey, cursor, deliveryCandidatePageSize)
	if err != nil {
		return err
	}
	if len(rows) == 0 && (cursor.Seq != 0 || cursor.ID != "") {
		// End of the paged range: wrap so candidates behind the cursor are
		// revisited instead of being starved by a growing history.
		cursor = storage.DeliveryCandidateCursor{}
		rows, err = d.runner.repos.Loops.ListDeliveryCandidates(ctx, d.projectID, loopType, evidenceKey, cursor, deliveryCandidatePageSize)
		if err != nil {
			return err
		}
	}
	var errs []error
	processed := 0
	for _, loop := range rows {
		if processed >= budget {
			break
		}
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		processed++
		if handleErr := d.withIssueLock(ctx, loop, handle); handleErr != nil {
			errs = append(errs, fmt.Errorf("%s loop %s: %w", loopType, loop.ID, handleErr))
		}
		d.runner.setDeliveryCandidateCursor(cursorKey, storage.DeliveryCandidateCursor{Seq: loop.Seq, ID: loop.ID})
	}
	return errors.Join(errs...)
}

// Share merge-watch's issue lock. Concurrent ticks can load the same candidate
// before either has persisted its result; never act on the queued stale copy.
func (d *deliveryReconciler) withIssueLock(ctx context.Context, row storage.LoopRecord, handle func(context.Context, storage.LoopRecord) error) error {
	var issue int64
	if row.Type == "planner" {
		if e, ok := planningEvidenceFor(row, d.projectID, d.repo); ok {
			issue = e.IssueNumber
		}
	} else if rec, err := delivery.FromMetadata(row.MetadataJSON); err == nil && rec != nil {
		issue = rec.IssueNumber
	}
	if issue <= 0 {
		return handle(ctx, row)
	}
	lock := d.runner.watchLock(d.repo, issue)
	lock.Lock()
	defer lock.Unlock()
	fresh, err := d.runner.repos.Loops.GetByID(ctx, row.ID)
	if err != nil || fresh == nil {
		return err
	}
	if fresh.Status != row.Status || fresh.UpdatedAt != row.UpdatedAt || fresh.MetadataJSON == nil || row.MetadataJSON == nil || *fresh.MetadataJSON != *row.MetadataJSON {
		return nil
	}
	return handle(ctx, *fresh)
}

// deliveryCandidateCursor and setDeliveryCandidateCursor keep the fair-scan
// position in RuntimeState so it survives configuration snapshots but not the
// daemon; losing it only restarts the rotation.
func (r *Runner) deliveryCandidateCursor(key string) storage.DeliveryCandidateCursor {
	if r == nil || r.state == nil {
		return storage.DeliveryCandidateCursor{}
	}
	r.state.mu.Lock()
	defer r.state.mu.Unlock()
	return r.state.deliveryCursors[key]
}

func (r *Runner) setDeliveryCandidateCursor(key string, cursor storage.DeliveryCandidateCursor) {
	if r == nil || r.state == nil {
		return
	}
	r.state.mu.Lock()
	defer r.state.mu.Unlock()
	r.state.deliveryCursors[key] = cursor
}

func (r *Runner) nowISO() string {
	return r.now().UTC().Format(jsISOStringLayout)
}

func deliveryBaseBranch(project *storage.ProjectRecord) string {
	if project == nil {
		return "main"
	}
	if branch := strings.TrimSpace(deliveryDerefString(project.BaseBranch)); branch != "" {
		return branch
	}
	return "main"
}

// deliveryReconcileState is the per-loop reconciliation marker. done stops a
// candidate from being scanned again, so it is only written once every effect
// for that candidate has either completed or been proven unnecessary; a partial
// failure leaves done false and is retried on a later tick.
type deliveryReconcileState struct {
	Done        bool   `json:"done,omitempty"`
	Kind        string `json:"kind,omitempty"`
	Outcome     string `json:"outcome,omitempty"`
	Reason      string `json:"reason,omitempty"`
	PRNumber    int64  `json:"prNumber,omitempty"`
	IssueNumber int64  `json:"issueNumber,omitempty"`
	MergeSHA    string `json:"mergeSha,omitempty"`
	HeadSHA     string `json:"headSha,omitempty"`
	// RecordHeadSHA binds an implementation marker to the delivery record it
	// judged; a re-published record with a new head is reconsidered.
	RecordHeadSHA    string `json:"recordHeadSha,omitempty"`
	IssueClosed      bool   `json:"issueClosed,omitempty"`
	CloseRequestedAt string `json:"closeRequestedAt,omitempty"`
	LabelsReconciled bool   `json:"labelsReconciled,omitempty"`
	ReconciledAt     string `json:"reconciledAt,omitempty"`
}

// implementationMergedState records that a verified implementation PR was
// merged. It is written independently of closability so a merge is never lost
// just because the issue could not be closed safely yet.
type implementationMergedState struct {
	PRNumber    int64  `json:"prNumber"`
	MergeSHA    string `json:"mergeSha"`
	HeadSHA     string `json:"headSha,omitempty"`
	BaseBranch  string `json:"baseBranch,omitempty"`
	IssueNumber int64  `json:"issueNumber"`
	MergedAt    string `json:"mergedAt,omitempty"`
	Outcome     string `json:"outcome,omitempty"`
}

// writeDeliveryState merges reconciliation state into the loop metadata with a
// compare-and-swap on the row read here, so a concurrent worker or reviewer
// write is never clobbered. A CAS miss is not an error: the next tick re-reads.
// An unchanged state is not rewritten, so deferred candidates do not churn rows.
func (d *deliveryReconciler) writeDeliveryState(ctx context.Context, loopID string, reconcile deliveryReconcileState, merged *implementationMergedState) error {
	current, err := d.runner.repos.Loops.GetByID(ctx, loopID)
	if err != nil || current == nil {
		return err
	}
	metadata := deliveryMetadataObject(current.MetadataJSON)
	if metadata == nil {
		if current.MetadataJSON != nil && strings.TrimSpace(*current.MetadataJSON) != "" {
			return fmt.Errorf("loop %s metadata is not a JSON object", loopID)
		}
		metadata = map[string]any{}
	}
	previous := readDeliveryReconcileState(metadata)
	if reconcile.CloseRequestedAt == "" && previous.PRNumber == reconcile.PRNumber {
		reconcile.CloseRequestedAt = previous.CloseRequestedAt
	}
	previousMerged := readImplementationMergedState(metadata)
	if merged != nil && previousMerged != nil && previousMerged.PRNumber == merged.PRNumber && previousMerged.MergeSHA == merged.MergeSHA && previousMerged.MergedAt != "" {
		merged.MergedAt = previousMerged.MergedAt
	}
	reconcile.ReconciledAt = previous.ReconciledAt
	sameReconcile := previous == reconcile
	sameMerged := merged == nil || (previousMerged != nil && *previousMerged == *merged)
	if sameReconcile && sameMerged {
		return nil
	}
	reconcile.ReconciledAt = d.runner.nowISO()
	metadata[deliveryReconcileMetadataKey] = reconcile
	if merged != nil {
		metadata[implementationMergedKey] = *merged
	}
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	value := string(encoded)
	_, err = d.runner.repos.Loops.UpdateMetadataIfMatch(ctx, loopID, &value, d.runner.nowISO(), current.MetadataJSON)
	return err
}

func readDeliveryReconcileState(metadata map[string]any) deliveryReconcileState {
	var state deliveryReconcileState
	if raw, ok := metadata[deliveryReconcileMetadataKey]; ok {
		if encoded, err := json.Marshal(raw); err == nil {
			_ = json.Unmarshal(encoded, &state)
		}
	}
	return state
}

func readImplementationMergedState(metadata map[string]any) *implementationMergedState {
	raw, ok := metadata[implementationMergedKey]
	if !ok {
		return nil
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return nil
	}
	var state implementationMergedState
	if json.Unmarshal(encoded, &state) != nil {
		return nil
	}
	return &state
}

func (d *deliveryReconciler) markDone(ctx context.Context, loop storage.LoopRecord, state deliveryReconcileState, merged *implementationMergedState) error {
	state.Done = true
	return d.writeDeliveryState(ctx, loop.ID, state, merged)
}

// issueHeld reports a human hold that must stop automatic handoff and closing.
// Hold labels are the operator's control surface, so they win over any derived
// state, including our own merged delivery fact.
func (d *deliveryReconciler) issueHeld(labels []string) bool {
	for _, loopType := range domain.LoopTypes {
		if domain.IsAutoLaneHeld(loopType, labels) {
			return true
		}
	}
	if hold := strings.TrimSpace(d.roles.Coordinator.Dispatch.Autonomous.HoldLabel); hold != "" && hasExactLabel(labels, hold) {
		return true
	}
	return false
}

// reviewFixInFlight reports reviewer/fixer scope or budget holds, pending scope
// evidence, and in-flight reviewer/fixer work on one pull request. Any of them
// means a human or a role is still deciding about this PR, so delivery
// reconciliation must not close the linked issue underneath it.
func (d *deliveryReconciler) reviewFixInFlight(ctx context.Context, prNumber int64) (bool, error) {
	if prNumber <= 0 {
		return false, nil
	}
	prLoops, err := d.runner.repos.Loops.ListByRepoAndPR(ctx, d.repo, prNumber)
	if err != nil {
		return false, err
	}
	for _, loop := range prLoops {
		if loop.ProjectID != d.projectID {
			continue
		}
		if loop.Type != "reviewer" && loop.Type != "fixer" {
			continue
		}
		if loops.IsReviewScopeHumanHold(loop) || loops.HasPendingReviewScopeHuman(loop) || loops.IsReviewFixBudgetHold(loop) {
			return true, nil
		}
		if deliveryLoopInFlight(loop.Status) {
			return true, nil
		}
		if ask, ok := loops.ReadHITLAsk(loop.MetadataJSON); ok && ask.Status == "awaiting" {
			return true, nil
		}
	}
	return false, nil
}

// workerInFlight reports an unfinished worker loop that still tracks the issue,
// excluding the loop whose delivery is being reconciled.
func (d *deliveryReconciler) workerInFlight(ctx context.Context, excludeLoopID string, issueNumber int64) (bool, error) {
	if issueNumber <= 0 {
		return false, nil
	}
	list, err := d.runner.repos.Loops.ListWorkerLoopsForIssue(ctx, d.projectID, d.repo, issueNumber)
	if err != nil {
		return false, err
	}
	for _, loop := range list {
		if loop.ID == excludeLoopID || loop.Type != "worker" || loop.ProjectID != d.projectID {
			continue
		}
		if !deliveryWorkerTracksIssue(loop, d.repo, issueNumber) {
			continue
		}
		if deliveryLoopInFlight(loop.Status) {
			return true, nil
		}
	}
	return false, nil
}

// deliveryLoopInFlight is the status set that means work is genuinely underway.
// waiting and idle loops are dormant and must not block a close forever.
func deliveryLoopInFlight(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "queued", "running", "awaiting_human", "human_takeover":
		return true
	default:
		return false
	}
}

// deliveryWorkerTracksIssue mirrors worker discovery's ownership rule: the loop
// targets the issue directly, or its persisted work input names the issue and
// issue repository.
func deliveryWorkerTracksIssue(loop storage.LoopRecord, repo string, issueNumber int64) bool {
	if issueNumber <= 0 {
		return false
	}
	if loop.TargetType == "issue" && strings.EqualFold(deliveryDerefString(loop.TargetID), fmt.Sprintf("issue:%s:%d", repo, issueNumber)) {
		return true
	}
	workerMeta, _ := deliveryMetadataObject(loop.MetadataJSON)["worker"].(map[string]any)
	if deliveryInt64(workerMeta["issueNumber"]) != issueNumber {
		return false
	}
	tracked := firstNonEmpty(deliveryDerefString(deliveryStringPtr(workerMeta["issueRepo"])), deliveryDerefString(deliveryStringPtr(workerMeta["repo"])), deliveryDerefString(loop.Repo))
	return strings.EqualFold(tracked, repo)
}

// implementationHandoffLabels returns the labels that hand an issue to the
// Worker after its spec landed on base.
func (d *deliveryReconciler) implementationHandoffLabels() []string {
	labels := requiredTriggerLabels(d.roles.Worker.Triggers)
	for _, label := range labels {
		if label == dispatch.DispatchImplement {
			return labels
		}
	}
	return append(labels, dispatch.DispatchImplement)
}

// planningStaleLabels returns the labels that would re-dispatch planning after
// the spec has landed.
func (d *deliveryReconciler) planningStaleLabels(labels []string) []string {
	patterns := []string{dispatch.DispatchPlan}
	patterns = append(patterns, requiredTriggerLabels(d.roles.Planner.Triggers)...)
	return matchingLabels(labels, patterns)
}

func deliveryMetadataObject(raw *string) map[string]any {
	if raw == nil || strings.TrimSpace(*raw) == "" {
		return nil
	}
	var object map[string]any
	if err := json.Unmarshal([]byte(*raw), &object); err != nil {
		return nil
	}
	return object
}

func deliveryInt64(value any) int64 {
	switch typed := value.(type) {
	case nil:
		return 0
	case float64:
		return int64(typed)
	case int64:
		return typed
	case json.Number:
		parsed, _ := typed.Int64()
		return parsed
	case string:
		return asInt64(typed)
	default:
		return 0
	}
}

func deliveryStringPtr(value any) *string {
	text := strings.TrimSpace(asString(value))
	if text == "" {
		return nil
	}
	return &text
}

func deliveryDerefString(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}

func deliveryDerefInt64(value *int64) int64 {
	if value == nil {
		return 0
	}
	return *value
}

func deliveryMissingLabels(existing, want []string) []string {
	missing := make([]string, 0, len(want))
	for _, label := range want {
		if label == "" || hasExactLabel(existing, label) {
			continue
		}
		missing = append(missing, label)
	}
	return missing
}
